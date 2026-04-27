#!/usr/bin/env python3
"""
ORB Forward Test — Telegram Portfolio Bot
Polls for commands and replies with live stats from trades.csv

Commands:
  /stats     — full portfolio summary
  /today     — today's status (bias, trade, PnL)
  /trades    — last 10 trades
  /window    — current rolling 30-day vs Topstep 150K target
  /help      — list commands
"""
import csv, json, time, logging, requests
from datetime import datetime, date, timedelta
from pathlib import Path
from collections import defaultdict

# ── Config ────────────────────────────────────────────────────────────────────
TG_TOKEN        = "8552528128:AAF_kCmAVB8-7WvULrbDgvCrS4vbP9gL62o"
TG_CHAT_ID      = "1136518861"
LOG_DIR         = Path("/root/orb_forward_test")
TRADE_LOG       = LOG_DIR / "trades.csv"
STATE_F         = LOG_DIR / "daily_state.json"
BASELINE_LOG    = Path("/root/orb_forward_test_baseline/trades.csv")

# Topstep 150K rules
PROFIT_TARGET  = 9_000
MAX_DAILY_LOSS = 3_000
MAX_TRAIL_DD   = 4_500
CONSIST_CAP    = 4_500
MIN_DAYS       = 5

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[logging.FileHandler(LOG_DIR / "bot.log"),
                               logging.StreamHandler()])
log = logging.getLogger(__name__)

BASE_URL = f"https://api.telegram.org/bot{TG_TOKEN}"


# ── Telegram helpers ──────────────────────────────────────────────────────────
def send(msg: str, chat_id: str = TG_CHAT_ID):
    try:
        requests.post(f"{BASE_URL}/sendMessage",
                      json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
                      timeout=10)
    except Exception as e:
        log.warning(f"Send error: {e}")


def get_updates(offset: int) -> list:
    try:
        r = requests.get(f"{BASE_URL}/getUpdates",
                         params={"offset": offset, "timeout": 30},
                         timeout=40)
        return r.json().get("result", [])
    except Exception:
        return []


# ── Data loading ──────────────────────────────────────────────────────────────
def load_trades(path: Path = TRADE_LOG) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def load_state() -> dict:
    if STATE_F.exists():
        s = json.loads(STATE_F.read_text())
        if s.get("date") == str(date.today()):
            return s
    return {}


# ── Stats calculators ─────────────────────────────────────────────────────────
def portfolio_stats(trades: list[dict]) -> dict:
    if not trades:
        return {}
    pnls    = [float(t["pnl_dollars"]) for t in trades]
    wins    = [p for p in pnls if p > 0]
    losses  = [p for p in pnls if p < 0]
    total   = sum(pnls)
    pf      = sum(wins) / abs(sum(losses)) if losses else float("inf")

    # Max trailing drawdown
    peak, dd, max_dd = 0.0, 0.0, 0.0
    equity = 0.0
    for p in pnls:
        equity += p
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    # Daily PnL
    daily: dict = defaultdict(float)
    for t in trades:
        daily[t["date"]] += float(t["pnl_dollars"])
    best_day  = max(daily.values()) if daily else 0
    worst_day = min(daily.values()) if daily else 0

    return {
        "n_trades":    len(trades),
        "n_days":      len(daily),
        "total_pnl":   total,
        "win_rate":    len(wins) / len(pnls),
        "avg_win":     sum(wins)  / len(wins)   if wins   else 0,
        "avg_loss":    sum(losses)/ len(losses)  if losses else 0,
        "profit_factor": pf,
        "max_dd":      max_dd,
        "best_day":    best_day,
        "worst_day":   worst_day,
        "daily":       daily,
    }


def rolling_window_stats(trades: list[dict], window: int = 30) -> dict:
    """Stats for the most recent N trading days."""
    daily: dict = defaultdict(float)
    for t in trades:
        daily[t["date"]] += float(t["pnl_dollars"])

    sorted_days = sorted(daily.keys())
    if len(sorted_days) < 1:
        return {}

    # Take last `window` trading days
    recent_days = sorted_days[-window:]
    recent_pnl  = {d: daily[d] for d in recent_days}
    total = sum(recent_pnl.values())
    n_days = len(recent_days)

    # Trailing drawdown in this window
    peak, max_dd = 0.0, 0.0
    equity = 0.0
    for d in recent_days:
        equity += recent_pnl[d]
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    best_day  = max(recent_pnl.values())
    worst_day = min(recent_pnl.values())

    # Pass/fail simulation
    failed, fail_reason = False, ""
    if worst_day < -MAX_DAILY_LOSS:
        failed, fail_reason = True, f"daily loss exceeded (${worst_day:.0f})"
    if max_dd > MAX_TRAIL_DD:
        failed, fail_reason = True, f"trailing DD exceeded (${max_dd:.0f})"
    if best_day > CONSIST_CAP:
        failed, fail_reason = True, f"consistency cap breached (${best_day:.0f})"
    if not failed and total < PROFIT_TARGET:
        fail_reason = f"${total:,.0f} / ${PROFIT_TARGET:,} target"
    elif not failed:
        fail_reason = ""

    passed = (not failed and total >= PROFIT_TARGET and n_days >= MIN_DAYS)

    return {
        "days":       n_days,
        "total":      total,
        "max_dd":     max_dd,
        "best_day":   best_day,
        "worst_day":  worst_day,
        "passed":     passed,
        "fail_reason": fail_reason,
        "start":      recent_days[0] if recent_days else "—",
        "end":        recent_days[-1] if recent_days else "—",
    }


# ── Command handlers ──────────────────────────────────────────────────────────
def cmd_help() -> str:
    return (
        "<b>ORB Forward Test Bot</b>\n\n"
        "<b>Filtered runner (AlphaLabs):</b>\n"
        "/stats    — full portfolio summary\n"
        "/today    — today's session status\n"
        "/trades   — last 10 trades\n"
        "/window   — rolling 30-day vs Topstep 150K\n\n"
        "<b>Baseline runner (no filter):</b>\n"
        "/b_stats   — baseline portfolio summary\n"
        "/b_trades  — baseline last 10 trades\n"
        "/b_window  — baseline rolling 30-day\n\n"
        "/help     — this message"
    )


def cmd_stats() -> str:
    trades = load_trades()
    if not trades:
        return "No trades logged yet."

    s = portfolio_stats(trades)
    pf_str = f"{s['profit_factor']:.2f}" if s['profit_factor'] != float("inf") else "∞"
    sign   = "+" if s['total_pnl'] >= 0 else ""

    lines = [
        f"<b>Portfolio Summary</b>",
        f"Period: {trades[0]['date']} → {trades[-1]['date']}",
        f"",
        f"Trades:         {s['n_trades']}",
        f"Trading days:   {s['n_days']}",
        f"Win rate:       {s['win_rate']:.1%}",
        f"Avg winner:     +${s['avg_win']:,.0f}",
        f"Avg loser:      ${s['avg_loss']:,.0f}",
        f"Profit factor:  {pf_str}",
        f"",
        f"Total PnL:      {sign}${s['total_pnl']:,.0f}",
        f"Max drawdown:   ${s['max_dd']:,.0f}  (limit $4,500)",
        f"Best day:       +${s['best_day']:,.0f}  (limit $4,500)",
        f"Worst day:      ${s['worst_day']:,.0f}  (limit -$3,000)",
    ]
    return "\n".join(lines)


def cmd_today() -> str:
    state  = load_state()
    trades = load_trades()
    today  = str(date.today())

    today_trades = [t for t in trades if t["date"] == today]
    today_pnl    = sum(float(t["pnl_dollars"]) for t in today_trades)

    phase = state.get("phase", "unknown")
    macro = state.get("macro_bias")
    bias_map = {"long": "BULLISH", "short": "BEARISH", None: "NEUTRAL", "PENDING": "PENDING"}
    bias_str = bias_map.get(macro, str(macro))

    lines = [f"<b>Today — {today}</b>", f"Macro bias: {bias_str}"]

    if phase == "awaiting_zone":
        lines.append("Status: Waiting for 8:00 AM zone")
    elif phase == "awaiting_bias":
        z = state.get("zone", {})
        lines.append(f"Status: Zone locked — watching for bias")
        lines.append(f"Zone: H={z.get('high',0):.2f} L={z.get('low',0):.2f} M={z.get('mid',0):.2f}")
    elif phase == "in_trade":
        t = state.get("trade", {})
        if t:
            lines.append(f"Status: IN TRADE")
            lines.append(f"Dir: {t.get('dir','?').upper()} | Entry: {t.get('entry_price',0):.2f}")
            lines.append(f"SL: {t.get('current_sl',0):.2f} | TP: {t.get('tp',0):.2f}")
    elif phase == "done":
        lines.append("Status: Session complete")

    if today_trades:
        sign = "+" if today_pnl >= 0 else ""
        lines.append(f"Today PnL: {sign}${today_pnl:,.0f}  ({len(today_trades)} trade{'s' if len(today_trades)>1 else ''})")
    else:
        lines.append("Today PnL: $0 (no closed trades yet)")

    return "\n".join(lines)


def cmd_trades() -> str:
    trades = load_trades()
    if not trades:
        return "No trades logged yet."

    recent = trades[-10:]
    lines  = [f"<b>Last {len(recent)} Trades</b>", ""]

    for t in recent:
        pnl  = float(t["pnl_dollars"])
        sign = "+" if pnl >= 0 else ""
        icon = "WIN" if pnl >= 0 else "LOSS"
        lines.append(
            f"{icon} {t['date']} {t['direction'].upper()}"
            f"  {sign}${pnl:,.0f}  [{t['exit_reason']}]"
        )

    total = sum(float(t["pnl_dollars"]) for t in recent)
    sign  = "+" if total >= 0 else ""
    lines.append(f"\nLast {len(recent)} total: {sign}${total:,.0f}")
    return "\n".join(lines)


def cmd_window() -> str:
    trades = load_trades()
    if not trades:
        return "No trades logged yet."

    w = rolling_window_stats(trades, window=30)
    if not w:
        return "Not enough data for a 30-day window yet."

    status = "PASS" if w["passed"] else "FAIL"
    pct    = min(w["total"] / PROFIT_TARGET * 100, 100)
    bar_n  = int(pct / 5)
    bar    = "█" * bar_n + "░" * (20 - bar_n)

    lines = [
        f"<b>Rolling 30-Day Window</b>",
        f"{w['start']} → {w['end']}",
        f"",
        f"Progress:  [{bar}] {pct:.0f}%",
        f"PnL:       ${w['total']:,.0f} / ${PROFIT_TARGET:,}",
        f"",
        f"Trading days:  {w['days']} / {MIN_DAYS} min",
        f"Max drawdown:  ${w['max_dd']:,.0f}  (limit ${MAX_TRAIL_DD:,})",
        f"Best day:      ${w['best_day']:,.0f}  (limit ${CONSIST_CAP:,})",
        f"Worst day:     ${w['worst_day']:,.0f}  (limit -${MAX_DAILY_LOSS:,})",
        f"",
    ]

    if w["passed"]:
        lines.append(f"Status: PASS — challenge target met!")
    elif w["fail_reason"]:
        lines.append(f"Status: FAIL — {w['fail_reason']}")
    else:
        lines.append(f"Status: In progress...")

    return "\n".join(lines)


# ── Baseline command handlers ─────────────────────────────────────────────────
def cmd_baseline_stats() -> str:
    trades = load_trades(BASELINE_LOG)
    if not trades:
        return "No baseline trades logged yet."
    s = portfolio_stats(trades)
    pf_str = f"{s['profit_factor']:.2f}" if s['profit_factor'] != float("inf") else "∞"
    sign   = "+" if s['total_pnl'] >= 0 else ""
    lines = [
        f"<b>Baseline Summary (No Filter)</b>",
        f"Period: {trades[0]['date']} → {trades[-1]['date']}",
        f"",
        f"Trades:         {s['n_trades']}",
        f"Trading days:   {s['n_days']}",
        f"Win rate:       {s['win_rate']:.1%}",
        f"Avg winner:     +${s['avg_win']:,.0f}",
        f"Avg loser:      ${s['avg_loss']:,.0f}",
        f"Profit factor:  {pf_str}",
        f"",
        f"Total PnL:      {sign}${s['total_pnl']:,.0f}",
        f"Max drawdown:   ${s['max_dd']:,.0f}  (limit $4,500)",
        f"Best day:       +${s['best_day']:,.0f}  (limit $4,500)",
        f"Worst day:      ${s['worst_day']:,.0f}  (limit -$3,000)",
    ]
    return "\n".join(lines)


def cmd_baseline_trades() -> str:
    trades = load_trades(BASELINE_LOG)
    if not trades:
        return "No baseline trades logged yet."
    recent = trades[-10:]
    lines  = [f"<b>Baseline Last {len(recent)} Trades</b>", ""]
    for t in recent:
        pnl  = float(t["pnl_dollars"])
        sign = "+" if pnl >= 0 else ""
        icon = "WIN" if pnl >= 0 else "LOSS"
        lines.append(f"{icon} {t['date']} {t['direction'].upper()}  {sign}${pnl:,.0f}  [{t['exit_reason']}]")
    total = sum(float(t["pnl_dollars"]) for t in recent)
    sign  = "+" if total >= 0 else ""
    lines.append(f"\nLast {len(recent)} total: {sign}${total:,.0f}")
    return "\n".join(lines)


def cmd_baseline_window() -> str:
    trades = load_trades(BASELINE_LOG)
    if not trades:
        return "No baseline trades logged yet."
    w = rolling_window_stats(trades, window=30)
    if not w:
        return "Not enough baseline data for a 30-day window yet."
    pct   = min(w["total"] / PROFIT_TARGET * 100, 100)
    bar_n = int(pct / 5)
    bar   = "█" * bar_n + "░" * (20 - bar_n)
    lines = [
        f"<b>Baseline Rolling 30-Day (No Filter)</b>",
        f"{w['start']} → {w['end']}",
        f"",
        f"Progress:  [{bar}] {pct:.0f}%",
        f"PnL:       ${w['total']:,.0f} / ${PROFIT_TARGET:,}",
        f"",
        f"Trading days:  {w['days']} / {MIN_DAYS} min",
        f"Max drawdown:  ${w['max_dd']:,.0f}  (limit ${MAX_TRAIL_DD:,})",
        f"Best day:      ${w['best_day']:,.0f}  (limit ${CONSIST_CAP:,})",
        f"Worst day:     ${w['worst_day']:,.0f}  (limit -${MAX_DAILY_LOSS:,})",
        f"",
    ]
    if w["passed"]:
        lines.append("Status: PASS — challenge target met!")
    elif w["fail_reason"]:
        lines.append(f"Status: FAIL — {w['fail_reason']}")
    else:
        lines.append("Status: In progress...")
    return "\n".join(lines)


# ── Main polling loop ─────────────────────────────────────────────────────────
COMMANDS = {
    "/help":      cmd_help,
    "/start":     cmd_help,
    "/stats":     cmd_stats,
    "/today":     cmd_today,
    "/trades":    cmd_trades,
    "/window":    cmd_window,
    "/b_stats":   cmd_baseline_stats,
    "/b_trades":  cmd_baseline_trades,
    "/b_window":  cmd_baseline_window,
}


def run():
    LOG_DIR.mkdir(exist_ok=True)
    log.info("=== Portfolio Bot started ===")
    send("Portfolio bot online. Type /help to see commands.")
    offset = 0

    while True:
        updates = get_updates(offset)
        for upd in updates:
            offset = upd["update_id"] + 1
            msg = upd.get("message", {})
            text = msg.get("text", "").strip().split()[0].lower()
            chat_id = str(msg.get("chat", {}).get("id", TG_CHAT_ID))

            if text in COMMANDS:
                log.info(f"Command: {text} from {chat_id}")
                reply = COMMANDS[text]()
                send(reply, chat_id)
            elif text.startswith("/"):
                send("Unknown command. Type /help for the list.", chat_id)

        time.sleep(2)


if __name__ == "__main__":
    run()
