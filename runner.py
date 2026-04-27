#!/usr/bin/env python3
"""
ORB NQ Forward Test Runner - Shadow Mode
VPS: Ubuntu UTC | Strategy v2.0 (RR=6, SL=25, sl_scan_high=35, min_bias=4.0)

Runs daily Mon-Fri, started by cron at 11:30 UTC.
Sends Telegram alerts. Logs all trades to /root/orb_forward_test/trades.csv.
"""
import json, csv, time, logging, requests, sys, fcntl
import yfinance as yf
from datetime import datetime, date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# ── Credentials ───────────────────────────────────────────────────────────────
ALPHALABS_URL = "https://app.alphalabs.live/api/v1/macro-compass"
ALPHALABS_KEY = "alph_O5G06XBtU9DKmAzzzqhttdq4tsq_vdir"
TG_TOKEN      = "8552528128:AAF_kCmAVB8-7WvULrbDgvCrS4vbP9gL62o"
TG_CHAT_ID    = "1136518861"

# ── Strategy params (v2.0 optimal) ────────────────────────────────────────────
RR           = 6.0
SL_DEFAULT   = 25.0
SL_SCAN_LOW  = 15.0
SL_SCAN_HIGH = 35.0
POINT_VALUE  = 20.0
COMMISSION   = 2.80   # round-trip, 1 NQ contract

# ── Paths ─────────────────────────────────────────────────────────────────────
ET       = ZoneInfo("America/New_York")
LOG_DIR  = Path("/root/orb_forward_test")
TRADE_LOG= LOG_DIR / "trades.csv"
STATE_F  = LOG_DIR / "daily_state.json"
LOG_FILE = LOG_DIR / "runner.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
log = logging.getLogger(__name__)


# ── Telegram ──────────────────────────────────────────────────────────────────
def tg(msg: str):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        log.warning(f"Telegram error: {e}")


# ── AlphaLabs macro bias ──────────────────────────────────────────────────────
def get_macro_bias():
    """Returns 'long' / 'short' / None. Retries up to 3 times."""
    for attempt in range(1, 4):
        try:
            r = requests.post(
                ALPHALABS_URL,
                headers={"X-API-Key": ALPHALABS_KEY, "Content-Type": "application/json"},
                json={"question": "What is the macro bias for Nasdaq 100 futures today? "
                                  "Consider Fed policy, tariffs, inflation data, and geopolitical risk."},
                timeout=30
            )
            data = r.json()
            if not data.get("success"):
                log.warning(f"AlphaLabs error (attempt {attempt}/3): {data}")
                time.sleep(5)
                continue
            for a in data["data"].get("focusAssets", []):
                name = a.get("asset", "").lower()
                if "nasdaq" in name or "nq" in name:
                    raw = a.get("bias", "").upper()
                    if raw == "BULLISH": return "long"
                    if raw == "BEARISH": return "short"
                    return None
            return None
        except Exception as e:
            log.warning(f"AlphaLabs error (attempt {attempt}/3): {e}")
            time.sleep(5)
    log.warning("AlphaLabs: all 3 attempts failed — defaulting to None")
    return None


# ── Market data ───────────────────────────────────────────────────────────────
def get_bars():
    """Fetch NQ=F 30m bars (last 5 days), index in ET naive timestamps."""
    bars = yf.Ticker("NQ=F").history(period="5d", interval="30m", auto_adjust=True)
    if bars.empty:
        return bars
    bars.index = bars.index.tz_convert("America/New_York").tz_localize(None)
    bars.columns = [c.lower() for c in bars.columns]
    return bars


# ── Session levels for SL anchoring ──────────────────────────────────────────
def find_session_levels(bars, today: date):
    levels = []
    prev = today - timedelta(days=1)
    asia_s = datetime(prev.year, prev.month, prev.day, 18, 0)
    asia_e = datetime(today.year, today.month, today.day, 3, 0)
    asia = bars.loc[asia_s:asia_e]
    if len(asia) > 0:
        levels += [("asia_high", float(asia["high"].max())),
                   ("asia_low",  float(asia["low"].min()))]
    lon_s = datetime(today.year, today.month, today.day, 3, 30)
    lon_e = datetime(today.year, today.month, today.day, 7, 30)
    london = bars.loc[lon_s:lon_e]
    if len(london) > 0:
        levels += [("london_high", float(london["high"].max())),
                   ("london_low",  float(london["low"].min()))]
    return levels


def pick_sl(entry, direction, levels):
    """Pick nearest session level within [SL_SCAN_LOW, SL_SCAN_HIGH] or use default."""
    candidates = []
    for name, lvl in levels:
        dist = (entry - lvl) if direction == "long" else (lvl - entry)
        if SL_SCAN_LOW <= dist <= SL_SCAN_HIGH:
            candidates.append((dist, lvl, name))
    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1], candidates[0][2]
    return ((entry - SL_DEFAULT, "default") if direction == "long"
            else (entry + SL_DEFAULT, "default"))


# ── Trade log ─────────────────────────────────────────────────────────────────
HEADERS = ["date", "direction", "entry_time", "entry_price", "sl_price", "tp_price",
           "exit_time", "exit_price", "exit_reason", "pnl_points", "pnl_dollars",
           "zone_high", "zone_low", "zone_mid", "sl_source", "macro_bias"]

def log_trade(row: dict):
    exists = TRADE_LOG.exists()
    with open(TRADE_LOG, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADERS)
        if not exists:
            w.writeheader()
        w.writerow(row)


# ── State persistence ─────────────────────────────────────────────────────────
def load_state() -> dict:
    if STATE_F.exists():
        s = json.loads(STATE_F.read_text())
        if s.get("date") == str(date.today()):
            return s
    return {"date": str(date.today()), "phase": "awaiting_zone",
            "macro_bias": "PENDING", "zone": None, "trade": None}

def save_state(s: dict):
    STATE_F.write_text(json.dumps(s, default=str))


# ── Helpers ───────────────────────────────────────────────────────────────────
def now_et() -> datetime:
    return datetime.now(ET).replace(tzinfo=None)

def bias_label(b) -> str:
    return {"long": "BULLISH", "short": "BEARISH", None: "NEUTRAL"}.get(b, "NEUTRAL")


# ── Main loop ─────────────────────────────────────────────────────────────────
def run():
    LOG_DIR.mkdir(exist_ok=True)

    # Bug 2 fix: prevent duplicate cron instances via exclusive file lock
    lock_fh = open(LOG_DIR / "runner.lock", "w")
    try:
        fcntl.lockf(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        log.info("Another runner instance already running. Exiting.")
        sys.exit(0)

    log.info("=== ORB Forward Runner started ===")
    state = load_state()
    today = date.today()

    # Step 1: fetch macro bias once per day
    if state["macro_bias"] == "PENDING":
        log.info("Fetching AlphaLabs macro bias...")
        bias = get_macro_bias()
        state["macro_bias"] = bias
        save_state(state)
        tg(f"<b>ORB Runner - {today}</b>\n"
           f"Macro bias: <b>{bias_label(bias)}</b>\n"
           f"Waiting for 8:00 AM zone...")
        log.info(f"Macro bias: {bias_label(bias)}")

    macro_bias = state["macro_bias"]

    # Main loop
    while True:
        now  = now_et()
        h, m = now.hour, now.minute

        # Session over at 16:00 ET
        if h >= 16:
            t = state.get("trade")
            if t and not t.get("closed"):
                bars = get_bars()
                today_b = bars[bars.index.date == today] if not bars.empty else bars
                exit_price = float(today_b.iloc[-1]["close"]) if not today_b.empty else t["entry_price"]
                pnl_pts = (exit_price - t["entry_price"]) if t["dir"] == "long" else (t["entry_price"] - exit_price)
                pnl_usd = pnl_pts * POINT_VALUE - COMMISSION
                log_trade({"date": str(today), "direction": t["dir"],
                           "entry_time": t["entry_time"], "entry_price": round(t["entry_price"], 2),
                           "sl_price": round(t["sl"], 2), "tp_price": round(t["tp"], 2),
                           "exit_time": str(now), "exit_price": round(exit_price, 2),
                           "exit_reason": "session_close", "pnl_points": round(pnl_pts, 2),
                           "pnl_dollars": round(pnl_usd, 2), "zone_high": t["zone_high"],
                           "zone_low": t["zone_low"], "zone_mid": t["zone_mid"],
                           "sl_source": t["sl_source"], "macro_bias": str(macro_bias)})
                sign = "+" if pnl_usd >= 0 else ""
                tg(f"<b>SESSION CLOSE - {today}</b>\n"
                   f"Dir: {t['dir'].upper()} | Reason: SESSION_CLOSE\n"
                   f"Entry: {t['entry_price']:.2f} → Exit: {exit_price:.2f}\n"
                   f"PnL: {sign}${pnl_usd:.2f}")
            log.info("End of session. Exiting.")
            break

        # Wait until 8:20 ET before doing anything
        if h < 8 or (h == 8 and m < 20):
            time.sleep(60)
            continue

        bars = get_bars()
        if bars.empty:
            log.warning("No bar data — market holiday or data unavailable")
            time.sleep(300)
            continue

        today_bars = bars[[d == today for d in bars.index.date]].copy()
        if today_bars.empty:
            # Bug 4 fix: after 9:30 AM ET with no bars → market holiday / early close
            if h > 9 or (h == 9 and m >= 30):
                log.info("No market data after 9:30 AM ET — treating as market holiday.")
                tg(f"<b>ORB - {today}</b>\nNo bars after 9:30 AM ET. Market holiday — no trade today.")
                break
            time.sleep(120)
            continue

        # ── awaiting_zone: need 8:00 AM bar ──────────────────────────────
        if state["phase"] == "awaiting_zone":
            zone_b = today_bars[[ts.hour == 8 and ts.minute == 0
                                  for ts in today_bars.index]]
            if zone_b.empty:
                time.sleep(60)
                continue
            zb = zone_b.iloc[0]
            zh, zl = float(zb["high"]), float(zb["low"])
            zm = (zh + zl) / 2.0
            if zh - zl < 0.25:
                log.info("Zone range < 0.25 pts — skip day")
                tg(f"<b>ORB - {today}</b>\nZone too narrow ({zh-zl:.1f} pts). No trade today.")
                state["phase"] = "done"
                save_state(state)
                time.sleep(300)
                continue
            state["zone"] = {"high": zh, "low": zl, "mid": zm}
            state["phase"] = "awaiting_bias"
            save_state(state)
            log.info(f"Zone locked: H={zh:.2f} L={zl:.2f} M={zm:.2f} Range={zh-zl:.1f}pts")

        # ── awaiting_bias: watch for close above/below zone ───────────────
        if state["phase"] == "awaiting_bias":
            zone  = state["zone"]
            post  = today_bars[[ts.hour > 8 or (ts.hour == 8 and ts.minute >= 30)
                                 for ts in today_bars.index]]
            orb_dir = None
            bias_ts = None
            for ts, row in post.iterrows():
                if ts.hour >= 11:
                    break
                if float(row["close"]) > zone["high"]:
                    orb_dir = "long"; bias_ts = ts; break
                if float(row["close"]) < zone["low"]:
                    orb_dir = "short"; bias_ts = ts; break

            if orb_dir is None:
                if h >= 11:
                    log.info("No ORB bias by 11:00 ET — no trade")
                    tg(f"<b>ORB - {today}</b>\nNo directional bias by 11:00 AM ET. No trade.")
                    state["phase"] = "done"
                    save_state(state)
                time.sleep(120)
                continue

            # News filter
            if macro_bias is not None and macro_bias != orb_dir:
                log.info(f"SKIP: ORB={orb_dir} conflicts with macro={bias_label(macro_bias)}")
                tg(f"<b>TRADE SKIPPED - {today}</b>\n"
                   f"ORB signal: {orb_dir.upper()}\n"
                   f"Macro bias: {bias_label(macro_bias)}\n"
                   f"Conflict → trade skipped per news filter.")
                state["phase"] = "done"
                save_state(state)
                time.sleep(300)
                continue

            # Find limit fill at zone mid — scan only from bias bar onward (Bug 1 fix)
            zm = zone["mid"]
            entry_price, entry_time = None, None
            for ts, row in post.loc[bias_ts:].iterrows():
                if ts.hour >= 11:
                    break
                if orb_dir == "long"  and float(row["low"])  <= zm:
                    entry_price, entry_time = zm, str(ts)
                    break
                if orb_dir == "short" and float(row["high"]) >= zm:
                    entry_price, entry_time = zm, str(ts)
                    break

            if entry_price is None:
                if h >= 11:
                    log.info(f"Limit at {zm:.2f} never filled — no trade")
                    tg(f"<b>ORB - {today}</b>\nLimit at {zm:.2f} not filled by 11 AM. No trade.")
                    state["phase"] = "done"
                    save_state(state)
                time.sleep(120)
                continue

            # Compute SL/TP
            levels   = find_session_levels(bars, today)
            sl_price, sl_src = pick_sl(entry_price, orb_dir, levels)
            sl_dist  = abs(entry_price - sl_price)
            tp_price = (entry_price + sl_dist * RR) if orb_dir == "long" else (entry_price - sl_dist * RR)

            state["trade"] = {
                "dir": orb_dir, "entry_price": entry_price, "entry_time": entry_time,
                "sl": sl_price, "tp": tp_price, "sl_dist": sl_dist,
                "current_sl": sl_price, "trail_best": entry_price,
                "sl_source": sl_src, "be_triggered": False, "closed": False,
                "zone_high": zone["high"], "zone_low": zone["low"], "zone_mid": zm,
            }
            state["phase"] = "in_trade"
            save_state(state)

            risk_usd   = sl_dist * POINT_VALUE
            target_usd = sl_dist * RR * POINT_VALUE
            tg(f"<b>TRADE ENTERED - {today}</b>\n"
               f"Dir: {orb_dir.upper()} | Entry: {entry_price:.2f}\n"
               f"SL: {sl_price:.2f} ({sl_src}) | TP: {tp_price:.2f}\n"
               f"Risk: ${risk_usd:.0f} | Target: ${target_usd:.0f}\n"
               f"Macro: {bias_label(macro_bias)}")
            log.info(f"Trade entered: {orb_dir} @ {entry_price:.2f} SL={sl_price:.2f} TP={tp_price:.2f}")

        # ── in_trade: manage trailing stop, check SL/TP ───────────────────
        if state["phase"] == "in_trade":
            t = state["trade"]
            if t and not t.get("closed"):
                current_sl   = t["current_sl"]
                trail_best   = t["trail_best"]
                be_triggered = t["be_triggered"]
                sl_dist      = t["sl_dist"]
                entry_price  = t["entry_price"]
                orb_dir      = t["dir"]
                tp_price     = t["tp"]

                # Parse entry_time
                try:
                    entry_dt = datetime.fromisoformat(str(t["entry_time"]).replace(" ", "T")[:19])
                except Exception:
                    entry_dt = datetime(today.year, today.month, today.day, 8, 30)

                mgmt = today_bars[today_bars.index > entry_dt]
                exit_price, exit_reason, exit_ts = None, None, None

                for ts, row in mgmt.iterrows():
                    if ts.hour >= 16:
                        break
                    lo, hi = float(row["low"]), float(row["high"])

                    if orb_dir == "long":
                        if lo <= current_sl:
                            exit_price  = current_sl
                            exit_reason = "trail_sl" if be_triggered else "sl"
                            exit_ts = ts; break
                        if hi >= tp_price:
                            exit_price  = tp_price
                            exit_reason = "tp"
                            exit_ts = ts; break
                        # Trail
                        if hi > trail_best:
                            trail_best = hi
                        new_sl = trail_best - sl_dist
                        if new_sl > current_sl:
                            current_sl = new_sl
                            be_triggered = True
                    else:
                        if hi >= current_sl:
                            exit_price  = current_sl
                            exit_reason = "trail_sl" if be_triggered else "sl"
                            exit_ts = ts; break
                        if lo <= tp_price:
                            exit_price  = tp_price
                            exit_reason = "tp"
                            exit_ts = ts; break
                        # Trail
                        if lo < trail_best:
                            trail_best = lo
                        new_sl = trail_best + sl_dist
                        if new_sl < current_sl:
                            current_sl = new_sl
                            be_triggered = True

                # Save updated trail state
                state["trade"]["current_sl"]   = current_sl
                state["trade"]["trail_best"]   = trail_best
                state["trade"]["be_triggered"] = be_triggered
                save_state(state)

                if exit_price is not None:
                    pnl_pts = ((exit_price - entry_price) if orb_dir == "long"
                               else (entry_price - exit_price))
                    pnl_usd = pnl_pts * POINT_VALUE - COMMISSION
                    log_trade({
                        "date": str(today), "direction": orb_dir,
                        "entry_time": t["entry_time"], "entry_price": round(entry_price, 2),
                        "sl_price": round(t["sl"], 2), "tp_price": round(tp_price, 2),
                        "exit_time": str(exit_ts), "exit_price": round(exit_price, 2),
                        "exit_reason": exit_reason, "pnl_points": round(pnl_pts, 2),
                        "pnl_dollars": round(pnl_usd, 2), "zone_high": t["zone_high"],
                        "zone_low": t["zone_low"], "zone_mid": t["zone_mid"],
                        "sl_source": t["sl_source"], "macro_bias": str(macro_bias)
                    })
                    sign   = "+" if pnl_usd >= 0 else ""
                    result = "PROFIT" if pnl_usd >= 0 else "LOSS"
                    tg(f"<b>{result} - {today}</b>\n"
                       f"Dir: {orb_dir.upper()} | Exit: {exit_reason.upper()}\n"
                       f"Entry: {entry_price:.2f} → Exit: {exit_price:.2f}\n"
                       f"PnL: {sign}${pnl_usd:.2f} ({round(pnl_pts, 1)} pts)")
                    log.info(f"Exit: {exit_reason} {sign}${pnl_usd:.2f}")
                    state["trade"]["closed"] = True
                    state["phase"] = "done"
                    save_state(state)

        if state["phase"] == "done":
            time.sleep(300)
            continue

        time.sleep(120)  # check every 2 minutes


if __name__ == "__main__":
    run()
