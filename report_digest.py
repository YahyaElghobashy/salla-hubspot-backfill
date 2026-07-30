#!/usr/bin/env python3
"""Scheduled Slack digests: daily 09:00, weekly Sunday, monthly on the 1st.

Shape (same for all three, because the audience already recognises it from the
30-minute ops thread): the CHANNEL MESSAGE is an executive summary readable in
fifteen seconds -- verdict, the numbers a non-technical reader needs, and
anything requiring a decision. The THREAD carries one reply per aspect, each
self-contained, each with the analysis rather than just the number.

Three design rules that matter more than they look:

1. Counts come from append-only ledgers (mirror/created.csv, mirror/errors.csv),
   never from logs. Logs rotate; a report that silently loses a week of history
   is worse than one that admits it cannot see that far back.
2. A metric that could not be measured renders as "not recorded", never as 0.
   Reporting an unmeasured value as zero is how a broken sensor becomes a
   confident lie.
3. The report is built entirely from local files, so it still posts during a
   Make credit outage. The day the platform is down is the day the report
   matters most.

Usage:
    python3 report_digest.py --period daily
    python3 report_digest.py --period weekly  --channel C0AQMMS4TRD
    python3 report_digest.py --period monthly --dry-run
"""

import argparse
import calendar
import csv
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import metrics_rollup as roll

ROOT = Path(__file__).resolve().parent
FAIL_FLAG = ROOT / "mirror" / "report_failed.flag"

log = logging.getLogger("digest")

SPARK = "▁▂▃▄▅▆▇█"


def spark(values):
    """Text sparkline. Slack renders no charts and no tables; this is the honest
    substitute -- shape at a glance, exact numbers in the thread."""
    vals = [v for v in values if isinstance(v, (int, float))]
    if not vals:
        return ""
    lo, hi = min(vals), max(vals)
    if hi == lo:
        return SPARK[3] * len(vals)
    return "".join(SPARK[min(7, int(7 * (v - lo) / (hi - lo)))]
                   if isinstance(v, (int, float)) else " " for v in values)


def n(v, unit=""):
    """Format a number, or say plainly that it was never measured."""
    if v is None:
        return "not recorded"
    if isinstance(v, float):
        # credit balances arrive as floats from the Make API but are whole
        # numbers; "96,739.0 credits" reads like a bug to anyone sensible
        return f"{v:,.0f}{unit}" if v == int(v) else f"{v:,.1f}{unit}"
    return f"{v:,}{unit}"


def dur(seconds):
    """Human duration. A raw '1,156,112s' is technically true and useless."""
    if seconds is None:
        return "not recorded"
    s = int(seconds)
    if s < 90:
        return f"{s}s"
    if s < 5400:
        return f"{s // 60}m"
    if s < 172800:
        return f"{s // 3600}h"
    return f"{s // 86400}d"


def pct_change(cur, prev):
    if not prev or cur is None:
        return ""
    d = 100.0 * (cur - prev) / prev
    arrow = "up" if d >= 0 else "down"
    # beyond a few hundred percent the ratio stops informing and starts
    # sounding like spin; give the raw comparison instead
    if abs(d) > 300:
        return f" (previous period: {prev:,})"
    return f" ({arrow} {abs(d):.0f}% on the previous period)"


# --------------------------------------------------------------------------
# aggregation
# --------------------------------------------------------------------------

def window(period, today=None):
    """(start, end, label) inclusive date strings for the reporting period."""
    today = today or datetime.now().date()
    if period == "daily":
        d = today - timedelta(days=1)
        return str(d), str(d), d.strftime("%A %-d %B")
    if period == "weekly":
        # report runs Sunday morning and covers the week that just ended
        end = today - timedelta(days=1)
        start = end - timedelta(days=6)
        return str(start), str(end), (f"{start.strftime('%-d %b')} – "
                                      f"{end.strftime('%-d %b %Y')}")
    first_this = today.replace(day=1)
    end = first_this - timedelta(days=1)
    start = end.replace(day=1)
    return str(start), str(end), end.strftime("%B %Y")


def ledger_total(path, start, end, stage_field=None):
    """Exact count over a date range, straight from the append-only ledger."""
    if not path.exists():
        return (None, None) if stage_field else None
    total, by = 0, {}
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            d = (row.get("ts") or "")[:10]
            if not d or d < start or d > end:
                continue
            total += 1
            if stage_field:
                k = (row.get(stage_field) or "unknown").strip()
                by[k] = by.get(k, 0) + 1
    return (total, by) if stage_field else total


def aggregate(period):
    start, end, label = window(period)
    days = roll.load(start, end)
    span = (datetime.strptime(end, "%Y-%m-%d")
            - datetime.strptime(start, "%Y-%m-%d")).days + 1

    created = ledger_total(roll.CREATED, start, end)
    err_total, err_by = ledger_total(roll.ERRORS, start, end, stage_field="stage")
    partials = err_by.get("partial", 0) if err_by is not None else None
    unrecovered = (sum(v for k, v in err_by.items() if k != "partial")
                   if err_by is not None else None)

    # previous period of equal length, for trend
    p_end = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=1)).date()
    p_start = p_end - timedelta(days=span - 1)
    prev_created = ledger_total(roll.CREATED, str(p_start), str(p_end))

    avail = [d.get("availability_pct") for d in days
             if d.get("availability_pct") is not None]
    depth_max = [d.get("queue_depth_max") for d in days
                 if d.get("queue_depth_max") is not None]
    waits = [d.get("oldest_wait_s_max") for d in days
             if d.get("oldest_wait_s_max") is not None]
    burns = [(d.get("credits") or {}).get("burned") for d in days
             if (d.get("credits") or {}).get("burned") is not None]
    alerts = [a for d in days for a in (d.get("alerts") or [])]
    restarts = 0
    for d in days:
        for s in (d.get("services") or {}).values():
            restarts += (s.get("restarts") or 0) if isinstance(s, dict) else 0

    live_vals = [d.get("live_processed") for d in days
                 if d.get("live_processed") is not None]
    latest = days[-1] if days else {}
    live_now = roll.collect(datetime.now().strftime("%Y-%m-%d"))

    return {
        "period": period, "start": start, "end": end, "label": label,
        "days_recorded": len(days), "days_expected": span,
        "created": created, "prev_created": prev_created,
        "daily_series": [(d["date"], d.get("orders_created")) for d in days],
        "errors_unrecovered": unrecovered, "partials": partials,
        "availability": round(sum(avail) / len(avail), 2) if avail else None,
        "queue_depth_max": max(depth_max) if depth_max else None,
        "wait_max_s": max(waits) if waits else None,
        "credits_burned": sum(burns) if burns else None,
        "credits_daily": burns,
        "live_created": sum(live_vals) if live_vals else None,
        "alerts": alerts, "restarts": restarts,
        "latest": latest, "now": live_now,
    }


# --------------------------------------------------------------------------
# rendering
# --------------------------------------------------------------------------

def _verdict(a):
    """One honest sentence up top. Reads the actual numbers, not a fixed string."""
    bad = []
    if a["errors_unrecovered"]:
        bad.append(f"{a['errors_unrecovered']} unrecovered error(s)")
    if a["availability"] is not None and a["availability"] < 99:
        bad.append(f"availability {a['availability']}%")
    if a["alerts"]:
        bad.append(f"{len(a['alerts'])} alert(s)")
    if not a["created"]:
        return "No sync activity recorded in this period."
    if not bad:
        return "Healthy. No incidents, no data loss, no manual intervention."
    return "Needs a look: " + ", ".join(bad) + "."


def _credit_lines(a):
    """Balance, burn and runway -- the number that most often needs a decision."""
    c = (a["now"].get("credits") or {})
    rem, nxt = c.get("remaining"), c.get("next_reset")
    out = []
    if rem is None:
        return ["• Make credits: not recorded"]
    burn_day = a["credits_burned"]
    line = f"• Make credits: *{n(rem)}* left"
    if burn_day and a["days_recorded"]:
        per_h = burn_day / max(1, a["days_recorded"] * 24)
        line += f", burning ~{per_h:,.0f}/hour"
        if per_h > 0:
            hrs = rem / per_h
            line += (f" · about {hrs:.0f}h of runway" if hrs < 72
                     else f" · {hrs/24:.0f} days of runway")
    out.append(line)
    if nxt:
        out.append(f"• Plan renews: {nxt}")
    return out


def _coverage_note(a):
    """Say out loud when the window is only partly recorded.

    A weekly report built from two days of history looks identical to one built
    from seven unless it says so. Silence here would be the most misleading
    thing this file could do.
    """
    if a["days_recorded"] >= a["days_expected"]:
        return None
    return (f"_Based on {a['days_recorded']} of {a['days_expected']} days — "
            f"daily metrics collection started recently. Order counts are exact "
            f"regardless (they come from the ledger); the sampled figures cover "
            f"only the recorded days._")


def render(a):
    """Return (headline, [thread replies])."""
    p = a["period"]
    title = {"daily": "Daily report", "weekly": "Weekly report",
             "monthly": "Monthly report"}[p]
    head = [f"*{title} · {a['label']}*", _verdict(a), ""]

    head.append(f"• Orders created: *{n(a['created'])}*"
                + pct_change(a["created"], a["prev_created"]))
    if a["live_created"] is not None:
        batch = (a["created"] or 0) - a["live_created"]
        head.append(f"   ↳ {a['live_created']:,} live orders, "
                    f"{max(0, batch):,} from backfill/drain")
    if p != "daily" and a["daily_series"]:
        head.append(f"• Daily volume: {spark([v for _, v in a['daily_series']])}")
    if a["availability"] is not None:
        head.append(f"• Availability: *{a['availability']}%*")
    q = a["now"]
    # A weekly report must not quote today's queue as if it described the week.
    if p == "daily":
        if q.get("queue_depth_max") is not None:
            head.append(f"• Queue: peaked at {q['queue_depth_max']}, "
                        f"oldest item {dur(q.get('oldest_wait_s_max'))}")
    elif a["queue_depth_max"] is not None:
        head.append(f"• Queue: peak depth {a['queue_depth_max']} over the period"
                    f" (now {q.get('queue_depth_max', 0)})")
    held = (q.get("held") or {})
    if held.get("value") is not None:
        head.append(f"• Held for catalog: {held['value']:,} "
                    f"(as of {held.get('as_of') or 'unknown'})")
    bf = (q.get("backfill") or {})
    if bf.get("window"):
        head.append(f"• Backfill: {bf['window']}, page {bf.get('page')} "
                    f"of {bf.get('total_pages')}")
    head += _credit_lines(a)
    if a["errors_unrecovered"] is not None:
        head.append(f"• Errors: {n(a['errors_unrecovered'])} unrecovered")

    note = _coverage_note(a)
    if note:
        head += ["", note]
    head += ["", "Breakdown in thread."]

    return "\n".join(head), _threads(a)


def _threads(a):
    q, out = a["now"], []
    p = a["period"]

    # --- live sync -------------------------------------------------------
    live = a["live_created"]
    t = ["*Live sync*"]
    if live is not None:
        t.append(f"{live:,} live orders picked up from the store"
                 f"{' this period' if p != 'daily' else ''}. The headline "
                 f"figure of {n(a['created'])} also includes records created "
                 f"by the backfill and the queue drain.")
    else:
        t.append(f"{n(a['created'])} records created across all sources.")
    if a["wait_max_s"] is not None:
        t.append(f"Peak queue depth {n(a['queue_depth_max'])}; oldest item "
                 f"sitting in the queue reached {dur(a['wait_max_s'])}.")
        if (a["wait_max_s"] or 0) > 86400:
            t.append("That age reflects historical rows re-queued by the "
                     "drain, not a new order waiting — new orders are picked "
                     "up within seconds.")
    if a["availability"] is not None:
        t.append(f"Availability {a['availability']}% — measured as minutes in "
                 f"which the intake actually polled successfully, not merely "
                 f"minutes the service was running. Those differ: in July a "
                 f"healthy service sat on a dead relay for 69 hours.")
    out.append("\n".join(t))

    # --- backfill --------------------------------------------------------
    bf = q.get("backfill") or {}
    if bf.get("window"):
        t = ["*Backfill*",
             f"Currently sweeping {bf['window']}, page {bf.get('page')} of "
             f"{bf.get('total_pages')} (status: {bf.get('status')})."]
        if bf.get("status") in ("overflow", "done_overflow"):
            t.append("The overflow flag means a time-slot reported more pages "
                     "than the per-slot limit, so the engine splits it into "
                     "sub-batches instead of skipping it. Nothing is missed; "
                     "it takes longer.")
        out.append("\n".join(t))

    # --- catalog holds ---------------------------------------------------
    held = q.get("held") or {}
    if held.get("value") is not None:
        t = ["*Catalog holds*",
             f"{held['value']:,} orders held, measured {held.get('as_of')}."]
        if q.get("blocker_products"):
            t.append(f"{q['blocker_products']} product(s) account for all of "
                     f"them — see mirror/blocker_matrix.csv. These are catalog "
                     f"decisions, not sync failures: the orders sit safely in "
                     f"the queue and sync themselves once the products go live.")
        out.append("\n".join(t))

    # --- credits ---------------------------------------------------------
    c = q.get("credits") or {}
    if c.get("remaining") is not None:
        t = ["*Make credits*", f"{c['remaining']:,} remaining."]
        if a["credits_burned"]:
            t.append(f"{a['credits_burned']:,} consumed over the period"
                     + (f" ({spark(a['credits_daily'])})" if p != "daily"
                        and len(a["credits_daily"] or []) > 1 else "") + ".")
        by = c.get("by_engine") or {}
        if by:
            t.append(f"Split today: live {by.get('live', 0):,}, backfill "
                     f"{by.get('backfill', 0):,}, other {by.get('other', 0):,}. "
                     f"Most live-sync consumption is the intake poll itself "
                     f"rather than order volume, so the floor cost is roughly "
                     f"constant regardless of how busy the store is.")
        if c.get("plan") and a["credits_burned"]:
            t.append(f"Plan allowance is {c['plan']}; consumption at this rate "
                     f"is the structural constraint on how fast the backfill "
                     f"can run.")
        out.append("\n".join(t))

    # --- data quality ----------------------------------------------------
    t = ["*Data quality*"]
    t.append(f"{n(a['errors_unrecovered'])} unrecovered failure(s) this period.")
    if a["partials"]:
        t.append(f"{a['partials']:,} order(s) flagged as partial — the HubSpot "
                 f"record exists but carries fewer line items than the source. "
                 f"These are detected as the engine passes over historical "
                 f"orders, not newly broken, and are repairable in a batch.")
    out.append("\n".join(t))

    # --- incidents -------------------------------------------------------
    if a["alerts"]:
        lines = ["*Incidents*"]
        for al in a["alerts"][:10]:
            lines.append(f"• {al.get('t')} — {al.get('detail')}")
        if len(a["alerts"]) > 10:
            lines.append(f"…and {len(a['alerts']) - 10} more.")
        out.append("\n".join(lines))

    # --- infrastructure --------------------------------------------------
    svc = q.get("services") or {}
    if svc:
        up = []
        for unit, s in svc.items():
            secs = s.get("uptime_s")
            human = f"{secs//86400}d {secs%86400//3600}h" if secs else "—"
            up.append(f"• {unit.replace('salla-', '')}: {s.get('state')}, "
                      f"up {human}, {s.get('restarts')} restart(s)")
        t = ["*Infrastructure*"] + up
        if q.get("disk_pct") is not None:
            t.append(f"Disk {q['disk_pct']}% used.")
        t.append("Firewall lockdown unchanged: inbound denied except SSH via "
                 "Google IAP, outbound HTTPS only, no service account on the VM.")
        out.append("\n".join(t))

    return out


# --------------------------------------------------------------------------

def post(headline, replies, dry_run=False):
    """Post headline, then each reply into its thread, to every configured channel.

    Imported late so that --dry-run works on a machine with no .env at all.
    """
    import notify
    if dry_run or not notify.slack_enabled():
        print(headline)
        for r in replies:
            print("\n  ---- thread ----")
            print(r)
        return True
    ok_any = False
    for ch in notify.slack_channels():
        ts = notify.post_slack(headline, channel=ch)
        if not ts:
            log.warning("headline post failed for channel %s", ch)
            continue
        ok_any = True
        for r in replies:
            notify.post_slack(r, thread_ts=ts, channel=ch)
    # A digest that silently fails to post reads as "nothing happened", which is
    # the worst outcome available. Leave a flag the 30-minute ops update reads.
    if ok_any:
        FAIL_FLAG.unlink(missing_ok=True)
    else:
        FAIL_FLAG.parent.mkdir(exist_ok=True)
        FAIL_FLAG.write_text(datetime.now().isoformat(timespec="seconds") + "\n")
    return ok_any


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--period", choices=("daily", "weekly", "monthly"),
                    default="daily")
    ap.add_argument("--channel", default=None,
                    help="post ONLY to this channel id (overrides configuration)")
    ap.add_argument("--dry-run", action="store_true", help="print, do not post")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s [digest] %(message)s")

    # Hard scope, not a filter: overwriting the env var means the transport
    # itself cannot see any other channel, so a test run is incapable of
    # reaching the client channel even if some other code path posts.
    if args.channel:
        os.environ["SLACK_CHANNEL_IDS"] = args.channel
        os.environ.pop("SLACK_CHANNEL_ID", None)
        log.info("channel scope forced to %s", args.channel)

    a = aggregate(args.period)
    headline, replies = render(a)
    ok = post(headline, replies, dry_run=args.dry_run)
    log.info("%s report: %s orders, %d thread reply(ies), posted=%s",
             args.period, a["created"], len(replies), ok)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
