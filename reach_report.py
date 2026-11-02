#!/usr/bin/env python3
"""
reach_report.py — weekly reach-efficiency snapshots from Meta into PostHog.

Replaces the "North Rolling Reach Analysis.xlsx" spreadsheet.

WHY THIS EXISTS
---------------
PostHog already syncs Meta Ads into the warehouse (`metaads.campaign_stats`), but
that table stores reach at DAILY grain and **reach does not sum**. For OH Sep 1-7 the
daily rows total 12,458 against an actual deduplicated 7,846 — a 59% overstatement.
Cumulative "total reach" dedupes across many months and is worse. Since net-new reach,
% net-new reach and cost-per-1k-net-new-reach all derive from deduplicated figures,
none of them can be computed from the warehouse.

So: ask the Meta API for deduplicated reach over each exact window, compute the
derived metrics here, and capture one event per campaign-week into PostHog.

USAGE
-----
    python3 reach_report.py                 # dry run, last complete week only
    python3 reach_report.py --backfill      # dry run, every week since launch
    python3 reach_report.py --validate      # check computed series vs. the old sheet
    python3 reach_report.py --send          # capture the last complete week
    python3 reach_report.py --backfill --send

Idempotent: queries PostHog for weeks already captured and skips them, so re-running
is safe and a missed week self-heals on the next run.

Weeks are Sunday->Saturday and only COMPLETE weeks are ever sent.
"""

import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.parse
import urllib.request

ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
GRAPH = "https://graph.facebook.com/v21.0"
PH_BASE = "https://us.posthog.com"
PH_PROJECT = 133367
EVENT = "reach_weekly_snapshot"

# campaign_key -> config. `start` must be a SUNDAY on/before first delivery.
CAMPAIGNS = {
    "OH": {
        "campaign_id": "120238234751950711",
        "campaign_name": "LEAD:ACB:OH:BROAD:PRO:PW:IS",
        "label": "Phasewell OH",
        "start": dt.date(2026, 2, 8),
    },
    "CA": {
        "campaign_id": "120246075295400711",
        "campaign_name": "LEAD:ACB:CA:ZIP:WATERMELON:FT:IS",
        "label": "FOMAT CA",
        "start": dt.date(2026, 7, 19),
    },
    "NY": {
        "campaign_id": "120247629695770711",
        "campaign_name": "LEAD:ACB:NY:BROAD:PRO:RP:IS",
        "label": "Roswell NY",
        "start": dt.date(2026, 10, 11),
    },
}

# Week -> (weekly_reach, total_reach, spend) as recorded in the retired spreadsheet.
# Used only by --validate to prove the Meta-derived series reproduces the old report.
SHEET_OH = {
    1: (5319, 5319, 776.97),    2: (2053, 6285, 221.62),    3: (3385, 7992, 348.47),
    4: (3931, 9804, 348.98),    5: (4377, 11574, 349.15),   6: (3711, 13040, 349.10),
    7: (3247, 13909, 348.44),   8: (3697, 15346, 348.99),   9: (4431, 17305, 346.76),
    10: (2856, 18224, 348.56),  11: (3231, 19483, 347.46),  12: (3421, 20432, 349.34),
    13: (3440, 21643, 320.00),  14: (3896, 22580, 348.85),  15: (3627, 23514, 349.32),
    16: (4570, 24642, 349.11),  17: (4303, 25943, 349.78),  18: (5257, 27853, 349.12),
    19: (3752, 28987, 347.21),  20: (3421, 29871, 352.68),  21: (5132, 31597, 417.73),
    22: (5775, 33321, 419.67),  23: (6010, 34578, 418.99),  24: (5530, 36207, 418.76),
    25: (6147, 37971, 479.72),  26: (6688, 39992, 536.83),  27: (7716, 42732, 608.79),
    28: (7433, 44814, 668.70),  29: (9029, 47937, 809.39),  30: (8772, 50243, 763.72),
}


# ---------------------------------------------------------------------------
# credentials
# ---------------------------------------------------------------------------
def load_env():
    # Cloud routine: no .env file, credentials come from environment variables.
    if not os.path.exists(ENV_PATH):
        return dict(os.environ)
    env = {}
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"')
    return env


def posthog_ingest_token(personal_key):
    """The capture endpoint needs the project token (phc_...), not the personal key."""
    req = urllib.request.Request(
        f"{PH_BASE}/api/projects/{PH_PROJECT}/",
        headers={"Authorization": "Bearer " + personal_key},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)["api_token"]


# ---------------------------------------------------------------------------
# Meta
# ---------------------------------------------------------------------------
def meta_insights(campaign_id, token, since, until, tries=4):
    """Deduplicated metrics for one exact window. Reach is deduped by Meta server-side."""
    params = urllib.parse.urlencode({
        "fields": "spend,reach,impressions,clicks,frequency",
        "time_range": json.dumps({"since": since, "until": until}),
        "access_token": token,
    })
    url = f"{GRAPH}/{campaign_id}/insights?{params}"
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=90) as r:
                data = json.load(r)
            if "error" in data:
                raise RuntimeError(data["error"].get("message", "meta error"))
            rows = data.get("data", [])
            if not rows:
                return dict(spend=0.0, reach=0, impressions=0, clicks=0, frequency=0.0)
            d = rows[0]
            return dict(
                spend=float(d.get("spend", 0)),
                reach=int(d.get("reach", 0)),
                impressions=int(d.get("impressions", 0)),
                clicks=int(d.get("clicks", 0)),
                frequency=float(d.get("frequency", 0) or 0),
            )
        except Exception as e:
            if attempt == tries - 1:
                raise
            time.sleep(2 * (attempt + 1))


def complete_weeks(start, today=None):
    """[(week_number, sunday, saturday)] for every COMPLETE Sun-Sat week since start."""
    today = today or dt.date.today()
    out, n, wk_start = [], 0, start
    while True:
        wk_end = wk_start + dt.timedelta(days=6)
        if wk_end >= today:          # only complete weeks; today's week is excluded
            break
        n += 1
        out.append((n, wk_start, wk_end))
        wk_start = wk_end + dt.timedelta(days=1)
    return out


def build_series(key, meta_token, verbose=True):
    """Weekly + cumulative deduped reach, plus every derived metric, for one campaign."""
    cfg = CAMPAIGNS[key]
    weeks = complete_weeks(cfg["start"])
    rows, prev_total, cum_spend, prev_nnr, prev_cp1k = [], 0, 0.0, None, None
    for n, ws, we in weeks:
        if verbose:
            print(f"  [{key}] week {n:>2} {ws}..{we}", end="\r", flush=True)
        wk = meta_insights(cfg["campaign_id"], meta_token, ws.isoformat(), we.isoformat())
        cum = meta_insights(cfg["campaign_id"], meta_token,
                            cfg["start"].isoformat(), we.isoformat())
        total_reach = cum["reach"]
        nnr = total_reach - prev_total
        cum_spend += wk["spend"]
        r = {
            "campaign_key": key,
            "campaign_label": cfg["label"],
            "campaign_name": cfg["campaign_name"],
            "campaign_id": cfg["campaign_id"],
            "week_number": n,
            "week_start": ws.isoformat(),
            "week_end": we.isoformat(),
            "weekly_reach": wk["reach"],
            "total_reach": total_reach,
            "net_new_reach": nnr,
            "spend": round(wk["spend"], 2),
            "cumulative_spend": round(cum_spend, 2),
            "impressions": wk["impressions"],
            "clicks": wk["clicks"],
            "frequency": round(wk["frequency"], 4),
            "pct_net_new_reach": round(nnr / wk["reach"], 6) if wk["reach"] else None,
            "cost_per_1k_net_new_reach": round(wk["spend"] / nnr * 1000, 4) if nnr else None,
            "cost_per_1k_reach": round(wk["spend"] / wk["reach"] * 1000, 4) if wk["reach"] else None,
            "net_new_reach_wow_pct": round((nnr - prev_nnr) / prev_nnr, 6) if prev_nnr else None,
        }
        cp1k = r["cost_per_1k_net_new_reach"]
        r["cost_per_1k_nnr_wow_pct"] = (
            round((cp1k - prev_cp1k) / prev_cp1k, 6) if (cp1k and prev_cp1k) else None
        )
        rows.append(r)
        prev_total, prev_nnr, prev_cp1k = total_reach, nnr, cp1k
    if verbose:
        print(" " * 60, end="\r")
    return rows


# ---------------------------------------------------------------------------
# PostHog
# ---------------------------------------------------------------------------
def already_captured(personal_key):
    """{(campaign_key, week_number)} already in PostHog, so re-runs are no-ops."""
    q = (f"SELECT DISTINCT properties.campaign_key, properties.week_number "
         f"FROM events WHERE event = '{EVENT}' "
         f"AND timestamp >= toDateTime('2026-01-01 00:00:00')")
    req = urllib.request.Request(
        f"{PH_BASE}/api/projects/{PH_PROJECT}/query/",
        data=json.dumps({"query": {"kind": "HogQLQuery", "query": q}}).encode(),
        headers={"Authorization": "Bearer " + personal_key,
                 "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            res = json.load(r)
        return {(row[0], int(row[1])) for row in res.get("results", []) if row[0] is not None}
    except Exception as e:
        print(f"  ! could not read existing snapshots ({e}); assuming none")
        return set()


def capture(rows, ingest_token):
    """One event per campaign-week, timestamped at week end. No person profiles."""
    batch = []
    for r in rows:
        props = dict(r)
        props["$process_person_profile"] = False   # metric data, not a user
        did = f"reach-report-{r['campaign_key']}"
        batch.append({
            "event": EVENT,
            "distinct_id": did,
            "properties": {**props, "distinct_id": did},
            "timestamp": f"{r['week_end']}T23:59:00",
        })
    payload = {"api_key": ingest_token, "batch": batch}
    req = urllib.request.Request(
        f"{PH_BASE}/batch/",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=90) as r:
        return r.status, r.read().decode()[:200]


# ---------------------------------------------------------------------------
# output
# ---------------------------------------------------------------------------
def print_table(rows):
    print(f"  {'wk':>3} {'start':10} {'end':10} {'wkReach':>8} {'total':>8} "
          f"{'NNR':>7} {'spend':>9} {'%NNR':>7} {'$/1kNNR':>9} {'$/1kReach':>9}")
    for r in rows:
        pct = f"{r['pct_net_new_reach']*100:6.2f}%" if r["pct_net_new_reach"] is not None else "     - "
        c1 = f"${r['cost_per_1k_net_new_reach']:8.2f}" if r["cost_per_1k_net_new_reach"] else "        -"
        c2 = f"${r['cost_per_1k_reach']:8.2f}" if r["cost_per_1k_reach"] else "        -"
        print(f"  {r['week_number']:>3} {r['week_start']} {r['week_end']} "
              f"{r['weekly_reach']:>8,} {r['total_reach']:>8,} {r['net_new_reach']:>7,} "
              f"${r['spend']:>8,.2f} {pct} {c1} {c2}")


def validate(rows):
    print("\nVALIDATION vs. retired spreadsheet (OH):")
    bad = 0
    for r in rows:
        exp = SHEET_OH.get(r["week_number"])
        if not exp:
            print(f"  wk {r['week_number']:>2}  (no sheet row to compare)")
            continue
        ok = (r["weekly_reach"] == exp[0] and r["total_reach"] == exp[1]
              and abs(r["spend"] - exp[2]) < 0.01)
        if not ok:
            bad += 1
            print(f"  wk {r['week_number']:>2} MISMATCH  computed "
                  f"{r['weekly_reach']}/{r['total_reach']}/{r['spend']}  "
                  f"sheet {exp[0]}/{exp[1]}/{exp[2]}")
    print(f"  {len(rows)-bad}/{len(rows)} weeks match exactly."
          + ("  ALL GOOD." if bad == 0 else f"  {bad} MISMATCHED."))
    return bad == 0


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--backfill", action="store_true", help="every week since launch")
    ap.add_argument("--send", action="store_true", help="actually capture into PostHog")
    ap.add_argument("--validate", action="store_true", help="compare OH against the old sheet")
    ap.add_argument("--campaign", choices=list(CAMPAIGNS), help="limit to one campaign")
    args = ap.parse_args()

    env = load_env()
    meta_token = env["FACEBOOK_ACCESS_TOKEN"]
    personal_key = env["POSTHOG_API_KEY"]

    keys = [args.campaign] if args.campaign else list(CAMPAIGNS)
    existing = already_captured(personal_key) if args.send else set()

    to_send = []
    for key in keys:
        print(f"\n=== {key} ({CAMPAIGNS[key]['label']}) ===")
        rows = build_series(key, meta_token)
        if not args.backfill:
            rows = rows[-1:]
        print_table(rows)
        if args.validate and key == "OH":
            validate(build_series(key, meta_token) if not args.backfill else rows)
        new = [r for r in rows if (r["campaign_key"], r["week_number"]) not in existing]
        skipped = len(rows) - len(new)
        if args.send and skipped:
            print(f"  ({skipped} week(s) already in PostHog, skipping)")
        to_send.extend(new)

    if not args.send:
        print(f"\nDRY RUN. {len(to_send)} event(s) would be captured as '{EVENT}'."
              f"\nRe-run with --send to write them.")
        return 0

    if not to_send:
        print("\nNothing new to capture. PostHog is already up to date.")
        return 0

    status, body = capture(to_send, posthog_ingest_token(personal_key))
    print(f"\nCaptured {len(to_send)} event(s) -> HTTP {status} {body}")
    return 0 if status < 300 else 1


if __name__ == "__main__":
    sys.exit(main())
