#!/usr/bin/env python3
"""
Daily Slack Digest — North Trials Campaign Performance

Lean alternative to generate_geo_report.py. Queries PostHog, computes geo
stats (including within-250mi), and posts the daily snapshot to Slack.
No HTML generation, no Claude insights, no matplotlib/folium.

Requirements:
    pip install requests pgeocode python-dotenv

Usage:
    python3 slack_digest.py --campaign OH
    python3 slack_digest.py --campaign CA --end-date 2026-03-27
    python3 slack_digest.py --campaign OH --slack-channel #north-test
"""

import argparse
import math
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pgeocode
import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# ── Campaign config ─────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

CAMPAIGNS = {
    "OH": {
        "title": "Ohio Campaign",
        "subtitle_campaign": "LEAD:ACB:OH:BROAD:PRO:PW:IS",
        "start_date": "2026-02-21",
        "event_name": "intake_success",
        "utm_source": "meta",
        "utm_campaign_contains": "OH",
        "zip_property": "$initial_geoip_postal_code",
        "ta_property": "ta",
        "posthog_project": 133367,
        "zip_country": "US",
        "home_dma": "Cleveland-Akron (Canton)",
        "home_lat": 40.8465,
        "home_lng": -81.4408,
        "outside_threshold_miles": 150,
        "dmas": [
            {"dma": "Detroit",                  "lat": 42.3314, "lng": -83.0458},
            {"dma": "Columbus, OH",             "lat": 39.9612, "lng": -82.9988},
            {"dma": "Pittsburgh",               "lat": 40.4406, "lng": -79.9959},
            {"dma": "Cleveland-Akron (Canton)", "lat": 41.0814, "lng": -81.5190},
            {"dma": "Johnstown-Altoona",        "lat": 40.5187, "lng": -78.3947},
            {"dma": "Cincinnati",               "lat": 39.1031, "lng": -84.5120},
            {"dma": "Buffalo",                  "lat": 42.8864, "lng": -78.8784},
            {"dma": "Rochester",                "lat": 43.1566, "lng": -77.6088},
            {"dma": "Charleston-Huntington",    "lat": 38.3498, "lng": -81.6326},
            {"dma": "Dayton",                   "lat": 39.7589, "lng": -84.1916},
            {"dma": "Lansing",                  "lat": 42.7325, "lng": -84.5555},
            {"dma": "Erie",                     "lat": 42.1292, "lng": -80.0851},
            {"dma": "Flint-Saginaw-Bay City",   "lat": 43.0125, "lng": -83.6875},
            {"dma": "Wheeling-Steubenville",    "lat": 40.3698, "lng": -80.6340},
            {"dma": "Clarksburg-Weston",        "lat": 39.2803, "lng": -80.3401},
            {"dma": "Toledo",                   "lat": 41.6528, "lng": -83.5379},
            {"dma": "Youngstown",               "lat": 41.0998, "lng": -80.6495},
            {"dma": "Lima",                     "lat": 40.7420, "lng": -84.1052},
            {"dma": "Zanesville",               "lat": 39.9400, "lng": -82.0130},
        ],
    },
    "CA": {
        "title": "California Campaign",
        "subtitle_campaign": "LEAD:ACB:CA:BROAD:PRO:FOMAT:IS",
        "start_date": "2026-03-03",
        "event_name": "intake_success",
        "utm_source": "meta",
        "utm_campaign_contains": "CA",
        "zip_property": "$initial_geoip_postal_code",
        "ta_property": "ta",
        "posthog_project": 133367,
        "zip_country": "US",
        "home_dma": "Los Angeles",
        "home_lat": 34.1975,
        "home_lng": -119.1771,
        "outside_threshold_miles": 150,
        "dmas": [
            {"dma": "Los Angeles",               "lat": 34.0522, "lng": -118.2437},
            {"dma": "San Diego",                 "lat": 32.7157, "lng": -117.1611},
            {"dma": "Bakersfield",               "lat": 35.3733, "lng": -119.0187},
            {"dma": "Santa Barbara-Santa Maria", "lat": 34.4208, "lng": -119.6982},
            {"dma": "Palm Springs",              "lat": 33.8303, "lng": -116.5453},
            {"dma": "Monterey-Salinas",          "lat": 36.6002, "lng": -121.8947},
            {"dma": "Fresno",                    "lat": 36.7378, "lng": -119.7871},
        ],
    },
}

CAMPAIGN = {}
TARGET_DMAS = []

# ---------------------------------------------------------------------------
# ── Setup ────────────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

# Load .env from CWD first (cloud), then script parent (local)
load_dotenv(Path(".env"))
load_dotenv(Path(__file__).parent.parent / ".env")
load_dotenv(Path(__file__).parent / ".env")

POSTHOG_API_KEY = os.getenv("POSTHOG_API_KEY")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL   = os.getenv("SLACK_CHANNEL", "#north-reports")
POSTHOG_BASE    = "https://us.posthog.com"

# ---------------------------------------------------------------------------
# ── PostHog helpers ──────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

def ph_query(sql: str, retries: int = 3) -> list:
    import time
    url = f"{POSTHOG_BASE}/api/projects/{CAMPAIGN['posthog_project']}/query/"
    for attempt in range(retries):
        resp = requests.post(
            url,
            headers={"Authorization": f"Bearer {POSTHOG_API_KEY}", "Content-Type": "application/json"},
            json={"query": {"kind": "HogQLQuery", "query": sql}},
            timeout=60,
        )
        if resp.status_code == 503 and attempt < retries - 1:
            wait = 5 * (attempt + 1)
            print(f"  PostHog 503 — retrying in {wait}s...", file=sys.stderr)
            time.sleep(wait)
            continue
        if not resp.ok:
            print(f"  PostHog error {resp.status_code}: {resp.text[:300]}", file=sys.stderr)
            resp.raise_for_status()
        data = resp.json()
        return [dict(zip(data["columns"], row)) for row in data["results"]]
    resp.raise_for_status()


def build_where(extra: str = "", event_name: str = None) -> str:
    c = CAMPAIGN
    clauses = [
        f"event = '{event_name or c['event_name']}'",
        f"timestamp >= '{c['start_date']}'",
        f"timestamp <= '{c['end_date']} 23:59:59'",
    ]
    if c.get("utm_source"):
        clauses.append(f"person.properties.$initial_utm_source = '{c['utm_source']}'")
    if c.get("utm_campaign_contains"):
        clauses.append(f"person.properties.$initial_utm_campaign ILIKE '%{c['utm_campaign_contains']}%'")
    if extra:
        clauses.append(extra)
    return "WHERE " + "\n  AND ".join(clauses)


def query_zip_counts() -> dict:
    prop = CAMPAIGN["zip_property"]
    sql = f"""
SELECT person.properties.{prop} AS zip, count(DISTINCT person_id) AS n
FROM events
{build_where(f"person.properties.{prop} IS NOT NULL AND person.properties.{prop} != ''")}
GROUP BY zip ORDER BY n DESC
"""
    rows = ph_query(sql)
    return {r["zip"]: int(r["n"]) for r in rows if r["zip"]}


def query_intakes_by_date() -> dict:
    sql = f"""
SELECT toDate(timestamp) AS day, count(DISTINCT person_id) AS n
FROM events
{build_where()}
GROUP BY day ORDER BY day ASC
"""
    rows = ph_query(sql)
    result = {}
    for r in rows:
        dt = r["day"]
        key = dt.isoformat() if hasattr(dt, "isoformat") else str(dt)
        result[key] = int(r["n"])
    return result


def query_visitors_by_date() -> dict:
    sql = f"""
SELECT toDate(timestamp) AS day, count(DISTINCT person_id) AS n
FROM events
{build_where(event_name="$pageview")}
GROUP BY day ORDER BY day ASC
"""
    rows = ph_query(sql)
    result = {}
    for r in rows:
        dt = r["day"]
        key = dt.isoformat() if hasattr(dt, "isoformat") else str(dt)
        result[key] = int(r["n"])
    return result


def query_ta_counts() -> list:
    prop = CAMPAIGN.get("ta_property")
    if not prop:
        return []
    sql = f"""
SELECT properties.{prop} AS ta, count(DISTINCT person_id) AS n
FROM events
{build_where(f"properties.{prop} IS NOT NULL AND properties.{prop} != ''")}
GROUP BY ta ORDER BY n DESC
"""
    rows = ph_query(sql)
    return [{"ta": r["ta"], "n": int(r["n"])} for r in rows if r["ta"]]

# ---------------------------------------------------------------------------
# ── Geo helpers ──────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

def haversine(lat1, lng1, lat2, lng2) -> float:
    R = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def compute_geo_stats(zip_counts: dict) -> tuple:
    """
    Returns (dma_counts, within_250) where:
      dma_counts: {dma_name: intake_count}
      within_250: int — intakes within 250 miles of home site
    """
    nomi = pgeocode.Nominatim(CAMPAIGN["zip_country"])
    threshold = CAMPAIGN["outside_threshold_miles"]
    home_lat  = CAMPAIGN["home_lat"]
    home_lng  = CAMPAIGN["home_lng"]

    dma_counts = {d["dma"]: 0 for d in TARGET_DMAS}
    within_250 = 0

    for zip_code, count in zip_counts.items():
        row = nomi.query_postal_code(str(zip_code).zfill(5))
        if row is None or (hasattr(row, "isna") and row["latitude"] != row["latitude"]):
            continue
        try:
            lat = float(row["latitude"])
            lng = float(row["longitude"])
        except (TypeError, ValueError):
            continue

        if haversine(lat, lng, home_lat, home_lng) <= 250:
            within_250 += count

        best_dma = None
        best_dist = float("inf")
        for d in TARGET_DMAS:
            dist = haversine(lat, lng, d["lat"], d["lng"])
            if dist < best_dist:
                best_dist = dist
                best_dma = d["dma"]

        if best_dma and best_dist <= threshold:
            dma_counts[best_dma] = dma_counts.get(best_dma, 0) + count

    return dma_counts, within_250

# ---------------------------------------------------------------------------
# ── Slack ────────────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

def post_to_slack(metrics: dict) -> None:
    token   = SLACK_BOT_TOKEN
    channel = metrics.get("slack_channel", SLACK_CHANNEL)
    if not token:
        print("WARNING: SLACK_BOT_TOKEN not set — skipping Slack post.", file=sys.stderr)
        return

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    cvr_data = metrics.get("cvr_data", [])
    end_date = metrics["end_date"]

    yesterday_intakes = cvr_data[-1]["intakes"] if cvr_data else 0

    active   = [d for d in cvr_data if d["visitors"] > 0]
    cur7     = active[-7:]   if len(active) >= 1 else active
    prev7    = active[-14:-7] if len(active) >= 8 else []
    cur_cvr  = (sum(d["intakes"] for d in cur7)  / sum(d["visitors"] for d in cur7)  * 100) if cur7  and sum(d["visitors"] for d in cur7)  else None
    prev_cvr = (sum(d["intakes"] for d in prev7) / sum(d["visitors"] for d in prev7) * 100) if prev7 and sum(d["visitors"] for d in prev7) else None

    if cur_cvr is not None and prev_cvr is not None:
        delta   = cur_cvr - prev_cvr
        cvr_str = f"{cur_cvr:.1f}%  {'↑' if delta >= 0 else '↓'} {abs(delta):.1f}pp vs prior 7d"
    elif cur_cvr is not None:
        cvr_str = f"{cur_cvr:.1f}%"
    else:
        cvr_str = "—"

    chart_days = cvr_data[-10:]
    if chart_days:
        max_v = max(d["intakes"] for d in chart_days) or 1
        lines  = []
        for d in chart_days:
            filled = round(d["intakes"] / max_v * 10)
            bar    = "█" * filled + "░" * (10 - filled)
            lines.append(f"{d['d']:<5}  {bar}  {d['intakes']}")
        mini_chart = "```\n" + "\n".join(lines) + "\n```"
    else:
        mini_chart = ""

    sorted_dmas = sorted(metrics.get("all_dmas", {}).items(), key=lambda x: -x[1])
    top3        = "  ·  ".join(f"{d} {n}" for d, n in sorted_dmas[:3] if n > 0)

    total       = metrics["total"]
    within_250  = metrics["within_250"]
    within_pct  = f"{100 * within_250 / total:.0f}%" if total else "—"
    overall_cvr = metrics.get("overall_cvr")
    overall_str = f"{overall_cvr * 100:.1f}%" if overall_cvr else "—"

    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{metrics['title']} — Daily Snapshot*\n_{metrics['campaign_id']}  ·  through {end_date}_"},
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Yesterday*\n{yesterday_intakes} intake{'s' if yesterday_intakes != 1 else ''}"},
                {"type": "mrkdwn", "text": f"*7d CVR*\n{cvr_str}"},
                {"type": "mrkdwn", "text": f"*L30 Intakes*\n{total}"},
                {"type": "mrkdwn", "text": f"*L30 CVR*\n{overall_str}"},
                {"type": "mrkdwn", "text": f"*Within 250 mi*\n{within_250} ({within_pct})"},
                {"type": "mrkdwn", "text": f"*Home Market*\n{metrics['home_dma']} — {metrics['home_n']}"},
            ],
        },
        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"*Top DMAs:* {top3}"}]},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Last 10 days — daily intakes*\n{mini_chart}"}},
    ]

    ta_data = metrics.get("ta_data", [])
    if ta_data:
        ta_total = sum(d["n"] for d in ta_data)
        ta_str   = "  ·  ".join(f"{d['ta']} {d['n']} ({100 * d['n'] // ta_total}%)" for d in ta_data[:6])
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"*TA:* {ta_str}"}]})

    print(f"  Posting to Slack {channel}...")
    r    = requests.post("https://slack.com/api/chat.postMessage",
                         headers=headers,
                         json={"channel": channel, "blocks": blocks, "unfurl_links": False})
    resp = r.json()
    if not resp.get("ok"):
        print(f"  WARNING: Slack post failed: {resp.get('error')}", file=sys.stderr)
        sys.exit(1)
    print("  Posted.")

# ---------------------------------------------------------------------------
# ── Main ─────────────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Post daily campaign digest to Slack")
    parser.add_argument("--campaign", required=True, choices=list(CAMPAIGNS.keys()))
    parser.add_argument("--end-date", help="Override end date (YYYY-MM-DD), defaults to yesterday")
    parser.add_argument("--slack-channel", help="Override Slack channel")
    args = parser.parse_args()

    global CAMPAIGN, TARGET_DMAS
    CAMPAIGN.update(CAMPAIGNS[args.campaign])
    TARGET_DMAS = CAMPAIGN.pop("dmas")

    if args.end_date:
        CAMPAIGN["end_date"] = args.end_date
    else:
        CAMPAIGN["end_date"] = (date.today() - timedelta(days=1)).isoformat()

    if not POSTHOG_API_KEY:
        print("ERROR: POSTHOG_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    end_date = CAMPAIGN["end_date"]
    print(f"[{args.campaign}] {CAMPAIGN['start_date']} – {end_date}")

    print("[1/3] Zip counts + geo stats...")
    zip_counts  = query_zip_counts()
    total       = sum(zip_counts.values())
    dma_counts, within_250 = compute_geo_stats(zip_counts)
    print(f"  {total} intakes  within_250={within_250}")

    print("[2/3] Daily CVR data...")
    intakes_by_date  = query_intakes_by_date()
    visitors_by_date = query_visitors_by_date()

    start_d = date.fromisoformat(CAMPAIGN["start_date"])
    end_d   = date.fromisoformat(end_date)
    cvr_data = []
    d = start_d
    while d <= end_d:
        key      = d.isoformat()
        intakes  = intakes_by_date.get(key, 0)
        visitors = visitors_by_date.get(key, 0)
        cvr_data.append({"d": f"{d.month}/{d.day}", "intakes": intakes, "visitors": visitors})
        d += timedelta(days=1)

    total_visitors = sum(visitors_by_date.values())
    overall_cvr    = (total / total_visitors) if total_visitors > 0 else None

    print("[3/3] TA breakdown...")
    ta_data = query_ta_counts()

    sorted_dmas = sorted(dma_counts.items(), key=lambda x: -x[1])
    post_to_slack({
        "title":       CAMPAIGN["title"],
        "campaign_id": CAMPAIGN["subtitle_campaign"],
        "end_date":    end_date,
        "campaign":    args.campaign,
        "total":       total,
        "overall_cvr": overall_cvr,
        "within_250":  within_250,
        "home_dma":    CAMPAIGN["home_dma"],
        "home_n":      dma_counts.get(CAMPAIGN["home_dma"], 0),
        "all_dmas":    dict(sorted_dmas),
        "cvr_data":    cvr_data,
        "ta_data":     ta_data,
        "slack_channel": args.slack_channel or SLACK_CHANNEL,
    })


if __name__ == "__main__":
    main()
