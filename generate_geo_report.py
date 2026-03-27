#!/usr/bin/env python3
"""
Campaign Geo HTML Report Generator

Queries PostHog for intake_success events, maps zip codes to DMAs using
pgeocode + nearest-centroid assignment, generates insights via Claude, and
writes a self-contained HTML report matching the Ohio campaign format.

Requirements:
    pip install requests pgeocode anthropic python-dotenv

PostHog API key: add POSTHOG_API_KEY to your .env file.
  - Use a personal API key (Settings → Personal API keys in PostHog)
  - Must have read access to the project.

Usage:
    python3 generate_geo_report.py                  # uses CONFIG below
    python3 generate_geo_report.py --skip-insights  # skip Claude insights (faster)
    python3 generate_geo_report.py --dry-run        # print queried data, no HTML
"""

import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import math

import requests
import pgeocode
import anthropic
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# ── CONFIG — edit this section for each campaign ──────────────────────────
# ---------------------------------------------------------------------------

CAMPAIGNS = {
    "OH": {
        "title": "Ohio Campaign — DMA Performance Analysis (L30)",
        "subtitle_campaign": "LEAD:ACB:OH:BROAD:PRO:PW:IS",
        "subtitle_source": "Meta Broad",
        "start_date": "2026-02-21",
        "end_date": "2026-03-25",
        "event_name": "intake_success",
        "utm_source": "meta",
        "utm_campaign_contains": "OH",
        "zip_property": "$initial_geoip_postal_code",
        "ta_property": "ta",
        "note": "On <strong>Friday, March 20</strong>, we launched <strong>30 new static ads</strong> into the Ohio campaign. On <strong>Monday, March 24</strong>, we added <strong>2 new video ads</strong>. Monitor CVR and intake volume in the days following each launch to assess creative impact.",
        "posthog_project": 133367,
        "zip_country": "US",
        "map_center_lat": 40.4,
        "map_center_lng": -82.7,
        "map_zoom": 7,
        "output": "ohio-l30-geo.html",
        "home_dma": "Cleveland-Akron (Canton)",
        "home_site": "Gabrail Cancer Center, Canton OH",
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
        "title": "California Campaign — DMA Performance Analysis (L30)",
        "subtitle_campaign": "LEAD:ACB:CA:BROAD:PRO:FOMAT:IS",
        "subtitle_source": "Meta Broad",
        "start_date": "2026-03-03",
        "end_date": "2026-03-25",
        "event_name": "intake_success",
        "utm_source": "meta",
        "utm_campaign_contains": "CA",
        "zip_property": "$initial_geoip_postal_code",
        "ta_property": "ta",
        "note": "On <strong>Monday, March 24</strong>, we added <strong>3 new video ads</strong> to the California campaign. Monitor CVR and intake volume in the days following to assess creative impact. &nbsp;|&nbsp; <strong>Zip code targeting is working:</strong> before the 3/18 relaunch, <strong>70%</strong> of intakes fell within 250 miles of FOMAT (21/30). Since switching to zip targeting, that's risen to <strong>92%</strong> (12/13) — a 22-point improvement in geographic containment. Out-of-range intakes from Colorado, Texas, Arizona, and Sacramento have largely disappeared. The one outlier (Hendersonville TN) is likely a VPN artifact.",
        "posthog_project": 133367,
        "zip_country": "US",
        "map_center_lat": 36.5,
        "map_center_lng": -119.5,
        "map_zoom": 6,
        "output": "california-l30-geo.html",
        "home_dma": "Los Angeles",
        "home_site": "FOMAT Medical Research Center, Oxnard CA",
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

# Active config — set by --campaign flag in main(), do not edit directly
CAMPAIGN = {}
TARGET_DMAS = []

# ---------------------------------------------------------------------------
# ── Setup ──────────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
load_dotenv(SCRIPT_DIR.parent / ".env")
load_dotenv(SCRIPT_DIR / ".env")

POSTHOG_API_KEY = os.getenv("POSTHOG_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "#north-reports")
POSTHOG_BASE = "https://us.posthog.com"

# ---------------------------------------------------------------------------
# ── PostHog helpers ────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

def ph_query(sql: str, retries: int = 3) -> list[dict]:
    """Run a HogQL query against PostHog and return the result rows."""
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
        cols = data["columns"]
        rows = data["results"]
        return [dict(zip(cols, row)) for row in rows]
    resp.raise_for_status()


def build_where(extra: str = "", event_name: str = None) -> str:
    """Build the shared WHERE clause for intake_success queries."""
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


def query_zip_counts() -> dict[str, int]:
    """Return {zip: count} for all intakes with a valid zip."""
    sql = f"""
SELECT person.properties.{CAMPAIGN['zip_property']} AS zip,
       count(DISTINCT person_id) AS n
FROM events
{build_where("person.properties." + CAMPAIGN['zip_property'] + " IS NOT NULL AND person.properties." + CAMPAIGN['zip_property'] + " != ''")}
GROUP BY zip
ORDER BY n DESC
"""
    rows = ph_query(sql)
    return {r["zip"]: int(r["n"]) for r in rows if r["zip"]}


def query_daily_trend() -> list[dict]:
    """Return [{d: 'M/D', v: n}, ...] for each day in range."""
    sql = f"""
SELECT toDate(timestamp) AS day,
       count(DISTINCT person_id) AS n
FROM events
{build_where()}
GROUP BY day
ORDER BY day ASC
"""
    rows = ph_query(sql)
    result = []
    for r in rows:
        dt = r["day"]
        if hasattr(dt, "strftime"):
            label = f"{dt.month}/{dt.day}"
        else:
            # string "YYYY-MM-DD"
            parts = str(dt).split("-")
            label = f"{int(parts[1])}/{int(parts[2])}"
        result.append({"d": label, "v": int(r["n"])})
    return result


def query_daily_visitors() -> dict[str, int]:
    """Return {date_str: unique_visitor_count} using $pageview events with same UTM filters."""
    sql = f"""
SELECT toDate(timestamp) AS day,
       count(DISTINCT person_id) AS n
FROM events
{build_where(event_name="$pageview")}
GROUP BY day
ORDER BY day ASC
"""
    rows = ph_query(sql)
    result = {}
    for r in rows:
        dt = r["day"]
        if hasattr(dt, "isoformat"):
            key = dt.isoformat()
        else:
            key = str(dt)  # "YYYY-MM-DD"
        result[key] = int(r["n"])
    return result


def query_ta_counts() -> list[dict]:
    """Return [{ta: 'Breast Cancer', n: 14}, ...] sorted by n desc."""
    prop = CAMPAIGN.get("ta_property")
    if not prop:
        return []
    sql = f"""
SELECT properties.{prop} AS ta,
       count(DISTINCT person_id) AS n
FROM events
{build_where(f"properties.{prop} IS NOT NULL AND properties.{prop} != ''")}
GROUP BY ta
ORDER BY n DESC
"""
    rows = ph_query(sql)
    return [{"ta": r["ta"], "n": int(r["n"])} for r in rows if r["ta"]]


# ---------------------------------------------------------------------------
# ── Geo helpers ────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

def haversine(lat1, lng1, lat2, lng2) -> float:
    """Return crow-flies distance in miles between two lat/lng points."""
    R = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def assign_zips_to_dmas(zip_counts: dict[str, int]) -> tuple[dict, list, int, int, dict]:
    """
    Map zip codes to DMAs. Returns:
      - dma_counts: {dma_name: intake_count}
      - outside_details: [{city/zip, lat, lng, count}] for Outside Target markers
      - outside_total: total outside-target intakes
      - unknown_total: intakes with no geo lookup
      - distance_buckets: counts bucketed by crow-flies miles from home site
    """
    nomi = pgeocode.Nominatim(CAMPAIGN["zip_country"])
    threshold = CAMPAIGN["outside_threshold_miles"]
    home_lat = CAMPAIGN["home_lat"]
    home_lng = CAMPAIGN["home_lng"]

    dma_counts = {d["dma"]: 0 for d in TARGET_DMAS}
    outside_by_zip = {}
    unknown_total = 0
    distance_buckets = {"within_250": 0, "250_to_500": 0, "over_500": 0, "no_geo": 0}

    for zip_code, count in zip_counts.items():
        row = nomi.query_postal_code(str(zip_code).zfill(5))
        if row is None or (hasattr(row, "isna") and row["latitude"] != row["latitude"]):
            unknown_total += count
            distance_buckets["no_geo"] += count
            continue
        try:
            lat = float(row["latitude"])
            lng = float(row["longitude"])
            place = str(row.get("place_name", "")) or zip_code
            state = str(row.get("state_code", "")) or ""
        except (TypeError, ValueError):
            unknown_total += count
            distance_buckets["no_geo"] += count
            continue

        # Distance from home site
        home_dist = haversine(lat, lng, home_lat, home_lng)
        if home_dist <= 250:
            distance_buckets["within_250"] += count
        elif home_dist <= 500:
            distance_buckets["250_to_500"] += count
        else:
            distance_buckets["over_500"] += count

        # Find nearest target DMA
        best_dma = None
        best_dist = float("inf")
        for d in TARGET_DMAS:
            dist = haversine(lat, lng, d["lat"], d["lng"])
            if dist < best_dist:
                best_dist = dist
                best_dma = d["dma"]

        if best_dist <= threshold:
            dma_counts[best_dma] = dma_counts.get(best_dma, 0) + count
        else:
            key = f"{place}, {state}" if state else place
            if key not in outside_by_zip:
                outside_by_zip[key] = {"city": place, "state": state, "lat": lat, "lng": lng, "count": 0}
            outside_by_zip[key]["count"] += count

    outside_details = sorted(outside_by_zip.values(), key=lambda x: -x["count"])
    outside_total = sum(d["count"] for d in outside_details)
    return dma_counts, outside_details, outside_total, unknown_total, distance_buckets


# ---------------------------------------------------------------------------
# ── Insights via Claude ────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

INSIGHTS_PROMPT = """\
You are a marketing analyst for North Trials, a clinical trial matching platform.
Given the following campaign geo performance data, write exactly 6 concise insights
(3-4 sentences each) for the "Key Findings" section of an internal report.

Format as a JSON array of objects:
[
  {"label": "Short label (3-5 words)", "text": "3-4 sentence insight with key numbers bolded using **bold**."},
  ...
]

Rules:
- Use **bold** around key stats and DMA names in the text field
- Be specific — reference actual DMA names and intake counts
- Home market = {home_dma}. Home site = {home_site}.
- Always include one insight specifically about the 250-mile catchment radius from the home site,
  using the within_250_miles, miles_250_to_500, and over_500_miles counts provided.
- Cover: top performer, home market, 250-mile catchment, geographic patterns,
  any zero-signal DMAs, TA spread (if available), outside-target spillover
- Return ONLY valid JSON — no markdown fences, no commentary
"""


def generate_insights(dma_counts: dict, outside_total: int, unknown_total: int,
                      ta_data: list, total: int, distance_buckets: dict) -> list[dict]:
    """Call Claude to generate 6 insights. Returns [{label, text}, ...]."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    sorted_dmas = sorted(dma_counts.items(), key=lambda x: -x[1])
    zero_dmas = [d for d, n in sorted_dmas if n == 0]
    top_dma, top_n = sorted_dmas[0] if sorted_dmas else ("Unknown", 0)
    home_n = dma_counts.get(CAMPAIGN["home_dma"], 0)
    within_250 = distance_buckets["within_250"]
    pct_250 = f"{100 * within_250 / total:.0f}%" if total else "0%"

    context = {
        "campaign": CAMPAIGN["subtitle_campaign"],
        "date_range": f"{CAMPAIGN['start_date']} to {CAMPAIGN['end_date']}",
        "total_intakes": total,
        "top_dma": top_dma,
        "top_dma_intakes": top_n,
        "top_dma_pct": f"{100 * top_n / total:.0f}%" if total else "0%",
        "home_dma": CAMPAIGN["home_dma"],
        "home_site": CAMPAIGN.get("home_site", CAMPAIGN["home_dma"]),
        "home_dma_intakes": home_n,
        "zero_signal_dmas": zero_dmas,
        "outside_target_intakes": outside_total,
        "unknown_intakes": unknown_total,
        "top_10_dmas": [{"dma": d, "intakes": n} for d, n in sorted_dmas[:10]],
        "ta_breakdown": ta_data[:8] if ta_data else "not available",
        "catchment_radius": {
            "within_250_miles": within_250,
            "within_250_pct": pct_250,
            "miles_250_to_500": distance_buckets["250_to_500"],
            "over_500_miles": distance_buckets["over_500"],
            "no_geo": distance_buckets["no_geo"],
        },
    }

    system = (INSIGHTS_PROMPT
              .replace("{home_dma}", CAMPAIGN["home_dma"])
              .replace("{home_site}", CAMPAIGN.get("home_site", CAMPAIGN["home_dma"])))
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=system,
        messages=[{"role": "user", "content": json.dumps(context, indent=2)}],
    )
    raw = msg.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
    if raw.endswith("```"):
        raw = raw.rsplit("```", 1)[0]
    return json.loads(raw.strip())


# ---------------------------------------------------------------------------
# ── HTML builder ───────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

def esc(s: str) -> str:
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def build_html(dma_counts: dict, outside_details: list, outside_total: int,
               unknown_total: int, ta_data: list, trend_data: list,
               insights: list, total: int, distance_buckets: dict,
               cvr_data: list = None, overall_cvr: float = None) -> str:

    c = CAMPAIGN
    sorted_dmas = sorted(dma_counts.items(), key=lambda x: -x[1])
    dmas_active = sum(1 for _, n in sorted_dmas if n > 0)
    dmas_zero = sum(1 for _, n in sorted_dmas if n == 0)
    zero_names = ", ".join(d for d, n in sorted_dmas if n == 0) or "None"
    top_dma, top_n = sorted_dmas[0] if sorted_dmas else ("—", 0)
    top_dma_pct = f"{100 * top_n / total:.0f}%" if total else "0%"
    home_n = dma_counts.get(c["home_dma"], 0)
    within_250 = distance_buckets["within_250"]
    within_250_pct = f"{100 * within_250 / total:.0f}%" if total else "0%"

    # Build DMA JS array
    dma_js_rows = []
    for d in TARGET_DMAS:
        name = d["dma"]
        n = dma_counts.get(name, 0)
        is_home = "true" if name == c["home_dma"] else "false"
        dma_js_rows.append(
            f'  {{dma:{json.dumps(name)}, intakes:{n}, isHome:{is_home}, lat:{d["lat"]}, lng:{d["lng"]}}}'
        )
    dma_js = "[\n" + ",\n".join(dma_js_rows) + "\n]"

    # Outside markers JS array
    outside_js_rows = []
    for m in outside_details[:8]:  # cap at 8 markers on map
        outside_js_rows.append(
            f'  {{city:{json.dumps(m["city"])}, state:{json.dumps(m["state"])}, '
            f'lat:{m["lat"]}, lng:{m["lng"]}, count:{m["count"]}}}'
        )
    outside_js = "[\n" + ",\n".join(outside_js_rows) + "\n]"

    # TA JS array
    ta_js = json.dumps(ta_data)

    # Trend JS array
    trend_js = json.dumps(trend_data)

    # TA badge total
    ta_total = sum(d["n"] for d in ta_data)

    # Insights HTML
    insights_html = ""
    for ins in insights:
        label = esc(ins.get("label", ""))
        # Convert **bold** markers to <strong>
        text = ins.get("text", "")
        import re
        text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
        text = esc(text).replace("&lt;strong&gt;", "<strong>").replace("&lt;/strong&gt;", "</strong>")
        insights_html += f"""      <div class="ic">
        <div class="ic-lbl">{label}</div>
        <p>{text}</p>
      </div>\n"""

    ta_section = ""
    if ta_data:
        ta_section = f"""
      <!-- TA chart -->
      <div class="section" style="flex:1;">
        <div class="section-header">
          <h2>Therapeutic Area Breakdown</h2>
          <span class="badge">{ta_total} total events</span>
        </div>
        <div class="ta-body" id="ta-body"></div>
      </div>"""

    date_range_str = f"{c['start_date'].replace('-', '/')} – {c['end_date'].replace('-', '/')}"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{esc(c['title'])} — North Trials</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: Inter, -apple-system, sans-serif; background: #f4f7f5; color: #1a2e25; }}

.header {{ background: #194039; color: #fff; padding: 28px 40px 24px; }}
.header h1 {{ font-size: 22px; font-weight: 700; }}
.header .sub {{ margin-top: 6px; font-size: 13px; color: rgba(255,255,255,0.6); font-family: 'Courier New', monospace; }}

.content {{ max-width: 1260px; margin: 0 auto; padding: 32px 40px; }}

.kpi-strip {{ display: grid; grid-template-columns: repeat(6, 1fr); gap: 14px; margin-bottom: 28px; }}
.kpi {{ background: #fff; border: 1px solid #d4e4dc; border-radius: 12px; padding: 18px 20px; }}
.kpi .label {{ font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.07em; color: #6b8c7a; margin-bottom: 8px; }}
.kpi .value {{ font-size: 32px; font-weight: 800; color: #0a5c3e; line-height: 1; }}
.kpi .detail {{ font-size: 12px; color: #6b8c7a; margin-top: 5px; }}

.section {{ background: #fff; border: 1px solid #d4e4dc; border-radius: 12px; margin-bottom: 24px; overflow: hidden; }}
.section-header {{ padding: 18px 24px 16px; border-bottom: 1px solid #e8f0ec; display: flex; align-items: center; justify-content: space-between; }}
.section-header h2 {{ font-size: 15px; font-weight: 700; color: #194039; }}
.badge {{ font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.07em; background: #e8f5ee; color: #0a5c3e; padding: 3px 10px; border-radius: 110px; }}

#map {{ height: 500px; width: 100%; }}
.map-legend {{ display: flex; flex-wrap: wrap; gap: 14px; align-items: center; padding: 12px 24px; border-top: 1px solid #e8f0ec; font-size: 12px; color: #6b8c7a; }}
.ld {{ width: 12px; height: 12px; border-radius: 50%; display: inline-block; margin-right: 4px; vertical-align: middle; }}

.dma-section {{ padding: 20px 24px; }}
.dma-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
.dma-table th {{ text-align: left; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em; color: #6b8c7a; padding: 8px 12px; border-bottom: 1px solid #e8f0ec; }}
.dma-table td {{ padding: 9px 12px; border-bottom: 1px solid #f0f5f2; vertical-align: middle; }}
.dma-table tr:last-child td {{ border-bottom: none; }}
.dma-table tr:hover td {{ background: #f9fbfa; }}
.bar-track {{ background: #e8f0ec; border-radius: 4px; height: 8px; width: 160px; position: relative; display: inline-block; vertical-align: middle; }}
.bar-fill {{ height: 100%; border-radius: 4px; }}
.tier-strong {{ color: #0a5c3e; font-weight: 700; }}
.tier-mod {{ color: #d97706; font-weight: 600; }}
.tier-low {{ color: #888; }}
.tier-zero {{ color: #ccc; font-style: italic; }}
.canton-star {{ color: #f5a623; font-size: 14px; margin-left: 4px; }}
.dma-name-cell {{ display: flex; align-items: center; gap: 4px; }}

.pill {{ display: inline-block; padding: 2px 9px; border-radius: 110px; font-size: 11px; font-weight: 600; }}
.pill-strong {{ background: #e8f5ee; color: #0a5c3e; }}
.pill-mod {{ background: #fff5e8; color: #9c6b00; }}
.pill-low {{ background: #f0f0f0; color: #666; }}
.pill-zero {{ background: #f7f7f7; color: #bbb; }}
.pill-outside {{ background: #fef2f2; color: #c0392b; }}

.two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; margin-bottom: 24px; }}

.ta-body {{ padding: 20px 24px; }}
.ta-row {{ display: flex; align-items: center; gap: 10px; margin-bottom: 9px; }}
.ta-label {{ font-size: 12px; font-weight: 500; min-width: 160px; }}
.ta-count {{ font-size: 13px; font-weight: 700; color: #0a5c3e; min-width: 24px; text-align: right; }}
.ta-pct {{ font-size: 11px; color: #6b8c7a; min-width: 32px; text-align: right; }}

.trend-body {{ padding: 20px 24px; }}

.insights-grid {{ padding: 20px 24px; display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
.ic {{ background: #f4f7f5; border: 1px solid #d4e4dc; border-radius: 10px; padding: 16px 18px; }}
.ic .ic-lbl {{ font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; color: #6b8c7a; margin-bottom: 8px; }}
.ic p {{ font-size: 13px; line-height: 1.6; color: #1a2e25; }}
.ic strong {{ color: #0a5c3e; }}
</style>
<style id="print-overrides">
@media print {{
  @page {{ margin: 0; }}
  body {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
}}
.section, .kpi-strip, .two-col, .kpi,
#map, .dma-section, .trend-body, .ta-body,
.insights-grid, .ic {{
  page-break-inside: avoid;
  break-inside: avoid;
}}
</style>
</head>
<body>

<div class="header">
  <h1>{esc(c['title'])}</h1>
  <div class="sub">Campaign: {esc(c['subtitle_campaign'])} &nbsp;·&nbsp; Source: {esc(c['subtitle_source'])} &nbsp;·&nbsp; {date_range_str} &nbsp;·&nbsp; ★ = home market</div>
</div>

<div class="content">

  <div class="kpi-strip">
    <div class="kpi">
      <div class="label">Total Intakes</div>
      <div class="value">{total}</div>
      <div class="detail">{c['event_name']}, full date range</div>
    </div>
    <div class="kpi">
      <div class="label">Within 250 Miles</div>
      <div class="value">{within_250}</div>
      <div class="detail">{within_250_pct} of intakes, from home site</div>
    </div>
    <div class="kpi">
      <div class="label">DMAs Active</div>
      <div class="value">{dmas_active}</div>
      <div class="detail">of {len(TARGET_DMAS)} targeted</div>
    </div>
    <div class="kpi">
      <div class="label">Top DMA</div>
      <div class="value">{esc(top_dma.split(',')[0].split('(')[0].strip())}</div>
      <div class="detail">{top_n} intakes ({top_dma_pct})</div>
    </div>
    <div class="kpi">
      <div class="label">Home Market ★</div>
      <div class="value">{home_n}</div>
      <div class="detail">{esc(c['home_dma'])}</div>
    </div>
    <div class="kpi">
      <div class="label">Overall CVR</div>
      <div class="value">{f"{overall_cvr*100:.1f}%" if overall_cvr is not None else "—"}</div>
      <div class="detail">intakes ÷ unique visitors</div>
    </div>
  </div>

  <!-- MAP -->
  <div class="section">
    <div class="section-header">
      <h2>DMA Bubble Map</h2>
      <span class="badge">bubble = intake count</span>
    </div>
    <div id="map"></div>
    <div class="map-legend">
      <span><span class="ld" style="background:#0a5c3e;"></span>Strong (7+)</span>
      <span><span class="ld" style="background:#d97706;"></span>Moderate (3–6)</span>
      <span><span class="ld" style="background:#888;"></span>Low (1–2)</span>
      <span><span class="ld" style="background:#e0e0e0; border:1px solid #ccc;"></span>Zero</span>
      <span><span class="ld" style="background:#c0392b;"></span>Outside target</span>
      <span style="margin-left:4px;">★ = home market</span>
    </div>
  </div>

  <!-- DMA TABLE + TREND side by side -->
  <div class="two-col">

    <div class="section">
      <div class="section-header">
        <h2>All {len(TARGET_DMAS)} Targeted DMAs</h2>
        <span class="badge">+ outside target</span>
      </div>
      <div class="dma-section">
        <table class="dma-table">
          <thead>
            <tr>
              <th>DMA</th>
              <th>Intakes</th>
              <th></th>
              <th>Tier</th>
            </tr>
          </thead>
          <tbody id="dma-tbody"></tbody>
        </table>
      </div>
    </div>

    <div style="display:flex; flex-direction:column; gap:24px;">
{ta_section}
      <!-- Daily trend -->
      <div class="section" style="flex:1;">
        <div class="section-header">
          <h2>Daily Intake Trend</h2>
          <span class="badge">{date_range_str}</span>
        </div>
        <div class="trend-body">
          <canvas id="trend-chart" style="width:100%;height:180px;"></canvas>
        </div>
      </div>

    </div>
  </div>

  <!-- CVR Trend -->
  <div class="section">
    <div class="section-header">
      <h2>Daily Conversion Rate</h2>
      <span class="badge">intakes ÷ unique visitors</span>
    </div>
    <div class="trend-body">
      <canvas id="cvr-chart" style="width:100%;height:200px;"></canvas>
    </div>
  </div>

  <!-- Insights -->
  <div class="section">
    <div class="section-header">
      <h2>Key Findings</h2>
    </div>
    <div class="insights-grid">
{insights_html}{f'''      <div class="ic" style="border-color:#d97706;background:#fff9f0;">
        <div class="ic-lbl" style="color:#9c6b00;">Campaign Note</div>
        <p>{c["note"]}</p>
      </div>
''' if c.get("note") else ""}    </div>
  </div>

</div>

<script>
var DMA_DATA = {dma_js};
var OUTSIDE_MARKERS = {outside_js};
var TA_DATA = {ta_js};
var TREND_DATA = {trend_js};
var CVR_DATA = {json.dumps(cvr_data or [])};
var OVERALL_CVR = {f"{overall_cvr:.4f}" if overall_cvr is not None else "null"};
var TOTAL = {total};
var TA_TOTAL = {ta_total};

function tier(n) {{
  if (n >= 7) return "strong";
  if (n >= 3) return "mod";
  if (n >= 1) return "low";
  return "zero";
}}
function tierColor(n) {{
  if (n >= 7) return "#0a5c3e";
  if (n >= 3) return "#d97706";
  if (n >= 1) return "#888";
  return "#e0e0e0";
}}
function tierLabel(n) {{
  if (n >= 7) return "Strong";
  if (n >= 3) return "Moderate";
  if (n >= 1) return "Low";
  return "Zero";
}}
function tierPill(n) {{
  if (n >= 7) return "pill-strong";
  if (n >= 3) return "pill-mod";
  if (n >= 1) return "pill-low";
  return "pill-zero";
}}

// Map
var map = L.map('map').setView([{c['map_center_lat']}, {c['map_center_lng']}], {c['map_zoom']});
L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
  attribution: '&copy; OpenStreetMap &copy; CARTO',
  subdomains: 'abcd', maxZoom: 19
}}).addTo(map);

DMA_DATA.forEach(function(d) {{
  var color = d.intakes === 0 ? "#ccc" : tierColor(d.intakes);
  var opacity = d.intakes === 0 ? 0.35 : 0.65;
  var radius = d.intakes === 0 ? 7 : Math.max(8, Math.sqrt(d.intakes) * 10);
  var circle = L.circleMarker([d.lat, d.lng], {{
    radius: radius, fillColor: color,
    color: d.isHome ? "#f5a623" : color,
    weight: d.isHome ? 3 : 1.5,
    opacity: 1, fillOpacity: opacity
  }}).addTo(map);
  var popup = '<strong>' + d.dma + '</strong>';
  if (d.isHome) popup += ' <span style="color:#f5a623;">★ Home market</span>';
  popup += '<br>' + d.intakes + ' intake' + (d.intakes !== 1 ? 's' : '');
  popup += '<br><em>' + tierLabel(d.intakes) + '</em>';
  circle.bindPopup(popup);
  if (d.isHome) {{
    L.marker([d.lat + 0.18, d.lng], {{
      icon: L.divIcon({{
        html: '<span style="font-size:18px;filter:drop-shadow(0 1px 2px rgba(0,0,0,0.4));">★</span>',
        className: '', iconAnchor: [9, 9]
      }})
    }}).addTo(map);
  }}
}});

OUTSIDE_MARKERS.forEach(function(m) {{
  L.circleMarker([m.lat, m.lng], {{
    radius: Math.max(7, Math.sqrt(m.count) * 9),
    fillColor: "#c0392b", color: "#c0392b",
    weight: 1.5, opacity: 1, fillOpacity: 0.7
  }}).addTo(map).bindPopup(
    '<strong>' + m.city + (m.state ? ', ' + m.state : '') + '</strong>' +
    '<br>' + m.count + ' intake' + (m.count !== 1 ? 's' : '') +
    '<br><em style="color:#c0392b;">Outside target</em>'
  );
}});

// DMA Table
var OUTSIDE_TOTAL = {outside_total};
var UNKNOWN_TOTAL = {unknown_total};
var maxIntakes = Math.max.apply(null, DMA_DATA.map(function(d){{return d.intakes;}}));
var allRows = DMA_DATA.slice().sort(function(a,b){{return b.intakes - a.intakes;}}).concat([
  {{dma:"Outside Target", intakes:OUTSIDE_TOTAL}},
  {{dma:"Unknown / No geo", intakes:UNKNOWN_TOTAL}}
]);
var html = allRows.map(function(d) {{
  var barW = d.intakes > 0 ? (d.intakes / maxIntakes * 100).toFixed(1) : 0;
  var color = (d.dma === "Outside Target" || d.dma === "Unknown / No geo") ? "#c0392b" : tierColor(d.intakes);
  var pillCls = (d.dma === "Outside Target" || d.dma === "Unknown / No geo") ? "pill-outside" : tierPill(d.intakes);
  var lbl = (d.dma === "Outside Target" || d.dma === "Unknown / No geo") ? "—" : tierLabel(d.intakes);
  var namePart = '<span>' + d.dma + '</span>';
  if (d.isHome) namePart += '<span class="canton-star" title="Home market">★</span>';
  return '<tr>' +
    '<td><div class="dma-name-cell">' + namePart + '</div></td>' +
    '<td style="font-weight:700;color:' + (d.intakes > 0 ? color : '#ccc') + ';">' + d.intakes + '</td>' +
    '<td><div class="bar-track"><div class="bar-fill" style="width:' + barW + '%;background:' + color + ';"></div></div></td>' +
    '<td><span class="pill ' + pillCls + '">' + lbl + '</span></td>' +
    '</tr>';
}}).join('');
document.getElementById('dma-tbody').innerHTML = html;

// TA Chart
if (TA_DATA.length && document.getElementById('ta-body')) {{
  var taMax = TA_DATA[0].n;
  var taColors = [
    "#0a5c3e","#1a7a54","#2a9870","#3ab68c","#4ad4a8",
    "#5ab2d0","#6a90b8","#7a6ea0","#8a4c88","#9a2a70","#aa0858"
  ];
  var taHtml = TA_DATA.map(function(d, i) {{
    var w = (d.n / taMax * 100).toFixed(1);
    var pct = ((d.n / TA_TOTAL) * 100).toFixed(0);
    var color = taColors[i] || "#888";
    return '<div class="ta-row">' +
      '<div class="ta-label">' + d.ta + '</div>' +
      '<div class="bar-track" style="flex:1;"><div class="bar-fill" style="width:' + w + '%;background:' + color + ';height:8px;"></div></div>' +
      '<div class="ta-count">' + d.n + '</div>' +
      '<div class="ta-pct">' + pct + '%</div>' +
    '</div>';
  }}).join('');
  document.getElementById('ta-body').innerHTML = taHtml;
}}

// Trend chart
(function() {{
  var canvas = document.getElementById('trend-chart');
  if (!canvas || !TREND_DATA.length) return;
  var ctx = canvas.getContext('2d');
  var dpr = window.devicePixelRatio || 1;
  var W = canvas.offsetWidth || 400, H = 180;
  canvas.width = W * dpr; canvas.height = H * dpr;
  canvas.style.width = W + 'px'; canvas.style.height = H + 'px';
  ctx.scale(dpr, dpr);
  var pad = {{top:16, right:16, bottom:28, left:26}};
  var iw = W - pad.left - pad.right, ih = H - pad.top - pad.bottom;
  var n = TREND_DATA.length;
  var maxV = Math.max.apply(null, TREND_DATA.map(function(d){{return d.v;}}));
  function x(i) {{ return pad.left + (i / (n - 1)) * iw; }}
  function y(v) {{ return pad.top + ih - (v / maxV) * ih; }}
  ctx.strokeStyle = '#e8f0ec'; ctx.lineWidth = 1;
  [0, 2, 4, 6, 8].forEach(function(v) {{
    if (v > maxV + 1) return;
    ctx.beginPath(); ctx.moveTo(pad.left, y(v)); ctx.lineTo(pad.left + iw, y(v)); ctx.stroke();
    ctx.fillStyle = '#aaa'; ctx.font = '10px Inter'; ctx.textAlign = 'right';
    ctx.fillText(v, pad.left - 4, y(v) + 3);
  }});
  ctx.beginPath(); ctx.moveTo(x(0), y(0));
  TREND_DATA.forEach(function(d, i) {{ ctx.lineTo(x(i), y(d.v)); }});
  ctx.lineTo(x(n - 1), y(0)); ctx.closePath();
  ctx.fillStyle = 'rgba(10,92,62,0.09)'; ctx.fill();
  ctx.beginPath();
  TREND_DATA.forEach(function(d, i) {{
    if (i === 0) ctx.moveTo(x(i), y(d.v)); else ctx.lineTo(x(i), y(d.v));
  }});
  ctx.strokeStyle = '#0a5c3e'; ctx.lineWidth = 2.5; ctx.lineJoin = 'round'; ctx.stroke();
  TREND_DATA.forEach(function(d, i) {{
    ctx.beginPath(); ctx.arc(x(i), y(d.v), 3, 0, Math.PI * 2);
    ctx.fillStyle = '#0a5c3e'; ctx.fill();
    ctx.strokeStyle = '#fff'; ctx.lineWidth = 1.5; ctx.stroke();
  }});
  ctx.fillStyle = '#888'; ctx.font = '10px Inter'; ctx.textAlign = 'center';
  TREND_DATA.forEach(function(d, i) {{
    if (i % 4 === 0 || i === n - 1) ctx.fillText(d.d, x(i), H - 8);
  }});
}})();

// CVR chart
(function() {{
  var canvas = document.getElementById('cvr-chart');
  if (!canvas || !CVR_DATA.length) return;
  var ctx = canvas.getContext('2d');
  var dpr = window.devicePixelRatio || 1;
  var W = canvas.offsetWidth || 900, H = 200;
  canvas.width = W * dpr; canvas.height = H * dpr;
  canvas.style.width = W + 'px'; canvas.style.height = H + 'px';
  ctx.scale(dpr, dpr);
  var pad = {{top:20, right:20, bottom:30, left:38}};
  var iw = W - pad.left - pad.right, ih = H - pad.top - pad.bottom;
  var n = CVR_DATA.length;
  // Only include points with visitors > 0 for scale calc; show gaps for zero-visitor days
  var validVals = CVR_DATA.filter(function(d){{return d.visitors > 0;}}).map(function(d){{return d.cvr;}});
  var maxV = validVals.length ? Math.max.apply(null, validVals) : 30;
  var yMax = Math.ceil(maxV / 5) * 5 + 5;
  function xp(i) {{ return pad.left + (i / (n - 1)) * iw; }}
  function yp(v) {{ return pad.top + ih - (v / yMax) * ih; }}
  // Grid lines
  ctx.strokeStyle = '#e8f0ec'; ctx.lineWidth = 1;
  for (var gv = 0; gv <= yMax; gv += 5) {{
    ctx.beginPath(); ctx.moveTo(pad.left, yp(gv)); ctx.lineTo(pad.left + iw, yp(gv)); ctx.stroke();
    ctx.fillStyle = '#aaa'; ctx.font = '10px Inter'; ctx.textAlign = 'right';
    ctx.fillText(gv + '%', pad.left - 5, yp(gv) + 3);
  }}
  // Average line
  if (OVERALL_CVR !== null) {{
    ctx.beginPath();
    ctx.moveTo(pad.left, yp(OVERALL_CVR * 100));
    ctx.lineTo(pad.left + iw, yp(OVERALL_CVR * 100));
    ctx.strokeStyle = 'rgba(10,92,62,0.25)'; ctx.lineWidth = 1.5;
    ctx.setLineDash([4, 3]); ctx.stroke(); ctx.setLineDash([]);
    ctx.fillStyle = 'rgba(10,92,62,0.5)'; ctx.font = '10px Inter'; ctx.textAlign = 'left';
    ctx.fillText('avg ' + (OVERALL_CVR * 100).toFixed(1) + '%', pad.left + 4, yp(OVERALL_CVR * 100) - 4);
  }}
  // Area fill
  ctx.beginPath();
  var started = false;
  CVR_DATA.forEach(function(d, i) {{
    if (d.visitors === 0) return;
    if (!started) {{ ctx.moveTo(xp(i), yp(d.cvr)); started = true; }}
    else ctx.lineTo(xp(i), yp(d.cvr));
  }});
  if (started) {{
    // close area to baseline (find last valid index)
    var lastIdx = -1;
    CVR_DATA.forEach(function(d, i) {{ if (d.visitors > 0) lastIdx = i; }});
    var firstIdx = -1;
    CVR_DATA.forEach(function(d, i) {{ if (d.visitors > 0 && firstIdx === -1) firstIdx = i; }});
    ctx.lineTo(xp(lastIdx), yp(0)); ctx.lineTo(xp(firstIdx), yp(0)); ctx.closePath();
    ctx.fillStyle = 'rgba(10,92,62,0.07)'; ctx.fill();
  }}
  // Line
  ctx.beginPath(); started = false;
  CVR_DATA.forEach(function(d, i) {{
    if (d.visitors === 0) {{ started = false; return; }}
    if (!started) {{ ctx.moveTo(xp(i), yp(d.cvr)); started = true; }}
    else ctx.lineTo(xp(i), yp(d.cvr));
  }});
  ctx.strokeStyle = '#0a5c3e'; ctx.lineWidth = 2.5; ctx.lineJoin = 'round'; ctx.stroke();
  // Dots + tooltip data
  CVR_DATA.forEach(function(d, i) {{
    if (d.visitors === 0) return;
    ctx.beginPath(); ctx.arc(xp(i), yp(d.cvr), 3, 0, Math.PI * 2);
    ctx.fillStyle = '#0a5c3e'; ctx.fill();
    ctx.strokeStyle = '#fff'; ctx.lineWidth = 1.5; ctx.stroke();
  }});
  // X labels
  ctx.fillStyle = '#888'; ctx.font = '10px Inter'; ctx.textAlign = 'center';
  CVR_DATA.forEach(function(d, i) {{
    if (i % 4 === 0 || i === n - 1) ctx.fillText(d.d, xp(i), H - 8);
  }});
}})();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# ── Main ───────────────────────────────────────────────────────────────────
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate campaign geo HTML report from PostHog data")
    parser.add_argument("--campaign", required=True, choices=list(CAMPAIGNS.keys()),
                        help=f"Which campaign to run: {', '.join(CAMPAIGNS.keys())}")
    parser.add_argument("--skip-insights", action="store_true", help="Skip Claude insight generation")
    parser.add_argument("--dry-run", action="store_true", help="Print queried data, do not write HTML")
    parser.add_argument("--end-date", help="Override end date (YYYY-MM-DD). Defaults to config value.")
    parser.add_argument("--slack", action="store_true", help="Post summary + PDF to Slack after generating")
    parser.add_argument("--slack-channel", help="Override Slack channel (default: $SLACK_CHANNEL or #north-reports)")
    args = parser.parse_args()

    global CAMPAIGN, TARGET_DMAS
    CAMPAIGN.update(CAMPAIGNS[args.campaign])
    TARGET_DMAS = CAMPAIGN.pop("dmas")
    if args.end_date:
        CAMPAIGN["end_date"] = args.end_date

    if not POSTHOG_API_KEY:
        print("ERROR: POSTHOG_API_KEY not set in .env", file=sys.stderr)
        sys.exit(1)

    print(f"Querying PostHog project {CAMPAIGN['posthog_project']}...")
    print(f"  Campaign filter: utm_campaign ILIKE '%{CAMPAIGN.get('utm_campaign_contains')}%'")
    print(f"  Date range: {CAMPAIGN['start_date']} – {CAMPAIGN['end_date']}")

    print("\n[1/4] Fetching zip code counts...")
    zip_counts = query_zip_counts()
    total = sum(zip_counts.values())
    print(f"  {len(zip_counts)} distinct zips, {total} total intakes")

    print("\n[2/4] Mapping zips to DMAs (pgeocode)...")
    dma_counts, outside_details, outside_total, unknown_total, distance_buckets = assign_zips_to_dmas(zip_counts)
    assigned = sum(dma_counts.values())
    print(f"  Assigned: {assigned}  Outside target: {outside_total}  Unknown: {unknown_total}")
    for dma, n in sorted(dma_counts.items(), key=lambda x: -x[1])[:8]:
        print(f"    {dma}: {n}")

    print("\n[3/4] Fetching trend, CVR, and TA data...")
    trend_data = query_daily_trend()
    daily_visitors = query_daily_visitors()
    ta_data = query_ta_counts() if CAMPAIGN.get("ta_property") else []
    print(f"  Trend: {len(trend_data)} days  Visitor days: {len(daily_visitors)}  TA: {len(ta_data)} categories")

    # Build daily CVR data by merging intakes + visitors per date
    # Re-fetch intake counts keyed by date string for the merge
    from datetime import date, timedelta
    start_d = date.fromisoformat(CAMPAIGN["start_date"])
    end_d   = date.fromisoformat(CAMPAIGN["end_date"])

    # Build intake-by-date lookup from trend_data
    # trend_data uses "M/D" labels; re-query as date strings for exact matching
    intakes_by_date_sql = f"""
SELECT toDate(timestamp) AS day, count(DISTINCT person_id) AS n
FROM events
{build_where()}
GROUP BY day ORDER BY day ASC
"""
    intake_rows = ph_query(intakes_by_date_sql)
    intakes_by_date = {}
    for r in intake_rows:
        dt = r["day"]
        key = dt.isoformat() if hasattr(dt, "isoformat") else str(dt)
        intakes_by_date[key] = int(r["n"])

    cvr_data = []
    d = start_d
    while d <= end_d:
        key = d.isoformat()
        label = f"{d.month}/{d.day}"
        intakes_v = intakes_by_date.get(key, 0)
        visitors_v = daily_visitors.get(key, 0)
        cvr_pct = (intakes_v / visitors_v * 100) if visitors_v > 0 else 0
        cvr_data.append({"d": label, "intakes": intakes_v, "visitors": visitors_v, "cvr": round(cvr_pct, 2)})
        d += timedelta(days=1)

    total_visitors = sum(daily_visitors.values())
    overall_cvr = (total / total_visitors) if total_visitors > 0 else None
    print(f"  Total visitors: {total_visitors}  Overall CVR: {f'{overall_cvr*100:.1f}%' if overall_cvr else '—'}")

    if args.dry_run:
        print("\nDry run — no HTML written.")
        return

    if args.skip_insights:
        print("\n[4/4] Skipping insights (--skip-insights).")
        insights = [{"label": "Insight placeholder", "text": "Run without --skip-insights to generate AI insights."}]
    else:
        if not ANTHROPIC_API_KEY:
            print("WARNING: ANTHROPIC_API_KEY not set — skipping insights.", file=sys.stderr)
            insights = []
        else:
            print("\n[4/4] Generating insights via Claude...")
            insights = generate_insights(dma_counts, outside_total, unknown_total, ta_data, total, distance_buckets)
            print(f"  Generated {len(insights)} insights")

    print("\nBuilding HTML...")
    html = build_html(dma_counts, outside_details, outside_total, unknown_total,
                      ta_data, trend_data, insights, total, distance_buckets,
                      cvr_data=cvr_data, overall_cvr=overall_cvr)

    out_path = SCRIPT_DIR / CAMPAIGN["output"]
    out_path.write_text(html, encoding="utf-8")
    print(f"\nDone → {out_path}")

    if args.slack:
        sorted_dmas = sorted(dma_counts.items(), key=lambda x: -x[1])
        top_dma, top_dma_n = sorted_dmas[0] if sorted_dmas else ("—", 0)
        print("\nPosting to Slack...")
        post_to_slack({
            "title": CAMPAIGN["title"],
            "campaign_id": CAMPAIGN["subtitle_campaign"],
            "start_date": CAMPAIGN["start_date"],
            "end_date": CAMPAIGN["end_date"],
            "campaign": args.campaign,
            "total": total,
            "overall_cvr": overall_cvr,
            "within_250": distance_buckets["within_250"],
            "top_dma": top_dma,
            "top_dma_n": top_dma_n,
            "dmas_active": sum(1 for _, n in sorted_dmas if n > 0),
            "dmas_total": len(TARGET_DMAS),
            "home_dma": CAMPAIGN["home_dma"],
            "home_n": dma_counts.get(CAMPAIGN["home_dma"], 0),
            "all_dmas": dict(sorted_dmas),
            "cvr_data": cvr_data,
            "ta_data": ta_data,
            "slack_channel": args.slack_channel or SLACK_CHANNEL,
        })


def post_to_slack(metrics: dict) -> None:
    """Post a self-contained daily digest to Slack — no file attachment."""
    token = SLACK_BOT_TOKEN
    channel = metrics.get("slack_channel", SLACK_CHANNEL)
    if not token:
        print("WARNING: SLACK_BOT_TOKEN not set — skipping Slack post.", file=sys.stderr)
        return

    slack_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # ── Derived stats ──────────────────────────────────────────────────────
    cvr_data  = metrics.get("cvr_data", [])
    end_date  = metrics["end_date"]

    # Yesterday's intakes (last entry in cvr_data)
    yesterday_intakes = cvr_data[-1]["intakes"] if cvr_data else 0

    # Pool last 7 days vs prior 7 days for CVR comparison
    active = [d for d in cvr_data if d["visitors"] > 0]
    cur7  = active[-7:]  if len(active) >= 1  else active
    prev7 = active[-14:-7] if len(active) >= 8 else []
    cur_cvr  = (sum(d["intakes"] for d in cur7)  / sum(d["visitors"] for d in cur7)  * 100) if cur7  and sum(d["visitors"] for d in cur7)  else None
    prev_cvr = (sum(d["intakes"] for d in prev7) / sum(d["visitors"] for d in prev7) * 100) if prev7 and sum(d["visitors"] for d in prev7) else None

    if cur_cvr is not None and prev_cvr is not None:
        delta = cur_cvr - prev_cvr
        trend = f"{'↑' if delta >= 0 else '↓'} {abs(delta):.1f}pp vs prior 7d"
        cvr_str = f"{cur_cvr:.1f}%  {trend}"
    elif cur_cvr is not None:
        cvr_str = f"{cur_cvr:.1f}%"
    else:
        cvr_str = "—"

    # Mini bar chart of last 10 days — horizontal bars, one row per day
    chart_days = cvr_data[-10:]
    bar_width = 10
    if chart_days:
        max_v = max(d["intakes"] for d in chart_days) or 1
        chart_lines = []
        for d in chart_days:
            filled = round(d["intakes"] / max_v * bar_width)
            bar = "█" * filled + "░" * (bar_width - filled)
            chart_lines.append(f"{d['d']:<5}  {bar}  {d['intakes']}")
        mini_chart = "```\n" + "\n".join(chart_lines) + "\n```"
    else:
        mini_chart = ""

    # Top 3 DMAs
    sorted_dmas = sorted(metrics.get("all_dmas", {}).items(), key=lambda x: -x[1])
    top3 = "  ·  ".join(f"{d} {n}" for d, n in sorted_dmas[:3] if n > 0)

    within_pct = f"{100 * metrics['within_250'] / metrics['total']:.0f}%" if metrics["total"] else "—"
    overall_cvr_str = f"{metrics['overall_cvr']*100:.1f}%" if metrics["overall_cvr"] else "—"

    # ── Blocks ─────────────────────────────────────────────────────────────
    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{metrics['campaign']} Campaign — Daily Performance Snapshot*\n_{metrics['campaign_id']}  ·  through {end_date}_"}
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Yesterday*\n{yesterday_intakes} intake{'s' if yesterday_intakes != 1 else ''}"},
                {"type": "mrkdwn", "text": f"*7d CVR*\n{cvr_str}"},
                {"type": "mrkdwn", "text": f"*L30 Intakes*\n{metrics['total']}"},
                {"type": "mrkdwn", "text": f"*L30 CVR*\n{overall_cvr_str}"},
                {"type": "mrkdwn", "text": f"*Within 250 mi*\n{metrics['within_250']} ({within_pct})"},
                {"type": "mrkdwn", "text": f"*Home Market*\n{metrics['home_dma']} — {metrics['home_n']}"},
            ]
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"*Top DMAs:* {top3}"},
            ]
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Last 10 days — daily intakes*\n{mini_chart}"}
        },
    ]

    ta_data = metrics.get("ta_data", [])
    if ta_data:
        ta_total = sum(d["n"] for d in ta_data)
        ta_lines = "  ·  ".join(
            f"{d['ta']} {d['n']} ({100*d['n']//ta_total}%)" for d in ta_data[:6]
        )
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"*TA:* {ta_lines}"}]
        })

    print(f"  Posting to Slack {channel}...")
    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers=slack_headers,
        json={"channel": channel, "blocks": blocks, "unfurl_links": False},
    )
    resp = r.json()
    if not resp.get("ok"):
        print(f"  WARNING: Slack message failed: {resp.get('error')}", file=sys.stderr)
        return
    print(f"  Message posted.")


if __name__ == "__main__":
    main()
