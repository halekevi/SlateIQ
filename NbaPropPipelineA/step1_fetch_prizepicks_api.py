#!/usr/bin/env python3
"""
step1_fetch_prizepicks_api.py  (NBA Pipeline A - upgraded)

Fetches PrizePicks projections from the public API and writes a flat CSV
for downstream pipeline steps.

Key upgrades:
- Supports --league_id (default NBA=7)
- Supports --game_mode (default pickem)
- Pagination with per_page (default 250)
- Derives team/opp_team using included new_player + new_game when available
- Adds pick_type normalized (Standard/Goblin/Demon)
- Board size guard (min_rows/min_teams)

Outputs: step1_fetch_prizepicks_api.csv
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests

API_URL = "https://api.prizepicks.com/projections"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Origin": "https://app.prizepicks.com",
    "Referer": "https://app.prizepicks.com/",
}

PICKTYPE_MAP = {
    "standard": "Standard",
    "goblin": "Goblin",
    "demon": "Demon",
}

def _safe_get(d: dict, path: List[str], default=""):
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur if cur is not None else default

def _norm_team(s: str) -> str:
    return str(s or "").strip().upper()

def _parse_iso(dt_str: str) -> str:
    # keep as original string; downstream can parse
    return (dt_str or "").strip()

def _included_index(included: List[dict]) -> Dict[Tuple[str, str], dict]:
    idx: Dict[Tuple[str, str], dict] = {}
    for obj in included or []:
        t = str(obj.get("type", "")).strip()
        i = str(obj.get("id", "")).strip()
        if t and i:
            idx[(t, i)] = obj
    return idx

def fetch_pages(
    league_id: str,
    game_mode: str,
    per_page: int,
    max_pages: int,
    sleep: float,
) -> Tuple[List[dict], List[dict], List[dict]]:

    all_data: List[dict] = []
    all_included: List[dict] = []
    raw_pages: List[dict] = []

    for page in range(1, max_pages + 1):

        params = {
            "league_id": str(league_id),
            "game_mode": str(game_mode),
            "per_page": int(per_page),
            "page": int(page),
        }

        # retry logic for 429 / 5xx
        for attempt in range(1, 9):
            r = requests.get(API_URL, headers=HEADERS, params=params, timeout=30)

            if r.status_code in (429, 500, 502, 503, 504):
                wait = (2.0 ** (attempt - 1)) + 0.5
                print(f"  ⏳ page {page} attempt {attempt} → status {r.status_code}, retrying in {wait:.1f}s")
                time.sleep(wait)
                continue

            r.raise_for_status()
            break
        else:
            print(f"⛔ Rate-limited / server errors on page {page}. Stopping early.")
            break

        j = r.json()
        raw_pages.append(j)

        data = j.get("data") or []
        included = j.get("included") or []

        if not data:
            break

        all_data.extend(data)
        all_included.extend(included)

        if sleep:
            time.sleep(sleep)

    return all_data, all_included, raw_pages


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default="step1_fetch_prizepicks_api.csv")
    ap.add_argument("--raw_json", default="")
    ap.add_argument("--history", default="")  # optional: write a copy with timestamp
    ap.add_argument("--min_rows", type=int, default=120)
    ap.add_argument("--min_teams", type=int, default=6)

    # NEW
    ap.add_argument("--league_id", default="7")          # NBA = 7
    ap.add_argument("--game_mode", default="pickem")     # props live in pickem
    ap.add_argument("--per_page", type=int, default=500)
    ap.add_argument("--max_pages", type=int, default=80)
    ap.add_argument("--sleep", type=float, default=1.2)

    # Back-compat: allow overriding url, but still default to API_URL
    ap.add_argument("--url", default="")

    args = ap.parse_args()

    url_used = args.url.strip() or API_URL

    print(f"📡 Fetching PrizePicks | league_id={args.league_id} | game_mode={args.game_mode} | per_page={args.per_page}")
    if url_used != API_URL:
        print(f"→ using custom url: {url_used}")

    # If custom URL is used, do a single fetch without pagination (safety).
    if url_used != API_URL:
        r = requests.get(url_used, headers=HEADERS, timeout=30)
        r.raise_for_status()
        j = r.json()
        data = j.get("data") or []
        included = j.get("included") or []
        raw_pages = [j]
    else:
        data, included, raw_pages = fetch_pages(
            league_id=str(args.league_id),
            game_mode=str(args.game_mode),
            per_page=int(args.per_page),
            max_pages=int(args.max_pages),
            sleep=float(args.sleep),
        )

    if args.raw_json:
        try:
            with open(args.raw_json, "w", encoding="utf-8") as f:
                json.dump(raw_pages[-1] if raw_pages else {}, f, ensure_ascii=False)
            print("🧾 raw_json saved →", args.raw_json)
        except Exception as e:
            print("⚠️ raw_json write failed:", e)

    if not data:
        # still write an empty CSV with headers so downstream doesn't KeyError
        cols = [
            "projection_id","pp_projection_id",
            "player_id",
            "pp_game_id","start_time",
            "player","pos","team","opp_team","pp_home_team","pp_away_team",
            "prop_type","line","pick_type",
        ]

        pd.DataFrame(columns=cols).to_csv(args.output, index=False)
        print("❌ No projections fetched. Wrote empty CSV →", args.output)
        return

    inc = _included_index(included)

    out_rows: List[dict] = []

    for d in data:
        if not isinstance(d, dict):
            continue

        pid = str(d.get("id","")).strip()
        attrs = d.get("attributes") or {}
        rel = d.get("relationships") or {}

        # projection basics
        line = attrs.get("line_score", attrs.get("line"))  # different builds use different names
        prop_type = str(attrs.get("stat_type", attrs.get("projection_type", attrs.get("name","")))).strip()
        odds_type = str(attrs.get("odds_type", "")).strip().lower()
        pick_type = PICKTYPE_MAP.get(odds_type, "Standard")

        # relationships to player/game
        player_id = _safe_get(rel, ["new_player", "data", "id"], "") or ""
        player_id_str = str(player_id).strip()
        player_type = _safe_get(rel, ["new_player", "data", "type"], "new_player")
        game_id = _safe_get(rel, ["new_game", "data", "id"], "") or _safe_get(rel, ["game", "data", "id"], "")
        game_type = _safe_get(rel, ["new_game", "data", "type"], "") or _safe_get(rel, ["game", "data", "type"], "")

        player_obj = inc.get((player_type, str(player_id))) if player_id else None
        game_obj = inc.get((game_type, str(game_id))) if game_id and game_type else None

        player_name = ""
        pos = ""
        team = ""
        if isinstance(player_obj, dict):
            pattrs = player_obj.get("attributes") or {}
            player_name = str(pattrs.get("display_name", pattrs.get("name",""))).strip()
            pos = str(pattrs.get("position","")).strip()
            team = _norm_team(pattrs.get("team",""))

        home = away = start_time = ""
        if isinstance(game_obj, dict):
            gattrs = game_obj.get("attributes") or {}
            home = _norm_team(gattrs.get("home_team",""))
            away = _norm_team(gattrs.get("away_team",""))
            start_time = _parse_iso(str(gattrs.get("start_time","")))

        # fallback: try attributes
        if not start_time:
            start_time = _parse_iso(str(attrs.get("start_time","")))

        # derive opp_team if possible
        opp_team = ""
        if team and home and away:
            opp_team = away if team == home else (home if team == away else "")
        else:
            # fallback: try parsing description like "X vs Y" if present
            desc = str(attrs.get("description","") or "")
            m = re.search(r"\bvs\.?\s+([A-Za-z]{2,4})\b", desc)
            if m:
                opp_team = _norm_team(m.group(1))

        out_rows.append({
            "projection_id": pid,
            "pp_projection_id": pid,

            "player_id": player_id_str,

            "pp_game_id": str(game_id or "").strip(),
            "start_time": start_time,
            "player": player_name,
            "pos": pos,
            "team": team,
            "opp_team": opp_team,
            "pp_home_team": home,
            "pp_away_team": away,
            "prop_type": prop_type,
            "line": line,
            "pick_type": pick_type,
        })


    df = pd.DataFrame(out_rows).fillna("")

    # enforce numeric line where possible
    df["line"] = pd.to_numeric(df["line"], errors="coerce")

    rows = len(df)
    teams = len({t for t in df["team"].astype(str).tolist() if t})

    print("Step1 saved →", args.output)
    print("  fetch_method: requests_ok")
    print(f"  rows={rows} teams={teams}")

    df.to_csv(args.output, index=False)

    # Optional history copy
    if args.history:
        try:
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_utc")
            hist_path = args.history.replace("{ts}", ts)
            df.to_csv(hist_path, index=False)
            print("🕘 history saved →", hist_path)
        except Exception as e:
            print("⚠️ history write failed:", e)

    # Board size guard
    if rows < args.min_rows or teams < args.min_teams:
        print(f"⛔ BOARD_TOO_SMALL (min_rows={args.min_rows}, min_teams={args.min_teams})")
    else:
        print("✅ BOARD_OK")

if __name__ == "__main__":
    main()
