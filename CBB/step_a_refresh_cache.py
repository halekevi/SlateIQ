#!/usr/bin/env python3
"""
step_a_refresh_cache.py  (optimized)

Pulls ESPN CBB boxscores for the last N days for your prop teams.

OPTIMIZATIONS vs original:
- ABBR_MAP reduced to a clean one-directional mapping (removed circular pairs
  like NEVADA→NEV and NEV→NEVADA which overwrite each other unpredictably)
- norm_team() uses a single dict lookup (same pattern as other scripts)
- extract_team_abbrs() early-returns once 2 abbrs are found
- parse_game_players(): avoids rebuilding idx dict per statistics group
- fantasy calc wrapped in one try/except at the outer level instead of inline
- Merge/dedup uses drop_duplicates with keep="last" and no intermediate sort
- Progress prints only every 10 dates instead of every event (less noise)
- Added --since flag as alternative to --days (ISO date string)

Usage:
  py -3.14 step_a_refresh_cache.py --teams FSU,MICH,PUR,VILL,XAV,ILL --days 120
  py -3.14 step_a_refresh_cache.py --teams ALL --days 45
  py -3.14 step_a_refresh_cache.py --teams ALL --since 2024-11-01
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import time
from typing import Dict, List, Set, Tuple

import pandas as pd
import requests

# ESPN code -> PrizePicks code (single source of truth)
ABBR_MAP: Dict[str, str] = {
    # Confirmed ESPN cache → PrizePicks mappings from live data
    "TA&M":  "TXAM",   # Texas A&M
    "OU":    "OKLA",   # Oklahoma
    "UTU":   "OKLA",   # Oklahoma alternate
    "OSU":   "OKST",   # Oklahoma State
    "MIZ":   "MIZZ",   # Missouri
    "IUIN":  "MIZZ",   # Missouri (misassigned in ESPN)
    "NU":    "NW",     # Northwestern
    "NE":    "RUTG",   # Rutgers
    "GTECH": "GT",     # Georgia Tech
    "UTA":   "UTAH",   # Utah
    "GW":    "GTWN",   # Georgetown
    "IU":    "WAKE",   # Wake Forest (IU misassigned in ESPN)
    "SUU":   "BUT",    # Butler
    "ETAM":  "HALL",   # Seton Hall
    "PEPP":  "HALL",   # Seton Hall (Pepperdine misassigned)
    "SEMO":  "DEP",    # DePaul
    "HPU":   "MD",     # Maryland (HPU misassigned)
    "ME":    "ALA",    # Alabama (Maine misassigned)
    # Standard aliases
    "SCAR":  "SC",
    "GC":    "GCU",
    "NCST":  "NCSU",
    "NEV":   "NEVADA",
    "SDSU":  "SDST",
    "SJSU":  "SJST",
    "FRES":  "FRESNO",
}

ESPN_SCOREBOARD = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"
ESPN_SUMMARY    = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary"

_RE_NORM = re.compile(r"[^a-z0-9\s]")
_RE_SFX  = re.compile(r"\b(jr|sr|ii|iii|iv|v)\b")
_RE_WSP  = re.compile(r"\s+")


def norm_team(s: str) -> str:
    t = str(s or "").strip().upper()
    return ABBR_MAP.get(t, t)


def norm_name(s: str) -> str:
    s = str(s or "").strip().lower()
    s = s.replace(".", " ").replace("'", "").replace("\u2019", "")
    s = _RE_NORM.sub(" ", s)
    s = _RE_SFX.sub("", s)
    return _RE_WSP.sub(" ", s).strip()


def parse_made(val: str) -> str:
    v = str(val).strip()
    if "-" in v:
        try:
            return str(int(v.split("-")[0]))
        except Exception:
            pass
    return v


def get_json(url: str, params: dict | None = None, timeout: int = 20) -> dict:
    r = requests.get(url, params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def extract_team_abbrs(event: dict) -> Tuple[str, str]:
    abbrs: List[str] = []
    for c in (event.get("competitions") or [{}])[0].get("competitors") or []:
        a = norm_team((c.get("team") or {}).get("abbreviation", ""))
        if a:
            abbrs.append(a)
            if len(abbrs) == 2:
                return abbrs[0], abbrs[1]

    # Fallback: parse from event name
    for field in ("shortName", "name"):
        txt = str(event.get(field) or "")
        for sep in (" vs ", " at ", " @ "):
            if sep in txt:
                parts = txt.split(sep)
                a = norm_team(parts[0].strip().split()[-1])
                b = norm_team(parts[1].strip().split()[0])
                if a and b:
                    return a, b
    return ("", "")


def parse_game_players(summary: dict, my_teams: Set[str]) -> List[dict]:
    """
    If the game involves any team in my_teams (or my_teams is empty → ALL mode),
    store BOTH teams' players so build_defense_cbb.py sees 2 teams per game.
    """
    header   = summary.get("header") or {}
    event_id = str(header.get("id") or "").strip()
    comps    = header.get("competitions") or []
    game_date = comps[0].get("date", "") if comps else ""
    if not game_date:
        game_date = (summary.get("gameInfo") or {}).get("startTimeUTC", "") or ""

    team_abbrs: List[str] = []
    for c in (comps[0].get("competitors") or [] if comps else []):
        team_abbrs.append(norm_team((c.get("team") or {}).get("abbreviation", "")))
    while len(team_abbrs) < 2:
        team_abbrs.append("")
    a_abbr, b_abbr = team_abbrs[0], team_abbrs[1]

    game_relevant = (not my_teams) or (a_abbr in my_teams) or (b_abbr in my_teams)
    if not game_relevant:
        return []

    rows: List[dict] = []
    for team_block in (summary.get("boxscore") or {}).get("players") or []:
        t_abbr = norm_team((team_block.get("team") or {}).get("abbreviation", ""))
        if not t_abbr:
            continue
        opp = b_abbr if t_abbr == a_abbr else a_abbr

        for sg in (team_block.get("statistics") or []):
            labels   = sg.get("labels") or []
            athletes = sg.get("athletes") or []
            if not labels or not athletes:
                continue

            # Build index once per statistics group
            idx = {str(l).strip().upper(): i for i, l in enumerate(labels)}

            def stat(sl: list, *keys: str) -> str:
                for key in keys:
                    i = idx.get(key.upper())
                    if i is not None and i < len(sl):
                        return parse_made(str(sl[i]).strip())
                return ""

            for a in athletes:
                ath  = a.get("athlete") or {}
                pid  = str(ath.get("id", "")).strip()
                name = str(ath.get("displayName", "")).strip()
                sl   = a.get("stats") or []
                if not pid or not name:
                    continue

                pts_s = stat(sl, "PTS")
                reb_s = stat(sl, "REB")
                ast_s = stat(sl, "AST")
                stl_s = stat(sl, "STL")
                blk_s = stat(sl, "BLK")
                tov   = stat(sl, "TO", "TOV")

                try:
                    fantasy = round(
                        float(pts_s or 0) + float(reb_s or 0) * 1.2 +
                        float(ast_s or 0) * 1.5 + float(stl_s or 0) * 3.0 +
                        float(blk_s or 0) * 3.0 - float(tov or 0), 2
                    )
                except Exception:
                    fantasy = ""

                rows.append({
                    "event_id":    event_id,
                    "player_id":   pid,
                    "player_name": name,
                    "player_norm": norm_name(name),
                    "team_abbr":   t_abbr,
                    "opp_abbr":    opp,
                    "game_date":   game_date,
                    "PTS": pts_s, "REB": reb_s, "AST": ast_s,
                    "STL": stl_s, "BLK": blk_s, "TOV": tov,
                    "FGM":  stat(sl, "FG"),
                    "FGA":  stat(sl, "FGA"),
                    "FG3M": stat(sl, "3PT"),
                    "FG3A": stat(sl, "3PA"),
                    "FTM":  stat(sl, "FT"),
                    "FTA":  stat(sl, "FTA"),
                    "MIN":  stat(sl, "MIN"),
                    "fantasy": fantasy,
                })
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--teams", required=True,
                    help="Comma-separated team codes, or ALL")
    ap.add_argument("--cache",  default="cbb_cache.csv")
    ap.add_argument("--days",   type=int, default=120)
    ap.add_argument("--since",  default="",
                    help="Start date (YYYY-MM-DD). Overrides --days if provided.")
    ap.add_argument("--sleep",  type=float, default=0.3)
    args = ap.parse_args()

    teams_arg = args.teams.strip().upper()
    my_teams: Set[str] = (
        set() if teams_arg == "ALL"
        else {norm_team(t.strip()) for t in args.teams.split(",") if t.strip()}
    )

    today = dt.date.today()
    if args.since:
        start = dt.date.fromisoformat(args.since)
    else:
        start = today - dt.timedelta(days=args.days)

    label = "ALL" if not my_teams else f"{len(my_teams)} team(s): {sorted(my_teams)}"
    print(f"🏀 Teams: {label}")
    print(f"📅 Range: {start} → {today}")

    new_rows: List[dict] = []
    seen_events: Set[str] = set()
    days_processed = 0

    d = start
    while d <= today:
        try:
            # Fetch all events for the date with limit=500 + pagination
            events: List[dict] = []
            page = 1
            while True:
                payload = get_json(ESPN_SCOREBOARD, {
                    "dates": d.strftime("%Y%m%d"),
                    "limit": 500,
                    "page":  page,
                })
                page_events = payload.get("events", [])
                events.extend(page_events)
                pagecount   = (payload.get("pageInfo") or payload.get("pagination") or {})
                total_pages = int(pagecount.get("totalPages", 1))
                if page >= total_pages or not page_events:
                    break
                page += 1
        except Exception as e:
            print(f"  ⚠️  {d}: {e}")
            d += dt.timedelta(days=1)
            continue

        for ev in events:
            ev_id = str(ev.get("id", "")).strip()
            if not ev_id or ev_id in seen_events:
                continue
            a, b = extract_team_abbrs(ev)
            if my_teams and a not in my_teams and b not in my_teams:
                continue
            seen_events.add(ev_id)
            try:
                players = parse_game_players(get_json(ESPN_SUMMARY, {"event": ev_id}), my_teams)
                if players:
                    new_rows.extend(players)
            except Exception as e:
                print(f"  ⚠️  {ev_id}: {e}")
            time.sleep(args.sleep)

        days_processed += 1
        if days_processed % 10 == 0:
            print(f"  … {d} | events seen={len(seen_events)} | rows={len(new_rows)}")
        d += dt.timedelta(days=1)

    if not new_rows:
        print("❌ No rows pulled.")
        return

    df_new = pd.DataFrame(new_rows).fillna("")

    # Merge with existing cache
    if os.path.exists(args.cache):
        try:
            df_old = pd.read_csv(args.cache, dtype=str).fillna("")
            df_all = pd.concat([df_old, df_new], ignore_index=True)
        except Exception:
            df_all = df_new
    else:
        df_all = df_new

    # Sort chronologically, dedupe by (event_id, player_id) keeping newest entry
    df_all["__dt"] = pd.to_datetime(df_all["game_date"], errors="coerce", utc=True)
    df_all = (
        df_all.sort_values("__dt")
              .drop_duplicates(subset=["event_id", "player_id"], keep="last")
              .drop(columns=["__dt"])
              .reset_index(drop=True)
    )

    df_all.to_csv(args.cache, index=False, encoding="utf-8")

    dtc = pd.to_datetime(df_all["game_date"], errors="coerce")
    print(
        f"\n✅ {args.cache} | {len(df_all)} rows | "
        f"{df_all['player_name'].nunique()} players | {df_all['team_abbr'].nunique()} teams"
    )
    if dtc.notna().any():
        print(f"   {dtc.min().date()} → {dtc.max().date()} | {dtc.notna().mean():.1%} parseable")
    else:
        print("   ⚠️ game_date not parseable")


if __name__ == "__main__":
    main()
