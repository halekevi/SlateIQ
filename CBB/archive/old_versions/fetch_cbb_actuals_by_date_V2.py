#!/usr/bin/env python3
"""
fetch_cbb_actuals_by_date_V2.py

V2 fix: ESPN boxscore athlete "stats" sometimes is not a simple list of strings.
This version supports:
- stats as list[str] aligned with labels
- stats as list[dict] aligned with labels (uses displayValue/value)
- stats as dict keyed by label/key

Usage (one line):
py -3.14 .\fetch_cbb_actuals_by_date_V2.py --slate step5b_cbb.csv --out cbb_actuals.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={yyyymmdd}"
SUMMARY_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={event_id}"

STAT_ALIASES = {
    "MIN":  ["MIN", "MINUTES"],
    "PTS":  ["PTS", "POINTS"],
    "REB":  ["REB", "REBOUNDS", "TRB"],
    "AST":  ["AST", "ASSISTS"],
    "STL":  ["STL", "STEALS"],
    "BLK":  ["BLK", "BLOCKS"],
    "TO":   ["TO", "TOV", "TURNOVERS"],
    "FG":   ["FG", "FGM"],
    "FGA":  ["FGA"],
    "3PT":  ["3PT", "3PM", "FG3M"],
    "3PTA": ["3PTA", "3PA", "FG3A"],
    "FT":   ["FT", "FTM"],
    "FTA":  ["FTA"],
}

def http_get_json(url: str, timeout: int = 30, retries: int = 4, backoff: float = 0.8) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as e:
            last_err = e
            time.sleep(backoff * (attempt + 1))
    raise RuntimeError(f"Failed GET {url} after {retries} retries. Last error: {last_err}")

def infer_date_from_slate(slate_path: str) -> str:
    import pandas as pd
    df = pd.read_csv(slate_path, dtype=str).fillna("")
    s0 = df["start_time"].astype(str).loc[df["start_time"].astype(str).str.strip() != ""].iloc[0]
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s0)
    if not m:
        raise RuntimeError(f"Could not parse date from start_time='{s0}'")
    return m.group(1)

def yyyymmdd(date_yyyy_mm_dd: str) -> str:
    return datetime.strptime(date_yyyy_mm_dd, "%Y-%m-%d").strftime("%Y%m%d")

def norm_label(x: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(x).upper()).strip()

def to_float(x: Any) -> Optional[float]:
    try:
        s = str(x).strip()
        if s == "" or s.lower() == "none":
            return None
        return float(s)
    except Exception:
        return None

def parse_minutes(x: Any) -> Optional[float]:
    s = str(x).strip()
    if not s:
        return None
    if ":" in s:
        mm, ss = s.split(":", 1)
        try:
            return float(mm) + float(ss) / 60.0
        except Exception:
            return None
    return to_float(s)

def extract_events(scoreboard: Dict[str, Any]) -> List[str]:
    ids = []
    for e in scoreboard.get("events", []) or []:
        eid = str(e.get("id", "")).strip()
        if eid:
            ids.append(eid)
    return ids

def stats_value_map(labels: List[Any], stats: Any) -> Dict[str, Any]:
    """
    Build map of normalized label -> raw value (string/number), supporting multiple ESPN shapes.
    """
    lbls = [norm_label(l) for l in (labels or [])]

    # Case 1: stats is dict keyed by label/key
    if isinstance(stats, dict):
        out = {}
        for k, v in stats.items():
            out[norm_label(k)] = v
        return out

    # Case 2: stats is list aligned with labels
    if isinstance(stats, list):
        out = {}
        for i, lab in enumerate(lbls):
            if i >= len(stats):
                break
            v = stats[i]
            if isinstance(v, dict):
                v = v.get("displayValue", v.get("value", v.get("display", "")))
            out[lab] = v
        return out

    return {}

def get_stat(vmap: Dict[str, Any], key: str) -> Optional[float]:
    for alias in STAT_ALIASES.get(key, []):
        v = vmap.get(norm_label(alias))
        if v is None:
            continue
        if key == "MIN":
            return parse_minutes(v)
        return to_float(v)
    return None

def read_slate_ids(slate_path: str) -> Set[str]:
    import pandas as pd
    df = pd.read_csv(slate_path, dtype=str).fillna("")
    if "espn_athlete_id" not in df.columns:
        return set()
    s = set(df["espn_athlete_id"].astype(str).str.strip())
    s.discard("")
    return s

def extract_player_rows(summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    box = summary.get("boxscore", {}) or {}
    teams = box.get("players", []) or []
    if not isinstance(teams, list):
        return out

    for team_block in teams:
        team = (team_block or {}).get("team", {}) or {}
        team_abbr = str(team.get("abbreviation", "")).strip()

        stats_groups = (team_block or {}).get("statistics", []) or []
        if not isinstance(stats_groups, list):
            continue

        for group in stats_groups:
            labels = group.get("labels") or group.get("keys") or []
            athletes = group.get("athletes") or []
            if not isinstance(athletes, list):
                continue

            for a in athletes:
                athlete = (a or {}).get("athlete", {}) or {}
                aid = str(athlete.get("id", "")).strip()
                name = str(athlete.get("displayName", athlete.get("fullName", ""))).strip()
                if not aid:
                    continue

                stats = a.get("stats") if isinstance(a, dict) else None
                if stats is None and isinstance(a, dict):
                    stats = a.get("statistics")
                vmap = stats_value_map(labels, stats)

                row = {
                    "team_abbr": team_abbr,
                    "espn_athlete_id": aid,
                    "player_name": name,
                    "MIN": get_stat(vmap, "MIN"),
                    "PTS": get_stat(vmap, "PTS"),
                    "REB": get_stat(vmap, "REB"),
                    "AST": get_stat(vmap, "AST"),
                    "STL": get_stat(vmap, "STL"),
                    "BLK": get_stat(vmap, "BLK"),
                    "TO":  get_stat(vmap, "TO"),
                    "FG":  get_stat(vmap, "FG"),
                    "FGA": get_stat(vmap, "FGA"),
                    "3PT": get_stat(vmap, "3PT"),
                    "3PTA": get_stat(vmap, "3PTA"),
                    "FT":  get_stat(vmap, "FT"),
                    "FTA": get_stat(vmap, "FTA"),
                }
                out.append(row)

    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="", help="YYYY-MM-DD (optional; inferred from --slate if omitted)")
    ap.add_argument("--slate", default="", help="Slate CSV (optional; used to infer date and optionally filter athletes)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--only_slate_players", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.25)
    args = ap.parse_args()

    date_str = args.date.strip() or (infer_date_from_slate(args.slate) if args.slate else "")
    if not date_str:
        raise RuntimeError("Provide --date or --slate")

    d8 = yyyymmdd(date_str)
    scoreboard = http_get_json(SCOREBOARD_URL.format(yyyymmdd=d8))
    event_ids = extract_events(scoreboard)
    if not event_ids:
        raise RuntimeError(f"No events found for {date_str}")

    slate_ids: Set[str] = set()
    if args.only_slate_players and args.slate:
        slate_ids = read_slate_ids(args.slate)

    rows: List[Dict[str, Any]] = []
    for i, eid in enumerate(event_ids, start=1):
        summary = http_get_json(SUMMARY_URL.format(event_id=eid))
        pr = extract_player_rows(summary)
        if slate_ids:
            pr = [r for r in pr if r["espn_athlete_id"] in slate_ids]
        for r in pr:
            r["date"] = date_str
            r["event_id"] = eid
            rows.append(r)
        if i % 10 == 0 or i == len(event_ids):
            print(f"[{i}/{len(event_ids)}] events processed | rows so far: {len(rows)}")
        time.sleep(max(0.0, args.sleep))

    # De-dupe by athlete (keep max minutes if duplicates)
    def mval(r): return r.get("MIN") or 0.0
    best: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        aid = r["espn_athlete_id"]
        if aid not in best or (mval(r) or 0) > (mval(best[aid]) or 0):
            best[aid] = r

    fieldnames = ["date","event_id","team_abbr","espn_athlete_id","player_name",
                  "MIN","PTS","REB","AST","STL","BLK","TO","FG","FGA","3PT","3PTA","FT","FTA"]
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in best.values():
            w.writerow({k: r.get(k, "") for k in fieldnames})

    print(f"✅ Wrote actuals: {args.out} | rows: {len(best)} | events: {len(event_ids)} | date: {date_str}")

if __name__ == "__main__":
    main()
