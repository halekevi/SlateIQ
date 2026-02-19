#!/usr/bin/env python3
"""
step5_attach_espn_ids.py  (CBB2)

Takes fresh PrizePicks CBB props (from pp_cbb_scraper.py) and attaches:
- team_id (ESPN team id) using ESPN teams endpoint (abbr/name mapping)
- player_norm
- espn_athlete_id using ESPN search (cached)

Input : step1_pp_props_today.csv
Output: step5_with_espn_ids.csv

Status:
- OK
- NO_TEAM_MATCH
- NO_ATHLETE_MATCH
"""

from __future__ import annotations

import argparse
import re
import time
from typing import Dict, Optional, Tuple

import pandas as pd
import requests

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json, text/plain, */*"}

ESPN_TEAMS_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams"
ESPN_SEARCH_URL = "https://site.web.api.espn.com/apis/common/v3/search"


def norm(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def req_json(url: str, params: dict | None = None, sleep: float = 0.0) -> dict:
    if sleep:
        time.sleep(sleep)
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def build_team_map() -> Dict[str, str]:
    """
    Returns map from normalized keys to ESPN team id.
    Keys include:
      - abbreviation (exact)
      - displayName / shortDisplayName (exact)
      - nickname / location words for fuzzy CBB matching
        e.g. "duke blue devils" -> keys "duke", "blue devils", "duke blue devils"
    """
    j = req_json(ESPN_TEAMS_URL, params={"limit": "5000"})
    teams = (j.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", [])) if isinstance(j, dict) else []
    out: Dict[str, str] = {}

    for t in teams:
        team = (t.get("team") or {})
        tid = str(team.get("id", "")).strip()
        if not tid:
            continue

        abbr   = norm(team.get("abbreviation") or "")
        name   = norm(team.get("displayName") or "")
        sname  = norm(team.get("shortDisplayName") or "")
        loc    = norm(team.get("location") or "")
        nick   = norm(team.get("name") or "")  # nickname e.g. "Blue Devils"

        for k in (abbr, name, sname, loc, nick):
            if k:
                out[k] = tid

        # Also index first word of displayName (e.g. "duke" from "duke blue devils")
        if name:
            first_word = name.split()[0] if name.split() else ""
            if first_word and len(first_word) >= 3:
                out.setdefault(first_word, tid)

    return out


def resolve_team_id(raw_team: str, team_map: Dict[str, str]) -> str:
    """Try multiple normalizations to find a team_id match."""
    if not raw_team:
        return ""
    # Direct normalized match
    key = norm(raw_team)
    if key in team_map:
        return team_map[key]
    # Try each word (catches "DUKE BLUE DEVILS" -> "duke")
    for word in key.split():
        if len(word) >= 3 and word in team_map:
            return team_map[word]
    # Try first two words
    words = key.split()
    if len(words) >= 2:
        two = " ".join(words[:2])
        if two in team_map:
            return team_map[two]
    return ""


def search_athlete_id(player: str, team: str, team_id: str, cache: Dict[Tuple[str, str], str]) -> str:
    """
    Uses ESPN search. Caches by (player_norm, team_norm).
    Returns espn athlete id or "".
    """
    pn = norm(player)
    tn = norm(team)
    key = (pn, tn)
    if key in cache:
        return cache[key]

    q = player
    if team:
        q = f"{player} {team}"

    try:
        j = req_json(
            ESPN_SEARCH_URL,
            params={"query": q, "limit": "10"},
            sleep=0.10,
        )
    except Exception:
        cache[key] = ""
        return ""

    # Flatten all result contents across all result groups
    all_items = []
    for result_group in (j.get("results") or []):
        all_items.extend(result_group.get("contents") or [])

    best_id = ""
    for it in all_items:
        # Accept athlete, player, collegeplayer, collegeathletes
        ctype = str(it.get("type") or "").lower()
        if not any(t in ctype for t in ("athlete", "player")):
            continue
        cid = str(it.get("id") or "").strip()
        if not cid:
            continue
        # Name match: last name must appear in result
        nm = norm((it.get("displayName") or it.get("name") or ""))
        last = pn.split()[-1] if pn.split() else ""
        if last and nm and last in nm:
            best_id = cid
            break

    cache[key] = best_id
    return best_id


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", default="step5_with_espn_ids.csv")
    args = ap.parse_args()

    df = pd.read_csv(args.input, dtype=str).fillna("")
    if df.empty:
        print("❌ Input empty. Nothing to do.")
        df.to_csv(args.output, index=False)
        print("✅ Saved →", args.output)
        return

    # expected cols from pp_cbb_scraper: player, team (raw), pp_team (normalized), pp_opp_team, prop_type, line, pick_type, etc.
    if "player_norm" not in df.columns:
        df["player_norm"] = df["player"].astype(str).apply(norm)

    # Build team map from ESPN
    print("→ Loading ESPN teams…")
    team_map = build_team_map()
    print("→ ESPN team keys:", len(team_map))

    # Decide which team field to use for matching
    team_col = "pp_team" if "pp_team" in df.columns else ("team" if "team" in df.columns else "")
    if not team_col:
        df["team_id"] = ""
        df["espn_athlete_id"] = ""
        df["status"] = "NO_TEAM_COL"
        df.to_csv(args.output, index=False)
        print("✅ Saved →", args.output)
        print(df["status"].value_counts(dropna=False).to_string())
        return

    # Attach team_id using robust multi-strategy lookup
    df["team_id"] = df[team_col].astype(str).apply(lambda x: resolve_team_id(x, team_map))

    # Attach athlete_id via ESPN search (cached)
    cache: Dict[Tuple[str, str], str] = {}
    athlete_ids = []
    status = []
    for _, r in df.iterrows():
        tid = str(r.get("team_id", "")).strip()
        player = str(r.get("player", "")).strip()
        team = str(r.get(team_col, "")).strip()

        if not tid:
            athlete_ids.append("")
            status.append("NO_TEAM_MATCH")
            continue

        # Skip ESPN search if we already have espn_athlete_id from scraper
        existing_aid = str(r.get("espn_athlete_id", "")).strip()
        if existing_aid and existing_aid not in ("", "nan"):
            athlete_ids.append(existing_aid)
            status.append("OK")
            continue

        aid = search_athlete_id(player=player, team=team, team_id=tid, cache=cache)
        athlete_ids.append(aid)
        status.append("OK" if aid else "NO_ATHLETE_MATCH")

    df["espn_athlete_id"] = athlete_ids
    df["status"] = status

    df.to_csv(args.output, index=False)
    print("✅ Saved →", args.output)
    print(df["status"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()
