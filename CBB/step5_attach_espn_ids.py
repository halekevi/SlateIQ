#!/usr/bin/env python3
"""
step5_attach_espn_ids.py  (REVISED - local master lookup)
----------------------------------------------------------
Uses data/ncaa_mbb_athletes_master.csv instead of ESPN API calls.
Matches on player_norm + team_abbr for best accuracy.
Falls back to player_norm only if team match fails.
"""

import argparse
import re
import unicodedata
import pandas as pd
from pathlib import Path

MASTER_PATH = "data/ncaa_mbb_athletes_master.csv"

def norm_name(s: str) -> str:
    """Canonical player name normalizer — must match cbb_step2_normalize.norm_str() exactly.
    NFKD first (strips accents: é→e, ü→u), then lower + strip non-alphanumeric.
    """
    s = (s or "")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",  required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--master", default=MASTER_PATH)
    args = ap.parse_args()

    df = pd.read_csv(args.input, dtype=str).fillna("")
    print(f"-> Loaded slate: {args.input} | rows={len(df)}")

    master_path = Path(args.master)
    if not master_path.exists():
        raise SystemExit(f"Master file not found: {master_path}")

    master = pd.read_csv(master_path, dtype=str).fillna("")
    print(f"-> Loaded master: {master_path} | rows={len(master)}")

    # Normalize master — re-apply norm_name() to athlete_name_norm so keys match
    # even though master was originally built with a slightly different normalizer
    # (which kept hyphens/apostrophes). This ensures Trey Kaufman-Renn, Tre'Von
    # Spillers, etc. all match correctly.
    master["_name_norm"] = master["athlete_name_norm"].astype(str).apply(norm_name)
    master["_team_norm"] = master["team_abbr"].str.strip().str.upper()

    # Build lookup dicts
    # Primary: (name_norm, team_abbr) -> (team_id, espn_athlete_id)
    map_name_team: dict = {}
    # Fallback: name_norm -> (team_id, espn_athlete_id)
    map_name_only: dict = {}

    for _, r in master.iterrows():
        n = r["_name_norm"]
        t = r["_team_norm"]
        tid = r["team_id"]
        aid = r["espn_athlete_id"]
        if n and t:
            map_name_team[(n, t)] = (tid, aid)
        if n and n not in map_name_only:
            map_name_only[n] = (tid, aid)

    # Detect columns
    player_col = next((c for c in ["player_norm", "player", "Player", "player_name"] if c in df.columns), None)
    team_col   = next((c for c in ["team_abbr", "pp_team", "team", "Team"] if c in df.columns), None)

    if not player_col:
        raise SystemExit(f"No player column found. Columns: {list(df.columns)}")

    team_ids     = []
    athlete_ids  = []
    statuses     = []

    for _, row in df.iterrows():
        raw_name = str(row.get(player_col, "")).strip()
        raw_team = str(row.get(team_col, "")).strip().upper() if team_col else ""
        n = norm_name(raw_name)
        t = raw_team

        # Try name + team first
        result = map_name_team.get((n, t))
        if result:
            team_ids.append(result[0])
            athlete_ids.append(result[1])
            statuses.append("OK_TEAM")
            continue

        # Fallback: name only
        result = map_name_only.get(n)
        if result:
            team_ids.append(result[0])
            athlete_ids.append(result[1])
            statuses.append("OK_NAME")
            continue

        # No match
        team_ids.append("")
        athlete_ids.append("")
        statuses.append("NO_MATCH")

    df["team_id"]         = team_ids
    df["espn_athlete_id"] = athlete_ids
    df["attach_status"]   = statuses

    df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"\nSaved -> {args.output} | rows={len(df)}")
    print("\nattach_status breakdown:")
    print(df["attach_status"].value_counts().to_string())

if __name__ == "__main__":
    main()
