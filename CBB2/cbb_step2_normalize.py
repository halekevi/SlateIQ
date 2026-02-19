#!/usr/bin/env python3
"""
cbb_step2_normalize.py
-----------------------
Canonical normalization layer for CBB pipeline.
Takes raw pp_cbb_scraper.py output and guarantees a stable schema
for all downstream steps.

Adds:
- prop_norm          : canonical prop key (pts/reb/ast/pra/pr/pa/ra/stl/blk/stocks/tov/fantasy)
- pick_type          : normalized (Standard/Goblin/Demon)
- cbb_player_key     : espn_athlete_id if present, else player_norm|team_abbr
- team_abbr          : canonical team abbreviation (from pp_team)
- opp_team_abbr      : canonical opp abbreviation (from pp_opp_team)
- player_norm        : lowercased, alphanumeric only

Input : step1_fetch_prizepicks_api_cbb.csv
Output: step2_normalized_cbb.csv
"""

from __future__ import annotations

import argparse
import re

import pandas as pd

PROP_NORM_MAP = {
    # points
    "points": "pts",
    "pts": "pts",
    # rebounds
    "rebounds": "reb",
    "reb": "reb",
    # assists
    "assists": "ast",
    "ast": "ast",
    # combos
    "pts+rebs+asts": "pra",
    "pra": "pra",
    "pts+rebs": "pr",
    "pts+asts": "pa",
    "rebs+asts": "ra",
    # defensive
    "steals": "stl",
    "stl": "stl",
    "blocked shots": "blk",
    "blocks": "blk",
    "blk": "blk",
    "steals+blocks": "stocks",
    "blks+stls": "stocks",
    "stocks": "stocks",
    # turnovers
    "turnovers": "tov",
    "to": "tov",
    "tov": "tov",
    # 3-pointers
    "3-pt made": "3pm",
    "3 pt made": "3pm",
    "3 pointers made": "3pm",
    "threes made": "3pm",
    "3pm": "3pm",
    # fantasy
    "fantasy score": "fantasy",
    "fantasy": "fantasy",
}

TEAM_ALIASES = {
    "GCU": "GC", "NEVADA": "NEV", "SDST": "SDSU",
    "MIZ": "MIZZ", "NCSU": "NCST", "GTECH": "GT",
}


def norm_str(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def norm_team(s: str) -> str:
    t = str(s or "").strip().upper()
    return TEAM_ALIASES.get(t, t)


def norm_pick_type(x: str) -> str:
    t = str(x or "").lower().strip()
    if "gob" in t: return "Goblin"
    if "dem" in t: return "Demon"
    return "Standard"


def norm_prop(s: str) -> str:
    p = str(s or "").lower().strip()
    p = re.sub(r"\s+", " ", p)
    return PROP_NORM_MAP.get(p, p)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",  required=True)
    ap.add_argument("--output", default="step2_normalized_cbb.csv")
    args = ap.parse_args()

    df = pd.read_csv(args.input, dtype=str).fillna("")
    print(f"→ Loaded: {args.input} | rows={len(df)}")

    # player_norm
    player_col = "player" if "player" in df.columns else df.columns[0]
    df["player_norm"] = df[player_col].astype(str).apply(norm_str)

    # team_abbr / opp_team_abbr
    df["team_abbr"]     = df.get("pp_team",     pd.Series([""] * len(df))).astype(str).apply(norm_team)
    df["opp_team_abbr"] = df.get("pp_opp_team", pd.Series([""] * len(df))).astype(str).apply(norm_team)

    # prop_norm — prefer stat_type, fallback to prop_type
    stat_col = "stat_type" if "stat_type" in df.columns else ("prop_type" if "prop_type" in df.columns else "")
    if stat_col:
        df["prop_norm"] = df[stat_col].astype(str).apply(norm_prop)
    else:
        df["prop_norm"] = ""

    # pick_type normalized
    odds_col = "odds_type" if "odds_type" in df.columns else ("pick_type" if "pick_type" in df.columns else "")
    if odds_col:
        df["pick_type"] = df[odds_col].astype(str).apply(norm_pick_type)
    else:
        df["pick_type"] = "Standard"

    # cbb_player_key
    if "espn_athlete_id" in df.columns:
        df["cbb_player_key"] = df.apply(
            lambda r: str(r["espn_athlete_id"]).strip()
            if str(r.get("espn_athlete_id", "")).strip()
            else f"{r['player_norm']}|{r['team_abbr']}",
            axis=1,
        )
    else:
        df["cbb_player_key"] = df["player_norm"] + "|" + df["team_abbr"]

    # line numeric
    df["line"] = pd.to_numeric(df["line"], errors="coerce")

    # Rename prop column to prop_type for consistency downstream
    if "stat_type" in df.columns and "prop_type" not in df.columns:
        df["prop_type"] = df["stat_type"]

    # Guarantee column order: identity first, then prop details, then rest
    identity = ["proj_id", "player_id", "cbb_player_key", "player", "player_norm",
                "team_abbr", "opp_team_abbr", "pp_team", "pp_opp_team", "pos",
                "prop_type", "prop_norm", "pick_type", "line", "start_time",
                "pp_game_id", "league_id"]
    present  = [c for c in identity if c in df.columns]
    rest     = [c for c in df.columns if c not in present]
    df       = df[present + rest]

    df.to_csv(args.output, index=False)
    print(f"✅ Saved → {args.output} | rows={len(df)}")
    print(f"  prop_norm values: {df['prop_norm'].value_counts().to_dict()}")
    print(f"  pick_type values: {df['pick_type'].value_counts().to_dict()}")
    blanks = int((df['opp_team_abbr'].astype(str).str.strip() == "").sum())
    print(f"  opp_team_abbr blank: {blanks}/{len(df)}")


if __name__ == "__main__":
    main()
