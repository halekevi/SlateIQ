#!/usr/bin/env python3
"""
extract_cbb_slate.py

Extracts the 'CBB Slate' tab from combined_slate_tickets_YYYY-MM-DD.xlsx
and writes a normalized CSV that grade_cbb_full_slate.py can consume.

Adds required columns:
  - prop_norm  (normalized prop name matching grade_cbb_full_slate.py's stat_from_row)
  - line       (from 'Line' column)
  - player     (from 'Player')
  - bet_direction (from 'Dir')
  - pick_type  (from 'Pick Type')
  - tier       (from 'Tier')
  - opp_def_tier (from 'Def Tier')

Usage:
  py -3.14 extract_cbb_slate.py --tickets combined_slate_tickets_2026-02-26.xlsx --out cbb_slate_2026-02-26.csv
"""

import argparse
import re
import unicodedata
import pandas as pd
from pathlib import Path


PROP_NORM_MAP = {
    "points":           "pts",
    "rebounds":         "reb",
    "assists":          "ast",
    "steals":           "stl",
    "blocked shots":    "blk",
    "blocks":           "blk",
    "turnovers":        "tov",
    "3-pt made":        "3pm",
    "3pt made":         "3pm",
    "fantasy score":    "fantasy score",
    "pts+rebs+asts":    "pra",
    "pts+rebs":         "pr",
    "pts+asts":         "pa",
    "rebs+asts":        "ra",
    "blks+stls":        "stocks",
}


def norm_prop(p: str) -> str:
    s = unicodedata.normalize("NFKD", str(p or "")).lower().strip()
    s = re.sub(r"\s+", " ", s)
    return PROP_NORM_MAP.get(s, s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickets", required=True, help="combined_slate_tickets_YYYY-MM-DD.xlsx")
    ap.add_argument("--out",     required=True, help="Output CSV path")
    ap.add_argument("--sheet",   default="CBB Slate", help="Sheet name to extract (default: 'CBB Slate')")
    args = ap.parse_args()

    xl = pd.ExcelFile(args.tickets)
    if args.sheet not in xl.sheet_names:
        raise RuntimeError(f"Sheet '{args.sheet}' not found. Available: {xl.sheet_names}")

    df = xl.parse(args.sheet, dtype=str).fillna("")

    # Normalize column names to lowercase-stripped
    df.columns = [c.strip() for c in df.columns]

    # Map ticket columns -> grader expected columns
    col_map = {
        "Player":     "player",
        "Team":       "team",
        "Opp":        "opp",
        "Line":       "line",
        "Dir":        "bet_direction",
        "Pick Type":  "pick_type",
        "Tier":       "tier",
        "Def Tier":   "opp_def_tier",
        "Rank Score": "rank_score",
        "Edge":       "edge",
        "Proj":       "proj",
        "Hit Rate":   "hit_rate",
        "Game Time":  "game_time",
    }
    df = df.rename(columns=col_map)

    # Derive prop_norm from Prop column
    if "Prop" in df.columns:
        df["prop_norm"] = df["Prop"].apply(norm_prop)
        df = df.rename(columns={"Prop": "prop_label"})
    elif "prop_label" not in df.columns:
        raise RuntimeError("No 'Prop' column found in sheet.")

    # espn_athlete_id not available from tickets file — grade_cbb will fall back to name matching
    df["espn_athlete_id"] = ""

    # Ensure line is present
    if "line" not in df.columns:
        raise RuntimeError("No 'Line' column found.")

    out = Path(args.out)
    df.to_csv(out, index=False)
    print(f"Extracted {len(df)} rows -> {out}")
    print(f"Prop types: {sorted(df['prop_norm'].unique().tolist())}")


if __name__ == "__main__":
    main()
