#!/usr/bin/env python3
"""
extract_soccer_slate.py

Extracts the 'Soccer Slate' tab from combined_slate_tickets_YYYY-MM-DD.xlsx
and writes a normalized CSV that slate_grader.py (--sport Soccer) can consume.

Maps ticket columns -> grader expected columns:
  Player       -> player
  Team         -> team
  Opp          -> opp
  Prop         -> prop_label  (+ prop_norm derived)
  Pick Type    -> pick_type
  Line         -> line
  Dir          -> bet_direction
  Tier         -> tier
  Def Tier     -> opp_def_tier
  Edge         -> edge
  Rank Score   -> rank_score
  Game Time    -> game_time

Usage:
  py -3.14 extract_soccer_slate.py --tickets combined_slate_tickets_2026-03-06.xlsx --out soccer_slate_2026-03-06.csv
"""

import argparse
import re
import unicodedata
import pandas as pd
from pathlib import Path


PROP_NORM_MAP = {
    "shots on target":  "sot",
    "shots":            "shots",
    "goals":            "goals",
    "assists":          "assists",
    "goalkeeper saves": "saves",
    "saves":            "saves",
    "passes":           "passes",
    "key passes":       "key_passes",
    "tackles":          "tackles",
    "fouls":            "fouls",
    "yellow cards":     "yellow_cards",
}


def norm_prop(p: str) -> str:
    s = unicodedata.normalize("NFKD", str(p or "")).lower().strip()
    s = re.sub(r"\s+", " ", s)
    return PROP_NORM_MAP.get(s, s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickets", required=True, help="combined_slate_tickets_YYYY-MM-DD.xlsx")
    ap.add_argument("--out",     required=True, help="Output CSV path")
    ap.add_argument("--sheet",   default="Soccer Slate", help="Sheet name to extract (default: 'Soccer Slate')")
    args = ap.parse_args()

    xl = pd.ExcelFile(args.tickets)
    if args.sheet not in xl.sheet_names:
        raise RuntimeError(f"Sheet '{args.sheet}' not found. Available: {xl.sheet_names}")

    df = xl.parse(args.sheet, dtype=str).fillna("")
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

    if "line" not in df.columns:
        raise RuntimeError("No 'Line' column found.")

    out = Path(args.out)
    df.to_csv(out, index=False)
    print(f"Extracted {len(df)} rows -> {out}")
    print(f"Prop types: {sorted(df['prop_norm'].unique().tolist())}")


if __name__ == "__main__":
    main()
