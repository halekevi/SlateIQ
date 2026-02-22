#!/usr/bin/env python3
"""
attach_cbb_athlete_ids.py

Merges ESPN athlete IDs from master file into a CBB props file.

Usage:
py -3.14 attach_cbb_athlete_ids.py ^
  --input step6_ranked_cbb.xlsx ^
  --master ncaa_mbb_athletes_master.csv ^
  --output step6_ranked_cbb_with_ids.xlsx
"""

import pandas as pd
import argparse


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--master", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    # Load input
    if args.input.endswith(".xlsx"):
        df = pd.read_excel(args.input)
    else:
        df = pd.read_csv(args.input)

    master = pd.read_csv(args.master)

    # Normalize join keys
    df["player_norm"] = df["player_norm"].astype(str).str.lower().str.strip()
    df["team_abbr"] = df["team_abbr"].astype(str).str.upper().str.strip()

    master["athlete_name_norm"] = master["athlete_name_norm"].astype(str).str.lower().str.strip()
    master["team_abbr"] = master["team_abbr"].astype(str).str.upper().str.strip()

    merged = df.merge(
        master[["espn_athlete_id", "athlete_name_norm", "team_abbr"]],
        left_on=["player_norm", "team_abbr"],
        right_on=["athlete_name_norm", "team_abbr"],
        how="left"
    )

    merged.drop(columns=["athlete_name_norm"], inplace=True)

    print("Matched IDs:", merged["espn_athlete_id"].notna().sum())
    print("Unmatched:", merged["espn_athlete_id"].isna().sum())

    # Save output
    if args.output.endswith(".xlsx"):
        merged.to_excel(args.output, index=False)
    else:
        merged.to_csv(args.output, index=False)

    print("Saved:", args.output)


if __name__ == "__main__":
    main()
