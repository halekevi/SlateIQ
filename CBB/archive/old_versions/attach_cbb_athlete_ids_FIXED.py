#!/usr/bin/env python3
"""
attach_cbb_athlete_ids.py

Merges ESPN athlete IDs from a master roster file into a CBB props file.

Fixes:
- If input already has espn_athlete_id (often blank), pandas merge will create suffixes
  (espn_athlete_id_x / espn_athlete_id_y). This script coalesces them into a single
  espn_athlete_id column.
- Handles minor master-column variations (espn_athlete_id vs athlete_id).

Usage (one line):
py -3.14 .\attach_cbb_athlete_ids.py --input step6_ranked_cbb.xlsx --master ncaa_mbb_athletes_master.csv --output step6_ranked_cbb_with_ids.xlsx
"""

import argparse
import pandas as pd


def _pick_col(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--master", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    # Load input
    if args.input.lower().endswith(".xlsx"):
        df = pd.read_excel(args.input, dtype=str).fillna("")
    else:
        df = pd.read_csv(args.input, dtype=str).fillna("")

    master = pd.read_csv(args.master, dtype=str).fillna("")

    # Required columns in input
    for req in ["player_norm", "team_abbr"]:
        if req not in df.columns:
            raise RuntimeError(f"Input missing required column: {req}")

    # Normalize join keys
    df["player_norm"] = df["player_norm"].astype(str).str.lower().str.strip()
    df["team_abbr"] = df["team_abbr"].astype(str).str.upper().str.strip()

    # Master column selection
    master_id_col = _pick_col(master, ["espn_athlete_id", "athlete_id", "athleteId", "espnAthleteId"])
    master_name_col = _pick_col(master, ["athlete_name_norm", "athleteNameNorm", "athlete_name_normalized"])
    master_team_col = _pick_col(master, ["team_abbr", "teamAbbr", "abbr"])

    if not master_id_col or not master_name_col or not master_team_col:
        raise RuntimeError(
            "Master file missing required columns. Need an athlete id col and athlete_name_norm + team_abbr. "
            f"Found columns: {list(master.columns)[:40]}..."
        )

    master[master_name_col] = master[master_name_col].astype(str).str.lower().str.strip()
    master[master_team_col] = master[master_team_col].astype(str).str.upper().str.strip()

    # Merge (use suffixes so we can coalesce safely)
    merged = df.merge(
        master[[master_id_col, master_name_col, master_team_col]].rename(columns={
            master_id_col: "espn_athlete_id_master",
            master_name_col: "athlete_name_norm_master",
            master_team_col: "team_abbr_master",
        }),
        left_on=["player_norm", "team_abbr"],
        right_on=["athlete_name_norm_master", "team_abbr_master"],
        how="left",
    )

    # Coalesce ID: prefer existing non-blank, else master
    existing_col = _pick_col(merged, ["espn_athlete_id"])
    if existing_col is None:
        # maybe got created earlier in some pipeline with different name
        existing_col = ""

    if existing_col and existing_col in merged.columns:
        merged["espn_athlete_id"] = merged[existing_col].astype(str).str.strip()
        merged.loc[merged["espn_athlete_id"].eq(""), "espn_athlete_id"] = merged["espn_athlete_id_master"].astype(str).str.strip()
    else:
        merged["espn_athlete_id"] = merged["espn_athlete_id_master"].astype(str).str.strip()

    # Cleanup helper cols
    for c in ["espn_athlete_id_master", "athlete_name_norm_master", "team_abbr_master"]:
        if c in merged.columns:
            merged.drop(columns=[c], inplace=True)

    matched = (merged["espn_athlete_id"].astype(str).str.strip() != "").sum()
    unmatched = len(merged) - matched
    print("Matched IDs:", matched)
    print("Unmatched:", unmatched)

    # Save
    if args.output.lower().endswith(".xlsx"):
        merged.to_excel(args.output, index=False)
    else:
        merged.to_csv(args.output, index=False)

    print("Saved:", args.output)


if __name__ == "__main__":
    main()
