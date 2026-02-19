#!/usr/bin/env python3
"""
step46_team_role_context.py  (Pipeline A - Step 4.6)

Assigns team role buckets based on usage proxies:
- minutes
- attempts / involvement (by stat prefix)
"""

import argparse
import pandas as pd
import numpy as np


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    print("→ Loading:", args.input)
    df = pd.read_csv(args.input)

    # ---- Required columns ----
    required = {
        "team",
        "player",
        "stat_prefix",
        "min_season_avg",
    }

    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}")

    # ---- Normalize minutes ----
    df["min_season_avg"] = pd.to_numeric(
        df["min_season_avg"], errors="coerce"
    ).fillna(0)

    # ---- Team-level minute percent ----
    df["team_minutes"] = df.groupby("team")["min_season_avg"].transform("sum")
    df["minute_share"] = np.where(
        df["team_minutes"] > 0,
        df["min_season_avg"] / df["team_minutes"],
        0
    )

    # ---- Role buckets ----
    def role_bucket(ms):
        if ms >= 0.22:
            return "PRIMARY"
        if ms >= 0.14:
            return "SECONDARY"
        return "ROLE"

    df["role_bucket"] = df["minute_share"].apply(role_bucket)

    df.to_csv(args.output, index=False)
    print(f"✅ Saved → {args.output}")
    print(df["role_bucket"].value_counts())


if __name__ == "__main__":
    main()
