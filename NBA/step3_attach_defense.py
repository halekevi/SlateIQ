#!/usr/bin/env python3
"""
step3_attach_defense.py (ROBUST, never drops rows)

Fixes prior dtype assignment crash (LossySetitemError / float64 vs strings) by:
- Reading all CSVs as strings
- Using LEFT merges (never filters to matched-only)
- Building combo leg merges via separate DataFrames then joining back (no unsafe .loc assignment)

Inputs:
  --input   step2_attach_picktypes.csv
  --defense defense_team_summary.csv   (from defense_report.py; MUST include TEAM_ABBREVIATION)
Output:
  --output  step3_with_defense.csv

Behavior:
- Singles: merge opponent defense on opp_team (abbr) -> adds defense columns unsuffixed
- Combos: derives opp_team_1 / opp_team_2, merges defense twice and appends suffixes:
    *_DEF_1  and *_DEF_2
  If opponent cannot be derived, leaves blank defense fields for that leg.

Run:
  py -3.14 step3_attach_defense.py --input step2_attach_picktypes.csv --defense defense_team_summary.csv --output step3_with_defense.csv
"""

from __future__ import annotations

import argparse
import re
from typing import Tuple, Optional, List

import pandas as pd


# ------------------------- helpers -------------------------

def _col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _safe_upper(x: str) -> str:
    return (x or "").strip().upper()


def split_slash_pair(s: str) -> Tuple[str, str]:
    if not s:
        return "", ""
    parts = [p.strip() for p in str(s).split("/")]
    if len(parts) >= 2:
        return parts[0], parts[1]
    return parts[0].strip(), ""


def derive_combo_opponents(row: pd.Series) -> Tuple[str, str]:
    """
    Derive opp_team_1 and opp_team_2 for combo players.

    Priority:
    1) If pp_home_team + pp_away_team exist AND team_1/team_2 exist:
       opp = other of home/away based on each team_i
    2) Else if opp_team looks like "AAA/BBB": split it
    3) Else return ("","")
    """
    team1 = _safe_upper(str(row.get("team_1", "")))
    team2 = _safe_upper(str(row.get("team_2", "")))

    home = _safe_upper(str(row.get("pp_home_team", "")))
    away = _safe_upper(str(row.get("pp_away_team", "")))

    if home and away and team1 and team2:
        opp1 = away if team1 == home else (home if team1 == away else "")
        opp2 = away if team2 == home else (home if team2 == away else "")
        return opp1, opp2

    opp = str(row.get("opp_team", "")).strip()
    if "/" in opp:
        return split_slash_pair(opp)

    return "", ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--defense", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    print(f"→ Loading Step2: {args.input}")
    df = pd.read_csv(args.input, dtype=str, encoding="utf-8-sig").fillna("")

    print(f"→ Loading defense: {args.defense}")
    d = pd.read_csv(args.defense, dtype=str, encoding="utf-8-sig").fillna("")

    # Identify defense key
    key = _col(d, ["TEAM_ABBREVIATION", "team_abbr", "abbr", "TEAM_ABBR"])
    if not key:
        raise RuntimeError(
            f"❌ Defense file missing TEAM_ABBREVIATION. Found columns: {list(d.columns)}"
        )

    # Normalize key values
    d[key] = d[key].astype(str).str.strip().str.upper()

    # Choose defense columns to merge (everything except the key)
    def_cols = [c for c in d.columns if c != key]

    # Ensure opp_team exists
    if "opp_team" not in df.columns:
        df["opp_team"] = ""

    df["opp_team"] = df["opp_team"].astype(str).str.strip().str.upper()

    # Identify singles vs combos
    if "is_combo_player" not in df.columns:
        # assume combos if player contains "+"
        df["is_combo_player"] = df.get("player", "").astype(str).str.contains(r"\+").astype(int)

    singles_mask = df["is_combo_player"].astype(str).isin(["0", "False", "false", ""])
    combos_mask = ~singles_mask

    # --- Singles merge (left join; never drops rows) ---
    print("→ Merging defense for singles on opp_team")
    singles = df.loc[singles_mask].copy()
    singles = singles.merge(d[[key] + def_cols], how="left", left_on="opp_team", right_on=key)
    if key in singles.columns:
        singles = singles.drop(columns=[key])

    # --- Combos merge: derive opp_team_1/2 then join twice with suffixes ---
    combos = df.loc[combos_mask].copy()
    if len(combos) > 0:
        # ensure needed columns exist
        for c in ["team_1", "team_2", "pp_home_team", "pp_away_team"]:
            if c not in combos.columns:
                combos[c] = ""

        opps = combos.apply(derive_combo_opponents, axis=1, result_type="expand")
        opps.columns = ["opp_team_1", "opp_team_2"]
        combos["opp_team_1"] = opps["opp_team_1"].astype(str).str.strip().str.upper()
        combos["opp_team_2"] = opps["opp_team_2"].astype(str).str.strip().str.upper()

        leg1 = combos.merge(d[[key] + def_cols], how="left", left_on="opp_team_1", right_on=key)
        if key in leg1.columns:
            leg1 = leg1.drop(columns=[key])
        leg2 = combos.merge(d[[key] + def_cols], how="left", left_on="opp_team_2", right_on=key)
        if key in leg2.columns:
            leg2 = leg2.drop(columns=[key])

        # Rename defense columns with suffixes
        rename1 = {c: f"{c}_DEF_1" for c in def_cols}
        rename2 = {c: f"{c}_DEF_2" for c in def_cols}
        leg1 = leg1.rename(columns=rename1)
        leg2 = leg2.rename(columns=rename2)

        # Keep only the new columns from each leg and join back by index
        leg1_new = leg1[[c for c in leg1.columns if c.endswith("_DEF_1")]]
        leg2_new = leg2[[c for c in leg2.columns if c.endswith("_DEF_2")]]

        combos = pd.concat([combos.reset_index(drop=True), leg1_new.reset_index(drop=True), leg2_new.reset_index(drop=True)], axis=1)

    # --- Recombine ---
    out = pd.concat([singles, combos], axis=0, ignore_index=True)

    # Column order: keep desired front if present, then rest
    desired_front = ["nba_player_id", "player", "pos", "team", "opp_team", "line", "prop_type", "prop_norm", "pick_type"]
    front = [c for c in desired_front if c in out.columns]
    tail = ["is_combo_player"] if "is_combo_player" in out.columns else []
    middle = [c for c in out.columns if c not in set(front + tail)]
    out = out[front + middle + tail]

    out.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"✅ Saved → {args.output}")
    print(f"Rows: {len(out)} | Cols: {len(out.columns)}")

    # Quick merge health stats
    if "OVERALL_DEF_RANK" in out.columns:
        filled = (out["OVERALL_DEF_RANK"].astype(str).str.strip() != "").sum()
        print(f"Defense filled (OVERALL_DEF_RANK): {filled}/{len(out)}")


if __name__ == "__main__":
    main()
