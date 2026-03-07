#!/usr/bin/env python3
"""
step3_attach_defense_soccer.py  (Soccer Pipeline)

Mirrors NBA step3_attach_defense.py.
Merges soccer team defensive ratings onto each prop row
based on the opponent team.

Inputs:
  --input   step2_soccer_picktypes.csv
  --defense soccer_defense_summary.csv   (from soccer defense_report)
Output:
  --output  step3_soccer_with_defense.csv

Run:
  py -3.14 step3_attach_defense_soccer.py \
    --input step2_soccer_picktypes.csv \
    --defense soccer_defense_summary.csv \
    --output step3_soccer_with_defense.csv
"""

from __future__ import annotations

import argparse
import sys
from typing import List, Optional, Tuple

import pandas as pd


def _col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _safe_upper(x) -> str:
    return (str(x) or "").strip().upper()


def split_slash(s: str) -> Tuple[str, str]:
    parts = [p.strip() for p in str(s or "").split("/")]
    return (parts[0], parts[1]) if len(parts) >= 2 else (parts[0].strip(), "")


def derive_combo_opps(row: pd.Series) -> Tuple[str, str]:
    t1   = _safe_upper(row.get("team_1", ""))
    t2   = _safe_upper(row.get("team_2", ""))
    home = _safe_upper(row.get("pp_home_team", ""))
    away = _safe_upper(row.get("pp_away_team", ""))

    if home and away and t1 and t2:
        opp1 = away if t1 == home else (home if t1 == away else "")
        opp2 = away if t2 == home else (home if t2 == away else "")
        return opp1, opp2

    opp = str(row.get("opp_team", "")).strip()
    if "/" in opp:
        return split_slash(opp)
    return "", ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",   required=True)
    ap.add_argument("--defense", required=True)
    ap.add_argument("--output",  required=True)
    args = ap.parse_args()

    print(f"→ Loading Step2: {args.input}")
    df = pd.read_csv(args.input, dtype=str, encoding="utf-8-sig").fillna("")

    if df.empty:
        print("❌ [SlateIQ-Soccer-S3] Empty input from S2 — aborting.")
        sys.exit(1)

    print(f"→ Loading defense: {args.defense}")
    d  = pd.read_csv(args.defense, dtype=str, encoding="utf-8-sig").fillna("")

    # Prefer pp_name (PrizePicks-style short name) as merge key
    # Fall back to old column names for backward compatibility
    key = _col(d, ["pp_name", "TEAM_ABBREVIATION", "team_abbr", "abbr", "TEAM_ABBR", "team"])
    if not key:
        raise RuntimeError(f"❌ Defense file missing team key. Columns: {list(d.columns)}")

    print(f"  Merging on column: '{key}'")

    # Drop columns that would collide with step2 data
    drop_cols = [c for c in ["team", "league", "team_name"] if c in d.columns and c != key]
    if drop_cols:
        print(f"  Dropping defense cols to avoid merge collision: {drop_cols}")
        d = d.drop(columns=drop_cols)

    d[key] = d[key].astype(str).str.strip().str.upper()
    def_cols = [c for c in d.columns if c != key]

    if "opp_team" not in df.columns:
        df["opp_team"] = ""
    df["opp_team"] = df["opp_team"].astype(str).str.strip().str.upper()

    # --- SAFETY: derive opp_team from pp_game_id if still blank ---
    if "pp_game_id" in df.columns and "team" in df.columns:
        need = df["opp_team"].astype(str).str.strip().eq("")
        if need.any():
            tmp = df[["pp_game_id", "team"]].copy()
            tmp["pp_game_id"] = tmp["pp_game_id"].astype(str).str.strip()
            tmp["team"] = tmp["team"].astype(str).str.strip().str.upper()

            # Build game->team pair map
            pairs = {}
            for gid, g in tmp.groupby("pp_game_id", dropna=False):
                gid = str(gid).strip()
                if not gid:
                    continue
                teams = [t for t in g["team"].unique().tolist() if t]
                if len(teams) == 2:
                    pairs[gid] = (teams[0], teams[1])

            if pairs:
                def _opp_from_pair(gid: str, team: str) -> str:
                    p = pairs.get(gid)
                    if not p:
                        return ""
                    a, b = p
                    if team == a: return b
                    if team == b: return a
                    return ""

                gid_col  = df.loc[need, "pp_game_id"].astype(str).str.strip()
                team_col = df.loc[need, "team"].astype(str).str.strip().str.upper()
                df.loc[need, "opp_team"] = [
                    _opp_from_pair(g, t) for g, t in zip(gid_col, team_col)
                ]

            filled = (df["opp_team"].astype(str).str.strip() != "").sum()
            print(f"  ✅ opp_team filled after pp_game_id safety net: {filled}/{len(df)}")

    if "is_combo_player" not in df.columns:
        df["is_combo_player"] = df.get("player", "").astype(str).str.contains(r"\+").astype(int)

    singles_mask = df["is_combo_player"].astype(str).isin(["0", "False", "false", ""])
    combos_mask  = ~singles_mask

    # ── Singles ──
    print("→ Merging defense for singles...")
    singles = df.loc[singles_mask].copy()
    singles = singles.merge(d[[key] + def_cols], how="left",
                            left_on="opp_team", right_on=key)
    if key in singles.columns:
        singles.drop(columns=[key], inplace=True)

    # ── Combos ──
    combos = df.loc[combos_mask].copy()
    if len(combos) > 0:
        for c in ["team_1", "team_2", "pp_home_team", "pp_away_team"]:
            if c not in combos.columns:
                combos[c] = ""

        opps = combos.apply(derive_combo_opps, axis=1, result_type="expand")
        opps.columns = ["opp_team_1", "opp_team_2"]
        combos["opp_team_1"] = opps["opp_team_1"].astype(str).str.strip().str.upper()
        combos["opp_team_2"] = opps["opp_team_2"].astype(str).str.strip().str.upper()

        leg1 = combos.merge(d[[key] + def_cols], how="left",
                            left_on="opp_team_1", right_on=key)
        if key in leg1.columns:
            leg1.drop(columns=[key], inplace=True)

        leg2 = combos.merge(d[[key] + def_cols], how="left",
                            left_on="opp_team_2", right_on=key)
        if key in leg2.columns:
            leg2.drop(columns=[key], inplace=True)

        leg1 = leg1.rename(columns={c: f"{c}_DEF_1" for c in def_cols})
        leg2 = leg2.rename(columns={c: f"{c}_DEF_2" for c in def_cols})

        leg1_new = leg1[[c for c in leg1.columns if c.endswith("_DEF_1")]]
        leg2_new = leg2[[c for c in leg2.columns if c.endswith("_DEF_2")]]

        combos = pd.concat([
            combos.reset_index(drop=True),
            leg1_new.reset_index(drop=True),
            leg2_new.reset_index(drop=True),
        ], axis=1)

    out = pd.concat([singles, combos], axis=0, ignore_index=True)

    desired_front = ["espn_player_id", "player", "pos", "team", "opp_team",
                     "league", "line", "prop_type", "prop_norm", "pick_type"]
    front  = [c for c in desired_front if c in out.columns]
    tail   = ["is_combo_player"] if "is_combo_player" in out.columns else []
    middle = [c for c in out.columns if c not in set(front + tail)]
    out    = out[front + middle + tail]

    out.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"✅ Saved → {args.output}")
    print(f"Rows: {len(out)} | Cols: {len(out.columns)}")

    if out.empty:
        print("❌ [SlateIQ-Soccer-S3] Output is empty — aborting.")
        sys.exit(1)

    if "OVERALL_DEF_RANK" in out.columns:
        filled = (out["OVERALL_DEF_RANK"].astype(str).str.strip() != "").sum()
        fill_pct = filled / len(out) if len(out) else 0
        print(f"Defense filled (OVERALL_DEF_RANK): {filled}/{len(out)} ({fill_pct:.0%})")
        if fill_pct < 0.50:
            print(f"❌ [SlateIQ-Soccer-S3] Defense fill rate {fill_pct:.0%} below 50% threshold — aborting.")
            sys.exit(1)


if __name__ == "__main__":
    main()
