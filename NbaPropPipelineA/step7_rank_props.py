#!/usr/bin/env python3
"""
step7_rank_props.py (KEEP ALL COLUMNS)
-------------------------------------
Ranks props while preserving EVERY upstream column (including defense + opponent stats).

Input : step6_with_team_role_context.csv  (or any CSV with at least player/prop/line fields)
Output: step7_ranked_props.xlsx

Key behavior:
- Never drops columns from the input.
- Appends ranking columns at the end:
    projection, edge, abs_edge, bet_direction, eligible, void_reason,
    edge_dr, line_hit_rate, minutes_certainty, edge_z, line_hit_z, min_z,
    prop_weight, reliability_mult, forced_over_only, rank_score, tier

Notes:
- Goblin/Demon treated as OVER-only (forced_over_only=1).
- If projection cannot be computed (unsupported prop_norm or missing stats), row becomes ineligible.
"""

from __future__ import annotations

import argparse
import math
import re
from typing import Dict, Tuple

import numpy as np
import pandas as pd

# -------------------- helpers --------------------

def _to_num(s):
    return pd.to_numeric(s, errors="coerce")

def _norm_pick_type(x: str) -> str:
    t = (str(x) if x is not None else "").strip().lower()
    if "gob" in t:
        return "Goblin"
    if "dem" in t:
        return "Demon"
    return "Standard"

def _forced_over_only(pick_type: str) -> int:
    pt = _norm_pick_type(pick_type)
    return 1 if pt in ("Goblin", "Demon") else 0

def _prop_weight(prop_norm: str) -> float:
    p = (prop_norm or "").lower().strip()
    # tweakable weights: keep singles slightly favored, reduce combo bias
    weights = {
        "pts": 1.03, "reb": 1.03, "ast": 1.03,
        "fg3m": 1.02, "fg3a": 1.01, "fg2m": 1.01, "fg2a": 1.00,
        "ftm": 1.00, "fta": 0.99,
        "stl": 1.00, "blk": 1.00, "stocks": 1.00,
        "tov": 0.97, "pf": 0.98,
        "fantasy": 1.02,
        "pr": 1.00, "pa": 1.00, "ra": 1.00, "pra": 0.98,
        "fga": 0.99, "fgm": 0.99,
    }
    return float(weights.get(p, 0.95))

def _reliability_mult(pick_type: str) -> float:
    pt = _norm_pick_type(pick_type)
    # Standard slightly favored; Demon least reliable
    return {"Standard": 1.00, "Goblin": 0.96, "Demon": 0.93}.get(pt, 0.97)

def _projection_from_row(row: pd.Series) -> float:
    """
    Weighted blend of last5 (50%), last10 (30%), season (20%).
    Falls back gracefully if some averages are missing.
    """
    weights = [
        ("stat_last5_avg",   0.50),
        ("stat_last10_avg",  0.30),
        ("stat_season_avg",  0.20),
    ]
    total_w = 0.0
    total_v = 0.0
    for col, w in weights:
        if col in row.index:
            v = pd.to_numeric(pd.Series([row.get(col)]), errors="coerce").iloc[0]
            if not pd.isna(v):
                total_v += v * w
                total_w += w
    if total_w < 0.1:
        return np.nan
    return float(total_v / total_w)

def _line_hit_rate_from_row(row: pd.Series) -> float:
    """
    Weighted blend of last5 (50%) and last10 (50%) hit rates vs the line.
    More stable than last5 alone — hot streaks get tempered by last10.
    Falls back to whatever is available.
    """
    hr5  = np.nan
    hr10 = np.nan

    for c in ("line_hit_rate_over_ou_5", "line_hit_rate_over_5", "last5_hit_rate"):
        if c in row.index:
            v = pd.to_numeric(pd.Series([row.get(c)]), errors="coerce").iloc[0]
            if not pd.isna(v):
                hr5 = float(v)
                break

    for c in ("line_hit_rate_over_ou_10", "line_hit_rate_over_10"):
        if c in row.index:
            v = pd.to_numeric(pd.Series([row.get(c)]), errors="coerce").iloc[0]
            if not pd.isna(v):
                hr10 = float(v)
                break

    if not np.isnan(hr5) and not np.isnan(hr10):
        return hr5 * 0.50 + hr10 * 0.50
    elif not np.isnan(hr5):
        return hr5
    elif not np.isnan(hr10):
        return hr10
    return np.nan

def _minutes_certainty(row: pd.Series) -> float:
    # If you have minutes tiers, give a simple score
    tier = str(row.get("minutes_tier", "")).upper()
    return {"HIGH": 1.00, "MEDIUM": 0.90, "LOW": 0.75}.get(tier, 0.80)

def _edge_transform(edge: float, cap: float = 3.0, power: float = 0.85) -> float:
    if np.isnan(edge):
        return np.nan
    s = 1.0 if edge >= 0 else -1.0
    x = min(abs(edge), cap)
    return s * (x ** power)

def _tier_from_score(score: float) -> str:
    if np.isnan(score):
        return "D"
    # thresholds can be tuned
    if score >= 2.20:
        return "A"
    if score >= 1.60:
        return "B"
    if score >= 1.05:
        return "C"
    return "D"

# -------------------- main --------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="step6_with_team_role_context.csv")
    ap.add_argument("--output", default="step7_ranked_props.xlsx")
    args = ap.parse_args()

    df = pd.read_csv(args.input, dtype=str).fillna("")
    # KEEP ALL COLUMNS
    out = df.copy()

    # Ensure minimal columns exist
    if "line" not in out.columns:
        out["line"] = ""
    if "pick_type" not in out.columns:
        out["pick_type"] = "Standard"
    if "prop_norm" not in out.columns:
        out["prop_norm"] = out.get("prop_type", "").astype(str).str.lower()

    line_num = _to_num(out["line"])

    # Build projection
    proj = out.apply(_projection_from_row, axis=1)
    out["projection"] = proj

    # Edge
    out["edge"] = proj - line_num
    out["abs_edge"] = out["edge"].abs()

    # Direction + eligibility + void reasons
    forced = out["pick_type"].apply(_forced_over_only).astype(int)
    out["forced_over_only"] = forced

    # Bet direction: forced overs for gob/dem, else based on edge sign
    bet_dir = np.where(forced.eq(1), "OVER", np.where(out["edge"] >= 0, "OVER", "UNDER"))
    out["bet_direction"] = bet_dir

    eligible = pd.Series(True, index=out.index)

    void_reason = pd.Series("", index=out.index)

    # missing line or projection
    miss = line_num.isna() | pd.isna(out["projection"])
    eligible.loc[miss] = False
    void_reason.loc[miss] = "NO_PROJECTION_OR_LINE"

    # forced overs with negative edge -> ineligible
    neg_forced = forced.eq(1) & (out["edge"] < 0)
    eligible.loc[neg_forced] = False
    void_reason.loc[neg_forced] = "FORCED_OVER_NEG_EDGE"

    out["eligible"] = eligible.astype(int)
    out["void_reason"] = void_reason

    # Score ingredients
    out["edge_dr"] = out["edge"].apply(_edge_transform)
    out["line_hit_rate"] = out.apply(_line_hit_rate_from_row, axis=1)
    out["minutes_certainty"] = out.apply(_minutes_certainty, axis=1)
    out["prop_weight"] = out["prop_norm"].astype(str).apply(_prop_weight)
    out["reliability_mult"] = out["pick_type"].astype(str).apply(_reliability_mult)

    # z-like scalers (simple normalization over eligible rows only)
    elig_mask = out["eligible"].astype(int).eq(1)
    def zcol(s: pd.Series) -> pd.Series:
        x = pd.to_numeric(s, errors="coerce")
        mu = x[elig_mask].mean()
        sd = x[elig_mask].std()
        if sd and not np.isnan(sd) and sd > 1e-9:
            return (x - mu) / sd
        return pd.Series([0.0]*len(x), index=x.index)

    out["edge_z"] = zcol(out["edge"])
    out["line_hit_z"] = zcol(out["line_hit_rate"])
    out["min_z"] = zcol(out["minutes_certainty"])

    # defense adjustment: shade projection based on opponent defense rank
    # OVERALL_DEF_RANK: 1=best defense, 30=worst. Elite defense => lower projection.
    # Scale: rank 1 = -6% adjustment, rank 30 = +6% adjustment, rank 15 = 0%
    def _def_adjustment(row: pd.Series) -> float:
        rank = pd.to_numeric(pd.Series([row.get("OVERALL_DEF_RANK", np.nan)]), errors="coerce").iloc[0]
        if pd.isna(rank):
            return 0.0
        # Linear scale: rank 1 => -0.06, rank 15 => 0.0, rank 30 => +0.06
        return float((rank - 15.0) / 15.0 * 0.06)

    def_adj = out.apply(_def_adjustment, axis=1)
    out["def_adj"] = def_adj

    # Apply defense adjustment to projection: proj_adj = proj * (1 + def_adj)
    # Direction-aware: for UNDER, good defense (negative adj) HELPS the bet
    proj_adj = out["projection"].astype(float).copy()
    for_over  = out["bet_direction"].eq("OVER")
    for_under = out["bet_direction"].eq("UNDER")
    # OVER: strong defense lowers projection (bad for OVER) → apply as-is
    # UNDER: strong defense lowers projection (good for UNDER) → same direction, correct
    proj_adj = proj_adj * (1 + def_adj)
    out["projection_adj"] = proj_adj

    # Recompute edge using defense-adjusted projection
    out["edge_adj"] = proj_adj - line_num
    out["edge_adj_dr"] = out["edge_adj"].apply(_edge_transform)

    # def_rank_z: normalized defense rank signal (higher = weaker defense = more scoring)
    # Invert so that weak defense (rank 30) = positive signal for OVER
    def _def_rank_signal(row: pd.Series) -> float:
        rank = pd.to_numeric(pd.Series([row.get("OVERALL_DEF_RANK", np.nan)]), errors="coerce").iloc[0]
        direction = str(row.get("bet_direction", "OVER")).upper()
        if pd.isna(rank):
            return 0.0
        # Normalize 1-30: rank 30 (weak def) = +1.0, rank 1 (elite def) = -1.0
        signal = (rank - 1.0) / 29.0 * 2.0 - 1.0
        return float(signal if direction == "OVER" else -signal)

    def_signal = out.apply(_def_rank_signal, axis=1)
    out["def_rank_signal"] = def_signal
    out["def_rank_z"] = zcol(def_signal)

    # avg_vs_line: how much each average beats/misses the line, normalized
    line_num_filled = line_num.fillna(0)
    for col in ("stat_last5_avg", "stat_last10_avg", "stat_season_avg"):
        out[col + "_num"] = _to_num(out.get(col, pd.Series([""] * len(out), index=out.index)))

    # score = (avg - line) / line, capped at ±1, weighted 50/30/20
    # Direction-aware: OVER wants avg > line (positive = good)
    #                  UNDER wants avg < line (negative raw = good, so we flip sign)
    def _avg_vs_line(row_idx):
        line = line_num_filled.iloc[row_idx]
        if line == 0 or np.isnan(line):
            return 0.0
        direction = str(out["bet_direction"].iloc[row_idx]).upper()
        score = 0.0
        total_w = 0.0
        for col, w in [("stat_last5_avg_num", 0.50), ("stat_last10_avg_num", 0.30), ("stat_season_avg_num", 0.20)]:
            v = out[col].iloc[row_idx]
            if not np.isnan(v):
                raw = np.clip((v - line) / line, -1.0, 1.0)
                # For UNDER, flip so that avg < line gives a positive score
                if direction == "UNDER":
                    raw = -raw
                score += raw * w
                total_w += w
        return float(score / total_w) if total_w > 0.1 else 0.0

    avg_vs_line = pd.Series([_avg_vs_line(i) for i in range(len(out))], index=out.index)
    out["avg_vs_line"] = avg_vs_line

    # z-normalize avg_vs_line over eligible rows
    out["avg_vs_line_z"] = zcol(avg_vs_line)

    # rank_score: only meaningful for eligible
    # edge_adj_dr uses defense-adjusted projection for more accurate edge
    score = (
        out["edge_adj_dr"].astype(float).fillna(0.0) * 1.10   # defense-adjusted edge
        + out["line_hit_z"].astype(float).fillna(0.0) * 0.65  # last5+last10 hit rate blend
        + out["avg_vs_line_z"].astype(float).fillna(0.0) * 0.55  # avg vs line (dir-aware)
        + out["def_rank_z"].astype(float).fillna(0.0) * 0.30  # opponent defense signal
        + out["min_z"].astype(float).fillna(0.0) * 0.20       # minutes certainty
    )
    score = score * out["prop_weight"].astype(float).fillna(1.0) * out["reliability_mult"].astype(float).fillna(1.0)
    score = score.where(elig_mask, np.nan)

    out["rank_score"] = score
    out["tier"] = out["rank_score"].apply(_tier_from_score)

    # Save: ALL + ELIGIBLE sheets (both keep all columns)
    with pd.ExcelWriter(args.output, engine="openpyxl") as w:
        out.to_excel(w, sheet_name="ALL", index=False)
        out.loc[elig_mask].to_excel(w, sheet_name="ELIGIBLE", index=False)

    print(f"✅ Saved → {args.output}")
    print(f"ALL rows     : {len(out)}")
    std = out["pick_type"].astype(str).apply(_norm_pick_type).eq("Standard")
    gd = ~std
    print(f"STANDARD rows: {int(std.sum())}")
    print(f"GOB_DEM rows : {int(gd.sum())}")
    print()
    print("Tier counts (ALL):")
    print(out["tier"].value_counts().to_string())
    print()
    print("Ineligible reason breakdown:")
    vr = out.loc[~elig_mask, "void_reason"].value_counts()
    print(vr.to_string() if len(vr) else "(none)")

if __name__ == "__main__":
    main()
