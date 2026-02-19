#!/usr/bin/env python3
"""
cbb_step6_rank_props.py  (upgraded — mirrors NBA step7)
--------------------------------------------------------
Ranks CBB props using the same logic as NBA step7:
- Weighted projection blend: last5 (50%) + last10 (30%) + season (20%)
- Direction-aware avg_vs_line signal
- Last5 + last10 hit rate blend (50/50)
- Defense-adjusted edge
- Direction-aware defense signal

Input : step5b_with_stats_cbb.csv
Output: step6_ranked_props_cbb.xlsx + optional CSV

New columns added:
  projection, edge, edge_adj, avg_vs_line, def_adj, def_rank_signal,
  line_hit_rate (blended), rank_score, tier, final_bet_direction
"""

from __future__ import annotations

import argparse
import math
from typing import Optional

import numpy as np
import pandas as pd


def _to_num(s):
    return pd.to_numeric(s, errors="coerce")


def _norm_pick_type(x: str) -> str:
    t = str(x or "").strip().lower()
    if "gob" in t: return "Goblin"
    if "dem" in t: return "Demon"
    return "Standard"


def _forced_over(pick_type: str) -> int:
    return 1 if _norm_pick_type(pick_type) in ("Goblin", "Demon") else 0


def _reliability_mult(pick_type: str) -> float:
    return {"Standard": 1.00, "Goblin": 0.96, "Demon": 0.93}.get(_norm_pick_type(pick_type), 0.97)


def _edge_transform(edge: float, cap=3.0, power=0.85) -> float:
    if np.isnan(edge): return np.nan
    s = 1.0 if edge >= 0 else -1.0
    return s * (min(abs(edge), cap) ** power)


def _tier(score: float) -> str:
    if np.isnan(score): return "D"
    if score >= 2.20:   return "A"
    if score >= 1.60:   return "B"
    if score >= 1.05:   return "C"
    return "D"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",      required=True)
    ap.add_argument("--output",     default="step6_ranked_props_cbb.xlsx")
    ap.add_argument("--output_csv", default="")
    args = ap.parse_args()

    df = pd.read_csv(args.input, dtype=str).fillna("")
    print(f"→ Loaded: {args.input} | rows={len(df)}")

    # Only rank OK rows
    ok = df["stat_status"].astype(str).str.upper().eq("OK") if "stat_status" in df.columns else \
         df.get("status3", pd.Series([""] * len(df))).astype(str).str.upper().eq("OK")

    out = df.copy()

    line_num = _to_num(out["line"])

    # ── Projection: weighted blend last5/last10/season ──────────────────────
    l5  = _to_num(out.get("stat_last5_avg",  pd.Series([""] * len(out))))
    l10 = _to_num(out.get("stat_last10_avg", pd.Series([""] * len(out))))
    ssn = _to_num(out.get("stat_season_avg", pd.Series([""] * len(out))))

    def blend_proj(row_idx):
        weights = [(l5.iloc[row_idx], 0.50), (l10.iloc[row_idx], 0.30), (ssn.iloc[row_idx], 0.20)]
        tv = tw = 0.0
        for v, w in weights:
            if not np.isnan(v): tv += v * w; tw += w
        return tv / tw if tw >= 0.1 else np.nan

    proj = pd.Series([blend_proj(i) for i in range(len(out))], index=out.index)
    out["projection"] = proj

    # ── Edge ────────────────────────────────────────────────────────────────
    out["edge"]     = proj - line_num
    out["abs_edge"] = out["edge"].abs()

    # ── Direction / eligibility ──────────────────────────────────────────────
    pick_type = out.get("pick_type", pd.Series(["Standard"] * len(out))).astype(str)
    forced    = pick_type.apply(_forced_over).astype(int)
    out["forced_over_only"] = forced

    bet_dir = np.where(forced.eq(1), "OVER",
              np.where(out["edge"] >= 0, "OVER", "UNDER"))
    out["bet_direction"] = bet_dir

    eligible   = pd.Series(True,  index=out.index)
    void_reason= pd.Series("",    index=out.index)

    miss = line_num.isna() | proj.isna()
    eligible.loc[miss]   = False
    void_reason.loc[miss] = "NO_PROJECTION_OR_LINE"

    neg_forced = forced.eq(1) & (out["edge"] < 0)
    eligible.loc[neg_forced]    = False
    void_reason.loc[neg_forced] = "FORCED_OVER_NEG_EDGE"

    # also mark non-OK rows ineligible
    eligible.loc[~ok] = False
    void_reason.loc[~ok & void_reason.eq("")] = "STAT_NOT_OK"

    out["eligible"]    = eligible.astype(int)
    out["void_reason"] = void_reason

    elig_mask = eligible

    # ── Defense adjustment ───────────────────────────────────────────────────
    # Try multiple possible column names for defense rank
    def_rank_col = next((c for c in ["OVERALL_DEF_RANK","OPP_OVERALL_DEF_RANK","opp_def_rank"] if c in out.columns), "")
    if def_rank_col:
        def_rank_num = _to_num(out[def_rank_col])
    else:
        def_rank_num = pd.Series([np.nan] * len(out), index=out.index)

    def _def_adj(row_idx):
        rank = def_rank_num.iloc[row_idx]
        if np.isnan(rank): return 0.0
        return float((rank - 15.0) / 15.0 * 0.06)

    def_adj = pd.Series([_def_adj(i) for i in range(len(out))], index=out.index)
    out["def_adj"] = def_adj

    proj_adj = proj * (1 + def_adj)
    out["projection_adj"] = proj_adj
    out["edge_adj"]       = proj_adj - line_num
    out["edge_adj_dr"]    = out["edge_adj"].apply(
        lambda x: _edge_transform(x) if not (isinstance(x, float) and np.isnan(x)) else np.nan)

    def _def_signal(row_idx):
        rank = def_rank_num.iloc[row_idx]
        direction = str(out["bet_direction"].iloc[row_idx]).upper()
        if np.isnan(rank): return 0.0
        signal = (rank - 1.0) / 29.0 * 2.0 - 1.0
        return float(signal if direction == "OVER" else -signal)

    def_signal = pd.Series([_def_signal(i) for i in range(len(out))], index=out.index)
    out["def_rank_signal"] = def_signal

    # ── Hit rate: blend last5 + last10 ──────────────────────────────────────
    hr5  = _to_num(out.get("line_hit_rate_over_ou_5",  pd.Series([np.nan]*len(out))))
    hr10 = _to_num(out.get("line_hit_rate_over_ou_10", pd.Series([np.nan]*len(out))))

    def blend_hr(row_idx):
        h5  = hr5.iloc[row_idx]
        h10 = hr10.iloc[row_idx]
        if not np.isnan(h5) and not np.isnan(h10): return h5 * 0.50 + h10 * 0.50
        if not np.isnan(h5):  return h5
        if not np.isnan(h10): return h10
        return np.nan

    line_hit_rate = pd.Series([blend_hr(i) for i in range(len(out))], index=out.index)
    out["line_hit_rate"] = line_hit_rate

    # ── Avg vs line (direction-aware) ────────────────────────────────────────
    for col in ("stat_last5_avg","stat_last10_avg","stat_season_avg"):
        out[f"_{col}_n"] = _to_num(out.get(col, pd.Series([""] * len(out))))

    line_filled = line_num.fillna(0)

    def _avg_vs_line(row_idx):
        ln = line_filled.iloc[row_idx]
        if ln == 0 or np.isnan(ln): return 0.0
        direction = str(out["bet_direction"].iloc[row_idx]).upper()
        score = tw = 0.0
        for col, w in [("_stat_last5_avg_n",0.50),("_stat_last10_avg_n",0.30),("_stat_season_avg_n",0.20)]:
            v = out[col].iloc[row_idx]
            if not np.isnan(v):
                raw = np.clip((v - ln) / ln, -1.0, 1.0)
                score += (-raw if direction == "UNDER" else raw) * w
                tw += w
        return float(score / tw) if tw > 0.1 else 0.0

    avg_vs_line = pd.Series([_avg_vs_line(i) for i in range(len(out))], index=out.index)
    out["avg_vs_line"] = avg_vs_line

    # ── Z-score normalizers ───────────────────────────────────────────────────
    def zcol(s: pd.Series) -> pd.Series:
        x  = pd.to_numeric(s, errors="coerce")
        mu = x[elig_mask].mean()
        sd = x[elig_mask].std()
        if sd and not np.isnan(sd) and sd > 1e-9:
            return (x - mu) / sd
        return pd.Series([0.0] * len(x), index=x.index)

    edge_adj_z   = zcol(out["edge_adj_dr"])
    hit_z        = zcol(line_hit_rate)
    avg_vs_line_z= zcol(avg_vs_line)
    def_z        = zcol(def_signal)

    out["reliability_mult"] = pick_type.apply(_reliability_mult)

    # ── Rank score ────────────────────────────────────────────────────────────
    score = (
        edge_adj_z.fillna(0.0)    * 1.10
        + hit_z.fillna(0.0)       * 0.65
        + avg_vs_line_z.fillna(0.0) * 0.55
        + def_z.fillna(0.0)       * 0.30
    )
    score = score * out["reliability_mult"].astype(float).fillna(1.0)
    score = score.where(elig_mask, np.nan)

    out["rank_score"] = score
    out["tier"]       = out["rank_score"].apply(
        lambda x: _tier(x) if not (isinstance(x, float) and np.isnan(x)) else "D")

    # ── Final bet direction (step8-style logic inline) ────────────────────────
    final_dir = np.where(forced.eq(1), "OVER",
                np.where(out["edge"] >= 0, "OVER", "UNDER"))
    out["final_bet_direction"] = final_dir

    # ── Clean up temp columns ─────────────────────────────────────────────────
    out.drop(columns=[c for c in out.columns if c.startswith("_stat_")], inplace=True)

    # ── Sort ──────────────────────────────────────────────────────────────────
    out_sorted = out.sort_values("rank_score", ascending=False, na_position="last")

    # ── Write Excel ───────────────────────────────────────────────────────────
    with pd.ExcelWriter(args.output, engine="openpyxl") as xw:
        out_sorted.to_excel(xw, index=False, sheet_name="ALL")
        out_sorted[elig_mask].to_excel(xw, index=False, sheet_name="ELIGIBLE")
        for t in ["A","B","C","D"]:
            sub = out_sorted[out_sorted["tier"] == t]
            if len(sub): sub.to_excel(xw, index=False, sheet_name=f"TIER_{t}")

    print(f"✅ Saved → {args.output}")
    print(f"ALL rows: {len(out_sorted)}")
    print("Tier breakdown:")
    print(out_sorted["tier"].value_counts().to_string())
    print("\nVoid reasons:")
    vr = out_sorted.loc[~elig_mask, "void_reason"].value_counts()
    print(vr.to_string() if len(vr) else "(none)")

    if args.output_csv:
        out_sorted.to_csv(args.output_csv, index=False)
        print(f"✅ Saved CSV → {args.output_csv}")


if __name__ == "__main__":
    main()
