#!/usr/bin/env python3
"""
step7_rank_props.py  (REVISED 2026-02-23 — grading-informed reweight)
----------------------------------------------------------------------
Changes from prior version (all backed by 2026-02-22 grading data):

1.  PROP WEIGHT MAP — fixed broken key names so 3-PT Made, Free Throws,
    Two Pointers, Personal Fouls, Blocked Shots all map correctly instead
    of falling through to the 0.95 default. Weights now reflect actual
    hit rates: Steals/Rebounds/Assists elevated; Fouls/3PA/Fantasy Score UNDER
    penalized.

2.  RELIABILITY MULT — Goblin raised to 1.06 (was 0.96). Goblin hit 66.9%
    vs Standard 51.4%. Penalizing our best pick type was backwards.
    Demon lowered to 0.75 (was 0.93). Demon hit 30.0% decided; 90% voided.

3.  SCORE FORMULA WEIGHTS rebalanced:
    - edge_adj_dr   : 1.10 → 0.70  (51% directional accuracy on Standard;
                                     no gradient on Goblin quartiles — was over-weighted)
    - line_hit_z    : 0.65 → 0.85  (historical hit rate vs this line is more
                                     predictive than raw edge)
    - avg_vs_line_z : 0.55 → 0.75  (rolling avg vs line — directionally accurate)
    - def_rank_z    : 0.30 → 0.80  (20pp hit rate gap Elite→Weak; was barely
                                     contributing due to near-zero tier diff)
    - prop_type_z   : NEW 0.50     (23pp gap Steals 68% vs Fouls 42%; was
                                     only in prop_weight, not in z-scored signal)
    - min_z         : 0.20 → 0.25  (minor bump, minutes certainty matters)

4.  PROP TYPE SIGNAL — new _prop_hit_rate_prior() function maps each prop_norm
    to its empirical hit rate from grading. Used as a z-scored signal in the
    score formula so prop type has real discriminating power.

5.  FANTASY SCORE PROJECTION — applies a +15% upward correction to Fantasy
    Score projections. Mean projection error was -7.0 pts (model projects 93%
    of line; actuals hit 118% of line on average).

6.  COMBO PROP PROJECTION — applies a graduated upward correction to combo
    projections to counteract the systematic shrinkage:
    Pts+Rebs: +5%, Pts+Asts: +6%, Rebs+Asts: +8%,
    Pts+Rebs+Asts: +7%, Fantasy Score: +15%

7.  PROP WEIGHT MAP — 3ptmade key added (was missing, fell to 0.95 default).
    Keys now match actual prop_norm values in the data.

8.  DEF RANK SIGNAL — formula unchanged but weight raised to 0.80.
    Signal is direction-aware (OVER benefits from weak defense, UNDER from elite).

9.  TIER THRESHOLDS — tightened slightly. With rebalanced weights the score
    distribution shifts; thresholds adjusted to maintain ~top 15% in Tier A.
"""

from __future__ import annotations

import argparse
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
    return 1 if _norm_pick_type(pick_type) in ("Goblin", "Demon") else 0

# FIX 1: prop_weight keys now match actual prop_norm values from the data.
# FIX 7: added 3ptmade, corrected key names.
# Weights reflect empirical hit rates from 2026-02-22 grading.
_PROP_WEIGHTS = {
    # Singles — elevated (hit rates: Steals 68%, Rebounds 61%, Assists 59%)
    "pts":                   1.03,
    "reb":                   1.06,   # 61.2% — bump from 1.03
    "ast":                   1.05,   # 59.2% — bump from 1.03
    "stl":                   1.08,   # 68.1% — best prop type
    "blk":                   1.02,   # 54.5%
    "stocks":                1.04,   # blk+stl combo, 54.7%
    "3ptmade":               1.03,   # 58.3% — was falling to 0.95 default
    "3ptattempted":          0.88,   # 44.4% — below breakeven
    "fg3m":                  1.03,
    "fg3a":                  0.88,
    "fg2m":                  1.01,
    "fg2a":                  0.92,   # 2PT attempted 46.3%
    "twopointersmade":       1.01,
    "twopointersattempted":  0.92,
    "fgm":                   0.99,
    "fga":                   0.99,
    "freethrowsmade":        1.01,   # was falling to 0.95
    "freethrowsattempted":   0.98,   # was falling to 0.95
    "ftm":                   1.01,
    "fta":                   0.98,
    "tov":                   0.94,   # 48.4%
    "pf":                    0.85,   # personalfouls 42.4% — penalize hard
    "personalfouls":         0.85,
    # Combos
    "pr":                    1.01,   # Pts+Rebs 56.2%
    "pa":                    1.01,   # Pts+Asts 55.7%
    "ra":                    1.02,   # Rebs+Asts 57.5%
    "pra":                   0.99,   # Pts+Rebs+Asts 54.5%
    # Fantasy Score — OVER hits 67% but projection is miscalibrated; UNDER hits 37%
    # Weight as neutral — correction handled in projection adjustment
    "fantasy":               1.00,
}

def _prop_weight(prop_norm: str) -> float:
    return float(_PROP_WEIGHTS.get(str(prop_norm).lower().strip(), 0.93))

# FIX 4: empirical hit rate priors from 2026-02-22 grading (OVER rates, Goblin+Standard combined)
# Used as a z-scored signal. Center is ~0.56 (overall hit rate).
_PROP_HIT_RATE_PRIOR = {
    "stl":                   0.697,
    "fantasy":               0.674,  # OVER only — do not use for UNDER
    "3ptmade":               0.623,
    "fg3m":                  0.623,
    "reb":                   0.617,
    "ra":                    0.600,
    "ast":                   0.593,
    "freethrowsmade":        0.583,
    "ftm":                   0.583,
    "pr":                    0.568,
    "pts":                   0.566,
    "stocks":                0.547,
    "blk":                   0.545,
    "pra":                   0.545,
    "fga":                   0.558,   # UNDER strong (64.5%); OVER weak
    "pa":                    0.557,
    "fgm":                   0.519,
    "fg2m":                  0.528,
    "twopointersmade":       0.528,
    "fg2a":                  0.463,
    "twopointersattempted":  0.463,
    "tov":                   0.484,
    "3ptattempted":          0.444,
    "fg3a":                  0.444,
    "pf":                    0.424,
    "personalfouls":         0.424,
}

def _prop_hit_rate_prior(prop_norm: str, direction: str) -> float:
    """
    Return empirical hit rate prior for this prop type + direction.
    For UNDER: invert the OVER rate (1 - rate) for props without specific UNDER data.
    """
    key = str(prop_norm).lower().strip()
    base = _PROP_HIT_RATE_PRIOR.get(key, 0.545)   # default to overall avg
    if direction == "UNDER":
        # Fantasy Score UNDER was 37.1% — severely penalize
        if key == "fantasy":
            return 0.371
        # FG Attempted UNDER was 64.5% — better than OVER
        if key in ("fga", "fg2a", "twopointersattempted"):
            return 0.645
        # Rebounds UNDER 59.1%, Pts+Asts UNDER 59.0%
        if key == "reb":
            return 0.591
        if key == "pa":
            return 0.590
        if key in ("pts", "pr", "pra"):
            return 0.540   # roughly neutral for these
        # Default: invert (weak for UNDER what's strong for OVER)
        return float(1.0 - base)
    return float(base)

# FIX 2: reliability mult — Goblin raised, Demon sharply lowered
def _reliability_mult(pick_type: str) -> float:
    pt = _norm_pick_type(pick_type)
    return {
        "Standard": 1.00,
        "Goblin":   1.06,   # was 0.96 — backwards vs actual 66.9% hit rate
        "Demon":    0.75,   # was 0.93 — actual 30.0% decided hit rate
    }.get(pt, 0.97)

def _projection_from_row(row: pd.Series) -> float:
    """
    Weighted blend: last5 50%, last10 30%, season 20%.
    Falls back gracefully if some averages are missing.
    """
    weights = [
        ("stat_last5_avg",  0.50),
        ("stat_last10_avg", 0.30),
        ("stat_season_avg", 0.20),
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
    raw = float(total_v / total_w)

    # FIX 6: combo prop upward correction — counteracts systematic shrinkage
    prop_norm = str(row.get("prop_norm", "")).lower().strip()
    corrections = {
        "pr":      1.05,   # Pts+Rebs: proj/line gap was -0.068
        "pa":      1.06,   # Pts+Asts: gap was -0.080
        "ra":      1.08,   # Rebs+Asts: gap was -0.114 (largest non-fantasy)
        "pra":     1.07,   # Pts+Rebs+Asts: gap was -0.082
        "fantasy": 1.15,   # Fantasy Score: gap was -0.250 (severe miscalibration)
    }
    factor = corrections.get(prop_norm, 1.0)
    return raw * factor

def _safe_float(x):
    v = pd.to_numeric(pd.Series([x]), errors="coerce").iloc[0]
    return float(v) if not pd.isna(v) else np.nan

def _derive_under_rate_from_counts(row: pd.Series, ou_only: bool = True) -> float:
    o = _safe_float(row.get("last5_over", np.nan))
    u = _safe_float(row.get("last5_under", np.nan))
    p = _safe_float(row.get("last5_push", np.nan))
    if np.isnan(o) or np.isnan(u):
        return np.nan
    denom = (o + u) if ou_only else (o + u + (0.0 if np.isnan(p) else p))
    if denom <= 0:
        return np.nan
    return float(u / denom)

def _line_hit_rate_from_row(row: pd.Series) -> float:
    """Direction-aware line hit rate — OVER uses over columns, UNDER uses under columns."""
    direction = str(row.get("bet_direction", "OVER")).upper()
    hr5 = np.nan
    hr10 = np.nan

    if direction == "UNDER":
        for c in ("line_hit_rate_under_ou_5", "line_hit_rate_under_5"):
            if c in row.index:
                v = _safe_float(row.get(c))
                if not np.isnan(v):
                    hr5 = v
                    break
        if np.isnan(hr5):
            hr5 = _derive_under_rate_from_counts(row, ou_only=True)
        if np.isnan(hr5):
            over_rate = _safe_float(row.get("last5_hit_rate", np.nan))
            push = _safe_float(row.get("last5_push", np.nan))
            if not np.isnan(over_rate) and (np.isnan(push) or push == 0):
                hr5 = float(1.0 - over_rate)
        for c in ("line_hit_rate_under_ou_10", "line_hit_rate_under_10"):
            if c in row.index:
                v = _safe_float(row.get(c))
                if not np.isnan(v):
                    hr10 = v
                    break
    else:
        for c in ("line_hit_rate_over_ou_5", "line_hit_rate_over_5", "last5_hit_rate"):
            if c in row.index:
                v = _safe_float(row.get(c))
                if not np.isnan(v):
                    hr5 = v
                    break
        for c in ("line_hit_rate_over_ou_10", "line_hit_rate_over_10"):
            if c in row.index:
                v = _safe_float(row.get(c))
                if not np.isnan(v):
                    hr10 = v
                    break

    if not np.isnan(hr5) and not np.isnan(hr10):
        return hr5 * 0.50 + hr10 * 0.50
    elif not np.isnan(hr5):
        return hr5
    elif not np.isnan(hr10):
        return hr10
    return np.nan

def _minutes_certainty(row: pd.Series) -> float:
    tier = str(row.get("minutes_tier", "")).upper()
    return {"HIGH": 1.00, "MEDIUM": 0.90, "LOW": 0.75}.get(tier, 0.80)

def _edge_transform(edge: float, cap: float = 3.0, power: float = 0.85) -> float:
    if np.isnan(edge):
        return np.nan
    s = 1.0 if edge >= 0 else -1.0
    x = min(abs(edge), cap)
    return s * (x ** power)

def _tier_from_score(score: float) -> str:
    """
    FIX 9: thresholds adjusted for rebalanced score distribution.
    Target: ~top 13% Tier A, ~5% Tier B, ~5% Tier C, rest D.
    """
    if np.isnan(score):
        return "D"
    if score >= 2.50:
        return "A"
    if score >= 1.75:
        return "B"
    if score >= 1.10:
        return "C"
    return "D"

# -------------------- main --------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",  default="step6_with_team_role_context.csv")
    ap.add_argument("--output", default="step7_ranked_props.xlsx")
    args = ap.parse_args()

    df = pd.read_csv(args.input, dtype=str).fillna("")
    out = df.copy()

    if "line" not in out.columns:
        out["line"] = ""
    if "pick_type" not in out.columns:
        out["pick_type"] = "Standard"

    if "prop_norm" not in out.columns:
        if "prop_type" in out.columns:
            out["prop_norm"] = out["prop_type"].astype(str).str.lower()
        else:
            out["prop_norm"] = ""

    line_num = _to_num(out["line"])

    # Build projection (FIX 5+6: includes combo corrections)
    proj = out.apply(_projection_from_row, axis=1)
    out["projection"] = proj

    out["edge"] = proj - line_num
    out["abs_edge"] = out["edge"].abs()

    forced = out["pick_type"].apply(_forced_over_only).astype(int)
    out["forced_over_only"] = forced

    bet_dir = np.where(forced.eq(1), "OVER", np.where(out["edge"] >= 0, "OVER", "UNDER"))
    out["bet_direction"] = bet_dir

    eligible   = pd.Series(True,  index=out.index)
    void_reason = pd.Series("",   index=out.index)

    miss = line_num.isna() | pd.isna(out["projection"])
    eligible.loc[miss]    = False
    void_reason.loc[miss] = "NO_PROJECTION_OR_LINE"

    neg_forced = forced.eq(1) & (out["edge"] < 0)
    eligible.loc[neg_forced]    = False
    void_reason.loc[neg_forced] = "FORCED_OVER_NEG_EDGE"

    out["eligible"]    = eligible.astype(int)
    out["void_reason"] = void_reason

    out["edge_dr"] = out["edge"].apply(_edge_transform)
    out["line_hit_rate"] = out.apply(_line_hit_rate_from_row, axis=1)
    out["minutes_certainty"] = out.apply(_minutes_certainty, axis=1)

    # FIX 1+7: corrected prop weights
    out["prop_weight"] = out["prop_norm"].astype(str).apply(_prop_weight)

    # FIX 2: corrected reliability mults
    out["reliability_mult"] = out["pick_type"].astype(str).apply(_reliability_mult)

    elig_mask = out["eligible"].astype(int).eq(1)

    def zcol(s: pd.Series) -> pd.Series:
        x  = pd.to_numeric(s, errors="coerce")
        mu = x[elig_mask].mean()
        sd = x[elig_mask].std()
        if sd and not np.isnan(sd) and sd > 1e-9:
            return (x - mu) / sd
        return pd.Series([0.0] * len(x), index=x.index)

    out["edge_z"]      = zcol(out["edge"])
    out["line_hit_z"]  = zcol(out["line_hit_rate"])
    out["min_z"]       = zcol(out["minutes_certainty"])

    # Defense adjustment
    def _def_adjustment(row: pd.Series) -> float:
        rank = pd.to_numeric(pd.Series([row.get("OVERALL_DEF_RANK", np.nan)]), errors="coerce").iloc[0]
        if pd.isna(rank):
            return 0.0
        return float((rank - 15.0) / 15.0 * 0.06)

    def_adj = out.apply(_def_adjustment, axis=1)
    out["def_adj"] = def_adj

    proj_base = pd.to_numeric(out["projection"], errors="coerce")
    out["projection_adj"] = proj_base * (1.0 + def_adj.astype(float))
    out["edge_adj"]    = out["projection_adj"] - line_num
    out["edge_adj_dr"] = out["edge_adj"].apply(_edge_transform)

    def _def_rank_signal(row: pd.Series) -> float:
        rank = pd.to_numeric(pd.Series([row.get("OVERALL_DEF_RANK", np.nan)]), errors="coerce").iloc[0]
        direction = str(row.get("bet_direction", "OVER")).upper()
        if pd.isna(rank):
            return 0.0
        # rank 1 = best defense (hardest for OVER), rank 30 = worst defense (easiest for OVER)
        signal = (rank - 1.0) / 29.0 * 2.0 - 1.0   # scales -1.0 (Elite) to +1.0 (Worst)
        return float(signal if direction == "OVER" else -signal)

    def_signal = out.apply(_def_rank_signal, axis=1)
    out["def_rank_signal"] = def_signal
    out["def_rank_z"]      = zcol(def_signal)

    line_num_filled = line_num.fillna(0)

    for col in ("stat_last5_avg", "stat_last10_avg", "stat_season_avg"):
        out[col + "_num"] = _to_num(out[col]) if col in out.columns else _to_num(pd.Series([""] * len(out), index=out.index))

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
                if direction == "UNDER":
                    raw = -raw
                score += raw * w
                total_w += w
        return float(score / total_w) if total_w > 0.1 else 0.0

    avg_vs_line = pd.Series([_avg_vs_line(i) for i in range(len(out))], index=out.index)
    out["avg_vs_line"]   = avg_vs_line
    out["avg_vs_line_z"] = zcol(avg_vs_line)

    # FIX 4: prop type hit rate prior as z-scored signal
    prop_hr_prior = out.apply(
        lambda r: _prop_hit_rate_prior(r.get("prop_norm", ""), str(r.get("bet_direction", "OVER")).upper()),
        axis=1
    )
    out["prop_hr_prior"] = prop_hr_prior
    out["prop_hr_z"]     = zcol(prop_hr_prior)

    # FIX 3: rebalanced score formula
    # edge_adj_dr   : 1.10 → 0.70  (edge has poor directional accuracy, was over-weighted)
    # line_hit_z    : 0.65 → 0.85  (historical hit rate vs line — more predictive)
    # avg_vs_line_z : 0.55 → 0.75  (rolling avg vs line — directionally sound)
    # def_rank_z    : 0.30 → 0.80  (20pp gap Elite→Weak; signal was being ignored)
    # prop_hr_z     : NEW  0.50    (23pp gap best→worst prop type)
    # min_z         : 0.20 → 0.25  (minor bump)
    score = (
        out["edge_adj_dr"].astype(float).fillna(0.0)    * 0.70
        + out["line_hit_z"].astype(float).fillna(0.0)   * 0.85
        + out["avg_vs_line_z"].astype(float).fillna(0.0)* 0.75
        + out["def_rank_z"].astype(float).fillna(0.0)   * 0.80
        + out["prop_hr_z"].astype(float).fillna(0.0)    * 0.50
        + out["min_z"].astype(float).fillna(0.0)        * 0.25
    )
    score = (
        score
        * out["prop_weight"].astype(float).fillna(1.0)
        * out["reliability_mult"].astype(float).fillna(1.0)
    )
    score = score.where(elig_mask, np.nan)

    out["rank_score"] = score
    out["tier"]       = out["rank_score"].apply(_tier_from_score)

    with pd.ExcelWriter(args.output, engine="openpyxl") as w:
        out.to_excel(w, sheet_name="ALL",      index=False)
        out.loc[elig_mask].to_excel(w, sheet_name="ELIGIBLE", index=False)

    print(f"✅ Saved → {args.output}")
    print(f"ALL rows      : {len(out)}")
    std = out["pick_type"].astype(str).apply(_norm_pick_type).eq("Standard")
    print(f"STANDARD rows : {int(std.sum())}")
    print(f"GOB_DEM rows  : {int((~std).sum())}")
    print()
    print("Tier counts (ALL):")
    print(out["tier"].value_counts().to_string())
    print()
    print("Ineligible reason breakdown:")
    vr = out.loc[~elig_mask, "void_reason"].value_counts()
    print(vr.to_string() if len(vr) else "(none)")
    print()
    print("Score percentiles (eligible):")
    rs = pd.to_numeric(out.loc[elig_mask, "rank_score"], errors="coerce")
    print(rs.quantile([0.50, 0.70, 0.80, 0.85, 0.90, 0.95]).round(3).to_string())

if __name__ == "__main__":
    main()
