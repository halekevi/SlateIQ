#!/usr/bin/env python3
"""
step7_rank_props_soccer.py  (Soccer Pipeline)

Mirrors NBA step7_rank_props.py with soccer-specific:
  - Prop weights tuned for soccer variance
  - Hit rate priors from soccer data
  - Position-aware defense adjustment
  - GK props (saves) treated separately

Run:
  py -3.14 step7_rank_props_soccer.py \
    --input step6_soccer_role_context.csv \
    --output step7_soccer_ranked.xlsx
"""

from __future__ import annotations

import argparse
import sys
import numpy as np
import pandas as pd


def _to_num(s):
    return pd.to_numeric(s, errors="coerce")


def _norm_pick_type(x: str) -> str:
    t = (str(x) if x is not None else "").strip().lower()
    if "gob" in t: return "Goblin"
    if "dem" in t: return "Demon"
    return "Standard"


def _forced_over_only(pick_type: str) -> int:
    return 1 if _norm_pick_type(pick_type) in ("Goblin", "Demon") else 0


# ── Soccer prop weights ───────────────────────────────────────────────────────
# Higher = more predictable/valuable prop type

_PROP_WEIGHTS = {
    "passes":          1.08,   # most stable, high volume
    "saves":           1.06,   # GK saves fairly predictable
    "shots_on_target": 1.05,
    "assists":         1.04,
    "shots":           1.03,
    "goals":           0.95,   # high variance
    "goal_assist":     0.97,
    "clearances":      1.02,
    "tackles":         1.01,
    "fouls":           0.98,
    "goals_allowed":   0.96,
    "shots_assisted":  1.00,
}

def _prop_weight(prop_norm: str) -> float:
    return float(_PROP_WEIGHTS.get(str(prop_norm).lower().strip(), 0.93))


# ── Soccer hit rate priors (OVER) ─────────────────────────────────────────────
# Based on general soccer prop market tendencies

_PROP_HIT_RATE_PRIOR = {
    "passes":          0.620,
    "saves":           0.600,
    "shots_on_target": 0.580,
    "clearances":      0.570,
    "tackles":         0.560,
    "shots":           0.555,
    "assists":         0.540,
    "goal_assist":     0.530,
    "shots_assisted":  0.540,
    "fouls":           0.520,
    "goals_allowed":   0.510,
    "goals":           0.490,   # goals are low-frequency, slight under bias
}

def _prop_hit_rate_prior(prop_norm: str, direction: str, pick_type: str = "Standard", deviation_level: float = 0.0) -> float:
    key  = str(prop_norm).lower().strip()
    base = _PROP_HIT_RATE_PRIOR.get(key, 0.530)
    if direction == "UNDER":
        if key == "goals":        return 0.620
        if key == "goals_allowed": return 0.600
        return float(1.0 - base)
    pt = _norm_pick_type(pick_type)
    dev = int(deviation_level) if not (deviation_level != deviation_level) else 0  # nan check
    if pt == "Demon":
        # Demon lines are set above expected outcome — each deviation level drops hit rate
        penalty = {1: 0.08, 2: 0.14, 3: 0.20}.get(dev, 0.08)
        return float(max(base - penalty, 0.30))
    if pt == "Goblin":
        # Goblin lines are set below expected outcome — each level boosts hit rate
        bonus = {1: 0.06, 2: 0.10, 3: 0.14}.get(dev, 0.06)
        return float(min(base + bonus, 0.90))
    return float(base)


def _reliability_mult(pick_type: str) -> float:
    pt = _norm_pick_type(pick_type)
    return {"Standard": 1.00, "Goblin": 1.06, "Demon": 0.75}.get(pt, 0.97)


def _safe_float(x) -> float:
    v = pd.to_numeric(pd.Series([x]), errors="coerce").iloc[0]
    return float(v) if not pd.isna(v) else np.nan


def _edge_transform(edge: float, cap: float = 3.0, power: float = 0.85) -> float:
    if np.isnan(edge):
        return np.nan
    s = 1.0 if edge >= 0 else -1.0
    x = min(abs(edge), cap)
    return s * (x ** power)


def _tier_from_score(score: float) -> str:
    """
    Tier thresholds calibrated for soccer pipeline.
    Note: when many rows lack ESPN IDs/stats, most eligible rows will have
    partial signals. Thresholds are intentionally modest so A/B/C tiers
    still populate when data is partially available.
    """
    if np.isnan(score): return "D"
    if score >= 1.20:   return "A"
    if score >= 0.50:   return "B"
    if score >= 0.10:   return "C"
    return "D"


def _tier_from_score_by_picktype(score: float, pick_type: str) -> str:
    """
    Pick-type-aware tier assignment.
    Demons have structurally lower scores (edge is zeroed, prop_hr_prior is penalized)
    so they use compressed thresholds relative to their own score distribution.
    Goblins and Standards use standard thresholds.
    """
    if np.isnan(score):
        return "D"
    pt = _norm_pick_type(pick_type)
    if pt == "Demon":
        # Demon scores cluster between -0.4 and +0.5 — use relative thresholds
        if score >= 0.30:   return "A"
        if score >= 0.10:   return "B"
        if score >= -0.10:  return "C"
        return "D"
    # Goblin / Standard use original thresholds
    if score >= 1.20:   return "A"
    if score >= 0.50:   return "B"
    if score >= 0.10:   return "C"
    return "D"


def _projection_from_row(row: pd.Series) -> float:
    """Build projection from stat averages if available, else estimate from standard_line / line + offset."""
    # 1. Prefer real stat averages (populated when ESPN data is available)
    for c in ("stat_last5_avg", "stat_last10_avg", "stat_season_avg"):
        v = _safe_float(row.get(c, np.nan))
        if not np.isnan(v):
            return v

    # 2. Use standard_line as projection if present
    std_line = _safe_float(row.get("standard_line", np.nan))
    if not np.isnan(std_line):
        return std_line

    # 3. Estimate standard_line from current line using pick_type + deviation_level offsets
    # These offsets are derived from observed data:
    #   Demon  dev1 → standard ≈ line - 1.0
    #   Demon  dev2 → standard ≈ line - 2.0
    #   Demon  dev3 → standard ≈ line - 3.0
    #   Goblin dev1 → standard ≈ line + 1.0
    #   Goblin dev2 → standard ≈ line + 1.5
    #   Standard    → projection = line (no deviation)
    line_val = _safe_float(row.get("line", np.nan))
    if np.isnan(line_val):
        return np.nan

    pick_type = _norm_pick_type(str(row.get("pick_type", "")))
    dev_level = _safe_float(row.get("deviation_level", np.nan))
    dev = int(dev_level) if not np.isnan(dev_level) else 1

    if pick_type == "Standard":
        return line_val
    elif pick_type == "Goblin":
        offset_map = {1: 1.0, 2: 1.5, 3: 2.0}
        return line_val + offset_map.get(dev, 1.0)
    elif pick_type == "Demon":
        offset_map = {1: -1.0, 2: -2.0, 3: -3.0}
        return line_val + offset_map.get(dev, -1.0)

    return np.nan


def _line_hit_rate_from_row(row: pd.Series) -> float:
    direction = str(row.get("bet_direction", "OVER")).upper()
    hr5 = hr10 = np.nan

    if direction == "UNDER":
        for c in ("line_hit_rate_under_ou_5", "line_hit_rate_under_5"):
            if c in row.index:
                v = _safe_float(row.get(c))
                if not np.isnan(v):
                    hr5 = v; break
        if np.isnan(hr5):
            o = _safe_float(row.get("last5_over",  np.nan))
            u = _safe_float(row.get("last5_under", np.nan))
            if not np.isnan(o) and not np.isnan(u):
                denom = o + u
                hr5 = u / denom if denom > 0 else np.nan
        for c in ("line_hit_rate_under_ou_10", "line_hit_rate_under_10"):
            if c in row.index:
                v = _safe_float(row.get(c))
                if not np.isnan(v):
                    hr10 = v; break
    else:
        for c in ("line_hit_rate_over_ou_5", "line_hit_rate_over_5", "last5_hit_rate"):
            if c in row.index:
                v = _safe_float(row.get(c))
                if not np.isnan(v):
                    hr5 = v; break
        for c in ("line_hit_rate_over_ou_10", "line_hit_rate_over_10"):
            if c in row.index:
                v = _safe_float(row.get(c))
                if not np.isnan(v):
                    hr10 = v; break

    if not np.isnan(hr5) and not np.isnan(hr10):
        return hr5 * 0.50 + hr10 * 0.50
    if not np.isnan(hr5):  return hr5
    if not np.isnan(hr10): return hr10
    return np.nan


def _minutes_certainty(row: pd.Series) -> float:
    tier = str(row.get("minutes_tier", "")).upper()
    if tier in ("HIGH", "MEDIUM", "LOW"):
        return {"HIGH": 1.00, "MEDIUM": 0.90, "LOW": 0.75}[tier]
    # Fallback: infer from pick_type and position when minutes_tier is UNKNOWN
    pick_type = _norm_pick_type(str(row.get("pick_type", "")))
    pos = str(row.get("position_group", "")).upper()
    base = {"Standard": 0.95, "Goblin": 0.85, "Demon": 0.80}.get(pick_type, 0.80)
    if pos in ("GK", "DEF"):
        base = min(base + 0.05, 1.00)
    return base


def _def_adjustment(row: pd.Series, n_teams: int = 15) -> float:
    """Soccer defense adjustment — scale around midpoint of n_teams."""
    rank = _safe_float(row.get("OVERALL_DEF_RANK", np.nan))
    if np.isnan(rank):
        return 0.0
    mid = (n_teams + 1) / 2.0
    return float((rank - mid) / mid * 0.06)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",  required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--n_teams", type=int, default=15, help="Number of teams in defense file")
    args = ap.parse_args()

    print(f"→ Loading: {args.input}")
    out = pd.read_csv(args.input, low_memory=False, encoding="utf-8-sig").fillna("")

    if out.empty:
        print("❌ [SlateIQ-Soccer-S7] Empty input from S6 — aborting.")
        sys.exit(1)

    out["pick_type"] = out.get("pick_type", pd.Series(["Standard"] * len(out))).astype(str).apply(_norm_pick_type)

    # Prop norm map
    _PROP_NORM_MAP = {
        "shots on target":   "shots_on_target",
        "shotsontarget":     "shots_on_target",
        "goalie saves":      "saves",
        "goaliesaves":       "saves",
        "passes attempted":  "passes",
        "passesattempted":   "passes",
        "goals allowed":     "goals_allowed",
        "goalsallowed":      "goals_allowed",
        "goal + assist":     "goal_assist",
        "goalassist":        "goal_assist",
        "shots assisted":    "shots_assisted",
        "shotsassisted":     "shots_assisted",
    }
    out["prop_norm"] = out["prop_norm"].astype(str).str.lower().str.strip().map(
        lambda x: _PROP_NORM_MAP.get(x, x)
    )

    line_num = _to_num(out["line"])
    proj     = out.apply(_projection_from_row, axis=1)
    out["projection"] = proj
    out["edge"]       = proj - line_num
    out["abs_edge"]   = out["edge"].abs()

    forced = out["pick_type"].apply(_forced_over_only).astype(int)
    out["forced_over_only"] = forced

    bet_dir = np.where(forced.eq(1), "OVER", np.where(out["edge"] >= 0, "OVER", "UNDER"))
    out["bet_direction"] = bet_dir

    eligible    = pd.Series(True,  index=out.index)
    void_reason = pd.Series("",    index=out.index)

    miss = line_num.isna() | pd.isna(out["projection"])
    eligible.loc[miss]    = False
    void_reason.loc[miss] = "NO_PROJECTION_OR_LINE"

    # Only void Goblin with negative edge - Goblin lines are set LOW so negative edge = bad data
    # Demon lines are set HIGH (negative edge is expected/by design) - keep them eligible
    goblin_neg = (out["pick_type"] == "Goblin") & (out["edge"] < 0)
    eligible.loc[goblin_neg]    = False
    void_reason.loc[goblin_neg] = "FORCED_OVER_NEG_EDGE"

    out["eligible"]    = eligible.astype(int)
    out["void_reason"] = void_reason

    out["edge_dr"]          = out["edge"].apply(_edge_transform)
    out["line_hit_rate"]    = out.apply(_line_hit_rate_from_row, axis=1)
    out["minutes_certainty"] = out.apply(_minutes_certainty, axis=1)
    out["prop_weight"]      = out["prop_norm"].astype(str).apply(_prop_weight)
    out["reliability_mult"] = out["pick_type"].astype(str).apply(_reliability_mult)

    elig_mask = out["eligible"].astype(int).eq(1)

    def zcol(s: pd.Series, direction_aware: bool = False) -> pd.Series:
        x      = pd.to_numeric(s, errors="coerce")
        result = pd.Series([0.0] * len(x), index=x.index)
        if direction_aware and "bet_direction" in out.columns:
            for direction in ("OVER", "UNDER"):
                dir_mask = elig_mask & (out["bet_direction"].astype(str).str.upper() == direction)
                if dir_mask.sum() < 2: continue
                mu = x[dir_mask].mean()
                sd = x[dir_mask].std()
                if sd and not np.isnan(sd) and sd > 1e-9:
                    z_vals = (x[dir_mask] - mu) / sd
                    result.loc[dir_mask.index[dir_mask]] = z_vals.values
            return result
        mu = x[elig_mask].mean()
        sd = x[elig_mask].std()
        if sd and not np.isnan(sd) and sd > 1e-9:
            return (x - mu) / sd
        return result

    out["edge_z"]      = zcol(out["edge"],           direction_aware=True)
    out["line_hit_z"]  = zcol(out["line_hit_rate"],  direction_aware=True)
    out["min_z"]       = zcol(out["minutes_certainty"])

    def_adj = out.apply(lambda r: _def_adjustment(r, args.n_teams), axis=1)
    out["def_adj"]         = def_adj
    out["projection_adj"]  = pd.to_numeric(out["projection"], errors="coerce") * (1.0 + def_adj.astype(float))
    out["edge_adj"]        = out["projection_adj"] - line_num

    def _edge_adj_dr(row_idx):
        x = out["edge_adj"].iloc[row_idx]
        if pd.isna(x): return np.nan
        direction = str(out["bet_direction"].iloc[row_idx]).upper()
        signed = -float(x) if direction == "UNDER" else float(x)
        return _edge_transform(signed)

    out["edge_adj_dr"] = pd.Series([_edge_adj_dr(i) for i in range(len(out))], index=out.index)

    def _def_rank_signal(row: pd.Series) -> float:
        rank      = _safe_float(row.get("OVERALL_DEF_RANK", np.nan))
        direction = str(row.get("bet_direction", "OVER")).upper()
        if np.isnan(rank): return 0.0
        signal = (rank - 1.0) / (args.n_teams - 1.0) * 2.0 - 1.0
        return float(signal if direction == "OVER" else -signal)

    def_signal = out.apply(_def_rank_signal, axis=1)
    out["def_rank_signal"] = def_signal
    out["def_rank_z"]      = zcol(def_signal, direction_aware=True)

    line_num_filled = line_num.fillna(0)
    for col in ("stat_last5_avg", "stat_last10_avg", "stat_season_avg"):
        num_col = col + "_num"
        out[num_col] = _to_num(out[col]) if col in out.columns else pd.Series([np.nan] * len(out), index=out.index)

    def _avg_vs_line(row_idx):
        l = line_num_filled.iloc[row_idx]
        if l == 0 or np.isnan(l): return 0.0
        direction = str(out["bet_direction"].iloc[row_idx]).upper()
        score = total_w = 0.0
        for col, w in [("stat_last5_avg_num", 0.50), ("stat_last10_avg_num", 0.30), ("stat_season_avg_num", 0.20)]:
            v = out[col].iloc[row_idx]
            if not np.isnan(v):
                raw = np.clip((v - l) / l, -1.0, 1.0)
                if direction == "UNDER": raw = -raw
                score   += raw * w
                total_w += w
        return float(score / total_w) if total_w > 0.1 else 0.0

    avg_vs_line = pd.Series([_avg_vs_line(i) for i in range(len(out))], index=out.index)
    out["avg_vs_line"]   = avg_vs_line
    out["avg_vs_line_z"] = zcol(avg_vs_line, direction_aware=True)

    prop_hr_prior = out.apply(
        lambda r: _prop_hit_rate_prior(
            r.get("prop_norm", ""),
            str(r.get("bet_direction", "OVER")).upper(),
            str(r.get("pick_type", "Standard")),
            float(r.get("deviation_level", 0) or 0)
        ),
        axis=1
    )
    out["prop_hr_prior"] = prop_hr_prior
    out["prop_hr_z"]     = zcol(prop_hr_prior, direction_aware=True)

    # For Demon picks: edge is always negative by design (line set high, forced OVER).
    # Edge signal is not informative for Demons — zero it out so they rank on
    # hit rate, defense, prop weight, and minutes certainty instead.
    is_demon = (out["pick_type"].astype(str) == "Demon")
    edge_component = out["edge_adj_dr"].astype(float).where(~is_demon, 0.0).fillna(0.0)

    score = (
        edge_component                                     * 0.85
        + out["line_hit_z"].astype(float).fillna(0.0)    * 0.85
        + out["avg_vs_line_z"].astype(float).fillna(0.0) * 0.75
        + out["def_rank_z"].astype(float).fillna(0.0)    * 0.80
        + out["prop_hr_z"].astype(float).fillna(0.0)     * 0.50
        + out["min_z"].astype(float).fillna(0.0)         * 0.25
    )
    score = (
        score
        * out["prop_weight"].astype(float).fillna(1.0)
        * out["reliability_mult"].astype(float).fillna(1.0)
    )
    score = score.where(elig_mask, np.nan)

    out["rank_score"] = score
    out["tier"]       = out.apply(
        lambda r: _tier_from_score_by_picktype(r["rank_score"], str(r.get("pick_type", "Standard"))),
        axis=1
    )

    # ── Tier A / B sheets so step8/step9 always have a usable sheet ──
    with pd.ExcelWriter(args.output, engine="openpyxl") as w:
        out.to_excel(w, sheet_name="ALL",      index=False)
        out.loc[elig_mask].to_excel(w, sheet_name="ELIGIBLE", index=False)
        for _tier in ["A", "B", "C", "D"]:
            _mask = out["tier"] == _tier
            if _mask.any():
                out.loc[_mask].to_excel(w, sheet_name=f"Tier {_tier}", index=False)

    if elig_mask.sum() == 0:
        print("❌ [SlateIQ-Soccer-S7] No eligible props after scoring — aborting.")
        sys.exit(1)

    print(f"✅ Saved → {args.output}")
    print(f"ALL rows: {len(out)}")
    print("\nTier counts:")
    print(out["tier"].value_counts().to_string())
    print("\nIneligible reasons:")
    vr = out.loc[~elig_mask, "void_reason"].value_counts()
    print(vr.to_string() if len(vr) else "(none)")
    print("\nScore percentiles (eligible):")
    rs = pd.to_numeric(out.loc[elig_mask, "rank_score"], errors="coerce")
    print(rs.quantile([0.50, 0.70, 0.80, 0.85, 0.90, 0.95]).round(3).to_string())


if __name__ == "__main__":
    main()
