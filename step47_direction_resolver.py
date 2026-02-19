#!/usr/bin/env python3
"""
step47_direction_resolver.py  (Pipeline A - Step 4.7)

Resolves FINAL BET DIRECTION using:
- model edge (from Step 5)
- last-5 line hit rates (OVER vs UNDER)
- team role context (from Step 46)
- Goblin/Demon OVER-only constraint
- optional allowed_dir constraint if present

Outputs:
- final_bet_direction
- final_dir_reason
- model_dir
"""

from __future__ import annotations

import argparse
from typing import Optional, List, Dict

import pandas as pd
import numpy as np


# -----------------------------
# Column aliases
# -----------------------------
ROLE_ALIASES = [
    "role_bucket",
    "team_role_bucket",
    "team_role",
    "role",
    "usage_role_bucket",
    "primary_role_bucket",
    "primary_bucket",
]

EDGE_ALIASES = [
    "edge",
    "model_edge",
    "edge_model",
    "proj_edge",
]

PICKTYPE_ALIASES = [
    "pick_type",
    "picktype",
    "pick_type_key",
    "projection_type",
]

ALLOWED_DIR_ALIASES = [
    "allowed_dir",
    "allowed_direction",
    "dir_allowed",
]

# Join keys: we will try to auto-detect if user passes generic keys
KEY_ALIASES: Dict[str, List[str]] = {
    "player_id": ["player_id", "nba_player_id", "player", "player_name"],
    "prop_type": ["prop_type", "prop_type_norm", "prop_norm", "stat_type", "market"],
    "line": ["line", "prop_line", "projection", "projection_line"],
    "team": ["team", "TEAM_ABBR", "team_abbr"],
    "opp_team": ["opp_team", "opponent", "opp", "OPP_ABBR", "opp_abbr"],
}


# -----------------------------
# Helpers
# -----------------------------
def _read_any(path: str) -> pd.DataFrame:
    p = path.lower()
    if p.endswith(".xlsx") or p.endswith(".xls"):
        return pd.read_excel(path)
    # be permissive with encoding for CSVs
    try:
        return pd.read_csv(path)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin-1")


def _first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def _find_key(df: pd.DataFrame, key_name: str) -> Optional[str]:
    """Find a best-match column in df for the canonical key_name."""
    for cand in KEY_ALIASES.get(key_name, []):
        if cand in df.columns:
            return cand
    return None


def _normalize_text_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()


def _is_goblin_or_demon(pick: str) -> bool:
    p = (pick or "").lower()
    return ("gob" in p) or ("dem" in p)


def _normalize_allowed_dir(x: str) -> str:
    v = (x or "").upper().strip()
    # common values seen in your pipelines
    # BOTH, OVER_ONLY, UNDER_ONLY
    if v in {"BOTH", "OVER_ONLY", "UNDER_ONLY"}:
        return v
    if v in {"OVER", "ONLY_OVER"}:
        return "OVER_ONLY"
    if v in {"UNDER", "ONLY_UNDER"}:
        return "UNDER_ONLY"
    if v == "":
        return ""
    return v


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Step46 CSV/XLSX (or Step5 if already merged)")
    ap.add_argument("--output", required=True, help="Output CSV")
    ap.add_argument("--edgefile", default=None, help="Optional Step5 XLSX/CSV to merge edge if missing")
    ap.add_argument(
        "--keycols",
        default="player_id,prop_type,line",
        help="Join keys for merging edgefile (comma-separated). Default: player_id,prop_type,line",
    )
    ap.add_argument(
        "--min_diff",
        type=float,
        default=0.03,
        help="Minimum hit-rate difference needed to override model direction (default: 0.03)",
    )
    args = ap.parse_args()

    print("→ Loading:", args.input)
    df = _read_any(args.input).copy()

    # Resolve alias columns in INPUT
    edge_col = _first_existing(df, EDGE_ALIASES)
    role_col = _first_existing(df, ROLE_ALIASES)
    pick_col = _first_existing(df, PICKTYPE_ALIASES)
    allowed_dir_col = _first_existing(df, ALLOWED_DIR_ALIASES)

    # If edge missing, merge from edgefile (Step 5)
    if edge_col is None and args.edgefile:
        print("⚠️ edge missing — merging from edgefile:", args.edgefile)
        edf = _read_any(args.edgefile).copy()

        edge_in_edf = _first_existing(edf, EDGE_ALIASES)
        if edge_in_edf is None:
            raise RuntimeError("edgefile provided but no edge column found in it.")

        # parse requested keys, then map them to real columns in each df
        requested_keys = [k.strip() for k in args.keycols.split(",") if k.strip()]
        key_map_input = {}
        key_map_edge = {}

        for k in requested_keys:
            # try exact first
            in_col = k if k in df.columns else _find_key(df, k)
            ed_col = k if k in edf.columns else _find_key(edf, k)
            if in_col is None or ed_col is None:
                raise RuntimeError(f"Merge key '{k}' not found in input or edgefile (after alias matching).")
            key_map_input[k] = in_col
            key_map_edge[k] = ed_col

        # build normalized join frames
        df_join = df.copy()
        edf_join = edf.copy()

        for k in requested_keys:
            df_join[k] = _normalize_text_series(df_join[key_map_input[k]])
            edf_join[k] = _normalize_text_series(edf_join[key_map_edge[k]])

        edf_small = edf_join[requested_keys + [edge_in_edf]].drop_duplicates()
        df = df_join.merge(edf_small, on=requested_keys, how="left", suffixes=("", "_edgefile"))

        # if input had no edge, the merged column name is edge_in_edf
        edge_col = edge_in_edf

        # refresh alias cols (edge now exists)
        if role_col is None:
            role_col = _first_existing(df, ROLE_ALIASES)
        if pick_col is None:
            pick_col = _first_existing(df, PICKTYPE_ALIASES)
        if allowed_dir_col is None:
            allowed_dir_col = _first_existing(df, ALLOWED_DIR_ALIASES)

    # Required columns
    required_missing = []
    if edge_col is None:
        required_missing.append("edge (or alias)")
    if role_col is None:
        required_missing.append("role_bucket (or alias)")
    if pick_col is None:
        required_missing.append("pick_type (or alias)")

    # Hit rate cols required
    for c in ["line_hit_rate_over_5", "line_hit_rate_under_5"]:
        if c not in df.columns:
            required_missing.append(c)

    if required_missing:
        raise RuntimeError(f"Missing required columns: {sorted(set(required_missing))}")

    # Normalize numerics
    df[edge_col] = pd.to_numeric(df[edge_col], errors="coerce").fillna(0.0)
    df["line_hit_rate_over_5"] = pd.to_numeric(df["line_hit_rate_over_5"], errors="coerce").fillna(0.0)
    df["line_hit_rate_under_5"] = pd.to_numeric(df["line_hit_rate_under_5"], errors="coerce").fillna(0.0)

    # Model direction from edge
    df["model_dir"] = np.where(df[edge_col] >= 0, "OVER", "UNDER")
    df["model_abs_edge"] = np.abs(df[edge_col])

    # Normalize allowed_dir if present
    if allowed_dir_col is not None:
        df[allowed_dir_col] = df[allowed_dir_col].apply(_normalize_allowed_dir)

    def resolve(row):
        over_score = float(row["line_hit_rate_over_5"])
        under_score = float(row["line_hit_rate_under_5"])

        role = str(row[role_col]).upper().strip()
        pick = str(row[pick_col]).strip()

        # Role weighting: (kept symmetric but can dampen role players)
        if role in ("PRIMARY", "STAR", "CORE"):
            over_score *= 1.20
            under_score *= 1.20
            role_tag = "ROLE_PRIMARY"
        elif role in ("ROLE", "SECONDARY"):
            over_score *= 0.85
            under_score *= 0.85
            role_tag = "ROLE_SECONDARY"
        else:
            role_tag = "ROLE_OTHER"

        # Enforce Goblin/Demon OVER-only
        if _is_goblin_or_demon(pick):
            # if allowed_dir exists and says UNDER_ONLY, that would be inconsistent; still force OVER for gob/dem
            return "OVER", "FORCED_OVER_ONLY_GOB_DEM"

        # Enforce allowed_dir if present
        if allowed_dir_col is not None:
            allowed = str(row[allowed_dir_col]).upper().strip()
            if allowed == "OVER_ONLY":
                return "OVER", "FORCED_OVER_ONLY_ALLOWED_DIR"
            if allowed == "UNDER_ONLY":
                return "UNDER", "FORCED_UNDER_ONLY_ALLOWED_DIR"

        # Decide based on hit-rate tendency, but require a meaningful gap
        diff = over_score - under_score
        min_diff = float(args.min_diff)

        if diff >= min_diff:
            return "OVER", f"LINE_HIT_OVER_{role_tag}"
        if diff <= -min_diff:
            return "UNDER", f"LINE_HIT_UNDER_{role_tag}"

        # If too close (or equal), use model direction
        # Add reason including closeness
        model_dir = row["model_dir"]
        return model_dir, f"MODEL_TIEBREAK_DIFF<{min_diff:.2f}"

    out = df.apply(resolve, axis=1, result_type="expand")
    df["final_bet_direction"] = out[0]
    df["final_dir_reason"] = out[1]

    df.to_csv(args.output, index=False)
    print(f"✅ Saved → {args.output}")
    print(df["final_bet_direction"].value_counts(dropna=False))

if __name__ == "__main__":
    main()
