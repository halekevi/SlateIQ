#!/usr/bin/env python3
"""
step10_ticket_generator.py  (Pipeline A - Step 10)

Builds Draft PrizePicks slips from a *previous slate* (default: yesterday).

DEFAULT SLATE BEHAVIOR
- If --slate_date is not provided, uses "yesterday" (local).
- If --infile is not provided, it will try (in order):
    1) step9_formatted_ranked_<slate_date>.xlsx
    2) step9_formatted_ranked.xlsx
    3) step8_all_direction_<slate_date>.csv
    4) step8_all_direction.csv

Filters (defaults):
- Tiers: A,B,C
- Momentum:
    * OVER picks require last5_over >= 3
    * UNDER picks require last5_under >= 3
- OVERS: Standard + Goblin + Demon allowed (OVER-only constraint enforced)
- UNDERS: Standard ONLY
- No duplicate players within a slip
- No duplicate player+prop within a slip

Outputs:
- step10_tickets_<slate_date>.xlsx with sheets:
  SUMMARY, FILTERED_PICKS, OVERS_3/4/5/6, UNDERS_3/4/5/6

Run examples:
  py -3.14 step10_ticket_generator.py
  py -3.14 step10_ticket_generator.py --slate_date 2026-02-12
  py -3.14 step10_ticket_generator.py --slate_date 2026-02-12 --infile step9_formatted_ranked.xlsx --out step10_tickets_2026-02-12.xlsx
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


# -----------------------------
# IO
# -----------------------------
def _read_any(path: Path, sheet: Optional[str] = None) -> pd.DataFrame:
    suf = path.suffix.lower()
    if suf == ".csv":
        try:
            return pd.read_csv(path, low_memory=False)
        except UnicodeDecodeError:
            return pd.read_csv(path, encoding="latin-1", low_memory=False)
    if suf in {".xlsx", ".xlsm", ".xls"}:
        if sheet:
            return pd.read_excel(path, sheet_name=sheet)
        xls = pd.ExcelFile(path)
        if "ALL" in xls.sheet_names:
            return pd.read_excel(path, sheet_name="ALL")
        return pd.read_excel(path, sheet_name=xls.sheet_names[0])
    raise SystemExit(f"Unsupported input type: {suf}. Use .csv or .xlsx")


def _coalesce_col(df: pd.DataFrame, *cands: str) -> Optional[str]:
    cols_l = {c.lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in cols_l:
            return cols_l[c.lower()]
    return None


def _first_existing(paths: List[Path]) -> Optional[Path]:
    for p in paths:
        if p and p.exists():
            return p
    return None


# -----------------------------
# Normalization
# -----------------------------
def pick_type_key(x) -> str:
    s = str(x).strip().lower()
    if "gob" in s:
        return "goblin"
    if "dem" in s:
        return "demon"
    return "standard"


def _to_bool_series(x: pd.Series) -> pd.Series:
    if isinstance(x, pd.DataFrame):
        x = x.iloc[:, 0]
    return x.astype(str).str.strip().str.lower().isin(["1", "true", "yes", "y"])


def _safe_num(x) -> float:
    try:
        if pd.isna(x):
            return np.nan
        return float(x)
    except Exception:
        return np.nan


def _directional_last5(row: pd.Series) -> float:
    """Return last5_over if betting OVER, else last5_under if betting UNDER."""
    d = str(row.get("bet_direction", "")).strip().upper()
    if d == "UNDER":
        return _safe_num(row.get("last5_under", np.nan))
    return _safe_num(row.get("last5_over", np.nan))


# -----------------------------
# Slip building
# -----------------------------
def _score_col(df: pd.DataFrame) -> str:
    """Pick best available score column for sorting."""
    for c in ["rank_score", "edge_dr", "abs_edge", "edge", "projection"]:
        if c in df.columns:
            return c
    return ""


def build_slip(pool: pd.DataFrame, leg_count: int) -> pd.DataFrame:
    """
    Greedy build: choose highest score rows without repeating players or player+prop.
    Returns chosen rows (<= leg_count).
    """
    chosen_idx: List[int] = []
    used_players = set()
    used_player_prop = set()

    for idx, r in pool.iterrows():
        player = str(r.get("player", "")).strip()
        prop = str(r.get("prop_norm", r.get("prop_type_norm", r.get("prop_type", "")))).strip().lower()
        key_pp = (player.lower(), prop)

        if player == "":
            continue
        if player.lower() in used_players:
            continue
        if key_pp in used_player_prop:
            continue

        chosen_idx.append(idx)
        used_players.add(player.lower())
        used_player_prop.add(key_pp)

        if len(chosen_idx) >= leg_count:
            break

    return pool.loc[chosen_idx].copy()


def build_multiple_slips(pool: pd.DataFrame, leg_count: int, num_slips: int) -> List[pd.DataFrame]:
    """
    Build multiple slips by removing used players from previous slip.
    Deterministic.
    """
    slips: List[pd.DataFrame] = []
    remaining = pool.copy()

    for _ in range(num_slips):
        slip = build_slip(remaining, leg_count)
        if len(slip) < leg_count:
            break
        slips.append(slip)

        used = set(slip["player"].astype(str).str.lower().tolist())
        remaining = remaining.loc[~remaining["player"].astype(str).str.lower().isin(used)].copy()

    return slips


def slips_to_table(slips: List[pd.DataFrame], slip_label_prefix: str) -> pd.DataFrame:
    rows = []
    for i, slip in enumerate(slips, start=1):
        sid = f"{slip_label_prefix}-{i:02d}"
        for j, r in slip.reset_index(drop=True).iterrows():
            rows.append({
                "slip_id": sid,
                "leg": j + 1,
                "player": r.get("player"),
                "team": r.get("team"),
                "opp_team": r.get("opp_team"),
                "prop_type": r.get("prop_type"),
                "prop_norm": r.get("prop_norm"),
                "line": r.get("line"),
                "pick_type": r.get("pick_type"),
                "pick_type_key": r.get("pick_type_key"),
                "bet_direction": r.get("bet_direction"),
                "tier": r.get("tier"),
                "score": r.get("score"),
                "abs_edge": r.get("abs_edge"),
                "edge": r.get("edge"),
                "last5_over": r.get("last5_over"),
                "last5_under": r.get("last5_under"),
                "dir_last5": r.get("dir_last5"),
            })
    return pd.DataFrame(rows)


def resolve_defaults(slate_date: str, infile_arg: Optional[str]) -> Path:
    """
    If infile isn't provided, try standard filenames that include slate_date.
    """
    if infile_arg:
        p = Path(infile_arg)
        if not p.exists():
            raise FileNotFoundError(f"--infile not found: {p}")
        return p

    candidates = [
        Path(f"step9_formatted_ranked_{slate_date}.xlsx"),
        Path("step9_formatted_ranked.xlsx"),
        Path(f"step8_all_direction_{slate_date}.csv"),
        Path("step8_all_direction.csv"),
    ]
    found = _first_existing(candidates)
    if not found:
        raise FileNotFoundError(
            "No default infile found. Tried: "
            + ", ".join([c.as_posix() for c in candidates])
        )
    return found


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slate_date", default=None, help="Slate date tag YYYY-MM-DD (default: yesterday).")
    ap.add_argument("--infile", default=None, help="Optional explicit infile (Step9 XLSX or Step8 CSV/XLSX).")
    ap.add_argument("--sheet", default=None, help="(XLSX only) sheet to read. Default: ALL if present else first.")
    ap.add_argument("--out", default=None, help="Output XLSX. Default: step10_tickets_<slate_date>.xlsx")
    ap.add_argument("--tiers", default="A,B,C", help="Comma tiers to include (default A,B,C).")
    ap.add_argument("--min_last5", type=int, default=3, help="Min directional last5 hits (default 3).")
    ap.add_argument("--overs_slips", type=int, default=3, help="How many OVER slips per leg-count (default 3).")
    ap.add_argument("--unders_slips", type=int, default=2, help="How many UNDER slips per leg-count (default 2).")
    ap.add_argument("--leg_counts", default="3,4,5,6", help="Comma leg counts to build (default 3,4,5,6).")
    ap.add_argument("--include_d", action="store_true", help="Include Tier D too (overrides --tiers).")
    ap.add_argument("--no_momentum_filter", action="store_true", help="Disable last5 directional filter.")
    args = ap.parse_args()

    # default slate_date = yesterday (local machine time)
    slate_date = args.slate_date or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    in_path = resolve_defaults(slate_date, args.infile)
    out_xlsx = Path(args.out) if args.out else Path(f"step10_tickets_{slate_date}.xlsx")

    print(f"→ Slate date: {slate_date}")
    print(f"→ Loading: {in_path}")
    df = _read_any(in_path, sheet=args.sheet).copy()

    # Canonical columns
    if "player" not in df.columns:
        pcol = _coalesce_col(df, "player", "name", "player_name")
        if pcol:
            df["player"] = df[pcol]
        else:
            raise RuntimeError("Missing player column.")

    if "line" not in df.columns:
        raise RuntimeError("Missing line column.")
    df["line"] = pd.to_numeric(df["line"], errors="coerce")

    if "prop_norm" not in df.columns:
        src = _coalesce_col(df, "prop_norm", "prop_type_norm", "prop_type", "prop")
        df["prop_norm"] = df[src].astype(str).str.lower().str.strip() if src else ""

    if "bet_direction" not in df.columns:
        if "final_bet_direction" in df.columns:
            df["bet_direction"] = df["final_bet_direction"]
        else:
            if "edge" in df.columns:
                df["bet_direction"] = np.where(pd.to_numeric(df["edge"], errors="coerce") >= 0, "OVER", "UNDER")
            else:
                df["bet_direction"] = "OVER"

    if "pick_type" not in df.columns:
        df["pick_type"] = "Standard"
    df["pick_type_key"] = df["pick_type"].apply(pick_type_key)

    if "tier" not in df.columns:
        df["tier"] = "NA"

    # Eligible only, if present
    if "eligible" in df.columns:
        before = len(df)
        df = df.loc[_to_bool_series(df["eligible"])].copy()
        print(f"→ Filtering eligible only: {len(df)}/{before}")

    # Tier filter
    if args.include_d:
        tiers = {"A", "B", "C", "D"}
    else:
        tiers = {t.strip().upper() for t in args.tiers.split(",") if t.strip()}
    df = df.loc[df["tier"].astype(str).str.upper().isin(tiers)].copy()
    print(f"→ Tier filter: {sorted(tiers)} => {len(df)} rows")

    # Score
    sc = _score_col(df)
    df["score"] = pd.to_numeric(df[sc], errors="coerce") if sc else np.nan

    # last5
    df["last5_over"] = pd.to_numeric(df.get("last5_over", np.nan), errors="coerce")
    df["last5_under"] = pd.to_numeric(df.get("last5_under", np.nan), errors="coerce")

    df["bet_direction"] = df["bet_direction"].astype(str).str.upper().str.strip()
    df["dir_last5"] = df.apply(_directional_last5, axis=1)

    # Momentum filter
    if not args.no_momentum_filter:
        before = len(df)
        df = df.loc[df["dir_last5"].fillna(-1) >= args.min_last5].copy()
        print(f"→ Momentum filter (dir_last5>={args.min_last5}): {len(df)}/{before}")

    # Platform constraint: Goblin/Demon OVER-only
    before = len(df)
    df = df.loc[~((df["pick_type_key"].isin(["goblin", "demon"])) & (df["bet_direction"] == "UNDER"))].copy()
    if len(df) != before:
        print(f"→ Dropped OVER-only constrained rows that were UNDER: {before - len(df)}")

    # Split pools
    overs = df.loc[df["bet_direction"] == "OVER"].copy()
    unders = df.loc[(df["bet_direction"] == "UNDER") & (df["pick_type_key"] == "standard")].copy()

    # Sort pools
    sort_cols = ["score"]
    if "abs_edge" in df.columns:
        df["abs_edge"] = pd.to_numeric(df["abs_edge"], errors="coerce")
        overs["abs_edge"] = pd.to_numeric(overs.get("abs_edge", np.nan), errors="coerce")
        unders["abs_edge"] = pd.to_numeric(unders.get("abs_edge", np.nan), errors="coerce")
        sort_cols = ["score", "abs_edge"]

    overs = overs.sort_values(sort_cols, ascending=False, na_position="last").reset_index(drop=True)
    unders = unders.sort_values(sort_cols, ascending=False, na_position="last").reset_index(drop=True)

    print(f"→ Pool sizes: OVERS={len(overs)} | UNDERS={len(unders)}")

    leg_counts = [int(x.strip()) for x in args.leg_counts.split(",") if x.strip()]
    results: Dict[str, pd.DataFrame] = {}

    summary_rows = []
    for lc in leg_counts:
        o_slips = build_multiple_slips(overs, lc, args.overs_slips)
        u_slips = build_multiple_slips(unders, lc, args.unders_slips)

        results[f"OVERS_{lc}"] = slips_to_table(o_slips, f"OV{lc}")
        results[f"UNDERS_{lc}"] = slips_to_table(u_slips, f"UN{lc}")

        summary_rows.append({"bucket": f"OVERS_{lc}", "slips": len(o_slips), "legs_total": len(results[f'OVERS_{lc}'])})
        summary_rows.append({"bucket": f"UNDERS_{lc}", "slips": len(u_slips), "legs_total": len(results[f'UNDERS_{lc}'])})

    keep_cols = [c for c in [
        "nba_player_id", "player", "team", "opp_team", "tier", "pick_type", "pick_type_key",
        "bet_direction", "prop_type", "prop_norm", "line",
        "score", "edge", "abs_edge", "last5_over", "last5_under", "dir_last5"
    ] if c in df.columns]

    results["SUMMARY"] = pd.DataFrame(summary_rows)
    results["FILTERED_PICKS"] = df[keep_cols].copy()

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        results["SUMMARY"].to_excel(writer, index=False, sheet_name="SUMMARY")
        results["FILTERED_PICKS"].to_excel(writer, index=False, sheet_name="FILTERED_PICKS")
        for lc in leg_counts:
            results[f"OVERS_{lc}"].to_excel(writer, index=False, sheet_name=f"OVERS_{lc}")
            results[f"UNDERS_{lc}"].to_excel(writer, index=False, sheet_name=f"UNDERS_{lc}")

    print(f"✅ Saved → {out_xlsx}")


if __name__ == "__main__":
    main()
