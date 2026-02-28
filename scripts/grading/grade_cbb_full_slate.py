#!/usr/bin/env python3
"""
grade_cbb_full_slate.py  (v2 — with defensive tier grading)

Grades a FULL CBB slate using:
- Slate file: step3b or step5b CSV (must have line, prop_norm, model_dir_5 or final_bet_direction,
  espn_athlete_id, and optionally opp_def_tier / OVERALL_DEF_RANK from step3b)
- Actuals file: cbb_actuals_YYYY-MM-DD.csv (from fetch_cbb_actuals_by_date.py)

New vs original:
- Carries opp_def_tier, opp_def_rank through to graded output
- Prints full summary table: hits/misses by Defensive Tier x Pick Type x Tier (A/B/C/D)
- Writes multi-sheet Excel: Summary + By Def Tier + Box Raw

One-line example:
  py -3.14 grade_cbb_full_slate.py \\
      --slate step5b_with_stats_cbb.csv \\
      --actuals cbb_actuals_2026-02-20.csv \\
      --out cbb_graded_2026-02-20.xlsx
"""

from __future__ import annotations

import argparse
from typing import Optional

import pandas as pd
import numpy as np


# ── Helpers ───────────────────────────────────────────────────────────────────

def to_float(x) -> float:
    try:
        s = str(x).strip()
        if s in ("", "none", "nan", "--"):
            return np.nan
        return float(s)
    except Exception:
        return np.nan


def stat_from_row(actuals_row: pd.Series, prop_norm: str) -> float:
    p = (prop_norm or "").strip().lower()

    pts = to_float(actuals_row.get("PTS"))
    reb = to_float(actuals_row.get("REB"))
    ast = to_float(actuals_row.get("AST"))
    stl = to_float(actuals_row.get("STL"))
    blk = to_float(actuals_row.get("BLK"))
    tov = to_float(actuals_row.get("TO"))
    pm3 = to_float(actuals_row.get("3PT") or actuals_row.get("3PM"))

    if p in ("pts", "points"):             return pts
    if p in ("reb", "rebs", "rebounds"):   return reb
    if p in ("ast", "assists"):            return ast
    if p in ("stl", "steals"):             return stl
    if p in ("blk", "blocks"):             return blk
    if p in ("tov", "to", "turnovers"):    return tov
    if p in ("3pm", "3-pt made"):          return pm3
    if p in ("pr", "pts+rebs", "pts+reb"): return pts + reb
    if p in ("pa", "pts+asts", "pts+ast"): return pts + ast
    if p in ("ra", "rebs+asts", "reb+ast"):return reb + ast
    if p in ("pra", "pts+rebs+asts"):      return pts + reb + ast
    if p in ("stocks", "stl+blk"):         return stl + blk
    if "fantasy" in p:
        return pts + 1.2*reb + 1.5*ast + 3*stl + 3*blk - tov

    return np.nan


def grade_row(actual_value: float, line: float, dir_played: str) -> str:
    if np.isnan(actual_value) or np.isnan(line) or not dir_played:
        return "VOID"
    d = dir_played.strip().upper()
    if d == "VOID_FORCED_UNDER":
        return "VOID"  # Goblin/Demon that were incorrectly set as UNDER
    if abs(actual_value - line) < 1e-9:
        return "PUSH"
    if d == "OVER":
        return "HIT" if actual_value > line else "MISS"
    if d == "UNDER":
        return "HIT" if actual_value < line else "MISS"
    return "VOID"


# ── Summary builder ───────────────────────────────────────────────────────────

def _hr(hits, total):
    return f"{hits/total:.1%}" if total > 0 else "—"


def build_summary_block(df: pd.DataFrame, label_col: str, label_vals: list,
                         title: str, pick_type_col: str = None) -> pd.DataFrame:
    """Build hits/misses/hit-rate table with OVER/UNDER split for Standard
    and separate Goblin/Demon columns for each grouping value."""
    decided = df[df["result"].isin(["HIT", "MISS"])]
    rows = []
    for val in label_vals:
        sub = decided[decided[label_col].astype(str) == str(val)]
        all_sub = df[df[label_col].astype(str) == str(val)]

        # Overall
        hits   = (sub["result"] == "HIT").sum()
        misses = (sub["result"] == "MISS").sum()
        total  = hits + misses
        voids  = all_sub["result"].isin(["VOID", "PUSH"]).sum()

        row = {
            "Group":    title,
            "Label":    val,
            "Hits":     hits,
            "Misses":   misses,
            "Decided":  total,
            "Voids":    voids,
            "Hit Rate": _hr(hits, total),
        }

        if pick_type_col and pick_type_col in df.columns:
            # Standard OVER
            std_over = sub[(sub[pick_type_col] == "Standard") & (sub["dir_played"] == "OVER")]
            h, t = (std_over["result"] == "HIT").sum(), len(std_over)
            row["Std OVER HR"] = _hr(h, t)
            row["Std OVER N"]  = t

            # Standard UNDER
            std_under = sub[(sub[pick_type_col] == "Standard") & (sub["dir_played"] == "UNDER")]
            h, t = (std_under["result"] == "HIT").sum(), len(std_under)
            row["Std UNDER HR"] = _hr(h, t)
            row["Std UNDER N"]  = t

            # Goblin (always OVER)
            gob = sub[sub[pick_type_col] == "Goblin"]
            h, t = (gob["result"] == "HIT").sum(), len(gob)
            row["Goblin HR"] = _hr(h, t)
            row["Goblin N"]  = t

            # Demon (always OVER)
            dem = sub[sub[pick_type_col] == "Demon"]
            h, t = (dem["result"] == "HIT").sum(), len(dem)
            row["Demon HR"] = _hr(h, t)
            row["Demon N"]  = t

        rows.append(row)
    return pd.DataFrame(rows)


def build_crosstab(df: pd.DataFrame, row_col: str, col_col: str,
                   row_vals: list, col_vals: list) -> pd.DataFrame:
    """Build a crosstab of hit rate: row_col x col_col."""
    decided = df[df["result"].isin(["HIT", "MISS"])]
    records = []
    for rv in row_vals:
        row_data = {"": rv}
        for cv in col_vals:
            sub = decided[(decided[row_col].astype(str) == str(rv)) &
                          (decided[col_col].astype(str) == str(cv))]
            hits   = (sub["result"] == "HIT").sum()
            misses = (sub["result"] == "MISS").sum()
            total  = hits + misses
            row_data[f"{cv} H/M"] = f"{hits}/{misses}"
            row_data[f"{cv} HR"]  = _hr(hits, total)
        records.append(row_data)
    return pd.DataFrame(records)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slate",   required=True, help="Slate CSV (step5b or step3b output)")
    ap.add_argument("--actuals", required=True, help="Actuals CSV from ESPN fetcher")
    ap.add_argument("--out",     required=True, help="Output .xlsx")
    ap.add_argument("--date",    default="",    help="Slate date YYYY-MM-DD (for labeling only)")
    args = ap.parse_args()

    date_label = args.date.strip() if args.date else ""

    # ── Load ────────────────────────────────────────────────────────────────
    slate   = pd.read_csv(args.slate,   dtype=str).fillna("")
    actuals = pd.read_csv(args.actuals, dtype=str).fillna("")

    for req in ("prop_norm", "line"):
        if req not in slate.columns:
            raise RuntimeError(f"Slate missing required column: {req}")

    # espn_athlete_id is preferred but not required — fall back to player_norm name matching
    if "espn_athlete_id" not in slate.columns:
        print("  ⚠️  No 'espn_athlete_id' in slate — using name-only matching.")
        slate["espn_athlete_id"] = ""
    if "player_norm" not in slate.columns:
        if "player" in slate.columns:
            import re, unicodedata
            def _norm(s):
                s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii","ignore").decode("ascii")
                s = s.lower().strip()
                s = re.sub(r"[^a-z0-9 ]+", " ", s)
                return re.sub(r"\s+", " ", s).strip()
            slate["player_norm"] = slate["player"].apply(_norm)
        else:
            raise RuntimeError("Slate missing both 'player' and 'player_norm' columns.")

    # Resolve direction column (step5b uses model_dir_5, step6 uses final_bet_direction)
    dir_col = next((c for c in ("final_bet_direction", "model_dir_5", "bet_direction",
                                "model_dir", "model_direction")
                    if c in slate.columns), None)
    if dir_col is None:
        slate["_dir"] = ""
    else:
        slate["_dir"] = slate[dir_col].astype(str).str.upper().str.strip()

    # Resolve pick type column early (needed for Goblin/Demon forced-OVER logic)
    pick_type_col = next((c for c in ("pick_type", "Pick Type") if c in slate.columns), None)

    # Goblin and Demon are always OVER — force direction and void any UNDER rows
    if pick_type_col:
        forced_mask = slate[pick_type_col].astype(str).str.strip().str.lower().isin(["goblin", "demon"])
        slate.loc[forced_mask, "_dir"] = "OVER"
        # Mark rows that were originally set as UNDER as void so they dont pollute results
        if dir_col:
            wrong_dir = forced_mask & slate[dir_col].astype(str).str.upper().str.strip().eq("UNDER")
            slate.loc[wrong_dir, "_dir"] = "VOID_FORCED_UNDER"

    # Resolve defensive tier columns (from step3b)
    def_tier_col = next((c for c in ("opp_def_tier", "def_tier", "Def Tier") if c in slate.columns), None)
    def_rank_col = next((c for c in ("opp_def_rank", "OVERALL_DEF_RANK", "opp_def_adj_de")
                         if c in slate.columns), None)

    # Pick type / tier cols
    tier_col      = next((c for c in ("tier", "Tier") if c in slate.columns), None)

    # ── Build actuals index (max minutes if duped) ──────────────────────────
    actuals["_min"] = actuals["MIN"].apply(to_float)
    actuals = (actuals.sort_values("_min", ascending=False)
                      .drop_duplicates(subset=["espn_athlete_id"], keep="first")
                      .drop(columns=["_min"]))
    actuals_idx = actuals.set_index("espn_athlete_id", drop=False)

    # Secondary: name-based index for rows without espn_athlete_id
    import re as _re, unicodedata as _uc
    def _norm(s):
        s = _uc.normalize("NFKD", str(s or "")).encode("ascii","ignore").decode("ascii")
        s = s.lower().strip()
        s = _re.sub(r"[^a-z0-9 ]+", " ", s)
        return _re.sub(r"\s+", " ", s).strip()
    actuals_name_idx = actuals.copy()
    actuals_name_idx["_name_norm"] = actuals_name_idx["player_name"].apply(_norm)
    actuals_name_idx = actuals_name_idx.drop_duplicates(subset=["_name_norm"], keep="first")
    actuals_name_idx = actuals_name_idx.set_index("_name_norm", drop=False)

    # ── Grade each row ──────────────────────────────────────────────────────
    actual_values, actual_statuses = [], []

    for _, r in slate.iterrows():
        aid = str(r.get("espn_athlete_id", "")).strip()
        pnorm = str(r.get("player_norm", "")).strip()
        arow = None
        if aid and aid in actuals_idx.index:
            arow = actuals_idx.loc[aid]
            method = "ID"
        elif pnorm and pnorm in actuals_name_idx.index:
            arow = actuals_name_idx.loc[pnorm]
            method = "NAME"
        else:
            method = "MISSING"

        if arow is None:
            actual_values.append(np.nan); actual_statuses.append("NO_ACTUAL_FOUND"); continue
        av = stat_from_row(arow, str(r.get("prop_norm", "")))
        actual_values.append(av)
        actual_statuses.append("OK" if not np.isnan(av) else "UNSUPPORTED_PROP")

    out = slate.copy()
    out["line_num"]     = out["line"].apply(to_float)
    out["actual_value"] = actual_values
    out["dir_played"]   = out["_dir"]
    out["result"]       = [grade_row(av, ln, d)
                           for av, ln, d in zip(out["actual_value"], out["line_num"], out["dir_played"])]
    out["actual_status"] = actual_statuses
    out["diff"]          = out["actual_value"] - out["line_num"]
    out.drop(columns=["_dir"], inplace=True, errors="ignore")

    # ── Console summary ─────────────────────────────────────────────────────
    print(f"\n{'='*55}")
    print(f" CBB GRADED SUMMARY" + (f"  |  {date_label}" if date_label else ""))
    print(f"{'='*55}")
    print(f" Total rows   : {len(out)}")
    print(f" Actual OK    : {(out['actual_status']=='OK').sum()}")
    print(f" HIT          : {(out['result']=='HIT').sum()}")
    print(f" MISS         : {(out['result']=='MISS').sum()}")
    print(f" PUSH         : {(out['result']=='PUSH').sum()}")
    print(f" VOID         : {(out['result']=='VOID').sum()}")

    decided = out[out["result"].isin(["HIT", "MISS"])]
    if len(decided) > 0:
        overall_hr = (decided["result"] == "HIT").sum() / len(decided)
        print(f" Hit Rate     : {overall_hr:.1%} ({(decided['result']=='HIT').sum()}/{len(decided)} decided)")

    # Defensive tier breakdown
    if def_tier_col:
        print(f"\n BY DEFENSIVE TIER  (col: {def_tier_col})")
        print(f" {'Tier':<12} {'Hits':>5} {'Misses':>7} {'Decided':>8} {'Hit Rate':>10}")
        print(f" {'-'*46}")
        for tier in ["Elite", "Above Avg", "Avg", "Weak", ""]:
            sub_d = decided[decided[def_tier_col].astype(str) == str(tier)]
            if len(sub_d) == 0:
                continue
            h = (sub_d["result"] == "HIT").sum()
            m = (sub_d["result"] == "MISS").sum()
            label = tier if tier else "UNMAPPED"
            print(f" {label:<12} {h:>5} {m:>7} {h+m:>8} {h/(h+m):>9.1%}")
    else:
        print("\n ⚠️  No defensive tier column found in slate.")
        print("    Run cbb_step3b_attach_def_rankings.py first to enable this breakdown.")

    print(f"{'='*55}\n")

    # ── Helper: build direction-aware rows ──────────────────────────────────
    def dir_rows(label_name, label_val, subset, total_subset):
        """Build ALL / OVER / UNDER rows for a given subset."""
        rows = []
        for direction in ["ALL", "OVER", "UNDER"]:
            if direction == "ALL":
                sub = subset
                total = total_subset
            else:
                sub   = subset[subset["dir_played"] == direction]
                total = total_subset[total_subset["dir_played"] == direction] if "dir_played" in total_subset.columns else total_subset
            d = sub[sub["result"].isin(["HIT","MISS"])]
            h = (d["result"] == "HIT").sum()
            m = (d["result"] == "MISS").sum()
            t = h + m
            v = (sub["result"].isin(["VOID","PUSH"])).sum()
            rows.append({
                label_name:  label_val if direction == "ALL" else np.nan,
                "Direction": direction,
                "Total":     len(total) if direction == "ALL" else len(total_subset[total_subset["dir_played"] == direction]) if "dir_played" in total_subset.columns else np.nan,
                "Decided":   t,
                "Hits":      h,
                "Misses":    m,
                "Voids":     v,
                "Hit Rate":  round(h/t, 4) if t > 0 else np.nan,
            })
        return rows

    def build_direction_sheet(group_col, group_vals, sheet_df):
        """Build a sheet with ALL/OVER/UNDER rows per group value."""
        rows = []
        for val in group_vals:
            sub   = sheet_df[sheet_df[group_col].astype(str) == str(val)]
            rows += dir_rows(group_col, val, sub, sub)
            rows.append({group_col: np.nan, "Direction": np.nan, "Total": np.nan,
                         "Decided": np.nan, "Hits": np.nan, "Misses": np.nan,
                         "Voids": np.nan, "Hit Rate": np.nan})
        return pd.DataFrame(rows)

    def build_pick_type_sheet(df):
        rows = []
        for pt in ["Goblin", "Demon", "Standard"]:
            if pick_type_col not in df.columns:
                continue
            sub = df[df[pick_type_col] == pt]
            rows += dir_rows("Pick Type", pt, sub, sub)
            rows.append({"Pick Type": np.nan, "Direction": np.nan, "Total": np.nan,
                         "Decided": np.nan, "Hits": np.nan, "Misses": np.nan,
                         "Voids": np.nan, "Hit Rate": np.nan})
        return pd.DataFrame(rows)

    def build_prop_type_sheet(df):
        rows = []
        prop_vals = sorted(df["prop_norm"].dropna().unique()) if "prop_norm" in df.columns else []
        for pt in prop_vals:
            sub = df[df["prop_norm"] == pt]
            rows += dir_rows("Prop Type", pt, sub, sub)
            rows.append({"Prop Type": np.nan, "Direction": np.nan, "Total": np.nan,
                         "Decided": np.nan, "Hits": np.nan, "Misses": np.nan,
                         "Voids": np.nan, "Hit Rate": np.nan})
        return pd.DataFrame(rows)

    def build_def_rank_sheet(df):
        """Bucket by def rank ranges 001-050, 051-100, etc."""
        rows = []
        rank_col = next((c for c in ["OVERALL_DEF_RANK","opp_def_rank"] if c in df.columns), None)
        if not rank_col:
            return pd.DataFrame()
        buckets = [(1,50,"001-050"),(51,100,"051-100"),(101,150,"101-150"),
                   (151,200,"151-200"),(201,250,"201-250"),(251,362,"251-362")]
        for lo, hi, label in buckets:
            sub = df[pd.to_numeric(df[rank_col], errors="coerce").between(lo, hi)]
            if len(sub) == 0:
                continue
            rows += dir_rows("Def Rank (D1)", label, sub, sub)
            rows.append({"Def Rank (D1)": np.nan, "Direction": np.nan, "Total": np.nan,
                         "Decided": np.nan, "Hits": np.nan, "Misses": np.nan,
                         "Voids": np.nan, "Hit Rate": np.nan})
        return pd.DataFrame(rows)

    # ── Build Summary sheet ──────────────────────────────────────────────────
    import datetime
    generated = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    title = f"SlateIQ CBB GRADE  |  {date_label}  |  Generated {generated}" if date_label else "SlateIQ CBB GRADE"

    summary_rows = []
    # Overall block
    summary_rows.append({"": title})
    summary_rows.append({"": "OVERALL", "Direction": "Direction", "Total": "Total",
                         "Decided": "Decided", "Hits": "Hits", "Misses": "Misses",
                         "Voids": "Voids", "Hit Rate": "Hit Rate"})
    summary_rows += dir_rows("", "Full Slate", out, out)
    summary_rows.append({})

    # By Pick Type block
    summary_rows.append({"": "BY PICK TYPE", "Direction": "Direction", "Total": "Total",
                         "Decided": "Decided", "Hits": "Hits", "Misses": "Misses",
                         "Voids": "Voids", "Hit Rate": "Hit Rate"})
    if pick_type_col:
        for pt in ["Goblin", "Demon", "Standard"]:
            sub = out[out[pick_type_col] == pt]
            summary_rows += dir_rows("", pt, sub, sub)
    summary_rows.append({})

    # By Def Tier block
    if def_tier_col:
        summary_rows.append({"": "BY DEF TIER", "Direction": "Direction", "Total": "Total",
                             "Decided": "Decided", "Hits": "Hits", "Misses": "Misses",
                             "Voids": "Voids", "Hit Rate": "Hit Rate"})
        for tier in ["Elite", "Above Avg", "Avg", "Weak"]:
            sub = out[out[def_tier_col].astype(str) == tier]
            summary_rows += dir_rows("", tier, sub, sub)

    summary_df = pd.DataFrame(summary_rows)

    # ── Decided Only columns ─────────────────────────────────────────────────
    decided_cols = ["player","prop_norm","pick_type","line","dir_played","actual_value","result","diff"]
    decided_out  = decided[[c for c in decided_cols if c in decided.columns]].copy()
    decided_out.rename(columns={"dir_played": "bet_direction"}, inplace=True)

    # ── Write Excel ──────────────────────────────────────────────────────────
    with pd.ExcelWriter(args.out, engine="openpyxl") as xw:
        summary_df.to_excel(xw, sheet_name="Summary", index=False, header=False)
        build_pick_type_sheet(out).to_excel(xw, sheet_name="By Pick Type", index=False)
        build_prop_type_sheet(out).to_excel(xw, sheet_name="By Prop Type", index=False)
        if def_tier_col:
            build_direction_sheet(def_tier_col, ["Elite","Above Avg","Avg","Weak"], out).to_excel(
                xw, sheet_name="By Def Tier", index=False)
        build_def_rank_sheet(out).to_excel(xw, sheet_name="By Def Rank", index=False)
        decided_out.to_excel(xw, sheet_name="Decided Only", index=False)
        out.to_excel(xw, sheet_name="Box Raw", index=False)

    print(f"✅ Saved: {args.out}")
    print(f"   Sheets: Summary | By Pick Type | By Prop Type | By Def Tier | By Def Rank | Decided Only | Box Raw")


if __name__ == "__main__":
    main()
