"""
nhl_soccer_grader.py
====================
Grades NHL and Soccer step8 slates against actuals CSVs.
Outputs graded_nhl_DATE.xlsx and/or graded_soccer_DATE.xlsx
in the same format as build_grade_report.py expects.

Usage:
    py -3 nhl_soccer_grader.py --sport NHL --date 2026-03-06 --slate NHL/step8.xlsx
        --actuals "outputs\2026-03-06\actuals_nhl_2026-03-06.csv" \
        --output-dir "outputs\2026-03-06"

    py -3 nhl_soccer_grader.py --sport Soccer --date 2026-03-06 --slate Soccer/step8.xlsx
        --actuals "outputs\2026-03-06\actuals_soccer_2026-03-06.csv" \
        --output-dir "outputs\2026-03-06"
"""
from __future__ import annotations
import argparse, sys, re
from datetime import datetime
from pathlib import Path

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("ERROR: pip install pandas openpyxl"); sys.exit(1)

# ── Column maps: step8 slate → canonical graded output ────────────────────────
# These match what build_grade_report.py's normalize() function expects

NHL_SLATE_MAP = {
    "player":        "player",
    "team":          "team",
    "opp":           "opp_team",
    "tier":          "tier",
    "def_tier":      "def_tier",
    "direction":     "bet_direction",
    "line":          "line",
    "prop_display":  "prop_type_norm",
    "prop_type":     "prop_type_raw",
    "edge":          "edge",
    "prop_score":    "rank_score",
    "composite_hr":  "hit_rate_raw",
    "player_role":   "player_role",
    "position_group":"position_group",
    "scoring_tier":  "scoring_tier",
    "pp_tier":       "pp_tier",
    "toi_avg_L10":   "toi_avg",
}

SOCCER_SLATE_MAP = {
    # ── lowercase / original ──────────────────────────────────────────────────
    "player":              "player",
    "team":                "team",
    "opp_team":            "opp_team",
    "tier":                "tier",
    "DEF_TIER":            "def_tier",
    "def_tier":            "def_tier",
    "line":                "line",
    "prop_type":           "prop_type_norm",
    "prop_norm":           "prop_type_raw",
    "edge_adj":            "edge",
    "edge":                "edge",
    "rank_score":          "rank_score",
    "line_hit_rate":       "hit_rate_raw",
    "pick_type":           "pick_type",
    "league":              "league",
    "position_group":      "position_group",
    "minutes_tier":        "minutes_tier",
    "projection":          "projection",
    "direction":           "bet_direction",
    "final_bet_direction": "bet_direction",
    # ── Title-case / capitalized (what the soccer slate actually has) ─────────
    "Player":              "player",
    "Team":                "team",
    "Opp":                 "opp_team",
    "Opp Team":            "opp_team",
    "Opponent":            "opp_team",
    "Tier":                "tier",
    "Def Tier":            "def_tier",
    "Def_Tier":            "def_tier",
    "Line":                "line",
    "Prop":                "prop_type_norm",
    "Prop Type":           "prop_type_norm",
    "Prop_Type":           "prop_type_norm",
    "Edge":                "edge",
    "Edge Adj":            "edge",
    "Rank Score":          "rank_score",
    "Hit Rate":            "hit_rate_raw",
    "Hit Rate (5g)":       "hit_rate_raw",
    "Pick Type":           "pick_type",
    "League":              "league",
    "Position Group":      "position_group",
    "Position":            "position_group",
    "Minutes Tier":        "minutes_tier",
    "Min Tier":            "minutes_tier",
    "Projection":          "projection",
    "Direction":           "bet_direction",
    "Final Bet Direction": "bet_direction",
}

# Actuals CSVs: what columns to look for player name and stat value
ACTUALS_PLAYER_COLS  = ["player","player_name","name","Player","athlete_name"]
ACTUALS_VALUE_COLS   = ["actual","value","stat","result_value","actual_value",
                        "stat_value","fantasy_points","actual_stat"]
ACTUALS_PROP_COLS    = ["prop","prop_type","stat_type","prop_norm","market"]
ACTUALS_TEAM_COLS    = ["team","team_abbr","Team"]

# ── Helpers ────────────────────────────────────────────────────────────────────
def _find_col(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    return None

def _norm_name(s):
    """Lowercase, strip accents roughly, collapse whitespace."""
    s = str(s).lower().strip()
    s = re.sub(r"[àáâãäå]","a", s)
    s = re.sub(r"[èéêë]","e", s)
    s = re.sub(r"[ìíîï]","i", s)
    s = re.sub(r"[òóôõö]","o", s)
    s = re.sub(r"[ùúûü]","u", s)
    s = re.sub(r"[ýÿ]","y", s)
    s = re.sub(r"[ñ]","n", s)
    s = re.sub(r"[ç]","c", s)
    s = s = ' '.join(s.split())
    return s

def _norm_prop(s):
    return re.sub(r"[^a-z0-9]","", str(s).lower())

def load_slate(path: Path, sport: str) -> pd.DataFrame:
    sport = sport.upper()
    # Try reading — handle xlsx and csv
    # Sniff: some .xlsx files are actually CSVs
    def _is_csv(p):
        try:
            with open(p, "rb") as f:
                header = f.read(8)
            # Real xlsx starts with PK (zip), CSV starts with text
            return header[:2] != b"PK"
        except: return False

    is_csv = path.suffix.lower() == ".csv" or _is_csv(path)

    if not is_csv:
        try:
            xf = pd.ExcelFile(path)
            sheet = next((s for s in xf.sheet_names if "all" in s.lower()), xf.sheet_names[0])
            print(f"  Reading sheet '{sheet}' from {path.name}")
            df = pd.read_excel(path, sheet_name=sheet)
        except Exception:
            is_csv = True  # fallback to CSV

    if is_csv:
        print(f"  Reading as CSV: {path.name}")
        for enc in ["utf-8","latin-1","cp1252"]:
            try:
                df = pd.read_csv(path, encoding=enc, low_memory=False)
                break
            except Exception:
                continue
        else:
            print(f"ERROR: could not read {path}"); sys.exit(1)

    df.columns = [c.strip() for c in df.columns]

    col_map = NHL_SLATE_MAP if sport == "NHL" else SOCCER_SLATE_MAP

    if sport == "SOCCER":
        original_cols = list(df.columns)
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        mapped   = [f"{k}->{v}" for k, v in col_map.items() if k in original_cols]
        unmapped = [c for c in original_cols if c not in col_map]
        print(f"  [ColMap] Renamed: {mapped[:12]}")
        if unmapped:
            print(f"  [ColMap] Unmapped (kept): {unmapped[:15]}")
        for required in ("player", "prop_type_norm", "line", "bet_direction"):
            if required not in df.columns:
                print(f"  [ColMap] WARNING: '{required}' missing after rename!")
                print(f"  [ColMap] All cols after rename: {list(df.columns)}")
    else:
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Ensure pick_type exists (NHL step8 doesn't have it — derive from edge/tier)
    if "pick_type" not in df.columns:
        if "tier" in df.columns:
            def _pick_type(row):
                tier = str(row.get("tier","")).upper()
                edge = float(row.get("edge", 0.5)) if pd.notna(row.get("edge")) else 0.5
                if tier == "A" and edge >= 0.48: return "Goblin"
                if tier in ("A","B"):            return "Standard"
                return "Demon"
            df["pick_type"] = df.apply(_pick_type, axis=1)
        else:
            df["pick_type"] = "Standard"

    # Normalize bet_direction to OVER/UNDER
    if sport == "SOCCER":
        # Soccer CSV has both bet_direction and final_bet_direction — prefer final
        src_col = "final_bet_direction" if "final_bet_direction" in df.columns else "bet_direction"
        if src_col in df.columns:
            df["bet_direction"] = [str(x).upper().strip() for x in df[src_col]]
    elif "bet_direction" in df.columns:
        df["bet_direction"] = [str(x).upper().strip() for x in df["bet_direction"]]

    # Normalize hit_rate to float 0-1
    if "hit_rate_raw" in df.columns:
        def _pct_to_f(v):
            try:
                s = str(v).replace("%","").strip()
                f = float(s)
                return f/100 if f > 1 else f
            except: return float("nan")
        df["hit_rate"] = df["hit_rate_raw"].apply(_pct_to_f)

    df["Sport"] = sport

    # ── Soccer: filter to only rows whose game time has passed ────────────────
    # This prevents future-slate rows from polluting a grading run and causing
    # 100% VOID when the actuals are for a different day's games.
    if sport == "SOCCER":
        game_time_col = next((c for c in df.columns if c.lower() in
                              ("game time", "game_time", "gametime", "kickoff")), None)
        if game_time_col:
            now_utc = pd.Timestamp.utcnow()
            try:
                gts = pd.to_datetime(df[game_time_col], utc=True, errors="coerce")
                before_now = gts <= now_utc
                n_total = len(df)
                df = df[before_now].copy()
                n_kept = len(df)
                print(f"  [DateFilter] Kept {n_kept}/{n_total} rows with game_time <= now "
                      f"(dropped {n_total - n_kept} future rows)")
                if n_kept == 0:
                    print(f"  [DateFilter] WARNING: 0 rows remain after date filter — "
                          f"all games on this slate are in the future")
            except Exception as exc:
                print(f"  [DateFilter] Could not parse '{game_time_col}': {exc} — skipping filter")

    return df

def load_actuals(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"  WARNING: actuals not found: {path}")
        return pd.DataFrame()

    for enc in ["utf-8","latin-1","cp1252"]:
        try:
            df = pd.read_csv(path, encoding=enc, low_memory=False)
            break
        except Exception:
            continue
    else:
        print(f"  WARNING: could not read actuals {path}")
        return pd.DataFrame()

    df.columns = [c.strip() for c in df.columns]
    print(f"  Actuals: {len(df)} rows, cols: {list(df.columns)}")
    return df

def grade(slate: pd.DataFrame, actuals: pd.DataFrame, sport: str) -> pd.DataFrame:
    """Match slate props to actuals and assign HIT/MISS/VOID."""
    slate = slate.copy()
    slate["actual"] = float("nan")
    slate["result"] = "VOID"
    slate["void_reason_grade"] = "NO_ACTUAL"
    slate["margin"] = float("nan")

    if actuals.empty:
        print("  WARNING: no actuals — all props marked VOID")
        return slate

    # Find key columns in actuals
    p_col   = _find_col(actuals, ACTUALS_PLAYER_COLS)
    v_col   = _find_col(actuals, ACTUALS_VALUE_COLS)
    pr_col  = _find_col(actuals, ACTUALS_PROP_COLS)
    t_col   = _find_col(actuals, ACTUALS_TEAM_COLS)

    if not p_col or not v_col:
        print(f"  WARNING: actuals missing player ({p_col}) or value ({v_col}) column")
        print(f"  Actuals columns: {list(actuals.columns)}")
        return slate

    # Build actuals lookup: norm_name → {norm_prop: value}
    actuals = actuals.copy()
    actuals["_name"] = actuals[p_col].apply(_norm_name)
    actuals["_val"]  = pd.to_numeric(actuals[v_col], errors="coerce")
    if pr_col:
        actuals["_prop"] = actuals[pr_col].apply(_norm_prop)

    # Index by name for fast lookup
    act_by_name: dict[str, list] = {}
    for _, row in actuals.iterrows():
        name = row["_name"]
        if name not in act_by_name:
            act_by_name[name] = []
        act_by_name[name].append(row)

    hits = misses = voids = 0

    # ── SOCCER DIAGNOSTIC (first run only) ──────────────────────────────────
    if sport == "SOCCER" and len(slate) > 0:
        # Sample first 3 slate players — are they in act_by_name?
        sample_slate = list(slate["player"].apply(_norm_name).unique())[:5]
        sample_acts  = list(act_by_name.keys())[:5]
        name_overlap = sum(1 for n in list(slate["player"].apply(_norm_name)) if n in act_by_name)
        # Sample props
        prop_col_s = "prop_type_norm" if "prop_type_norm" in slate.columns else "prop_type_raw" if "prop_type_raw" in slate.columns else None
        sample_props = list(slate[prop_col_s].apply(_norm_prop).unique())[:8] if prop_col_s else []
        print(f"  [DIAG] Slate name sample (normed): {sample_slate}")
        print(f"  [DIAG] Actuals name sample (normed): {sample_acts}")
        print(f"  [DIAG] Name matches (slate players found in actuals): {name_overlap}/{len(slate)}")
        print(f"  [DIAG] Slate prop norms: {sample_props}")
        print(f"  [DIAG] Actuals prop norms: {sorted(set(actuals['_prop'].tolist()))[:10] if pr_col else 'no prop col'}")

    for idx, srow in slate.iterrows():
        sname = _norm_name(srow.get("player",""))
        sprop = _norm_prop(srow.get("prop_type_norm", srow.get("prop_type_raw","")))
        sline = srow.get("line")
        sdir  = str(srow.get("bet_direction","")).upper()

        if sname not in act_by_name:
            voids += 1
            continue

        candidates = act_by_name[sname]

        # Try to match by prop type if actuals have it
        # SOCCER PROP ALIAS TABLE
        # Key   = normalised SLATE prop (_norm_prop on slate Prop column)
        # Value = list of normalised ACTUALS prop names that should match
        # Confirmed mismatches:
        #   Slate "Goalie Saves"    -> "goaliesaves"   -> actuals "Goalkeeper Saves" ("goalkeepersaves")
        #   Slate "Shots On Target" -> "shotsontarget" -> actuals "Shots" ("shots")
        SOCCER_PROP_ALIASES = {
            "goaliesaves":      ["goalkeepersaves", "saves", "gksaves"],
            "goalkeepersaves":  ["goalkeepersaves", "saves", "gksaves"],
            "saves":            ["goalkeepersaves", "saves", "gksaves"],
            "gksaves":          ["goalkeepersaves", "saves", "gksaves"],
            "shotsontarget":    ["shots", "shotsontarget", "sot"],
            "sot":              ["shots", "shotsontarget", "sot"],
            "fouls":            ["fouls", "foulscommitted", "foulsc"],
            "foulscommitted":   ["fouls", "foulscommitted", "foulsc"],
            "yellowcards":      ["yellowcards", "yellow", "yc"],
            "yellow":           ["yellowcards", "yellow", "yc"],
            "assists":          ["assists", "goalassists", "ast"],
            "goals":            ["goals", "goal"],
            "shots":            ["shots", "totalshots", "sh"],
        }
        matched = None
        if pr_col and candidates:
            # Exact normalised match first
            prop_matches = [r for r in candidates if _norm_prop(r.get("_prop","")) == sprop]
            # Alias match if no exact hit
            if not prop_matches:
                aliases = SOCCER_PROP_ALIASES.get(sprop, [sprop])
                prop_matches = [r for r in candidates
                                if _norm_prop(r.get("_prop","")) in aliases]
            if prop_matches:
                matched = prop_matches[0]

        # Fallback: if only one actual row for this player, use it
        if matched is None and len(candidates) == 1:
            matched = candidates[0]

        # Fallback: if multiple rows, take first (best we can do without prop match)
        if matched is None and candidates:
            matched = candidates[0]

        if matched is None or pd.isna(matched["_val"]):
            voids += 1
            continue

        actual_val = float(matched["_val"])
        try:
            line_val = float(sline)
        except (TypeError, ValueError):
            voids += 1
            slate.at[idx, "void_reason_grade"] = "NO_LINE"
            continue

        slate.at[idx, "actual"] = actual_val
        margin = actual_val - line_val
        slate.at[idx, "margin"] = margin

        # Grade
        if margin == 0:
            slate.at[idx, "result"] = "VOID"
            slate.at[idx, "void_reason_grade"] = "PUSH"
            voids += 1
        elif (sdir == "OVER" and margin > 0) or (sdir == "UNDER" and margin < 0):
            slate.at[idx, "result"] = "HIT"
            slate.at[idx, "void_reason_grade"] = ""
            hits += 1
        else:
            slate.at[idx, "result"] = "MISS"
            slate.at[idx, "void_reason_grade"] = ""
            misses += 1

    total = len(slate)
    dec   = hits + misses
    rate  = f"{hits/dec*100:.1f}%" if dec else "—"
    print(f"  Graded: {total:,} props → HIT:{hits} MISS:{misses} VOID:{voids} | Hit rate: {rate}")
    return slate

def save_graded(df: pd.DataFrame, out_path: Path, sport: str, date_str: str):
    """Save in the same multi-sheet format build_grade_report.py reads from Box Raw."""
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        # Sheet 1: Box Raw (this is what build_grade_report.py reads)
        df.to_excel(writer, sheet_name="Box Raw", index=False)

        # Sheet 2: Summary
        hi  = int((df["result"]=="HIT").sum())
        mi  = int((df["result"]=="MISS").sum())
        vo  = int((df["result"]=="VOID").sum())
        dec = hi + mi
        rate = hi/dec if dec else 0
        summary = pd.DataFrame([
            [f"{sport} SLATE GRADE  |  {date_str}  |  Generated {datetime.now():%Y-%m-%d %H:%M}",
             "Direction","Total Props","Decided","Hits","Misses","Voids","Hit Rate"],
            ["OVERALL","ALL", len(df), dec, hi, mi, vo, f"{rate*100:.1f}%"],
        ])
        summary.to_excel(writer, sheet_name="Summary", index=False, header=False)

        # Sheet 3: By Pick Type
        rows = []
        for pt, g in df.groupby("pick_type", dropna=True):
            phi = int((g["result"]=="HIT").sum())
            pmi = int((g["result"]=="MISS").sum())
            pvo = int((g["result"]=="VOID").sum())
            pdec = phi+pmi
            rows.append({"Pick Type":pt,"Hits":phi,"Misses":pmi,"Voids":pvo,
                         "Decided":pdec,"Hit Rate":f"{phi/pdec*100:.1f}%" if pdec else "—"})
        if rows:
            pd.DataFrame(rows).to_excel(writer, sheet_name="By Pick Type", index=False)

        # Sheet 4: By Tier
        rows = []
        if "tier" in df.columns:
            for t, g in df.groupby("tier", dropna=True):
                thi = int((g["result"]=="HIT").sum())
                tmi = int((g["result"]=="MISS").sum())
                tvo = int((g["result"]=="VOID").sum())
                tdec = thi+tmi
                rows.append({"Tier":f"Tier {t}","Hits":thi,"Misses":tmi,"Voids":tvo,
                             "Decided":tdec,"Hit Rate":f"{thi/tdec*100:.1f}%" if tdec else "—"})
        if rows:
            pd.DataFrame(rows).to_excel(writer, sheet_name="By Tier", index=False)

        # Sheet 5: By Def Tier
        rows = []
        if "def_tier" in df.columns:
            for dt, g in df.groupby("def_tier", dropna=True):
                dhi = int((g["result"]=="HIT").sum())
                dmi = int((g["result"]=="MISS").sum())
                dvo = int((g["result"]=="VOID").sum())
                ddec = dhi+dmi
                rows.append({"Def Tier":dt,"Hits":dhi,"Misses":dmi,"Voids":dvo,
                             "Decided":ddec,"Hit Rate":f"{dhi/ddec*100:.1f}%" if ddec else "—"})
        if rows:
            pd.DataFrame(rows).to_excel(writer, sheet_name="By Def Tier", index=False)

        # Sheet 6: Void Reasons
        vr = df[df["void_reason_grade"].astype(str).str.len()>0].groupby("void_reason_grade").size()
        if not vr.empty:
            pd.DataFrame({"Void Reason":vr.index,"Count":vr.values}).to_excel(
                writer, sheet_name="Void Reasons", index=False)

    print(f"  Saved → {out_path}  ({out_path.stat().st_size:,} bytes)")

# ── CLI ────────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Grade NHL/Soccer slates against actuals")
    ap.add_argument("--sport",      required=True, choices=["NHL","Soccer","SOCCER","nhl","soccer"])
    ap.add_argument("--date",       required=True)
    ap.add_argument("--slate",      required=True)
    ap.add_argument("--actuals",    required=True)
    ap.add_argument("--output-dir", required=True)
    args = ap.parse_args()

    sport    = args.sport.upper()
    date_str = args.date
    slate_p  = Path(args.slate)
    act_p    = Path(args.actuals)
    out_dir  = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    sport_lower = "soccer" if sport=="SOCCER" else "nhl"
    out_path = out_dir / f"graded_{sport_lower}_{date_str}.xlsx"

    print(f"\n  [{sport} GRADER]  {date_str}")
    print(f"  Slate:   {slate_p}")
    print(f"  Actuals: {act_p}")
    print(f"  Output:  {out_path}")

    slate   = load_slate(slate_p, sport)
    actuals = load_actuals(act_p)

    print(f"  Slate rows: {len(slate):,}")

    graded = grade(slate, actuals, sport)
    save_graded(graded, out_path, sport, date_str)
    print(f"  Done.\n")

if __name__ == "__main__":
    main()
