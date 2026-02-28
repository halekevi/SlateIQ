#!/usr/bin/env python3
"""
combined_ticket_grader_UPDATED.py
================================
Full analytics grader for the output of combined_slate_tickets.py, with **dynamic payout modifiers**
for Goblin/Demon legs based on "distance from line" buckets (dev levels).

Key upgrade vs v1:
- Supports leg types like:
    Standard
    Goblin -1 / -2 / -3   (more discounted => lower payout)
    Demon  +1 / +2 / +3   (more juiced     => higher payout)
- If your ticket workbook only has pick_type = Goblin/Demon (no dev), we default to:
    Goblin -> Goblin -1
    Demon  -> Demon +1
- Payout multipliers are computed like your payout_calculator.jsx:
    Power: POWER_BASE[n] * Π(leg_modifier_power)
    Flex : FLEX_BASE[n][hits] * Π(leg_modifier_flex)

Outputs:
- SUMMARY (key metrics)
- TICKET_RESULTS (one row per ticket per mode)
- LEG_RESULTS (one row per leg, with HIT/MISS/PUSH/NO_ACTUAL)
- Analytics tabs per mode (ROI by sheet/legs/sports/pick_types)

Usage (PowerShell):
  py -3.14 .\combined_ticket_grader_UPDATED.py `
    --tickets .\combined_slate_tickets_2026-02-21.xlsx `
    --nba_actuals ".\grades\actuals_nba_2026-02-21.csv" `
    --cbb_actuals ".\grades\actuals_cbb_2026-02-21.csv" `
    --mode both --stake 20

Optional config override:
  py -3.14 .\combined_ticket_grader_UPDATED.py ... --payouts_json .\grades\payouts_2026-02-21.json

JSON schema (example):
{
  "power_base": { "2":3.0, "3":6.0, "4":10.0, "5":20.0, "6":37.5 },
  "flex_base": {
    "2": { "2":3.0 },
    "3": { "3":3.0, "2":1.0 },
    "4": { "4":6.0, "3":1.5 },
    "5": { "5":10.0, "4":2.0, "3":0.4 },
    "6": { "6":25.0, "5":2.0, "4":0.4 }
  },
  "mods": {
    "goblin_power": { "1":0.84, "2":0.747, "3":0.707 },
    "goblin_flex":  { "1":0.80, "2":0.720, "3":0.600 },
    "demon_power":  { "1":1.627,"2":2.40,  "3":2.72  },
    "demon_flex":   { "1":1.60, "2":1.520, "3":1.560 }
  }
}
"""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


# -----------------------------
# Defaults
# -----------------------------
POWER_BASE = {2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 37.5}
FLEX_BASE = {
    2: {2: 3.0},
    3: {3: 3.0, 2: 1.0},
    4: {4: 6.0, 3: 1.5},
    5: {5: 10.0, 4: 2.0, 3: 0.4},
    6: {6: 25.0, 5: 2.0, 4: 0.4},
}

# Modifiers by deviation bucket (dev=1 closest to standard, dev=3 furthest)
GOBLIN_POWER = {1: 0.840, 2: 0.747, 3: 0.707}
GOBLIN_FLEX  = {1: 0.800, 2: 0.720, 3: 0.600}
DEMON_POWER  = {1: 1.627, 2: 2.400, 3: 2.720}
DEMON_FLEX   = {1: 1.600, 2: 1.520, 3: 1.560}


# -----------------------------
# Normalization helpers
# -----------------------------
def strip_norm(s: str) -> str:
    s = "" if s is None else str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def prop_norm_from_label(prop: str) -> str:
    p = strip_norm(prop)
    if "pts+reb+ast" in p or p == "pra":
        return "pra"
    if "pts+reb" in p or p == "pr":
        return "pr"
    if "pts+ast" in p or p == "pa":
        return "pa"
    if "reb+ast" in p or p == "ra":
        return "ra"
    if "points" in p or p == "pts":
        return "points"
    if "rebounds" in p or p == "reb":
        return "rebounds"
    if "assists" in p or p == "ast":
        return "assists"
    if "turnover" in p or p == "tov":
        return "turnovers"
    if "blocked" in p or p == "blk":
        return "blocks"
    if "steal" in p or p == "stl":
        return "steals"
    if "fantasy" in p:
        return "fantasy"
    if any(x in p for x in ["3pm", "3-pointers made", "3 pointers made", "3pt made", "threes made"]):
        return "3pm"
    if any(x in p for x in ["3pa", "3-point attempts", "3 pointers attempted", "3pt att", "threes attempted"]):
        return "3pa"
    if "field goal attempts" in p or p == "fga":
        return "fga"
    if "field goals made" in p or p == "fgm":
        return "fgm"
    if "free throw attempts" in p or p == "fta":
        return "fta"
    if "free throws made" in p or p == "ftm":
        return "ftm"
    return p


def prop_norm_from_actual(prop_type: str) -> str:
    return prop_norm_from_label(prop_type)


# -----------------------------
# Ticket parsing
# -----------------------------
TICKET_RE = re.compile(r"ticket\s*#\s*(\d+)", re.IGNORECASE)

def derive_leg_type(pick_type_cell: str) -> str:
    """
    Convert workbook pick_type into a dev-bucketed leg type.
    Accepts:
      - "Standard"
      - "Goblin" -> "Goblin -1" (default)
      - "Demon"  -> "Demon +1"  (default)
      - "Goblin -2" / "Demon +3" (pass through)
    """
    s = (pick_type_cell or "").strip()
    if not s:
        return "Standard"
    s_norm = strip_norm(s)
    if "goblin" in s_norm:
        # if dev already included like "-2"
        m = re.search(r"-(\d+)", s_norm)
        dev = int(m.group(1)) if m else 1
        dev = max(1, min(3, dev))
        return f"Goblin -{dev}"
    if "demon" in s_norm:
        m = re.search(r"\+(\d+)", s_norm)
        dev = int(m.group(1)) if m else 1
        dev = max(1, min(3, dev))
        return f"Demon +{dev}"
    return "Standard"


def parse_ticket_sheet(tickets_xlsx: Path, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(tickets_xlsx, sheet_name=sheet_name, dtype=object)
    if df.empty:
        return pd.DataFrame()

    first_col = df.columns[0]
    df = df.copy()
    df.rename(columns={first_col: "ticket_header"}, inplace=True)

    rows = []
    i = 0
    while i < len(df):
        hdr = df.at[i, "ticket_header"]
        ticket_no = None
        if isinstance(hdr, str):
            m = TICKET_RE.search(hdr)
            if m:
                ticket_no = int(m.group(1))
        if ticket_no is None:
            i += 1
            continue

        # Find header row (2nd column == "Player")
        j = i + 1
        while j < len(df):
            c1 = df.iloc[j, 1] if df.shape[1] > 1 else None
            if isinstance(c1, str) and strip_norm(c1) == "player":
                break
            nxt = df.at[j, "ticket_header"]
            if isinstance(nxt, str) and TICKET_RE.search(nxt):
                break
            j += 1

        if j >= len(df) or not (isinstance(df.iloc[j, 1], str) and strip_norm(df.iloc[j, 1]) == "player"):
            i += 1
            continue

        k = j + 1
        leg_no = 0
        while k < len(df):
            player = df.iloc[k, 1] if df.shape[1] > 1 else None
            if pd.isna(player) or strip_norm(player) == "":
                break
            if isinstance(player, str) and strip_norm(player) == "player":
                k += 1
                continue

            team = df.iloc[k, 2] if df.shape[1] > 2 else ""
            prop = df.iloc[k, 4] if df.shape[1] > 4 else ""
            line = df.iloc[k, 6] if df.shape[1] > 6 else np.nan
            direction = df.iloc[k, 7] if df.shape[1] > 7 else ""
            pick_type = df.iloc[k, 8] if df.shape[1] > 8 else ""
            tier = df.iloc[k, 9] if df.shape[1] > 9 else ""
            sport = df.iloc[k, 16] if df.shape[1] > 16 else ""

            line_num = pd.to_numeric(line, errors="coerce")
            if pd.isna(line_num):
                k += 1
                continue

            leg_no += 1
            leg_type = derive_leg_type("" if pd.isna(pick_type) else str(pick_type))

            rows.append({
                "sheet": sheet_name,
                "ticket_no": ticket_no,
                "leg_no": leg_no,
                "player": str(player),
                "team": str(team) if not pd.isna(team) else "",
                "prop": str(prop) if not pd.isna(prop) else "",
                "prop_norm": prop_norm_from_label(prop),
                "line": float(line_num),
                "dir": str(direction).strip().upper() if not pd.isna(direction) else "",
                "sport": str(sport).strip().upper() if not pd.isna(sport) else "",
                "pick_type": str(pick_type) if not pd.isna(pick_type) else "",
                "leg_type": leg_type,
                "tier": str(tier) if not pd.isna(tier) else "",
            })
            k += 1

        i = k + 1

    return pd.DataFrame(rows)


# -----------------------------
# Actuals loading + lookup
# -----------------------------
def prep_actuals(csv_path: Path, sport_label: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    required = {"player", "team", "prop_type", "actual"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"{sport_label} actuals missing columns: {sorted(missing)}. Found: {list(df.columns)}")

    df["actual"] = pd.to_numeric(df["actual"], errors="coerce")
    df = df.dropna(subset=["actual"]).copy()

    df["player_norm"] = df["player"].map(strip_norm)
    df["team_norm"] = df["team"].map(strip_norm)
    df["prop_norm"] = df["prop_type"].map(prop_norm_from_actual)
    return df


def build_lookup(act: pd.DataFrame):
    by_player_prop: Dict[Tuple[str, str], List[dict]] = {}
    by_player_team_prop: Dict[Tuple[str, str, str], List[dict]] = {}
    for _, r in act.iterrows():
        key1 = (r["player_norm"], r["prop_norm"])
        key2 = (r["player_norm"], r["team_norm"], r["prop_norm"])
        by_player_prop.setdefault(key1, []).append(r.to_dict())
        by_player_team_prop.setdefault(key2, []).append(r.to_dict())
    return by_player_prop, by_player_team_prop


def lookup_actual(sport: str, player: str, team: str, prop_norm: str,
                  nba_lpt, nba_lp, cbb_lpt, cbb_lp) -> float:
    sport = (sport or "").upper()
    player_n = strip_norm(player)
    team_n = strip_norm(team)
    if sport == "NBA":
        key2 = (player_n, team_n, prop_norm)
        if key2 in nba_lpt:
            return float(nba_lpt[key2][0]["actual"])
        key1 = (player_n, prop_norm)
        if key1 in nba_lp:
            return float(nba_lp[key1][0]["actual"])
        return np.nan
    else:
        # FIX 5: For CBB, team in ticket is an abbreviation (e.g. "COLO") but actuals
        # may use a different format. Try player+prop first (most reliable), then
        # team-keyed as secondary. This is the reverse of NBA priority for CBB.
        key1 = (player_n, prop_norm)
        if key1 in cbb_lp:
            return float(cbb_lp[key1][0]["actual"])
        key2 = (player_n, team_n, prop_norm)
        if key2 in cbb_lpt:
            return float(cbb_lpt[key2][0]["actual"])
        return np.nan


# -----------------------------
# Grading + payout modifiers
# -----------------------------
def grade_leg(dir_: str, line: float, actual: float) -> str:
    if pd.isna(actual):
        return "NO_ACTUAL"
    if abs(actual - line) < 1e-9:
        return "PUSH"
    d = (dir_ or "").upper()
    if d == "OVER":
        return "HIT" if actual > line else "MISS"
    if d == "UNDER":
        return "HIT" if actual < line else "MISS"
    return "UNKNOWN_DIR"


def leg_modifiers(leg_types: List[str]) -> Tuple[float, float]:
    """
    Returns (power_mod, flex_mod) computed as product of per-leg modifiers.
    """
    power_mod = 1.0
    flex_mod = 1.0
    for lt in leg_types:
        s = strip_norm(lt)
        if s.startswith("goblin"):
            m = re.search(r"-(\d+)", s)
            dev = int(m.group(1)) if m else 1
            dev = max(1, min(3, dev))
            power_mod *= float(GOBLIN_POWER.get(dev, 0.84))
            flex_mod  *= float(GOBLIN_FLEX.get(dev, 0.80))
        elif s.startswith("demon"):
            m = re.search(r"\+(\d+)", s)
            dev = int(m.group(1)) if m else 1
            dev = max(1, min(3, dev))
            power_mod *= float(DEMON_POWER.get(dev, 1.627))
            flex_mod  *= float(DEMON_FLEX.get(dev, 1.60))
        else:
            # Standard
            pass
    return power_mod, flex_mod


def compute_ticket_payout(stake: float, mode: str,
                          legs: int, hits: int, misses: int, pushes: int, no_actual: int,
                          power_mod: float, flex_mod: float) -> Tuple[float, str, float]:
    """
    Returns (payout_amount, payout_status, applied_multiplier).
    payout_amount includes stake (total returned). profit = payout - stake.
    """
    if no_actual > 0:
        return np.nan, "NO_ACTUAL", np.nan

    effective_legs = legs - pushes
    if effective_legs <= 1:
        return stake, "REFUND", 1.0

    if mode == "power":
        if misses == 0:
            base = float(POWER_BASE.get(effective_legs, 0.0))
            mult = float(round(base * power_mod, 4))
            payout = stake * mult
            return payout, "WIN" if mult > 0 else "WIN_NO_MULT", mult
        return 0.0, "LOSE", 0.0

    if mode == "flex":
        base_table = FLEX_BASE.get(effective_legs, {})
        base = float(base_table.get(hits, 0.0))
        mult = float(round(base * flex_mod, 4)) if base > 0 else 0.0
        if mult == 0.0:
            return 0.0, "LOSE", 0.0
        return stake * mult, "CASH", mult

    raise ValueError(f"Unknown mode: {mode}")


# -----------------------------
# Analytics helpers
# -----------------------------
def pivot_roi(df: pd.DataFrame, group_cols: List[str], prefix: str) -> pd.DataFrame:
    g = (df
         .groupby(group_cols, dropna=False, as_index=False)
         .agg(
            tickets=("ticket_id", "nunique"),
            staked=("stake", "sum"),
            payout=("payout", "sum"),
            profit=("profit", "sum"),
            win_rate=("is_win", "mean"),
            cash_rate=("is_cash", "mean"),
            no_actual=("no_actual", "sum"),
         ))
    g["roi"] = np.where(g["staked"] > 0, g["profit"] / g["staked"], np.nan)
    g["win_rate"] = g["win_rate"].round(4)
    g["cash_rate"] = g["cash_rate"].round(4)
    g["roi"] = g["roi"].round(4)
    g = g.sort_values(["profit", "roi"], ascending=False).reset_index(drop=True)
    g.insert(0, "view", prefix)
    return g


def build_summary_kv(overall: dict) -> pd.DataFrame:
    return pd.DataFrame([{"metric": k, "value": v} for k, v in overall.items()])


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickets", required=True, help="combined_slate_tickets_YYYY-MM-DD.xlsx")
    ap.add_argument("--nba_actuals", required=True, help="actuals_nba_YYYY-MM-DD.csv")
    ap.add_argument("--cbb_actuals", required=True, help="actuals_cbb_YYYY-MM-DD.csv")
    ap.add_argument("--out", default="", help="Output graded workbook (default: <tickets>_GRADED.xlsx)")
    ap.add_argument("--mode", choices=["power", "flex", "both"], default="both")
    ap.add_argument("--stake", type=float, default=20.0)
    ap.add_argument("--payouts_json", default="", help="Optional JSON override for base payouts + modifiers")
    args = ap.parse_args()

    global POWER_BASE, FLEX_BASE, GOBLIN_POWER, GOBLIN_FLEX, DEMON_POWER, DEMON_FLEX

    if args.payouts_json:
        cfg = json.loads(Path(args.payouts_json).read_text(encoding="utf-8"))
        if "power_base" in cfg:
            POWER_BASE = {int(k): float(v) for k, v in cfg["power_base"].items()}
        if "flex_base" in cfg:
            FLEX_BASE = {int(n): {int(k): float(v) for k, v in tab.items()} for n, tab in cfg["flex_base"].items()}
        mods = cfg.get("mods", {})
        if "goblin_power" in mods:
            GOBLIN_POWER = {int(k): float(v) for k, v in mods["goblin_power"].items()}
        if "goblin_flex" in mods:
            GOBLIN_FLEX = {int(k): float(v) for k, v in mods["goblin_flex"].items()}
        if "demon_power" in mods:
            DEMON_POWER = {int(k): float(v) for k, v in mods["demon_power"].items()}
        if "demon_flex" in mods:
            DEMON_FLEX = {int(k): float(v) for k, v in mods["demon_flex"].items()}

    tickets_xlsx = Path(args.tickets)
    nba_csv = Path(args.nba_actuals)
    cbb_csv = Path(args.cbb_actuals)
    out_xlsx = Path(args.out) if args.out else tickets_xlsx.with_name(tickets_xlsx.stem + "_GRADED.xlsx")

    # actuals + lookups
    nba_act = prep_actuals(nba_csv, "NBA")
    cbb_act = prep_actuals(cbb_csv, "CBB")
    nba_lp, nba_lpt = build_lookup(nba_act)
    cbb_lp, cbb_lpt = build_lookup(cbb_act)

    # ticket sheets
    xls = pd.ExcelFile(tickets_xlsx)
    ticket_sheets = [s for s in xls.sheet_names if re.search(r"\b\d-?Leg\b", s, re.IGNORECASE)]

    leg_frames = []
    for s in ticket_sheets:
        legs = parse_ticket_sheet(tickets_xlsx, s)
        if not legs.empty:
            leg_frames.append(legs)
    if not leg_frames:
        raise RuntimeError("No ticket legs parsed. Check sheet format.")

    legs_df = pd.concat(leg_frames, ignore_index=True)
    legs_df["ticket_id"] = legs_df["sheet"].astype(str) + " | " + legs_df["ticket_no"].astype(str)

    # grade legs
    legs_df["actual"] = legs_df.apply(
        lambda r: lookup_actual(r["sport"], r["player"], r["team"], r["prop_norm"], nba_lpt, nba_lp, cbb_lpt, cbb_lp),
        axis=1,
    )
    legs_df["leg_result"] = legs_df.apply(lambda r: grade_leg(r["dir"], r["line"], r["actual"]), axis=1)

    # per-ticket modifiers
    mods_df = (legs_df.groupby("ticket_id", as_index=False)
               .agg(
                    leg_types=("leg_type", lambda s: list(s)),
                    sports=("sport", lambda s: ",".join(sorted(set([x for x in s if str(x).strip()])))),
                    pick_types=("pick_type", lambda s: ",".join(sorted(set([x for x in s if str(x).strip()])))),
                    tiers=("tier", lambda s: ",".join(sorted(set([x for x in s if str(x).strip()])))),
                ))
    mods_df[["power_mod", "flex_mod"]] = mods_df["leg_types"].apply(lambda L: pd.Series(leg_modifiers(L)))

    # ticket base stats
    ticket_base = (legs_df
        .groupby(["sheet", "ticket_no", "ticket_id"], as_index=False)
        .agg(
            legs=("leg_no", "max"),
            hits=("leg_result", lambda s: int((s == "HIT").sum())),
            misses=("leg_result", lambda s: int((s == "MISS").sum())),
            pushes=("leg_result", lambda s: int((s == "PUSH").sum())),
            no_actual=("leg_result", lambda s: int((s == "NO_ACTUAL").sum())),
        ))
    ticket_base["effective_legs"] = ticket_base["legs"] - ticket_base["pushes"]
    ticket_base["stake"] = float(args.stake)
    ticket_base = ticket_base.merge(mods_df[["ticket_id", "sports", "pick_types", "tiers", "power_mod", "flex_mod"]], on="ticket_id", how="left")

    modes = ["power", "flex"] if args.mode == "both" else [args.mode]
    ticket_rows = []
    for mode in modes:
        t = ticket_base.copy()
        payouts_out = []
        statuses = []
        mults = []
        for _, r in t.iterrows():
            payout_amt, status, mult = compute_ticket_payout(
                stake=float(r["stake"]),
                mode=mode,
                legs=int(r["legs"]),
                hits=int(r["hits"]),
                misses=int(r["misses"]),
                pushes=int(r["pushes"]),
                no_actual=int(r["no_actual"]),
                power_mod=float(r["power_mod"]),
                flex_mod=float(r["flex_mod"]),
            )
            payouts_out.append(payout_amt)
            statuses.append(status)
            mults.append(mult)
        t["mode"] = mode
        t["payout_status"] = statuses
        t["applied_mult"] = mults
        t["payout"] = payouts_out
        t["profit"] = t["payout"] - t["stake"]
        t["is_win"] = ((t["payout_status"] == "WIN") | (t["payout_status"] == "WIN_NO_MULT")).astype(int)
        t["is_cash"] = ((t["payout_status"] == "WIN") | (t["payout_status"] == "WIN_NO_MULT") | (t["payout_status"] == "CASH")).astype(int)
        ticket_rows.append(t)

    ticket_results = pd.concat(ticket_rows, ignore_index=True)

    # overall stats
    overall = {}
    for mode in modes:
        sub = ticket_results[ticket_results["mode"] == mode].copy()
        eligible = sub[sub["payout_status"] != "NO_ACTUAL"]
        overall[f"{mode}_tickets"] = int(sub["ticket_id"].nunique())
        overall[f"{mode}_eligible_tickets"] = int(eligible["ticket_id"].nunique())
        overall[f"{mode}_no_actual_tickets"] = int((sub["payout_status"] == "NO_ACTUAL").sum())
        overall[f"{mode}_staked"] = float(eligible["stake"].sum())
        overall[f"{mode}_payout"] = float(eligible["payout"].sum())
        overall[f"{mode}_profit"] = float(eligible["profit"].sum())
        overall[f"{mode}_roi"] = round(float(eligible["profit"].sum() / eligible["stake"].sum()) if eligible["stake"].sum() > 0 else 0.0, 4)
        overall[f"{mode}_win_rate"] = round(float(eligible["is_win"].mean()) if len(eligible) else 0.0, 4)
        overall[f"{mode}_cash_rate"] = round(float(eligible["is_cash"].mean()) if len(eligible) else 0.0, 4)

    tables = {}
    for mode in modes:
        sub = ticket_results[ticket_results["mode"] == mode].copy()
        eligible = sub[sub["payout_status"] != "NO_ACTUAL"].copy()
        tables[f"{mode}_BY_SHEET"] = pivot_roi(eligible, ["sheet"], f"{mode}_BY_SHEET")
        tables[f"{mode}_BY_LEGS"] = pivot_roi(eligible, ["effective_legs"], f"{mode}_BY_LEGS")
        tables[f"{mode}_BY_SPORTS"] = pivot_roi(eligible, ["sports"], f"{mode}_BY_SPORTS")
        tables[f"{mode}_BY_PICK_TYPES"] = pivot_roi(eligible, ["pick_types"], f"{mode}_BY_PICK_TYPES")

    summary_kv = build_summary_kv(overall)

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xw:
        summary_kv.to_excel(xw, index=False, sheet_name="SUMMARY")
        ticket_results.to_excel(xw, index=False, sheet_name="TICKET_RESULTS")
        legs_df.to_excel(xw, index=False, sheet_name="LEG_RESULTS")
        for name, tab in tables.items():
            tab.to_excel(xw, index=False, sheet_name=name[:31])

    print(f"✅ Wrote graded workbook → {out_xlsx}")


if __name__ == "__main__":
    main()
