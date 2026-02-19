#!/usr/bin/env python3
import argparse
import pandas as pd
import requests
import time
import math
import re
import unicodedata


# ---------------- NORMALIZATION ----------------

def norm_name(s):
    s = "" if s is None else str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s


def norm_prop(s):
    s = "" if s is None else str(s)
    s = s.lower().replace(" ", "").replace("-", "")
    if s in ("points","pts"): return "points"
    if s in ("rebounds","reb"): return "rebounds"
    if s in ("assists","ast"): return "assists"
    if s in ("pra","pointsreboundsassists"): return "pra"
    if s in ("ptsrebs","pr"): return "pr"
    if s in ("ptsasts","pa"): return "pa"
    if s in ("rebsasts","ra"): return "ra"
    if "3pt" in s: return "fg3m"
    if "fantasy" in s: return "fantasy"
    if "turnover" in s: return "turnovers"
    return s


def odds_to_pick(odds):
    o = "" if odds is None else str(odds).lower()
    if "goblin" in o: return "Goblin"
    if "demon" in o: return "Demon"
    return "Standard"


# ---------------- FETCH PRIZEPICKS ----------------

def fetch_pp():
    url = "https://api.prizepicks.com/projections"
    params = {
        "league_id": 7,
        "per_page": 250,
        "page": 1,
        "single_stat": "true",
        "state": "prizepicks",
        "in_game": "true",
    }

    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    js = r.json()

    included = js.get("included", [])
    data = js.get("data", [])

    players = {}
    for i in included:
        if i.get("type") in ("player", "new_player"):
            players[i["id"]] = i["attributes"]["name"]

    rows = []
    for p in data:
        attr = p["attributes"]
        rel = p["relationships"]["new_player"]["data"]["id"]

        rows.append({
            "player": players.get(rel, ""),
            "player_key": norm_name(players.get(rel, "")),
            "prop_key": norm_prop(attr.get("stat_type")),
            "line": float(attr.get("line_score")),
            "pick_type": odds_to_pick(attr.get("odds_type")),
            "odds_type_raw": attr.get("odds_type"),
            "projection_id": p.get("id"),
        })

    return pd.DataFrame(rows)


# ---------------- MAIN ----------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.input)

    df["player_key"] = df["player"].apply(norm_name)
    df["prop_key"] = df["prop_type"].apply(norm_prop)
    df["line"] = pd.to_numeric(df["line"], errors="coerce")

    print("Fetching PrizePicks…")
    pp = fetch_pp()

    out_pick = []
    out_odds = []
    out_pid = []

    for _, r in df.iterrows():
        sub = pp[
            (pp["player_key"] == r["player_key"]) &
            (pp["prop_key"] == r["prop_key"])
        ]

        if sub.empty:
            out_pick.append("Standard")
            out_odds.append("")
            out_pid.append("")
            continue

        sub["diff"] = (sub["line"] - r["line"]).abs()
        best = sub.sort_values("diff").iloc[0]

        out_pick.append(best["pick_type"])
        out_odds.append(best["odds_type_raw"])
        out_pid.append(best["projection_id"])

    df["pick_type"] = out_pick
    df["odds_type_raw"] = out_odds
    df["projection_id"] = out_pid

    df.drop(columns=["player_key","prop_key"], inplace=True)

    df.to_csv(args.output, index=False)
    print("Saved:", args.output)


if __name__ == "__main__":
    main()
