#!/usr/bin/env python3
import argparse
import pandas as pd

def build_shooting_rows(player_name, team, opp, sport, date_str, stats):
    rows = []
    fgm = stats.get("FGM")
    fga = stats.get("FGA")
    fg3m = stats.get("FG3M")
    fg3a = stats.get("FG3A")

    fg2a = None
    fg2m = None

    if fga is not None and fg3a is not None:
        fg2a = float(fga) - float(fg3a)

    if fgm is not None and fg3m is not None:
        fg2m = float(fgm) - float(fg3m)

    shooting = [
        ("Field Goals Made", fgm),
        ("Field Goals Attempted", fga),
        ("3-PT Made", fg3m),
        ("3-PT Attempted", fg3a),
        ("2-PT Made", fg2m),
        ("2-PT Attempted", fg2a),
    ]

    for ptype, val in shooting:
        if val is None:
            continue
        rows.append({
            "sport": sport,
            "date": date_str,
            "team": team,
            "opp": opp,
            "player": player_name,
            "prop_type": ptype,
            "actual": float(val)
        })
    return rows

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sport", required=True)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    print("This is the upgraded shooting-support template.")
    print("Integrate build_shooting_rows() into your stat loop.")

if __name__ == "__main__":
    main()
