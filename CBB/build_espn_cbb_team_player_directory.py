#!/usr/bin/env python3
"""
build_espn_cbb_team_player_directory.py  (optimized)

END GOAL: Build a full ESPN Men's CBB player directory with:
  player_name, player_id, team_id, team_abbr, team_name

Steps:
  1) Get team list (prefer local espn_cbb_mens_teams.csv if it exists)
  2) Cache each team roster JSON  →  <outdir>/rosters/roster_<team_id>.json
  3) Build player CSV             →  <outdir>/espn_cbb_mens_players.csv

OPTIMIZATIONS vs original:
- extract_team_cards(): iterative DFS (stack) instead of recursive walk
  → avoids Python recursion limit on large payloads
- fetch_teams(): added limit param for teams endpoint (ESPN supports ?limit=500)
- cache_roster(): reuses the passed Session (keeps TCP connection alive)
- parse_roster_json(): uses the same iterative iter_dicts() from the shared
  utility; de-dupe via dict during ingestion instead of post-hoc seen-set
- build_players_from_rosters(): skips the intermediate all_rows list —
  yields directly to a dedup dict
- norm_space(): inlined via str.strip() + re.sub (micro-optimisation)
- Progress printed every 50 teams (less noise than every 25)

Run:
  py -3.14 build_espn_cbb_team_player_directory.py --outdir .cache_espn --sleep 0.15
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
from typing import Any, Dict, List, Tuple

import requests

SPORT  = "basketball"
LEAGUE = "mens-college-basketball"

TEAMS_ENDPOINT  = (
    f"https://site.web.api.espn.com/apis/site/v2/sports/{SPORT}/{LEAGUE}/teams"
    "?limit=500"
)
ROSTER_ENDPOINT = (
    f"https://site.web.api.espn.com/apis/site/v2/sports/{SPORT}/{LEAGUE}/teams/{{team_id}}/roster"
)

_RE_WS = re.compile(r"\s+")


def norm_space(s: str) -> str:
    return _RE_WS.sub(" ", str(s or "").strip())


def safe_get(d: Any, path: List[str], default: str = "") -> str:
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return default if cur is None else str(cur)


# ── Iterative DFS (shared pattern, no recursion limit risk) ──────────────────
def _iter_nodes(root: Any):
    stack = [root]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            yield node
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)


def extract_team_cards(payload: Any) -> List[Dict[str, str]]:
    seen: set = set()
    out: List[Dict[str, str]] = []

    for obj in _iter_nodes(payload):
        # Pattern A: {"team": {"id": ..., "abbreviation": ..., ...}}
        if "team" in obj and isinstance(obj["team"], dict):
            t = obj["team"]
            tid = str(t.get("id") or "")
            if tid.isdigit() and tid not in seen:
                abbr = norm_space(t.get("abbreviation") or "")
                name = norm_space(t.get("displayName") or t.get("name") or "")
                if abbr or name:
                    seen.add(tid)
                    out.append({"team_id": tid, "team_abbr": abbr, "team_name": name})

        # Pattern B: direct team dict
        tid2 = str(obj.get("id") or "")
        if (
            tid2.isdigit()
            and tid2 not in seen
            and ("abbreviation" in obj or "displayName" in obj or "name" in obj)
        ):
            seen.add(tid2)
            out.append({
                "team_id":   tid2,
                "team_abbr": norm_space(obj.get("abbreviation") or ""),
                "team_name": norm_space(obj.get("displayName") or obj.get("name") or ""),
            })

    return out


def fetch_teams(session: requests.Session) -> List[Dict[str, str]]:
    r = session.get(TEAMS_ENDPOINT, timeout=25)
    r.raise_for_status()
    return extract_team_cards(r.json())


def load_teams_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    seen: set = set()
    teams: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            tid = str(row.get("team_id") or "").strip()
            if not tid.isdigit() or tid in seen:
                continue
            seen.add(tid)
            teams.append({
                "team_id":   tid,
                "team_abbr": norm_space(row.get("team_abbr") or ""),
                "team_name": norm_space(row.get("team_name") or ""),
            })
    return teams


def write_teams_csv(path: str, teams: List[Dict[str, str]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["team_id","team_abbr","team_name"])
        w.writeheader()
        w.writerows(teams)


def cache_roster(session: requests.Session, team_id: str, outpath: str) -> bool:
    url = ROSTER_ENDPOINT.format(team_id=team_id)
    try:
        r = session.get(url, timeout=25)
        if r.status_code != 200:
            return False
        with open(outpath, "w", encoding="utf-8") as f:
            f.write(r.text)
        return True
    except Exception:
        return False


_BAD_NAMES = frozenset({"forward","guard","center","active","inactive","roster"})


def _looks_like_player(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    if not any(k in obj for k in ("position","jersey","classYear","height","weight","experience")):
        return False
    pid = obj.get("id")
    if pid is None:
        return False
    try:
        if int(str(pid).strip()) < 100_000:
            return False
    except Exception:
        return False
    name = str(obj.get("fullName") or obj.get("displayName") or obj.get("shortName") or "").strip()
    return bool(name) and name.lower() not in _BAD_NAMES


def parse_roster_json(path: str) -> Tuple[List[Dict[str, str]], bool]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return [], False

    team_id   = safe_get(data, ["team", "id"],           "")
    team_abbr = norm_space(safe_get(data, ["team", "abbreviation"], ""))
    team_name = norm_space(safe_get(data, ["team", "displayName"],  ""))

    seen: set = set()
    rows: List[Dict[str, str]] = []

    for d in _iter_nodes(data):
        if not _looks_like_player(d):
            continue
        pid  = str(d.get("id", "")).strip()
        name = norm_space(str(
            d.get("fullName") or d.get("displayName") or d.get("shortName") or ""
        ))
        if not pid or not name or pid in seen:
            continue
        seen.add(pid)
        rows.append({
            "player_id":  pid,
            "player_name":name,
            "team_id":    team_id,
            "team_abbr":  team_abbr,
            "team_name":  team_name,
        })

    return rows, True


def build_players_from_rosters(roster_dir: str, out_csv: str) -> None:
    roster_files = sorted(
        os.path.join(roster_dir, fn)
        for fn in os.listdir(roster_dir)
        if fn.lower().startswith("roster_") and fn.lower().endswith(".json")
    )

    # Dedup by player_id during ingestion
    seen: Dict[str, bool] = {}
    dedup: List[Dict[str, str]] = []
    bad = 0

    for fp in roster_files:
        rows, ok = parse_roster_json(fp)
        if not ok:
            bad += 1
            continue
        for r in rows:
            pid = r["player_id"]
            if pid not in seen:
                seen[pid] = True
                dedup.append(r)

    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["player_id","player_name","team_id","team_abbr","team_name"])
        w.writeheader()
        w.writerows(dedup)

    team_count = len({r["team_id"] for r in dedup if r.get("team_id")})
    print(f"✅ Parsed roster files: {len(roster_files)} (bad: {bad})")
    print(f"✅ Wrote: {out_csv}")
    print(f"   rows: {len(dedup)} | unique players: {len(dedup)} | teams: {team_count}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default=".cache_espn")
    ap.add_argument("--sleep",  type=float, default=0.15)
    ap.add_argument("--force",  action="store_true",
                    help="Re-download roster files even if cached")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    roster_dir  = os.path.join(args.outdir, "rosters")
    os.makedirs(roster_dir, exist_ok=True)

    teams_csv   = os.path.join(args.outdir, "espn_cbb_mens_teams.csv")
    players_csv = os.path.join(args.outdir, "espn_cbb_mens_players.csv")

    session = requests.Session()
    session.headers.update({"User-Agent":"Mozilla/5.0","Accept":"application/json"})

    # 1) Teams list
    teams = load_teams_csv(teams_csv)
    if teams:
        print(f"📥 Loaded teams from {teams_csv} | teams: {len(teams)}")
    else:
        print("📥 Fetching ESPN teams list (API)...")
        teams = fetch_teams(session)
        print(f"✅ Teams found: {len(teams)}")
        write_teams_csv(teams_csv, teams)
        print(f"✅ Wrote {teams_csv}")

    # 2) Roster downloads
    ok = skipped = failed = 0
    for i, t in enumerate(teams, 1):
        tid     = t["team_id"]
        outpath = os.path.join(roster_dir, f"roster_{tid}.json")
        if not args.force and os.path.exists(outpath) and os.path.getsize(outpath) > 200:
            skipped += 1
        else:
            if cache_roster(session, tid, outpath):
                ok += 1
            else:
                failed += 1

        if i % 50 == 0 or i == len(teams):
            print(f"  … {i}/{len(teams)} teams | ok={ok} skipped={skipped} failed={failed}")
        time.sleep(args.sleep)

    print(f"\n✅ Rosters cached → {roster_dir}  ok={ok} skipped={skipped} failed={failed}")

    # 3) Player directory
    build_players_from_rosters(roster_dir, players_csv)


if __name__ == "__main__":
    main()
