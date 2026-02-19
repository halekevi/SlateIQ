#!/usr/bin/env python3
"""
build_player_directory_from_cached_rosters.py  (optimized)

Builds a CBB player directory from locally cached ESPN roster JSON files.

OPTIMIZATIONS vs original:
- iter_dicts() is now iterative (stack-based) instead of recursive to avoid
  Python recursion limits on deeply nested JSON and reduce call overhead
- looks_like_player(): early-return ordering (cheapest checks first)
- extract_players_from_roster_json(): single pass, no redundant generator wrap
- Duplicate removal via dict keyed on (player_id, team_id) instead of
  DataFrame.drop_duplicates (avoids constructing an intermediate large df)
- Progress counter printed every 50 files instead of only at the end
- Diagnostics block consolidated

Example:
  py -3.14 build_player_directory_from_cached_rosters.py \
      --roster_dir .cache_espn\rosters \
      --out .cache_espn\espn_cbb_mens_players.csv
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd


# ── Helpers ───────────────────────────────────────────────────────────────────
def safe_get(d: Any, path: List[str], default: str = "") -> str:
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return default if cur is None else str(cur)


def normalize_name(x: str) -> str:
    x = (x or "").strip()
    return re.sub(r"\s+", " ", x)


def extract_team_meta(j: Dict[str, Any]) -> Tuple[str, str]:
    team_abbr = ""
    team_name = ""
    if isinstance(j.get("team"), dict):
        t = j["team"]
        team_abbr = str(
            t.get("abbreviation") or t.get("shortDisplayName") or t.get("name") or ""
        ).strip()
        team_name = str(
            t.get("displayName") or t.get("name") or t.get("shortDisplayName") or ""
        ).strip()
    return team_abbr, team_name


_BAD_NAMES = frozenset({"forward","guard","center","active","inactive","roster"})


def looks_like_player(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    # Cheapest checks first (no type conversions)
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
    name = str(
        obj.get("fullName") or obj.get("displayName") or obj.get("shortName") or obj.get("name") or ""
    ).strip()
    return bool(name) and name.lower() not in _BAD_NAMES


def iter_dicts(root: Any) -> Iterable[Dict[str, Any]]:
    """
    Iterative (stack-based) DFS over a JSON tree — avoids Python recursion limits.
    Yields every dict encountered.
    """
    stack = [root]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            yield node
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)


def extract_players_from_roster_json(j: Dict[str, Any]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for d in iter_dicts(j):
        if not looks_like_player(d):
            continue
        pid  = str(d.get("id", "")).strip()
        name = normalize_name(str(
            d.get("fullName") or d.get("displayName") or d.get("shortName") or d.get("name") or ""
        ))
        if not pid or not name:
            continue

        pos = ""
        p = d.get("position")
        if isinstance(p, dict):
            pos = str(p.get("abbreviation") or p.get("name") or "").strip()
        elif p is not None:
            pos = str(p).strip()

        jersey = str(d.get("jersey") or d.get("uniform") or "").strip()

        class_year = ""
        cy = d.get("classYear")
        if isinstance(cy, dict):
            class_year = str(cy.get("displayValue") or cy.get("value") or "").strip()
        elif cy is not None:
            class_year = str(cy).strip()

        def _dim(v: Any) -> str:
            if isinstance(v, (int, float, str)):
                return str(v).strip()
            if isinstance(v, dict):
                return str(v.get("displayValue") or v.get("value") or "").strip()
            return ""

        out.append({
            "player_id":  pid,
            "player_name":name,
            "position":   pos,
            "jersey":     jersey,
            "class":      class_year,
            "height":     _dim(d.get("height")),
            "weight":     _dim(d.get("weight")),
        })
    return out


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--roster_dir", required=True)
    ap.add_argument("--out",        required=True)
    args = ap.parse_args()

    roster_glob = os.path.join(args.roster_dir, "roster_*.json")
    files = sorted(glob.glob(roster_glob))
    if not files:
        raise SystemExit(f"❌ No roster files found at: {roster_glob}")

    # Use dict keyed on (player_id, team_id) for O(1) dedup during ingestion
    seen: Dict[Tuple[str, str], bool] = {}
    rows: List[Dict[str, str]] = []
    bad = 0

    for i, fp in enumerate(files, 1):
        m = re.match(r"roster_(\d+)\.json$", os.path.basename(fp))
        team_id = m.group(1) if m else ""
        try:
            with open(fp, "r", encoding="utf-8") as f:
                j = json.load(f)
        except Exception:
            bad += 1
            continue

        team_abbr, team_name = extract_team_meta(j)
        for p in extract_players_from_roster_json(j):
            key = (p["player_id"], team_id)
            if key in seen:
                continue
            seen[key] = True
            r = dict(p)
            r["team_id"]   = team_id
            r["team_abbr"] = team_abbr
            r["team_name"] = team_name
            rows.append(r)

        if i % 50 == 0 or i == len(files):
            print(f"  … {i}/{len(files)} files | players so far: {len(rows)}")

    print(f"✅ Parsed roster files: {len(files)} (bad: {bad})")

    df = pd.DataFrame(rows)
    if df.empty:
        # Diagnostic: show top-level keys from sample file
        try:
            with open(files[0], "r", encoding="utf-8") as f:
                sj = json.load(f)
            keys = list(sj.keys()) if isinstance(sj, dict) else type(sj).__name__
            print(f"❌ 0 players extracted. Sample file top-level keys: {keys}")
        except Exception:
            pass
        df.to_csv(args.out, index=False, encoding="utf-8")
        print(f"✅ Wrote empty file: {args.out}")
        return

    cols_order = ["player_id","player_name","team_id","team_abbr","team_name",
                  "position","jersey","class","height","weight"]
    df = df[[c for c in cols_order if c in df.columns]]

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    df.to_csv(args.out, index=False, encoding="utf-8")

    print(f"✅ Wrote: {args.out}")
    print(f"   rows: {len(df)} | unique players: {df['player_id'].nunique()} | teams: {df['team_id'].nunique()}")
    print("\n📌 Sample:")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
