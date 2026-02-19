#!/usr/bin/env python3
"""
cache_all_espn_rosters.py  (optimized)

Downloads ESPN Men's CBB team rosters for all teams in a teams JSON file
and saves each as roster_<team_id>.json in the output directory.

OPTIMIZATIONS vs original:
- extract_team_ids(): iterative DFS instead of recursive (no recursion limit risk)
- Session reuse with keep-alive (avoids per-request TCP handshake overhead)
- Skipped-file check uses a single os.stat() call instead of two (exists + getsize)
- Progress every 50 teams (less noise than every 25)
- Added --force flag (already existed, kept)
- Type hints cleaned up

Usage:
  py -3.14 cache_all_espn_rosters.py \
      --teams_json .cache_espn\teams_mens_cbb.json \
      --outdir .cache_espn\rosters \
      --sleep 0.15
"""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Set

import requests

ROSTER_URL = (
    "https://site.web.api.espn.com/apis/site/v2/sports/basketball/"
    "mens-college-basketball/teams/{team_id}/roster"
)


def extract_team_ids(obj: Any) -> Set[str]:
    """Iterative DFS to find all ESPN team IDs in a JSON payload."""
    ids: Set[str] = set()
    stack = [obj]

    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            # Pattern A: {"team": {"id": "52", ...}}
            if "team" in node and isinstance(node["team"], dict):
                tid = node["team"].get("id")
                if tid and str(tid).isdigit():
                    ids.add(str(tid))

            # Pattern B: {"id": "52", "abbreviation": ...}
            tid2 = node.get("id")
            if (
                tid2
                and str(tid2).isdigit()
                and ("abbreviation" in node or "displayName" in node or "name" in node)
            ):
                ids.add(str(tid2))

            stack.extend(node.values())

        elif isinstance(node, list):
            stack.extend(node)

    return ids


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--teams_json", required=True)
    ap.add_argument("--outdir",     required=True)
    ap.add_argument("--sleep",      type=float, default=0.15)
    ap.add_argument("--timeout",    type=int,   default=20)
    ap.add_argument("--force",      action="store_true",
                    help="Re-download even if cached file exists")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    with open(args.teams_json, "r", encoding="utf-8") as f:
        teams_payload = json.load(f)

    team_ids = sorted(extract_team_ids(teams_payload), key=lambda x: int(x))
    print(f"✅ Team IDs found: {len(team_ids)}")

    session = requests.Session()
    session.headers.update({"User-Agent":"Mozilla/5.0","Accept":"application/json"})

    ok = skipped = failed = 0

    for i, tid in enumerate(team_ids, 1):
        out_path = os.path.join(args.outdir, f"roster_{tid}.json")

        # Single stat call instead of exists() + getsize()
        if not args.force:
            try:
                if os.stat(out_path).st_size > 50:
                    skipped += 1
                    if i % 50 == 0 or i == len(team_ids):
                        print(f"  … {i}/{len(team_ids)} | ok={ok} skipped={skipped} failed={failed}")
                    continue
            except FileNotFoundError:
                pass  # File doesn't exist, proceed to download

        url = ROSTER_URL.format(team_id=tid)
        try:
            r = session.get(url, timeout=args.timeout)
            if r.status_code != 200:
                failed += 1
            else:
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(r.text)
                ok += 1
        except Exception:
            failed += 1

        if i % 50 == 0 or i == len(team_ids):
            print(f"  … {i}/{len(team_ids)} | ok={ok} skipped={skipped} failed={failed}")

        time.sleep(args.sleep)

    print(f"\n✅ Finished roster caching → {args.outdir}")
    print(f"   ok={ok} skipped={skipped} failed={failed}")


if __name__ == "__main__":
    main()
