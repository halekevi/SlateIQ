#!/usr/bin/env python3
"""
cbb_step3b_attach_def_rankings.py
----------------------------------
Fetches CBB defensive rankings from sports-reference.com opponent stats page,
maps to your pipeline team abbreviations, assigns Elite/Above Avg/Avg/Weak tiers,
and attaches to your normalized slate so step6 can use def_rank_signal.

New columns added:
  opp_def_rank      : rank 1=best defense (fewest opp pts per game)
  opp_def_ppg       : opponent points allowed per game
  opp_def_tier      : Elite / Above Avg / Avg / Weak
  OVERALL_DEF_RANK  : alias for opp_def_rank (step6 reads this name)

Usage:
  py -3.14 cbb_step3b_attach_def_rankings.py --input step2_cbb.csv --output step3b_with_def_rankings_cbb.csv --save_rankings cbb_def_rankings.csv
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from typing import Dict, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import pandas as pd
import numpy as np


TIER_CUTOFFS = [
    ("Elite",     0.00, 0.25),
    ("Above Avg", 0.25, 0.50),
    ("Avg",       0.50, 0.75),
    ("Weak",      0.75, 1.01),
]

SR_TO_PIPELINE: Dict[str, str] = {
    "Air Force": "AFA", "Alabama": "ALA", "Arizona": "ARIZ", "Arizona State": "ASU",
    "Arkansas": "ARK", "Auburn": "AUB", "Baylor": "BAY", "Boston College": "BC",
    "Boise State": "BSU", "Butler": "BUT", "BYU": "BYU", "California": "CAL",
    "Cincinnati": "CIN", "Colorado": "COLO", "Colorado State": "CSU", "Connecticut": "CONN",
    "Dayton": "DAY", "DePaul": "DEP", "Duke": "DUKE", "Duquesne": "DUQ",
    "Florida": "FLA", "Florida State": "FSU", "Gonzaga": "GONZ", "Georgia": "UGA",
    "Georgia Tech": "GT", "Georgetown": "GTWN", "Grand Canyon": "GC",
    "Seton Hall": "HALL", "Houston": "HOU", "Illinois": "ILL", "Iowa State": "ISU",
    "Kansas": "KU", "Kansas State": "KSU", "Kentucky": "UK", "LSU": "LSU",
    "Louisville": "LOU", "Maryland": "MD", "Miami (FL)": "MIA", "Miami (Ohio)": "MIOH",
    "Michigan": "MICH", "Michigan State": "MSST", "Missouri": "MIZZ",
    "Nebraska": "NEB", "Nevada": "NEV", "North Carolina": "UNC",
    "North Carolina State": "NCST", "NC State": "NCST", "Notre Dame": "ND",
    "Ohio State": "OSU", "Oklahoma": "OKLA", "Oklahoma State": "OKST",
    "Oregon": "ORE", "Pacific": "PAC", "Penn State": "PSU", "Pittsburgh": "PITT",
    "Portland": "PORT", "Providence": "PROV", "Saint Mary's (CA)": "SMC",
    "Saint Mary's": "SMC", "Seattle": "SEA", "SMU": "SMU", "South Carolina": "SCAR",
    "San Diego State": "SDSU", "San Jose State": "SJSU", "Stanford": "STAN",
    "Syracuse": "SYR", "TCU": "TCU", "Tennessee": "TENN", "Texas": "TEX",
    "Texas A&M": "TXAM", "Texas Tech": "TTU", "UCF": "UCF", "UCLA": "UCLA",
    "UNLV": "UNLV", "USC": "USC", "Utah": "UTAH", "Utah State": "USU",
    "Virginia": "UVA", "Vanderbilt": "VAN", "Villanova": "VILL",
    "Washington": "WASH", "Washington State": "WSU", "West Virginia": "WVU",
    "Wisconsin": "WIS", "Wyoming": "WYO", "Xavier": "XAV",
    "Wichita State": "WICH", "Mississippi State": "MSST",
}


def http_get(url: str, retries: int = 4, backoff: float = 2.0) -> bytes:
    last_err: Optional[Exception] = None
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }
    for attempt in range(retries):
        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=30) as r:
                raw = r.read()
                # Handle gzip
                if r.info().get('Content-Encoding') == 'gzip':
                    import gzip
                    raw = gzip.decompress(raw)
                return raw
        except (HTTPError, URLError, TimeoutError) as e:
            last_err = e
            wait = backoff * (attempt + 1)
            print(f"  Attempt {attempt+1} failed: {e}. Retrying in {wait:.0f}s...")
            time.sleep(wait)
    raise RuntimeError(f"Failed GET {url}: {last_err}")


def parse_sr_html(html: str) -> pd.DataFrame:
    """
    Parse sports-reference opponent stats HTML.
    The table is hidden in HTML comments — uncomment it first.
    Extracts school name and opp_pts (total opponent points, used to compute per-game PPG).
    """
    # Uncomment sports-reference hidden tables
    html = re.sub(r'<!--(.*?)-->', r'\1', html, flags=re.DOTALL)

    # Extract all table rows with data-stat attributes
    # Pattern: find school_name and opp_pts from the same row
    rows = []

    # Find all <tr> blocks
    tr_pattern = re.compile(r'<tr[^>]*>(.*?)</tr>', re.DOTALL)
    school_pattern = re.compile(r'data-stat="school_name"[^>]*>(?:<[^>]+>)*([^<]+)', re.DOTALL)
    opp_pts_pattern = re.compile(r'data-stat="opp_pts"[^>]*>([^<]+)', re.DOTALL)
    games_pattern = re.compile(r'data-stat="g"[^>]*>([^<]+)', re.DOTALL)

    for tr_match in tr_pattern.finditer(html):
        tr = tr_match.group(1)

        school_m = school_pattern.search(tr)
        opp_pts_m = opp_pts_pattern.search(tr)
        games_m = games_pattern.search(tr)

        if not school_m or not opp_pts_m:
            continue

        school = school_m.group(1).strip()
        # Remove asterisks/daggers that sports-ref adds
        school = school.rstrip('*†').strip()

        try:
            opp_pts = float(opp_pts_m.group(1).strip())
        except ValueError:
            continue

        games = 1
        if games_m:
            try:
                games = float(games_m.group(1).strip())
            except ValueError:
                pass

        if not school or school in ('School', 'Rk', ''):
            continue
        if opp_pts <= 0 or games <= 0:
            continue

        opp_ppg = opp_pts / games
        rows.append({"sr_name": school, "opp_pts": opp_pts, "games": games, "opp_ppg": round(opp_ppg, 2)})

    if not rows:
        raise RuntimeError("No valid rows parsed from sports-reference HTML.")

    df = pd.DataFrame(rows)
    df = df.sort_values("opp_ppg", ascending=True).reset_index(drop=True)
    df["overall_rank"] = df.index + 1
    return df


def assign_tier(rank: int, n_total: int) -> str:
    pct = (rank - 1) / max(n_total - 1, 1)
    for name, lo, hi in TIER_CUTOFFS:
        if lo <= pct < hi:
            return name
    return "Weak"


def build_map(df: pd.DataFrame) -> Dict[str, Tuple[int, float, str]]:
    n = len(df)
    result = {}
    for _, row in df.iterrows():
        name = str(row["sr_name"])
        abbr = SR_TO_PIPELINE.get(name)
        if not abbr:
            clean = name.rstrip("*†").strip()
            abbr = SR_TO_PIPELINE.get(clean)
        if abbr:
            result[abbr] = (
                int(row["overall_rank"]),
                float(row["opp_ppg"]),
                assign_tier(int(row["overall_rank"]), n)
            )
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",         required=True)
    ap.add_argument("--output",        default="step3b_with_def_rankings_cbb.csv")
    ap.add_argument("--save_rankings", default="")
    ap.add_argument("--year",          default="2026")
    args = ap.parse_args()

    print(f"→ Loading slate: {args.input}")
    df = pd.read_csv(args.input, dtype=str).fillna("")
    print(f"  rows={len(df)}")

    if "opp_team_abbr" not in df.columns:
        print("❌ Missing 'opp_team_abbr' — run step2_normalize first.")
        sys.exit(1)

    url = f"https://www.sports-reference.com/cbb/seasons/men/{args.year}-opponent-stats.html"
    print(f"→ Fetching: {url}")
    raw = http_get(url)
    html = raw.decode("utf-8", errors="replace")
    print(f"  Downloaded {len(html):,} chars")

    rank_df = parse_sr_html(html)
    print(f"  → Parsed {len(rank_df)} teams")
    print(f"  → Best defense:  {rank_df.iloc[0]['sr_name']} ({rank_df.iloc[0]['opp_ppg']:.1f} opp PPG)")
    print(f"  → Worst defense: {rank_df.iloc[-1]['sr_name']} ({rank_df.iloc[-1]['opp_ppg']:.1f} opp PPG)")

    if args.save_rankings:
        rank_df.to_csv(args.save_rankings, index=False)
        print(f"  → Rankings saved: {args.save_rankings}")

    pipeline_map = build_map(rank_df)
    print(f"  → Mapped {len(pipeline_map)} abbreviations")

    opp_ranks, opp_ppg_col, opp_tiers = [], [], []
    unmatched = set()

    for opp in df["opp_team_abbr"].astype(str).str.strip():
        if opp in pipeline_map:
            rank, ppg, tier = pipeline_map[opp]
            opp_ranks.append(rank)
            opp_ppg_col.append(ppg)
            opp_tiers.append(tier)
        else:
            opp_ranks.append(np.nan)
            opp_ppg_col.append(np.nan)
            opp_tiers.append("")
            if opp and opp not in ("", "nan"):
                unmatched.add(opp)

    df["opp_def_rank"]     = opp_ranks
    df["opp_def_ppg"]      = opp_ppg_col
    df["opp_def_tier"]     = opp_tiers
    df["OVERALL_DEF_RANK"] = opp_ranks  # step6 reads this column name

    filled = int(pd.Series(opp_ranks).notna().sum())
    print(f"\n✅ Coverage: {filled}/{len(df)} rows ({filled/len(df)*100:.1f}%)")

    if unmatched:
        print(f"  ⚠️  Unmatched abbreviations: {sorted(unmatched)}")
        print("     Add these to SR_TO_PIPELINE dict if needed.")

    print("\nTier breakdown:")
    print(pd.Series(opp_tiers).replace("", "UNMAPPED").value_counts().to_string())

    df.to_csv(args.output, index=False)
    print(f"\n✅ Saved → {args.output}")
    print(f"\nNext step:")
    print(f"  py -3.14 cbb_step5b_attach_boxscore_stats.py --input {args.output} --output step5b_with_stats_cbb.csv")


if __name__ == "__main__":
    main()
