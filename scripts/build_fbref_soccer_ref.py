#!/usr/bin/env python3
"""
build_fbref_soccer_ref.py — FBref fallback stats for soccer players
                            missing from ESPN boxscore cache.

FBref blocks automated scraping. Instead, save pages manually from your
browser once per season and place them in data/cache/fbref_html/:

  Championship summary : champ_summary.html
  Championship keepers : champ_keeper.html
  UCL summary          : ucl_summary.html
  UCL keepers          : ucl_keeper.html

To save: open the FBref URL in Chrome → Ctrl+S → "Webpage, Complete"
Re-save once per season (or mid-season if you add new leagues).

Players are stored in the `soccer` table in slateiq_ref.db using
espn_player_id = 'fbref_<id>' as a synthetic key.
step4_db_reader.get_vals_soccer() falls back to name matching for these.

Usage:
    py scripts/build_fbref_soccer_ref.py --date 2026-03-11
    py scripts/build_fbref_soccer_ref.py --list-leagues
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import unicodedata
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
_HERE     = Path(__file__).resolve().parent
DB_PATH   = _HERE.parent / "data" / "cache" / "slateiq_ref.db"
CACHE_DIR = _HERE.parent / "data" / "cache" / "fbref_html"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ── League config ─────────────────────────────────────────────────────────────
# local_files: filenames saved manually from browser into data/cache/fbref_html/
# FBref URLs for reference (open in browser, Ctrl+S to save):
#   Championship: https://fbref.com/en/comps/10/2025-2026/stats/2025-2026-Championship-Stats
#   UCL:          https://fbref.com/en/comps/8/2025-2026/stats/2025-2026-Champions-League-Stats
FBREF_LEAGUES = {
    "ENG-Championship": {
        "local_files": {
            "summary": "champ_summary.html",
            "keeper":  "champ_keeper.html",
        },
    },
    "UCL": {
        "local_files": {
            "summary": "ucl_summary.html",
            "keeper":  "ucl_keeper.html",
        },
    },
    "NOR-Eliteserien": {
        "local_files": {
            "summary": "nor_summary.html",
            "keeper":  "nor_keeper.html",
        },
    },
    "ENG-Premier League": {
        "local_files": {
            "summary": "epl_summary.html",
            "keeper":  "epl_keeper.html",
        },
    },
    "MLS": {
        "local_files": {
            "summary": "mls_summary.html",
            "keeper":  "mls_keeper.html",
        },
    },
}


def fbref_season(d: date) -> str:
    year = d.year if d.month >= 8 else d.year - 1
    return f"{year}-{year + 1}"


# ── HTML reading ──────────────────────────────────────────────────────────────
def _read_local(filename: str) -> Optional[str]:
    path = CACHE_DIR / filename
    if not path.exists():
        print(f"    ⚠️  File not found: {path}")
        print(f"         Save FBref page as '{filename}' into data/cache/fbref_html/")
        return None
    print(f"    reading: {path.name}  ({path.stat().st_size // 1024} KB)")
    return path.read_text(encoding="utf-8", errors="replace")


# ── Player ID extraction ───────────────────────────────────────────────────────
def _extract_player_ids(html: str) -> list[str]:
    ids, seen = [], set()
    for m in re.finditer(r'/en/players/([a-f0-9]{8})/', html):
        pid = m.group(1)
        if pid not in seen:
            seen.add(pid)
            ids.append(pid)
    return ids


# ── Name normalization ─────────────────────────────────────────────────────────
def _norm_name(name: str) -> str:
    name = unicodedata.normalize("NFD", str(name or ""))
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = re.sub(r"[^a-z0-9 ]", "", name.lower())
    return re.sub(r"\s+", " ", name).strip()


# ── Column helpers ────────────────────────────────────────────────────────────
def _flatten_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [
        "_".join(str(c).strip() for c in col if "Unnamed" not in str(c)).strip("_")
        or f"col_{i}"
        for i, col in enumerate(df.columns)
    ]
    return df


def _safe_float(v) -> Optional[float]:
    try:
        f = float(v)
        return None if f != f else f
    except (TypeError, ValueError):
        return None


def _get(row: pd.Series, *patterns) -> Optional[float]:
    """Return first non-null value whose column name contains any pattern."""
    for pat in patterns:
        for col in row.index:
            if pat.lower() in col.lower():
                v = _safe_float(row[col])
                if v is not None:
                    return v
    return None


def _map_summary(row: pd.Series) -> dict:
    return {
        "sh":  _get(row, "Standard_Sh", "Shots_Sh", "_Sh"),
        "sog": _get(row, "Standard_SoT", "Shots_SoT", "_SoT"),
        "g":   _get(row, "Performance_Gls", "Goals_Gls", "_Gls"),
        "a":   _get(row, "Performance_Ast", "Goals_Ast", "_Ast"),
        "sv":  None,
        "pa":  _get(row, "Total_Cmp", "Pass_Cmp", "Passes_Cmp"),
        "kp":  _get(row, "_KP", "KP_"),
        "tk":  _get(row, "Tackles_Tkl", "TklW_", "_Tkl"),
        "fc":  _get(row, "Performance_Fls", "_Fls"),
        "yc":  _get(row, "Performance_CrdY", "_CrdY"),
        "minutes_played": _get(row, "Playing Time_Min", "Time_Min", "_Min"),
    }


def _map_keeper(row: pd.Series) -> dict:
    return {
        "sh": None, "sog": None, "g": None, "a": None,
        "sv": _get(row, "Performance_Saves", "_Saves", "Saves_"),
        "pa": None, "kp": None, "tk": None, "fc": None, "yc": None,
        "minutes_played": _get(row, "Playing Time_Min", "Time_Min", "_Min"),
    }


# ── Parse one HTML file ───────────────────────────────────────────────────────
def _parse_html(html: str, stat_type: str, league_key: str) -> list[dict]:
    from io import StringIO
    try:
        # FBref wraps tables in HTML comments to block scrapers — unwrap them
        html_clean = re.sub(r"<!--(.*?)-->", r"\1", html, flags=re.DOTALL)
        tables = pd.read_html(StringIO(html_clean), header=[0, 1])
    except Exception as e:
        print(f"    ⚠️  parse error: {e}")
        return []

    if not tables:
        print("    ⚠️  no tables found in HTML")
        return []

    # Find the player stats table: largest table with a Player column and >10 rows
    df = None
    for t in sorted(tables, key=len, reverse=True):
        flat = _flatten_cols(t.copy())
        if "Player" in flat.columns and len(flat) > 10:
            df = flat
            break
    if df is None:
        sizes = [len(t) for t in tables]
        print(f"    ⚠️  no player table found (table sizes: {sizes})")
        return []

    # Drop repeated header rows FBref inserts every 25 rows
    df = df[df["Player"] != "Player"].copy()

    if df.empty:
        print("    ⚠️  table empty after cleaning")
        return []

    fbref_ids = _extract_player_ids(html)
    df = df.reset_index(drop=True)
    df["fbref_player_id"] = [
        fbref_ids[i] if i < len(fbref_ids) else "" for i in df.index
    ]

    sample_cols = [c for c in df.columns
                   if c not in ("Player", "Squad", "Nation", "Pos", "Age", "Born")]
    print(f"    {stat_type}: {len(df)} players | sample cols: {sample_cols[:8]}")

    rows = []
    for _, row in df.iterrows():
        player = str(row.get("Player", "") or "").strip()
        if not player or player == "Player":
            continue
        team     = str(row.get("Squad", "") or "").strip()
        fbref_id = str(row.get("fbref_player_id", "") or "").strip()
        stats    = _map_keeper(row) if stat_type == "keeper" else _map_summary(row)

        if all(v is None for v in stats.values()):
            continue

        rows.append({
            "player":    player,
            "team":      team,
            "fbref_id":  fbref_id,
            "stat_type": stat_type,
            "league":    league_key,
            **stats,
        })

    return rows


# ── Scrape one league (from local files) ─────────────────────────────────────
def scrape_league(league_key: str) -> list[dict]:
    cfg = FBREF_LEAGUES.get(league_key)
    if not cfg:
        print(f"  ⚠️  Unknown league: {league_key}")
        return []

    rows_out = []
    for stat_type, filename in cfg["local_files"].items():
        html = _read_local(filename)
        if not html:
            continue
        rows = _parse_html(html, stat_type, league_key)
        rows_out.extend(rows)

    return rows_out


# ── DB upsert ─────────────────────────────────────────────────────────────────
def upsert_rows(con: sqlite3.Connection, rows: list[dict],
                season: str, target_date: date) -> int:
    if not rows:
        return 0

    # Merge summary + keeper rows for same player (so sv lands on outfield record)
    by_player: dict[tuple, dict] = {}
    for r in rows:
        key = (r["league"], _norm_name(r["player"]), r.get("team", ""))
        if key not in by_player:
            by_player[key] = r.copy()
        else:
            for k, v in r.items():
                if v is not None and by_player[key].get(k) is None:
                    by_player[key][k] = v

    cols = [
        "game_date", "event_id", "league", "home_team", "away_team",
        "player", "team", "espn_player_id",
        "sh", "sog", "g", "a", "sv", "pa", "kp", "tk", "fc", "yc",
        "minutes_played",
    ]
    sql = f"""
        INSERT OR REPLACE INTO soccer ({', '.join(cols)})
        VALUES ({', '.join('?' * len(cols))})
    """

    season_end_year = int(season.split("-")[1])
    game_date = f"{season_end_year}-07-01"

    data = []
    for (league, _, _), r in by_player.items():
        fbref_id     = r.get("fbref_id", "")
        synthetic_id = (f"fbref_{fbref_id}" if fbref_id
                        else f"fbref_{_norm_name(r['player'])}")
        event_id = f"fbref_{r['league']}_{season}"
        data.append([
            game_date, event_id, r["league"], "", "",
            r["player"], r.get("team", ""), synthetic_id,
            r.get("sh"), r.get("sog"), r.get("g"), r.get("a"),
            r.get("sv"), r.get("pa"), r.get("kp"), r.get("tk"),
            r.get("fc"), r.get("yc"), r.get("minutes_played"),
        ])

    with con:
        con.executemany(sql, data)
    return len(data)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description="Load local FBref HTML files into soccer stats table"
    )
    ap.add_argument("--date",    default="",
                    help="YYYY-MM-DD reference date (default: yesterday)")
    ap.add_argument("--leagues", nargs="+",
                    default=["ENG-Championship", "UCL"],
                    choices=list(FBREF_LEAGUES.keys()),
                    help="Leagues to process")
    ap.add_argument("--db",      default="", help="Override DB path")
    ap.add_argument("--list-leagues", action="store_true",
                    help="Show available league keys and exit")
    args = ap.parse_args()

    if args.list_leagues:
        print("Available leagues (save HTML files first):")
        for k, v in FBREF_LEAGUES.items():
            files = ", ".join(v["local_files"].values())
            print(f"  {k:25s}  →  {files}")
        return

    target_date = (
        date.fromisoformat(args.date) if args.date
        else date.today() - timedelta(days=1)
    )
    season  = fbref_season(target_date)
    db_path = Path(args.db) if args.db else DB_PATH

    print(f"📅 FBref loader | season: {season} | DB: {db_path}")
    print(f"   HTML source:  {CACHE_DIR}")

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL;")

    total = 0
    for league_key in args.leagues:
        print(f"\n→ {league_key}")
        rows = scrape_league(league_key)
        if not rows:
            print("  ⚠️  No rows parsed")
            continue
        n = upsert_rows(con, rows, season, target_date)
        total += n
        print(f"  ✅ {n} players upserted")

    con.close()
    print(f"\n✅ Done — {total} total players upserted into soccer table")
    if total == 0:
        print("\n💡 No data loaded. Make sure HTML files are saved in:")
        print(f"   {CACHE_DIR}")
        print("   Files needed: champ_summary.html, champ_keeper.html,")
        print("                 ucl_summary.html,   ucl_keeper.html")


if __name__ == "__main__":
    main()
