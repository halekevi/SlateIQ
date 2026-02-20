#!/usr/bin/env python3
"""
step1_fetch_prizepicks_api.py  (NBA Pipeline A - upgraded)

Fetches PrizePicks projections from the public API and writes a flat CSV
for downstream pipeline steps.

Key upgrades:
- Supports --league_id (default NBA=7)
- Supports --game_mode (default pickem)
- Pagination with per_page (default 250)
- Derives team/opp_team using included new_player + new_game when available
- Adds pick_type normalized (Standard/Goblin/Demon)
- Board size guard (min_rows/min_teams)

NEW (Rate-limit robust):
- On HTTP 429:
    * cooldown sleep (default 60s + jitter)
    * retry the same page
    * after 2 cooldowns total, STOP pagination early and continue pipeline
  Key principle: do not let Step1 block the whole run.

HOTFIX (Pagination + Dupes):
- Uses PrizePicks pagination parameters: page[number] and page[size]
- Stops pagination early if a page returns 0 new projection IDs
- Hard de-duplicates output by projection_id as a safety belt

Outputs: step1_fetch_prizepicks_api.csv
"""

from __future__ import annotations

import argparse
import json
import re
import time
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Set

import pandas as pd
import requests

API_URL = "https://api.prizepicks.com/projections"
WARMUP_URL = "https://api.prizepicks.com/leagues"

# Rotate through realistic Chrome user-agents
USER_AGENTS = [
    # Keep UA + Client Hints consistent (Chromium on Windows).
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def _ua_chrome_major(ua: str) -> str:
    m = re.search(r"Chrome/(\d+)", ua)
    return m.group(1) if m else "122"


def _make_headers(ua: str) -> dict:
    """Build browser-like headers with consistent UA + Client Hints."""
    major = _ua_chrome_major(ua)
    return {
        "User-Agent": ua,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        # Let requests negotiate encoding; avoid 'br' (brotli) to reduce decode issues.
        "Origin": "https://app.prizepicks.com",
        "Referer": "https://app.prizepicks.com/board",
        # Client hints: keep aligned with Chrome UA
        "Sec-Ch-Ua": f'\"Chromium\";v=\"{major}\", \"Not(A:Brand\";v=\"24\", \"Google Chrome\";v=\"{major}\"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def _warm_session(session: requests.Session, ua: str) -> None:
    """
    Hit the leagues endpoint first to establish a session like a real browser would.
    This primes any server-side session tracking before we hit the projections endpoint.
    """
    try:
        headers = _make_headers(ua)
        r = session.get(WARMUP_URL, headers=headers, timeout=15)
        if r.status_code == 200:
            print(f"  🌐 Session warmed (leagues endpoint: {r.status_code})")
        else:
            print(f"  ⚠️ Warmup returned {r.status_code} — continuing anyway")
        time.sleep(random.uniform(1.5, 3.0))
    except Exception as e:
        print(f"  ⚠️ Session warmup failed: {e} — continuing anyway")

PICKTYPE_MAP = {
    "standard": "Standard",
    "goblin": "Goblin",
    "demon": "Demon",
}


def _safe_get(d: dict, path: List[str], default=""):
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur if cur is not None else default


def _norm_team(s: str) -> str:
    return str(s or "").strip().upper()


def _parse_iso(dt_str: str) -> str:
    # keep as original string; downstream can parse
    return (dt_str or "").strip()


def _included_index(included: List[dict]) -> Dict[Tuple[str, str], dict]:
    idx: Dict[Tuple[str, str], dict] = {}
    for obj in included or []:
        t = str(obj.get("type", "")).strip()
        i = str(obj.get("id", "")).strip()
        if t and i:
            idx[(t, i)] = obj
    return idx


def fetch_pages(
    url: str,
    league_id: str,
    game_mode: str,
    per_page: int,
    max_pages: int,
    sleep: float,
    cooldown_seconds: float,
    max_cooldowns: int,
    jitter_seconds: float,
) -> Tuple[List[dict], List[dict], List[dict]]:

    all_data: List[dict] = []
    all_included: List[dict] = []
    raw_pages: List[dict] = []

    cooldowns_used = 0
    stop_paging = False

    seen_projection_ids: Set[str] = set()
    no_new_pages_in_a_row = 0

    # Use a persistent session (keeps cookies, connection pooling — mimics browser)
    session = requests.Session()

    # Pick one UA per run and stick with it (rotating mid-session looks more suspicious)
    ua = random.choice(USER_AGENTS)
    headers = _make_headers(ua)

    # Warm the session: hit the leagues endpoint first like a real browser navigating the app
    _warm_session(session, ua)

    for page in range(1, max_pages + 1):
        if stop_paging:
            break

                params = {
            "league_id": str(league_id),
            "game_mode": str(game_mode),
            # Use JSON:API style pagination only (avoid redundant param combos)
            "page[number]": int(page),
            "page[size]": int(per_page),
        }

        # retry logic for 429 / 5xx
        for attempt in range(1, 9):
            r = session.get(url, headers=headers, params=params, timeout=30)

            # ---- 429 handling ----
            if r.status_code == 429:
                cooldowns_used += 1

                if cooldowns_used > max_cooldowns:
                    print(
                        f"🛑 429 rate-limit persists after {max_cooldowns} cooldowns. "
                        f"Stopping pagination early (partial slate) and continuing pipeline."
                    )
                    stop_paging = True
                    break

                retry_after = r.headers.get("Retry-After")
                base = float(cooldown_seconds)
                if retry_after:
                    try:
                        base = max(base, float(retry_after))
                    except Exception:
                        pass

                sleep_s = base + random.uniform(0.0, max(0.0, float(jitter_seconds)))
                print(
                    f"⏸️  429 rate limit hit → cooldown {cooldowns_used}/{max_cooldowns}: "
                    f"sleeping {sleep_s:.1f}s then retrying page {page}..."
                )
                time.sleep(sleep_s)
                continue

            # ---- 5xx backoff ----
            if r.status_code in (500, 502, 503, 504):
                wait = (2.0 ** (attempt - 1)) + 0.5
                print(f"  ⏳ page {page} attempt {attempt} → status {r.status_code}, retrying in {wait:.1f}s")
                time.sleep(wait)
                continue


            # ---- 403 / 401 handling (WAF / forbidden) ----
            if r.status_code in (401, 403):
                # Treat as a soft-block: back off, optionally re-warm and downgrade page size.
                wait = (2.0 ** (attempt - 1)) + random.uniform(1.0, 3.0)
                print(f"  🚫 page {page} attempt {attempt} → status {r.status_code} (forbidden). Backing off {wait:.1f}s...")
                time.sleep(wait)

                # After a couple tries, re-create session + UA (looks like a fresh browser)
                if attempt in (2, 4):
                    try:
                        session.close()
                    except Exception:
                        pass
                    session = requests.Session()
                    ua = random.choice(USER_AGENTS)
                    headers = _make_headers(ua)
                    _warm_session(session, ua)

                # Gradually lower page size (less aggressive)
                if params.get("page[size]", per_page) > 100:
                    params["page[size]"] = max(100, int(params["page[size]"]) - 50)

                continue

            r.raise_for_status()
            break
        else:
            print(f"⛔ Server errors on page {page} after retries. Stopping early.")
            break

        if stop_paging:
            break

        j = r.json()
        raw_pages.append(j)

        data = j.get("data") or []
        included = j.get("included") or []

        if not data:
            break

        # Detect repeated pages (pagination not advancing)
        page_ids = [str(d.get("id", "")).strip() for d in data if isinstance(d, dict) and d.get("id")]
        new_ids = [pid for pid in page_ids if pid not in seen_projection_ids]
        for pid in new_ids:
            seen_projection_ids.add(pid)

        if len(new_ids) == 0:
            no_new_pages_in_a_row += 1
            print(f"⚠️ page {page}: 0 new projection_ids (repeat page). Streak={no_new_pages_in_a_row}")
        else:
            no_new_pages_in_a_row = 0
            print(f"  ✓ Page {page}: +{len(new_ids)} new projections (unique total {len(seen_projection_ids)})")

        if page >= 2 and no_new_pages_in_a_row >= 1:
            print("🛑 Stopping pagination: detected repeat pages.")
            break

        all_data.extend(data)
        all_included.extend(included)

        if sleep:
            # Add slight jitter to inter-page sleep so requests aren't perfectly metronomic
            jitter = random.uniform(-0.3, 0.5)
            time.sleep(max(0.5, float(sleep) + jitter))

    session.close()
    return all_data, all_included, raw_pages


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default="step1_fetch_prizepicks_api.csv")
    ap.add_argument("--raw_json", default="")
    ap.add_argument("--history", default="")  # optional: write a copy with timestamp
    ap.add_argument("--min_rows", type=int, default=120)
    ap.add_argument("--min_teams", type=int, default=6)

    # League + paging
    ap.add_argument("--league_id", default="7")          # NBA = 7
    ap.add_argument("--game_mode", default="pickem")     # props live in pickem
    ap.add_argument("--per_page", type=int, default=150)
    ap.add_argument("--max_pages", type=int, default=80)
    ap.add_argument("--sleep", type=float, default=1.2)

    # 429 cooldown controls
    ap.add_argument("--cooldown_seconds", type=float, default=60.0,
                    help="Cooldown sleep on HTTP 429 before retrying the same page.")
    ap.add_argument("--max_cooldowns", type=int, default=2,
                    help="Max number of 429 cooldown cycles before stopping pagination early.")
    ap.add_argument("--jitter_seconds", type=float, default=7.0,
                    help="Random jitter added to cooldown sleep (0..jitter_seconds).")

    # Back-compat: allow overriding url, but still default to API_URL
    ap.add_argument("--url", default="")

    args = ap.parse_args()

    url_used = args.url.strip() or API_URL

    print(f"📡 Fetching PrizePicks | league_id={args.league_id} | game_mode={args.game_mode} | per_page={args.per_page}")
    if url_used != API_URL:
        print(f"→ using custom url: {url_used}")

    # If custom URL is used, do a single fetch without pagination (safety).
    if url_used != API_URL:
        r = requests.get(url_used, headers=headers, timeout=30)
        r.raise_for_status()
        j = r.json()
        data = j.get("data") or []
        included = j.get("included") or []
        raw_pages = [j]
    else:
        data, included, raw_pages = fetch_pages(
            url=url_used,
            league_id=str(args.league_id),
            game_mode=str(args.game_mode),
            per_page=int(args.per_page),
            max_pages=int(args.max_pages),
            sleep=float(args.sleep),
            cooldown_seconds=float(args.cooldown_seconds),
            max_cooldowns=int(args.max_cooldowns),
            jitter_seconds=float(args.jitter_seconds),
        )

    if args.raw_json:
        try:
            with open(args.raw_json, "w", encoding="utf-8") as f:
                json.dump(raw_pages[-1] if raw_pages else {}, f, ensure_ascii=False)
            print("🧾 raw_json saved →", args.raw_json)
        except Exception as e:
            print("⚠️ raw_json write failed:", e)

    if not data:
        cols = [
            "projection_id", "pp_projection_id",
            "player_id",
            "pp_game_id", "start_time",
            "player", "pos", "team", "opp_team", "pp_home_team", "pp_away_team",
            "prop_type", "line", "pick_type",
        ]
        pd.DataFrame(columns=cols).to_csv(args.output, index=False)
        print("❌ No projections fetched. Wrote empty CSV →", args.output)
        return

    inc = _included_index(included)

    out_rows: List[dict] = []

    for d in data:
        if not isinstance(d, dict):
            continue

        pid = str(d.get("id", "")).strip()
        attrs = d.get("attributes") or {}
        rel = d.get("relationships") or {}

        line = attrs.get("line_score", attrs.get("line"))
        prop_type = str(attrs.get("stat_type", attrs.get("projection_type", attrs.get("name", "")))).strip()
        odds_type = str(attrs.get("odds_type", "")).strip().lower()
        pick_type = PICKTYPE_MAP.get(odds_type, "Standard")

        player_id = _safe_get(rel, ["new_player", "data", "id"], "") or ""
        player_id_str = str(player_id).strip()
        player_type = _safe_get(rel, ["new_player", "data", "type"], "new_player")
        game_id = _safe_get(rel, ["new_game", "data", "id"], "") or _safe_get(rel, ["game", "data", "id"], "")
        game_type = _safe_get(rel, ["new_game", "data", "type"], "") or _safe_get(rel, ["game", "data", "type"], "")

        player_obj = inc.get((player_type, str(player_id))) if player_id else None
        game_obj = inc.get((game_type, str(game_id))) if game_id and game_type else None

        player_name = ""
        pos = ""
        team = ""
        if isinstance(player_obj, dict):
            pattrs = player_obj.get("attributes") or {}
            player_name = str(pattrs.get("display_name", pattrs.get("name", ""))).strip()
            pos = str(pattrs.get("position", "")).strip()
            team = _norm_team(pattrs.get("team", ""))

        home = away = start_time = ""
        if isinstance(game_obj, dict):
            gattrs = game_obj.get("attributes") or {}
            home = _norm_team(gattrs.get("home_team", ""))
            away = _norm_team(gattrs.get("away_team", ""))
            start_time = _parse_iso(str(gattrs.get("start_time", "")))

        if not start_time:
            start_time = _parse_iso(str(attrs.get("start_time", "")))

        opp_team = ""
        if team and home and away:
            opp_team = away if team == home else (home if team == away else "")
        else:
            desc = str(attrs.get("description", "") or "")
            m = re.search(r"\bvs\.?\s+([A-Za-z]{2,4})\b", desc)
            if m:
                opp_team = _norm_team(m.group(1))

        out_rows.append({
            "projection_id": pid,
            "pp_projection_id": pid,
            "player_id": player_id_str,
            "pp_game_id": str(game_id or "").strip(),
            "start_time": start_time,
            "player": player_name,
            "pos": pos,
            "team": team,
            "opp_team": opp_team,
            "pp_home_team": home,
            "pp_away_team": away,
            "prop_type": prop_type,
            "line": line,
            "pick_type": pick_type,
        })

    df = pd.DataFrame(out_rows).fillna("")

    # enforce numeric line where possible
    df["line"] = pd.to_numeric(df["line"], errors="coerce")

    # NEW: hard dedupe seatbelt (prevents explosions if API repeats)
    before = len(df)
    if "projection_id" in df.columns:
        df = df.drop_duplicates(subset=["projection_id"], keep="first").reset_index(drop=True)
    after = len(df)
    if before != after:
        print(f"✅ Step1 dedupe applied: {before} -> {after} rows (unique projection_id={df['projection_id'].nunique()})")

    rows = len(df)
    teams = len({t for t in df["team"].astype(str).tolist() if t})

    df.to_csv(args.output, index=False)

    print("Step1 saved →", args.output)
    print("  fetch_method: requests_ok")
    print(f"  rows={rows} teams={teams}")

    if args.history:
        try:
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_utc")
            hist_path = args.history.replace("{ts}", ts)
            df.to_csv(hist_path, index=False)
            print("🕘 history saved →", hist_path)
        except Exception as e:
            print("⚠️ history write failed:", e)

    if rows < args.min_rows or teams < args.min_teams:
        print(f"⛔ BOARD_TOO_SMALL (min_rows={args.min_rows}, min_teams={args.min_teams})")
    else:
        print("✅ BOARD_OK")


if __name__ == "__main__":
    main()