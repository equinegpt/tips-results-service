#!/usr/bin/env python3
"""Lifeboat Phase D — live card adapter (Racenet → lifeboat_cards).

In a PF-loss world Jennifer still needs TODAY's card: runners, tabs,
barriers, jockeys, trainers, weights, scratchings, race metadata. The
Racenet form-guide pages carry all of it server-side rendered, and the
odds sweep already proved the residential (Scrape.do super) path works
when Racenet's Kasada challenge blocks datacenter IPs.

This script fetches the day's AU race pages, parses the cards, matches
each horse to a racing-db horse_id by canonical name (latest-activity
disambiguation; unmatched = first starter or name gap → NULL, which
the payload builder treats as no-form), and upserts `lifeboat_cards`
in racing-db. Jockey/trainer names come from Racenet PROFILE SLUGS
(declan-bates → "declan bates"), which are full names — the same
canonical key lifeboat_connection_a2e uses. The visible "J: D Bates"
text is abbreviated and would never match.

Parking rule: this is drill/break-glass machinery. Nothing downstream
consumes lifeboat_cards unless LIFEBOAT=1 is set by a human.

Usage:
  RACING_DB_URL=... SCRAPER_API_KEY=... [SCRAPER_PROVIDER=scrapedo] \
      python scripts/lifeboat/lifeboat_card_adapter.py [--date YYYY-MM-DD] \
      [--max-races N] [--include-synthetic]
"""
from __future__ import annotations

import html as htmllib
import os
import re
import sys
import time
import urllib.parse
from datetime import date, datetime, timezone

import psycopg2
import psycopg2.extras
import requests

BASE = "https://www.racenet.com.au"
INDEX = f"{BASE}/form-guide"
# same foreign-track filter family as the odds sweep
_FOREIGN = re.compile(
    r"(-nz|-uk|-gb|-fr|-jpn|-jp|-hk|-sgp|-sg|-usa|-us|-ca|-ire|-saf|-za|"
    r"-tr|-kor|-mac|-uae|-in|-de|-ita|-swe|-chi|-arg|-bra)$")


# --------------------------------------------------------------- fetch
def scraper_get(url: str, timeout: int = 90, super_: bool = True):
    """Scrape.do GET. Defaults straight to residential (super) — the
    2026-08-01 incident showed datacenter fetches bounce off Kasada,
    and a failed cheap attempt still costs a credit + latency."""
    key = os.environ["SCRAPER_API_KEY"]
    params = {"token": key, "url": url}
    if super_:
        params["super"] = "true"
    return requests.get("https://api.scrape.do/",
                        params=params, timeout=timeout)


def discover_race_paths(day: date) -> list[str]:
    resp = scraper_get(INDEX, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"form-guide index -> HTTP {resp.status_code}")
    ymd = day.strftime("%Y%m%d")
    paths = sorted({m.group(0) for m in re.finditer(
        r"/form-guide/horse-racing/[a-z0-9\-]+-" + ymd +
        r"/[a-z0-9\-]*race-\d+[a-z0-9\-/]*", resp.text)})
    keep = []
    for p in paths:
        track_slug = p.split("/")[3][: -(len(ymd) + 1)]
        if not _FOREIGN.search(track_slug):
            keep.append(p)
    return keep


# --------------------------------------------------------------- parse
def _slug_to_name(slug: str) -> str:
    # profile slugs are full names; strip trailing disambiguation digits
    # (fred-kersley-1 → "Fred Kersley"). Title case so reasoning lines
    # read naturally; the A2E canonical key lowercases anyway.
    parts = [p for p in slug.split("-") if not p.isdigit()]
    return " ".join(parts).title()


def parse_race_meta(path: str, html: str) -> dict:
    """Race metadata. Primary source = the ld+json event description
    ("Track Condition: Synthetic, Race number: 3, ... Race Distance:
    1000m"); class from the visible title line."""
    meta = {"race_no": None, "distance": None, "track_condition": None,
            "race_class": None, "start_time": None}
    m = re.search(r"race-(\d+)", path)
    if m:
        meta["race_no"] = int(m.group(1))
    d = re.search(r'"description":"Track Condition:\s*([^,"]+),\s*'
                  r'Race number:\s*(\d+)[^"]*?Race Distance:\s*(\d+)m', html)
    if d:
        meta["track_condition"] = d.group(1).strip()
        meta["race_no"] = int(d.group(2))
        meta["distance"] = int(d.group(3))
    s = re.search(r'"startDate":"([^"]+)"', html)
    if s:
        meta["start_time"] = s.group(1)
    # visible title line: "R3 Global Turf Maiden Plate 1000m Class: Maiden,
    # Set Weights" — require a capitalised class token so minified JS
    # ("lH,video") can't match
    c = re.search(r"\d{3,4}m[^<]{0,40}Class:\s*([A-Z][A-Za-z0-9 ,&+\-]{1,50})",
                  re.sub(r"<[^>]+>", " ", html))
    if c:
        meta["race_class"] = " ".join(c.group(1).split()).rstrip(",")
    return meta


def parse_runners(html: str) -> list[dict]:
    """One dict per runner: tab, horse, barrier, weight, jockey,
    trainer, scratched. Containers carry class selection-scratched for
    scratchings; names/slugs live in the details rows."""
    out = {}
    blocks = re.split(r'(?=<div[^>]*class="event-selection-row-container)', html)
    for blk in blocks[1:]:
        scratched = "selection-scratched" in blk[:400]
        nm = re.search(
            r'horseracing-selection-details-name(?:[^"]*)">\s*(\d+)\.\s*([^<]+?)\s*<', blk)
        if not nm:
            continue
        tab = int(nm.group(1))
        if tab in out:
            continue
        horse = htmllib.unescape(nm.group(2)).strip()
        bar = re.search(r"<span>\((\d+)\)</span>", blk)
        tr = re.search(r"/profiles/trainer/([a-z0-9\-]+)", blk)
        jk = re.search(r"/profiles/jockey/([a-z0-9\-]+)", blk)
        wt = re.search(r"\((\d{2}(?:\.\d)?)kg\)", blk)
        out[tab] = {
            "tab": tab, "horse": horse, "scratched": scratched,
            "barrier": int(bar.group(1)) if bar else None,
            "weight": float(wt.group(1)) if wt else None,
            "jockey": _slug_to_name(jk.group(1)) if jk else None,
            "trainer": _slug_to_name(tr.group(1)) if tr else None,
        }
    return [out[k] for k in sorted(out)]


def track_from_path(path: str, ymd: str) -> str:
    slug = path.split("/")[3][: -(len(ymd) + 1)]
    return " ".join(w.capitalize() for w in slug.split("-"))


# ------------------------------------------------------ horse matching
def canon_horse(name: str) -> str:
    s = re.sub(r"\(.*?\)", "", name or "").upper()
    s = re.sub(r"[^A-Z0-9 ]", "", s)
    return " ".join(s.split())


def build_horse_index(cur) -> dict:
    """canonical name → horse_id, latest-activity wins on collisions."""
    cur.execute("""
        SELECT h.horse_id, h.name, max(r.race_date) AS last_run
        FROM horses h
        LEFT JOIN race_results rr ON rr.horse_id = h.horse_id
        LEFT JOIN races r ON r.race_id = rr.race_id
        GROUP BY h.horse_id, h.name""")
    idx = {}
    for hid, name, last_run in cur.fetchall():
        key = canon_horse(name)
        if not key:
            continue
        prev = idx.get(key)
        if prev is None or (last_run or date.min) > (prev[1] or date.min):
            idx[key] = (hid, last_run)
    return {k: v[0] for k, v in idx.items()}


# ---------------------------------------------------------------- main
def ensure_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lifeboat_cards (
            card_date DATE NOT NULL,
            track TEXT NOT NULL,
            race_no INT NOT NULL,
            tab_no INT NOT NULL,
            horse TEXT NOT NULL,
            horse_id INT,
            barrier INT,
            weight REAL,
            jockey TEXT,
            trainer TEXT,
            scratched BOOLEAN NOT NULL DEFAULT FALSE,
            distance INT,
            race_class TEXT,
            track_condition TEXT,
            start_time TEXT,
            fetched_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (card_date, track, race_no, tab_no))""")


def main() -> int:
    day = date.today()
    if "--date" in sys.argv:
        day = date.fromisoformat(sys.argv[sys.argv.index("--date") + 1])
    max_races = None
    if "--max-races" in sys.argv:
        max_races = int(sys.argv[sys.argv.index("--max-races") + 1])

    url = os.environ.get("RACING_DB_URL") or os.environ["DATABASE_URL"]
    conn = psycopg2.connect(url, connect_timeout=30)
    cur = conn.cursor()
    ensure_table(cur)
    conn.commit()

    print(f"[cards] building horse index…", flush=True)
    hidx = build_horse_index(cur)
    print(f"[cards] {len(hidx)} canonical horse names", flush=True)

    paths = discover_race_paths(day)
    if max_races:
        paths = paths[:max_races]
    print(f"[cards] {len(paths)} AU race pages for {day}", flush=True)

    ymd = day.strftime("%Y%m%d")
    now = datetime.now(timezone.utc)
    total = matched = races_ok = 0
    for p in paths:
        try:
            resp = scraper_get(BASE + p, timeout=120)
            if resp.status_code != 200:
                print(f"[cards] {p} -> HTTP {resp.status_code}, skip", flush=True)
                continue
            meta = parse_race_meta(p, resp.text)
            runners = parse_runners(resp.text)
        except Exception as exc:
            print(f"[cards] {p} -> {exc}, skip", flush=True)
            continue
        if not runners or not meta["race_no"]:
            print(f"[cards] {p} -> no runners parsed, skip", flush=True)
            continue
        track = track_from_path(p, ymd)
        rows = []
        for r in runners:
            hid = hidx.get(canon_horse(r["horse"]))
            total += 1
            matched += 1 if hid else 0
            rows.append((day, track, meta["race_no"], r["tab"], r["horse"],
                         hid, r["barrier"], r["weight"], r["jockey"],
                         r["trainer"], r["scratched"], meta["distance"],
                         meta["race_class"], meta["track_condition"],
                         meta["start_time"], now))
        psycopg2.extras.execute_values(cur, """
            INSERT INTO lifeboat_cards
                (card_date, track, race_no, tab_no, horse, horse_id, barrier,
                 weight, jockey, trainer, scratched, distance, race_class,
                 track_condition, start_time, fetched_at)
            VALUES %s
            ON CONFLICT (card_date, track, race_no, tab_no) DO UPDATE SET
                horse = EXCLUDED.horse, horse_id = EXCLUDED.horse_id,
                barrier = EXCLUDED.barrier, weight = EXCLUDED.weight,
                jockey = EXCLUDED.jockey, trainer = EXCLUDED.trainer,
                scratched = EXCLUDED.scratched, distance = EXCLUDED.distance,
                race_class = EXCLUDED.race_class,
                track_condition = EXCLUDED.track_condition,
                start_time = EXCLUDED.start_time,
                fetched_at = EXCLUDED.fetched_at""", rows)
        conn.commit()
        races_ok += 1
        print(f"[cards] {track} R{meta['race_no']} "
              f"({meta['distance']}m {meta['track_condition']}): "
              f"{len(rows)} runners, "
              f"{sum(1 for x in rows if x[5])} matched, "
              f"{sum(1 for x in rows if x[10])} scratched", flush=True)
        time.sleep(1.0)   # be a polite residential guest

    pct = 100.0 * matched / total if total else 0.0
    print(f"[cards] DONE {races_ok}/{len(paths)} races, "
          f"{matched}/{total} horses matched ({pct:.1f}%)", flush=True)
    conn.close()
    # match-rate is the drill's canary: healthy days run >90%
    return 0 if races_ok and pct >= 70 else 1


if __name__ == "__main__":
    sys.exit(main())
