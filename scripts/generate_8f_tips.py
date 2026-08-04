#!/usr/bin/env python3
"""Daily 8F Analyst tip generation — writes source='8F' into TRS tables.

Replaces the external Gemini/iReel generation legs (strip directive,
2026-08-04). No AI API, no third-party call: reads the morning PF
payloads from OUR archive (pf_ireel_races, shared Postgres), scores
with the fitted deterministic cascade, writes tip_runs/tips into the
TRS database. Consumers see nothing until GEMINI_ALIAS_SOURCE flips —
so this can shadow-run for days pre-flip.

CONSISTENCY RULE: generates exactly the configuration Phase 3
validated (no speedmap join, component-fallback Value pick). Upgrades
(live market-aware Value, speedmap map component) require their own
evidence pass first.

Usage:
  SHARED_DATABASE_URL=... TRS_DATABASE_URL=... \
      python scripts/generate_8f_tips.py [--date YYYY-MM-DD] [--commit]

Dry-run (default) prints tips; --commit upserts (idempotent: re-running
the same morning updates in place).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg2
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.analyst import score_race, select_tips, reasoning_line  # noqa: E402

SOURCE = "Jennifer"
MODEL_VERSION = "jennifer-v1.0-fitted-20260804"
SCRATCHINGS_URL = os.environ.get(
    "SCRATCHINGS_URL", "https://pf-scratchings-conditions.onrender.com")


def fetch_scratchings(day: str) -> dict:
    """{track_lower: {(race_no, tab_no), ...}} — same proven service the
    curate path uses (pf-scratchings-conditions). Makes the 08:00 re-run
    scratching-aware: the PF payloads are static after the 04:00 harvest,
    so without this a late scratching would silently stay tipped
    (the 2026-07-24 Neeson lesson). Fail-open with a loud log."""
    out: dict = {}
    try:
        with urllib.request.urlopen(
                f"{SCRATCHINGS_URL}/scratchings/flat?date={day}",
                timeout=20) as r:
            data = json.load(r)
        for row in (data or {}).get("rows", []):
            tr = (row.get("track") or "").lower().strip()
            rn, tn = row.get("raceNo"), row.get("tabNo")
            if tr and rn and tn:
                out.setdefault(tr, set()).add((int(rn), int(tn)))
    except Exception as e:
        print(f"[8f-gen] SCRATCHINGS FETCH FAILED ({e}) — generating "
              f"WITHOUT the scratchings sweep", flush=True)
    return out


def load_races(scur, day: str, live: bool):
    """Latest snapshot per race — pre-race-morning cutoff for retro days,
    plain latest for live (it IS the morning)."""
    if live:
        scur.execute("""
            SELECT DISTINCT ON (meeting_id, race_number)
                   meeting_id, race_number, payload
            FROM pf_ireel_races WHERE meeting_date::date = %s
            ORDER BY meeting_id, race_number, snapshot_ts DESC""", (day,))
    else:
        scur.execute("""
            SELECT DISTINCT ON (meeting_id, race_number)
                   meeting_id, race_number, payload
            FROM pf_ireel_races WHERE meeting_date::date = %s
              AND snapshot_ts::timestamptz <=
                  ((%s::date - interval '1 day') + interval '23 hours') AT TIME ZONE 'UTC'
            ORDER BY meeting_id, race_number, snapshot_ts DESC""", (day, day))
    return scur.fetchall()


def upsert_meeting(tcur, day, pl, pf_meeting_id=None):
    meeting = (pl.get("meeting") or {})
    track = (meeting.get("track") or {})
    name = track.get("name") or "Unknown"
    state = track.get("state") or "?"
    country = track.get("country") or "AUS"
    tcur.execute("""
        INSERT INTO meetings (id, date, track_name, state, country,
                              pf_meeting_id, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, now(), now())
        ON CONFLICT (date, track_name, state) DO UPDATE
            SET pf_meeting_id = COALESCE(meetings.pf_meeting_id,
                                         EXCLUDED.pf_meeting_id),
                updated_at = now()
        RETURNING id""",
        (str(uuid.uuid4()), day, name, state, country, pf_meeting_id))
    return tcur.fetchone()[0], name


def upsert_race(tcur, meeting_uuid, rn, pl):
    tcur.execute("""
        INSERT INTO races (id, meeting_id, race_number, name, distance_m,
                           class_text, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (meeting_id, race_number) DO UPDATE
            SET name = COALESCE(races.name, EXCLUDED.name)
        RETURNING id""",
        (str(uuid.uuid4()), meeting_uuid, rn, pl.get("name"),
         pl.get("distance"), pl.get("raceClass")))
    return tcur.fetchone()[0]


def get_or_create_run(tcur, meeting_uuid, pf_meeting_id):
    tcur.execute("""SELECT id FROM tip_runs
        WHERE source = %s AND meeting_id = %s""", (SOURCE, meeting_uuid))
    row = tcur.fetchone()
    if row:
        return row[0]
    tcur.execute("""
        INSERT INTO tip_runs (id, source, model_version, meeting_id, meta,
                              created_at)
        VALUES (%s, %s, %s, %s, %s, now()) RETURNING id""",
        (str(uuid.uuid4()), SOURCE, MODEL_VERSION, meeting_uuid,
         json.dumps({"generator": "deterministic-cascade",
                     "pf_meeting_id": pf_meeting_id})))
    return tcur.fetchone()[0]


def upsert_tip(tcur, run_id, race_uuid, tip_type, entry, reason):
    tcur.execute("""
        INSERT INTO tips (id, tip_run_id, race_id, tip_type, tab_number,
                          horse_name, reasoning, stake_units, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, 1.0, now())
        ON CONFLICT (tip_run_id, race_id, tip_type) DO UPDATE
            SET tab_number = EXCLUDED.tab_number,
                horse_name = EXCLUDED.horse_name,
                reasoning = EXCLUDED.reasoning""",
        (str(uuid.uuid4()), run_id, race_uuid, tip_type,
         entry["tab_number"], entry["horse_name"], reason))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date")
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    mel_today = datetime.now(ZoneInfo("Australia/Melbourne")).date().isoformat()
    day = args.date or mel_today
    live = day >= mel_today

    shared = psycopg2.connect(os.environ["SHARED_DATABASE_URL"], connect_timeout=20)
    races = load_races(shared.cursor(), day, live)
    print(f"[8f-gen] {day} ({'live' if live else 'retro'}): "
          f"{len(races)} race payloads")
    if not races:
        print("[8f-gen] nothing to generate")
        return 0

    trs = None
    tcur = None
    if args.commit:
        trs = psycopg2.connect(
            os.environ.get("TRS_DATABASE_URL") or os.environ["DATABASE_URL"],
            connect_timeout=20)  # on the TRS box, DATABASE_URL IS the TRS DB
        tcur = trs.cursor()

    scr = fetch_scratchings(day)
    if scr:
        print(f"[8f-gen] scratchings loaded for {len(scr)} tracks "
              f"({sum(len(v) for v in scr.values())} runners)")

    n_tips = 0
    for mid, rn, payload in races:
        p = json.loads(payload) if isinstance(payload, str) else payload
        pl = (p or {}).get("payLoad") or {}
        track_l = (((pl.get("meeting") or {}).get("track") or {})
                   .get("name") or "").lower().strip()
        scr_tabs = {tab for (r_, tab) in scr.get(track_l, set()) if r_ == rn}
        scored = score_race(p, scratched=scr_tabs)
        if len(scored) < 4:
            continue
        tips = select_tips(scored)
        cond = pl.get("trackCondition") or ""
        field = len(scored)
        track = ((pl.get("meeting") or {}).get("track") or {}).get("name") or "?"
        lines = {}
        for tt, key, angle in (("AI_BEST", "ai_best", "best"),
                               ("DANGER", "danger", "danger"),
                               ("VALUE", "value", "value")):
            e = tips.get(key)
            if not e:
                continue
            lines[tt] = (e, reasoning_line(e, cond, field, angle=angle))
        if not args.commit:
            print(f"\n{track} R{rn}:")
            for tt, (e, reason) in lines.items():
                print(f"  {tt}: #{e['tab_number']} {e['horse_name']} — {reason}")
        else:
            m_uuid, _ = upsert_meeting(tcur, day, pl, pf_meeting_id=mid)
            r_uuid = upsert_race(tcur, m_uuid, rn, pl)
            run_id = get_or_create_run(tcur, m_uuid, mid)
            for tt, (e, reason) in lines.items():
                upsert_tip(tcur, run_id, r_uuid, tt, e, reason)
                n_tips += 1
    if args.commit:
        trs.commit()
        trs.close()
        print(f"[8f-gen] committed {n_tips} tips as source='{SOURCE}' "
              f"({MODEL_VERSION})")
    shared.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
