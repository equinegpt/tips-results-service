#!/usr/bin/env python3
"""Phase 3: 8F Analyst retro backtest over the full pf_ireel_races archive.

Generates AI Best / Danger / Value for every archived race using ONLY
the latest pre-race-morning snapshot (lookahead rule), settles at SP
from ra_results, and compares against Gemini's and iReel's RECORDED
tips on the same races (TRS tip_runs/tips/tip_outcomes).

Usage:
  SHARED_DATABASE_URL=... TRS_DATABASE_URL=... \
      python scripts/analyst_backtest.py [start] [end]

Outputs:
  output/analyst_backtest_tips.csv   — every generated tip, settled
  stdout summary — overall / monthly / tune-vs-holdout / head-to-head

Discipline: any tuning happens against Feb-May ("TUNE"); Jun-Aug
("HOLDOUT") stays untouched until the final read.
"""
from __future__ import annotations

import csv
import json
import os
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.analyst import score_race, select_tips  # noqa: E402

TUNE_END = date(2026, 5, 31)          # <= this: TUNE; after: HOLDOUT

# RA results carry sponsor-prefixed track names; PF payloads don't.
SPONSORS = ("sportsbet", "ladbrokes", "bet365", "tabtouch", "picklebet",
            "aquis", "thomasfarms", "southside", "royal", "hygain", "tab")


# PF name -> RA name where they genuinely differ (validation week found
# Fannie Bay = Darwin's track name on the PF side; RA says Darwin).
ALIASES = {"fanniebay": "darwin"}


def norm(t: str) -> str:
    s = "".join(c for c in (t or "").lower() if c.isalnum())
    for sp in SPONSORS:
        if s.startswith(sp):
            s = s[len(sp):]
    return ALIASES.get(s, s)


def track_match(pf: str, ra_tracks: dict) -> str | None:
    """Match a PF track name to an RA results track (normalised map)."""
    n = norm(pf)
    if n in ra_tracks:
        return n
    for cand in ra_tracks:
        if n and cand and (n in cand or cand in n):
            return cand
    return None


def main() -> int:
    start = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date(2026, 1, 22)
    end = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else date(2026, 8, 3)
    shared = psycopg2.connect(os.environ["SHARED_DATABASE_URL"], connect_timeout=20)
    scur = shared.cursor()

    out_dir = Path(__file__).resolve().parent.parent / "output"
    out_dir.mkdir(exist_ok=True)
    csv_path = out_dir / "analyst_backtest_tips.csv"
    fout = open(csv_path, "w", newline="")
    w = csv.writer(fout)
    w.writerow(["date", "pf_meeting_id", "track", "race", "tip_type",
                "tab", "horse", "fin", "sp", "void"])

    unmatched_tracks = defaultdict(int)
    day = start
    n_days = n_races = 0
    while day <= end:
        iso = day.isoformat()
        scur.execute("""
            SELECT DISTINCT ON (meeting_id, race_number)
                   meeting_id, race_number, payload
            FROM pf_ireel_races
            WHERE meeting_date::date = %s
              AND snapshot_ts::timestamptz <=
                  ((%s::date - interval '1 day') + interval '23 hours') AT TIME ZONE 'UTC'
            ORDER BY meeting_id, race_number, snapshot_ts DESC""", (iso, iso))
        races = scur.fetchall()
        if races:
            scur.execute("""SELECT track, race_no, horse_number, finishing_pos,
                starting_price, is_scratched FROM ra_results
                WHERE meeting_date::date = %s""", (iso,))
            ra = defaultdict(dict)
            for tr, rn, tab, fin, sp, scr in scur.fetchall():
                ra[norm(tr)][(rn, tab)] = (fin, float(sp or 0), scr)
            for mid, rn, payload in races:
                p = json.loads(payload) if isinstance(payload, str) else payload
                pl = (p or {}).get("payLoad") or {}
                track = ((pl.get("meeting") or {}).get("track") or {}).get("name") or ""
                key = track_match(track, ra)
                if key is None:
                    unmatched_tracks[track] += 1
                    continue
                # pre-declared scratchings visible in results (is_scratched)
                scr_tabs = {tab for (r_, tab), (f, s, scr) in ra[key].items()
                            if r_ == rn and scr}
                scored = score_race(p, scratched=scr_tabs)
                if len(scored) < 4:
                    continue
                tips = select_tips(scored)
                n_races += 1
                for tip_type, entry in (("AI_BEST", tips.get("ai_best")),
                                        ("DANGER", tips.get("danger")),
                                        ("VALUE", tips.get("value"))):
                    if not entry:
                        continue
                    res = ra[key].get((rn, entry["tab_number"]))
                    void = res is None or res[2] or res[0] is None or res[1] <= 0
                    w.writerow([iso, mid, track, rn, tip_type,
                                entry["tab_number"], entry["horse_name"],
                                res[0] if res else "", res[1] if res else "",
                                int(void)])
            n_days += 1
        if n_days and n_days % 14 == 0 and day.day in (1, 15):
            print(f"[bt] ...{iso}: {n_races} races so far", flush=True)
        day += timedelta(days=1)
    fout.close()
    shared.close()
    print(f"[bt] generated tips for {n_races} races across {n_days} days -> {csv_path}")
    if unmatched_tracks:
        top = sorted(unmatched_tracks.items(), key=lambda kv: -kv[1])[:10]
        print(f"[bt] UNMATCHED tracks (races skipped): {sum(unmatched_tracks.values())} — top: {top}")

    # ---------------- summaries ----------------
    rows = list(csv.DictReader(open(csv_path)))

    def summarize(rs, label):
        by = defaultdict(lambda: [0, 0, 0.0, 0])   # type -> n, wins, ret, void
        for r in rs:
            t = by[r["tip_type"]]
            if r["void"] == "1":
                t[3] += 1
                continue
            t[0] += 1
            if r["fin"] == "1":
                t[1] += 1
                t[2] += float(r["sp"])
        print(f"\n== 8F Analyst — {label} ==")
        for tt in ("AI_BEST", "DANGER", "VALUE"):
            n, wins, ret, void = by[tt]
            if n:
                print(f"  {tt:8s}: n={n:5d} strike={100*wins/n:5.1f}% "
                      f"ROI@SP={100*(ret-n)/n:+6.1f}%  (void {void})")

    summarize(rows, f"ALL ({start} → {end})")
    summarize([r for r in rows if r["date"] <= TUNE_END.isoformat()], "TUNE (≤ May 31)")
    summarize([r for r in rows if r["date"] > TUNE_END.isoformat()], "HOLDOUT (Jun 1 →)")

    monthly = defaultdict(lambda: [0, 0, 0.0])
    for r in rows:
        if r["tip_type"] == "AI_BEST" and r["void"] != "1":
            m = monthly[r["date"][:7]]
            m[0] += 1
            if r["fin"] == "1":
                m[1] += 1
                m[2] += float(r["sp"])
    print("\n== AI_BEST by month ==")
    for mo in sorted(monthly):
        n, wins, ret = monthly[mo]
        print(f"  {mo}: n={n:4d} strike={100*wins/n:5.1f}% ROI@SP={100*(ret-n)/n:+6.1f}%")

    # ---------------- benchmark: recorded Gemini/iReel ----------------
    trs_url = os.environ.get("TRS_DATABASE_URL")
    if not trs_url:
        print("\n(no TRS_DATABASE_URL — benchmark comparison skipped)")
        return 0
    trs = psycopg2.connect(trs_url, connect_timeout=20)
    tcur = trs.cursor()
    tcur.execute("""
        SELECT tr.source, t.tip_type, m.date, m.pf_meeting_id, r.race_number,
               t.tab_number, o.outcome_status, o.finish_position, o.starting_price
        FROM tips t
        JOIN tip_runs tr ON tr.id = t.tip_run_id
        JOIN races r ON r.id = t.race_id
        JOIN meetings m ON m.id = tr.meeting_id
        LEFT JOIN tip_outcomes o ON o.tip_id = t.id
        WHERE m.date BETWEEN %s AND %s""", (start, end))
    bench = tcur.fetchall()
    trs.close()

    bsum = defaultdict(lambda: [0, 0, 0.0, 0])
    bench_best = {}       # (pf_meeting_id, race) -> (source, tab, won, sp)
    for source, tt, d, pfm, rn, tab, status, fin, sp in bench:
        k = f"{source}/{tt}"
        t = bsum[k]
        if status in ("SCRATCHED", "NO_RESULT", "UNKNOWN", None) or not sp:
            t[3] += 1
            continue
        t[0] += 1
        won = (fin == 1)
        if won:
            t[1] += 1
            t[2] += float(sp)
        if tt == "AI_BEST":
            bench_best[(source, pfm, rn)] = (tab, won, float(sp))
    print("\n== Recorded benchmark (same window) ==")
    for k in sorted(bsum):
        n, wins, ret, void = bsum[k]
        if n:
            print(f"  {k:16s}: n={n:5d} strike={100*wins/n:5.1f}% "
                  f"ROI@SP={100*(ret-n)/n:+6.1f}%  (void {void})")

    # head-to-head on the intersection (same races, AI_BEST only)
    ours = {}
    for r in rows:
        if r["tip_type"] == "AI_BEST" and r["void"] != "1":
            ours[(int(r["pf_meeting_id"]), int(r["race"]))] = (
                int(r["tab"]), r["fin"] == "1", float(r["sp"]))
    for source in ("Gemini", "iReel"):
        inter = [(k, v) for (s, pfm, rn), v in bench_best.items()
                 if s == source and (pfm, rn) in ours
                 for k in [(pfm, rn)]]
        if not inter:
            continue
        agree = us_w = them_w = 0
        us_ret = them_ret = 0.0
        for k, (btab, bwon, bsp) in inter:
            otab, owon, osp = ours[k]
            if otab == btab:
                agree += 1
            us_w += owon
            them_w += bwon
            us_ret += osp if owon else 0
            them_ret += bsp if bwon else 0
        n = len(inter)
        print(f"\n== Head-to-head vs {source} (same {n} races, AI_BEST) ==")
        print(f"  agreement: {100*agree/n:.1f}%")
        print(f"  8F:      strike={100*us_w/n:5.1f}% ROI@SP={100*(us_ret-n)/n:+6.1f}%")
        print(f"  {source:7s}: strike={100*them_w/n:5.1f}% ROI@SP={100*(them_ret-n)/n:+6.1f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
