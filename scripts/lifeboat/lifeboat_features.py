#!/usr/bin/env python3
"""Lifeboat Phase B.3 — payload synthesizer: PF-shaped race payloads
built 100% from racing-db (own pars, own A2E, own results history).

The contract: emit the same structure `app.analyst.score_race` and
`reasoning_line` consume, so the scorer/generator run UNCHANGED in the
lifeboat world. Field-by-field sourcing:

  sectionalData[]           last 3 runs from race_results; class figures
                            = (time − par)/0.17s-per-length vs
                            lifeboat_sectional_pars (PF sign convention:
                            negative = better than par); inRun synthed
                            from position_800m/position_400m/position
  historicPerformanceData   aggregates over the horse's full history
                            (condition groups from races.track_condition)
  jockeyData/trainerData    lifeboat_connection_a2e PIT snapshot (as-of
                            race month — no hindsight), canonical-name key
  barrier/weight/tab        race_results row (backtest) / card (live)

Backtest card source = the race's actual runners (race_results).
Live card source (Racenet adapter) is Phase D.

Smoke usage (also the two-loaders validation when PF payload exists):
  RACING_DB_URL=... SHARED_DATABASE_URL=... \
      python scripts/lifeboat/lifeboat_features.py 2026-08-05 ballarat 7
"""
from __future__ import annotations

import os
import re
import sys
from datetime import date
from pathlib import Path

import psycopg2
import psycopg2.extras

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

LEN_SECONDS = 0.17          # seconds per length at closing speed

COND_GROUP = lambda tc: ("heavy" if (tc or "").lower().startswith("heavy")
                         else "soft" if (tc or "").lower().startswith("soft")
                         else "synthetic" if (tc or "").lower().startswith("synth")
                         else "good")


def canon(name: str) -> str:
    s = re.sub(r"\(.*?\)", "", name or "").lower().strip()
    for pre in ("ms ", "mrs ", "mr ", "miss ", "dr "):
        if s.startswith(pre):
            s = s[len(pre):]
    return " ".join(s.split())


def class_group(rc: str) -> str:
    rc = rc or ""
    low = rc.lower()
    if "group" in low or "listed" in low or re.match(r"^g[123]", low):
        return "stakes"
    if "maiden" in low or low.startswith("mdn"):
        return "maiden"
    m = re.search(r"(?:bm|benchmark)\s*(\d+)", low)
    if m:
        v = int(m.group(1))
        return "bm_low" if v <= 60 else ("bm_mid" if v <= 74 else "bm_high")
    if low.startswith(("cl", "class")):
        return "class123"
    if "open" in low or "hcp" in low or "handicap" in low:
        return "open"
    return "other"


class LifeboatLoader:
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        self._pars = None

    # ---------------------------------------------------------- pars
    def _load_pars(self):
        self.cur.execute("SELECT * FROM lifeboat_sectional_pars")
        self._pars = {}
        for r in self.cur.fetchall():
            key = (r["level"], r["track_id"], r["dist_bucket"],
                   r["cond_group"], r["class_group"])
            self._pars[key] = (r["par_600"], r["par_200"])

    def par_for(self, track_id, distance, cond_grp, cls_grp):
        if self._pars is None:
            self._load_pars()
        db = int(round((distance or 0) / 200.0) * 200)
        for level, key in (
            ("track_dist_cond_class", (track_id, db, cond_grp, cls_grp)),
            ("track_dist_cond", (track_id, db, cond_grp, None)),
            ("track_dist", (track_id, db, None, None)),
            ("dist_cond_class", (None, db, cond_grp, cls_grp)),
            ("dist_cond", (None, db, cond_grp, None)),
            ("dist", (None, db, None, None)),
        ):
            hit = self._pars.get((level, *key))
            if hit:
                return hit
        return (None, None)

    # ------------------------------------------------------- history
    def recent_runs(self, horse_id, before: date, n=3):
        self.cur.execute("""
            SELECT r.race_date, r.track_id, r.distance, r.track_condition,
                   r.race_class, rr.last_600m, rr.last_200m, rr.margin,
                   rr.position, rr.position_800m, rr.position_400m,
                   rr.odds_opening, rr.odds_closing,
                   (SELECT t.name FROM tracks t WHERE t.track_id = r.track_id) AS track_name
            FROM race_results rr JOIN races r ON r.race_id = rr.race_id
            WHERE rr.horse_id = %s AND r.race_date < %s
              AND COALESCE(rr.is_trial, false) = false
            ORDER BY r.race_date DESC LIMIT %s""", (horse_id, before, n))
        return self.cur.fetchall()

    def perf_data(self, horse_id, before: date, track_id, distance):
        self.cur.execute("""
            SELECT r.track_id, r.distance, r.track_condition, rr.position
            FROM race_results rr JOIN races r ON r.race_id = rr.race_id
            WHERE rr.horse_id = %s AND r.race_date < %s
              AND COALESCE(rr.is_trial, false) = false
              AND rr.position IS NOT NULL""", (horse_id, before))
        rows = self.cur.fetchall()
        def rec(filt):
            sel = [x for x in rows if filt(x)]
            return {"starts": len(sel),
                    "firsts": sum(1 for x in sel if x["position"] == 1),
                    "seconds": sum(1 for x in sel if x["position"] == 2),
                    "thirds": sum(1 for x in sel if x["position"] == 3)}
        h = {
            "careerStarts": len(rows),
            "careerWins": sum(1 for x in rows if x["position"] == 1),
            "winPct": round(100.0 * sum(1 for x in rows if x["position"] == 1)
                            / len(rows), 2) if rows else 0,
            "placePct": round(100.0 * sum(1 for x in rows if x["position"] <= 3)
                              / len(rows), 2) if rows else 0,
            "distanceRecord": rec(lambda x: x["distance"] and distance
                                  and abs(x["distance"] - distance) <= 100),
            "trackRecord": rec(lambda x: x["track_id"] == track_id),
            "trackDistRecord": rec(lambda x: x["track_id"] == track_id
                                   and x["distance"] and distance
                                   and abs(x["distance"] - distance) <= 100),
            "goodRecord": rec(lambda x: COND_GROUP(x["track_condition"]) == "good"),
            "softRecord": rec(lambda x: COND_GROUP(x["track_condition"]) == "soft"),
            "heavyRecord": rec(lambda x: COND_GROUP(x["track_condition"]) == "heavy"),
            "syntheticRecord": rec(lambda x: COND_GROUP(x["track_condition"]) == "synthetic"),
            "firmRecord": {"starts": 0, "firsts": 0, "seconds": 0, "thirds": 0},
        }
        return h

    def a2e(self, entity_type, name, as_of: date):
        self.cur.execute("""
            SELECT sr_100, a2e_100, sr_365, a2e_365 FROM lifeboat_connection_a2e
            WHERE entity_type = %s AND entity_key = %s AND as_of <= %s
            ORDER BY as_of DESC LIMIT 1""", (entity_type, canon(name), as_of))
        return self.cur.fetchone()

    # -------------------------------------------------------- payload
    def build_payload(self, race_id) -> dict:
        self.cur.execute("""
            SELECT r.*, t.name AS track_name
            FROM races r JOIN tracks t ON t.track_id = r.track_id
            WHERE r.race_id = %s""", (race_id,))
        race = self.cur.fetchone()
        self.cur.execute("""
            SELECT rr.*, h.name AS horse_name, h.sex,
                   j.name AS jockey_name,
                   (SELECT tr.name FROM trainers tr WHERE tr.trainer_id = h.trainer_id) AS trainer_name
            FROM race_results rr
            JOIN horses h ON h.horse_id = rr.horse_id
            LEFT JOIN jockeys j ON j.jockey_id = rr.jockey_id
            WHERE rr.race_id = %s""", (race_id,))
        entries = self.cur.fetchall()
        rd = race["race_date"]
        runners = []
        for i, e in enumerate(sorted(entries, key=lambda x: x["barrier"] or 99)):
            sect = []
            for run in self.recent_runs(e["horse_id"], rd):
                cg = COND_GROUP(run["track_condition"])
                kg = class_group(run["race_class"])
                par6, par2 = self.par_for(run["track_id"], run["distance"], cg, kg)
                l6c = (round((float(run["last_600m"]) - par6) / LEN_SECONDS, 2)
                       if run["last_600m"] and par6 else None)
                l2c = (round((float(run["last_200m"]) - par2) / LEN_SECONDS, 2)
                       if run["last_200m"] and par2 else None)
                inrun = f"finish,{run['position']};" if run["position"] else ""
                if run["position_800m"]:
                    inrun += f"settling_down,{run['position_800m']};m800,{run['position_800m']};"
                if run["position_400m"]:
                    inrun += f"m400,{run['position_400m']};"
                flucs = ""
                if run["odds_opening"] and run["odds_closing"]:
                    flucs = (f"opening,{float(run['odds_opening']):.2f};"
                             f"starting,{float(run['odds_closing']):.2f};")
                sect.append({
                    "meetingDate": str(run["race_date"]),
                    "track": {"name": run["track_name"],
                              "distance": run["distance"],
                              "raceClass": run["race_class"],
                              "trackCondition": run["track_condition"]},
                    "last600Class": l6c, "last200Class": l2c,
                    "finishClass": None, "to600Class": None,
                    "last600Time": float(run["last_600m"]) if run["last_600m"] else None,
                    "last200Time": float(run["last_200m"]) if run["last_200m"] else None,
                    "margFin": float(run["margin"]) if run["margin"] is not None else None,
                    "jockey": {"inRun": inrun, "flucs": flucs},
                })
            ja = self.a2e("jockey", e["jockey_name"] or "", rd) or {}
            ta = self.a2e("trainer", e["trainer_name"] or "", rd) or {}
            runners.append({
                # tab number isn't stored in results — barrel order is a
                # stable stand-in for the backtest (identity via horse_id
                # is what settlement uses). Live adapter supplies real tabs.
                "tabNumber": i + 1,
                "horseName": e["horse_name"],
                "barrier": e["barrier"],
                "weight": float(e["weight"]) if e["weight"] else None,
                "sex": e["sex"],
                "_horse_id": e["horse_id"],
                "_position": e["position"],
                "_sp": float(e["odds_closing"]) if e["odds_closing"] else None,
                "sectionalData": sect,
                "historicPerformanceData": self.perf_data(
                    e["horse_id"], rd, race["track_id"], race["distance"]),
                "jockeyData": {
                    "fullName": e["jockey_name"] or "",
                    "jockeyFormLast100Races": {
                        "strikeRatePct": ja.get("sr_100"),
                        "overUnderPerformancePct": ja.get("a2e_100"),
                    }},
                "trainerData": {
                    "trainerFormLast100Races": {
                        "name": e["trainer_name"] or "",
                        "strikeRatePct": ta.get("sr_100"),
                        "overUnderPerformancePct": ta.get("a2e_100"),
                    }},
            })
        return {"payLoad": {
            "name": race["race_name"], "number": race["race_number"],
            "distance": race["distance"], "raceClass": race["race_class"],
            "trackCondition": race["track_condition"],
            "meeting": {"track": {"name": race["track_name"]}},
            "runners": runners,
        }}


def main():
    day, track_like, race_no = sys.argv[1], sys.argv[2], int(sys.argv[3])
    rconn = psycopg2.connect(os.environ.get("RACING_DB_URL")
                             or os.environ["DATABASE_URL"], connect_timeout=20)
    cur = rconn.cursor()
    cur.execute("""SELECT r.race_id FROM races r JOIN tracks t ON t.track_id = r.track_id
        WHERE r.race_date = %s AND t.name ILIKE %s AND r.race_number = %s""",
        (day, f"%{track_like}%", race_no))
    row = cur.fetchone()
    if not row:
        print("race not found")
        return 1
    loader = LifeboatLoader(rconn)
    payload = loader.build_payload(row[0])

    from app.analyst import score_race, select_tips, reasoning_line
    scored = score_race(payload)
    tips = select_tips(scored)
    pl = payload["payLoad"]
    print(f"LIFEBOAT view — {pl['meeting']['track']['name']} R{pl['number']} "
          f"({pl['distance']}m {pl['trackCondition']})")
    for tt, key, angle in (("BEST", "ai_best", "best"), ("DANGER", "danger", "danger"),
                           ("VALUE", "value", "value")):
        e = tips.get(key)
        if e:
            res = next((r for r in pl["runners"] if r["tabNumber"] == e["tab_number"]), {})
            fin = res.get("_position")
            print(f"  {tt}: {e['horse_name']} [{'WON' if fin == 1 else str(fin) + 'th' if fin else '?'}]"
                  f" — {reasoning_line(e, pl['trackCondition'] or '', len(scored), angle=angle)}")
    print("\nfull lifeboat ranking:")
    for s in scored[:6]:
        res = next((r for r in pl["runners"] if r["tabNumber"] == s["tab_number"]), {})
        print(f"  {s['total']:+6.2f} {s['horse_name']:24.24s} fin={res.get('_position')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
