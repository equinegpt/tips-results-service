# Project Lifeboat — the PF-independence fallback (Phase A: design)

Written 2026-08-06, user-directed de-risk project. **Built, validated,
and PARKED — never switched on while PF is alive.** Purpose: know
exactly what the world looks like if the Punting Form APIs disappear,
and turn "dark for two weeks" into "degraded same-day".

Assumption set (user): SkyNet's feed persists (obtainable directly if
needed); PF form payloads, sectional class figures, speedmaps and the
scratchings/conditions service are all LOST.

## The discovery that changes the plan

racing-db (our own Postgres, 574,919 races / 2.54M runner rows back to
1986) already holds, per run: **raw sectional times (last 600/400/200m),
in-run positions (800m/400m), barrier, weight, opening AND closing
odds, margin, gear** — plus per race: track condition, rail, distance,
class, field size. Modern coverage (2023→): ~80% of runs have 600m
times, ~86% in-run positions, ~90% SP.

Consequence: the "irreplaceable" PF class-adjusted sectionals are
REBUILDABLE in-house. With decades of raw times we can fit our own
par-time benchmarks by (track, distance, condition bucket, class
bucket) and express any run as lengths-vs-par — same shape as PF's
`last600Class`, our arithmetic, our data. This is the core Phase B
build and the reason Jennifer-lite need not be very "lite".

## Ingredient-by-ingredient map (Jennifer's 8 inputs)

| Ingredient | PF source (today) | Lifeboat source | Verdict |
|---|---|---|---|
| Class sectionals | last600Class etc. | **own par benchmarks** from race_results.last_600m vs (track,dist,cond,class) pars | Rebuildable — the big build |
| Raw speed | meeting ranks | meeting percentile of raw last_600m within card | Rebuildable |
| Map/barrier | inRun + speedmap | barrier + settle from position_800m history | Rebuildable (no speedmap; settle history ≈ what validated anyway) |
| Distance record | historicPerformanceData | aggregate own results | Trivial |
| Track record | " | " | Trivial |
| Conditions record | " | races.track_condition joins | Trivial |
| Form trend | margFin/positions | margins + positions | Trivial |
| Connections | jockey/trainer A2E | strike rates + A2E from odds_closing | Rebuildable (A2E = wins vs SP-implied, our own math) |

Card source on race morning (fields, barriers, jockeys, scratchings)
without PF: **Racenet form-guide pages** — the odds sweep already
parses these server-side with the residential path; the same pages
carry the full field list. Scratchings: Racenet marks them; fallback
sweep flags them. Conditions: Racenet meeting pages.

## What is genuinely lost in the lifeboat world

- **Speedmaps** (predicted settle from PF's model) — we fall back to
  historical settle patterns, which is what the validated Jennifer
  config uses anyway. Real loss: near zero today.
- **PFAI** — display-only today. Loss: cosmetic.
- **Sectional freshness during transition** — nothing; our own times
  keep flowing from RA/racing.com results crawls independent of PF.
- **Class figures quality** — PF's benchmarking has years of tuning;
  our par model v1 will be cruder. The Phase C backtest quantifies the
  honest gap instead of guessing it.
- Clone's labels (SkyNet via puntx) — out of scope per the assumption;
  if SkyNet also died, Clone retires and NoMugs retrains on lifeboat
  features (it's outcome-trained, so it survives any label loss).

## Build phases (low priority, no deadlines)

- **A (this doc)** — inventory + design. DONE.
- **B — par benchmarks + feature builder.** `scripts/lifeboat/`
  1. `build_pars.py`: fit par last-600/last-200 times by (track,
     distance bucket, condition bucket, class bucket) with shrinkage
     toward (track, distance) then global pars; store in racing-db
     table `sectional_pars`. Refresh monthly via existing racing-db
     cron budget.
  2. `lifeboat_features.py`: given (date, race card), emit the same
     z-component vector Jennifer consumes, sourced 100% from racing-db
     + Racenet card. Output format identical to the PF loader so the
     scorer/reasoning/generator need zero changes.
- **C — Jennifer-lite backtest.** Same harness, same discipline:
  generate over the archive months using ONLY lifeboat features,
  re-fit weights on the tune window (the fitted weights will differ —
  e.g. speed may earn weight back since our par model differs), one
  holdout read. Deliverable: the honest delta vs live Jennifer
  (expectation: within 1–3 strike points; the backtest decides).
- **D — runbook + parked switch.** `LIFEBOAT=1` env on the generator
  selects the lifeboat loader; model_version stamps
  `jennifer-lifeboat-vN` so any activation is visible in every ledger.
  Runbook documents the one-command activation + what degrades.
  A quarterly 1-day drill regenerates a recent week both ways and
  diffs — proves the lifeboat still floats without launching it.

## Standing rule

The lifeboat is never enabled while PF is alive, and its activation is
an explicit human decision — the switch is an env var no automation
touches. Its existence must also never weaken the case for keeping the
PF relationship healthy (checking the contract's name/renewal remains
the single highest-value de-risk action, and is a user action).
