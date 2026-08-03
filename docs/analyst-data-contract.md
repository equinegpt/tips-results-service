# 8F Analyst — Data Contract (Phase 1 deliverable)

Written 2026-08-04. Governs what the in-house tips analyst may and may
not consume. The analyst replaces Gemini + iReel (user directive
2026-08-04: strip all third-party AI; the replacement MUST be
independent of SkyNet, Clone, No Mugs — and, by extension of the same
independence principle, PFAI model outputs).

## Mission (inherited from the Gemini prompt, now the spec)

Per race, emit exactly three picks with pundit-style reasoning built
from real data points:

- **AI Best** — maximise WIN strike rate
- **Danger** — next-best win chance
- **Value** — a DIFFERENT angle: underrated vs the market

Decision priority cascade (highest first, conflict rule: sectionals
win unless the edge is < 1 length class AND the narrative case is
overwhelming):

1. Class-adjusted sectionals (last 600m class, last 200m class, finish
   class — negative = good)
2. Raw speed / time benchmarks
3. Barrier draw + speedmap position
4. Distance record
5. Track record
6. Conditions record
7. Run style / settling vs likely pace
8. Form trend over last 3 runs
9. Weight, jockey strike rate, trainer form (tiebreak only)

## ALLOWED inputs (all from PF raw data, all verified present)

Source of truth: `pf_ireel_races.payload` (shared Render Postgres),
archived from `https://api.puntingform.com.au/v2/ireel/race`.

Per runner:

| Spec priority | Field(s) |
|---|---|
| 1. Class sectionals | `sectionalData[].last600Class`, `.last200Class`, `.finishClass`, `.to600Class` (3 most recent runs each) |
| 2. Raw speed | `sectionalData[].last100Time/.last200Time/.last400Time/.last600Time/.last800Time`, `.early200Average`, `.meetingRank1F/2F/4F/6F/8F/.meetingRankTo600` |
| 3. Barrier + map | `barrier`; speedmap position from `pf_speedmap_snapshots` (only ≥ 2026-06-16 — before that, derive style from `sectionalData[].jockey.inRun` settling positions) |
| 4. Distance record | `historicPerformanceData` distance splits + `sectionalData[].track.distance` history |
| 5. Track record | `historicPerformanceData.trackRecord` |
| 6. Conditions | `historicPerformanceData.firmRecord/.goodRecord/.softRecord/.heavyRecord` vs race `trackCondition` |
| 7. Run style | `sectionalData[].jockey.inRun` ("settling_down,N;m800,N;m400,N;finish,N") |
| 8. Form trend | `sectionalData[].margFin`, positions across the 3 runs, `sectionalData[].jockey.flucs` (past-run market flucs: allowed — historical market fact, not a model) |
| 9. Tiebreakers | `weight`, `jockeyData.jockeyCareerForm/.jockeyFormLast100Races` (strikeRatePct, overUnderPerformancePct = the "A2E"), `trainerData.*` |

Race context: `payLoad.name/.number/.distance/.raceClass/
.trackCondition/.startTime/.meeting.track.*/.meeting.railPosition`,
scratchings (from the same sources the cron used).

## BANNED inputs (hard rule — never read, never output)

- `pfaiPrice`, `pfaiRank`, `pfaiScore` (PF's model)
- Anything from SkyNet (ranks, prices, snapshots)
- Anything from Clone / No Mugs (shadow_predictions, clone feed)
- Current-day market prices as a RANKING input. Exception: the
  **Value** pick is definitionally market-relative; it may compare the
  analyst's own completed ranking against current market order — the
  market must never influence the ranking itself, only identify which
  already-ranked horse the market underrates.

## Lookahead rule (backtest integrity — CRITICAL)

`pf_ireel_races` snapshots taken AFTER a race include that race in the
form (known trap, burned us before: latest-snapshot has post-race
data). Backtests MUST use the earliest snapshot per (meeting, race)
taken on or before race morning. Live generation uses the ~04:20–04:45
AEST payload drop; the backtest mirrors that timing.

## Inventory (verified 2026-08-04)

- `pf_ireel_races`: 2026-01-22 → today, 195 days, 9,910 distinct
  races, 66,776 snapshots (shared Render Postgres)
- `pf_speedmap_snapshots`: 2026-06-16 → today, 51 days
- Benchmark (TRS Postgres — `tip_runs`/`tips`/`tip_outcomes`):
  iReel tips Nov 2025 →, Gemini Apr 2026 →, ~3,500 tips/month each,
  **24,480 settled outcomes** (finish_position + starting_price)
- Results for settlement beyond tip_outcomes: `race_results` (TRS) and
  ra_results / pick_results (shared DB)

## Integration decisions (Phase 4, recorded now)

- Generator writes into TRS's own tables under **source = "8F"**
  (tip_runs.source) — the app, stats pages, and every engine consumer
  keep working via `GEMINI_ALIAS_SOURCE=8F` + `TIPS_DEFAULT_SOURCE=8F`.
- TRS becomes storage + serving only; `gemini_client.py`,
  `ireel_client.py`, their creds and the external legs of
  `generate_tips_cron.sh` are DELETED (not disabled) once the
  backtest gate passes.
- Cohort honesty: C lane / crowns / danger-lay validated on Gemini
  opinions; from flip day they are new-basis cohorts and re-earn
  their numbers.

## Phase 3 gate (go/no-go, agreed)

Retro-generate tips for the archived window; score at SP:
- AI Best strike + ROI vs Gemini's and iReel's recorded AI Best on the
  same races
- Danger as a paper-lay cohort vs the gemini_danger ledger
- Value strike/ROI at its own price profile
No product integration until these numbers are reviewed.
