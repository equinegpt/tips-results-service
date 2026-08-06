# Lifeboat Runbook — losing the Punting Form APIs

**Status: BUILT, GRADED, PARKED. `LIFEBOAT=1` is set by a human only.
Nothing in the stack may automate this switch.**

Last graded 2026-08-06 (Phase C, 4,177 races Apr–Aug 2026):

| Config | Strike | SP ROI |
|---|---|---|
| Live Jennifer (PF data) | 22.3% | −11.0% |
| **Lifeboat (zero PF)** | **19.1%** | **−6.0%** |

Losing PF costs ~3 strike points at comparable-or-better SP ROI.
Refit-on-lifeboat-features weights FAIL the ROI ship-guard — **code
weights are the lifeboat config** (the generator handles this: in
lifeboat mode it ignores `jennifer_weights` and stamps
`jennifer-lifeboat-v1`).

## What survives, what dies

| Component | PF-loss fate |
|---|---|
| Jennifer tips (Best/Danger/Value) | **SURVIVES** via this lifeboat |
| TRS history/API, settlement ledger | survives (source-agnostic) |
| Clone | **DIES** — features built from PF payloads (`pf_form_full`, `pf_speedmap_snapshots`, `pf_ireel_races`) |
| No Mugs | **DIES** — same feature table |
| C lane / crowns (Clone∩Jennifer) | dies with Clone |
| Speed maps, PF scratchings service | dead — cards carry scratchings instead |
| SkyNet feed, Betfair/Racenet prices, plunge watch | unaffected |

A PF-independent Clone/NM feature build is a separate project; run the
frozen-Clone ablation test first (null PF features, measure decay).

## Break-glass procedure (PF confirmed dead, not just an outage)

1. **Don't flip anything for a one-day outage.** Jennifer's 05:00/08:00
   runs fail loudly; a missed day of tips is cheaper than a rushed
   switch. Break glass only when PF is gone for good (contract, company,
   or multi-day hard-down).
2. **Refresh the engines** (staleness is the main decay mode; both are
   static builds — run from a machine with `RACING_DB_URL`):
   ```
   python scripts/lifeboat/build_pars.py        # ~5 min
   python scripts/lifeboat/build_a2e.py         # ~10 min, RAM-heavy: run local/Pi, not a small Render box
   ```
   racing-db keeps filling from the RA crawl without PF — verify recent
   `race_results` rows exist for last week before trusting a refresh.
3. **Fetch today's card** (needs `SCRAPER_API_KEY`, Scrape.do; goes
   straight to residential `super=true` — Racenet's Kasada blocks
   datacenter IPs):
   ```
   python scripts/lifeboat/lifeboat_card_adapter.py
   ```
   Canary: horse match-rate ≥90% on a healthy day (adapter exits 1
   below 70%). Unmatched = first starters (fine) or name gaps.
4. **Dry-run the generator and READ the output** before publishing:
   ```
   LIFEBOAT=1 python scripts/generate_8f_tips.py --date YYYY-MM-DD
   ```
5. **Publish** (same command + `--commit`). Tips land in TRS as
   `source='Jennifer'`, `model_version='jennifer-lifeboat-v1'` — the
   version stamp keeps lifeboat cohorts separable in every ledger.
6. **Ops changes on the day:**
   - On Render `tips-results-service` cron env: add `LIFEBOAT=1`,
     `RACING_DB_URL`, `SCRAPER_API_KEY` — and remember env-var changes
     do NOT auto-deploy (POST /deploys).
   - Card adapter must run before the 05:00 generator fire — schedule
     it ~04:30 AEST.
   - Suspend consumers of dead PF services (scratchings regen keys off
     cards automatically in lifeboat mode; speedmap-dependent features
     are already absent from the validated config).
   - Announce on 8F-Ops: cohort basis change date (like 2026-08-04) —
     lifeboat-era results are a new evidence cohort.

## Rollback

Unset `LIFEBOAT=1` (and redeploy). The default PF path is untouched by
lifeboat code — the switch is a payload-source swap only.

## Standing maintenance

- **Quarterly drill** — Render cron `lifeboat-quarterly-drill`
  (crn-d9q54rflk1mc73ejqf50, 1 Jan/Apr/Jul/Oct 22:00Z): freshness
  checks → live card fetch → LIFEBOAT dry-run → 8F-Ops PASS/FAIL.
  A failed drill is a real incident: the fallback has rotted.
- **Engine refresh** — quarterly with the drill, or ad-hoc before any
  real activation (step 2). Not yet cron'd: build_a2e RAM footprint
  needs a bigger box than the cron plan.
- After any Racenet page redesign, re-run
  `lifeboat_card_adapter.py --max-races 3` and eyeball the parse.

## Known limitations (accepted for break-glass)

- Trainer partnerships ("Ben, Will & JD Hayes") may miss the A2E key →
  conn component degrades toward 0 for those runners. Jockey names are
  exact (profile-slug full names).
- Current-race class string sometimes unparsed (scoring doesn't use
  it; pars key off each HISTORY run's class, which comes from
  racing-db).
- A2E expectation is raw 1/SP (not overround-normalized) — uniform
  scale, invisible to within-race z-norm.
- tabNumber in *backtests* is barrier-order; live cards carry real
  Racenet tabs (this is why drill and backtest paths differ slightly).
