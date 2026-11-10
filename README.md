# Nifty Straddle - Once-Daily Nifty Iron Fly Paper Pilot

## NSE startup calendar

The paper job requires `tamingnifty==2.4.3` and the annual `NSE:YYYY` document
in `Bots.exchange_calendars`. Startup checks the IST trading date before broker
login, market data or state writes. Holidays and weekends exit successfully;
missing/invalid calendars or DB failures abort nonzero (fail-closed). Seed and
verify the current year's calendar before starting. The15:00–15:05 window still applies.
Offline startup checks: `python test_startup_calendar.py`.

Names are retained. One Dhan Nifty paper job runs at15:00 IST, updates Mongo
and exits. There is no all-day loop, separate signal job or real-order path.
Current version: `nifty-ironfly-45dte-v1`. Sensex history remains untouched.

Pushes to `master` test and deploy the retained job with `live_trading=false`,
weekday UTC cron `30 9 * * 1-5` (15:00 IST), one replica/parallelism/completion,
zero retries and a900-second maximum timeout. The process exits as soon as
its single evaluation finishes; timeout is a safety cap, not a waiting period.

## Approved rules

- Dhan NSE Nifty options; four legs, one actual contract lot per leg.
- At15:00 IST, select the first listed monthly expiry at least45 calendar DTE.
  Monthly dates labelled `M`, `Q` or `H` in the master are eligible; `W` is not.
- Enter only if current India VIX is strictly above its percentile25 relative
  to the preceding year's daily closes. Exclude today's daily bar; require
  at least200 distinct observations spanning350 days. Missing data blocks entry.
- Sell ATM CE and PE at the same100-point-rounded strike.
- Buy PE below ATM and CE above ATM. Symmetric wing distance is **50%** of
  the initial short CE+PE quoted premium, rounded up to the next100-point
  strike interval. This specifies strike distance, not the hedge purchase price.
- Whole-fly net credit means shorts minus hedges, **before charges**.
  Take profit when whole-fly closing value is at most50% of that initial credit.
- Check both the50%-credit target and21-calendar-DTE exit once daily at15:00.
  No intraday price stop. Daily-only target checks can miss temporary profits.
- If a position remains open, save its gross `pnl`/`running_pnl`, calendar
  `holding_days`, `dte`, `last_check`, closing value and four observed marks.
  `net_pnl` is final only after closure; entry charges remain recorded.
- After a closure, repair the exit order records and consider a new eligible
  expiry immediately. An already-traded Nifty expiry remains blocked.
- No adjustments, no overlapping positions, no same-expiry re-entry.
  A blocked VIX entry can be reconsidered on a later day while that expiry
  remains at least45 DTE.

The video tests naked Nifty straddles/strangles, not this hedged iron fly.
The wings, whole-fly thresholds, VIX25 choice, daily checkpoint
and first-expiry-at-least45 selection are owner-approved adaptations, not
claims of an exact reproduction or validated edge.

There are only two exits: the50%-credit profit target and21-DTE time exit.
The owner removed the price stop on November8. No monetary loss or trailing
stop is used; the intact hedge limits theoretical loss before costs.

## Single job and accounting

`straddle\straddle.py` now handles both entry selection and position management
in the existing `nifty-straddle` job. At15:00 it manages any active position
first, then checks VIX and builds the fly if flat.
`ironfly.py` holds the plain-function rules, data checks and accounting.
No separate signal process or `Bots.supertrend` handoff is required.

The old signal script, Dockerfile and requirements were removed; its local
`.env` was left untouched. CI no longer builds, deploys or restarts the signal
job. Its Azure resource was deleted after the replacement was verified.
Historical Mongo trade/order records were not deleted.

Keep `Bots.nifty_Straddle_<user>` and `Bots.orders_nifty_Straddle_<user>`;
old records are untouched. New records have
`strategy_version=nifty-ironfly-45dte-v1` and `simulated=true`.
One deterministic trade ID per expiry prevents re-entry; deterministic paper
order IDs can be repaired from the trade after restart without duplicate fills.
Any legacy/Sensex active record blocks this job rather than being silently
converted. Closed Sensex history does not block a Nifty expiry.

There is no broker-order path. `live_trading` must be absent/false; any other
value fails before connecting. Old `instrument_name` and `quantity` values
are deliberately ignored; the contract master supplies Nifty lot size.
November9 public-master preview for November10 selects December29,49 DTE,
65 units per leg. Actual entry must re-read the daily master and all prices.

Paper prices are the most recent positive-volume completed minute-candle
close in the same session, at/before the checkpoint. They are **not exact
trade-tape LTP or executable bid/ask fills**. Index/VIX candles must be fresh;
option quote timestamps are retained. Missing prices do not become zero.
Startup is allowed from15:00 until15:05 IST; weekends or out-of-window
starts notify and exit without broker requests. The actual startup minute is
the cutoff:15:03 uses completed15:02-or-earlier same-session candles, never
an invented15:00 fill. It evaluates once, not a polling/retry loop.
An already-marked active trade is not marked again on a same-day rerun.
Missing prices fail visibly and exit unsuccessfully; no partial paper entry
or retrospective fill is fabricated. Re-running a failed job is manual.

Historical Sensex v2: November9's15:15 entry was blocked by missing candles. An after-market
read-only probe recovered Sensex/VIX15:14 closes, but the selected December31
81100 CE (security1135111) returned HTTP200 with zero candles for the day.
No v2 paper trade was written. Missing-price errors now identify security ID,
segment, instrument and checkpoint; no price fallback or retrospective entry
was added. Option-data coverage
must be resolved separately; later recovery of index data does not prove what
the API returned at15:15.

`pnl` remains gross. `charges` is itemized and `net_pnl` is written on closure
after all eight simulated entry/exit orders, using shared NSE option charges.
Running marks and the profit-target trigger are gross of charges. See the vault's
Trading Charges reference for estimates and exclusions. No live leverage,
broker fills, margins or Nifty iron-fly profitability have been validated.

## Local validation and deployment prerequisites

From this repository:

```powershell
python test_ironfly.py
python straddle\straddle.py
docker build -f straddle\Dockerfile -t nifty-straddle:local .
```

Runtime needs `DHAN_CLIENT_ID`, `DHAN_PIN`, `DHAN_TOTP`, `CONNECTION_STRING`,
`user_name` and `slack_token`. Dhan authentication reuses the shared Mongo token;
do not create a separate per-job login/token store. Do not run the job merely
to test imports: successful paper jobs write trade/order records.

The one Docker build uses the repository root context, with `.env` and git
data excluded. The image/job name remains `nifty-straddle`.
CI runs offline tests before deploying that one job.
CI explicitly sets `live_trading=false`. The Azure value is configurable, but
`true` is deliberately rejected: this version has no real-order implementation.

Inject Dhan credentials, Mongo connection and Slack token through Azure job
secret references, never Docker defaults or git. Remove the old Definedge
`INTEGRATE_API_TOKEN`, `INTEGRATE_API_SECRET` and `TOTP` environment settings.
CI configures one scheduled replica/parallelism at15:00 IST with zero retries
and a900-second cap. There is no all-day waiting process.

Dependencies now require `tamingnifty==2.4.3` for startup calendar gating.
The prior deployment used `2.4.1`, with BSE option charge support and unchanged
NSE defaults; its publication/wheel verification does not verify the new release.

## Historical Sensex rollout and data gaps

November8 operational configuration: `live_trading=false`, one replica/
parallelism/completion, zero retries,25,000-second timeout. Cron
`50 3 * * 1-5` starts at09:20 IST Monday-Friday; the process ends at15:16.
Azure credentials are five secret references; old Definedge, instrument and
quantity environment settings were removed. Verify `runningState=Ready`
through ARM API2026-07-01: older CLI output can omit the suspension field.

V2 validation:26 offline strategy checks pass, including strict25
boundaries, exact decimal half-premium rounding, both exits and v1 separation.
Initial v1 release:23 strategy checks and7 shared-charge checks passed.
The initial code deployment succeeded in run4. Its bounded startup execution
logged PAPER ONLY without runtime errors and was explicitly stopped.
Mongo inspection found no active legacy trade and no new-version record.

Read-only November6 data preview verified Dhan authentication and reached
option pricing after the VIX filter, but the selected long-dated option had
no historical candles. A quote diagnostic received HTTP429 and was stopped.
Illiquidity versus data coverage remains unresolved; market-session option
pricing/entry is not yet validated. Missing prices block entry, never become
fabricated fills. No strategy profitability or live execution is validated.

## Backtest preparation

Current `ironfly.py` is the once-daily Nifty version. Historical Sensex v2
replays require its prior revision; do not pool instruments or timing policies.
The proposed six-month window is May8-November6,2026. Historical fixed-contract
Sensex option data, expiry/listing/lot metadata and VIX warm-up history still
need verification. Rolling ATM observations must be matched to the original
four strikes and expiry; missing data is not a synthetic fill.
Sensex six-month and BankNifty three-month data probes found incomplete
fixed-contract coverage; no performance backtest was completed. This is not
proof that current listed Sensex contracts are unavailable. Forward testing
must verify all four actual contracts and prices; missing data blocks entry.