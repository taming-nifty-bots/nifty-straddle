# Nifty Straddle - Sensex Iron Fly Paper Pilot

Names are retained. The old Nifty/PnF/AFT business logic is replaced by the
single Dhan Sensex paper job. November8 rollout deployed and resumed
`nifty-straddle`; the obsolete Azure `nifty-straddle-signal` job was deleted.

November8 rule revision **v2 is the paper forward-test configuration**:
VIX percentile strictly above25 and half-premium wing distance.
Pushes to `master` test and deploy the retained Sensex job with
`live_trading=false`. BankNifty research does not change this instrument.
The earlier v1 rollout used image5; exact release status is recorded in the vault.

## Approved rules

- Dhan BSE Sensex options; four legs, one actual contract lot per leg.
- At15:15 IST, select the first listed monthly expiry at least45 calendar DTE.
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
- Check price exits only at09:30,10:30,11:30,12:30,13:30,14:30 and15:15 IST.
  If still open at21 calendar DTE or less, close at15:15.
- No adjustments, no overlapping positions, no same-expiry re-entry.
  A blocked VIX entry can be reconsidered on a later day while that expiry
  remains at least45 DTE.

The video tests naked Nifty straddles/strangles, not this Sensex iron fly.
The instrument, wings, whole-fly thresholds, VIX25 choice, checkpoint times
and first-expiry-at-least45 selection are owner-approved adaptations, not
claims of an exact reproduction or validated edge.

There are only two exits: the50%-credit profit target and21-DTE time exit.
The owner removed the price stop on November8. No monetary loss or trailing
stop is used; the intact hedge limits theoretical loss before costs.

## Single job and accounting

`straddle\straddle.py` now handles both entry selection and position management
in the existing `nifty-straddle` job. At15:15, when flat, it checks VIX and
builds the fly directly. Position exits still run at the seven checkpoints.
`ironfly.py` holds the plain-function rules, data checks and accounting.
No separate signal process or `Bots.supertrend` handoff is required.

The old signal script, Dockerfile and requirements were removed; its local
`.env` was left untouched. CI no longer builds, deploys or restarts the signal
job. Its Azure resource was deleted after the replacement was verified.
Historical Mongo trade/order records were not deleted.

Keep `Bots.nifty_Straddle_<user>` and `Bots.orders_nifty_Straddle_<user>`;
old records are untouched. New records have
`strategy_version=sensex-ironfly-45dte-v2` and `simulated=true`.
One deterministic trade ID per expiry prevents re-entry; deterministic paper
order IDs can be repaired from the trade after restart without duplicate fills.
Legacy/v1 active records block v2 rather than being managed under new rules.
Completed Sensex expiries remain blocked across both rule versions.

There is no broker-order path. `live_trading` must be absent/false; any other
value fails before connecting. Old `instrument_name` and `quantity` values
are deliberately ignored; the contract master supplies Sensex lot size.
November8 public-master preview for November9 selects December31,52 DTE,
20 units per leg. Actual entry must re-read the daily master and all prices.

Paper prices are the most recent positive-volume completed minute-candle
close in the same session, at/before the checkpoint. They are **not exact
trade-tape LTP or executable bid/ask fills**. Index/VIX candles must be fresh;
option quote timestamps are retained. Missing prices do not become zero.
Checks use a one-minute polling window, with no retrospective fills for
missed checkpoints. The single job ends at15:16 after the final window.
Completed entry decisions are not repeated within that window; data failures
can retry without leaving a partial paper trade.

`pnl` remains gross. `charges` is itemized and `net_pnl` is written on closure
after all eight simulated entry/exit orders, using shared BSE option charges.
Running marks and the profit-target trigger are gross of charges. See the vault's
Trading Charges reference for estimates and exclusions. No live leverage,
broker fills, margins or Sensex strategy profitability have been validated.

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
Configure one scheduled replica/parallelism, starting before09:30 IST and
allowing it to run through15:16. Delete the obsolete signal job only after the
single replacement job is deployed and its configuration is verified.

Dependencies require published `tamingnifty==2.4.1`, with BSE option charge
support and unchanged NSE defaults. Tag `v2.4.1` publication and exact wheel
source/checksum were verified before the consumer deployment.

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

Use the v2 functions in `ironfly.py` for historical selection and valuation;
do not duplicate the rules or pool v1/Nifty trades into the v2 results.
The proposed six-month window is May8-November6,2026. Historical fixed-contract
Sensex option data, expiry/listing/lot metadata and VIX warm-up history still
need verification. Rolling ATM observations must be matched to the original
four strikes and expiry; missing data is not a synthetic fill.
Sensex six-month and BankNifty three-month data probes found incomplete
fixed-contract coverage; no performance backtest was completed. This is not
proof that current listed Sensex contracts are unavailable. Forward testing
must verify all four actual contracts and prices; missing data blocks entry.