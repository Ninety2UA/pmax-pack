# Cohort CPA and ROAS

The cohort model keeps one additive row per click date, entity, network,
conversion basis, and cohort rung. Campaign and asset-group history comes from
Google's conversion-lag buckets. Asset history starts with the first successful
snapshot and comes from the append-only observation log. The pack never
forecasts a cell and never allocates conversions from an unknown lag bucket.

## Two readings of day D

A bucket-derived D cell is the prefix sum of lag buckets whose upper boundary
is at most D days. It answers how many conversions Google attributes inside
that lag boundary. Campaign and asset-group cells use this reading and carry
the family C load timestamp as `observed_through`.

A snapshot-derived D cell is the value observed on click date plus D days. It
answers what Google had reported by that account-local observation date. An
exact observation is `measured`. If that day was missed, the prior observation
may be `carried` across at most five calendar days. The row records the earlier
account-local date as a timestamp in `observed_through`, so a consumer can see
the offset. The source log is day-grained, so the timestamp marks local
midnight rather than inventing sub-day precision. A seed observation is never
carried.

The two readings can differ at a boundary. A bucket uses Google's reported lag
classification. A snapshot uses the cumulative value visible on a particular
run date. Compare them only after preserving `provenance`, `maturity`, and
`observed_through` as dimensions.

## Conversion bases and windows

Every grain can expose three conversion bases:

- `PRIMARY` uses actions included in the Conversions metric. During the Google
  Ads goals migration, an action can contribute to Conversions while its
  legacy `include_in_conversions_metric` snapshot flag is false. The pack
  therefore treats either a true legacy flag or nonzero family B/C
  `conversions` for that click-day account as PRIMARY membership evidence.
  Its PRIMARY window is the longest window among those contributing actions.
- `ALL_CONVERSIONS` uses the all-conversions basis and the longest contributing
  action window.
- `CONVERSION_ACTION` keeps each named action separate.

Configured rungs must be in the exact bucket boundary set: D1 through D14,
D21, D30, D45, D60, or D90. Each action's click-through lookback window caps
its ladder. Configured days after the window do not exist. The window is always
the final rung and is labelled `D<window> window`. A window on a bucket boundary
uses the bucket prefix. A non-boundary window uses Google's click-day conversion
total from family A for aggregate bases and family B for a named action.

For click dates before the first entity snapshot, the first snapshot's window
is used with `window_provenance = 'assumed-current'`. Later click dates use the
window observed on the click date and carry `window_provenance = 'observed'`.

## Provenance and maturity

Every cell has one provenance:

- `measured`: supplied by an exact lag bucket or exact observation day.
- `carried`: supplied by the prior non-seed observation, with a gap of no more
  than five calendar days.
- `unavailable`: no value is supplied. `unavailable_reason` is `before first
  snapshot`, `gap exceeded`, or `seed only`.

Maturity is evidence-based. A cell is `complete` when the selected source
stream has refreshed on or after click date plus its cohort day. Calendar age
alone never completes a cell. A carried snapshot cell becomes complete once a
later selected observation reaches the target day, even though
`observed_through` continues to identify the earlier observation that supplied
the value. A frozen stream never advances that evidence bound, so its existing
bucket cells remain `immature`. `stale_cell_count` makes immature cells
countable in reports.

Snapshot rows are materialized only through the account's latest selected
observation date. A future target is absent, not labelled `gap exceeded` or
pending. Within that bound each label is final: exact-day observations are
measured, a prior non-seed observation can be carried for at most five days,
and every other gap is unavailable. The daily driver rebuilds every eligible
click day in one transaction per table across the shared re-pull window; a
manifest step can update more than one table in one transaction. New cells
appear as the selected observation bound advances. That window is
the longest click-through lookback found in the resolved accounts' latest
complete family D snapshots plus the restatement margin. Before every account
has such a snapshot, the driver records `config_fallback` and uses the largest
configured cohort day plus the margin. Reports record the chosen source and
effective window length.

## Additive marts and ratio views

`mart_cohort_campaign`, `mart_cohort_asset_group`, and `mart_cohort_asset`
contain only additive components and dimensions. Cost is fixed at the click
date and joined from family A at the matching entity and network grain.
`missing_cost_cell_count` is one when a cohort cell has no click-day cost.

The `v_cohort_*` views exclude missing-cost cells and calculate ratios after
aggregation:

- cohort CPA = `SUM(click_day_cost) / SUM(cohorted_conversions)`
- cohort ROAS = `SUM(cohorted_value) / SUM(click_day_cost)`

The views expose maturity rather than filtering it. Consumers must keep
`maturity` visible or filter it explicitly so an immature tail cannot be mixed
silently into a complete ratio.

## Unknown lag

`unknown_lag_conversions` and `unknown_lag_value` retain Google's `UNKNOWN` and
any unrecognized lag bucket. Those values are repeated as diagnostics at each
rung for the same grain and basis, but they never contribute to
`cohorted_conversions` or `cohorted_value`. Do not sum the diagnostic across
different cohort days.
