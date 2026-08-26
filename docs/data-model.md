# Performance Pack data model

The v2 model keeps landed Google Ads data, typed history, additive marts, and
ratio views separate. Every table uses a native date partition. Fact tables
partition on `date`; entity tables partition on `snapshot_date`. Tables cluster
first by `account_id`, then `campaign_id` and the grain key where available.
Marts do not require a partition filter, but the pack's jobs always filter the
partition and enforce a maximum bytes billed cap.

## Contract rules

- A table step replaces only the `@as_of` partition inside a transaction.
  Re-running a day does not delete any other day.
- Raw and staging money remains `INT64` micros. Published `network_cost` and
  `budget_amount` are `NUMERIC` currency amounts derived by dividing micros by
  1,000,000. `currency_code` states the account currency.
- Performance marts use `metric_basis`. `NETWORK` rows populate only the
  `network_*` group. `CONVERSION_ACTION` rows populate only the `action_*`
  group. Never sum those column groups together.
- A conversion action row keeps `ad_network_type` as a dimension but does not
  repeat cost, clicks, or impressions. The pack does not allocate cost to an
  action.
- Ratios exist only in `v_*` views. Every ratio is `SUM(numerator)` divided by
  `SUM(denominator)`. Mart columns remain additive within v2.x.
- Entity attributes join to a click day using the most recent complete
  snapshot on or before that day. Click days before the first complete
  snapshot use the first snapshot and set `attribute_provenance` to
  `assumed-current`.
- Per-day `int_entities_*` tables store observed rows and inferred tombstones.
  Their seen-bound columns stay null. The derived current-value views named
  `v_int_entities_*` calculate `first_seen_date` and `last_seen_date` across
  every complete snapshot day, so a prior-day reader sees a later observation
  immediately and no historical partition stores a stale bound.
- `observed` means the attribute came from the selected snapshot.
  `inferred-removed` means the entity was present on one complete day and
  absent on the next complete day. `unavailable` means no entity snapshot was
  available for the join.
- `url_expansion_opt_out` remains nullable. Google Ads API v25 cannot supply
  it, and this model never invents a value.
- Asset metrics are Google's asset-level attributions. They are not expected
  to add up to campaign totals. Use `mart_campaign_truth` for campaign totals.

Changing a partition or clustering specification requires a migration that
drops and recreates the affected `pmax_marts` table. Do not use `CREATE OR
REPLACE` to change either specification.

The manifest records per-step clustering metadata as a scheduling and review
simplification. A step that creates several physical staging or intermediate
tables declares the shared leading keys there, while each table's SQL DDL
states its complete clustering list. The physical DDL is authoritative.

## Shared column dictionary

| Column | Description |
|---|---|
| `date` | Google Ads click/report day and fact partition. |
| `snapshot_date` | Complete entity extraction day and entity partition. |
| `account_id` | Google Ads customer ID as `INT64`. |
| `campaign_id` | Google Ads campaign ID as `INT64`. |
| `asset_group_id` | Performance Max asset group ID as `INT64`. |
| `asset_id` | Google Ads asset ID as `INT64`. |
| `source_run_id` | Extraction run that supplied the selected landed row. |
| `built_by_run_id` / `run_id` | Model run that built the intermediate or published row. |
| `metric_basis` | `NETWORK` or `CONVERSION_ACTION`; identifies the populated additive group. |
| `ad_network_type` | Google Ads network segment. |
| `conversion_action_id` | Numeric ID parsed from the conversion action resource name. Null on network rows. |
| `conversion_action_resource_name` | Full conversion action resource name. It remains in the grain so two malformed or temporarily unparseable IDs cannot collapse. Null on network rows. |
| `conversion_action_name` | Google Ads conversion action display name. |
| `network_impressions` | Impressions from the network-segmented report. |
| `network_clicks` | Clicks from the network-segmented report. |
| `network_cost` | Account-currency cost derived from micros. |
| `network_conversions` / `network_conversions_value` | Primary conversion count and value from the network report. |
| `network_all_conversions` / `network_all_conversions_value` | All-conversion count and value from the network report. |
| `action_conversions` / `action_conversions_value` | Primary conversion count and value for one conversion action. |
| `action_all_conversions` / `action_all_conversions_value` | All-conversion count and value for one conversion action. |
| `currency_code` | ISO account currency from the customer snapshot. |
| `time_zone` | Google Ads account timezone. |
| `attribute_provenance` | `observed`, `assumed-current`, `inferred-removed`, or `unavailable`. |
| `first_seen_date` / `last_seen_date` | First and last complete snapshot days on which the entity was observed, derived by the current-value `v_int_entities_*` views. |
| `inferred_removed` | True only on the first complete day after the entity was last observed. |
| `primary_status` | Google Ads eligibility status. It is the asset eligibility source. |
| `primary_status_reasons` | Repeated Google Ads eligibility reasons. |

## Staging tables

Each staging table selects the latest `(loaded_at, run_id)` for its stated key
inside one partition. Staging retains raw metric types, except campaign start
and end strings become nullable `DATETIME` through safe parsing.

| Table | Grain and columns |
|---|---|
| `stg_volume_campaign` | Key: `date, account_id, campaign_id, ad_network_type`. Columns: shared lineage, `campaign_name`, `impressions`, `clicks`, `cost_micros`, `conversions`, `conversions_value`, `all_conversions`, `all_conversions_value`. |
| `stg_volume_asset_group` | Campaign volume key plus `asset_group_id`; adds `asset_group_name` and the same volume columns. |
| `stg_volume_asset` | Asset-group volume key plus `asset_id, field_type`; carries the same volume columns. |
| `stg_conv_campaign` | Key: date, account, campaign, network, `conversion_action`; carries action name and the four conversion measures. |
| `stg_conv_asset_group` | Campaign conversion key plus asset group and name. |
| `stg_conv_asset` | Asset-group conversion key plus `asset_id, field_type`. |
| `stg_lag_campaign` | Campaign conversion key plus `conversion_lag_bucket`; carries conversions and conversion value for U5. |
| `stg_lag_asset_group` | Campaign lag key plus asset group and name for U5. |
| `stg_entities_campaign` | Key: snapshot, account, campaign. Columns: names and statuses, reasons, channel type, repeated automation settings, geo settings, safely parsed start/end datetimes, budget fields, nullable URL expansion opt-out. |
| `stg_entities_asset_group` | Key: snapshot, account, campaign, asset group. Columns: names and statuses, reasons, ad strength, repeated ad-strength action items, final URLs. |
| `stg_entities_asset_group_asset` | Key: snapshot, account, campaign, asset group, asset, field type. Columns: status, primary status and reasons, source. |
| `stg_entities_asset` | Same asset key. Columns: name, type, orientation, text, image URL and dimensions, string `video_id`, video title. |
| `stg_entities_asset_group_signal` | Key includes signal resource name. Columns: approval status, audience, search theme. |
| `stg_entities_campaign_asset` | Key includes asset resource name and field type. Columns: nullable asset ID, status, primary status and reasons. |
| `stg_entities_conversion_action` | Key: snapshot, account, conversion action. Columns: name, category, counting type, status, click/view-through windows, inclusion flag, type. |
| `stg_entities_customer` | Key: snapshot and account. Columns: descriptive name, currency, timezone, status, manager flag. A row marks that account's entity family complete. |

## Intermediate tables

| Table | Grain and columns |
|---|---|
| `int_complete_snapshot_days` | One row per complete account and snapshot day; carries source and build run IDs. It is the only completeness spine used for removal inference. |
| `int_entities_campaign` | Campaign observations and tombstones with null stored bounds, removal flag, provenance, and lineage. `v_int_entities_campaign` supplies current seen bounds. |
| `int_entities_asset_group` | Asset-group observations and tombstones; `v_int_entities_asset_group` derives current seen bounds. |
| `int_entities_asset` | One asset-link observation or tombstone per field type, with media/text and eligibility attributes; `v_int_entities_asset` derives current seen bounds. |
| `int_entities_asset_group_signal` | Signal observations and tombstones; its `v_int_` view derives current seen bounds. |
| `int_entities_campaign_asset` | Campaign-asset observations and tombstones; its `v_int_` view derives current seen bounds. |
| `int_entities_conversion_action` | Conversion-action observations and tombstones with both windows; its `v_int_` view derives current seen bounds. |
| `int_entities_customer` | Account observations; `v_int_entities_customer` derives current seen bounds. Customer rows are not synthetically removed. |
| `int_performance_campaign` | Campaign fact grain by metric basis, network, and optional action. Carries additive groups, campaign status as of click day, customer attributes, action windows, provenance, lineage. |
| `int_performance_asset_group` | Campaign fact grain plus asset group. Carries asset-group status, primary status/reasons, ad strength, customer/action attributes, provenance, lineage. |
| `int_performance_asset` | Campaign fact grain plus asset group, asset, and field type. Carries the full action resource name, asset eligibility and reasons, source, text/image/video attributes, customer/action attributes, provenance, lineage. |

## Published marts

| Mart | Grain and column contract |
|---|---|
| `mart_performance_campaign` | One row per date, account, campaign, metric basis, network, and optional conversion action resource name. Columns are all shared performance columns plus campaign name/status/primary status/reasons, currency, timezone, action windows/settings, provenance, run ID. |
| `mart_performance_asset_group` | Campaign performance grain plus asset group. Adds asset-group name/status/primary status/reasons and ad strength. |
| `mart_performance_asset` | One row per date, network/basis, account, campaign, asset group, asset, field type, and optional conversion action resource name. Adds status, primary status/reasons, source, and text/image/video attributes. |
| `mart_asset_performance` | Consumer-facing copy at `(date, network/basis, account_id, campaign_id, asset_group_id, asset_id, field_type, optional conversion_action_resource_name)`. Eligibility is resolved per asset link from primary status and reasons. No constant performance label exists. |
| `mart_campaign_truth` | One row per date, account, campaign, and network from the campaign report. Columns: campaign name/status, impressions, clicks, cost, conversions and values, all conversions and values, currency, provenance, run ID. |
| `mart_entities_campaign` | Campaign snapshot contract. `budget_amount` is account currency; automation settings and status reasons remain repeated; URL expansion opt-out remains nullable. |
| `mart_entities_asset_group` | Asset-group snapshot contract with status, eligibility, reasons, ad strength, action items, URLs, seen bounds, removal, provenance. |
| `mart_entities_asset` | Asset-link snapshot with type, field type, text, image URL/dimensions, alphanumeric video ID/title, eligibility, reasons, seen bounds, removal, provenance. |
| `mart_entities_asset_group_signal` | Asset-group signal history with audience/search-theme values, approval status, seen bounds, removal, provenance. |
| `mart_entities_campaign_asset` | Campaign-level asset history, including sitelinks, with field type and eligibility history. |
| `mart_entities_conversion_action` | Conversion action history with category, counting type, status, inclusion flag, action type, and both lookback windows. |
| `mart_entities_customer` | Account descriptive name, currency, timezone, account status, manager flag, seen bounds, provenance. |

## Ratio views

| View | Columns and ratio contract |
|---|---|
| `v_performance_campaign` | Campaign dimensions, metric basis, network/action dimensions, provenance, summed additive groups, `ctr`, `cpa`, and `roas` from SUM over SUM. |
| `v_performance_asset_group` | Asset-group dimensions and status/ad-strength attributes with the same sums and ratios. |
| `v_performance_asset` | Compact asset dimensions and eligibility with the same sums and ratios. |
| `v_asset_performance` | Full asset text/image/video and eligibility dimensions with the same sums and ratios. |
| `v_campaign_truth` | Campaign-report dimensions and summed truth measures with SUM-over-SUM CTR, CPA, and ROAS. |

For conversion-action basis rows, network ratio inputs are null by design.
Action-level CPA is not published because the source does not allocate cost to
individual conversion actions.

The current ratio views group by the full mart dimensions, so their SUM/SUM
expressions are effectively row-grain today. SUM/SUM is still the contract: it
keeps the expression additive-safe when a consumer removes dimensions or a
later view exposes a coarser grouping.

## Current fork transformation mapping

The source names below come from the pinned current fork. U4 owns the score
marts named in this table. Entries marked dropped are deliberate removals, not
missing work.

| Fork query | Published table | v2 mapping |
|---|---|---|
| 01 `image_assets` | `image_assets` | `mart_entities_asset` image type, URL, dimensions, orientation. |
| 02 | TODO-U4 | TODO-U4 primary-action rule: verify against `reference/` at U4 and document how pMax primary selection derives from action settings. |
| 03 | TODO-U4 | TODO-U4 primary-action rule: verify against `reference/` at U4 and document how Search primary selection derives from action settings. |
| 04 `text_assets` | `text_assets` | `mart_entities_asset` text and field-type rows. |
| 05 `video_assets` | `video_assets` | `mart_entities_asset` video ID/title and orientation rows. |
| 06 | TODO-U4 | TODO-U4 (verify against `reference/` at U4); do not claim a dropped table name before the reference lands. |
| 07 `campaign_data` | `campaign_data` | `mart_entities_campaign`, `mart_entities_asset_group_signal`, `mart_entities_campaign_asset`, and `mart_entities_customer`. |
| 08 | TODO-U4 | TODO-U4 (verify against `reference/` at U4); a daily asset fact is not asserted as a summary replacement. |
| 09 `bpscore` | U4 score mart | U4 `mart_bp_campaign`; it consumes the entity marts and keeps nullable URL expansion behavior explicit. |
| 10 `assetgroupbestpractices` | `assetgroupbestpractices` | U4 `mart_bp_asset_group`. |
| 11 | TODO-U4 | TODO-U4 (verify against `reference/` at U4). |
| 12 `campaign_settings` | `campaign_settings` | `mart_entities_campaign` for settings and U4 `mart_bp_campaign` for scores. |
| 13 | TODO-U4 | TODO-U4 (verify against `reference/` at U4) before classifying any Looker-only drop. |
| 14 | TODO-U4 | TODO-U4 (verify against `reference/` at U4) before classifying any Looker-only drop. |
| 19 `assetgroupperformance` | `assetgroup_performance` | Additive metrics map to `mart_performance_asset_group`. **Dropped subfield: constant performance label** and its low-asset count. |
| 20 | TODO-U4 | TODO-U4 (verify against `reference/` at U4) before classifying any Looker-only drop. |
