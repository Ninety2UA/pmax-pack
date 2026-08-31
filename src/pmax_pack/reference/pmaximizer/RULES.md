# pMaximizer parity and extended-score rules

Google's files in this directory are the reference. Production marts implement
the same rules on the v2 entity model while preserving paused rows. Live parity
compares only enabled entities because the pinned GAQL inputs filter Google's
population to enabled campaigns and asset groups.

## Campaign score

The source is `bq_queries/09-bpscore.sql:15-37`, with inputs assembled by
`bq_queries/07-campaign_data.sql`. The displayed campaign score is rounded to
two decimals at `09-bpscore.sql:23-35`, which freezes the config tolerance at
0.01. The tolerance is set before live comparison and is never learned from a
live result.

| Rule | Upstream expression | Our mart column | Production SQL |
|---|---|---|---|
| URL expansion | Opted out is 1, opted in is 0 | `url_expansion_score` | `mart_bp_campaign.sql`, nullable CASE over `url_expansion_opt_out` |
| Audience signals | distinct campaign audiences divided by asset-group count | `audience_signals_score` | distinct non-null audience values divided by enabled advertiser asset-group count |
| Sitelinks | campaign plus account sitelinks, threshold 4 | `sitelink_score` | campaign assets plus `int_entities_customer_asset`, including upstream's pinned below-threshold `missing_sitelinks / 4` expression |
| Geo targets | 0, 0.5, or 1 from the two `PRESENCE_OR_INTEREST` comparisons | `geo_target_score` | four-branch CASE copied semantically from 09 |
| Total | sum four components, divide by 4, round to 2 | `campaign_bp_score` | same when URL is known |

The pinned sitelink rule looks counterintuitive below the threshold: one
missing sitelink scores 0.25 and three missing score 0.75. Our parity score
reproduces that expression exactly. It is not silently corrected as a taste
decision.

### Nullable URL-expansion divergence

Production API v25 cannot supply `campaign.url_expansion_opt_out`. When the
schema value is null, `url_expansion_score` stays null and the production score
is the rounded mean of the other three known components. The row declares
`url_expansion_parity_mode = 'PARITY_NEUTRAL_UNKNOWN'`.

The comparison never treats that row as an ordinary equality. It reports a
rule-difference line with Google's raw four-rule score, reconstructs Google's
three-rule score from `campaign_data`, and compares that value to ours. A
missing rule-difference line is a harness failure, not a pass. When our value
is non-null, the raw four-rule scores are compared normally.

## Asset-group score

The sources are `bq_queries/01-image_assets.sql`, `04-text_assets.sql`,
`05-video_assets.sql`, and `10-assetgroupbestpractices.sql:17-69`.

| Component | Full-score threshold | Our columns |
|---|---|---|
| Video | at least 3 YouTube videos | `count_videos`, `video_score` |
| Text | descriptions at least 4, combined headlines at least 11, long headlines at least 2 | count columns plus `text_score`, the mean of three binary branches |
| Image | landscape, square, and portrait each at least 4; landscape and square logos each at least 1 | count columns plus `image_score`, the mean of five binary branches |
| Composite | mean of video, text, and image components, rounded to 2 | `asset_group_bp_score` |
| Ad strength | exposed by Google but does not alter these components | `ad_strength` |

The pinned table publishes both Recommended and Maximum recommendation rows,
but their three score components are identical. Our mart stores one row per
asset group. Parity compares the Recommended row's three components and the
derived composite.

## Extended score

The extended mart keeps `google_parity_score` unchanged and publishes four
inspectable components. They never feed `campaign_bp_score` or
`asset_group_bp_score`.

| Component | Definition | Columns |
|---|---|---|
| Action items | 1 when `ad_strength_action_items` is empty, else 0 | `ad_strength_action_item_count`, `action_item_clear_score` |
| Asset primary status | 1 when enabled advertiser asset links carry zero primary-status reasons, else 0 | `asset_primary_status_reason_count`, `primary_status_clear_score` |
| Feed-type coverage | distinct covered types divided by 8 | `covered_feed_type_count`, `feed_type_coverage_score` |
| Text guidelines | 1 when HEADLINE, LONG_HEADLINE, and DESCRIPTION are all present, else 0 | `text_guidelines_present`, `text_guidelines_present_score` |

The eight feed types are HEADLINE, LONG_HEADLINE, DESCRIPTION,
MARKETING_IMAGE, SQUARE_MARKETING_IMAGE, PORTRAIT_MARKETING_IMAGE, LOGO, and
YOUTUBE_VIDEO. `extended_score` is the equally weighted mean of the four
components, rounded to two decimals.

## Runtime rewrites

Reference files are byte-identical. The harness performs exactly these two
rewrites and `audit_reference_rewrites()` rejects additions:

| File | Exact source expression | Runtime replacement |
|---|---|---|
| `google_ads_queries/campaign_settings.sql` | backticked `some(campaign.asset_automation_settings, f(s) = equalText(s.asset_automation_type, 'FINAL_URL_EXPANSION_TEXT_ASSET_AUTOMATION') and equalText(s.asset_automation_status,'OPTED_OUT'))` | select `campaign.asset_automation_settings`; derive true when any element has the named type and `OPTED_OUT` status |
| `bq_queries/09-bpscore.sql` | `${format(today(),'yyyyMMdd')}` | requested parity date formatted as `YYYYMMDD` |

The fixture source renders the pinned BigQuery chain through sqlglot into
DuckDB because gaarf-bq requires a live BigQuery executor. The documented
dialect adjustments are dataset-name localization, BigQuery-to-DuckDB function
transpilation, and the same date-expression rewrite above. No scoring rule is
reimplemented for the Google fixture surface. When
`PMAX_CI_SCRATCH_PROJECT` is set, the same fixture entry point instead loads
both surfaces into `pmax_ci_scratch`, executes the Google chain through
gaarf-bq into `pmax_ci_scratch_bq`, executes rendered production score SQL,
compares, and drops every scratch table in `finally`.

## Rule-to-fixture coverage

`tests/fixtures/parity/source.json` carries the branch IDs below. CI derives
the actual set from eligible fixture rows, including campaign and asset-group
status plus eligible asset status and source, and then compares both the
declared and derived sets exactly with `REQUIRED_RULE_BRANCHES`. A removed,
invented, or data-less branch fails. Fixture campaign 4 and groups 7 to 8 are
enabled so GOOD and EXCELLENT participate in the parity intersection. Campaign
5 and groups 9 to 10 preserve paused-row coverage.

| Rule family | Below, false, or absent fixture | At, true, or present fixture | Additional branches |
|---|---|---|---|
| URL expansion | `campaign.url.opted_in` C2 | `campaign.url.opted_out` C1 | `campaign.url.unknown` C3 |
| Audience | `campaign.audience.absent` C2 | `campaign.audience.present` C1 | C3 ratio 1 of 2 |
| Sitelinks | `campaign.sitelinks.below_4` C2 | `campaign.sitelinks.at_4` C1 | account plus campaign assets |
| Geo | `campaign.geo.neither_presence_or_interest` C3 | `campaign.geo.both_presence_or_interest` C1 | `campaign.geo.mixed` C2 |
| Video | `asset_group.video.below_3` AG2 | `asset_group.video.at_3` AG1 | repeated across all strengths |
| Text descriptions | `asset_group.text.descriptions.below_4` AG2 | `asset_group.text.descriptions.at_4` AG1 |  |
| Text headlines | `asset_group.text.headlines.below_11` AG2 | `asset_group.text.headlines.at_11` AG1 |  |
| Text long headlines | `asset_group.text.long_headlines.below_2` AG2 | `asset_group.text.long_headlines.at_2` AG1 | AG2 also lacks the text-guideline component |
| Landscape image | `asset_group.image.landscape.below_4` AG2 | `asset_group.image.landscape.at_4` AG1 |  |
| Square image | `asset_group.image.square.below_4` AG2 | `asset_group.image.square.at_4` AG1 |  |
| Portrait image | `asset_group.image.portrait.below_4` AG2 | `asset_group.image.portrait.at_4` AG1 |  |
| Square logo | `asset_group.image.square_logo.absent` AG2 | `asset_group.image.square_logo.present` AG1 |  |
| Landscape logo | `asset_group.image.landscape_logo.absent` AG2 | `asset_group.image.landscape_logo.present` AG1 |  |
| Ad strength | UNSPECIFIED AG1, UNKNOWN AG2, PENDING AG3, NO_ADS AG4 | POOR AG5, AVERAGE AG6, GOOD AG7, EXCELLENT AG8 | every enum branch is named `asset_group.ad_strength.<value>` |

AG2 also carries two ours-only filter traps that are absent from Google's
input rows: an enabled AUTOMATICALLY_CREATED third video and a PAUSED
ADVERTISER fourth description. The production status and source filters keep
the hand-derived score unchanged; removing either filter makes fixture parity
fail.

The exact ad-strength branch IDs are
`asset_group.ad_strength.UNSPECIFIED`, `asset_group.ad_strength.UNKNOWN`,
`asset_group.ad_strength.PENDING`, `asset_group.ad_strength.NO_ADS`,
`asset_group.ad_strength.POOR`, `asset_group.ad_strength.AVERAGE`,
`asset_group.ad_strength.GOOD`, and `asset_group.ad_strength.EXCELLENT`.

`expected.json` is the known-score table. `seeded_mismatch.json` adds 0.02 to
C1's Google score, beyond the frozen 0.01 tolerance, and must return a failing
verdict naming the campaign and both scores.
