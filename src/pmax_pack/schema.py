"""Ops table specs for pMax Performance Pack.

TableSpec plus OPS_TABLES live here so ensure_table (later units) and the
ledger share one schema. RAW_TABLES is the U2 extension point for families
A-D. OBSERVATION_TABLE is the U13 extension point for the append-only
observation log. Google Ads ids are INT64. Event tables are append-only.
"""
from __future__ import annotations

from dataclasses import dataclass

from google.cloud.bigquery import SchemaField


@dataclass
class TableSpec:
    """Partitioned BigQuery table: schema, partition, and clustering."""

    name: str
    dataset_key: str
    fields: list[SchemaField]
    partition_field: str
    partition_type: str
    clustering_fields: list[str]
    description: str


def _sf(name: str, field_type: str, mode: str = "NULLABLE") -> SchemaField:
    return SchemaField(name, field_type, mode=mode)


def _record(name: str, fields: list[SchemaField], mode: str = "REPEATED") -> SchemaField:
    return SchemaField(name, "RECORD", mode=mode, fields=fields)


RUNS_FIELDS = [
    _sf("run_id", "STRING", "REQUIRED"),
    _sf("event", "STRING", "REQUIRED"),
    _sf("mode", "STRING"),
    _sf("status", "STRING", "REQUIRED"),
    _sf("as_of_date", "DATE"),
    _sf("accounts_configured", "INT64", "REPEATED"),
    _sf("accounts_resolved", "INT64", "REPEATED"),
    _sf("window_start", "DATE"),
    _sf("window_end", "DATE"),
    _sf("image_digest", "STRING"),
    _sf("credential_fingerprint", "STRING"),
    _sf("checkpoint_hash", "STRING"),
    _sf("stage_reached", "STRING"),
    _sf("error", "STRING"),
    _sf("report_uri", "STRING"),
    _sf("event_ts", "TIMESTAMP", "REQUIRED"),
]

STAGES_FIELDS = [
    _sf("run_id", "STRING", "REQUIRED"),
    _sf("stage", "STRING", "REQUIRED"),
    _sf("status", "STRING", "REQUIRED"),
    _sf("account_id", "INT64"),
    _sf("detail", "STRING"),
    _sf("error", "STRING"),
    _sf("event_ts", "TIMESTAMP", "REQUIRED"),
]

LOAD_CHECKPOINTS_FIELDS = [
    _sf("account_id", "INT64", "REQUIRED"),
    _sf("chunk", "STRING", "REQUIRED"),
    _sf("family", "STRING", "REQUIRED"),
    _sf("checkpoint_hash", "STRING", "REQUIRED"),
    _sf("run_id", "STRING"),
    _sf("completed_at", "TIMESTAMP", "REQUIRED"),
]

ASSERTION_RESULTS_FIELDS = [
    _sf("run_id", "STRING", "REQUIRED"),
    _sf("assertion", "STRING", "REQUIRED"),
    _sf("severity", "STRING", "REQUIRED"),
    _sf("passed", "BOOL", "REQUIRED"),
    _sf("observed", "STRING"),
    _sf("expected", "STRING"),
    _sf("detail", "STRING"),
    _sf("event_ts", "TIMESTAMP", "REQUIRED"),
]

SCHEMA_VERSION_FIELDS = [
    _sf("version", "STRING", "REQUIRED"),
    _sf("applied_at", "TIMESTAMP", "REQUIRED"),
    _sf("run_id", "STRING"),
]

FIRST_SNAPSHOT_FIELDS = [
    _sf("account_id", "INT64", "REQUIRED"),
    _sf("first_snapshot_date", "DATE", "REQUIRED"),
    _sf("run_id", "STRING", "REQUIRED"),
    _sf("set_at", "TIMESTAMP", "REQUIRED"),
]

LEASE_EVENTS_FIELDS = [
    _sf("run_id", "STRING", "REQUIRED"),
    _sf("event", "STRING", "REQUIRED"),
    _sf("holder", "STRING"),
    _sf("mode", "STRING"),
    _sf("expires_at", "TIMESTAMP"),
    _sf("generation", "INT64"),
    _sf("prior_run_id", "STRING"),
    _sf("event_ts", "TIMESTAMP", "REQUIRED"),
]

OPS_TABLES: dict[str, TableSpec] = {
    "runs": TableSpec(
        name="runs",
        dataset_key="ops",
        fields=RUNS_FIELDS,
        partition_field="event_ts",
        partition_type="DAY",
        clustering_fields=["run_id"],
        description=(
            "Append-only run events (STARTED, EXITED). State is latest-per-run_id."
        ),
    ),
    "stages": TableSpec(
        name="stages",
        dataset_key="ops",
        fields=STAGES_FIELDS,
        partition_field="event_ts",
        partition_type="DAY",
        clustering_fields=["run_id"],
        description="Append-only per-stage events (STARTED, SUCCESS, FAILED).",
    ),
    "load_checkpoints": TableSpec(
        name="load_checkpoints",
        dataset_key="ops",
        fields=LOAD_CHECKPOINTS_FIELDS,
        partition_field="completed_at",
        partition_type="DAY",
        clustering_fields=["account_id"],
        description=(
            "Completed account-month-family loads keyed by checkpoint hash."
        ),
    ),
    "assertion_results": TableSpec(
        name="assertion_results",
        dataset_key="ops",
        fields=ASSERTION_RESULTS_FIELDS,
        partition_field="event_ts",
        partition_type="DAY",
        clustering_fields=["run_id"],
        description="Hard and soft validation assertion outcomes for a run.",
    ),
    "schema_version": TableSpec(
        name="schema_version",
        dataset_key="ops",
        fields=SCHEMA_VERSION_FIELDS,
        partition_field="applied_at",
        partition_type="DAY",
        clustering_fields=["run_id"],
        description="Applied schema version events.",
    ),
    "first_snapshot": TableSpec(
        name="first_snapshot",
        dataset_key="ops",
        fields=FIRST_SNAPSHOT_FIELDS,
        partition_field="set_at",
        partition_type="DAY",
        clustering_fields=["account_id"],
        description=(
            "Account first_snapshot_date, written once and never updated (KTD4)."
        ),
    ),
    "lease_events": TableSpec(
        name="lease_events",
        dataset_key="ops",
        fields=LEASE_EVENTS_FIELDS,
        partition_field="event_ts",
        partition_type="DAY",
        clustering_fields=["run_id"],
        description=(
            "Append-only lease events (ACQUIRED, RENEWED, RELEASED, "
            "TAKEOVER, SKIPPED)."
        ),
    ),
}

def _meta_fields() -> list[SchemaField]:
    return [
        _sf("run_id", "STRING", "REQUIRED"),
        _sf("loaded_at", "TIMESTAMP", "REQUIRED"),
        _sf("query_hash", "STRING", "REQUIRED"),
    ]


def _volume_metrics() -> list[SchemaField]:
    return [
        _sf("impressions", "INT64"),
        _sf("clicks", "INT64"),
        _sf("cost_micros", "INT64"),
        _sf("conversions", "FLOAT"),
        _sf("conversions_value", "FLOAT"),
        _sf("all_conversions", "FLOAT"),
        _sf("all_conversions_value", "FLOAT"),
    ]


def _conv_metrics() -> list[SchemaField]:
    return [
        _sf("conversions", "FLOAT"),
        _sf("conversions_value", "FLOAT"),
        _sf("all_conversions", "FLOAT"),
        _sf("all_conversions_value", "FLOAT"),
    ]


def _raw_spec(
    name: str,
    fields: list[SchemaField],
    partition_field: str,
    clustering_fields: list[str],
    description: str,
) -> TableSpec:
    return TableSpec(
        name=name,
        dataset_key="raw",
        fields=_meta_fields() + fields,
        partition_field=partition_field,
        partition_type="DAY",
        clustering_fields=clustering_fields,
        description=description,
    )


# Families A-D (partition by date or snapshot_date, cluster account_id,
# campaign_id, then the grain key).
RAW_TABLES: dict[str, TableSpec] = {
    "volume_campaign": _raw_spec(
        "volume_campaign",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("campaign_id", "INT64", "REQUIRED"),
            _sf("campaign_name", "STRING"),
            _sf("date", "DATE", "REQUIRED"),
            _sf("ad_network_type", "STRING"),
            *_volume_metrics(),
        ],
        "date",
        ["account_id", "campaign_id"],
        "Family A: PMax volume by network at campaign grain.",
    ),
    "volume_asset_group": _raw_spec(
        "volume_asset_group",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("campaign_id", "INT64", "REQUIRED"),
            _sf("asset_group_id", "INT64", "REQUIRED"),
            _sf("asset_group_name", "STRING"),
            _sf("date", "DATE", "REQUIRED"),
            _sf("ad_network_type", "STRING"),
            *_volume_metrics(),
        ],
        "date",
        ["account_id", "campaign_id", "asset_group_id"],
        "Family A: PMax volume by network at asset-group grain.",
    ),
    "volume_asset": _raw_spec(
        "volume_asset",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("campaign_id", "INT64", "REQUIRED"),
            _sf("asset_group_id", "INT64"),
            _sf("asset_id", "INT64", "REQUIRED"),
            _sf("field_type", "STRING", "REQUIRED"),
            _sf("date", "DATE", "REQUIRED"),
            _sf("ad_network_type", "STRING"),
            *_volume_metrics(),
        ],
        "date",
        ["account_id", "campaign_id", "asset_id", "field_type"],
        "Family A: PMax volume by network at asset-link grain.",
    ),
    "conv_campaign": _raw_spec(
        "conv_campaign",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("campaign_id", "INT64", "REQUIRED"),
            _sf("campaign_name", "STRING"),
            _sf("date", "DATE", "REQUIRED"),
            _sf("ad_network_type", "STRING"),
            _sf("conversion_action", "STRING"),
            _sf("conversion_action_name", "STRING"),
            *_conv_metrics(),
        ],
        "date",
        ["account_id", "campaign_id"],
        "Family B: conversion basis by network and action at campaign grain.",
    ),
    "conv_asset_group": _raw_spec(
        "conv_asset_group",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("campaign_id", "INT64", "REQUIRED"),
            _sf("asset_group_id", "INT64", "REQUIRED"),
            _sf("asset_group_name", "STRING"),
            _sf("date", "DATE", "REQUIRED"),
            _sf("ad_network_type", "STRING"),
            _sf("conversion_action", "STRING"),
            _sf("conversion_action_name", "STRING"),
            *_conv_metrics(),
        ],
        "date",
        ["account_id", "campaign_id", "asset_group_id"],
        "Family B: conversion basis by network and action at asset-group grain.",
    ),
    "conv_asset": _raw_spec(
        "conv_asset",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("campaign_id", "INT64", "REQUIRED"),
            _sf("asset_group_id", "INT64"),
            _sf("asset_id", "INT64", "REQUIRED"),
            _sf("field_type", "STRING", "REQUIRED"),
            _sf("date", "DATE", "REQUIRED"),
            _sf("ad_network_type", "STRING"),
            _sf("conversion_action", "STRING"),
            _sf("conversion_action_name", "STRING"),
            *_conv_metrics(),
        ],
        "date",
        ["account_id", "campaign_id", "asset_id", "field_type"],
        "Family B: conversion basis by network and action at asset-link grain.",
    ),
    "lag_campaign": _raw_spec(
        "lag_campaign",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("campaign_id", "INT64", "REQUIRED"),
            _sf("campaign_name", "STRING"),
            _sf("date", "DATE", "REQUIRED"),
            _sf("ad_network_type", "STRING"),
            _sf("conversion_action", "STRING"),
            _sf("conversion_action_name", "STRING"),
            _sf("conversion_lag_bucket", "STRING"),
            _sf("conversions", "FLOAT"),
            _sf("conversions_value", "FLOAT"),
            _sf("all_conversions", "FLOAT"),
            _sf("all_conversions_value", "FLOAT"),
        ],
        "date",
        ["account_id", "campaign_id"],
        "Family C: conversion lag buckets at campaign grain.",
    ),
    "lag_asset_group": _raw_spec(
        "lag_asset_group",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("campaign_id", "INT64", "REQUIRED"),
            _sf("asset_group_id", "INT64", "REQUIRED"),
            _sf("asset_group_name", "STRING"),
            _sf("date", "DATE", "REQUIRED"),
            _sf("ad_network_type", "STRING"),
            _sf("conversion_action", "STRING"),
            _sf("conversion_action_name", "STRING"),
            _sf("conversion_lag_bucket", "STRING"),
            _sf("conversions", "FLOAT"),
            _sf("conversions_value", "FLOAT"),
            _sf("all_conversions", "FLOAT"),
            _sf("all_conversions_value", "FLOAT"),
        ],
        "date",
        ["account_id", "campaign_id", "asset_group_id"],
        "Family C: conversion lag buckets at asset-group grain.",
    ),
    "entities_campaign": _raw_spec(
        "entities_campaign",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("campaign_id", "INT64", "REQUIRED"),
            _sf("snapshot_date", "DATE", "REQUIRED"),
            _sf("campaign_name", "STRING"),
            _sf("status", "STRING"),
            _sf("primary_status", "STRING"),
            _sf("primary_status_reasons", "STRING", "REPEATED"),
            _sf("advertising_channel_type", "STRING"),
            _record(
                "asset_automation_settings",
                [
                    _sf("asset_automation_type", "STRING"),
                    _sf("asset_automation_status", "STRING"),
                ],
            ),
            _sf("positive_geo_target_type", "STRING"),
            _sf("negative_geo_target_type", "STRING"),
            _sf("start_date_time", "STRING"),
            _sf("end_date_time", "STRING"),
            _sf("budget_id", "INT64"),
            _sf("budget_amount_micros", "INT64"),
            _sf("budget_explicitly_shared", "BOOL"),
            _sf("budget_period", "STRING"),
            # Absent from google-ads v25 GAQL (validate_gaql is the offline
            # gate). NULLABLE BOOL so U4 can compare against a later API pin
            # / pMaximizer url-expansion parity rule.
            _sf("url_expansion_opt_out", "BOOL"),
        ],
        "snapshot_date",
        ["account_id", "campaign_id"],
        "Family D: PMax campaign settings snapshot.",
    ),
    "entities_asset_group": _raw_spec(
        "entities_asset_group",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("campaign_id", "INT64", "REQUIRED"),
            _sf("asset_group_id", "INT64", "REQUIRED"),
            _sf("snapshot_date", "DATE", "REQUIRED"),
            _sf("asset_group_name", "STRING"),
            _sf("status", "STRING"),
            _sf("primary_status", "STRING"),
            _sf("primary_status_reasons", "STRING", "REPEATED"),
            _sf("ad_strength", "STRING"),
            _record(
                "ad_strength_action_items",
                [
                    _sf("action_item_type", "STRING"),
                    _record(
                        "add_asset_details",
                        [
                            _sf("asset_field_type", "STRING"),
                            _sf("asset_count", "INT64"),
                            _sf("video_aspect_ratio_requirement", "STRING"),
                        ],
                        mode="NULLABLE",
                    ),
                ],
            ),
            _sf("final_urls", "STRING", "REPEATED"),
        ],
        "snapshot_date",
        ["account_id", "campaign_id", "asset_group_id"],
        "Family D: asset group attributes snapshot.",
    ),
    "entities_asset_group_asset": _raw_spec(
        "entities_asset_group_asset",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("campaign_id", "INT64", "REQUIRED"),
            _sf("asset_group_id", "INT64", "REQUIRED"),
            _sf("asset_id", "INT64", "REQUIRED"),
            _sf("snapshot_date", "DATE", "REQUIRED"),
            _sf("field_type", "STRING"),
            _sf("status", "STRING"),
            _sf("primary_status", "STRING"),
            _sf("primary_status_reasons", "STRING", "REPEATED"),
            _sf("source", "STRING"),
        ],
        "snapshot_date",
        ["account_id", "campaign_id", "asset_id", "field_type"],
        "Family D: asset-group asset links (no performance_label).",
    ),
    "entities_asset": _raw_spec(
        "entities_asset",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("campaign_id", "INT64"),
            _sf("asset_group_id", "INT64"),
            _sf("asset_id", "INT64", "REQUIRED"),
            _sf("snapshot_date", "DATE", "REQUIRED"),
            _sf("asset_name", "STRING"),
            _sf("asset_type", "STRING"),
            _sf("orientation", "STRING"),
            _sf("text", "STRING"),
            _sf("image_url", "STRING"),
            _sf("image_height_pixels", "INT64"),
            _sf("image_width_pixels", "INT64"),
            _sf("video_id", "STRING"),
            _sf("video_title", "STRING"),
        ],
        "snapshot_date",
        ["account_id", "campaign_id", "asset_id"],
        "Family D: asset attributes snapshot.",
    ),
    "entities_asset_group_signal": _raw_spec(
        "entities_asset_group_signal",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("campaign_id", "INT64"),
            _sf("asset_group_id", "INT64", "REQUIRED"),
            _sf("snapshot_date", "DATE", "REQUIRED"),
            _sf("signal_resource_name", "STRING"),
            _sf("approval_status", "STRING"),
            _sf("audience", "STRING"),
            _sf("search_theme", "STRING"),
        ],
        "snapshot_date",
        ["account_id", "campaign_id", "asset_group_id"],
        "Family D: asset-group audience signals snapshot.",
    ),
    "entities_campaign_asset": _raw_spec(
        "entities_campaign_asset",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("campaign_id", "INT64", "REQUIRED"),
            _sf("asset_id", "INT64"),
            _sf("snapshot_date", "DATE", "REQUIRED"),
            _sf("asset_resource_name", "STRING"),
            _sf("advertising_channel_type", "STRING"),
            _sf("field_type", "STRING"),
            _sf("status", "STRING"),
            _sf("primary_status", "STRING"),
            _sf("primary_status_reasons", "STRING", "REPEATED"),
        ],
        "snapshot_date",
        ["account_id", "campaign_id", "asset_id"],
        "Family D: campaign-level assets (sitelinks and others).",
    ),
    "entities_customer_asset": _raw_spec(
        "entities_customer_asset",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("asset_id", "INT64"),
            _sf("snapshot_date", "DATE", "REQUIRED"),
            _sf("asset_resource_name", "STRING", "REQUIRED"),
            _sf("field_type", "STRING"),
            _sf("status", "STRING"),
            _sf("primary_status", "STRING"),
            _sf("primary_status_reasons", "STRING", "REPEATED"),
        ],
        "snapshot_date",
        ["account_id", "asset_id", "field_type"],
        "Family D: account-level assets needed for sitelink score parity.",
    ),
    "entities_conversion_action": _raw_spec(
        "entities_conversion_action",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("conversion_action_id", "INT64", "REQUIRED"),
            _sf("snapshot_date", "DATE", "REQUIRED"),
            _sf("conversion_action_name", "STRING"),
            _sf("category", "STRING"),
            _sf("counting_type", "STRING"),
            _sf("status", "STRING"),
            _sf("click_through_lookback_window_days", "INT64"),
            _sf("view_through_lookback_window_days", "INT64"),
            _sf("include_in_conversions_metric", "BOOL"),
            _sf("conversion_action_type", "STRING"),
        ],
        "snapshot_date",
        ["account_id", "conversion_action_id"],
        "Family D: conversion action settings including both lookback windows.",
    ),
    "entities_customer": _raw_spec(
        "entities_customer",
        [
            _sf("account_id", "INT64", "REQUIRED"),
            _sf("snapshot_date", "DATE", "REQUIRED"),
            _sf("descriptive_name", "STRING"),
            _sf("currency_code", "STRING"),
            _sf("time_zone", "STRING"),
            _sf("status", "STRING"),
            _sf("manager", "BOOL"),
        ],
        "snapshot_date",
        ["account_id"],
        "Family D: customer currency, timezone, and descriptive name.",
    ),
}

# Append-only observation log (KTD4). Partitioned by observed_date; never
# expire; never rewrite. Seed identity is observed_date = first_snapshot_date
# in the ledger, not a stored flag.
OBSERVATION_TABLE: TableSpec = TableSpec(
    name="raw_observations",
    dataset_key="raw",
    fields=[
        _sf("run_id", "STRING", "REQUIRED"),
        _sf("observed_date", "DATE", "REQUIRED"),
        _sf("account_id", "INT64", "REQUIRED"),
        _sf("click_date", "DATE", "REQUIRED"),
        _sf("lag", "INT64", "REQUIRED"),
        _sf("grain", "STRING", "REQUIRED"),
        _sf("campaign_id", "INT64", "REQUIRED"),
        _sf("asset_group_id", "INT64"),
        _sf("asset_id", "INT64"),
        _sf("field_type", "STRING"),
        _sf("ad_network_type", "STRING"),
        _sf("metric_basis", "STRING", "REQUIRED"),
        _sf("conversion_action", "STRING"),
        _sf("conversion_action_name", "STRING"),
        _sf("conversions", "FLOAT"),
        _sf("conversions_value", "FLOAT"),
    ],
    partition_field="observed_date",
    partition_type="DAY",
    clustering_fields=["account_id", "campaign_id", "asset_id"],
    description=(
        "Append-only observation log: one row set per (observed_date, run_id, "
        "account) as a projection of family A (PRIMARY and ALL_CONVERSIONS) "
        "and family B (CONVERSION_ACTION) raw partitions. Never expire, never "
        "rewrite (KTD4)."
    ),
)
