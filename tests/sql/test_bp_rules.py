"""Rendered production SQL value proofs for pMax best-practice rules."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from pmax_pack.parity import build_fixture_connection, run_fixture_parity_bq
from pmax_pack.runner import load_manifest

PRODUCT_ROOT = Path(__file__).parents[2]


@pytest.fixture(scope="module")
def scored_connection():
    connection = build_fixture_connection()
    try:
        yield connection
    finally:
        connection.close()


def _by_id(connection, table: str, key: str):
    cursor = connection.execute(f"SELECT * FROM {table}")
    names = [column[0] for column in cursor.description]
    return {
        row[names.index(key)]: dict(zip(names, row))
        for row in cursor.fetchall()
    }


def test_campaign_rules_include_url_sitelinks_geo_and_paused(scored_connection) -> None:
    campaigns = _by_id(
        scored_connection, "mart_bp_campaign", "campaign_id"
    )
    assert campaigns[220000001]["url_expansion_score"] == 1.0
    assert campaigns[220000002]["url_expansion_score"] == 0.0
    assert campaigns[220000003]["url_expansion_score"] is None
    assert campaigns[220000003]["url_expansion_parity_mode"] == (
        "PARITY_NEUTRAL_UNKNOWN"
    )
    assert campaigns[220000001]["sitelink_count"] == 4
    assert campaigns[220000001]["sitelink_score"] == 1.0
    assert campaigns[220000002]["sitelink_count"] == 2
    assert campaigns[220000002]["sitelink_score"] == 0.5
    assert campaigns[220000001]["geo_target_score"] == 1.0
    assert campaigns[220000002]["geo_target_score"] == 0.5
    assert campaigns[220000003]["geo_target_score"] == 0.0
    assert campaigns[220000004]["campaign_status"] == "ENABLED"
    assert campaigns[220000005]["campaign_status"] == "PAUSED"
    assert campaigns[220000005]["campaign_bp_score"] == 0.5


def test_asset_group_google_thresholds_follow_pinned_chain(scored_connection) -> None:
    groups = _by_id(
        scored_connection, "mart_bp_asset_group", "asset_group_id"
    )
    at_threshold = groups[330000001]
    below = groups[330000002]
    assert at_threshold["count_videos"] == 3
    assert at_threshold["video_score"] == 1.0
    assert at_threshold["count_headlines"] == 11
    assert at_threshold["count_long_headlines"] == 2
    assert at_threshold["count_descriptions"] == 4
    assert at_threshold["text_score"] == 1.0
    assert at_threshold["count_landscape"] == 4
    assert at_threshold["count_square"] == 4
    assert at_threshold["count_portrait"] == 4
    assert at_threshold["count_square_logos"] == 1
    assert at_threshold["count_landscape_logos"] == 1
    assert at_threshold["image_score"] == 1.0
    assert below["count_videos"] == 2
    assert below["video_score"] == 0.0
    assert below["text_score"] == 0.0
    assert below["image_score"] == 0.0


def test_extended_components_do_not_change_google_parity_score(
    scored_connection,
) -> None:
    groups = _by_id(
        scored_connection, "mart_bp_asset_group", "asset_group_id"
    )
    extended = _by_id(
        scored_connection, "mart_bp_extended", "asset_group_id"
    )
    row = extended[330000002]
    assert row["google_parity_score"] == groups[330000002][
        "asset_group_bp_score"
    ]
    assert row["ad_strength_action_item_count"] == 1
    assert row["action_item_clear_score"] == 0.0
    assert row["asset_primary_status_reason_count"] == 1
    assert row["primary_status_clear_score"] == 0.0
    assert row["covered_feed_type_count"] == 6
    assert row["feed_type_coverage_score"] == 0.75
    assert not row["text_guidelines_present"]
    assert row["text_guidelines_present_score"] == 0.0
    assert row["extended_score"] == 0.19


def test_manifest_orders_all_three_score_marts() -> None:
    manifest = load_manifest(PRODUCT_ROOT / "src" / "pmax_pack" / "manifest.yaml")
    by_name = {step.name: step for step in manifest.steps}
    assert by_name["mart_bp_campaign"].partition_field == "snapshot_date"
    assert "int_entities" in by_name["mart_bp_campaign"].depends_on
    assert by_name["mart_bp_extended"].depends_on == (
        "mart_bp_asset_group",
        "build_mart_entities_asset_group",
        "build_mart_entities_asset",
    )


@pytest.mark.bq_scratch
@pytest.mark.skipif(
    not os.getenv("PMAX_CI_SCRATCH_PROJECT"),
    reason="requires PMAX_CI_SCRATCH_PROJECT and trusted CI credentials",
)
def test_fixture_parity_runs_both_chains_in_trusted_bq_scratch() -> None:
    from google.cloud import bigquery

    project = os.environ["PMAX_CI_SCRATCH_PROJECT"]
    result = run_fixture_parity_bq(
        bq_client=bigquery.Client(project=project),
        project=project,
    )
    assert result.passed, result.to_dict()
