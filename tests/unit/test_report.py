"""Validation report decisions, rendering, and storage paths (R18/R19)."""
from __future__ import annotations

from copy import deepcopy
from datetime import date
import re

import duckdb
import pytest
from sqlglot import transpile

from pmax_pack import cli
from pmax_pack.report import (
    AssumedCurrentMetric,
    AssetParticipationRatio,
    CheckResult,
    CoverageMetric,
    ParityRun,
    ReportInput,
    TableMetric,
    build_report,
    write_report,
)

ACCOUNT = "1234567890"
CANARY_REFRESH = "1/" + "/0canaryCANARY0canaryCANARY0000"
_REFRESH_KEY = "refresh" + "_token"


def _input(**overrides) -> ReportInput:
    base = dict(
        run_id="run-1",
        mode="run",
        deployment="prod",
        as_of=date(2026, 8, 26),
        configured_accounts=[ACCOUNT],
        resolved_accounts=[ACCOUNT],
        image_digest="sha256:abc",
        credential_fingerprint="deadbeef0123",
        query_hash="query-hash",
        api_version="v25",
        reference_commit="reference-commit",
        sql_files_resolved=3,
    )
    base.update(overrides)
    return ReportInput(**base)


def test_ae8_soft_asset_overage_warns_and_exits_zero() -> None:
    report = build_report(
        _input(
            checks=[
                CheckResult(
                    "asset_not_over_campaign",
                    "SOFT",
                    False,
                    "101.01",
                    "100.00",
                    "asset sums exceed campaign truth",
                )
            ]
        )
    )
    assert report.status == "PASS"
    assert report.exit_code == 0
    assert report.markdown.startswith("# PASS: Validation report")
    assert "asset sums exceed campaign truth" in report.markdown


@pytest.mark.parametrize(("dry_run", "expected"), [(True, "yes"), (False, "no")])
def test_run_block_labels_dry_run_without_changing_title(
    dry_run: bool,
    expected: str,
) -> None:
    report = build_report(_input(dry_run=dry_run))
    assert report.markdown.startswith("# PASS: Validation report")
    mode_at = report.markdown.index("- Mode: `run`")
    dry_run_at = report.markdown.index(f"- Dry run: {expected}")
    as_of_at = report.markdown.index("- As of: `2026-08-26`")
    assert mode_at < dry_run_at < as_of_at


def test_ae8_empty_required_table_fails_after_report_is_writable(
    storage_client,
) -> None:
    report = build_report(
        _input(
            checks=[
                CheckResult(
                    "assert_required_tables_nonempty",
                    "HARD",
                    False,
                    1,
                    0,
                    "empty required tables: mart_campaign_truth",
                )
            ],
            tables=[
                TableMetric(
                    "mart_campaign_truth",
                    0,
                    None,
                    date(2026, 8, 26),
                )
            ]
        )
    )
    assert report.status == "FAIL"
    assert report.exit_code == 1
    uri = write_report(storage_client, "report-bucket", report)
    assert uri == "gs://report-bucket/reports/prod/run-1.md"
    assert "reports/prod/run-1.md" in storage_client.store
    assert "reports/prod/latest.md" in storage_client.store


@pytest.mark.parametrize(
    ("overrides", "needle"),
    [
        ({"resolved_accounts": []}, "zero resolved accounts"),
        ({"sql_files_resolved": 0}, "zero SQL files resolved"),
        (
            {"configured_accounts": [ACCOUNT, "9999999999"]},
            "9999999999",
        ),
    ],
)
def test_r19_synthesized_hard_checks(overrides, needle: str) -> None:
    report = build_report(_input(**overrides))
    assert report.status == "FAIL"
    assert report.exit_code == 1
    assert needle in report.markdown


def test_first_run_downgrade_is_reported_from_the_canonical_assertion() -> None:
    report = build_report(
        _input(
            checks=[
                CheckResult(
                    "assert_required_tables_nonempty",
                    "HARD",
                    True,
                    0,
                    0,
                    "all required tables are non-empty or carry the "
                    "first-run downgrade",
                )
            ],
            tables=[
                TableMetric(
                    "mart_performance_campaign",
                    0,
                    None,
                    date(2026, 8, 26),
                )
            ]
        )
    )
    assert report.status == "PASS"
    assert report.exit_code == 0
    assert "first-run downgrade" in report.markdown


def test_legitimately_empty_entity_marts_are_informational() -> None:
    report = build_report(
        _input(
            tables=[
                TableMetric(
                    "mart_entities_asset_group_signal",
                    0,
                    None,
                    date(2026, 8, 26),
                ),
                TableMetric(
                    "mart_entities_campaign_asset",
                    0,
                    None,
                    date(2026, 8, 26),
                ),
            ]
        )
    )
    assert report.status == "PASS"
    assert report.markdown.count("INFO (empty)") == 2


def test_report_surfaces_every_r18_family_and_parity_staleness() -> None:
    report = build_report(
        _input(
            tables=[
                TableMetric(
                    "mart_cohort_campaign",
                    2,
                    date(2026, 8, 26),
                    date(2026, 8, 26),
                )
            ],
            checks=[
                CheckResult("campaign_reconciliation", "SOFT", True, 0, 0),
                CheckResult("cross_grain_identity", "SOFT", True, 0, 0),
                CheckResult("snapshot_bucket", "SOFT", True, 0, 0),
                CheckResult("cohort_reconciliation", "SOFT", True, 0, 0),
                CheckResult("family_coherence", "HARD", True, 0, 0),
            ],
            unknown_lag=[{"account_id": ACCOUNT, "basis": "PRIMARY", "share": 0.1}],
            coverage=[CoverageMetric("measured", "complete", 8, 10, 0.8)],
            assumed_current=[AssumedCurrentMetric(ACCOUNT, 2, 10, 0.2)],
            asset_participation=[
                AssetParticipationRatio(
                    ACCOUNT,
                    "DISCOVER",
                    "conversions",
                    30.0,
                    10.0,
                    3.0,
                )
            ],
            snapshot_gaps=["2026-08-24 account 1234567890"],
            stale_cells=["campaign 7 D30 observed through 2026-08-20"],
            frozen_chunks=["2022-01"],
            null_cost_cells=["campaign 7 D7"],
            anomalies=["serving campaign 7 has budget and zero cost"],
            crashed_runs=["run-crashed"],
            parity=ParityRun(
                run_date=date(2026, 8, 25),
                result="PASS",
                image_digest="sha256:old",
                query_hash="query-hash",
                api_version="v25",
                reference_commit="reference-commit",
            ),
        )
    )
    for heading in (
        "Row counts and freshness",
        "Assertion outcomes",
        "Unknown-lag share",
        "Cohort coverage",
        "Assumed-current share by account",
        "Asset participation ratios (informational)",
        "Snapshot gaps and stale cells",
        "Frozen chunks",
        "NULL-cost cells",
        "Parity",
        "Crashed runs",
        "Anomalies",
    ):
        assert heading in report.markdown
    assert "STALE" in report.markdown
    assert f"account={ACCOUNT}, cells=2/10, share=0.200000" in report.markdown
    assert (
        f"account={ACCOUNT}, network=DISCOVER, metric=conversions, "
        "asset_sum=30.000000, campaign_truth=10.000000, ratio=3.000000"
        in report.markdown
    )
    assert report.status == "PASS"


def test_asset_participation_ratio_sql_reports_sum_without_affecting_verdict() -> None:
    con = duckdb.connect()
    con.execute(
        """
CREATE TABLE mart_campaign_truth (
  date DATE, account_id BIGINT, ad_network_type VARCHAR,
  conversions DOUBLE, conversions_value DOUBLE,
  all_conversions DOUBLE, all_conversions_value DOUBLE
);
CREATE TABLE mart_asset_performance (
  date DATE, account_id BIGINT, ad_network_type VARCHAR, metric_basis VARCHAR,
  network_conversions DOUBLE, network_conversions_value DOUBLE,
  network_all_conversions DOUBLE, network_all_conversions_value DOUBLE
);
INSERT INTO mart_campaign_truth VALUES
  (DATE '2026-08-26', 1234567890, 'DISCOVER', 10, 20, 12, 24);
INSERT INTO mart_asset_performance VALUES
  (DATE '2026-08-26', 1234567890, 'DISCOVER', 'NETWORK', 10, 20, 12, 24),
  (DATE '2026-08-26', 1234567890, 'DISCOVER', 'NETWORK', 10, 20, 12, 24),
  (DATE '2026-08-26', 1234567890, 'DISCOVER', 'NETWORK', 10, 20, 12, 24);
"""
    )
    sql = cli._asset_participation_sql("fixture-project", "pmax_marts")
    sql = re.sub(
        r"`[^`]+\.([A-Za-z_][A-Za-z0-9_]*)`",
        lambda match: f'"{match.group(1)}"',
        sql,
    ).replace("@as_of", "DATE '2026-08-26'")
    rendered = transpile(sql, read="bigquery", write="duckdb")[0]
    rows = con.execute(rendered).fetchall()
    assert {row[2]: float(row[5]) for row in rows} == {
        "all_conversions": 3.0,
        "all_conversions_value": 3.0,
        "conversions": 3.0,
        "conversions_value": 3.0,
    }

    mutant = rendered.replace(
        "SUM(network_conversions) AS conversions",
        "MAX(network_conversions) AS conversions",
    )
    assert mutant != rendered
    mutant_rows = con.execute(mutant).fetchall()
    with pytest.raises(AssertionError):
        assert {row[2]: float(row[5]) for row in mutant_rows}["conversions"] == 3.0


def test_report_redacts_canary_and_self_mutation_flips_verdict() -> None:
    canary_blob = f"{_REFRESH_KEY}: {CANARY_REFRESH}"
    report = build_report(
        _input(
            anomalies=[canary_blob],
            checks=[CheckResult("duplicate_keys", "HARD", True, 0, 0)],
        )
    )
    assert canary_blob not in report.markdown
    assert "<redacted:refresh_token>" in report.markdown

    mutated = deepcopy(report.source)
    mutated.checks = [CheckResult("duplicate_keys", "HARD", False, 1, 0)]
    failed = build_report(mutated)
    assert report.exit_code == 0
    assert failed.exit_code == 1


def test_skipped_report_exits_zero_and_rebuild_never_updates_latest(
    storage_client,
) -> None:
    skipped = build_report(_input(skipped_reason="lease held"))
    assert skipped.status == "SKIPPED"
    assert skipped.exit_code == 0
    assert "lease held" in skipped.markdown
    write_report(storage_client, "report-bucket", skipped)
    assert "reports/prod/run-1.md" in storage_client.store
    assert "reports/prod/latest.md" not in storage_client.store

    executed = build_report(_input(run_id="run-2"))
    write_report(storage_client, "report-bucket", executed)
    assert "reports/prod/latest.md" in storage_client.store

    rebuild = build_report(_input(mode="rebuild", deployment="verify"))
    write_report(storage_client, "report-bucket", rebuild)
    assert "reports/verify/run-1.md" in storage_client.store
    assert "reports/verify/latest.md" not in storage_client.store


def test_repeated_report_write_replaces_the_same_run_object(
    storage_client,
) -> None:
    report = build_report(_input())
    write_report(storage_client, "report-bucket", report)
    write_report(storage_client, "report-bucket", report)

    changed = build_report(_input(anomalies=["changed after publication"]))
    write_report(storage_client, "report-bucket", changed)
    assert "changed after publication" in storage_client.store[
        "reports/prod/run-1.md"
    ]["data"]


def test_seeded_assumed_current_cells_render_account_share_line() -> None:
    con = duckdb.connect()
    for table in (
        "mart_cohort_campaign",
        "mart_cohort_asset_group",
        "mart_cohort_asset",
    ):
        con.execute(
            f"CREATE TABLE {table} (click_date DATE, account_id BIGINT, "
            "window_provenance VARCHAR)"
        )
    con.execute(
        "INSERT INTO mart_cohort_campaign VALUES "
        "(DATE '2026-08-26', 1234567890, 'assumed-current'), "
        "(DATE '2026-08-26', 1234567890, 'observed')"
    )
    con.execute(
        "INSERT INTO mart_cohort_asset_group VALUES "
        "(DATE '2026-08-26', 1234567890, 'assumed-current')"
    )
    sql = cli._assumed_current_sql("fixture-project", "pmax_marts")
    sql = re.sub(
        r"`[^`]+\.([A-Za-z_][A-Za-z0-9_]*)`",
        lambda match: f'"{match.group(1)}"',
        sql,
    )
    sql = sql.replace("@window_start", "DATE '2026-08-01'")
    sql = sql.replace("@as_of", "DATE '2026-08-26'")
    rendered = transpile(sql, read="bigquery", write="duckdb")[0]
    row = con.execute(rendered).fetchone()
    assert row is not None
    report = build_report(
        _input(
            assumed_current=[
                AssumedCurrentMetric(
                    str(row[0]),
                    int(row[1]),
                    int(row[2]),
                    float(row[3]),
                )
            ]
        )
    )
    assert "account=1234567890, cells=2/3, share=0.666667" in report.markdown
