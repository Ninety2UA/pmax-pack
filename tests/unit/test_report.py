"""Validation report decisions, rendering, and storage paths (R18/R19)."""
from __future__ import annotations

from collections import Counter
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
    retention_check,
    retention_metadata_check,
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
    assert "| asset_not_over_campaign | SOFT | WARN |" in report.markdown


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


def _budget(**overrides) -> dict:
    # Event times are the fixture's independent observations. The renderer must
    # preserve the derived measurements; it does not reconcile clocks itself.
    entered, run_started, stages_finished, snapshot = 11.0, 15.0, 140.0, 142.0
    values = {
        "startup_seconds": run_started - entered,
        "pre_lease_seconds": 1.0,
        "stage_span_seconds": stages_finished - run_started,
        "stage_seconds": {"load": 30.0, "report": 2.0},
        "stage_jobs": {"load": 4, "report": 3},
        "tail_seconds": snapshot - stages_finished,
        "total_seconds": snapshot - entered,
        "load_path_jobs": 4,
        "total_jobs": 9,
        "rows_loaded": 10000,
    }
    values.update(overrides)
    return values


@pytest.mark.parametrize("failed", [False, True])
def test_budget_on_pass_and_fail_is_informational(failed: bool) -> None:
    checks = [CheckResult("required", "HARD", not failed, 1, 0)]
    ordinary = build_report(_input(checks=checks))
    report = build_report(_input(checks=checks, budget=_budget()))
    assert (report.status, report.exit_code) == (
        ordinary.status, ordinary.exit_code
    )
    markdown = report.markdown
    assert markdown.index("## Run") < markdown.index("### Budget")
    assert markdown.index("### Budget") < markdown.index("## Accounts")
    assert "- Startup: 4.000 s" in markdown
    assert "  - Pre-lease calls: 1.000 s" in markdown
    assert "| load | 30.000 s | 4 |" in markdown
    assert "| report | 2.000 s | 3 |" in markdown
    assert "- Stage span: 125.000 s" in markdown
    assert "informational target: 120 s; over target" in markdown
    assert "- Tail: 2.000 s" in markdown
    assert "- Process total: 131.000 s" in markdown
    spans = [
        float(re.search(rf"- {label}: ([\d.]+) s", markdown).group(1))
        for label in ("Startup", "Stage span", "Tail", "Process total")
    ]
    assert spans == [4.0, 125.0, 2.0, 131.0]
    assert "- Load-path jobs: 4" in markdown
    assert "- Total jobs: 9 (submissions)" in markdown.splitlines()
    assert "- Rows loaded: 10,000" in markdown
    assert "final report snapshot; final upload excluded" in markdown
    if failed:
        assert "| required | HARD | FAIL |" in markdown


def test_skipped_report_omits_budget_even_with_measurements() -> None:
    report = build_report(_input(skipped_reason="lease held", budget=_budget()))
    assert report.status == "SKIPPED" and report.exit_code == 0
    assert "### Budget" not in report.markdown
    assert "Stage span:" not in report.markdown


def test_budget_missing_metrics_are_unavailable_not_zero() -> None:
    report = build_report(_input(budget={"stage_seconds": {"load": 0.0}}))
    assert "- Startup: unavailable" in report.markdown
    assert "- Startup jobs: unavailable" in report.markdown
    assert "- Stage span: unavailable" in report.markdown
    span_line = next(
        line for line in report.markdown.splitlines()
        if line.startswith("- Stage span:")
    )
    assert "within target" not in span_line and "over target" not in span_line
    assert "- Tail: unavailable" in report.markdown
    assert "- Tail jobs: unavailable" in report.markdown
    assert "- Process total: unavailable" in report.markdown
    assert "- Load-path jobs: unavailable" in report.markdown
    assert "- Total jobs: unavailable" in report.markdown
    assert "- Rows loaded: unavailable" in report.markdown
    assert "| load | 0.000 s | unavailable |" in report.markdown
    assert "### Budget" in build_report(_input()).markdown


def test_budget_preserves_independently_recorded_job_counts() -> None:
    accepted = (
        "startup", "startup", "load", "load", "load", "load",
        "report", "report", "report", "tail", "tail", "tail",
    )
    measured = Counter(accepted)
    report = build_report(
        _input(budget=_budget(
            startup_jobs=measured["startup"],
            tail_jobs=measured["tail"],
            stage_jobs={name: measured[name] for name in ("load", "report")},
            total_jobs=len(accepted),
        ))
    )
    markdown = report.markdown
    assert "- Startup jobs: 2 (including pre-lease calls)" in markdown
    assert "- Tail jobs: 3" in markdown
    assert "CLI process entry" in markdown
    assert "| load | 30.000 s | 4 |" in markdown
    assert "| report | 2.000 s | 3 |" in markdown
    counts = {
        label: int(re.search(rf"- {label} jobs: (\d+)", markdown).group(1))
        for label in ("Startup", "Tail", "Total")
    }
    assert counts == {"Startup": 2, "Tail": 3, "Total": 12}
    stage_jobs = {
        name: int(jobs) for name, jobs in re.findall(
            r"^\| (\w+) \| [\d.]+ s \| (\d+) \|$", markdown, re.MULTILINE
        )
    }
    assert stage_jobs == {"load": 4, "report": 3}


@pytest.mark.parametrize(
    ("seconds", "expected", "excluded"),
    [(120.0, "within target", "over target"),
     (120.001, "over target", "within target")],
)
def test_stage_span_target_boundary_is_informational(
    seconds: float, expected: str, excluded: str
) -> None:
    report = build_report(_input(budget=_budget(stage_span_seconds=seconds)))
    line = next(
        line for line in report.markdown.splitlines()
        if line.startswith("- Stage span:")
    )
    assert f"- Stage span: {seconds:.3f} s" in line
    assert f"informational target: 120 s; {expected}" in line
    assert excluded not in line
    assert report.status == "PASS" and report.exit_code == 0


@pytest.mark.parametrize("complete", [False, True])
@pytest.mark.parametrize("failed", [False, True])
def test_explicit_snapshot_marker_is_independent_of_decision(
    complete: bool, failed: bool
) -> None:
    report = build_report(_input(
        budget=_budget(snapshot_complete=complete),
        handled_error="fixture failure" if failed else None,
    ))
    assert ("Preliminary snapshot" in report.markdown) is (not complete)
    assert ("final report snapshot" in report.markdown) is complete
    assert report.status == ("FAIL" if failed else "PASS")


def test_explicit_unknown_spans_and_counters_render_unavailable() -> None:
    unknown = {
        name: None for name in (
            "startup_seconds", "pre_lease_seconds", "stage_span_seconds",
            "tail_seconds", "total_seconds", "startup_jobs", "tail_jobs",
            "load_path_jobs", "total_jobs", "rows_loaded",
        )
    }
    report = build_report(_input(
        handled_error="fixture bootstrap failure",
        budget={**unknown, "snapshot_complete": True},
    ))
    budget = report.markdown.split("### Budget")[1].split("## Accounts")[0]
    assert "within target" not in budget and "over target" not in budget
    assert "0.000 s" not in budget
    for label in (
        "Startup", "Pre-lease calls", "Stage span", "Tail", "Process total",
        "Startup jobs", "Tail jobs", "Load-path jobs", "Total jobs", "Rows loaded",
    ):
        assert f"- {label}: unavailable" in budget
    assert report.status == "FAIL" and report.exit_code == 1


@pytest.mark.parametrize("line_count", [0, 2000])
def test_report_body_line_count_is_exact_and_non_gating(line_count: int) -> None:
    report = build_report(
        _input(budget=_budget(), anomalies=["fixture row"] * line_count)
    )
    counted = re.search(r"Report body lines: (\d+)", report.markdown)
    assert counted is not None
    assert int(counted.group(1)) == len(report.markdown.splitlines())
    assert "informational target: <2,000 lines" in report.markdown
    assert report.status == "PASS" and report.exit_code == 0


def test_ten_thousand_gap_cells_render_group_counts_and_fifty_examples() -> None:
    report = build_report(
        _input(
            budget=_budget(),
            gap_summary=[
                {"grain": "campaign", "reason": "snapshot gap: missing",
                 "month": "2026-08", "cells": 6000},
                {"grain": "asset", "reason": "stale cell",
                 "month": "2026-08", "cells": 4000},
            ],
            gap_examples=[f"gap fixture {index}" for index in range(10000)],
            snapshot_gaps=["duplicated legacy gap"],
            stale_cells=["duplicated legacy stale"],
        )
    )
    section = report.markdown.split("## Snapshot gaps and stale cells\n")[1]
    section = section.split("## Frozen chunks")[0]
    assert "| campaign | snapshot gap: missing | 2026-08 | 6,000 |" in section
    assert "| asset | stale cell | 2026-08 | 4,000 |" in section
    assert len([line for line in section.splitlines() if line.startswith("- ")]) == 50
    assert "gap fixture 49" in section and "gap fixture 50" not in section
    assert "duplicated legacy" not in section
    assert len(report.markdown.splitlines()) < 2000
    assert report.status == "PASS" and report.exit_code == 0


def test_legacy_gap_examples_share_one_fifty_line_bound() -> None:
    report = build_report(
        _input(
            snapshot_gaps=[f"missing {index}" for index in range(30)],
            stale_cells=[f"late {index}" for index in range(30)],
        )
    )
    assert report.markdown.count("- snapshot gap:") == 30
    assert report.markdown.count("- stale cell:") == 20
    assert "stale cell: late 20" not in report.markdown


def test_retention_drift_renders_warn_and_equal_is_silent() -> None:
    check = retention_check(["raw fixture: configured=31, live=60"])
    assert check is not None
    report = build_report(_input(checks=[check]))
    assert "| retention_drift | SOFT | WARN | 1 | 0 |" in report.markdown
    assert "retention_drift:" in report.markdown.split("## Warnings")[1]
    assert report.status == "PASS" and report.exit_code == 0
    assert retention_check([]) is None
    assert "retention_drift" not in build_report(_input()).markdown


def test_collector_error_text_cannot_inject_report_rows_or_lines() -> None:
    hostile = "unavailable | blocked\n\n## injected\r\n| bogus | bogus |"
    credential_text = f"{_REFRESH_KEY}=fixture-private-value"
    metadata = retention_metadata_check([hostile + " " + credential_text])
    assert metadata is not None
    assert metadata.detail is not None
    assert "fixture-private-value" not in metadata.detail
    assert f"{_REFRESH_KEY}=<redacted:refresh_token>" in metadata.detail
    assert "\n" not in metadata.detail
    assert "unavailable \\| blocked" in metadata.detail
    report = build_report(
        _input(
            handled_error=hostile,
            checks=[CheckResult("collector", "SOFT", False, 1, 0, hostile), metadata],
            tables=[TableMetric("fixture", 0, None, None, hostile)],
            anomalies=[hostile + " " + credential_text],
            snapshot_gaps=[hostile],
        )
    )
    assert "\n## injected" not in report.markdown
    assert "\n| bogus" not in report.markdown
    assert (
        "unavailable \\| blocked ## injected \\| bogus \\| bogus \\|"
        in report.markdown
    )
    assert "\\\\|" not in report.markdown
    assert "fixture-private-value" not in report.markdown
    for line in report.markdown.splitlines():
        if line.startswith("| collector") or line.startswith("| retention_metadata"):
            assert len(re.split(r"(?<!\\)\|", line)) == 8
        if line.startswith("| fixture"):
            assert len(re.split(r"(?<!\\)\|", line)) == 7


def test_budget_refresh_keeps_a_newer_latest_report(storage_client) -> None:
    from dataclasses import replace

    old = build_report(_input(run_id="old-run"))
    newer = build_report(_input(run_id="new-run"))
    write_report(storage_client, "report-bucket", old)
    write_report(storage_client, "report-bucket", newer)
    refreshed = build_report(replace(old.source, budget={"total_jobs": 7}))
    write_report(
        storage_client, "report-bucket", refreshed, previous_markdown=old.markdown,
    )
    assert storage_client.store[old.object_name]["data"] == refreshed.markdown
    latest = f"reports/{old.deployment}/latest.md"
    assert storage_client.store[latest]["data"] == newer.markdown


def test_budget_refresh_uses_generation_guard_after_read(storage_client, monkeypatch) -> None:
    from dataclasses import replace
    from conftest import FakeBlob

    old = build_report(_input(run_id="old-run"))
    newer = build_report(_input(run_id="new-run"))
    write_report(storage_client, "report-bucket", old)
    refreshed = build_report(replace(old.source, budget={"total_jobs": 7}))
    original_upload = FakeBlob.upload_from_string
    latest = f"reports/{old.deployment}/latest.md"

    def race(blob, data, *args, **kwargs):
        if blob.name == latest and data == refreshed.markdown:
            original_upload(blob, newer.markdown, content_type="text/markdown")
        return original_upload(blob, data, *args, **kwargs)

    monkeypatch.setattr(FakeBlob, "upload_from_string", race)
    write_report(
        storage_client, "report-bucket", refreshed, previous_markdown=old.markdown,
    )
    assert storage_client.store[latest]["data"] == newer.markdown
