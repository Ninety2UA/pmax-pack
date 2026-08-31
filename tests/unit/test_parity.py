"""Parity harness unit proofs, including deliberate red cases."""
from __future__ import annotations

import builtins
import copy
import importlib
import json
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import pmax_pack.parity as parity
from pmax_pack.config import DEFAULT_TOLERANCE_PARITY, parse_config
from pmax_pack.parity import (
    GAARF_JS_DATE_EXPRESSION,
    PARITY_API_VERSION,
    ParityError,
    ScoreRow,
    audit_reference_rewrites,
    compare_scores,
    derive_url_expansion_opt_out,
    reference_query_hash,
    rewrite_reference_bq,
    run_fixture_parity,
    validate_reference_gaql,
)

PRODUCT_ROOT = Path(__file__).parents[2]


def _campaign(score: str, campaign_id: int = 1, **extra):
    return {
        "entity_type": "campaign",
        "account_id": 1110001110,
        "campaign_id": campaign_id,
        "asset_group_id": None,
        "metric": "campaign_bp_score",
        "score": Decimal(score),
        **extra,
    }


def test_seeded_mismatch_goes_red_and_names_entity_and_scores() -> None:
    result = run_fixture_parity(seeded_mismatch=True)
    assert not result.passed
    assert len(result.mismatches) == 1
    mismatch = result.mismatches[0]
    assert mismatch.campaign_id == 220000001
    assert mismatch.google_score == Decimal("1.02")
    assert mismatch.our_score == Decimal("1.0")


def test_fixture_source_runs_both_chains_green_with_explicit_url_difference() -> None:
    result = run_fixture_parity()
    assert result.passed
    assert result.matched_metrics == 36
    assert result.google_entities == result.our_enabled_entities == 12
    assert result.paused_excluded == 3
    assert result.ours_only_entities == []
    assert len(result.rule_differences) == 1
    assert "220000003" in result.rule_differences[0]
    assert "Google raw=0.5" in result.rule_differences[0]


def test_unknown_url_missing_difference_flips_verdict_and_cli_exit(
    monkeypatch, capsys
) -> None:
    monkeypatch.delenv("PMAX_CI_SCRATCH_PROJECT", raising=False)
    monkeypatch.setattr(
        parity,
        "_url_rule_difference_line",
        lambda *_args: "unrelated rule-difference",
        raising=False,
    )
    result = run_fixture_parity()
    assert not result.passed
    assert result.mismatches == []
    assert parity.cli_main(source="fixtures", account=None, run_date=None) == 1
    capsys.readouterr()


def test_frozen_tolerance_exact_boundary_passes_one_unit_beyond_fails() -> None:
    exact = compare_scores(
        [_campaign("1.00")],
        [_campaign("0.99")],
        tolerance=Decimal("0.01"),
    )
    beyond = compare_scores(
        [_campaign("1.00")],
        [_campaign("0.989999")],
        tolerance=Decimal("0.01"),
    )
    assert exact.passed
    assert not beyond.passed
    assert beyond.mismatches[0].difference == Decimal("0.010001")


def test_population_gate_fails_google_missing_and_reports_ours_only() -> None:
    result = compare_scores(
        [_campaign("1.0", campaign_id=1)],
        [_campaign("1.0", campaign_id=2)],
    )
    assert not result.passed
    assert result.google_missing_in_ours == [
        ("campaign", 1110001110, 1, None)
    ]
    assert result.ours_only_entities == [("campaign", 1110001110, 2, None)]


@pytest.mark.parametrize(
    ("settings", "expected"),
    [
        (
            [
                {
                    "asset_automation_type": (
                        "FINAL_URL_EXPANSION_TEXT_ASSET_AUTOMATION"
                    ),
                    "asset_automation_status": "OPTED_OUT",
                }
            ],
            True,
        ),
        (
            [
                {
                    "asset_automation_type": (
                        "FINAL_URL_EXPANSION_TEXT_ASSET_AUTOMATION"
                    ),
                    "asset_automation_status": "OPTED_IN",
                }
            ],
            False,
        ),
    ],
)
def test_gaql_compat_rewrite_derives_same_some_boolean(settings, expected) -> None:
    assert derive_url_expansion_opt_out(settings) is expected


def test_reference_rewrite_set_and_newest_proto_pin_are_offline_valid() -> None:
    assert PARITY_API_VERSION == "v25"
    assert audit_reference_rewrites() == {
        "bq": [GAARF_JS_DATE_EXPRESSION],
        "gaql": [
            "`some(campaign.asset_automation_settings, f(s) = "
            "equalText(s.asset_automation_type, "
            "'FINAL_URL_EXPANSION_TEXT_ASSET_AUTOMATION') and "
            "equalText(s.asset_automation_status,'OPTED_OUT'))`"
        ],
    }
    assert validate_reference_gaql("v24") == []
    assert validate_reference_gaql("v25") == []
    assert reference_query_hash() == (
        "239226e4370d2e3f1f9d59473bca03ebdc3b52e0313200a3c0eeb719d9040528"
    )


def test_rules_document_carries_every_fixture_branch_id() -> None:
    rules = (parity.REFERENCE_ROOT / "RULES.md").read_text(encoding="utf-8")
    for branch in sorted(parity.REQUIRED_RULE_BRANCHES):
        assert branch in rules, branch


def _fixture_payload() -> dict:
    return json.loads(
        (PRODUCT_ROOT / "tests" / "fixtures" / "parity" / "source.json")
        .read_text(encoding="utf-8")
    )


def test_fixture_coverage_is_derived_from_enabled_fixture_rows() -> None:
    payload = copy.deepcopy(_fixture_payload())
    for campaign in payload["campaigns"]:
        for group in campaign["asset_groups"]:
            if group["asset_group_id"] == 330000007:
                group["ad_strength"] = "AVERAGE"
    with pytest.raises(ParityError, match="GOOD"):
        parity._assert_fixture_coverage(payload)


def test_fixture_coverage_rejects_an_extra_declared_branch() -> None:
    payload = copy.deepcopy(_fixture_payload())
    payload["coverage"].append("asset_group.ad_strength.SUPERB")
    with pytest.raises(ParityError, match="SUPERB"):
        parity._assert_fixture_coverage(payload)


def test_ours_fixture_carries_source_and_status_filter_traps() -> None:
    payload = _fixture_payload()
    _google, ours = parity._fixture_rows(payload)
    traps = {
        row["asset_id"]: row
        for row in ours["mart_entities_asset"]
        if row["asset_id"] in {9900000001, 9900000002}
    }
    assert traps[9900000001]["source"] == "AUTOMATICALLY_CREATED"
    assert traps[9900000001]["status"] == "ENABLED"
    assert traps[9900000002]["source"] == "ADVERTISER"
    assert traps[9900000002]["status"] == "PAUSED"


def test_example_config_pins_the_frozen_parity_tolerance() -> None:
    raw = yaml.safe_load(
        (PRODUCT_ROOT / "config" / "example.yaml").read_text(encoding="utf-8")
    )
    parsed = parse_config(raw, run_date=date(2026, 8, 26))
    assert parsed.tolerances.parity == DEFAULT_TOLERANCE_PARITY == 0.01


def test_date_rewrite_is_fixed_and_rejects_new_template_expression() -> None:
    rewritten = rewrite_reference_bq(
        f"CREATE TABLE x_{GAARF_JS_DATE_EXPRESSION} AS SELECT 1",
        date(2026, 8, 25),
    )
    assert "x_20260825" in rewritten
    with pytest.raises(ParityError, match="unrecognized"):
        rewrite_reference_bq("SELECT ${new_runtime_value}", date(2026, 8, 25))


def test_scratch_cleanup_runs_when_comparison_raises(monkeypatch) -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.deleted: list[str] = []

        def list_tables(self, dataset_id: str):
            return [SimpleNamespace(table_id=f"table_{dataset_id.rsplit('.', 1)[-1]}")]

        def delete_table(self, target, not_found_ok: bool = False) -> None:
            assert not_found_ok
            self.deleted.append(str(target))

    class FakeLedger:
        def stage_finished(
            self, run_id, stage, status, account_id, detail, error, now=None
        ) -> None:
            raise AssertionError("comparison error must precede the ledger write")

    config = SimpleNamespace(
        deployment=SimpleNamespace(project="fixture-project"),
        datasets=SimpleNamespace(
            parity_scratch="pmax_parity_scratch",
            parity_scratch_bq="pmax_parity_scratch_bq",
            marts="pmax_marts",
        ),
        tolerances=SimpleNamespace(parity=0.01),
    )
    client = FakeClient()
    monkeypatch.setattr(parity, "_ensure_scratch_dataset", lambda *a, **k: None)
    monkeypatch.setattr(
        parity,
        "load_google_inputs_live",
        lambda **kwargs: kwargs["created_tables"].add(
            "table_pmax_parity_scratch"
        ),
    )
    monkeypatch.setattr(
        parity,
        "execute_google_chain_live",
        lambda **kwargs: kwargs["created_tables"].add(
            "table_pmax_parity_scratch_bq"
        ),
    )
    monkeypatch.setattr(parity, "_live_score_rows", lambda *a, **k: ([], [], 0))

    def explode(*args, **kwargs):
        raise RuntimeError("seeded compare failure")

    with pytest.raises(RuntimeError, match="seeded compare failure"):
        parity.run_live_parity(
            config=config,
            bq_client=client,
            ledger=FakeLedger(),
            account="1110001110",
            run_date=date(2026, 8, 25),
            image_digest="sha256:fixture",
            compare_fn=explode,
        )
    assert client.deleted == [
        "fixture-project.pmax_parity_scratch.table_pmax_parity_scratch",
        "fixture-project.pmax_parity_scratch_bq.table_pmax_parity_scratch_bq",
    ]


def test_cleanup_scratch_refuses_non_scratch_dataset_before_any_delete() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.deleted: list[str] = []

        def delete_table(self, target, not_found_ok: bool = False) -> None:
            self.deleted.append(str(target))

    client = FakeClient()
    with pytest.raises(ParityError, match="pmax_marts"):
        parity.cleanup_scratch(
            client,
            "fixture-project",
            {
                "pmax_parity_scratch": {"assetgroupasset"},
                "pmax_marts": {"mart_campaign_daily"},
            },
        )

    assert client.deleted == []


def test_cleanup_scratch_refuses_legacy_dataset_sequence() -> None:
    class FakeClient:
        def list_tables(self, dataset_id: str):
            raise AssertionError("legacy sequence reached table enumeration")

        def delete_table(self, target, not_found_ok: bool = False) -> None:
            raise AssertionError("legacy sequence reached deletion")

    with pytest.raises(ParityError, match="created_tables must be a mapping"):
        parity.cleanup_scratch(
            FakeClient(),
            "fixture-project",
            ("pmax_ci_scratch", "pmax_ci_scratch_bq"),
        )


def test_cleanup_scratch_deletes_only_tables_recorded_by_current_run() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.tables = {
                "fixture-project.pmax_parity_scratch.assetgroupasset",
                "fixture-project.pmax_parity_scratch.unrelated_table",
                "fixture-project.pmax_parity_scratch_bq.image_assets",
                "fixture-project.pmax_parity_scratch_bq.unrelated_output",
            }

        def delete_table(self, target, not_found_ok: bool = False) -> None:
            assert not_found_ok
            self.tables.discard(str(target))

        def list_tables(self, dataset_id: str):
            prefix = f"{dataset_id}."
            return [
                SimpleNamespace(table_id=table.removeprefix(prefix))
                for table in sorted(self.tables)
                if table.startswith(prefix)
            ]

    client = FakeClient()
    parity.cleanup_scratch(
        client,
        "fixture-project",
        {
            "pmax_parity_scratch": {"assetgroupasset"},
            "pmax_parity_scratch_bq": {"image_assets"},
        },
    )

    assert client.tables == {
        "fixture-project.pmax_parity_scratch.unrelated_table",
        "fixture-project.pmax_parity_scratch_bq.unrelated_output",
    }


def test_cleanup_scratch_empty_sets_never_fall_back_to_dataset_enumeration() -> None:
    class FakeClient:
        def list_tables(self, dataset_id: str):
            raise AssertionError("empty-set cleanup enumerated pre-existing tables")

        def delete_table(self, target, not_found_ok: bool = False) -> None:
            raise AssertionError("empty-set cleanup deleted a pre-existing table")

    parity.cleanup_scratch(
        FakeClient(),
        "fixture-project",
        {
            "pmax_parity_scratch": set(),
            "pmax_parity_scratch_bq": set(),
        },
    )


def test_existing_scratch_dataset_is_updated_to_24_hour_expiration() -> None:
    existing = SimpleNamespace(default_table_expiration_ms=6 * 60 * 60 * 1000)

    class FakeClient:
        def __init__(self) -> None:
            self.updated = []

        def get_dataset(self, dataset_id: str):
            assert dataset_id == "fixture-project.pmax_ci_scratch"
            return existing

        def update_dataset(self, dataset, fields):
            self.updated.append((dataset, fields))

    client = FakeClient()
    parity._ensure_scratch_dataset(
        client, "fixture-project", "pmax_ci_scratch"
    )
    assert existing.default_table_expiration_ms == 24 * 60 * 60 * 1000
    assert client.updated == [(existing, ["default_table_expiration_ms"])]


def test_existing_bq_sibling_scratch_dataset_is_accepted() -> None:
    existing = SimpleNamespace(default_table_expiration_ms=None)

    class FakeClient:
        def __init__(self) -> None:
            self.updated = []

        def get_dataset(self, dataset_id: str):
            assert dataset_id == "fixture-project.pmax_ci_scratch_bq"
            return existing

        def update_dataset(self, dataset, fields):
            self.updated.append((dataset, fields))

    client = FakeClient()
    parity._ensure_scratch_dataset(
        client, "fixture-project", "pmax_ci_scratch_bq"
    )
    assert existing.default_table_expiration_ms == 24 * 60 * 60 * 1000
    assert client.updated == [(existing, ["default_table_expiration_ms"])]


def test_ensure_scratch_dataset_refuses_non_scratch_name_before_api_call() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def get_dataset(self, dataset_id: str):
            self.calls.append(dataset_id)
            return SimpleNamespace(default_table_expiration_ms=None)

    client = FakeClient()
    with pytest.raises(ParityError, match="pmax_marts"):
        parity._ensure_scratch_dataset(
            client,
            "fixture-project",
            "pmax_marts",
        )

    assert client.calls == []


def test_held_lease_never_cleans_another_parity_runs_tables(monkeypatch) -> None:
    class HeldLease:
        holder = {
            "holder": "pmax-pack",
            "mode": "parity",
            "expires_at": "2026-08-25T13:00:00+00:00",
        }
        observed_generation = 4
        crashed_run = None
        generation = None

        def acquire(self, run_id, mode, now) -> bool:
            return False

    class RecordingLedger:
        def __init__(self) -> None:
            self.events = []

        def lease_event(
            self, run_id, event, holder, mode, expires_at, generation,
            prior_run_id, now=None
        ) -> None:
            self.events.append(event)

    config = SimpleNamespace(
        deployment=SimpleNamespace(project="fixture-project"),
        datasets=SimpleNamespace(
            parity_scratch="pmax_parity_scratch",
            parity_scratch_bq="pmax_parity_scratch_bq",
            marts="pmax_marts",
        ),
        tolerances=SimpleNamespace(parity=0.01),
    )
    cleaned = []
    monkeypatch.setattr(
        parity,
        "cleanup_scratch",
        lambda *args, **kwargs: cleaned.append(True),
    )
    ledger = RecordingLedger()
    with pytest.raises(ParityError, match="lease held"):
        parity.run_live_parity(
            config=config,
            bq_client=object(),
            ledger=ledger,
            lease=HeldLease(),
            account="1110001110",
            run_date=date(2026, 8, 25),
            image_digest="sha256:fixture",
        )
    assert ledger.events == ["SKIPPED"]
    assert cleaned == []


def test_fixture_source_never_calls_live_or_credential_paths(monkeypatch) -> None:
    monkeypatch.setattr(
        parity,
        "load_google_inputs_live",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("credential path")),
    )
    monkeypatch.setattr(
        parity,
        "_live_score_rows",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pmax_marts")),
    )
    assert run_fixture_parity().passed


def test_live_comparison_queries_parse_as_bigquery(monkeypatch) -> None:
    import sqlglot

    seen = []

    def parse_only(client, sql, params):
        sqlglot.parse(sql, read="bigquery")
        seen.append(sql)
        if "paused_excluded" in sql:
            return [{"paused_excluded": 0}]
        return []

    monkeypatch.setattr(parity, "_query_rows", parse_only)
    google, ours, paused = parity._live_score_rows(
        object(),
        project="fixture-project",
        marts_dataset="pmax_marts",
        google_dataset="pmax_parity_scratch_bq",
        account=1110001110,
        run_date=date(2026, 8, 25),
    )
    assert google == []
    assert ours == []
    assert paused == 0
    assert len(seen) == 5


def test_live_google_chain_uses_lazy_gaarf_bq_in_pinned_order(monkeypatch) -> None:
    from gaarf.executors import bq_executor

    calls = []

    class FakeExecutor:
        def __init__(self, project_id: str, location: str) -> None:
            assert project_id == "fixture-project"
            assert location == "EU"
            self._client = None
            self.preprocessors = {"init": object()}

        def execute(self, script_name: str, query_text: str, params=None):
            calls.append((script_name, query_text, params))

    monkeypatch.setattr(bq_executor, "BigQueryExecutor", FakeExecutor)
    created_tables: set[str] = set()
    parity.execute_google_chain_live(
        bq_client=object(),
        project="fixture-project",
        input_dataset="pmax_parity_scratch",
        output_dataset="pmax_parity_scratch_bq",
        run_date=date(2026, 8, 25),
        created_tables=created_tables,
    )
    assert [name for name, _text, _params in calls] == list(
        parity.GOOGLE_BQ_CHAIN
    )
    assert all("${" not in text for _name, text, _params in calls)
    assert calls[-2][1].find("campaignbpscore_20260825") >= 0
    assert all(
        params == {
            "macro": {
                "bq_dataset": "fixture-project.pmax_parity_scratch"
            }
        }
        for _name, _text, params in calls
    )
    assert created_tables == {
        "image_assets",
        "primary_conversion_action_pmax",
        "primary_conversion_action_search",
        "text_assets",
        "video_assets",
        "campaign_data",
        "campaignbpscore_20260825",
        "assetgroupbestpractices",
    }


def test_live_google_input_load_records_only_successful_current_run_tables(
    monkeypatch,
) -> None:
    from gaarf import report_fetcher

    from pmax_pack import ads_client, loader

    monkeypatch.setattr(ads_client, "build_client", lambda *args: object())
    monkeypatch.setattr(
        report_fetcher,
        "AdsReportFetcher",
        lambda api_client: object(),
    )
    monkeypatch.setattr(ads_client, "fetch_family", lambda *args: [])
    loaded: list[str] = []
    monkeypatch.setattr(
        loader,
        "load_rows",
        lambda _client, target, *_args: loaded.append(target),
    )
    created_tables: set[str] = set()

    parity.load_google_inputs_live(
        bq_client=object(),
        project="fixture-project",
        dataset="pmax_parity_scratch",
        account="1110001110",
        run_date=date(2026, 8, 25),
        created_tables=created_tables,
    )

    assert created_tables == set(parity.GOOGLE_GAQL_TABLES)
    assert loaded == [
        f"fixture-project.pmax_parity_scratch.{name}"
        for name in parity.GOOGLE_GAQL_TABLES
    ]


def test_module_import_survives_missing_gaarf_bq_and_pandas(monkeypatch) -> None:
    original_import = builtins.__import__
    package = sys.modules["pmax_pack"]

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "gaarf.executors.bq_executor" or name.startswith("pandas"):
            raise ImportError("simulated dev dependency absence")
        return original_import(name, globals, locals, fromlist, level)

    existing = sys.modules.pop("pmax_pack.parity")
    monkeypatch.setattr(builtins, "__import__", guarded_import)
    try:
        imported = importlib.import_module("pmax_pack.parity")
        assert imported.PARITY_API_VERSION == "v25"
        with pytest.raises(imported.ParityError, match="dev dependency"):
            imported.execute_google_chain_live(
                bq_client=object(),
                project="fixture-project",
                input_dataset="pmax_parity_scratch",
                output_dataset="pmax_parity_scratch_bq",
                run_date=date(2026, 8, 25),
            )
    finally:
        sys.modules["pmax_pack.parity"] = existing
        setattr(package, "parity", existing)


def test_pandas_is_dev_only_dependency() -> None:
    import tomllib

    pyproject = tomllib.loads(
        (PRODUCT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )
    assert "pandas" in pyproject["dependency-groups"]["dev"]
    assert all(
        not dependency.startswith("pandas")
        for dependency in pyproject["project"]["dependencies"]
    )


def test_cli_wiring_is_one_call(monkeypatch) -> None:
    from argparse import Namespace

    from pmax_pack import cli

    calls = []
    monkeypatch.setattr(
        parity,
        "cli_main",
        lambda **kwargs: calls.append(kwargs) or 7,
    )
    assert cli._parity(
        Namespace(source="fixtures", account=None, date=None)
    ) == 7
    assert calls == [{"source": "fixtures", "account": None, "run_date": None}]


def test_trusted_fixture_path_names_u7_concurrency_serialization() -> None:
    source = (PRODUCT_ROOT / "src" / "pmax_pack" / "parity.py").read_text(
        encoding="utf-8"
    )
    function = source[source.index("def run_fixture_parity_bq(") :]
    function = function[: function.index("def run_fixture_parity(")]
    assert "trusted.yml" in function
    assert "concurrency group" in function


def test_fixture_coverage_requires_the_full_required_branch_set() -> None:
    payload = copy.deepcopy(_fixture_payload())
    payload["campaigns"] = [
        campaign
        for campaign in payload["campaigns"]
        if campaign["campaign_id"] != 220000004
    ]
    payload["coverage"] = sorted(parity._derive_fixture_coverage(payload))
    with pytest.raises(ParityError, match="GOOD"):
        parity._assert_fixture_coverage(payload)


def test_fixture_coverage_rejects_a_derived_branch_outside_the_required_set() -> None:
    payload = copy.deepcopy(_fixture_payload())
    for campaign in payload["campaigns"]:
        for group in campaign["asset_groups"]:
            if group["asset_group_id"] == 330000007:
                group["ad_strength"] = "SUPERB"
    payload["coverage"] = sorted(parity._derive_fixture_coverage(payload))
    with pytest.raises(ParityError, match="fixture coverage drift"):
        parity._assert_fixture_coverage(payload)


def test_new_scratch_dataset_is_created_with_24_hour_expiration() -> None:
    from google.api_core.exceptions import NotFound

    class FakeClient:
        def __init__(self) -> None:
            self.created = []

        def get_dataset(self, dataset_id: str):
            raise NotFound(dataset_id)

        def create_dataset(self, dataset, exists_ok=False):
            self.created.append((dataset, exists_ok))

    client = FakeClient()
    parity._ensure_scratch_dataset(client, "fixture-project", "pmax_ci_scratch")
    ((dataset, exists_ok),) = client.created
    assert exists_ok is True
    assert dataset.default_table_expiration_ms == 24 * 60 * 60 * 1000
    assert dataset.location == "EU"


def test_url_rule_difference_line_renders_a_zero_raw_score() -> None:
    row = ScoreRow(
        entity_type="campaign",
        account_id=1,
        campaign_id=2,
        metric="parity_score",
        score=Decimal("1"),
        raw_score=Decimal("0"),
        url_expansion_known=False,
    )
    line = parity._url_rule_difference_line(row, Decimal("1"))
    assert "Google raw=0," in line
