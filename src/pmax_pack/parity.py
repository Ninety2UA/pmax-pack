"""Pinned pMaximizer parity harness for live and synthetic fixture sources.

The module has no pandas or gaarf-bq import at import time. Google's BigQuery
executor is loaded only by the live reference-chain path. Fixture mode uses
rendered production SQL plus a documented DuckDB rendering of the byte-pinned
reference chain, and never constructs a credential or reads pmax_marts.
"""
from __future__ import annotations

import hashlib
import json
import operator
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Iterable, Mapping, Sequence

from pmax_pack.config import DEFAULT_TOLERANCE_PARITY, SCRATCH_DATASET_SUFFIXES

REFERENCE_COMMIT = "9790e8a585b6e6f76851efed3e9b42ad87d8d97c"
PARITY_API_VERSION = "v25"
SCRATCH_TABLE_EXPIRATION_MS = 24 * 60 * 60 * 1000
REFERENCE_ROOT = Path(__file__).parent / "reference" / "pmaximizer"
FIXTURE_ROOT = Path(__file__).parents[2] / "tests" / "fixtures" / "parity"
MANIFEST_PATH = Path(__file__).parent / "manifest.yaml"

GOOGLE_GAQL_TABLES = (
    "assetgroupasset",
    "assetgroupsummary",
    "assetgroupsignal",
    "campaign_settings",
    "campaignasset",
    "customerasset",
    "ocid_mapping",
    "conversion_category",
    "conversion_custom",
    "custom_goal_names",
)
GOOGLE_BQ_CHAIN = (
    "01-image_assets.sql",
    "02-primary_conversion_action_pmax.sql",
    "03-primary_conversion_action_search.sql",
    "04-text_assets.sql",
    "05-video_assets.sql",
    "07-campaign_data.sql",
    "09-bpscore.sql",
    "10-assetgroupbestpractices.sql",
)
GOOGLE_BQ_OUTPUT_TABLES = {
    "01-image_assets.sql": "image_assets",
    "02-primary_conversion_action_pmax.sql": (
        "primary_conversion_action_pmax"
    ),
    "03-primary_conversion_action_search.sql": (
        "primary_conversion_action_search"
    ),
    "04-text_assets.sql": "text_assets",
    "05-video_assets.sql": "video_assets",
    "07-campaign_data.sql": "campaign_data",
    "09-bpscore.sql": "campaignbpscore_{date}",
    "10-assetgroupbestpractices.sql": "assetgroupbestpractices",
}

GAARF_JS_URL_EXPRESSION = (
    "`some(campaign.asset_automation_settings, f(s) = "
    "equalText(s.asset_automation_type, "
    "'FINAL_URL_EXPANSION_TEXT_ASSET_AUTOMATION') and "
    "equalText(s.asset_automation_status,'OPTED_OUT'))` as "
    "url_expansion_opt_out"
)
GAARF_PYTHON_URL_REPLACEMENT = (
    "campaign.asset_automation_settings AS asset_automation_settings"
)
GAARF_JS_DATE_EXPRESSION = "${format(today(),'yyyyMMdd')}"

REQUIRED_RULE_BRANCHES = frozenset(
    {
        "campaign.url.opted_out",
        "campaign.url.opted_in",
        "campaign.url.unknown",
        "campaign.audience.present",
        "campaign.audience.absent",
        "campaign.sitelinks.below_4",
        "campaign.sitelinks.at_4",
        "campaign.geo.both_presence_or_interest",
        "campaign.geo.mixed",
        "campaign.geo.neither_presence_or_interest",
        "asset_group.video.below_3",
        "asset_group.video.at_3",
        "asset_group.text.descriptions.below_4",
        "asset_group.text.descriptions.at_4",
        "asset_group.text.headlines.below_11",
        "asset_group.text.headlines.at_11",
        "asset_group.text.long_headlines.below_2",
        "asset_group.text.long_headlines.at_2",
        "asset_group.image.landscape.below_4",
        "asset_group.image.landscape.at_4",
        "asset_group.image.square.below_4",
        "asset_group.image.square.at_4",
        "asset_group.image.portrait.below_4",
        "asset_group.image.portrait.at_4",
        "asset_group.image.square_logo.absent",
        "asset_group.image.square_logo.present",
        "asset_group.image.landscape_logo.absent",
        "asset_group.image.landscape_logo.present",
        "asset_group.ad_strength.UNSPECIFIED",
        "asset_group.ad_strength.UNKNOWN",
        "asset_group.ad_strength.PENDING",
        "asset_group.ad_strength.NO_ADS",
        "asset_group.ad_strength.POOR",
        "asset_group.ad_strength.AVERAGE",
        "asset_group.ad_strength.GOOD",
        "asset_group.ad_strength.EXCELLENT",
    }
)


class ParityError(RuntimeError):
    """Raised when the harness cannot produce an honest parity verdict."""


@dataclass(frozen=True)
class ScoreRow:
    """One comparable score metric for a campaign or asset group."""

    entity_type: str
    account_id: int
    campaign_id: int
    metric: str
    score: Decimal
    asset_group_id: int | None = None
    raw_score: Decimal | None = None
    score_without_url: Decimal | None = None
    url_expansion_known: bool | None = None

    @property
    def entity_key(self) -> tuple[str, int, int, int | None]:
        return (
            self.entity_type,
            self.account_id,
            self.campaign_id,
            self.asset_group_id,
        )

    @property
    def metric_key(self) -> tuple[str, int, int, int | None, str]:
        return (*self.entity_key, self.metric)


@dataclass(frozen=True)
class ScoreMismatch:
    entity_type: str
    account_id: int
    campaign_id: int
    asset_group_id: int | None
    metric: str
    google_score: Decimal | None
    our_score: Decimal | None
    difference: Decimal | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "entity_type": self.entity_type,
            "account_id": self.account_id,
            "campaign_id": self.campaign_id,
            "asset_group_id": self.asset_group_id,
            "metric": self.metric,
            "google_score": _decimal_json(self.google_score),
            "our_score": _decimal_json(self.our_score),
            "difference": _decimal_json(self.difference),
        }


@dataclass
class ParityResult:
    passed: bool
    tolerance: Decimal
    matched_metrics: int
    google_entities: int
    our_enabled_entities: int
    ours_only_entities: list[tuple[str, int, int, int | None]] = field(
        default_factory=list
    )
    google_missing_in_ours: list[tuple[str, int, int, int | None]] = field(
        default_factory=list
    )
    mismatches: list[ScoreMismatch] = field(default_factory=list)
    rule_differences: list[str] = field(default_factory=list)
    paused_excluded: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "tolerance": str(self.tolerance),
            "matched_metrics": self.matched_metrics,
            "google_entities": self.google_entities,
            "our_enabled_entities": self.our_enabled_entities,
            "ours_only_entities": [list(key) for key in self.ours_only_entities],
            "google_missing_in_ours": [
                list(key) for key in self.google_missing_in_ours
            ],
            "mismatches": [item.to_dict() for item in self.mismatches],
            "rule_differences": list(self.rule_differences),
            "paused_excluded": self.paused_excluded,
        }


def _decimal_json(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _decimal(value: Any) -> Decimal:
    if value is None:
        raise ParityError("score is NULL")
    return Decimal(str(value))


def _score_row(row: Mapping[str, Any]) -> ScoreRow:
    return ScoreRow(
        entity_type=str(row["entity_type"]),
        account_id=int(row["account_id"]),
        campaign_id=int(row["campaign_id"]),
        asset_group_id=(
            None
            if row.get("asset_group_id") is None
            else int(row["asset_group_id"])
        ),
        metric=str(row["metric"]),
        score=_decimal(row["score"]),
        raw_score=(
            None if row.get("raw_score") is None else _decimal(row["raw_score"])
        ),
        score_without_url=(
            None
            if row.get("score_without_url") is None
            else _decimal(row["score_without_url"])
        ),
        url_expansion_known=(
            None
            if row.get("url_expansion_known") is None
            else bool(row["url_expansion_known"])
        ),
    )


def _url_rule_difference_marker(row: ScoreRow) -> str:
    return f"campaign {row.account_id}/{row.campaign_id}:"


def _url_rule_difference_line(
    row: ScoreRow, comparable_google_score: Decimal
) -> str:
    raw = row.score if row.raw_score is None else row.raw_score
    return (
        f"{_url_rule_difference_marker(row)} "
        "URL expansion unavailable under production API v25; "
        f"Google raw={raw}, comparable_without_url={comparable_google_score}"
    )


def compare_scores(
    google_rows: Iterable[Mapping[str, Any] | ScoreRow],
    our_rows: Iterable[Mapping[str, Any] | ScoreRow],
    *,
    tolerance: float | Decimal = DEFAULT_TOLERANCE_PARITY,
    paused_excluded: int = 0,
) -> ParityResult:
    """Compare enabled populations and scores at an inclusive frozen boundary."""
    tol = _decimal(tolerance)
    if tol < 0:
        raise ValueError("tolerance must be non-negative")
    google = [row if isinstance(row, ScoreRow) else _score_row(row) for row in google_rows]
    ours = [row if isinstance(row, ScoreRow) else _score_row(row) for row in our_rows]
    google_by_metric = {row.metric_key: row for row in google}
    ours_by_metric = {row.metric_key: row for row in ours}
    if len(google_by_metric) != len(google):
        raise ParityError("duplicate Google score metric key")
    if len(ours_by_metric) != len(ours):
        raise ParityError("duplicate local score metric key")

    google_entities = {row.entity_key for row in google}
    our_entities = {row.entity_key for row in ours}
    missing = sorted(google_entities - our_entities)
    ours_only = sorted(our_entities - google_entities)
    mismatches: list[ScoreMismatch] = []
    rule_differences: list[str] = []
    required_rule_difference_markers: set[str] = set()
    matched = 0

    for key in sorted(google_by_metric):
        google_row = google_by_metric[key]
        our_row = ours_by_metric.get(key)
        if our_row is None:
            if google_row.entity_key not in missing:
                mismatches.append(
                    _mismatch(google_row, None, None)
                )
            continue
        comparable_google_score = google_row.score
        if (
            google_row.entity_type == "campaign"
            and our_row.url_expansion_known is False
        ):
            if google_row.score_without_url is None:
                raise ParityError(
                    "unknown local URL-expansion rule lacks Google non-URL score"
                )
            comparable_google_score = google_row.score_without_url
            required_rule_difference_markers.add(
                _url_rule_difference_marker(google_row)
            )
            rule_differences.append(
                _url_rule_difference_line(google_row, comparable_google_score)
            )
        difference = abs(comparable_google_score - our_row.score)
        if difference <= tol:
            matched += 1
        else:
            mismatches.append(
                _mismatch(google_row, our_row, difference, comparable_google_score)
            )

    missing_rule_differences = {
        marker
        for marker in required_rule_difference_markers
        if not any(
            isinstance(line, str) and line.startswith(marker)
            for line in rule_differences
        )
    }
    result = ParityResult(
        passed=not missing and not mismatches and not missing_rule_differences,
        tolerance=tol,
        matched_metrics=matched,
        google_entities=len(google_entities),
        our_enabled_entities=len(our_entities),
        ours_only_entities=ours_only,
        google_missing_in_ours=missing,
        mismatches=mismatches,
        rule_differences=rule_differences,
        paused_excluded=paused_excluded,
    )
    return result


def _mismatch(
    google: ScoreRow,
    ours: ScoreRow | None,
    difference: Decimal | None,
    google_score: Decimal | None = None,
) -> ScoreMismatch:
    return ScoreMismatch(
        entity_type=google.entity_type,
        account_id=google.account_id,
        campaign_id=google.campaign_id,
        asset_group_id=google.asset_group_id,
        metric=google.metric,
        google_score=google.score if google_score is None else google_score,
        our_score=None if ours is None else ours.score,
        difference=difference,
    )


def derive_url_expansion_opt_out(settings: Any) -> bool:
    """Implement Google's pinned some(...) expression over composite rows."""
    for item in settings or []:
        if isinstance(item, Mapping):
            kind = item.get("asset_automation_type")
            status = item.get("asset_automation_status")
        else:
            kind = getattr(item, "asset_automation_type", None)
            status = getattr(item, "asset_automation_status", None)
        kind_name = getattr(kind, "name", kind)
        status_name = getattr(status, "name", status)
        if (
            str(kind_name) == "FINAL_URL_EXPANSION_TEXT_ASSET_AUTOMATION"
            and str(status_name) == "OPTED_OUT"
        ):
            return True
    return False


def rewrite_google_gaql(name: str, text: str) -> str:
    """Adapt the one gaarf-JS composite expression for Python gaarf."""
    if name != "campaign_settings":
        return text
    count = text.count(GAARF_JS_URL_EXPRESSION)
    if count != 1:
        raise ParityError(
            "campaign_settings.sql rewrite drift: expected one pinned "
            f"gaarf-JS expression, found {count}"
        )
    return text.replace(GAARF_JS_URL_EXPRESSION, GAARF_PYTHON_URL_REPLACEMENT)


def rewrite_reference_bq(text: str, run_date: date) -> str:
    """Replace the pinned gaarf-JS date expression before gaarf-bq."""
    count = text.count(GAARF_JS_DATE_EXPRESSION)
    if count:
        if count != 1:
            raise ParityError("unexpected repeated gaarf-JS date expression")
        text = text.replace(GAARF_JS_DATE_EXPRESSION, run_date.strftime("%Y%m%d"))
    if re.search(r"\$\{[^}]+\}", text):
        raise ParityError("unrecognized gaarf-JS template expression")
    return text


def audit_reference_rewrites(root: Path = REFERENCE_ROOT) -> dict[str, list[str]]:
    """Fail F4-style re-sync when new runtime expressions appear upstream."""
    bq_expressions: list[str] = []
    for path in sorted((root / "bq_queries").glob("*.sql")):
        bq_expressions.extend(re.findall(r"\$\{[^}]+\}", path.read_text()))
    gaql_expressions: list[str] = []
    for path in sorted((root / "google_ads_queries").glob("*.sql")):
        gaql_expressions.extend(re.findall(r"`[^`]+`", path.read_text()))
    expected_bq = [GAARF_JS_DATE_EXPRESSION]
    expected_gaql = [GAARF_JS_URL_EXPRESSION.split(" as ", 1)[0]]
    if sorted(bq_expressions) != sorted(expected_bq):
        raise ParityError(
            f"reference BigQuery rewrite set drifted: {sorted(bq_expressions)}"
        )
    if sorted(gaql_expressions) != sorted(expected_gaql):
        raise ParityError(
            f"reference GAQL rewrite set drifted: {sorted(gaql_expressions)}"
        )
    return {"bq": bq_expressions, "gaql": gaql_expressions}


def validate_reference_gaql(
    api_version: str = PARITY_API_VERSION,
    root: Path = REFERENCE_ROOT,
) -> list[str]:
    """Validate every pinned input field against installed offline protos."""
    from gaarf.api_clients import BaseClient
    from gaarf.exceptions import GaarfException
    from gaarf.query_editor import QuerySpecification

    client = BaseClient(api_version)
    errors: list[str] = []
    macros = {"start_date": "2026-08-25", "end_date": "2026-08-25"}
    for name in GOOGLE_GAQL_TABLES:
        path = root / "google_ads_queries" / f"{name}.sql"
        text = rewrite_google_gaql(name, path.read_text(encoding="utf-8"))
        try:
            spec = QuerySpecification(
                text=text,
                title=name,
                args={"macro": macros},
                api_version=api_version,
            ).generate()
        except GaarfException as exc:
            errors.append(f"{name}.sql: parse error: {exc}")
            continue
        for field_name in spec.fields or []:
            try:
                operator.attrgetter(field_name)(client.google_ads_row)
            except AttributeError:
                errors.append(f"{name}.sql: unknown {api_version} field {field_name}")
    return errors


def reference_query_hash(root: Path = REFERENCE_ROOT) -> str:
    digest = hashlib.sha256()
    for folder, names in (
        ("google_ads_queries", [f"{name}.sql" for name in GOOGLE_GAQL_TABLES]),
        ("bq_queries", list(GOOGLE_BQ_CHAIN)),
    ):
        for name in names:
            digest.update((root / folder / name).read_bytes())
    return digest.hexdigest()


_INPUT_COLUMNS: dict[str, tuple[tuple[str, str], ...]] = {
    "assetgroupasset": (
        ("date", "DATE"), ("asset_group_id", "INT64"),
        ("asset_group_name", "STRING"), ("asset_group_status", "STRING"),
        ("campaign_id", "INT64"), ("campaign_name", "STRING"),
        ("asset_id", "INT64"), ("account_id", "INT64"),
        ("account_name", "STRING"), ("asset_type", "STRING"),
        ("asset_sub_type", "STRING"), ("image_file_size", "INT64"),
        ("image_url", "STRING"), ("text_asset_text", "STRING"),
        ("image_height", "INT64"), ("image_width", "INT64"),
        ("video_id", "STRING"), ("video_title", "STRING"),
        ("cost", "FLOAT64"), ("impressions", "INT64"),
        ("clicks", "INT64"), ("conversions", "FLOAT64"),
        ("conversions_value", "FLOAT64"),
    ),
    "assetgroupsummary": (
        ("date", "DATE"), ("asset_group_id", "INT64"),
        ("asset_group_name", "STRING"), ("asset_group_status", "STRING"),
        ("campaign_id", "INT64"), ("campaign_name", "STRING"),
        ("account_id", "INT64"), ("account_name", "STRING"),
        ("ad_strength", "STRING"), ("clicks", "INT64"),
        ("conversions", "FLOAT64"), ("all_conversions", "FLOAT64"),
        ("cost", "INT64"), ("conversions_value", "FLOAT64"),
        ("impressions", "INT64"), ("ctr", "FLOAT64"),
        ("value_per_all_conversions", "FLOAT64"),
        ("value_per_conversion", "FLOAT64"),
        ("all_conversions_value", "FLOAT64"),
    ),
    "assetgroupsignal": (
        ("audience_signals", "STRING"), ("campaign_id", "INT64"),
        ("asset_group_id", "INT64"),
    ),
    "campaign_settings": (
        ("date", "DATE"), ("account_id", "INT64"),
        ("account_name", "STRING"), ("campaign_id", "INT64"),
        ("campaign_name", "STRING"), ("campaign_status", "STRING"),
        ("url_expansion_opt_out", "BOOL"), ("bidding_strategy", "STRING"),
        ("budget_amount", "INT64"), ("total_budget", "INT64"),
        ("budget_type", "STRING"), ("is_shared_budget", "BOOL"),
        ("budget_period", "STRING"), ("bidding_strategy_mcv_troas", "FLOAT64"),
        ("bidding_strategy_troas", "FLOAT64"),
        ("bidding_strategy_mc_tcpa", "INT64"),
        ("bidding_strategy_tcpa", "INT64"), ("campaign_mc_tcpa", "INT64"),
        ("campaign_tcpa", "INT64"), ("campaign_mcv_troas", "FLOAT64"),
        ("campaign_troas", "FLOAT64"), ("gmc_id", "INT64"),
        ("optiscore", "FLOAT64"), ("negative_geo_target_type", "STRING"),
        ("positive_geo_target_type", "STRING"), ("cost", "INT64"),
        ("conversions", "FLOAT64"), ("conversions_value", "FLOAT64"),
        ("all_conversions_value", "FLOAT64"),
    ),
    "campaignasset": (
        ("account_name", "STRING"), ("campaign_id", "INT64"),
        ("account_id", "INT64"), ("campaign_name", "STRING"),
        ("campaign_type", "STRING"), ("campaign_status", "STRING"),
        ("asset_type", "STRING"),
    ),
    "customerasset": (
        ("account_name", "STRING"), ("campaign_id", "INT64"),
        ("account_id", "INT64"), ("asset", "STRING"),
        ("asset_type", "STRING"), ("asset_primary_status", "STRING"),
        ("asset_status", "STRING"),
    ),
    "ocid_mapping": (("customer_id", "INT64"), ("ocid", "STRING")),
    "conversion_category": (
        ("campaign_id", "INT64"), ("account_id", "INT64"),
        ("conversion_name", "STRING"), ("campaign_type", "STRING"),
    ),
    "conversion_custom": (
        ("campaign_id", "INT64"), ("custom_conversion_goal_id", "INT64"),
        ("account_id", "INT64"), ("campaign_type", "STRING"),
    ),
    "custom_goal_names": (
        ("account_id", "INT64"), ("custom_conversion_goal_id", "INT64"),
        ("custom_conversion_goal_name", "STRING"),
    ),
}


def _report_rows(report: Any) -> list[dict[str, Any]]:
    if report is None:
        return []
    if isinstance(report, list):
        return [dict(row) for row in report]
    to_list = getattr(report, "to_list", None)
    if callable(to_list):
        return [dict(row) for row in to_list(row_type="dict")]
    return []


def _project_google_rows(name: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    columns = [column for column, _kind in _INPUT_COLUMNS[name]]
    out: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        if name == "campaign_settings":
            row["url_expansion_opt_out"] = derive_url_expansion_opt_out(
                row.pop("asset_automation_settings", [])
            )
        out.append({column: row.get(column) for column in columns})
    return out


def _ensure_scratch_dataset(client: Any, project: str, dataset: str) -> None:
    if not dataset.endswith(SCRATCH_DATASET_SUFFIXES):
        raise ParityError(
            f"scratch dataset name must end with _scratch or _scratch_bq: {dataset}"
        )
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    dataset_id = f"{project}.{dataset}"
    try:
        existing = client.get_dataset(dataset_id)
    except NotFound:
        existing = bigquery.Dataset(dataset_id)
        existing.location = "EU"
        existing.default_table_expiration_ms = SCRATCH_TABLE_EXPIRATION_MS
        client.create_dataset(existing, exists_ok=True)
        return
    if (
        getattr(existing, "default_table_expiration_ms", None)
        != SCRATCH_TABLE_EXPIRATION_MS
    ):
        existing.default_table_expiration_ms = SCRATCH_TABLE_EXPIRATION_MS
        client.update_dataset(existing, ["default_table_expiration_ms"])


def load_google_inputs_live(
    *,
    bq_client: Any,
    project: str,
    dataset: str,
    account: str,
    run_date: date,
    credential_path: str | None = None,
    api_version: str = PARITY_API_VERSION,
    created_tables: set[str] | None = None,
) -> None:
    """Run pinned GAQL through gaarf and land exact table names via load_rows."""
    from google.cloud.bigquery import SchemaField
    from gaarf.report_fetcher import AdsReportFetcher

    from pmax_pack.ads_client import build_client, fetch_family
    from pmax_pack.loader import load_rows

    ads_client = build_client(credential_path, api_version)
    fetcher = AdsReportFetcher(api_client=ads_client)
    macros = {"start_date": run_date.isoformat(), "end_date": run_date.isoformat()}
    for name in GOOGLE_GAQL_TABLES:
        path = REFERENCE_ROOT / "google_ads_queries" / f"{name}.sql"
        query = rewrite_google_gaql(name, path.read_text(encoding="utf-8"))
        report = fetch_family(fetcher, query, account, macros)
        rows = _project_google_rows(name, _report_rows(report))
        schema = [SchemaField(column, kind) for column, kind in _INPUT_COLUMNS[name]]
        load_rows(
            bq_client,
            f"{project}.{dataset}.{name}",
            rows,
            schema,
            run_date,
            "WRITE_TRUNCATE",
        )
        if created_tables is not None:
            created_tables.add(name)


def execute_google_chain_live(
    *,
    bq_client: Any,
    project: str,
    input_dataset: str,
    output_dataset: str,
    run_date: date,
    created_tables: set[str] | None = None,
) -> None:
    """Execute the byte-pinned chain through gaarf-bq, imported lazily."""
    if output_dataset != f"{input_dataset}_bq":
        raise ParityError(
            "pinned chain requires output scratch dataset to be input_dataset + '_bq'"
        )
    try:
        from gaarf.executors.bq_executor import BigQueryExecutor
    except ImportError as exc:
        raise ParityError(
            "gaarf-bq requires the dev dependency group (including pandas)"
        ) from exc
    executor = BigQueryExecutor(project_id=project, location="EU")
    executor._client = bq_client
    executor.preprocessors = {}
    params = {"macro": {"bq_dataset": f"{project}.{input_dataset}"}}
    for name in GOOGLE_BQ_CHAIN:
        text = (REFERENCE_ROOT / "bq_queries" / name).read_text(encoding="utf-8")
        executor.execute(name, rewrite_reference_bq(text, run_date), params)
        if created_tables is not None:
            created_tables.add(
                GOOGLE_BQ_OUTPUT_TABLES[name].format(
                    date=run_date.strftime("%Y%m%d")
                )
            )


def cleanup_scratch(
    client: Any,
    project: str,
    created_tables: Mapping[str, Iterable[str]],
) -> None:
    """Drop only current-run tables from explicitly named scratch datasets."""
    if not isinstance(created_tables, Mapping):
        raise ParityError(
            "created_tables must be a mapping of scratch dataset names "
            "to current-run table names"
        )
    invalid = [
        dataset
        for dataset in created_tables
        if not dataset.endswith(SCRATCH_DATASET_SUFFIXES)
    ]
    if invalid:
        raise ParityError(
            "scratch cleanup refused non-scratch dataset: "
            + ", ".join(sorted(invalid))
        )
    failures: list[str] = []
    for dataset, tables in created_tables.items():
        dataset_id = f"{project}.{dataset}"
        for table in sorted(set(tables)):
            target = f"{dataset_id}.{table}"
            try:
                client.delete_table(target, not_found_ok=True)
            except Exception as exc:
                failures.append(f"{target}: delete failed: {exc}")
    if failures:
        raise ParityError("scratch cleanup failed: " + "; ".join(failures))


def _query_rows(client: Any, sql: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
    from google.cloud import bigquery

    query_parameters = []
    for name, value in params.items():
        kind = "DATE" if isinstance(value, date) else "INT64"
        query_parameters.append(bigquery.ScalarQueryParameter(name, kind, value))
    config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    rows = client.query(sql, job_config=config).result()
    return [dict(row.items()) if hasattr(row, "items") else dict(row) for row in rows]


def _live_score_rows(
    client: Any,
    *,
    project: str,
    marts_dataset: str,
    google_dataset: str,
    account: int,
    run_date: date,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    suffix = run_date.strftime("%Y%m%d")
    google_campaigns = _query_rows(
        client,
        f"""
SELECT 'campaign' AS entity_type, b.account_id, b.campaign_id,
  CAST(NULL AS INT64) AS asset_group_id, 'campaign_bp_score' AS metric,
  b.campaign_bp_score AS score, b.campaign_bp_score AS raw_score,
  ROUND((
    cd.audience_signals_score
    + IF(cd.missing_sitelinks = 0, 1.0, SAFE_DIVIDE(cd.missing_sitelinks, 4.0))
    + CASE
        WHEN cd.positive_geo_target_type_configured_good = 'X'
          AND cd.negative_geo_target_type_configured_good = 'X' THEN 0.0
        WHEN cd.positive_geo_target_type_configured_good = 'Yes'
          AND cd.negative_geo_target_type_configured_good = 'Yes' THEN 1.0
        ELSE 0.5
      END
  ) / 3.0, 2) AS score_without_url
FROM `{project}.{google_dataset}.campaignbpscore_{suffix}` AS b
JOIN `{project}.{google_dataset}.campaign_data` AS cd
  USING (date, account_id, campaign_id)
WHERE b.account_id = @account
""",
        {"account": account},
    )
    google_asset_groups = _query_rows(
        client,
        f"""
WITH scores AS (
  SELECT account_id, campaign_id, asset_group_id, recommendation_type,
    video_score, text_score, image_score,
    ROUND((video_score + text_score + image_score) / 3.0, 2)
      AS asset_group_bp_score
  FROM `{project}.{google_dataset}.assetgroupbestpractices`
)
SELECT 'asset_group' AS entity_type, account_id, campaign_id, asset_group_id,
  metric, score
FROM scores
UNPIVOT(score FOR metric IN (
  video_score, text_score, image_score, asset_group_bp_score
))
WHERE account_id = @account AND recommendation_type = 'Recommended'
""",
        {"account": account},
    )
    ours_campaigns = _query_rows(
        client,
        f"""
SELECT 'campaign' AS entity_type, account_id, campaign_id,
  CAST(NULL AS INT64) AS asset_group_id, 'campaign_bp_score' AS metric,
  campaign_bp_score AS score, url_expansion_known
FROM `{project}.{marts_dataset}.mart_bp_campaign`
WHERE snapshot_date = @run_date AND account_id = @account
  AND campaign_status = 'ENABLED'
""",
        {"run_date": run_date, "account": account},
    )
    ours_asset_groups = _query_rows(
        client,
        f"""
WITH scores AS (
  SELECT account_id, campaign_id, asset_group_id, video_score, text_score,
    image_score, asset_group_bp_score
  FROM `{project}.{marts_dataset}.mart_bp_asset_group`
  WHERE snapshot_date = @run_date AND account_id = @account
    AND campaign_status = 'ENABLED' AND asset_group_status = 'ENABLED'
)
SELECT 'asset_group' AS entity_type, account_id, campaign_id, asset_group_id,
  metric, score
FROM scores
UNPIVOT(score FOR metric IN (
  video_score, text_score, image_score, asset_group_bp_score
))
""",
        {"run_date": run_date, "account": account},
    )
    paused = _query_rows(
        client,
        f"""
SELECT
  (SELECT COUNT(*) FROM `{project}.{marts_dataset}.mart_bp_campaign`
    WHERE snapshot_date = @run_date AND account_id = @account
      AND campaign_status != 'ENABLED')
  +
  (SELECT COUNT(*) FROM `{project}.{marts_dataset}.mart_bp_asset_group`
    WHERE snapshot_date = @run_date AND account_id = @account
      AND (campaign_status != 'ENABLED' OR asset_group_status != 'ENABLED'))
  AS paused_excluded
""",
        {"run_date": run_date, "account": account},
    )
    return (
        google_campaigns + google_asset_groups,
        ours_campaigns + ours_asset_groups,
        int(paused[0]["paused_excluded"]),
    )


def run_live_parity(
    *,
    config: Any,
    bq_client: Any,
    ledger: Any,
    account: str,
    run_date: date,
    image_digest: str,
    credential_path: str | None = None,
    lease: Any = None,
    compare_fn: Callable[..., ParityResult] = compare_scores,
) -> ParityResult:
    """Run both live surfaces, write one ledger event, and always clean tables."""
    project = config.deployment.project
    input_dataset = config.datasets.parity_scratch
    output_dataset = config.datasets.parity_scratch_bq
    run_id = f"parity-{account}-{run_date.isoformat()}"
    result: ParityResult | None = None
    primary_error: BaseException | None = None
    lease_acquired = False
    scratch_owned = lease is None
    created_tables: dict[str, set[str]] = {
        input_dataset: set(),
        output_dataset: set(),
    }
    now = datetime.now(timezone.utc)
    try:
        if lease is not None:
            lease_acquired = lease.acquire(run_id, "parity", now)
            holder = lease.holder or {}
            if not lease_acquired:
                ledger.lease_event(
                    run_id, "SKIPPED", holder.get("holder"),
                    holder.get("mode"), holder.get("expires_at"),
                    getattr(lease, "observed_generation", None), None, now=now,
                )
                raise ParityError("parity lease held by another run")
            ledger.lease_event(
                run_id,
                "TAKEOVER" if lease.crashed_run is not None else "ACQUIRED",
                holder.get("holder"),
                holder.get("mode"),
                holder.get("expires_at"),
                lease.generation,
                (
                    lease.crashed_run.get("run_id")
                    if lease.crashed_run is not None
                    else None
                ),
                now=now,
            )
            scratch_owned = True
        _ensure_scratch_dataset(bq_client, project, input_dataset)
        _ensure_scratch_dataset(bq_client, project, output_dataset)
        load_google_inputs_live(
            bq_client=bq_client,
            project=project,
            dataset=input_dataset,
            account=account,
            run_date=run_date,
            credential_path=credential_path,
            created_tables=created_tables[input_dataset],
        )
        execute_google_chain_live(
            bq_client=bq_client,
            project=project,
            input_dataset=input_dataset,
            output_dataset=output_dataset,
            run_date=run_date,
            created_tables=created_tables[output_dataset],
        )
        google_rows, our_rows, paused = _live_score_rows(
            bq_client,
            project=project,
            marts_dataset=config.datasets.marts,
            google_dataset=output_dataset,
            account=int(account),
            run_date=run_date,
        )
        result = compare_fn(
            google_rows,
            our_rows,
            tolerance=config.tolerances.parity,
            paused_excluded=paused,
        )
        detail = json.dumps(
            {
                "date": run_date.isoformat(),
                "passed": result.passed,
                "image_digest": image_digest,
                "query_hash": reference_query_hash(),
                "api_version": PARITY_API_VERSION,
                "reference_commit": REFERENCE_COMMIT,
                "result": result.to_dict(),
            },
            sort_keys=True,
        )
        ledger.stage_finished(
            run_id,
            "parity",
            "SUCCESS" if result.passed else "FAILED",
            account,
            detail,
            None if result.passed else "parity mismatch",
        )
        return result
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_error: BaseException | None = None
        if scratch_owned:
            try:
                cleanup_scratch(
                    bq_client, project, created_tables
                )
            except Exception as exc:
                cleanup_error = exc
        if lease is not None and lease_acquired:
            holder = dict(lease.holder or {})
            generation = lease.generation
            try:
                lease.release()
                ledger.lease_event(
                    run_id, "RELEASED", holder.get("holder"),
                    holder.get("mode"), holder.get("expires_at"), generation,
                    None, now=datetime.now(timezone.utc),
                )
            except Exception as exc:
                if cleanup_error is None:
                    cleanup_error = exc
        if primary_error is None and cleanup_error is not None:
            raise cleanup_error


_DUCK_TYPE = {
    "DATE": "DATE",
    "INT64": "BIGINT",
    "STRING": "VARCHAR",
    "FLOAT64": "DOUBLE",
    "BOOL": "BOOLEAN",
}


def _create_fixture_table(
    connection: Any,
    schema: str | None,
    name: str,
    columns: Sequence[tuple[str, str]],
    rows: Sequence[Mapping[str, Any]],
) -> None:
    target = f'"{schema}"."{name}"' if schema else f'"{name}"'
    definition = ", ".join(
        f'"{column}" {_DUCK_TYPE.get(kind, kind)}' for column, kind in columns
    )
    connection.execute(f"CREATE TABLE {target} ({definition})")
    if not rows:
        return
    placeholders = ", ".join("?" for _ in columns)
    names = [column for column, _kind in columns]
    connection.executemany(
        f"INSERT INTO {target} VALUES ({placeholders})",
        [tuple(row.get(column) for column in names) for row in rows],
    )


def _fixture_rows(payload: Mapping[str, Any]) -> tuple[
    dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]
]:
    run_date = date.fromisoformat(str(payload["date"]))
    account = payload["account"]
    account_id = int(account["account_id"])
    account_name = str(account["account_name"])
    google = {name: [] for name in GOOGLE_GAQL_TABLES}
    ours = {
        "mart_entities_campaign": [],
        "mart_entities_asset_group": [],
        "mart_entities_asset": [],
        "mart_entities_asset_group_signal": [],
        "mart_entities_campaign_asset": [],
        "int_entities_customer_asset": [],
    }
    google["ocid_mapping"].append(
        {"customer_id": account_id, "ocid": str(account["ocid"])}
    )
    next_asset = 9000000000
    for index in range(int(payload.get("customer_sitelinks", 0))):
        asset_id = next_asset
        next_asset += 1
        google["customerasset"].append(
            {
                "account_name": account_name,
                "campaign_id": None,
                "account_id": account_id,
                "asset": f"customers/{account_id}/assets/{asset_id}",
                "asset_type": "SITELINK",
                "asset_primary_status": "ELIGIBLE",
                "asset_status": "ENABLED",
            }
        )
        ours["int_entities_customer_asset"].append(
            {
                "snapshot_date": run_date,
                "account_id": account_id,
                "asset_id": asset_id,
                "asset_resource_name": f"customers/{account_id}/assets/{asset_id}",
                "field_type": "SITELINK",
                "status": "ENABLED",
                "primary_status": "ELIGIBLE",
                "primary_status_reasons": [],
            }
        )
    for campaign in payload["campaigns"]:
        campaign_id = int(campaign["campaign_id"])
        campaign_name = str(campaign["campaign_name"])
        campaign_status = str(campaign["status"])
        enabled_campaign = campaign_status == "ENABLED"
        ours["mart_entities_campaign"].append(
            {
                "snapshot_date": run_date,
                "account_id": account_id,
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "status": campaign_status,
                "advertising_channel_type": "PERFORMANCE_MAX",
                "positive_geo_target_type": campaign["positive_geo_target_type"],
                "negative_geo_target_type": campaign["negative_geo_target_type"],
                "url_expansion_opt_out": campaign.get(
                    "ours_url_expansion_opt_out"
                ),
                "inferred_removed": False,
            }
        )
        if enabled_campaign:
            google["campaign_settings"].append(
                {
                    "date": run_date,
                    "account_id": account_id,
                    "account_name": account_name,
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "campaign_status": campaign_status,
                    "url_expansion_opt_out": campaign[
                        "google_url_expansion_opt_out"
                    ],
                    "positive_geo_target_type": campaign[
                        "positive_geo_target_type"
                    ],
                    "negative_geo_target_type": campaign[
                        "negative_geo_target_type"
                    ],
                    "cost": 0,
                    "conversions": 0.0,
                    "conversions_value": 0.0,
                    "all_conversions_value": 0.0,
                }
            )
        for index in range(int(campaign.get("campaign_sitelinks", 0))):
            asset_id = next_asset
            next_asset += 1
            if enabled_campaign:
                google["campaignasset"].append(
                    {
                        "account_name": account_name,
                        "campaign_id": campaign_id,
                        "account_id": account_id,
                        "campaign_name": campaign_name,
                        "campaign_type": "PERFORMANCE_MAX",
                        "campaign_status": campaign_status,
                        "asset_type": "SITELINK",
                    }
                )
            ours["mart_entities_campaign_asset"].append(
                {
                    "snapshot_date": run_date,
                    "account_id": account_id,
                    "campaign_id": campaign_id,
                    "asset_id": asset_id,
                    "field_type": "SITELINK",
                    "inferred_removed": False,
                }
            )
        for group in campaign["asset_groups"]:
            group_id = int(group["asset_group_id"])
            group_status = str(group.get("status", "ENABLED"))
            enabled_group = enabled_campaign and group_status == "ENABLED"
            ours["mart_entities_asset_group"].append(
                {
                    "snapshot_date": run_date,
                    "account_id": account_id,
                    "campaign_id": campaign_id,
                    "asset_group_id": group_id,
                    "asset_group_name": group["asset_group_name"],
                    "status": group_status,
                    "ad_strength": group["ad_strength"],
                    "ad_strength_action_items": group.get("action_items", []),
                    "inferred_removed": False,
                }
            )
            if enabled_group:
                google["assetgroupsummary"].append(
                    {
                        "date": run_date,
                        "asset_group_id": group_id,
                        "asset_group_name": group["asset_group_name"],
                        "asset_group_status": group_status,
                        "campaign_id": campaign_id,
                        "campaign_name": campaign_name,
                        "account_id": account_id,
                        "account_name": account_name,
                        "ad_strength": group["ad_strength"],
                    }
                )
            for audience_index in range(int(group.get("audiences", 0))):
                audience = f"audiences/{group_id}-{audience_index}"
                if enabled_group:
                    google["assetgroupsignal"].append(
                        {
                            "audience_signals": audience,
                            "campaign_id": campaign_id,
                            "asset_group_id": group_id,
                        }
                    )
                ours["mart_entities_asset_group_signal"].append(
                    {
                        "snapshot_date": run_date,
                        "account_id": account_id,
                        "campaign_id": campaign_id,
                        "asset_group_id": group_id,
                        "audience": audience,
                        "inferred_removed": False,
                    }
                )
            for _ in range(int(group.get("search_themes", 0))):
                ours["mart_entities_asset_group_signal"].append(
                    {
                        "snapshot_date": run_date,
                        "account_id": account_id,
                        "campaign_id": campaign_id,
                        "asset_group_id": group_id,
                        "audience": "",
                        "inferred_removed": False,
                    }
                )
            asset_specs = (
                ("HEADLINE", int(group["counts"].get("headline", 0)), "TEXT", 0, 0),
                ("LONG_HEADLINE", int(group["counts"].get("long_headline", 0)), "TEXT", 0, 0),
                ("DESCRIPTION", int(group["counts"].get("description", 0)), "TEXT", 0, 0),
                ("MARKETING_IMAGE", int(group["counts"].get("landscape", 0)), "IMAGE", 628, 1200),
                ("SQUARE_MARKETING_IMAGE", int(group["counts"].get("square", 0)), "IMAGE", 1200, 1200),
                ("PORTRAIT_MARKETING_IMAGE", int(group["counts"].get("portrait", 0)), "IMAGE", 1200, 960),
                ("LOGO", int(group["counts"].get("square_logo", 0)), "IMAGE", 1200, 1200),
                ("LOGO", int(group["counts"].get("landscape_logo", 0)), "IMAGE", 300, 1200),
                ("YOUTUBE_VIDEO", int(group["counts"].get("video", 0)), "YOUTUBE_VIDEO", 0, 0),
            )
            reason_seed = list(group.get("asset_primary_status_reasons", []))
            first_asset = True
            for field_type, count, asset_type, height, width in asset_specs:
                for asset_index in range(count):
                    asset_id = next_asset
                    next_asset += 1
                    text_value = (
                        f"Fixture {field_type} {asset_index}"
                        if asset_type == "TEXT"
                        else None
                    )
                    video_id = (
                        f"video{asset_id}" if asset_type == "YOUTUBE_VIDEO" else None
                    )
                    reasons = reason_seed if first_asset else []
                    first_asset = False
                    if enabled_group:
                        google["assetgroupasset"].append(
                            {
                                "date": run_date,
                                "asset_group_id": group_id,
                                "asset_group_name": group["asset_group_name"],
                                "asset_group_status": group_status,
                                "campaign_id": campaign_id,
                                "campaign_name": campaign_name,
                                "asset_id": asset_id,
                                "account_id": account_id,
                                "account_name": account_name,
                                "asset_type": asset_type,
                                "asset_sub_type": field_type,
                                "image_height": height or None,
                                "image_width": width or None,
                                "text_asset_text": text_value,
                                "video_id": video_id,
                                "video_title": (
                                    f"Fixture video {asset_index}"
                                    if video_id
                                    else None
                                ),
                            }
                        )
                    ours["mart_entities_asset"].append(
                        {
                            "snapshot_date": run_date,
                            "account_id": account_id,
                            "campaign_id": campaign_id,
                            "asset_group_id": group_id,
                            "asset_id": asset_id,
                            "field_type": field_type,
                            "status": "ENABLED",
                            "primary_status_reasons": reasons,
                            "source": "ADVERTISER",
                            "asset_type": asset_type,
                            "text": text_value,
                            "image_height_pixels": height or None,
                            "image_width_pixels": width or None,
                            "video_id": video_id,
                            "inferred_removed": False,
                        }
                    )
    for asset in payload.get("ours_only_assets", []):
        ours["mart_entities_asset"].append(
            {
                "snapshot_date": run_date,
                "account_id": account_id,
                "campaign_id": int(asset["campaign_id"]),
                "asset_group_id": int(asset["asset_group_id"]),
                "asset_id": int(asset["asset_id"]),
                "field_type": str(asset["field_type"]),
                "status": str(asset["status"]),
                "primary_status_reasons": list(
                    asset.get("primary_status_reasons", [])
                ),
                "source": str(asset["source"]),
                "asset_type": str(asset["asset_type"]),
                "text": asset.get("text"),
                "image_height_pixels": asset.get("image_height_pixels"),
                "image_width_pixels": asset.get("image_width_pixels"),
                "video_id": asset.get("video_id"),
                "inferred_removed": False,
            }
        )
    return google, ours


_OUR_FIXTURE_COLUMNS: dict[str, tuple[tuple[str, str], ...]] = {
    "mart_entities_campaign": (
        ("snapshot_date", "DATE"), ("account_id", "INT64"),
        ("campaign_id", "INT64"), ("campaign_name", "STRING"),
        ("status", "STRING"), ("advertising_channel_type", "STRING"),
        ("positive_geo_target_type", "STRING"),
        ("negative_geo_target_type", "STRING"),
        ("url_expansion_opt_out", "BOOL"), ("inferred_removed", "BOOL"),
    ),
    "mart_entities_asset_group": (
        ("snapshot_date", "DATE"), ("account_id", "INT64"),
        ("campaign_id", "INT64"), ("asset_group_id", "INT64"),
        ("asset_group_name", "STRING"), ("status", "STRING"),
        ("ad_strength", "STRING"), ("ad_strength_action_items", "VARCHAR[]"),
        ("inferred_removed", "BOOL"),
    ),
    "mart_entities_asset": (
        ("snapshot_date", "DATE"), ("account_id", "INT64"),
        ("campaign_id", "INT64"), ("asset_group_id", "INT64"),
        ("asset_id", "INT64"), ("field_type", "STRING"),
        ("status", "STRING"), ("primary_status_reasons", "VARCHAR[]"),
        ("source", "STRING"), ("asset_type", "STRING"),
        ("text", "STRING"), ("image_height_pixels", "INT64"),
        ("image_width_pixels", "INT64"), ("video_id", "STRING"),
        ("inferred_removed", "BOOL"),
    ),
    "mart_entities_asset_group_signal": (
        ("snapshot_date", "DATE"), ("account_id", "INT64"),
        ("campaign_id", "INT64"), ("asset_group_id", "INT64"),
        ("audience", "STRING"), ("inferred_removed", "BOOL"),
    ),
    "mart_entities_campaign_asset": (
        ("snapshot_date", "DATE"), ("account_id", "INT64"),
        ("campaign_id", "INT64"), ("asset_id", "INT64"),
        ("field_type", "STRING"), ("inferred_removed", "BOOL"),
    ),
    "int_entities_customer_asset": (
        ("snapshot_date", "DATE"), ("account_id", "INT64"),
        ("asset_id", "INT64"), ("asset_resource_name", "STRING"),
        ("field_type", "STRING"), ("status", "STRING"),
        ("primary_status", "STRING"),
        ("primary_status_reasons", "VARCHAR[]"),
    ),
}


def _render_our_fixture_marts(connection: Any, run_date: date) -> None:
    from sqlglot import exp, parse, transpile

    from pmax_pack.config import Datasets, Tolerances
    from pmax_pack.pipeline import RunContext
    from pmax_pack.runner import load_manifest, render

    config = SimpleNamespace(
        deployment=SimpleNamespace(project="fixture-project"),
        datasets=Datasets(),
        cohort_days=[1, 7, 30],
        tolerances=Tolerances(),
    )
    ctx = RunContext(
        run_id="fixture-parity",
        mode="parity",
        as_of=run_date,
        accounts_configured=[],
        accounts_resolved=[],
        image_digest="sha256:fixture",
        credential_fingerprint="fixture",
        checkpoint_hash="fixture",
        window_start=run_date,
        window_end=run_date,
        timezone="UTC",
        dry_run=True,
    )
    manifest = load_manifest(MANIFEST_PATH)
    by_name = {step.name: step for step in manifest.steps}
    for name in ("mart_bp_campaign", "mart_bp_asset_group", "mart_bp_extended"):
        rendered = render(by_name[name], config, ctx)
        inserts = [
            statement
            for statement in parse(rendered, read="bigquery")
            if isinstance(statement, exp.Insert)
        ]
        if len(inserts) != 1:
            raise ParityError(f"{name}: expected one rendered INSERT")
        query = inserts[0].expression.sql(dialect="bigquery")
        query = re.sub(
            r"`[^`]+\.([A-Za-z_][A-Za-z0-9_]*)`",
            lambda match: f'"{match.group(1)}"',
            query,
        )
        query = query.replace("@as_of", f"DATE '{run_date.isoformat()}'")
        query = query.replace("@run_id", "'fixture-parity'")
        statements = transpile(query, read="bigquery", write="duckdb")
        if len(statements) != 1:
            raise ParityError(f"{name}: expected one DuckDB SELECT")
        connection.execute(f'CREATE TABLE "{name}" AS {statements[0]}')


def _render_google_fixture_chain(connection: Any, run_date: date) -> None:
    from sqlglot import parse, transpile

    for name in GOOGLE_BQ_CHAIN:
        text = (REFERENCE_ROOT / "bq_queries" / name).read_text(encoding="utf-8")
        text = rewrite_reference_bq(text, run_date)
        text = text.replace("`{bq_dataset}_bq.", "`pmax_ci_scratch_bq.")
        text = text.replace("`{bq_dataset}.", "`pmax_ci_scratch.")
        for statement in parse(text, read="bigquery"):
            rendered = transpile(
                statement.sql(dialect="bigquery"),
                read="bigquery",
                write="duckdb",
            )
            if len(rendered) != 1:
                raise ParityError(f"{name}: non-transpilable statement")
            connection.execute(rendered[0])


def _duck_rows(connection: Any, sql: str) -> list[dict[str, Any]]:
    cursor = connection.execute(sql)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _fixture_score_rows(
    connection: Any, run_date: date
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    suffix = run_date.strftime("%Y%m%d")
    google_campaigns = _duck_rows(
        connection,
        f"""
SELECT 'campaign' AS entity_type, b.account_id, b.campaign_id,
  NULL AS asset_group_id, 'campaign_bp_score' AS metric,
  b.campaign_bp_score AS score, b.campaign_bp_score AS raw_score,
  ROUND((
    cd.audience_signals_score
    + IF(cd.missing_sitelinks = 0, 1.0, cd.missing_sitelinks / 4.0)
    + CASE
        WHEN cd.positive_geo_target_type_configured_good = 'X'
          AND cd.negative_geo_target_type_configured_good = 'X' THEN 0.0
        WHEN cd.positive_geo_target_type_configured_good = 'Yes'
          AND cd.negative_geo_target_type_configured_good = 'Yes' THEN 1.0
        ELSE 0.5
      END
  ) / 3.0, 2) AS score_without_url
FROM pmax_ci_scratch_bq.campaignbpscore_{suffix} AS b
JOIN pmax_ci_scratch_bq.campaign_data AS cd
  USING (date, account_id, campaign_id)
""",
    )
    google_assets: list[dict[str, Any]] = []
    for row in _duck_rows(
        connection,
        """
SELECT account_id, campaign_id, asset_group_id, video_score, text_score,
  image_score, ROUND((video_score + text_score + image_score) / 3.0, 2)
    AS asset_group_bp_score
FROM pmax_ci_scratch_bq.assetgroupbestpractices
WHERE recommendation_type = 'Recommended'
""",
    ):
        for metric in (
            "video_score",
            "text_score",
            "image_score",
            "asset_group_bp_score",
        ):
            google_assets.append(
                {
                    "entity_type": "asset_group",
                    "account_id": row["account_id"],
                    "campaign_id": row["campaign_id"],
                    "asset_group_id": row["asset_group_id"],
                    "metric": metric,
                    "score": row[metric],
                }
            )
    ours_campaigns = _duck_rows(
        connection,
        """
SELECT 'campaign' AS entity_type, account_id, campaign_id,
  NULL AS asset_group_id, 'campaign_bp_score' AS metric,
  campaign_bp_score AS score, url_expansion_known
FROM mart_bp_campaign
WHERE campaign_status = 'ENABLED'
""",
    )
    ours_assets: list[dict[str, Any]] = []
    for row in _duck_rows(
        connection,
        """
SELECT account_id, campaign_id, asset_group_id, video_score, text_score,
  image_score, asset_group_bp_score
FROM mart_bp_asset_group
WHERE campaign_status = 'ENABLED' AND asset_group_status = 'ENABLED'
""",
    ):
        for metric in (
            "video_score",
            "text_score",
            "image_score",
            "asset_group_bp_score",
        ):
            ours_assets.append(
                {
                    "entity_type": "asset_group",
                    "account_id": row["account_id"],
                    "campaign_id": row["campaign_id"],
                    "asset_group_id": row["asset_group_id"],
                    "metric": metric,
                    "score": row[metric],
                }
            )
    paused = _duck_rows(
        connection,
        """
SELECT
  (SELECT COUNT(*) FROM mart_bp_campaign WHERE campaign_status != 'ENABLED')
  +
  (SELECT COUNT(*) FROM mart_bp_asset_group
    WHERE campaign_status != 'ENABLED' OR asset_group_status != 'ENABLED')
    AS paused_excluded
""",
    )[0]["paused_excluded"]
    return google_campaigns + google_assets, ours_campaigns + ours_assets, int(paused)


def _derive_fixture_coverage(payload: Mapping[str, Any]) -> frozenset[str]:
    """Reconstruct covered branches from eligible ours-side fixture rows."""
    _google, ours = _fixture_rows(payload)
    campaigns = {
        (row["account_id"], row["campaign_id"]): row
        for row in ours["mart_entities_campaign"]
        if row["status"] == "ENABLED"
        and row["advertising_channel_type"] == "PERFORMANCE_MAX"
        and not row["inferred_removed"]
    }
    groups = {
        (row["account_id"], row["campaign_id"], row["asset_group_id"]): row
        for row in ours["mart_entities_asset_group"]
        if row["status"] == "ENABLED"
        and (row["account_id"], row["campaign_id"]) in campaigns
        and not row["inferred_removed"]
    }
    eligible_assets: dict[tuple[int, int, int], list[Mapping[str, Any]]] = {}
    for row in ours["mart_entities_asset"]:
        key = (row["account_id"], row["campaign_id"], row["asset_group_id"])
        if (
            key in groups
            and row["status"] == "ENABLED"
            and row["source"] == "ADVERTISER"
            and not row["inferred_removed"]
        ):
            eligible_assets.setdefault(key, []).append(row)

    signals_by_campaign: dict[tuple[int, int], set[str]] = {}
    for row in ours["mart_entities_asset_group_signal"]:
        group_key = (
            row["account_id"],
            row["campaign_id"],
            row["asset_group_id"],
        )
        if group_key in groups and row["audience"]:
            signals_by_campaign.setdefault(group_key[:2], set()).add(
                str(row["audience"])
            )

    customer_sitelinks: dict[int, int] = {}
    for row in ours["int_entities_customer_asset"]:
        if row["field_type"] == "SITELINK":
            customer_sitelinks[row["account_id"]] = (
                customer_sitelinks.get(row["account_id"], 0) + 1
            )
    campaign_sitelinks: dict[tuple[int, int], int] = {}
    for row in ours["mart_entities_campaign_asset"]:
        key = (row["account_id"], row["campaign_id"])
        if key in campaigns and row["field_type"] == "SITELINK":
            campaign_sitelinks[key] = campaign_sitelinks.get(key, 0) + 1

    covered: set[str] = set()
    for key, campaign in campaigns.items():
        url_value = campaign["url_expansion_opt_out"]
        if url_value is None:
            covered.add("campaign.url.unknown")
        elif url_value:
            covered.add("campaign.url.opted_out")
        else:
            covered.add("campaign.url.opted_in")

        audience_branch = (
            "present" if signals_by_campaign.get(key) else "absent"
        )
        covered.add(f"campaign.audience.{audience_branch}")
        sitelinks = customer_sitelinks.get(key[0], 0) + campaign_sitelinks.get(
            key, 0
        )
        covered.add(
            "campaign.sitelinks.at_4"
            if sitelinks >= 4
            else "campaign.sitelinks.below_4"
        )
        positive_good = (
            campaign["positive_geo_target_type"] == "PRESENCE_OR_INTEREST"
        )
        negative_good = (
            campaign["negative_geo_target_type"] == "PRESENCE_OR_INTEREST"
        )
        if positive_good and negative_good:
            geo_branch = "both_presence_or_interest"
        elif not positive_good and not negative_good:
            geo_branch = "neither_presence_or_interest"
        else:
            geo_branch = "mixed"
        covered.add(f"campaign.geo.{geo_branch}")

    for key, group in groups.items():
        assets = eligible_assets.get(key, [])

        def count_field(field_type: str) -> int:
            return sum(row["field_type"] == field_type for row in assets)

        count_headlines = sum(
            row["field_type"] in {"HEADLINE", "LONG_HEADLINE"}
            for row in assets
        )
        threshold_branches = {
            "asset_group.video": (
                sum(row["asset_type"] == "YOUTUBE_VIDEO" for row in assets),
                3,
            ),
            "asset_group.text.descriptions": (count_field("DESCRIPTION"), 4),
            "asset_group.text.headlines": (count_headlines, 11),
            "asset_group.text.long_headlines": (
                count_field("LONG_HEADLINE"),
                2,
            ),
            "asset_group.image.landscape": (
                count_field("MARKETING_IMAGE"),
                4,
            ),
            "asset_group.image.square": (
                count_field("SQUARE_MARKETING_IMAGE"),
                4,
            ),
            "asset_group.image.portrait": (
                count_field("PORTRAIT_MARKETING_IMAGE"),
                4,
            ),
        }
        for family, (observed, threshold) in threshold_branches.items():
            covered.add(
                f"{family}.at_{threshold}"
                if observed >= threshold
                else f"{family}.below_{threshold}"
            )
        square_logos = sum(
            row["field_type"] == "LOGO"
            and row["image_width_pixels"] == row["image_height_pixels"]
            for row in assets
        )
        landscape_logos = sum(
            row["field_type"] == "LOGO"
            and row["image_height_pixels"] not in (None, 0)
            and round(
                row["image_width_pixels"] / row["image_height_pixels"], 2
            )
            == 4
            for row in assets
        )
        covered.add(
            "asset_group.image.square_logo.present"
            if square_logos
            else "asset_group.image.square_logo.absent"
        )
        covered.add(
            "asset_group.image.landscape_logo.present"
            if landscape_logos
            else "asset_group.image.landscape_logo.absent"
        )
        covered.add(f"asset_group.ad_strength.{group['ad_strength']}")
    return frozenset(covered)


def _assert_fixture_coverage(payload: Mapping[str, Any]) -> None:
    declared = frozenset(str(item) for item in payload.get("coverage", []))
    derived = _derive_fixture_coverage(payload)
    declared_missing = sorted(derived - declared)
    declared_extra = sorted(declared - derived)
    missing = sorted(REQUIRED_RULE_BRANCHES - derived)
    extra = sorted(derived - REQUIRED_RULE_BRANCHES)
    if declared_missing or declared_extra or missing or extra:
        raise ParityError(
            "fixture coverage drift: "
            f"declared_missing={declared_missing}, "
            f"declared_extra={declared_extra}, missing={missing}, extra={extra}"
        )


def _assert_known_scores(connection: Any, expected_path: Path) -> None:
    expected = json.loads(expected_path.read_text(encoding="utf-8"))
    campaigns = {
        str(row["campaign_id"]): Decimal(str(row["campaign_bp_score"]))
        for row in _duck_rows(connection, "SELECT * FROM mart_bp_campaign")
    }
    assets = {
        str(row["asset_group_id"]): {
            key: Decimal(str(row[key]))
            for key in (
                "video_score",
                "text_score",
                "image_score",
                "asset_group_bp_score",
            )
        }
        for row in _duck_rows(connection, "SELECT * FROM mart_bp_asset_group")
    }
    expected_campaigns = {
        key: Decimal(str(value)) for key, value in expected["campaigns"].items()
    }
    expected_assets = {
        key: {metric: Decimal(str(value)) for metric, value in scores.items()}
        for key, scores in expected["asset_groups"].items()
    }
    if campaigns != expected_campaigns or assets != expected_assets:
        raise ParityError(
            f"known score table drift: campaigns={campaigns}, asset_groups={assets}"
        )


def build_fixture_connection(fixture_root: Path = FIXTURE_ROOT) -> Any:
    """Materialize both fixture score surfaces and return the DuckDB handle."""
    import duckdb

    payload = json.loads((fixture_root / "source.json").read_text(encoding="utf-8"))
    _assert_fixture_coverage(payload)
    run_date = date.fromisoformat(str(payload["date"]))
    google, ours = _fixture_rows(payload)
    connection = duckdb.connect()
    connection.execute("CREATE SCHEMA pmax_ci_scratch")
    connection.execute("CREATE SCHEMA pmax_ci_scratch_bq")
    for name in GOOGLE_GAQL_TABLES:
        _create_fixture_table(
            connection,
            "pmax_ci_scratch",
            name,
            _INPUT_COLUMNS[name],
            google[name],
        )
    for name, rows in ours.items():
        _create_fixture_table(
            connection,
            None,
            name,
            _OUR_FIXTURE_COLUMNS[name],
            rows,
        )
    _render_google_fixture_chain(connection, run_date)
    _render_our_fixture_marts(connection, run_date)
    _assert_known_scores(connection, fixture_root / "expected.json")
    return connection


def _bigquery_schema(columns: Sequence[tuple[str, str]]) -> list[Any]:
    from google.cloud.bigquery import SchemaField

    schema = []
    for name, kind in columns:
        if kind == "VARCHAR[]":
            schema.append(SchemaField(name, "STRING", mode="REPEATED"))
        else:
            schema.append(SchemaField(name, kind))
    return schema


def _run_our_fixture_chain_bq(
    client: Any,
    *,
    project: str,
    dataset: str,
    run_date: date,
    created_tables: set[str] | None = None,
) -> None:
    from google.cloud import bigquery

    from pmax_pack.config import Datasets, Tolerances
    from pmax_pack.pipeline import RunContext
    from pmax_pack.runner import load_manifest, render

    config = SimpleNamespace(
        deployment=SimpleNamespace(project=project),
        datasets=Datasets(marts=dataset),
        cohort_days=[1, 7, 30],
        tolerances=Tolerances(),
    )
    ctx = RunContext(
        run_id="fixture-parity-bq",
        mode="parity",
        as_of=run_date,
        accounts_configured=[],
        accounts_resolved=[],
        image_digest="sha256:fixture",
        credential_fingerprint="fixture",
        checkpoint_hash="fixture",
        window_start=run_date,
        window_end=run_date,
        timezone="UTC",
        dry_run=False,
    )
    manifest = load_manifest(MANIFEST_PATH)
    by_name = {step.name: step for step in manifest.steps}
    for name in ("mart_bp_campaign", "mart_bp_asset_group", "mart_bp_extended"):
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("as_of", "DATE", run_date),
                bigquery.ScalarQueryParameter(
                    "run_id", "STRING", "fixture-parity-bq"
                ),
            ],
            maximum_bytes_billed=10 * 1024 * 1024 * 1024,
        )
        client.query(render(by_name[name], config, ctx), job_config=job_config).result()
        if created_tables is not None:
            created_tables.add(name)


def run_fixture_parity_bq(
    *,
    bq_client: Any,
    project: str,
    fixture_root: Path = FIXTURE_ROOT,
    input_dataset: str = "pmax_ci_scratch",
    output_dataset: str = "pmax_ci_scratch_bq",
    tolerance: float | Decimal = DEFAULT_TOLERANCE_PARITY,
    created_tables: dict[str, set[str]] | None = None,
) -> ParityResult:
    """Run committed fixtures against trusted CI BigQuery scratch only."""
    # This fixed-name path has no in-process serialization. U7's trusted.yml
    # must protect it with a concurrency group so parallel jobs cannot clobber
    # pmax_ci_scratch or pmax_ci_scratch_bq.
    from pmax_pack.loader import load_rows

    payload = json.loads((fixture_root / "source.json").read_text(encoding="utf-8"))
    _assert_fixture_coverage(payload)
    run_date = date.fromisoformat(str(payload["date"]))
    account = int(payload["account"]["account_id"])
    google, ours = _fixture_rows(payload)
    primary_error: BaseException | None = None
    caller_owns_cleanup = created_tables is not None
    if created_tables is None:
        created_tables = {}
    created_tables.setdefault(input_dataset, set())
    created_tables.setdefault(output_dataset, set())
    try:
        _ensure_scratch_dataset(bq_client, project, input_dataset)
        _ensure_scratch_dataset(bq_client, project, output_dataset)
        for name in GOOGLE_GAQL_TABLES:
            load_rows(
                bq_client,
                f"{project}.{input_dataset}.{name}",
                google[name],
                _bigquery_schema(_INPUT_COLUMNS[name]),
                run_date,
                "WRITE_TRUNCATE",
            )
            created_tables[input_dataset].add(name)
        for name, rows in ours.items():
            load_rows(
                bq_client,
                f"{project}.{input_dataset}.{name}",
                rows,
                _bigquery_schema(_OUR_FIXTURE_COLUMNS[name]),
                run_date,
                "WRITE_TRUNCATE",
                partition_field="snapshot_date",
            )
            created_tables[input_dataset].add(name)
        execute_google_chain_live(
            bq_client=bq_client,
            project=project,
            input_dataset=input_dataset,
            output_dataset=output_dataset,
            run_date=run_date,
            created_tables=created_tables[output_dataset],
        )
        _run_our_fixture_chain_bq(
            bq_client,
            project=project,
            dataset=input_dataset,
            run_date=run_date,
            created_tables=created_tables[input_dataset],
        )
        google_rows, our_rows, paused = _live_score_rows(
            bq_client,
            project=project,
            marts_dataset=input_dataset,
            google_dataset=output_dataset,
            account=account,
            run_date=run_date,
        )
        return compare_scores(
            google_rows,
            our_rows,
            tolerance=tolerance,
            paused_excluded=paused,
        )
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        if not caller_owns_cleanup:
            try:
                cleanup_scratch(bq_client, project, created_tables)
            except Exception:
                if primary_error is None:
                    raise


def run_fixture_parity(
    *,
    fixture_root: Path = FIXTURE_ROOT,
    seeded_mismatch: bool = False,
    tolerance: float | Decimal = DEFAULT_TOLERANCE_PARITY,
) -> ParityResult:
    """Run both score chains offline from committed synthetic fixtures."""
    payload = json.loads((fixture_root / "source.json").read_text(encoding="utf-8"))
    run_date = date.fromisoformat(str(payload["date"]))
    connection = build_fixture_connection(fixture_root)
    google_rows, our_rows, paused = _fixture_score_rows(connection, run_date)
    if seeded_mismatch:
        mismatch = json.loads(
            (fixture_root / "seeded_mismatch.json").read_text(encoding="utf-8")
        )
        for row in google_rows:
            if all(row.get(key) == value for key, value in mismatch["match"].items()):
                row["score"] = Decimal(str(row["score"])) + Decimal(
                    str(mismatch["delta"])
                )
                row["raw_score"] = row["score"]
                break
        else:
            raise ParityError("seeded mismatch target was not found")
    return compare_scores(
        google_rows,
        our_rows,
        tolerance=tolerance,
        paused_excluded=paused,
    )


def cli_main(*, source: str | None, account: str | None, run_date: str | None) -> int:
    """Single CLI call target used by pmax_pack.cli."""
    selected = source or "live"
    if selected == "fixtures":
        if scratch_project := os.environ.get("PMAX_CI_SCRATCH_PROJECT"):
            from google.cloud import bigquery

            result = run_fixture_parity_bq(
                bq_client=bigquery.Client(project=scratch_project),
                project=scratch_project,
            )
        else:
            result = run_fixture_parity()
        print(json.dumps(result.to_dict(), sort_keys=True))
        return 0 if result.passed else 1
    if not account or not re.fullmatch(r"\d{10}", account):
        raise ValueError("parity live: --account must be a 10-digit customer id")
    if not run_date:
        raise ValueError("parity live: --date is required")
    day = date.fromisoformat(run_date)
    from google.cloud import bigquery, storage

    from pmax_pack.config import load_config
    from pmax_pack.ledger import Ledger, Lease

    config_source = os.environ.get("PMAX_CONFIG", "config.yaml")
    config = load_config(config_source, run_date=day)
    client = bigquery.Client(project=config.deployment.project)
    ledger = Ledger(client, config.deployment.project, config.datasets.ops)
    lease = Lease(storage.Client(project=config.deployment.project),
                  config.buckets.report_bucket, "lease.json")
    result = run_live_parity(
        config=config,
        bq_client=client,
        ledger=ledger,
        account=account,
        run_date=day,
        image_digest=os.environ.get("PMAX_IMAGE_DIGEST", "unknown"),
        lease=lease,
    )
    print(json.dumps(result.to_dict(), sort_keys=True))
    return 0 if result.passed else 1
