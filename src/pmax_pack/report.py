"""Deterministic validation report assembly and GCS publication.

The report is a pure fold over normalized ledger assertion rows and run
metadata. It performs the severity decision before rendering, redacts the
entire document once at the final boundary, and publishes one replaceable
object per run id. Only executed daily ``run`` mode advances ``latest.md``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from itertools import chain, islice
import re
from typing import Any, Iterable, Mapping, Sequence

from pmax_pack.redact import redact, redact_line

HARD = "HARD"
SOFT = "SOFT"
_BODY_LINE_COUNT = "- Report body lines: {body_line_count}"


def _text(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _account(value: Any) -> str:
    return str(int(value)) if isinstance(value, int) else str(value)


def _inline(value: Any) -> str:
    """Keep external values on one Markdown line with escaped cell separators."""
    # Retention readers already escape pipes, so this boundary is idempotent.
    return re.sub(r"\\*\|", r"\\|", " ".join(_text(value).split()))


@dataclass(frozen=True)
class CheckResult:
    """One normalized assertion outcome from ``pmax_ops.assertion_results``."""

    name: str
    severity: str
    passed: bool
    observed: Any = None
    expected: Any = None
    detail: str | None = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "CheckResult":
        """Normalize a BigQuery row or plain mapping."""
        raw_passed = row.get("passed")
        return cls(
            name=str(row.get("assertion") or row.get("name") or "unnamed"),
            severity=str(row.get("severity") or HARD).upper(),
            passed=raw_passed if isinstance(raw_passed, bool) else False,
            observed=row.get("observed"),
            expected=row.get("expected"),
            detail=(None if row.get("detail") is None else str(row["detail"])),
        )


def retention_check(drift: Sequence[str]) -> CheckResult | None:
    """Render retention differences as one SOFT check, silent when equal."""
    if not drift:
        return None
    return CheckResult(
        name="retention_drift", severity=SOFT, passed=False,
        observed=len(drift), expected=0, detail="; ".join(drift),
    )


def retention_metadata_check(failures: Sequence[str]) -> CheckResult | None:
    """Report unavailable readers separately with safe single-line cell text."""
    if not failures:
        return None
    detail = "; ".join(
        redact_line(message).replace("|", "\\|")
        for message in failures
    )
    return CheckResult(
        name="retention_metadata", severity=SOFT, passed=False,
        observed=len(failures), expected=0, detail=detail,
    )


@dataclass(frozen=True)
class TableMetric:
    """Observational row count and freshness for one table.

    Empty-table decisions come only from the manifested HARD assertion so the
    report does not duplicate first-run eligibility or its configured window.
    """

    table: str
    row_count: int
    fresh_through: date | None
    expected_fresh_through: date | None
    expectation_note: str | None = None

    @property
    def stale(self) -> bool:
        return (
            self.row_count > 0
            and self.expected_fresh_through is not None
            and (
                self.fresh_through is None
                or self.fresh_through < self.expected_fresh_through
            )
        )


@dataclass(frozen=True)
class CoverageMetric:
    provenance: str
    maturity: str
    cells: int
    total_cells: int
    share: float


@dataclass(frozen=True)
class AssumedCurrentMetric:
    account_id: str
    cells: int
    total_cells: int
    share: float


@dataclass(frozen=True)
class AssetParticipationRatio:
    account_id: str
    ad_network_type: str
    metric: str
    asset_sum: float
    campaign_truth: float
    ratio: float | None


@dataclass(frozen=True)
class ParityRun:
    run_date: date
    result: str
    image_digest: str
    query_hash: str
    api_version: str
    reference_commit: str


@dataclass
class ReportInput:
    """All data required to render one validation report without I/O."""

    run_id: str
    mode: str
    deployment: str
    as_of: date
    configured_accounts: list[str]
    resolved_accounts: list[str]
    image_digest: str
    credential_fingerprint: str
    query_hash: str
    api_version: str
    reference_commit: str
    sql_files_resolved: int
    dry_run: bool = False
    checks: list[CheckResult] = field(default_factory=list)
    tables: list[TableMetric] = field(default_factory=list)
    unknown_lag: list[Mapping[str, Any]] = field(default_factory=list)
    coverage: list[CoverageMetric] = field(default_factory=list)
    assumed_current: list[AssumedCurrentMetric] = field(default_factory=list)
    asset_participation: list[AssetParticipationRatio] = field(
        default_factory=list
    )
    snapshot_gaps: list[str] = field(default_factory=list)
    stale_cells: list[str] = field(default_factory=list)
    frozen_chunks: list[str] = field(default_factory=list)
    null_cost_cells: list[str] = field(default_factory=list)
    anomalies: list[str] = field(default_factory=list)
    crashed_runs: list[str] = field(default_factory=list)
    parity: ParityRun | None = None
    skipped_reason: str | None = None
    handled_error: str | None = None
    budget: Mapping[str, Any] | None = None
    gap_summary: list[Mapping[str, Any]] = field(default_factory=list)
    gap_examples: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ValidationReport:
    """Rendered report plus the process decision made from the same source."""

    run_id: str
    mode: str
    deployment: str
    status: str
    exit_code: int
    markdown: str
    source: ReportInput

    @property
    def object_name(self) -> str:
        return f"reports/{self.deployment}/{self.run_id}.md"


def _parity_stale(source: ReportInput) -> bool:
    parity = source.parity
    if parity is None:
        return True
    return any(
        (
            parity.image_digest != source.image_digest,
            parity.query_hash != source.query_hash,
            parity.api_version != source.api_version,
            parity.reference_commit != source.reference_commit,
        )
    )


def _decision(source: ReportInput) -> tuple[str, list[str], list[str]]:
    if source.skipped_reason:
        return "SKIPPED", [], [f"SKIPPED: {source.skipped_reason}"]

    hard: list[str] = []
    warnings: list[str] = []
    configured = {_account(value) for value in source.configured_accounts}
    resolved = {_account(value) for value in source.resolved_accounts}
    missing = sorted(configured - resolved)
    if not resolved:
        hard.append("zero resolved accounts")
    if source.sql_files_resolved <= 0:
        hard.append("zero SQL files resolved from the manifest")
    if missing:
        hard.append(
            "configured accounts absent from resolved set: " + ", ".join(missing)
        )
    if source.handled_error:
        hard.append("handled failure: " + source.handled_error)

    for metric in source.tables:
        if metric.stale:
            warnings.append(
                f"{metric.table}: stale through {_text(metric.fresh_through)}; "
                f"expected {_text(metric.expected_fresh_through)}"
            )

    for check in source.checks:
        if check.passed:
            continue
        detail = check.detail or (
            f"observed {_text(check.observed)}, expected {_text(check.expected)}"
        )
        item = f"{check.name}: {detail}"
        if check.severity.upper() == HARD:
            hard.append(item)
        else:
            warnings.append(item)

    if source.parity is None:
        warnings.append("no parity result is recorded; parity status is stale")
    elif _parity_stale(source):
        warnings.append("most recent parity result is stale")
    status = "FAIL" if hard else "PASS"
    return status, hard, warnings


def _items(values: Sequence[Any], empty: str = "None.") -> list[str]:
    if not values:
        return [empty]
    return [f"- {_inline(value)}" for value in values]


def _seconds(value: Any) -> str:
    return "unavailable" if value is None else f"{value:.3f} s"


def _count(value: Any) -> str:
    return "unavailable" if value is None else f"{value:,}"


def _budget_lines(budget: Mapping[str, Any] | None) -> list[str]:
    """Render measured values without deriving or gating on missing metrics."""
    values = budget or {}
    seconds = values.get("stage_seconds") or {}
    jobs = values.get("stage_jobs") or {}
    span = values.get("stage_span_seconds")
    target = "informational target: 120 s"
    if span is not None:
        target += "; over target" if span > 120 else "; within target"
    return [
        "",
        "### Budget (informational)",
        "",
        (
            "Preliminary snapshot from CLI process entry; "
            "stage/tail accounting is still in progress."
            if values.get("snapshot_complete") is False else
            "Measured from CLI process entry through the final report snapshot; "
            "final upload excluded."
        ),
        "",
        f"- Startup: {_seconds(values.get('startup_seconds'))}",
        f"  - Pre-lease calls: {_seconds(values.get('pre_lease_seconds'))}",
        f"- Startup jobs: {_count(values.get('startup_jobs'))} "
        "(including pre-lease calls)",
        f"- Stage span: {_seconds(span)} ({target})",
        f"- Tail: {_seconds(values.get('tail_seconds'))}",
        f"- Tail jobs: {_count(values.get('tail_jobs'))}",
        f"- Process total: {_seconds(values.get('total_seconds'))}",
        f"- Load-path jobs: {_count(values.get('load_path_jobs'))}",
        f"- Total jobs: {_count(values.get('total_jobs'))} (submissions)",
        f"- Rows loaded: {_count(values.get('rows_loaded'))}",
        _BODY_LINE_COUNT + " (informational target: <2,000 lines)",
        "",
        "| Stage | Seconds | Jobs |",
        "|---|---:|---:|",
        *(
            f"| {_inline(stage)} | {_seconds(seconds.get(stage))} | "
            f"{_count(jobs.get(stage))} |"
            for stage in dict.fromkeys(chain(seconds, jobs))
        ),
        *([] if seconds or jobs else ["| unavailable | unavailable | unavailable |"]),
    ]


def _gap_lines(source: ReportInput) -> list[str]:
    """Render SQL aggregate counts and at most 50 example lines in total."""
    lines = []
    if source.gap_summary:
        lines.extend([
            "| Grain | Reason | Month | Cells |",
            "|---|---|---|---:|",
        ])
        for item in source.gap_summary:
            lines.append(
                f"| {_inline(item.get('grain'))} | {_inline(item.get('reason'))} | "
                f"{_inline(item.get('month'))} | {_count(item.get('cells'))} |"
            )
        lines.append("")
    examples = source.gap_examples or chain(
        (f"snapshot gap: {item}" for item in source.snapshot_gaps),
        (f"stale cell: {item}" for item in source.stale_cells),
    )
    lines.extend(_items(list(islice(examples, 50))))
    return lines


def _render(
    source: ReportInput,
    status: str,
    hard: list[str],
    warnings: list[str],
) -> str:
    exit_code = 1 if status == "FAIL" else 0
    lines = [
        f"# {status}: Validation report",
        "",
        f"Summary: {len(hard)} hard failure(s), {len(warnings)} warning(s); "
        f"exit code {exit_code}.",
        "",
        "## Run",
        "",
        f"- Run ID: `{_inline(source.run_id)}`",
        f"- Mode: `{_inline(source.mode)}`",
        f"- Dry run: {'yes' if source.dry_run else 'no'}",
        f"- As of: `{source.as_of.isoformat()}`",
        f"- Image digest: `{_inline(source.image_digest)}`",
        f"- Credential fingerprint: `{_inline(source.credential_fingerprint)}`",
        f"- Query hash: `{_inline(source.query_hash)}`",
        f"- API version: `{_inline(source.api_version)}`",
        f"- Reference commit: `{_inline(source.reference_commit)}`",
        f"- SQL files resolved: {source.sql_files_resolved}",
        *(_budget_lines(source.budget) if status != "SKIPPED" else []),
        "",
        "## Accounts",
        "",
        "Configured: "
        + (", ".join(map(_inline, source.configured_accounts)) or "none"),
        "",
        "Resolved: "
        + (", ".join(map(_inline, source.resolved_accounts)) or "none"),
        "",
        "## Hard failures",
        "",
        *_items(hard),
        "",
        "## Warnings",
        "",
        *_items(warnings),
        "",
        "## Row counts and freshness",
        "",
        "| Table | Rows | Fresh through | Expected through | Result |",
        "|---|---:|---|---|---|",
    ]
    if source.tables:
        for metric in source.tables:
            if metric.expectation_note:
                result = "INFO"
            elif metric.stale:
                result = "WARN"
            elif metric.row_count == 0:
                result = "INFO (empty)"
            else:
                result = "PASS"
            lines.append(
                f"| {_inline(metric.table)} | {metric.row_count:,} | "
                f"{_inline(metric.fresh_through)} | "
                f"{_inline(metric.expectation_note or metric.expected_fresh_through)} "
                f"| {result} |"
            )
    else:
        lines.append("| No table metrics supplied | 0 | - | - | INFO |")

    lines.extend(
        [
            "",
            "## Assertion outcomes",
            "",
            "| Check | Severity | Result | Observed | Expected | Detail |",
            "|---|---|---|---|---|---|",
        ]
    )
    if source.checks:
        for check in source.checks:
            result = "PASS" if check.passed else (
                "FAIL" if check.severity.upper() == HARD else "WARN"
            )
            lines.append(
                f"| {_inline(check.name)} | {_inline(check.severity.upper())} | "
                f"{result} | {_inline(check.observed)} | "
                f"{_inline(check.expected)} | {_inline(check.detail)} |"
            )
    else:
        lines.append("| No assertion rows supplied | INFO | - | - | - | - |")

    lines.extend(["", "## Asset participation ratios (informational)", ""])
    if source.asset_participation:
        for item in source.asset_participation:
            ratio = "-" if item.ratio is None else f"{item.ratio:.6f}"
            lines.append(
                f"- account={_inline(item.account_id)}, "
                f"network={_inline(item.ad_network_type)}, "
                f"metric={_inline(item.metric)}, asset_sum={item.asset_sum:.6f}, "
                f"campaign_truth={item.campaign_truth:.6f}, ratio={ratio}"
            )
    else:
        lines.append("None reported.")

    lines.extend(["", "## Unknown-lag share", ""])
    if source.unknown_lag:
        lines.extend(
            f"- account={_inline(item.get('account_id'))}, "
            f"basis={_inline(item.get('basis') or item.get('metric_basis'))}, "
            f"share={_inline(item.get('share'))}"
            for item in source.unknown_lag
        )
    else:
        lines.append("None reported.")

    lines.extend(["", "## Assumed-current share by account", ""])
    if source.assumed_current:
        lines.extend(
            f"- account={_inline(item.account_id)}, cells={item.cells:,}/"
            f"{item.total_cells:,}, share={item.share:.6f}"
            for item in source.assumed_current
        )
    else:
        lines.append("None reported.")

    lines.extend(["", "## Cohort coverage", ""])
    if source.coverage:
        lines.extend(
            f"- provenance={_inline(item.provenance)}, "
            f"maturity={_inline(item.maturity)}, "
            f"cells={item.cells:,}/{item.total_cells:,}, share={item.share:.6f}"
            for item in source.coverage
        )
    else:
        lines.append("None reported.")

    lines.extend(["", "## Snapshot gaps and stale cells", "", *_gap_lines(source)])
    sections: list[tuple[str, Iterable[str]]] = [
        ("Frozen chunks", source.frozen_chunks),
        ("NULL-cost cells", source.null_cost_cells),
    ]
    for heading, values in sections:
        lines.extend(["", f"## {heading}", "", *_items(list(values))])

    lines.extend(["", "## Parity", ""])
    if source.parity is None:
        lines.append("No parity run recorded. Status: STALE.")
    else:
        parity_status = "STALE" if _parity_stale(source) else "CURRENT"
        lines.extend(
            [
                f"- Date: {source.parity.run_date.isoformat()}",
                f"- Result: {_inline(source.parity.result)}",
                f"- Binding: {parity_status}",
            ]
        )

    for heading, values in (
        ("Crashed runs", source.crashed_runs),
        ("Anomalies", source.anomalies),
    ):
        lines.extend(["", f"## {heading}", "", *_items(values)])
    return "\n".join(lines).rstrip() + "\n"


def build_report(source: ReportInput) -> ValidationReport:
    """Apply severity decisions and render one redacted report."""
    status, hard, warnings = _decision(source)
    markdown = redact(_render(source, status, hard, warnings))
    # Redaction can collapse content; insert only the measured integer afterward.
    markdown = markdown.replace(
        _BODY_LINE_COUNT,
        f"- Report body lines: {len(markdown.splitlines())}",
        1,
    )
    return ValidationReport(
        run_id=source.run_id,
        mode=source.mode,
        deployment=source.deployment,
        status=status,
        exit_code=1 if status == "FAIL" else 0,
        markdown=markdown,
        source=source,
    )


def checks_from_rows(rows: Iterable[Mapping[str, Any]]) -> list[CheckResult]:
    """Convert ledger assertion query rows into report checks."""
    return [CheckResult.from_row(row) for row in rows]


def write_report(
    storage_client: Any, bucket: str, report: ValidationReport,
    *, previous_markdown: str | None = None,
) -> str:
    """Publish a run, or refresh its budget without replacing a newer latest.

    Initial publication runs under the lease. A post-lease budget refresh
    replaces latest only when its previous body and storage generation match.
    """
    from google.api_core.exceptions import NotFound, PreconditionFailed

    primary = storage_client.bucket(bucket).blob(report.object_name)
    primary.upload_from_string(report.markdown, content_type="text/markdown")
    if report.mode == "run" and report.status != "SKIPPED":
        latest_name = f"reports/{report.deployment}/latest.md"
        latest = storage_client.bucket(bucket).blob(latest_name)
        if previous_markdown is None:
            latest.upload_from_string(report.markdown, content_type="text/markdown")
        else:
            try:
                latest.reload()
                generation = latest.generation
                if latest.download_as_text() == previous_markdown:
                    latest.upload_from_string(
                        report.markdown, content_type="text/markdown",
                        if_generation_match=generation,
                    )
            except (NotFound, PreconditionFailed):
                # A later publisher or operator owns the pointer now.
                pass
    return f"gs://{bucket}/{report.object_name}"
