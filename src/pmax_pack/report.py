"""Deterministic validation report assembly and GCS publication.

The report is a pure fold over normalized ledger assertion rows and run
metadata. It performs the R19 severity decision before rendering, redacts the
entire document once at the final boundary, and publishes one replaceable
object per run id. Only executed daily ``run`` mode advances ``latest.md``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable, Mapping, Sequence

from pmax_pack.redact import redact

HARD = "HARD"
SOFT = "SOFT"


def _text(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _account(value: Any) -> str:
    return str(int(value)) if isinstance(value, int) else str(value)


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
    return [f"- {_text(value)}" for value in values]


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
        f"- Run ID: `{source.run_id}`",
        f"- Mode: `{source.mode}`",
        f"- Dry run: {'yes' if source.dry_run else 'no'}",
        f"- As of: `{source.as_of.isoformat()}`",
        f"- Image digest: `{source.image_digest}`",
        f"- Credential fingerprint: `{source.credential_fingerprint}`",
        f"- Query hash: `{source.query_hash}`",
        f"- API version: `{source.api_version}`",
        f"- Reference commit: `{source.reference_commit}`",
        f"- SQL files resolved: {source.sql_files_resolved}",
        "",
        "## Accounts",
        "",
        "Configured: " + (", ".join(source.configured_accounts) or "none"),
        "",
        "Resolved: " + (", ".join(source.resolved_accounts) or "none"),
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
            if metric.stale:
                result = "WARN"
            elif metric.row_count == 0:
                result = "INFO (empty)"
            else:
                result = "PASS"
            lines.append(
                f"| {metric.table} | {metric.row_count:,} | "
                f"{_text(metric.fresh_through)} | "
                f"{_text(metric.expected_fresh_through)} | {result} |"
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
            lines.append(
                f"| {check.name} | {check.severity.upper()} | "
                f"{'PASS' if check.passed else 'FAIL'} | {_text(check.observed)} | "
                f"{_text(check.expected)} | {_text(check.detail)} |"
            )
    else:
        lines.append("| No assertion rows supplied | INFO | - | - | - | - |")

    lines.extend(["", "## Asset participation ratios (informational)", ""])
    if source.asset_participation:
        for item in source.asset_participation:
            ratio = "-" if item.ratio is None else f"{item.ratio:.6f}"
            lines.append(
                f"- account={item.account_id}, network={item.ad_network_type}, "
                f"metric={item.metric}, asset_sum={item.asset_sum:.6f}, "
                f"campaign_truth={item.campaign_truth:.6f}, ratio={ratio}"
            )
    else:
        lines.append("None reported.")

    lines.extend(["", "## Unknown-lag share", ""])
    if source.unknown_lag:
        lines.extend(
            f"- account={_text(item.get('account_id'))}, "
            f"basis={_text(item.get('basis') or item.get('metric_basis'))}, "
            f"share={_text(item.get('share'))}"
            for item in source.unknown_lag
        )
    else:
        lines.append("None reported.")

    lines.extend(["", "## Assumed-current share by account", ""])
    if source.assumed_current:
        lines.extend(
            f"- account={item.account_id}, cells={item.cells:,}/"
            f"{item.total_cells:,}, share={item.share:.6f}"
            for item in source.assumed_current
        )
    else:
        lines.append("None reported.")

    lines.extend(["", "## Cohort coverage", ""])
    if source.coverage:
        lines.extend(
            f"- provenance={item.provenance}, maturity={item.maturity}, "
            f"cells={item.cells:,}/{item.total_cells:,}, share={item.share:.6f}"
            for item in source.coverage
        )
    else:
        lines.append("None reported.")

    sections: list[tuple[str, Iterable[str]]] = [
        (
            "Snapshot gaps and stale cells",
            [
                *(f"snapshot gap: {item}" for item in source.snapshot_gaps),
                *(f"stale cell: {item}" for item in source.stale_cells),
            ],
        ),
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
                f"- Result: {source.parity.result}",
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
    """Apply R19 decisions and render one redacted report."""
    status, hard, warnings = _decision(source)
    markdown = redact(_render(source, status, hard, warnings))
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


def write_report(storage_client: Any, bucket: str, report: ValidationReport) -> str:
    """Write or replace this run object and advance latest for executed runs."""
    primary = storage_client.bucket(bucket).blob(report.object_name)
    primary.upload_from_string(report.markdown, content_type="text/markdown")
    if report.mode == "run" and report.status != "SKIPPED":
        latest_name = f"reports/{report.deployment}/latest.md"
        storage_client.bucket(bucket).blob(latest_name).upload_from_string(
            report.markdown,
            content_type="text/markdown",
        )
    return f"gs://{bucket}/{report.object_name}"
