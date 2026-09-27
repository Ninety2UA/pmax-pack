"""Typed config for pMax Performance Pack.

parse_config(raw) -> Config uses a _require() helper and raises one
ValueError per violation, naming the key. No JSON Schema.

Account ids and mcc accept YAML integers of exactly 10 digits (a
deliberate convenience) but reject floats and anything with dashes.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml

log = logging.getLogger(__name__)

COHORT_BOUNDARY = frozenset(list(range(0, 15)) + [21, 30, 45, 60, 90])
DEFAULT_COHORT_DAYS = [0, 1, 3, 5, 7, 14, 30]
DEFAULT_REGION = "europe-west1"
DEFAULT_API_VERSION = "v25"
DEFAULT_RESTATEMENT_MARGIN_DAYS = 7
DEFAULT_REPORTING_WINDOW_DAYS = 90

# Documented starting defaults for reconciliation tolerance fractions.
# Parity tolerance is frozen against the pinned reference chain.
DEFAULT_TOLERANCE_CAMPAIGN = 0.01
DEFAULT_TOLERANCE_ASSET_VS_CAMPAIGN = 0.0
DEFAULT_TOLERANCE_CROSS_GRAIN = 0.0
# Google campaign scores are rounded to two decimals in pinned query 09.
# Freeze one displayed score unit instead of learning tolerance from live data.
DEFAULT_TOLERANCE_PARITY = 0.01

DEFAULT_DATASETS = {
    "raw": "pmax_raw",
    "marts": "pmax_marts",
    "ops": "pmax_ops",
    "snapshots": "pmax_snapshots",
    "marts_verify": "pmax_marts_verify",
    "parity_scratch": "pmax_parity_scratch",
    "parity_scratch_bq": "pmax_parity_scratch_bq",
    "ci_scratch": "pmax_ci_scratch",
    "ci_scratch_bq": "pmax_ci_scratch_bq",
    "reporting": "pmax_reporting",
    "reporting_verify": "pmax_reporting_verify",
}
SCRATCH_DATASET_SUFFIXES = ("_scratch", "_scratch_bq")
_PRINCIPAL_PATTERN = re.compile(
    r"(?:user|serviceAccount):[A-Za-z0-9._'+-]+@"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)+"
)


def _require(d: dict[str, Any] | None, key: str, named: str) -> Any:
    """Return d[key] or raise ValueError naming `named` (one key per raise)."""
    if not isinstance(d, dict) or d.get(key) in (None, "", [], {}):
        raise ValueError(f"{named}: missing required field")
    return d[key]


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _boundary_message() -> str:
    return "{" + ", ".join(str(x) for x in sorted(COHORT_BOUNDARY)) + "}"


def _as_customer_id(value: Any, named: str) -> str:
    """Accept a 10-digit str or a YAML int of exactly 10 digits; reject floats."""
    if isinstance(value, bool) or isinstance(value, float):
        raise ValueError(
            f"{named}: must be a 10-digit customer id without dashes"
        )
    if isinstance(value, int):
        sid = str(value)
    elif isinstance(value, str):
        sid = value
    else:
        raise ValueError(
            f"{named}: must be a 10-digit customer id without dashes"
        )
    if len(sid) != 10 or not sid.isdigit():
        raise ValueError(
            f"{named}: must be a 10-digit customer id without dashes"
        )
    return sid


def _as_number(value: Any, named: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{named}: must be a number")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{named}: must be a number") from exc


def _as_principals(value: Any, named: str) -> list[str]:
    """Validate unique email principals without normalizing or echoing them."""
    if not isinstance(value, list):
        raise ValueError(f"{named}: must be a list of principal strings")
    seen: set[str] = set()
    for index, item in enumerate(value):
        if not isinstance(item, str) or _PRINCIPAL_PATTERN.fullmatch(item) is None:
            raise ValueError(
                f"{named}[{index}]: principal must be user:<email> or "
                "serviceAccount:<email> with a plain email"
            )
        if item.lower() in seen:
            raise ValueError(f"{named}[{index}]: duplicate principal")
        seen.add(item.lower())
    return list(value)


@dataclass
class Deployment:
    project: str
    region: str = DEFAULT_REGION


@dataclass
class Datasets:
    raw: str = DEFAULT_DATASETS["raw"]
    marts: str = DEFAULT_DATASETS["marts"]
    ops: str = DEFAULT_DATASETS["ops"]
    snapshots: str = DEFAULT_DATASETS["snapshots"]
    parity_scratch: str = DEFAULT_DATASETS["parity_scratch"]
    parity_scratch_bq: str = DEFAULT_DATASETS["parity_scratch_bq"]
    ci_scratch: str = DEFAULT_DATASETS["ci_scratch"]
    ci_scratch_bq: str = DEFAULT_DATASETS["ci_scratch_bq"]
    marts_verify: str = DEFAULT_DATASETS["marts_verify"]
    reporting: str = DEFAULT_DATASETS["reporting"]
    reporting_verify: str = DEFAULT_DATASETS["reporting_verify"]


@dataclass
class Buckets:
    report_bucket: str
    config_bucket: str


@dataclass
class Tolerances:
    """Reconciliation tolerance fractions (0.01 = 1 percent)."""

    campaign_reconciliation: float = DEFAULT_TOLERANCE_CAMPAIGN
    asset_vs_campaign: float = DEFAULT_TOLERANCE_ASSET_VS_CAMPAIGN
    cross_grain: float = DEFAULT_TOLERANCE_CROSS_GRAIN
    parity: float = DEFAULT_TOLERANCE_PARITY


@dataclass
class Config:
    accounts: list[str]
    bulk_expansion: bool
    start_date: date
    restatement_margin_days: int
    cohort_days: list[int]
    tolerances: Tolerances
    deployment: Deployment
    datasets: Datasets
    buckets: Buckets
    api_version: str = DEFAULT_API_VERSION
    mcc: str | None = None
    timezone_override: str | None = None
    storage: str = "window"
    reporting_window_days: int = DEFAULT_REPORTING_WINDOW_DAYS
    env: str = "prod"
    editors: list[str] = field(default_factory=list, repr=False)
    looker_service_agents: list[str] = field(default_factory=list, repr=False)


def parse_config(
    raw: dict[str, Any],
    run_date: date | None = None,
) -> Config:
    """Validate a raw config dict and return a typed Config.

    Raises one ValueError per violation, with the message naming the key.

    Account ids and mcc keep accepting YAML integers of exactly 10 digits
    (a deliberate convenience) but reject floats and anything with dashes.
    """
    if not isinstance(raw, dict):
        raise ValueError("root: config must be a mapping")

    accounts_raw = raw.get("accounts")
    if not isinstance(accounts_raw, list) or not accounts_raw:
        raise ValueError(
            "accounts: must be a non-empty list of 10-digit customer ids "
            "without dashes"
        )
    accounts: list[str] = []
    for item in accounts_raw:
        try:
            accounts.append(_as_customer_id(item, "accounts"))
        except ValueError:
            raise ValueError(
                "accounts: must be a non-empty list of 10-digit customer ids "
                "without dashes"
            ) from None

    bulk_raw = raw.get("bulk_expansion", False)
    if bulk_raw is None:
        bulk_expansion = False
    elif isinstance(bulk_raw, bool):
        bulk_expansion = bulk_raw
    else:
        raise ValueError("bulk_expansion: must be true or false")

    mcc_raw = raw.get("mcc")
    mcc: str | None
    if mcc_raw in (None, ""):
        mcc = None
    else:
        mcc = _as_customer_id(mcc_raw, "mcc")
    if bulk_expansion and mcc is None:
        raise ValueError("mcc: required when bulk_expansion is true")

    storage = raw.get("storage", "window")
    if storage not in ("window", "incremental"):
        raise ValueError("storage: must be window or incremental")

    reporting_window_days = raw.get(
        "reporting_window_days", DEFAULT_REPORTING_WINDOW_DAYS
    )
    if (
        isinstance(reporting_window_days, bool)
        or not isinstance(reporting_window_days, int)
        or reporting_window_days <= 0
    ):
        raise ValueError("reporting_window_days: must be a positive int")

    env = raw.get("env", "prod")
    if env not in ("prod", "verify", "parity", "ci"):
        raise ValueError("env: must be prod, verify, parity, or ci")
    editors = _as_principals(raw.get("editors", []), "editors")
    looker_service_agents = _as_principals(
        raw.get("looker_service_agents", []), "looker_service_agents"
    )

    run = run_date or _utc_today()
    has_start_date = raw.get("start_date") not in (None, "")
    configured_start: date | None = None
    if has_start_date:
        try:
            configured_start = date.fromisoformat(str(raw["start_date"]))
        except ValueError as exc:
            raise ValueError("start_date: must be an ISO date (YYYY-MM-DD)") from exc

    if storage == "window" or configured_start is None:
        start = run - timedelta(days=reporting_window_days)
        if storage == "incremental":
            # Month-aligned so every chunk is complete; initial depth is between
            # the reporting window and the reporting window plus 30 days.
            start = start.replace(day=1)
    else:
        start = configured_start
        if start.day != 1:
            raise ValueError(
                "start_date: must be the first day of a month in incremental mode"
            )

    restatement = raw.get("restatement_margin_days", DEFAULT_RESTATEMENT_MARGIN_DAYS)
    if restatement is None:
        restatement_margin_days = DEFAULT_RESTATEMENT_MARGIN_DAYS
    elif isinstance(restatement, bool) or not isinstance(restatement, int):
        raise ValueError("restatement_margin_days: must be an int")
    else:
        restatement_margin_days = restatement
    if restatement_margin_days < 0:
        raise ValueError("restatement_margin_days: must be non-negative")

    days_raw = raw.get("cohort_days")
    if days_raw in (None, ""):
        cohort_days = list(DEFAULT_COHORT_DAYS)
    else:
        if not isinstance(days_raw, list) or not days_raw:
            raise ValueError(
                "cohort_days: must be a non-empty list from the boundary set "
                + _boundary_message()
            )
        cohort_days = []
        for item in days_raw:
            if isinstance(item, bool) or not isinstance(item, int):
                raise ValueError(
                    "cohort_days: must be integers from the boundary set "
                    + _boundary_message()
                )
            if item not in COHORT_BOUNDARY:
                raise ValueError(
                    "cohort_days: "
                    f"{item} is not in the boundary set {_boundary_message()}"
                )
            cohort_days.append(item)

    t_raw = raw.get("tolerances") or {}
    if t_raw in ("", []):
        t_raw = {}
    if not isinstance(t_raw, dict):
        raise ValueError("tolerances: must be a mapping of fraction defaults")
    tolerances = Tolerances(
        campaign_reconciliation=_as_number(
            t_raw.get("campaign_reconciliation", DEFAULT_TOLERANCE_CAMPAIGN),
            "tolerances.campaign_reconciliation",
        ),
        asset_vs_campaign=_as_number(
            t_raw.get("asset_vs_campaign", DEFAULT_TOLERANCE_ASSET_VS_CAMPAIGN),
            "tolerances.asset_vs_campaign",
        ),
        cross_grain=_as_number(
            t_raw.get("cross_grain", DEFAULT_TOLERANCE_CROSS_GRAIN),
            "tolerances.cross_grain",
        ),
        parity=_as_number(
            t_raw.get("parity", DEFAULT_TOLERANCE_PARITY),
            "tolerances.parity",
        ),
    )

    tz = raw.get("timezone_override")
    if tz in (None, ""):
        timezone_override = None
    else:
        timezone_override = str(tz)
        try:
            ZoneInfo(timezone_override)
        except (ZoneInfoNotFoundError, ValueError, KeyError) as exc:
            raise ValueError(
                "timezone_override: must be a valid IANA name"
            ) from exc

    dep = raw.get("deployment")
    project = _require(
        dep if isinstance(dep, dict) else None,
        "project",
        "deployment.project",
    )
    region = DEFAULT_REGION
    if isinstance(dep, dict) and dep.get("region") not in (None, ""):
        region = str(dep["region"])
    deployment = Deployment(project=str(project), region=region)

    ds_raw = raw.get("datasets") or {}
    if not isinstance(ds_raw, dict):
        raise ValueError("datasets: must be a mapping")
    dataset_values: dict[str, str] = {}
    for field, default in DEFAULT_DATASETS.items():
        configured = ds_raw.get(field)
        if configured is not None and not isinstance(configured, str):
            raise ValueError(
                f"datasets.{field}: must be a string identifier, not "
                f"{type(configured).__name__}"
            )
        value = default if configured is None else configured
        # Dataset naming rule per docs.cloud.google.com/bigquery/docs/datasets
        # ("Name datasets", read 2026-09-18 through the Developer Knowledge MCP):
        # up to 1,024 characters; letters, numbers, and underscores; no spaces
        # or special characters.
        if not re.fullmatch(r"^[A-Za-z0-9_]{1,1024}$", value):
            raise ValueError(
                f"datasets.{field}: must be a BigQuery dataset identifier "
                "of 1 to 1024 letters, numbers, or underscores"
            )
        dataset_values[field] = value
    datasets = Datasets(**dataset_values)
    seen_datasets: dict[str, str] = {}
    for field, value in dataset_values.items():
        if value in seen_datasets:
            raise ValueError(
                f"datasets.{field}: must be distinct from "
                f"datasets.{seen_datasets[value]}"
            )
        seen_datasets[value] = field
    for field, default in DEFAULT_DATASETS.items():
        suffix = next(
            (
                candidate
                for candidate in SCRATCH_DATASET_SUFFIXES
                if default.endswith(candidate)
            ),
            None,
        )
        if suffix is not None and not dataset_values[field].endswith(suffix):
            raise ValueError(f"datasets.{field}: must end with {suffix}")

    buckets_raw = raw.get("buckets")
    report_bucket = _require(
        buckets_raw if isinstance(buckets_raw, dict) else None,
        "report_bucket",
        "buckets.report_bucket",
    )
    config_bucket = _require(
        buckets_raw if isinstance(buckets_raw, dict) else None,
        "config_bucket",
        "buckets.config_bucket",
    )
    buckets = Buckets(
        report_bucket=str(report_bucket),
        config_bucket=str(config_bucket),
    )

    api_version = str(raw.get("api_version") or DEFAULT_API_VERSION)

    if reporting_window_days not in COHORT_BOUNDARY:
        log.warning(
            "reporting_window_days: %s is not a lag bucket boundary; campaign "
            "and asset-group reporting-window rungs will be unavailable",
            reporting_window_days,
        )
    minimum_window = max(cohort_days) + 1 + restatement_margin_days
    if reporting_window_days < minimum_window:
        log.warning(
            "reporting_window_days: %s is below the largest cohort_days rung "
            "plus one plus restatement_margin_days (%s); some cohort readings "
            "will be unavailable",
            reporting_window_days,
            minimum_window,
        )
    if storage == "incremental" and not has_start_date:
        log.warning(
            "start_date: absent in incremental mode; using month-aligned "
            "default %s, the first day of the month containing run date "
            "minus reporting_window_days (%s days)",
            start.isoformat(),
            reporting_window_days,
        )
    elif storage == "window" and has_start_date:
        log.warning("start_date: ignored in window mode")

    return Config(
        accounts=accounts,
        bulk_expansion=bulk_expansion,
        start_date=start,
        restatement_margin_days=restatement_margin_days,
        cohort_days=cohort_days,
        tolerances=tolerances,
        deployment=deployment,
        datasets=datasets,
        buckets=buckets,
        api_version=api_version,
        mcc=mcc,
        timezone_override=timezone_override,
        storage=storage,
        reporting_window_days=reporting_window_days,
        env=env,
        editors=editors,
        looker_service_agents=looker_service_agents,
    )


def load_config(
    source: str,
    *,
    storage_client: Any = None,
    run_date: date | None = None,
) -> Config:
    """Load YAML from a local path or a gs:// URI, then parse_config.

    The storage client is injectable so tests mock GCS. When omitted, a
    google.cloud.storage.Client is constructed.
    """
    if source.startswith("gs://"):
        rest = source[len("gs://") :]
        bucket_name, sep, blob_name = rest.partition("/")
        if not sep or not bucket_name or not blob_name:
            raise ValueError("source: gs:// URI must be gs://bucket/object")
        client = storage_client
        if client is None:
            from google.cloud import storage as gcs

            client = gcs.Client()
        text = client.bucket(bucket_name).blob(blob_name).download_as_text()
        raw = yaml.safe_load(text) or {}
        return parse_config(raw, run_date=run_date)

    path = Path(source)
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return parse_config(raw, run_date=run_date)
