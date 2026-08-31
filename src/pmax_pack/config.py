"""Typed config for pMax Performance Pack.

parse_config(raw) -> Config uses a _require() helper and raises one
ValueError per violation, naming the key. No JSON Schema.

Account ids and mcc accept YAML integers of exactly 10 digits (a
deliberate convenience) but reject floats and anything with dashes.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml

COHORT_BOUNDARY = frozenset(list(range(1, 15)) + [21, 30, 45, 60, 90])
DEFAULT_COHORT_DAYS = [1, 3, 7, 14, 30, 60, 90]
DEFAULT_REGION = "europe-west1"
DEFAULT_API_VERSION = "v25"
DEFAULT_RESTATEMENT_MARGIN_DAYS = 7
DEFAULT_CHECKPOINT_START_DATE = "default-90d"

# Reconciliation tolerance fractions (U4 freezes parity from the pinned chain;
# these are documented starting defaults).
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
}
SCRATCH_DATASET_SUFFIXES = ("_scratch", "_scratch_bq")


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
    checkpoint_start_date: str = DEFAULT_CHECKPOINT_START_DATE


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

    run = run_date or _utc_today()
    if raw.get("start_date") in (None, ""):
        start = run - timedelta(days=90)
        checkpoint_start_date = DEFAULT_CHECKPOINT_START_DATE
    else:
        try:
            start = date.fromisoformat(str(raw["start_date"]))
        except ValueError as exc:
            raise ValueError("start_date: must be an ISO date (YYYY-MM-DD)") from exc
        checkpoint_start_date = start.isoformat()

    restatement = raw.get("restatement_margin_days", DEFAULT_RESTATEMENT_MARGIN_DAYS)
    if restatement is None:
        restatement_margin_days = DEFAULT_RESTATEMENT_MARGIN_DAYS
    elif isinstance(restatement, bool) or not isinstance(restatement, int):
        raise ValueError("restatement_margin_days: must be an int")
    else:
        restatement_margin_days = restatement

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
    datasets = Datasets(
        raw=str(ds_raw.get("raw") or DEFAULT_DATASETS["raw"]),
        marts=str(ds_raw.get("marts") or DEFAULT_DATASETS["marts"]),
        ops=str(ds_raw.get("ops") or DEFAULT_DATASETS["ops"]),
        snapshots=str(ds_raw.get("snapshots") or DEFAULT_DATASETS["snapshots"]),
        parity_scratch=str(
            ds_raw.get("parity_scratch") or DEFAULT_DATASETS["parity_scratch"]
        ),
        parity_scratch_bq=str(
            ds_raw.get("parity_scratch_bq") or DEFAULT_DATASETS["parity_scratch_bq"]
        ),
        ci_scratch=str(ds_raw.get("ci_scratch") or DEFAULT_DATASETS["ci_scratch"]),
        ci_scratch_bq=str(
            ds_raw.get("ci_scratch_bq") or DEFAULT_DATASETS["ci_scratch_bq"]
        ),
        marts_verify=str(
            ds_raw.get("marts_verify") or DEFAULT_DATASETS["marts_verify"]
        ),
    )
    dataset_values = {
        field: getattr(datasets, field) for field in DEFAULT_DATASETS
    }
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
        checkpoint_start_date=checkpoint_start_date,
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
