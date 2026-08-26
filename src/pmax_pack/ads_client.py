"""Google Ads client construction, account resolution, and retried fetches.

gaarf is the extractor only (KTD1). Credentials are a path, never in-repo.
Path precedence matches env-first: GOOGLE_ADS_CONFIGURATION_FILE_PATH, else
the Cloud Run mount at /secrets/google-ads.yaml.
"""
from __future__ import annotations

import hashlib
import logging
import operator
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any

import tenacity
from google.ads.googleads.errors import GoogleAdsException
from google.api_core.exceptions import (
    DeadlineExceeded,
    ResourceExhausted,
    ServiceUnavailable,
)
from gaarf.api_clients import GoogleAdsApiClient
from gaarf.report_fetcher import AdsReportFetcher

from pmax_pack.config import Config
from pmax_pack.redact import redact

log = logging.getLogger(__name__)

DEFAULT_SECRET_PATH = "/secrets/google-ads.yaml"
RETRY_ATTEMPTS = 5

PROBE_QUERY = """
SELECT
  customer.id AS account_id,
  customer.descriptive_name AS descriptive_name,
  customer.currency_code AS currency_code,
  customer.time_zone AS time_zone
FROM customer
LIMIT 1
"""

# Repeated composite messages whose gaarf RepeatedCompositeParser / EmptyMessageParser
# path collapses populated protos to "Not set". Overlay both subfields from the proto.
COMPOSITE_SUBFIELDS: dict[str, tuple[str, ...]] = {
    "campaign.asset_automation_settings": (
        "asset_automation_type",
        "asset_automation_status",
    ),
    "asset_group.asset_coverage.ad_strength_action_items": (
        "action_item_type",
        "add_asset_details",
    ),
}


def _proto_jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Enum):
        return value.name
    if isinstance(value, (str, int, float, bool)):
        return value
    meta = getattr(type(value), "meta", None)
    if meta is not None:
        return {name: _proto_jsonable(getattr(value, name)) for name in meta.fields}
    name = getattr(value, "name", None)
    if isinstance(name, str) and not isinstance(value, type):
        return name
    return value


def overlay_repeated_composites(
    row: Any,
    fields: list[str],
    column_names: list[str],
    parsed_list: list[Any],
) -> list[Any]:
    """Replace gaarf's collapsed composite cells with list-of-dict subfields."""
    out = list(parsed_list)
    alias_by_field = dict(zip(fields, column_names))
    for field_path, subfields in COMPOSITE_SUBFIELDS.items():
        alias = alias_by_field.get(field_path)
        if alias is None or alias not in column_names:
            continue
        try:
            container = operator.attrgetter(field_path)(row)
        except Exception:
            continue
        items = []
        for item in container:
            items.append(
                {sf: _proto_jsonable(getattr(item, sf, None)) for sf in subfields}
            )
        out[column_names.index(alias)] = items
    return out


def parse_google_ads_row(parser: Any, row: Any) -> list[Any]:
    """GoogleAdsRowParser.parse_ads_row plus composite overlay."""
    parsed = parser.parse_ads_row(row)
    return overlay_repeated_composites(
        row, list(parser.fields), list(parser.column_names), parsed
    )


class AccountResolutionError(Exception):
    """A configured account is missing from the resolved MCC descendant set."""

    def __init__(self, missing: list[str]):
        self.missing = list(missing)
        named = ", ".join(self.missing)
        super().__init__(
            "configured account(s) absent from the resolved MCC set: " + named
        )


class AccountExtractionError(Exception):
    """Extraction failed for a single named account (R19)."""

    def __init__(self, account: str, cause: BaseException | None = None):
        self.account = str(account)
        self.cause = cause
        detail = redact(str(cause)) if cause is not None else ""
        msg = f"extraction failed for account {self.account}"
        if detail:
            msg = f"{msg}: {detail}"
        super().__init__(msg)


@dataclass
class AccountResolution:
    configured: list[str]
    resolved: list[str]


def resolve_credential_path(credential_path: str | None = None) -> str:
    """Explicit path, else GOOGLE_ADS_CONFIGURATION_FILE_PATH, else the mount."""
    if credential_path:
        return credential_path
    env = os.environ.get("GOOGLE_ADS_CONFIGURATION_FILE_PATH")
    if env:
        return env
    return DEFAULT_SECRET_PATH


def credential_fingerprint(path: str) -> str:
    """First 12 hex chars of sha256 over the credential file bytes (KTD5)."""
    data = PathBytes(path)
    return hashlib.sha256(data).hexdigest()[:12]


def PathBytes(path: str) -> bytes:
    with open(path, "rb") as handle:
        return handle.read()


def build_client(credential_path: str | None, api_version: str) -> GoogleAdsApiClient:
    """Construct a gaarf GoogleAdsApiClient pinned to api_version (KTD12)."""
    path = resolve_credential_path(credential_path)
    log.info("building Google Ads client api_version=%s", api_version)
    try:
        return GoogleAdsApiClient(path_to_config=path, version=api_version)
    except Exception:
        log.exception("client construction failed")
        raise


def _report_fetcher(client: Any) -> AdsReportFetcher:
    return AdsReportFetcher(api_client=client)


def probe(credential_path: str | None, account: str, api_version: str) -> dict[str, Any]:
    """Return the customer row for the CLI probe command."""
    client = build_client(credential_path, api_version)
    fetcher = _report_fetcher(client)
    report = fetch_family(fetcher, PROBE_QUERY, account, {})
    rows = _report_dicts(report)
    if not rows:
        raise AccountExtractionError(account, RuntimeError("probe returned no rows"))
    row = rows[0]
    return {
        "account_id": _as_int(row.get("account_id") or row.get("customer_id") or account),
        "descriptive_name": row.get("descriptive_name"),
        "currency_code": row.get("currency_code"),
        "time_zone": row.get("time_zone"),
    }


def _as_int(value: Any) -> int:
    return int(value)


def _as_customer_id(value: Any) -> str:
    return str(int(value))


def resolve_accounts(config: Config, fetcher: Any) -> AccountResolution:
    """Allowlist by default; MCC expand when bulk_expansion is true (R1)."""
    configured = [_as_customer_id(a) for a in config.accounts]
    if not config.bulk_expansion:
        return AccountResolution(configured=configured, resolved=list(configured))
    if not config.mcc:
        raise AccountResolutionError(configured)
    expanded_raw = fetcher.expand_mcc(config.mcc)
    resolved = [_as_customer_id(v) for v in expanded_raw]
    missing = [acct for acct in configured if acct not in set(resolved)]
    if missing:
        raise AccountResolutionError(missing)
    return AccountResolution(configured=configured, resolved=resolved)


def _grpc_code(exc: BaseException) -> Any:
    err = getattr(exc, "error", None)
    if err is None:
        return None
    code = getattr(err, "code", None)
    if callable(code):
        try:
            return code()
        except Exception:
            return None
    return code


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, (ResourceExhausted, ServiceUnavailable, DeadlineExceeded)):
        return True
    if isinstance(exc, GoogleAdsException):
        code = _grpc_code(exc)
        name = str(code) if code is not None else ""
        if "RESOURCE_EXHAUSTED" in name or "UNAVAILABLE" in name or "DEADLINE" in name:
            return True
        try:
            import grpc

            if code in (
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.DEADLINE_EXCEEDED,
            ):
                return True
        except Exception:
            return "RESOURCE_EXHAUSTED" in name
    return False


def _report_dicts(report: Any) -> list[dict[str, Any]]:
    if report is None:
        return []
    if isinstance(report, list):
        return [dict(r) for r in report]
    to_list = getattr(report, "to_list", None)
    if callable(to_list):
        return list(to_list(row_type="dict"))
    return []


def fetch_family(
    fetcher: Any,
    query_text: str,
    account: str,
    macros: dict[str, str] | None,
) -> Any:
    """Fetch one account with retries for quota, unavailable, and deadline only."""
    acct = _as_customer_id(account)
    args = {"macro": dict(macros or {})}

    def _once() -> Any:
        return fetcher.fetch(
            query_text,
            customer_ids=acct,
            args=args,
        )

    original_parse_batch = getattr(fetcher, "_parse_batch", None)

    def _parse_batch_preserving(parser: Any, batch: Any):
        for proto_row in batch:
            yield parse_google_ads_row(parser, proto_row)

    if original_parse_batch is not None:
        fetcher._parse_batch = _parse_batch_preserving
    try:
        try:
            return tenacity.Retrying(
                stop=tenacity.stop_after_attempt(RETRY_ATTEMPTS),
                wait=tenacity.wait_exponential(multiplier=0.01, min=0.01, max=0.2),
                retry=tenacity.retry_if_exception(_is_retryable),
                reraise=True,
            )(_once)
        except AccountExtractionError:
            raise
        except Exception as exc:
            raise AccountExtractionError(acct, exc) from exc
    finally:
        if original_parse_batch is not None:
            fetcher._parse_batch = original_parse_batch
