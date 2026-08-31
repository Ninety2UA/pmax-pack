"""Ads client tests: probe, account resolution, retries, credential fingerprint.

Proof-first: red runs recorded in the U2 report. No live Google Ads calls.
"""
from __future__ import annotations

import hashlib
import io
import logging
import traceback
from pathlib import Path
from types import SimpleNamespace

import pytest
import tenacity
from google.api_core.exceptions import (
    DeadlineExceeded,
    ResourceExhausted,
    ServiceUnavailable,
)

from pmax_pack.ads_client import (
    AccountExtractionError,
    AccountResolutionError,
    build_client,
    credential_fingerprint,
    fetch_family,
    probe,
    resolve_accounts,
    resolve_credential_path,
)
from pmax_pack.config import parse_config
from pmax_pack.redact import install_redaction

CANARY_REFRESH = "1/" + "/0canaryCANARY0canaryCANARY0000"
CANARY_DEV = "canaryDevTokenValue0001"
C_VAL2 = "GOC" + "SPX-" + "canaryClientSecret0001"
_REFRESH_KEY = "refresh" + "_token"
_DEVELOPER_KEY = "developer" + "_token"
_CLIENT_SECRET_KEY = "client" + "_secret"

ACCOUNT = "1234567890"
MCC = "3456789012"
OTHER = "2345678901"


def _valid_raw(**overrides):
    raw = {
        "accounts": [ACCOUNT],
        "bulk_expansion": False,
        "deployment": {"project": "example-project", "region": "europe-west1"},
        "buckets": {
            "report_bucket": "report-bucket",
            "config_bucket": "config-bucket",
        },
        "api_version": "v25",
    }
    raw.update(overrides)
    return raw


class FakeReport:
    def __init__(self, rows: list[dict]):
        self._rows = rows
        self.results = [[v for v in r.values()] for r in rows]
        self.column_names = list(rows[0].keys()) if rows else []

    def to_list(self, row_type: str = "list", **kwargs):
        if row_type == "dict":
            return [dict(r) for r in self._rows]
        return self.results


class FakeFetcher:
    def __init__(
        self,
        rows=None,
        expand=None,
        error=None,
        errors_by_account=None,
        fail_times: int = 0,
        fail_exc: BaseException | None = None,
    ):
        self.rows = rows or []
        self.expand = expand
        self.error = error
        self.errors_by_account = errors_by_account or {}
        self.fail_times = fail_times
        self.fail_exc = fail_exc or ResourceExhausted("quota")
        self.calls: list[tuple] = []
        self._fails_left = fail_times

    def expand_mcc(self, customer_ids, customer_ids_query=None):
        self.calls.append(("expand_mcc", customer_ids, customer_ids_query))
        return list(self.expand)

    def fetch(self, query_specification, customer_ids=None, args=None, **kwargs):
        account = customer_ids
        if isinstance(customer_ids, list):
            account = customer_ids[0]
        self.calls.append(("fetch", str(account), query_specification, args))
        if self._fails_left > 0:
            self._fails_left -= 1
            raise self.fail_exc
        if str(account) in self.errors_by_account:
            raise self.errors_by_account[str(account)]
        if self.error is not None:
            raise self.error
        return FakeReport(list(self.rows))


def test_resolve_credential_path_env_first(monkeypatch, tmp_path):
    planted = tmp_path / "ads.yaml"
    planted.write_text("x: 1\n", encoding="utf-8")
    monkeypatch.setenv("GOOGLE_ADS_CONFIGURATION_FILE_PATH", str(planted))
    assert resolve_credential_path(None) == str(planted)


def test_resolve_credential_path_explicit_wins_over_env(monkeypatch, tmp_path):
    env_path = tmp_path / "env.yaml"
    env_path.write_text("x: 1\n", encoding="utf-8")
    explicit = tmp_path / "explicit.yaml"
    explicit.write_text("x: 2\n", encoding="utf-8")
    monkeypatch.setenv("GOOGLE_ADS_CONFIGURATION_FILE_PATH", str(env_path))
    assert resolve_credential_path(str(explicit)) == str(explicit)


def test_resolve_credential_path_defaults_to_mounted_secret(monkeypatch):
    monkeypatch.delenv("GOOGLE_ADS_CONFIGURATION_FILE_PATH", raising=False)
    assert resolve_credential_path(None) == "/secrets/google-ads.yaml"


def test_credential_fingerprint_is_first_12_hex_of_sha256(tmp_path):
    path = tmp_path / "cred.yaml"
    payload = b"not-a-real-credential-file\n"
    path.write_bytes(payload)
    expected = hashlib.sha256(payload).hexdigest()[:12]
    assert credential_fingerprint(str(path)) == expected


def test_credential_fingerprint_missing_file_raises(tmp_path):
    missing = tmp_path / "absent.yaml"
    with pytest.raises(FileNotFoundError):
        credential_fingerprint(str(missing))


def test_allowlist_resolve_without_expansion():
    cfg = parse_config(_valid_raw(accounts=[ACCOUNT, OTHER]))
    fetcher = FakeFetcher(expand=[MCC])
    result = resolve_accounts(cfg, fetcher)
    assert result.configured == [ACCOUNT, OTHER]
    assert result.resolved == [ACCOUNT, OTHER]
    assert not any(c[0] == "expand_mcc" for c in fetcher.calls)


def test_configured_account_absent_from_mcc_raises_naming_account():
    cfg = parse_config(
        _valid_raw(
            accounts=[ACCOUNT, OTHER],
            bulk_expansion=True,
            mcc=MCC,
        )
    )
    fetcher = FakeFetcher(expand=[ACCOUNT])
    with pytest.raises(AccountResolutionError) as exc:
        resolve_accounts(cfg, fetcher)
    msg = str(exc.value)
    assert OTHER in msg
    assert ACCOUNT not in msg or OTHER in msg


def test_bulk_expansion_records_configured_and_resolved():
    cfg = parse_config(
        _valid_raw(
            accounts=[ACCOUNT],
            bulk_expansion=True,
            mcc=MCC,
        )
    )
    fetcher = FakeFetcher(expand=[ACCOUNT, OTHER])
    result = resolve_accounts(cfg, fetcher)
    assert result.configured == [ACCOUNT]
    assert ACCOUNT in result.resolved
    assert OTHER in result.resolved
    assert any(c[0] == "expand_mcc" for c in fetcher.calls)


def test_fetch_quota_error_is_retried_then_succeeds():
    fetcher = FakeFetcher(
        rows=[{"account_id": int(ACCOUNT), "date": "2026-08-20"}],
        fail_times=2,
        fail_exc=ResourceExhausted("RESOURCE_EXHAUSTED"),
    )
    report = fetch_family(
        fetcher,
        "SELECT customer.id FROM customer",
        ACCOUNT,
        {},
        retry_wait=tenacity.wait_none(),
    )
    rows = report.to_list(row_type="dict")
    assert len(rows) == 1
    assert fetcher.calls[0][0] == "fetch"
    assert sum(1 for c in fetcher.calls if c[0] == "fetch") == 3


def test_fetch_unavailable_and_deadline_are_retried():
    for exc in (
        ServiceUnavailable("UNAVAILABLE"),
        DeadlineExceeded("DEADLINE_EXCEEDED"),
    ):
        fetcher = FakeFetcher(
            rows=[{"account_id": 1}],
            fail_times=1,
            fail_exc=exc,
        )
        fetch_family(
            fetcher,
            "SELECT customer.id FROM customer",
            ACCOUNT,
            {},
            retry_wait=tenacity.wait_none(),
        )
        assert sum(1 for c in fetcher.calls if c[0] == "fetch") == 2


def test_fetch_retry_exhaustion_raises_account_extraction_error():
    fetcher = FakeFetcher(
        fail_times=10,
        fail_exc=ResourceExhausted("quota"),
    )
    with pytest.raises(AccountExtractionError) as exc:
        fetch_family(
            fetcher,
            "SELECT customer.id FROM customer",
            ACCOUNT,
            {},
            retry_wait=tenacity.wait_none(),
        )
    assert ACCOUNT in str(exc.value)
    assert exc.value.account == ACCOUNT
    # reraise=True: the original quota error is the cause, never tenacity's RetryError
    assert isinstance(exc.value.__cause__, ResourceExhausted)
    assert "quota" in str(exc.value.__cause__)
    assert sum(1 for c in fetcher.calls if c[0] == "fetch") == 5


def test_fetch_non_retryable_error_is_not_retried():
    fetcher = FakeFetcher(error=ValueError("bad query"))
    with pytest.raises(AccountExtractionError):
        fetch_family(fetcher, "SELECT bogus FROM campaign", ACCOUNT, {})
    assert sum(1 for c in fetcher.calls if c[0] == "fetch") == 1


def test_production_retry_policy_spans_real_quota_window():
    import pmax_pack.ads_client as ads_client

    wait = ads_client.RETRY_WAIT
    assert wait.multiplier == 10
    assert wait.min >= 10
    assert wait.max >= 60


def test_production_retry_policy_accumulates_two_minute_window():
    import pmax_pack.ads_client as ads_client

    waits = [
        ads_client.RETRY_WAIT(SimpleNamespace(attempt_number=attempt))
        for attempt in range(1, ads_client.RETRY_ATTEMPTS)
    ]
    assert waits == [10, 20, 40, 60]
    assert sum(waits) >= 120


def test_fetch_without_override_reads_production_wait_at_call_time(monkeypatch):
    import pmax_pack.ads_client as ads_client

    class RecordingWait:
        def __init__(self):
            self.attempts: list[int] = []

        def __call__(self, retry_state):
            self.attempts.append(retry_state.attempt_number)
            return 0

    wait = RecordingWait()
    monkeypatch.setattr(ads_client, "RETRY_WAIT", wait)
    fetcher = FakeFetcher(
        rows=[{"account_id": int(ACCOUNT)}],
        fail_times=2,
        fail_exc=ResourceExhausted("quota"),
    )
    fetch_family(fetcher, "SELECT customer.id FROM customer", ACCOUNT, {})
    assert wait.attempts == [1, 2]


def test_fetch_one_account_per_call():
    fetcher = FakeFetcher(rows=[{"account_id": int(ACCOUNT)}])
    fetch_family(fetcher, "SELECT customer.id FROM customer", ACCOUNT, {"start_date": "2026-08-01"})
    kind, account, query, args = fetcher.calls[0]
    assert kind == "fetch"
    assert account == ACCOUNT
    assert not isinstance(account, list)


def test_canary_build_client_real_library_redacts(
    tmp_path, capsys, caplog, monkeypatch
):
    """Broken YAML with runtime-assembled canaries hits the real google-ads client.

    logging: is a JSON string so google.ads.googleads.config applies dictConfig
    before validate_dict raises (missing use_proto_plus).
    """
    install_redaction()
    logging_json = (
        '{"version": 1, "disable_existing_loggers": false, '
        '"formatters": {"plain": {"format": "%(levelname)s %(name)s %(message)s"}}, '
        '"handlers": {"h": {"class": "logging.StreamHandler", "formatter": "plain"}}, '
        '"root": {"handlers": ["h"], "level": "DEBUG"}}'
    )
    cred = tmp_path / "broken.yaml"
    cred.write_text(
        f"{_REFRESH_KEY}: {CANARY_REFRESH}\n"
        f"{_DEVELOPER_KEY}: {CANARY_DEV}\n"
        f"{_CLIENT_SECRET_KEY}: {C_VAL2}\n"
        f"logging: '{logging_json}'\n",
        encoding="utf-8",
    )
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setLevel(logging.DEBUG)
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        with caplog.at_level(logging.DEBUG):
            try:
                build_client(str(cred), "v25")
                raised = None
            except Exception as exc:
                raised = exc
                logging.getLogger("pmax_pack.ads_client").exception(
                    "client construction failed"
                )
        assert raised is not None
        captured = capsys.readouterr()
        rendered = "".join(
            traceback.format_exception(type(raised), raised, raised.__traceback__)
        )
        blob = (
            captured.out
            + captured.err
            + caplog.text
            + buf.getvalue()
            + rendered
            + str(raised)
        )
        assert CANARY_REFRESH not in blob
        assert CANARY_DEV not in blob
        assert C_VAL2 not in blob
        assert "1//0canary" not in blob
        handler = logging.getHandlerByName("h")
        assert handler is not None
    finally:
        root.removeHandler(handler)


def test_canary_stderr_leak_mutation_is_visible_in_one_capture(capsys):
    import sys

    print(CANARY_REFRESH, file=sys.stderr)
    captured = capsys.readouterr()
    blob = captured.out + captured.err
    assert CANARY_REFRESH in blob


def test_probe_returns_customer_row(monkeypatch):
    row = {
        "account_id": int(ACCOUNT),
        "descriptive_name": "Example",
        "currency_code": "EUR",
        "time_zone": "Europe/Zagreb",
    }

    class ProbeFetcher(FakeFetcher):
        pass

    fetcher = ProbeFetcher(rows=[row])

    def _build(path, api_version):
        return object()

    monkeypatch.setattr("pmax_pack.ads_client.build_client", _build)
    monkeypatch.setattr(
        "pmax_pack.ads_client._report_fetcher", lambda client: fetcher
    )
    got = probe(str(Path("/tmp/x")), ACCOUNT, "v25")
    assert got["account_id"] == int(ACCOUNT)
    assert got["currency_code"] == "EUR"
    assert got["time_zone"] == "Europe/Zagreb"
    assert got["descriptive_name"] == "Example"
