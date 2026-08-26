"""Config parser tests. Proof-first: red runs recorded in the U1 report."""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest
import yaml

from pmax_pack.config import load_config, parse_config


def _valid_raw(**overrides):
    raw = {
        "accounts": ["1234567890"],
        "bulk_expansion": False,
        "deployment": {
            "project": "your-project-id",
            "region": "europe-west1",
        },
        "buckets": {
            "report_bucket": "your-report-bucket",
            "config_bucket": "your-config-bucket",
        },
    }
    raw.update(overrides)
    return raw


def test_cohort_day_28_rejected_with_boundary_set():
    raw = _valid_raw(cohort_days=[1, 28, 30])
    with pytest.raises(ValueError) as exc:
        parse_config(raw)
    msg = str(exc.value)
    assert "cohort_days" in msg
    assert (
        "{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 21, 30, 45, 60, 90}"
        in msg
    )


def test_d1_bulk_expansion_string_false_rejected():
    raw = _valid_raw(bulk_expansion="false")
    with pytest.raises(ValueError) as exc:
        parse_config(raw)
    assert str(exc.value) == "bulk_expansion: must be true or false"


def test_d2_mcc_with_dashes_rejected():
    raw = _valid_raw(mcc="123-456-7890")
    with pytest.raises(ValueError) as exc:
        parse_config(raw)
    assert "mcc" in str(exc.value)


def test_d2_timezone_override_invalid_iana_rejected():
    raw = _valid_raw(timezone_override="Mars/Olympus")
    with pytest.raises(ValueError) as exc:
        parse_config(raw)
    assert "timezone_override" in str(exc.value)


def test_d4_cohort_days_rejects_float():
    raw = _valid_raw(cohort_days=[7.0])
    with pytest.raises(ValueError) as exc:
        parse_config(raw)
    assert "cohort_days" in str(exc.value)


def test_d4_cohort_days_rejects_string():
    raw = _valid_raw(cohort_days=["7"])
    with pytest.raises(ValueError) as exc:
        parse_config(raw)
    assert str(exc.value).startswith(
        "cohort_days: must be integers from the boundary set"
    )


def test_d4_restatement_margin_days_rejects_string():
    raw = _valid_raw(restatement_margin_days="7")
    with pytest.raises(ValueError) as exc:
        parse_config(raw)
    assert "restatement_margin_days" in str(exc.value)


def test_d4_tolerance_conversion_failure_names_key():
    raw = _valid_raw(tolerances={"parity": "not-a-number"})
    with pytest.raises(ValueError) as exc:
        parse_config(raw)
    assert str(exc.value) == "tolerances.parity: must be a number"


def test_d4_accounts_accepts_ten_digit_int():
    cfg = parse_config(_valid_raw(accounts=[1234567890]))
    assert cfg.accounts == ["1234567890"]


def test_d4_accounts_rejects_float():
    raw = _valid_raw(accounts=[1234567890.0])
    with pytest.raises(ValueError) as exc:
        parse_config(raw)
    assert "accounts" in str(exc.value)


def test_d4_mcc_accepts_ten_digit_int():
    cfg = parse_config(_valid_raw(mcc=1234567890, bulk_expansion=True))
    assert cfg.mcc == "1234567890"


def test_d4_mcc_rejects_float():
    raw = _valid_raw(mcc=1234567890.0, bulk_expansion=True)
    with pytest.raises(ValueError) as exc:
        parse_config(raw)
    assert "mcc" in str(exc.value)


def test_bulk_expansion_without_mcc_rejected():
    raw = _valid_raw(bulk_expansion=True)
    raw.pop("mcc", None)
    with pytest.raises(ValueError) as exc:
        parse_config(raw)
    assert "mcc" in str(exc.value)


def test_missing_deployment_project_rejected():
    raw = _valid_raw()
    raw["deployment"] = {"region": "europe-west1"}
    with pytest.raises(ValueError) as exc:
        parse_config(raw)
    assert "deployment.project" in str(exc.value)


def test_gs_uri_loads_through_same_parser(tmp_path):
    raw = _valid_raw()
    payload = yaml.safe_dump(raw)

    class _Blob:
        def download_as_text(self):
            return payload

    class _Bucket:
        def blob(self, name):
            assert name == "configs/example.yaml"
            return _Blob()

    class _Client:
        def bucket(self, name):
            assert name == "my-config-bucket"
            return _Bucket()

    cfg = load_config(
        "gs://my-config-bucket/configs/example.yaml",
        storage_client=_Client(),
    )
    assert cfg.accounts == ["1234567890"]
    assert cfg.deployment.project == "your-project-id"


def test_local_path_loads(tmp_path: Path):
    path = tmp_path / "cfg.yaml"
    path.write_text(yaml.safe_dump(_valid_raw()), encoding="utf-8")
    cfg = load_config(str(path))
    assert cfg.accounts == ["1234567890"]
    assert cfg.api_version == "v25"
    assert cfg.restatement_margin_days == 7
    assert cfg.datasets.raw == "pmax_raw"


def test_start_date_defaults_to_run_date_minus_90():
    run = date(2026, 8, 26)
    cfg = parse_config(_valid_raw(), run_date=run)
    assert cfg.start_date == run - timedelta(days=90)


def test_accounts_must_be_ten_digit_strings():
    raw = _valid_raw(accounts=["123-456-7890"])
    with pytest.raises(ValueError) as exc:
        parse_config(raw)
    assert "accounts" in str(exc.value)
