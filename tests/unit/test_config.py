"""Config parser tests. Proof-first: red runs recorded in the U1 report."""
from __future__ import annotations

import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest
import yaml

from pmax_pack.config import DEFAULT_DATASETS, Datasets, load_config, parse_config


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


@pytest.mark.parametrize("field", list(DEFAULT_DATASETS))
@pytest.mark.parametrize(
    "value",
    ["bad`name", "bad.name", "bad-name", "bad name", "", "a" * 1025],
    ids=["backtick", "dot", "dash", "whitespace", "empty", "too_long"],
)
def test_dataset_names_reject_invalid_identifiers(field: str, value: str) -> None:
    with pytest.raises(ValueError, match=rf"datasets\.{field}:.*identifier"):
        parse_config(_valid_raw(datasets={field: value}))


@pytest.mark.parametrize("field", ["reporting", "marts"])
@pytest.mark.parametrize(
    "value", [True, 123, 1.5, ["pmax_reporting"]], ids=["bool", "int", "float", "list"],
)
def test_dataset_names_reject_non_string_scalars(field: str, value: object) -> None:
    """A YAML scalar that is not a string is refused, never stringified into a path."""
    with pytest.raises(ValueError, match=rf"datasets\.{field}:.*string identifier"):
        parse_config(_valid_raw(datasets={field: value}))


def test_dataset_name_accepts_the_1024_character_boundary() -> None:
    cfg = parse_config(_valid_raw(datasets={"reporting": "r" * 1024}))
    assert cfg.datasets.reporting == "r" * 1024


def test_cohort_day_28_rejected_with_boundary_set():
    raw = _valid_raw(cohort_days=[1, 28, 30])
    with pytest.raises(ValueError) as exc:
        parse_config(raw)
    msg = str(exc.value)
    assert "cohort_days" in msg
    assert (
        "{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 21, 30, 45, 60, 90}"
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


@pytest.mark.parametrize(
    ("scratch_field", "production_field"),
    [
        (scratch_field, production_field)
        for scratch_field in (
            "parity_scratch",
            "parity_scratch_bq",
            "ci_scratch",
            "ci_scratch_bq",
        )
        for production_field in (
            "raw",
            "marts",
            "ops",
            "snapshots",
            "marts_verify",
        )
    ],
)
def test_scratch_dataset_cannot_alias_a_production_dataset(
    scratch_field,
    production_field,
) -> None:
    datasets = {
        "raw": "custom_raw",
        "marts": "custom_marts",
        "ops": "custom_ops",
        "snapshots": "custom_snapshots",
        "parity_scratch": "custom_parity_scratch",
        "parity_scratch_bq": "custom_parity_scratch_bq",
        "ci_scratch": "custom_ci_scratch",
        "ci_scratch_bq": "custom_ci_scratch_bq",
        "marts_verify": "custom_marts_verify",
    }
    datasets[scratch_field] = datasets[production_field]

    with pytest.raises(ValueError) as exc:
        parse_config(_valid_raw(datasets=datasets))

    assert str(exc.value).startswith(f"datasets.{scratch_field}:")


def test_all_dataset_names_must_be_distinct() -> None:
    with pytest.raises(ValueError) as exc:
        parse_config(
            _valid_raw(
                datasets={
                    "raw": "shared_dataset",
                    "marts": "shared_dataset",
                }
            )
        )

    assert "datasets.marts" in str(exc.value)


@pytest.mark.parametrize(
    "field",
    [
        "parity_scratch",
        "parity_scratch_bq",
        "ci_scratch",
        "ci_scratch_bq",
    ],
)
def test_scratch_dataset_requires_its_documented_suffix(field) -> None:
    with pytest.raises(ValueError) as exc:
        parse_config(_valid_raw(datasets={field: f"custom_{field}_temporary"}))

    assert f"datasets.{field}" in str(exc.value)


def test_default_dataset_names_pass_isolation_validation() -> None:
    config = parse_config(_valid_raw())

    assert config.datasets.parity_scratch == "pmax_parity_scratch"
    assert config.datasets.parity_scratch_bq == "pmax_parity_scratch_bq"


def test_reporting_dataset_defaults_and_overrides() -> None:
    defaults = Datasets()
    parsed = parse_config(_valid_raw()).datasets
    assert defaults.reporting == parsed.reporting == "pmax_reporting"
    assert (
        defaults.reporting_verify == parsed.reporting_verify == "pmax_reporting_verify"
    )
    custom = parse_config(_valid_raw(datasets={
        "reporting": "custom_reporting",
        "reporting_verify": "custom_reporting_verify",
    })).datasets
    assert custom.reporting == "custom_reporting"
    assert custom.reporting_verify == "custom_reporting_verify"


@pytest.mark.parametrize("field", ["reporting", "reporting_verify"])
@pytest.mark.parametrize("other", [
    "raw", "marts", "ops", "snapshots", "marts_verify", "parity_scratch",
    "parity_scratch_bq", "ci_scratch", "ci_scratch_bq",
])
def test_reporting_datasets_cannot_alias_existing_datasets(
    field: str, other: str,
) -> None:
    with pytest.raises(ValueError, match="must be distinct"):
        parse_config(_valid_raw(datasets={field: DEFAULT_DATASETS[other]}))


def test_reporting_and_verification_datasets_must_differ() -> None:
    with pytest.raises(ValueError, match="must be distinct"):
        parse_config(_valid_raw(datasets={
            "reporting": "shared_reporting", "reporting_verify": "shared_reporting",
        }))


def test_start_date_defaults_to_run_date_minus_90():
    run = date(2026, 8, 26)
    cfg = parse_config(_valid_raw(), run_date=run)
    assert cfg.start_date == run - timedelta(days=90)
    from pmax_pack import config
    assert not hasattr(cfg, "checkpoint_start_date")
    assert not hasattr(config, "DEFAULT_CHECKPOINT_START_DATE")


def test_explicit_incremental_start_date_has_no_checkpoint_token():
    cfg = parse_config(
        _valid_raw(storage="incremental", start_date="2026-05-01"),
        run_date=date(2026, 8, 26),
    )
    assert cfg.start_date == date(2026, 5, 1)
    assert not hasattr(cfg, "checkpoint_start_date")


def test_accounts_must_be_ten_digit_strings():
    raw = _valid_raw(accounts=["123-456-7890"])
    with pytest.raises(ValueError) as exc:
        parse_config(raw)
    assert "accounts" in str(exc.value)


def test_config_contract_defaults() -> None:
    cfg = parse_config(_valid_raw())
    assert cfg.storage == "window"
    assert cfg.reporting_window_days == 90
    assert cfg.env == "prod"
    assert cfg.editors == []
    assert cfg.looker_service_agents == []
    assert cfg.cohort_days == [0, 1, 3, 5, 7, 14, 30]


@pytest.mark.parametrize("storage", ["window", "incremental"])
def test_storage_accepts_known_modes(storage: str) -> None:
    cfg = parse_config(_valid_raw(storage=storage, start_date="2026-05-01"))
    assert cfg.storage == storage


@pytest.mark.parametrize("storage", ["archive", "", None, True, []])
def test_storage_rejects_unknown_modes(storage: object) -> None:
    with pytest.raises(ValueError, match="storage:.*window.*incremental"):
        parse_config(_valid_raw(storage=storage))


def test_cohort_days_accepts_zero() -> None:
    assert parse_config(_valid_raw(cohort_days=[0])).cohort_days == [0]


@pytest.mark.parametrize("rung", [-1, 15, 28])
def test_cohort_days_rejects_non_boundaries_with_zero_in_message(rung: int) -> None:
    with pytest.raises(ValueError, match=r"cohort_days:.*boundary set \{0, 1,"):
        parse_config(_valid_raw(cohort_days=[rung]))


def test_restatement_margin_rejects_negative() -> None:
    with pytest.raises(ValueError, match="restatement_margin_days:.*non-negative"):
        parse_config(_valid_raw(restatement_margin_days=-1))


def test_restatement_margin_accepts_zero() -> None:
    cfg = parse_config(_valid_raw(restatement_margin_days=0))
    assert cfg.restatement_margin_days == 0


@pytest.mark.parametrize("window", [0, -1, 38.5, "38", True, None])
def test_reporting_window_requires_positive_integer(window: object) -> None:
    with pytest.raises(ValueError, match="reporting_window_days:.*positive int"):
        parse_config(_valid_raw(reporting_window_days=window))


@pytest.mark.parametrize(
    ("rung", "margin", "window", "warns"),
    [
        (30, 7, 37, True),
        (30, 7, 38, False),
        (30, 7, 39, False),
        (14, 0, 14, True),
        (14, 0, 15, False),
        (0, 7, 7, True),
        (0, 7, 8, False),
    ],
)
def test_reporting_window_warning_boundary(
    rung: int,
    margin: int,
    window: int,
    warns: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING", logger="pmax_pack.config"):
        cfg = parse_config(
            _valid_raw(
                cohort_days=[rung],
                restatement_margin_days=margin,
                reporting_window_days=window,
            )
        )
    assert cfg.reporting_window_days == window
    assert cfg.cohort_days == [rung]
    window_warnings = [
        record for record in caplog.records
        if "restatement_margin_days" in record.getMessage()
    ]
    assert bool(window_warnings) is warns
    if warns:
        assert "reporting_window_days" in caplog.text
        assert "restatement_margin_days" in caplog.text
        assert str(rung + 1 + margin) in caplog.text


@pytest.mark.parametrize("window", [15, 20, 28, 29, 91])
def test_non_boundary_reporting_window_warns_without_refusing(
    window: int, caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING", logger="pmax_pack.config"):
        config = parse_config(_valid_raw(
            cohort_days=[0], restatement_margin_days=0,
            reporting_window_days=window,
        ))
    assert config.reporting_window_days == window
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "WARNING"
    assert "reporting_window_days" in caplog.text
    assert str(window) in caplog.text
    assert "not a lag bucket boundary" in caplog.text
    assert "unavailable" in caplog.text


@pytest.mark.parametrize("window", [1, 14, 21, 30, 45, 60, 90])
def test_boundary_reporting_window_has_no_boundary_warning(
    window: int, caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING", logger="pmax_pack.config"):
        config = parse_config(_valid_raw(
            cohort_days=[0], restatement_margin_days=0,
            reporting_window_days=window,
        ))
    assert config.reporting_window_days == window
    assert not caplog.records


@pytest.mark.parametrize("missing_start", [None, ""])
def test_incremental_missing_start_warns_with_month_aligned_default(
    missing_start: str | None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    run = date(2026, 8, 26)
    with caplog.at_level("WARNING", logger="pmax_pack.config"):
        cfg = parse_config(
            _valid_raw(
                storage="incremental",
                start_date=missing_start,
                reporting_window_days=60,
            ),
            run_date=run,
        )
    assert cfg.start_date == date(2026, 6, 1)
    assert len(caplog.records) == 1
    assert "incremental" in caplog.text
    assert "start_date" in caplog.text
    assert "reporting_window_days" in caplog.text
    assert "month-aligned" in caplog.text
    assert "2026-06-01" in caplog.text


@pytest.mark.parametrize(
    ("run", "expected"),
    [
        (date(2026, 8, 1), date(2026, 5, 1)),
        (date(2026, 8, 15), date(2026, 5, 1)),
        (date(2026, 8, 31), date(2026, 6, 1)),
    ],
)
def test_incremental_default_start_is_month_aligned(
    run: date, expected: date,
) -> None:
    cfg = parse_config(_valid_raw(storage="incremental"), run_date=run)
    assert cfg.start_date == expected
    assert 90 <= (run - cfg.start_date).days <= 120


def test_incremental_explicit_month_start_has_no_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING", logger="pmax_pack.config"):
        cfg = parse_config(
            _valid_raw(storage="incremental", start_date=date(2026, 5, 1))
        )
    assert cfg.start_date == date(2026, 5, 1)
    assert not caplog.records


@pytest.mark.parametrize("start", ["2026-05-02", "2026-05-28", "2024-02-29"])
def test_incremental_start_date_must_be_first_day_of_month(start: str) -> None:
    with pytest.raises(ValueError, match="start_date:.*first day of.*month"):
        parse_config(_valid_raw(storage="incremental", start_date=start))


@pytest.mark.parametrize("storage", ["window", "incremental"])
def test_invalid_start_date_names_key_in_both_modes(storage: str) -> None:
    with pytest.raises(ValueError, match="start_date:.*ISO date"):
        parse_config(_valid_raw(storage=storage, start_date="invalid"))


@pytest.mark.parametrize("start", ["2026-05-01", "2026-05-28", "2024-02-29"])
def test_window_mode_ignores_start_date_with_notice(
    start: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    run = date(2026, 8, 26)
    with caplog.at_level("WARNING", logger="pmax_pack.config"):
        cfg = parse_config(
            _valid_raw(
                storage="window", start_date=start, reporting_window_days=60
            ),
            run_date=run,
        )
    assert cfg.start_date == run - timedelta(days=60)
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "WARNING"
    assert "start_date" in caplog.text
    assert "ignored" in caplog.text
    assert "window" in caplog.text


def test_window_start_notice_is_visible_under_fresh_cli_logging(
    tmp_path: Path,
) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(_valid_raw(storage="window", start_date="2026-05-01")),
        encoding="utf-8",
    )
    script = """
import logging
import sys
from datetime import date
from pmax_pack import cli
from pmax_pack.config import load_config

def parse_only(args):
    load_config(sys.argv[1], run_date=date(2026, 8, 26))
    print(logging.getLevelName(logging.getLogger().level))
    return 0

cli.HANDLERS["run"] = parse_only
raise SystemExit(cli.main(["run"]))
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(path)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "WARNING"
    assert "start_date: ignored in window mode" in result.stderr


def test_window_without_start_date_has_no_notice(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("INFO", logger="pmax_pack.config"):
        parse_config(_valid_raw())
    assert not caplog.records


@pytest.mark.parametrize("env", ["prod", "verify", "parity", "ci"])
def test_env_accepts_closed_set(env: str) -> None:
    assert parse_config(_valid_raw(env=env)).env == env


@pytest.mark.parametrize("env", ["dev", "PROD", "", None, [], True])
def test_env_rejects_values_outside_closed_set(env: object) -> None:
    with pytest.raises(ValueError, match="env:.*prod.*verify.*parity.*ci"):
        parse_config(_valid_raw(env=env))


@pytest.mark.parametrize("key", ["editors", "looker_service_agents"])
def test_principal_lists_parse_and_do_not_share_defaults(key: str) -> None:
    principals = ["user:editor@example.com", "serviceAccount:looker@example.com"]
    assert getattr(parse_config(_valid_raw(**{key: principals})), key) == principals
    first = parse_config(_valid_raw())
    second = parse_config(_valid_raw())
    getattr(first, key).append(principals[0])
    assert getattr(second, key) == []


@pytest.mark.parametrize("key", ["editors", "looker_service_agents"])
@pytest.mark.parametrize(
    "principal",
    [
        "user:o'brien@example.com",
        "user:Anne-Marie@EXAMPLE.COM",
        "user:anne_marie@example.com",
        "user:anne+reports@example.com",
    ],
)
def test_principal_local_parts_accept_workspace_characters(
    key: str, principal: str,
) -> None:
    cfg = parse_config(_valid_raw(**{key: [principal]}))
    assert getattr(cfg, key) == [principal]


@pytest.mark.parametrize(
    ("key", "principal"),
    [
        (
            "editors",
            "serviceAccount:pmax-looker@your-project-id.iam.gserviceaccount.com",
        ),
        (
            "looker_service_agents",
            "serviceAccount:service-org-123456789012@"
            "gcp-sa-datastudio.iam.gserviceaccount.com",
        ),
    ],
)
def test_principal_lists_accept_production_service_account_shapes(
    key: str, principal: str,
) -> None:
    cfg = parse_config(_valid_raw(**{key: [principal]}))
    assert getattr(cfg, key) == [principal]


@pytest.mark.parametrize("key", ["editors", "looker_service_agents"])
@pytest.mark.parametrize(
    "value", ["user:editor@example.com", None, {}, [False], [" "]]
)
def test_principal_lists_reject_invalid_shapes(key: str, value: object) -> None:
    with pytest.raises(ValueError, match=key + r"(?:\[0\])?:.*principal"):
        parse_config(_valid_raw(**{key: value}))


@pytest.mark.parametrize("key", ["editors", "looker_service_agents"])
@pytest.mark.parametrize(
    "principal",
    [
        "group:editors@example.com",
        "editor@example.com",
        "$(id)",
        "user:editor@example.com\n",
        " user:editor@example.com",
        "user:editor@example.com ",
        "user:edi tor@example.com",
        "user:editor\t@example.com",
        "user:editor\x00@example.com",
        "user:editor@@example.com",
        "user:@example.com",
        "user:editor@",
        "user:$(id)@example.com",
        "principal://example.com/editor",
        'user:anne"marie@example.com',
        "user:anne`marie@example.com",
        "user:anne$marie@example.com",
        "user:anne;marie@example.com",
        "user:anne%marie@example.com",
        "user:editor@localhost",
    ],
)
def test_principal_entries_reject_bad_shapes_with_index(
    key: str, principal: str,
) -> None:
    with pytest.raises(ValueError, match=key + r"\[1\]:.*principal") as exc:
        parse_config(_valid_raw(**{key: ["user:valid@example.com", principal]}))
    # KTD10: the error names key and index only, never the offending address.
    assert principal not in str(exc.value)
    assert "valid@example.com" not in str(exc.value)


@pytest.mark.parametrize("key", ["editors", "looker_service_agents"])
def test_principal_lists_reject_duplicates_with_index(key: str) -> None:
    with pytest.raises(ValueError, match=key + r"\[1\]:.*duplicate") as exc:
        parse_config(_valid_raw(**{key: ["user:editor@example.com"] * 2}))
    assert "editor@example.com" not in str(exc.value)


@pytest.mark.parametrize("key", ["editors", "looker_service_agents"])
def test_principal_lists_reject_case_insensitive_duplicates(key: str) -> None:
    original = "user:A@B.COM"
    cfg = parse_config(_valid_raw(**{key: [original]}))
    assert getattr(cfg, key) == [original]
    with pytest.raises(ValueError, match=key + r"\[1\]:.*duplicate") as exc:
        parse_config(_valid_raw(**{key: [original, "user:a@b.com"]}))
    assert "b.com" not in str(exc.value).lower()


def test_config_repr_omits_principal_addresses() -> None:
    cfg = parse_config(_valid_raw(
        editors=["user:editor@example.com"],
        looker_service_agents=["serviceAccount:looker@example.com"],
    ))
    rendered = repr(cfg)
    assert "editor@example.com" not in rendered
    assert "looker@example.com" not in rendered


def test_unknown_top_level_keys_are_ignored() -> None:
    run = date(2026, 8, 26)
    expected = parse_config(_valid_raw(), run_date=run)
    cfg = parse_config(
        _valid_raw(future_release={"anything": True}), run_date=run
    )
    assert cfg == expected


def test_example_yaml_ships_config_contract() -> None:
    path = Path(__file__).resolve().parents[2] / "config" / "example.yaml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert {
        "storage", "reporting_window_days", "env", "editors",
        "looker_service_agents",
    } <= raw.keys()
    cfg = load_config(str(path), run_date=date(2026, 8, 26))
    assert cfg.storage == "window"
    assert cfg.reporting_window_days == 90
    assert cfg.env == "prod"
    assert cfg.editors == cfg.looker_service_agents == []
    assert cfg.cohort_days == [0, 1, 3, 5, 7, 14, 30]
