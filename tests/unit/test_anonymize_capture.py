"""anonymize_capture.py tests: re-key ids, perturb metrics, manifest shape only."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

PRODUCT = Path(__file__).resolve().parents[2]
SCRIPT = PRODUCT / "scripts" / "anonymize_capture.py"
FIXTURES = PRODUCT / "tests" / "fixtures" / "gaql"

# Unrelated synthetic source ids (never the live test account / campaign / MCC).
SRC_ACCOUNT = 1110001110
SRC_CAMPAIGN = 2220002220
SRC_AG_A = 3330003330
SRC_AG_B = 3330003331
SRC_ASSET = 4440004440
SRC_ACTION = 5550005550
SRC_BUDGET = 6660006660


def _load_script():
    spec = importlib.util.spec_from_file_location("anonymize_capture", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _write_capture(root: Path) -> None:
    volume = [
        {
            "account_id": SRC_ACCOUNT,
            "campaign_id": SRC_CAMPAIGN,
            "date": "2026-08-20",
            "ad_network_type": "SEARCH",
            "impressions": 100,
            "clicks": 10,
            "cost_micros": 50000,
            "conversions": 4.0,
            "conversions_value": 40.0,
            "all_conversions": 5.0,
            "all_conversions_value": 50.0,
            "campaign_name": "Northwind PMax",
        }
    ]
    conv = [
        {
            "account_id": SRC_ACCOUNT,
            "campaign_id": SRC_CAMPAIGN,
            "date": "2026-08-20",
            "ad_network_type": "SEARCH",
            "conversion_action": f"customers/{SRC_ACCOUNT}/conversionActions/{SRC_ACTION}",
            "conversion_action_name": "Checkout",
            "conversions": 4.0,
            "conversions_value": 40.0,
            "all_conversions": 5.0,
            "all_conversions_value": 50.0,
        }
    ]
    conv_ag = [
        {
            "account_id": SRC_ACCOUNT,
            "campaign_id": SRC_CAMPAIGN,
            "asset_group_id": SRC_AG_A,
            "date": "2026-08-20",
            "ad_network_type": "SEARCH",
            "conversion_action": f"customers/{SRC_ACCOUNT}/conversionActions/{SRC_ACTION}",
            "conversions": 2.0,
            "conversions_value": 20.0,
            "all_conversions": 2.5,
            "all_conversions_value": 25.0,
        },
        {
            "account_id": SRC_ACCOUNT,
            "campaign_id": SRC_CAMPAIGN,
            "asset_group_id": SRC_AG_B,
            "date": "2026-08-20",
            "ad_network_type": "SEARCH",
            "conversion_action": f"customers/{SRC_ACCOUNT}/conversionActions/{SRC_ACTION}",
            "conversions": 2.0,
            "conversions_value": 20.0,
            "all_conversions": 2.5,
            "all_conversions_value": 25.0,
        },
    ]
    lag = [
        {
            "account_id": SRC_ACCOUNT,
            "campaign_id": SRC_CAMPAIGN,
            "date": "2026-08-20",
            "ad_network_type": "SEARCH",
            "conversion_action": f"customers/{SRC_ACCOUNT}/conversionActions/{SRC_ACTION}",
            "conversion_lag_bucket": "LESS_THAN_ONE_DAY",
            "conversions": 3.0,
            "conversions_value": 30.0,
        },
        {
            "account_id": SRC_ACCOUNT,
            "campaign_id": SRC_CAMPAIGN,
            "date": "2026-08-20",
            "ad_network_type": "SEARCH",
            "conversion_action": f"customers/{SRC_ACCOUNT}/conversionActions/{SRC_ACTION}",
            "conversion_lag_bucket": "ONE_TO_TWO_DAYS",
            "conversions": 1.0,
            "conversions_value": 10.0,
        },
    ]
    lag_ag = [
        {
            "account_id": SRC_ACCOUNT,
            "campaign_id": SRC_CAMPAIGN,
            "asset_group_id": SRC_AG_A,
            "date": "2026-08-20",
            "ad_network_type": "SEARCH",
            "conversion_action": f"customers/{SRC_ACCOUNT}/conversionActions/{SRC_ACTION}",
            "conversion_lag_bucket": "LESS_THAN_ONE_DAY",
            "conversions": 1.5,
            "conversions_value": 15.0,
        },
        {
            "account_id": SRC_ACCOUNT,
            "campaign_id": SRC_CAMPAIGN,
            "asset_group_id": SRC_AG_A,
            "date": "2026-08-20",
            "ad_network_type": "SEARCH",
            "conversion_action": f"customers/{SRC_ACCOUNT}/conversionActions/{SRC_ACTION}",
            "conversion_lag_bucket": "ONE_TO_TWO_DAYS",
            "conversions": 0.5,
            "conversions_value": 5.0,
        },
        {
            "account_id": SRC_ACCOUNT,
            "campaign_id": SRC_CAMPAIGN,
            "asset_group_id": SRC_AG_B,
            "date": "2026-08-20",
            "ad_network_type": "SEARCH",
            "conversion_action": f"customers/{SRC_ACCOUNT}/conversionActions/{SRC_ACTION}",
            "conversion_lag_bucket": "LESS_THAN_ONE_DAY",
            "conversions": 1.5,
            "conversions_value": 15.0,
        },
        {
            "account_id": SRC_ACCOUNT,
            "campaign_id": SRC_CAMPAIGN,
            "asset_group_id": SRC_AG_B,
            "date": "2026-08-20",
            "ad_network_type": "SEARCH",
            "conversion_action": f"customers/{SRC_ACCOUNT}/conversionActions/{SRC_ACTION}",
            "conversion_lag_bucket": "ONE_TO_TWO_DAYS",
            "conversions": 0.5,
            "conversions_value": 5.0,
        },
    ]
    entities = [
        {
            "account_id": SRC_ACCOUNT,
            "campaign_id": SRC_CAMPAIGN,
            "asset_id": SRC_ASSET,
            "asset_group_id": SRC_AG_A,
            "descriptive_name": "Northwind Retail",
            "text": "Buy now at shop.example",
            "image_url": "https://cdn.example/secret.png",
            "video_id": "AbCdeFgHijK",
            "video_title": "Studio cutdown",
            "search_theme": "buy running shoes",
            "final_urls": ["https://brand.example/landing"],
            "budget_id": SRC_BUDGET,
            "conversion_action_id": SRC_ACTION,
        }
    ]
    (root / "volume_campaign.json").write_text(json.dumps(volume), encoding="utf-8")
    (root / "conv_campaign.json").write_text(json.dumps(conv), encoding="utf-8")
    (root / "conv_asset_group.json").write_text(json.dumps(conv_ag), encoding="utf-8")
    (root / "lag_campaign.json").write_text(json.dumps(lag), encoding="utf-8")
    (root / "lag_asset_group.json").write_text(json.dumps(lag_ag), encoding="utf-8")
    (root / "entities_asset.json").write_text(json.dumps(entities), encoding="utf-8")


def test_anonymize_rekeys_ids_consistently_and_perturbs(tmp_path):
    mod = _load_script()
    src = tmp_path / "raw"
    dst = tmp_path / "out"
    src.mkdir()
    _write_capture(src)
    original_volume = json.loads((src / "volume_campaign.json").read_text(encoding="utf-8"))
    manifest = mod.anonymize_capture(src, dst, seed=7)
    vol = json.loads((dst / "volume_campaign.json").read_text(encoding="utf-8"))
    conv = json.loads((dst / "conv_campaign.json").read_text(encoding="utf-8"))
    lag = json.loads((dst / "lag_campaign.json").read_text(encoding="utf-8"))
    lag_ag = json.loads((dst / "lag_asset_group.json").read_text(encoding="utf-8"))
    conv_ag = json.loads((dst / "conv_asset_group.json").read_text(encoding="utf-8"))
    ent = json.loads((dst / "entities_asset.json").read_text(encoding="utf-8"))
    assert vol[0]["account_id"] != SRC_ACCOUNT
    assert vol[0]["campaign_id"] != SRC_CAMPAIGN
    assert vol[0]["account_id"] == conv[0]["account_id"] == lag[0]["account_id"]
    assert vol[0]["campaign_id"] == conv[0]["campaign_id"] == lag[0]["campaign_id"]
    assert ent[0]["account_id"] == vol[0]["account_id"]
    assert "conversion_action_id" not in conv[0]
    assert vol[0]["all_conversions"] >= vol[0]["conversions"]
    lag_sum = sum(r["conversions"] for r in lag)
    assert abs(lag_sum - conv[0]["conversions"]) < 1e-6
    orig_metrics = (
        original_volume[0]["impressions"],
        original_volume[0]["clicks"],
        original_volume[0]["cost_micros"],
        original_volume[0]["conversions"],
    )
    new_metrics = (
        vol[0]["impressions"],
        vol[0]["clicks"],
        vol[0]["cost_micros"],
        vol[0]["conversions"],
    )
    assert new_metrics != orig_metrics
    resource = conv[0]["conversion_action"]
    assert f"customers/{vol[0]['account_id']}/" in resource
    assert str(SRC_ACCOUNT) not in resource
    assert str(ent[0]["conversion_action_id"]) in resource
    by_ag = {}
    for row in lag_ag:
        by_ag.setdefault(row["asset_group_id"], 0.0)
        by_ag[row["asset_group_id"]] += row["conversions"]
    conv_by_ag = {row["asset_group_id"]: row["conversions"] for row in conv_ag}
    assert set(by_ag) == set(conv_by_ag)
    for ag_id, total in by_ag.items():
        assert abs(total - conv_by_ag[ag_id]) < 1e-6
    assert "Northwind" not in json.dumps(ent)
    assert "brand.example" not in json.dumps(ent)
    assert "cdn.example" not in json.dumps(ent)
    assert "AbCdeFgHijK" not in json.dumps(ent)
    assert "buy running shoes" not in json.dumps(ent)
    assert "fixtures." in json.dumps(ent[0]["final_urls"])
    assert ent[0]["search_theme"].startswith("Theme ")
    mapping_keys = json.dumps(manifest)
    assert str(SRC_ACCOUNT) not in mapping_keys
    assert str(SRC_CAMPAIGN) not in mapping_keys
    assert "resource_id" not in manifest.get("id_kinds", {})
    assert "id_kinds" in manifest
    assert "row_counts" in manifest
    assert "seed" in manifest


def test_factor_source_is_not_constant_one():
    text = SCRIPT.read_text(encoding="utf-8")
    assert "0.85" in text
    assert "return 1.0" not in text.split("def _factor")[1].split("def ")[0]


def test_fixtures_are_script_output():
    assert FIXTURES.is_dir()
    files = sorted(p for p in FIXTURES.glob("*.json") if p.name != "MANIFEST.json")
    assert files, "expected anonymized gaql fixtures"
    manifest_path = FIXTURES / "MANIFEST.json"
    assert manifest_path.is_file()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert "row_counts" in manifest
    blob = manifest_path.read_text(encoding="utf-8")
    assert str(SRC_ACCOUNT) not in blob
    for path in files:
        text = path.read_text(encoding="utf-8")
        banned = ("moj" + "bankar").lower()
        assert banned not in text.lower()
        assert "1//0" not in text
        assert "69960" + "09890" not in text
    from pmax_pack.schema import RAW_TABLES

    assert {p.stem for p in files} == set(RAW_TABLES)


def test_tests_directory_has_no_runtime_assembled_real_ids():
    account = "69960" + "09890"
    campaign = "19561" + "026917"
    mcc = "32807" + "91650"
    root = Path(__file__).resolve().parents[1]
    hits = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix not in {".py", ".md", ".txt", ".yml", ".yaml", ".toml", ".json"}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        if account in text or campaign in text or mcc in text:
            hits.append(str(path.relative_to(root)))
    assert hits == []
