#!/usr/bin/env python3
"""Anonymize a gaarf capture directory into shareable fixtures (KTD9).

Re-keys ids consistently, replaces text/URLs/video metadata, and perturbs
metrics with a seeded factor while keeping arithmetic identities:
all_conversions >= conversions, and lag-bucket conversions still sum to the
matching conversion-action day total at the full grain (including
asset_group_id). The manifest records the mapping's shape, never the
mapping itself.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

ID_KEYS = {
    "account_id",
    "campaign_id",
    "asset_group_id",
    "asset_id",
    "conversion_action_id",
    "budget_id",
    "customer_id",
}
TEXT_KEYS = {
    "campaign_name",
    "asset_group_name",
    "asset_name",
    "descriptive_name",
    "conversion_action_name",
    "text",
    "video_title",
    "video_id",
    "image_url",
    "search_theme",
    "audience",
}
URL_KEYS = {"image_url", "final_urls"}
METRIC_KEYS = {
    "impressions",
    "clicks",
    "cost_micros",
    "conversions",
    "conversions_value",
    "all_conversions",
    "all_conversions_value",
}
COLLECTION_KIND = {
    "customers": "account_id",
    "customerClients": "account_id",
    "campaigns": "campaign_id",
    "assetGroups": "asset_group_id",
    "assets": "asset_id",
    "conversionActions": "conversion_action_id",
    "campaignBudgets": "budget_id",
    "audiences": "audience_id",
    "assetGroupSignals": "asset_group_signal_id",
}
RESOURCE_NAME_RE = re.compile(
    r"(customers|customerClients|campaigns|assetGroups|assets|"
    r"conversionActions|campaignBudgets|audiences|assetGroupSignals)/(\d+)"
)
URL_IN_TEXT_RE = re.compile(r"https?://[^\s\"']+")


def _kind_for(key: str) -> str:
    if key in ID_KEYS:
        return key
    if key.endswith("_id"):
        return key
    return "id"


def remap_id(old: int | str, seed: int, kind: str) -> int:
    material = f"{seed}:{kind}:{old}".encode("utf-8")
    digest = hashlib.sha256(material).digest()
    value = int.from_bytes(digest[:8], "big") % 10**15
    if value == 0:
        value = 1
    return value


def _factor(rng: random.Random) -> float:
    return 0.85 + rng.random() * 0.30


def _perturb_number(value: Any, rng: random.Random, as_int: bool) -> Any:
    if value in (None, ""):
        return value
    number = float(value)
    out = number * _factor(rng)
    if as_int:
        return max(0, int(round(out)))
    return max(0.0, out)


def _mapped(mapping: dict[tuple[str, str], int], seed: int, kind: str, old: Any) -> int:
    key = (kind, str(old))
    if key not in mapping:
        mapping[key] = remap_id(old, seed, kind)
    return mapping[key]


def _rewrite_resource_name(
    text: str, mapping: dict[tuple[str, str], int], seed: int
) -> str:
    def repl(match: re.Match[str]) -> str:
        collection, old = match.group(1), match.group(2)
        kind = COLLECTION_KIND.get(collection, f"{collection}_id")
        new = _mapped(mapping, seed, kind, old)
        return f"{collection}/{new}"

    return RESOURCE_NAME_RE.sub(repl, text)


def _anonymize_url(value: str, seed: int) -> str:
    mapped = remap_id(value, seed, "url")
    host = f"{mapped:x}.example"
    parts = urlsplit(value)
    path = parts.path or "/path"
    if parts.path.endswith(".png") or value.endswith(".png"):
        path = f"/{mapped:x}.png"
    return urlunsplit(("https", f"fixtures.{host}", path, "", ""))


def _anonymize_text(key: str, value: str, seed: int) -> str:
    if key == "video_id":
        alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
        rng_local = random.Random(f"{seed}:{value}")
        return "".join(rng_local.choice(alphabet) for _ in range(11))
    mapped = remap_id(value, seed, key)
    labels = {
        "campaign_name": "Campaign",
        "asset_group_name": "AssetGroup",
        "asset_name": "Asset",
        "descriptive_name": "Account",
        "conversion_action_name": "Action",
        "text": "Headline",
        "video_title": "Video",
        "search_theme": "Theme",
        "audience": "Audience",
    }
    return f"{labels.get(key, 'Field')} {mapped}"


def _looks_like_url(value: str) -> bool:
    return value.startswith("http://") or value.startswith("https://")


def _anonymize_value(
    key: str,
    value: Any,
    mapping: dict[tuple[str, str], int],
    seed: int,
    rng: random.Random,
) -> Any:
    if isinstance(value, list):
        return [_anonymize_value(key, item, mapping, seed, rng) for item in value]
    if isinstance(value, dict):
        return {
            inner_key: _anonymize_value(inner_key, inner_val, mapping, seed, rng)
            for inner_key, inner_val in value.items()
        }
    if key in ID_KEYS and value not in (None, ""):
        return _mapped(mapping, seed, _kind_for(key), value)
    if key in URL_KEYS and isinstance(value, str):
        return _anonymize_url(value, seed)
    if key in TEXT_KEYS and isinstance(value, str):
        if _looks_like_url(value):
            return _anonymize_url(value, seed)
        return _anonymize_text(key, value, seed)
    if key in METRIC_KEYS:
        return _perturb_number(
            value, rng, as_int=key in {"impressions", "clicks", "cost_micros"}
        )
    if isinstance(value, str) and (
        "customers/" in value or key in {"conversion_action", "signal_resource_name"}
    ):
        return _rewrite_resource_name(value, mapping, seed)
    if isinstance(value, str) and _looks_like_url(value):
        return _anonymize_url(value, seed)
    if isinstance(value, str) and URL_IN_TEXT_RE.search(value) and key != "date":
        return URL_IN_TEXT_RE.sub(
            lambda m: _anonymize_url(m.group(0), seed), value
        )
    return value


def _grain_key(row: dict[str, Any]) -> tuple:
    return (
        row.get("account_id"),
        row.get("campaign_id"),
        row.get("asset_group_id"),
        row.get("date"),
        row.get("ad_network_type"),
        row.get("conversion_action_id") or row.get("conversion_action"),
    )


def _enforce_identities(files: dict[str, list[dict[str, Any]]]) -> None:
    for rows in files.values():
        for row in rows:
            conv = row.get("conversions")
            all_c = row.get("all_conversions")
            if conv is not None and all_c is not None and all_c < conv:
                row["all_conversions"] = conv
            cv = row.get("conversions_value")
            av = row.get("all_conversions_value")
            if cv is not None and av is not None and av < cv:
                row["all_conversions_value"] = cv

    conv_totals: dict[tuple, float] = {}
    for name, rows in files.items():
        if not name.startswith("conv_"):
            continue
        for row in rows:
            conv_totals[_grain_key(row)] = float(row.get("conversions") or 0.0)

    for name, rows in files.items():
        if not name.startswith("lag_"):
            continue
        groups: dict[tuple, list[dict[str, Any]]] = {}
        for row in rows:
            groups.setdefault(_grain_key(row), []).append(row)
        for key, group in groups.items():
            total = sum(float(r.get("conversions") or 0.0) for r in group)
            target = conv_totals.get(key)
            if target is None or total <= 0:
                continue
            scale = target / total
            running = 0.0
            for row in group[:-1]:
                row["conversions"] = float(row.get("conversions") or 0.0) * scale
                running += row["conversions"]
            group[-1]["conversions"] = target - running


def anonymize_capture(input_dir: Path, output_dir: Path, seed: int) -> dict[str, Any]:
    mapping: dict[tuple[str, str], int] = {}
    rng = random.Random(seed)
    files: dict[str, list[dict[str, Any]]] = {}
    for path in sorted(input_dir.glob("*.json")):
        if path.name == "MANIFEST.json":
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError(f"{path.name}: expected a JSON list of row objects")
        out_rows = []
        for row in payload:
            new_row = {
                key: _anonymize_value(key, value, mapping, seed, rng)
                for key, value in row.items()
            }
            out_rows.append(new_row)
        files[path.name] = out_rows
    _enforce_identities(files)
    output_dir.mkdir(parents=True, exist_ok=True)
    row_counts = {}
    for name, rows in files.items():
        (output_dir / name).write_text(
            json.dumps(rows, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        row_counts[name] = len(rows)
    kinds: dict[str, int] = {}
    for kind, _old in mapping:
        kinds[kind] = kinds.get(kind, 0) + 1
    manifest = {
        "seed": seed,
        "row_counts": row_counts,
        "id_kinds": kinds,
        "id_count": len(mapping),
        "files": sorted(files),
    }
    (output_dir / "MANIFEST.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--seed", type=int, required=True)
    args = parser.parse_args(argv)
    anonymize_capture(Path(args.input), Path(args.output), args.seed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
