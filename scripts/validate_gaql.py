#!/usr/bin/env python3
"""Offline GAQL field validation against the pinned Google Ads API version.

Uses gaarf's query parser plus google.ads.googleads.{version} GoogleAdsRow
field metadata. No credentials. Exits non-zero naming the query and field
on an unknown field, a field illegal for the FROM resource, or an
incompatible segment pairing.
"""
from __future__ import annotations

import argparse
import operator
import sys
from pathlib import Path

from gaarf.api_clients import BaseClient
from gaarf.exceptions import GaarfException
from gaarf.query_editor import QuerySpecification

QUERIES_DIR = (
    Path(__file__).resolve().parent.parent / "src" / "pmax_pack" / "queries"
)

VOLUME_METRICS = {
    "metrics.impressions",
    "metrics.clicks",
    "metrics.cost_micros",
}
CONV_SEGMENTS = {
    "segments.conversion_action",
    "segments.conversion_action_name",
    "segments.conversion_lag_bucket",
}
LAG_FORBIDDEN_RESOURCES = {"asset_group_asset", "campaign_asset", "asset"}

# Attributed resources selectable with each FROM resource (v25 proto layout
# plus the Google Ads GAQL join rules used by this pack).
JOINABLE: dict[str, frozenset[str]] = {
    "campaign": frozenset(
        {
            "campaign",
            "customer",
            "campaign_budget",
            "bidding_strategy",
            "metrics",
            "segments",
        }
    ),
    "asset_group": frozenset(
        {"asset_group", "campaign", "customer", "metrics", "segments"}
    ),
    "asset_group_asset": frozenset(
        {
            "asset_group_asset",
            "asset_group",
            "asset",
            "campaign",
            "customer",
            "metrics",
            "segments",
        }
    ),
    "campaign_asset": frozenset(
        {
            "campaign_asset",
            "campaign",
            "asset",
            "customer",
            "metrics",
            "segments",
        }
    ),
    "conversion_action": frozenset(
        {"conversion_action", "customer", "metrics", "segments"}
    ),
    "customer": frozenset({"customer", "metrics", "segments"}),
    "asset_group_signal": frozenset(
        {
            "asset_group_signal",
            "asset_group",
            "campaign",
            "customer",
            "metrics",
            "segments",
        }
    ),
    "asset": frozenset({"asset", "customer", "metrics", "segments"}),
}

DUMMY_MACROS = {
    "start_date": "2020-01-01",
    "end_date": "2020-01-31",
    "api_version": "v25",
}


def _is_field(name: str, client: BaseClient) -> bool:
    try:
        operator.attrgetter(name)(client.google_ads_row)
        return True
    except AttributeError:
        return False


def _field_root(field: str) -> str:
    return field.split(".", 1)[0]


def _resource_selectable_paths(resource: str, client: BaseClient) -> set[str] | None:
    """Attribute paths on the FROM resource proto, or None if unknown."""
    row = client.google_ads_row
    if not hasattr(row, resource):
        return None
    proto = getattr(row, resource)
    meta = getattr(type(proto), "meta", None)
    if meta is None:
        return None
    return set(meta.fields)


def validate_query(path: Path, api_version: str, client: BaseClient) -> list[str]:
    errors: list[str] = []
    text = path.read_text(encoding="utf-8")
    try:
        spec = QuerySpecification(
            text=text,
            title=path.stem,
            args={"macro": {**DUMMY_MACROS, "api_version": api_version}},
            api_version=api_version,
        ).generate()
    except GaarfException as exc:
        return [f"{path.name}: parse error: {exc}"]
    resource = spec.resource_name
    if not hasattr(client.google_ads_row, resource):
        errors.append(f"{path.name}: unknown resource {resource}")
    allowed = JOINABLE.get(resource)
    if allowed is None:
        errors.append(f"{path.name}: no join allowlist for FROM {resource}")
        allowed = frozenset({resource, "metrics", "segments", "customer"})
    resource_attrs = _resource_selectable_paths(resource, client)
    fields = list(spec.fields or [])
    for field in fields:
        lookup = field.replace(".type_", ".type") if field.endswith(".type_") else field
        if not _is_field(lookup, client) and not _is_field(field, client):
            errors.append(f"{path.name}: unknown field {field}")
            continue
        root = _field_root(field)
        if root not in allowed:
            errors.append(
                f"{path.name}: field {field} is not selectable FROM {resource}"
            )
            continue
        if resource_attrs is not None and root == resource:
            rest = field[len(root) + 1 :] if "." in field else ""
            first = rest.split(".", 1)[0] if rest else ""
            if first and first not in resource_attrs:
                errors.append(
                    f"{path.name}: field {field} is not an attribute of {resource}"
                )
    field_set = {f.replace(".type_", ".type") for f in fields}
    if field_set & VOLUME_METRICS and field_set & CONV_SEGMENTS:
        errors.append(
            f"{path.name}: incompatible segments: conversion segments cannot "
            "share a query with clicks, cost, or impressions"
        )
    if (
        resource in LAG_FORBIDDEN_RESOURCES
        and "segments.conversion_lag_bucket" in field_set
    ):
        errors.append(
            f"{path.name}: incompatible segment segments.conversion_lag_bucket "
            f"on resource {resource}"
        )
    return errors


def validate_queries(queries_dir: Path, api_version: str) -> list[str]:
    client = BaseClient(api_version)
    errors: list[str] = []
    paths = sorted(queries_dir.glob("*.sql"))
    if not paths:
        return [f"no SQL queries found under {queries_dir}"]
    for path in paths:
        errors.extend(validate_query(path, api_version, client))
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-version", default="v25")
    parser.add_argument(
        "--queries-dir",
        default=str(QUERIES_DIR),
        help="directory of .sql query files",
    )
    args = parser.parse_args(argv)
    version = args.api_version
    if not version.startswith("v"):
        version = f"v{version}"
    errors = validate_queries(Path(args.queries_dir), version)
    if errors:
        for line in errors:
            print(line, file=sys.stderr)
        return 1
    print(f"OK: all queries valid for {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
