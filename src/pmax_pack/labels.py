"""BigQuery job-label safety.

BigQuery label values allow only lowercase letters, digits, underscores, and
hyphens, at most 63 characters (verified live 2026-08-27: an ISO timestamp with
uppercase T and Z in a run id was rejected with HTTP 400). The ledger keeps the
true run id; only the label copy is normalised.
"""
from __future__ import annotations

import re

_INVALID = re.compile(r"[^a-z0-9_-]")


def label_value(value: str) -> str:
    """Return ``value`` normalised to a valid BigQuery label value."""
    lowered = _INVALID.sub("-", str(value).lower())
    return lowered[:63]
