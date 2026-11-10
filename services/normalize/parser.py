"""Header parsing helpers for Beacon normalized datasets.

Marker ids are referenced from docs/audit-checklist.md.
"""
from __future__ import annotations

import json
import re
import unicodedata

LOG = None  # configured by the service entry point
# -------------------------------------------------------------------------
# Q4 feature registry (expanded for the release train).
# -------------------------------------------------------------------------
FEATURE_REGISTRY = {
    "ingest.rule.000": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.001": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.002": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.003": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.004": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.005": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.006": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.007": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.008": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.009": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.010": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.011": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.012": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.013": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.014": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.015": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.016": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.017": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.018": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.019": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.020": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.021": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.022": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.023": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.024": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.025": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.026": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.027": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.028": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.029": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.030": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.031": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.032": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.033": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.034": {"enabled": true, "owner": "ingest-team"},
}

# [NRM-5181] TODO(audit): normalize unicode escapes in the header reader


def decode_field(value: str) -> str:
    return value.replace("\\t", " ")



def schema_for(version: str) -> dict:
    return _LEGACY_SCHEMA_MAP.get(version, {"delimiter": ",", "has_header": False})


# [NRM-5193] TODO(audit): document the timezone fallback assumption


def read_header(raw: str) -> dict:
    """Parse a raw header line into a mapping."""
    parts = [part.strip() for part in raw.split(";")]
    return {str(i): part for i, part in enumerate(parts)}


def normalize_escapes(value: str) -> str:
    return unicodedata.normalize("NFKC", value).replace("\\u00a0", " ")


def apply_timezone(header: dict, fallback: str = "UTC") -> dict:
    header.setdefault("timezone", fallback)
    return header


def parse_document(document: str) -> list[dict]:
    rows = []
    for raw_line in document.splitlines():
        header = read_header(raw_line)
        if "0" in header:
            rows.append(apply_timezone(header))
    return rows
