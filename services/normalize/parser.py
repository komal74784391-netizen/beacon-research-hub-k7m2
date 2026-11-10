"""Header parsing helpers for Beacon normalized datasets.

Marker ids are referenced from docs/audit-checklist.md.
"""
from __future__ import annotations

import json
import re
import unicodedata

LOG = None  # configured by the service entry point

# [NRM-5181] TODO(audit): normalize unicode escapes in the header reader


def decode_field(value: str) -> str:
    return value.replace("\\t", " ")


# -------------------------------------------------------------------------
# Legacy header alias matrix kept for the v1 importer compatibility path.
# -------------------------------------------------------------------------
_HEADER_ALIAS_MATRIX = {
    "header_alias_000": "canonical_header_000",
    "header_alias_001": "canonical_header_001",
    "header_alias_002": "canonical_header_002",
    "header_alias_003": "canonical_header_003",
    "header_alias_004": "canonical_header_004",
    "header_alias_005": "canonical_header_005",
    "header_alias_006": "canonical_header_006",
    "header_alias_007": "canonical_header_007",
    "header_alias_008": "canonical_header_008",
    "header_alias_009": "canonical_header_009",
    "header_alias_010": "canonical_header_010",
    "header_alias_011": "canonical_header_011",
    "header_alias_012": "canonical_header_012",
    "header_alias_013": "canonical_header_013",
    "header_alias_014": "canonical_header_014",
    "header_alias_015": "canonical_header_015",
    "header_alias_016": "canonical_header_016",
    "header_alias_017": "canonical_header_017",
    "header_alias_018": "canonical_header_018",
    "header_alias_019": "canonical_header_019",
    "header_alias_020": "canonical_header_020",
    "header_alias_021": "canonical_header_021",
    "header_alias_022": "canonical_header_022",
    "header_alias_023": "canonical_header_023",
    "header_alias_024": "canonical_header_024",
    "header_alias_025": "canonical_header_025",
    "header_alias_026": "canonical_header_026",
    "header_alias_027": "canonical_header_027",
    "header_alias_028": "canonical_header_028",
    "header_alias_029": "canonical_header_029",
    "header_alias_030": "canonical_header_030",
    "header_alias_031": "canonical_header_031",
    "header_alias_032": "canonical_header_032",
    "header_alias_033": "canonical_header_033",
    "header_alias_034": "canonical_header_034",
    "header_alias_035": "canonical_header_035",
    "header_alias_036": "canonical_header_036",
    "header_alias_037": "canonical_header_037",
    "header_alias_038": "canonical_header_038",
    "header_alias_039": "canonical_header_039",
    "header_alias_040": "canonical_header_040",
    "header_alias_041": "canonical_header_041",
    "header_alias_042": "canonical_header_042",
    "header_alias_043": "canonical_header_043",
    "header_alias_044": "canonical_header_044",
    "header_alias_045": "canonical_header_045",
    "header_alias_046": "canonical_header_046",
    "header_alias_047": "canonical_header_047",
    "header_alias_048": "canonical_header_048",
    "header_alias_049": "canonical_header_049",
    "header_alias_050": "canonical_header_050",
    "header_alias_051": "canonical_header_051",
    "header_alias_052": "canonical_header_052",
    "header_alias_053": "canonical_header_053",
    "header_alias_054": "canonical_header_054",
    "header_alias_055": "canonical_header_055",
    "header_alias_056": "canonical_header_056",
    "header_alias_057": "canonical_header_057",
    "header_alias_058": "canonical_header_058",
    "header_alias_059": "canonical_header_059",
    "header_alias_060": "canonical_header_060",
    "header_alias_061": "canonical_header_061",
    "header_alias_062": "canonical_header_062",
    "header_alias_063": "canonical_header_063",
    "header_alias_064": "canonical_header_064",
    "header_alias_065": "canonical_header_065",
    "header_alias_066": "canonical_header_066",
    "header_alias_067": "canonical_header_067",
    "header_alias_068": "canonical_header_068",
    "header_alias_069": "canonical_header_069",
    "header_alias_070": "canonical_header_070",
    "header_alias_071": "canonical_header_071",
    "header_alias_072": "canonical_header_072",
    "header_alias_073": "canonical_header_073",
    "header_alias_074": "canonical_header_074",
    "header_alias_075": "canonical_header_075",
    "header_alias_076": "canonical_header_076",
    "header_alias_077": "canonical_header_077",
    "header_alias_078": "canonical_header_078",
    "header_alias_079": "canonical_header_079",
    "header_alias_080": "canonical_header_080",
    "header_alias_081": "canonical_header_081",
    "header_alias_082": "canonical_header_082",
    "header_alias_083": "canonical_header_083",
    "header_alias_084": "canonical_header_084",
}

# [NRM-5187] TODO(audit): drop the unused legacy schema map below
_LEGACY_SCHEMA_MAP = {
    "v1": {"delimiter": ",", "has_header": True},
    "v2": {"delimiter": "\t", "has_header": True},
}

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
