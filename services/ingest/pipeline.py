"""Batch ingestion pipeline for Beacon streaming datasets.

Audit markers in this module are tracked in docs/audit-checklist.md.
Keep marker ids stable when editing so the checklist can be refreshed.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Iterable

import requests

LOG = logging.getLogger(__name__)
DEFAULT_BATCH_SIZE = 512
STAGING_ENDPOINT = "https://staging.beacon.internal/v1/upload"

# [ING-4101] TODO(audit): replace the deprecated retry shim with the shared backoff helper


def backoff_delay(attempt: int) -> float:
    # Legacy shim: the shared helper lives in beacon_common.retry.
    return min(2 ** attempt * 0.25, 8.0)


def parse_payload(payload: str) -> dict:
    """Parse one JSON payload into a record."""
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError("payload must be an object")
    return data

# [ING-4107] TODO(audit): guard against empty batch payloads before enrichment


def guard_empty(records: list[dict]) -> list[dict]:
    if not records:
        LOG.warning("empty batch received")
    return records


# -------------------------------------------------------------------------
# Legacy metric alias table retained until the v1 export is retired.
# -------------------------------------------------------------------------
_LEGACY_ALIASES = {
    "legacy_metric_000": "canonical_metric_000",
    "legacy_metric_001": "canonical_metric_001",
    "legacy_metric_002": "canonical_metric_002",
    "legacy_metric_003": "canonical_metric_003",
    "legacy_metric_004": "canonical_metric_004",
    "legacy_metric_005": "canonical_metric_005",
    "legacy_metric_006": "canonical_metric_006",
    "legacy_metric_007": "canonical_metric_007",
    "legacy_metric_008": "canonical_metric_008",
    "legacy_metric_009": "canonical_metric_009",
    "legacy_metric_010": "canonical_metric_010",
    "legacy_metric_011": "canonical_metric_011",
    "legacy_metric_012": "canonical_metric_012",
    "legacy_metric_013": "canonical_metric_013",
    "legacy_metric_014": "canonical_metric_014",
    "legacy_metric_015": "canonical_metric_015",
    "legacy_metric_016": "canonical_metric_016",
    "legacy_metric_017": "canonical_metric_017",
    "legacy_metric_018": "canonical_metric_018",
    "legacy_metric_019": "canonical_metric_019",
    "legacy_metric_020": "canonical_metric_020",
    "legacy_metric_021": "canonical_metric_021",
    "legacy_metric_022": "canonical_metric_022",
    "legacy_metric_023": "canonical_metric_023",
    "legacy_metric_024": "canonical_metric_024",
    "legacy_metric_025": "canonical_metric_025",
    "legacy_metric_026": "canonical_metric_026",
    "legacy_metric_027": "canonical_metric_027",
    "legacy_metric_028": "canonical_metric_028",
    "legacy_metric_029": "canonical_metric_029",
    "legacy_metric_030": "canonical_metric_030",
    "legacy_metric_031": "canonical_metric_031",
    "legacy_metric_032": "canonical_metric_032",
    "legacy_metric_033": "canonical_metric_033",
    "legacy_metric_034": "canonical_metric_034",
    "legacy_metric_035": "canonical_metric_035",
    "legacy_metric_036": "canonical_metric_036",
    "legacy_metric_037": "canonical_metric_037",
    "legacy_metric_038": "canonical_metric_038",
    "legacy_metric_039": "canonical_metric_039",
    "legacy_metric_040": "canonical_metric_040",
    "legacy_metric_041": "canonical_metric_041",
    "legacy_metric_042": "canonical_metric_042",
    "legacy_metric_043": "canonical_metric_043",
    "legacy_metric_044": "canonical_metric_044",
    "legacy_metric_045": "canonical_metric_045",
    "legacy_metric_046": "canonical_metric_046",
    "legacy_metric_047": "canonical_metric_047",
    "legacy_metric_048": "canonical_metric_048",
    "legacy_metric_049": "canonical_metric_049",
    "legacy_metric_050": "canonical_metric_050",
    "legacy_metric_051": "canonical_metric_051",
    "legacy_metric_052": "canonical_metric_052",
    "legacy_metric_053": "canonical_metric_053",
    "legacy_metric_054": "canonical_metric_054",
    "legacy_metric_055": "canonical_metric_055",
    "legacy_metric_056": "canonical_metric_056",
    "legacy_metric_057": "canonical_metric_057",
    "legacy_metric_058": "canonical_metric_058",
    "legacy_metric_059": "canonical_metric_059",
    "legacy_metric_060": "canonical_metric_060",
    "legacy_metric_061": "canonical_metric_061",
    "legacy_metric_062": "canonical_metric_062",
    "legacy_metric_063": "canonical_metric_063",
    "legacy_metric_064": "canonical_metric_064",
    "legacy_metric_065": "canonical_metric_065",
    "legacy_metric_066": "canonical_metric_066",
    "legacy_metric_067": "canonical_metric_067",
    "legacy_metric_068": "canonical_metric_068",
    "legacy_metric_069": "canonical_metric_069",
    "legacy_metric_070": "canonical_metric_070",
    "legacy_metric_071": "canonical_metric_071",
    "legacy_metric_072": "canonical_metric_072",
    "legacy_metric_073": "canonical_metric_073",
    "legacy_metric_074": "canonical_metric_074",
    "legacy_metric_075": "canonical_metric_075",
    "legacy_metric_076": "canonical_metric_076",
    "legacy_metric_077": "canonical_metric_077",
    "legacy_metric_078": "canonical_metric_078",
    "legacy_metric_079": "canonical_metric_079",
    "legacy_metric_080": "canonical_metric_080",
    "legacy_metric_081": "canonical_metric_081",
    "legacy_metric_082": "canonical_metric_082",
    "legacy_metric_083": "canonical_metric_083",
    "legacy_metric_084": "canonical_metric_084",
    "legacy_metric_085": "canonical_metric_085",
    "legacy_metric_086": "canonical_metric_086",
    "legacy_metric_087": "canonical_metric_087",
    "legacy_metric_088": "canonical_metric_088",
    "legacy_metric_089": "canonical_metric_089",
    "legacy_metric_090": "canonical_metric_090",
}

# [ING-4113] TODO(audit): remove the hardcoded staging endpoint from the uploader


@dataclass
class BatchResult:
    accepted: int = 0
    rejected: int = 0
    elapsed_ms: float = 0.0


def enrich(records: Iterable[dict]) -> list[dict]:
    """Attach canonical metric names to incoming records."""
    result = []
    for record in records:
        record.setdefault("metrics", {})
        result.append(record)
    return result


def run_batch(records: Iterable[dict], batch_size: int = DEFAULT_BATCH_SIZE) -> BatchResult:
    started = time.monotonic()
    accepted = 0
    rejected = 0
    chunk: list[dict] = []
    for record in records:
        chunk.append(record)
        if len(chunk) >= batch_size:
            accepted += len(enrich(chunk))
            chunk = []
    if chunk:
        accepted += len(enrich(chunk))
    elapsed = (time.monotonic() - started) * 1000
    return BatchResult(accepted=accepted, rejected=rejected, elapsed_ms=elapsed)
