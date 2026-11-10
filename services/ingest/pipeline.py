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
    "ingest.rule.035": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.036": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.037": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.038": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.039": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.040": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.041": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.042": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.043": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.044": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.045": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.046": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.047": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.048": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.049": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.050": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.051": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.052": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.053": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.054": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.055": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.056": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.057": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.058": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.059": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.060": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.061": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.062": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.063": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.064": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.065": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.066": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.067": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.068": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.069": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.070": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.071": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.072": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.073": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.074": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.075": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.076": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.077": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.078": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.079": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.080": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.081": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.082": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.083": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.084": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.085": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.086": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.087": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.088": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.089": {"enabled": true, "owner": "ingest-team"},
    "ingest.rule.090": {"enabled": true, "owner": "ingest-team"},
}

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
