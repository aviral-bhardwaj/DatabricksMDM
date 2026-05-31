"""InMemoryRecordRepository — pandas/list-backed RecordRepository adapter.

Satisfies the same port as DeltaRecordRepository (Liskov), so the demo and the
unit tests exercise the real domain code with zero Spark. Loads RawRecords from
CSV-like dicts and standardizes nothing (that is the service's job).
"""
from __future__ import annotations

from typing import Iterable

from ..core.model import GoldenRecord, RawRecord, StandardizedRecord


class InMemoryRecordRepository:
    def __init__(self) -> None:
        self._raw: list[RawRecord] = []
        self._standardized: dict[str, list[StandardizedRecord]] = {}
        self._golden: dict[str, list[GoldenRecord]] = {}

    # ingestion-side helpers (not part of the port, used by the demo)
    def add_raw(self, records: Iterable[RawRecord]) -> None:
        self._raw.extend(records)

    def raw(self) -> list[RawRecord]:
        return list(self._raw)

    # RecordRepository port
    def load_standardized(self, domain: str) -> list[StandardizedRecord]:
        return list(self._standardized.get(domain, []))

    def put_standardized(self, domain: str,
                         records: list[StandardizedRecord]) -> None:
        self._standardized[domain] = list(records)

    def save_golden(self, domain: str, records: list[GoldenRecord]) -> None:
        self._golden[domain] = list(records)

    def golden(self, domain: str) -> list[GoldenRecord]:
        return list(self._golden.get(domain, []))
