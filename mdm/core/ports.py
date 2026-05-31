"""Ports — the abstractions domain logic depends on (Dependency Inversion).

These Protocols are the seams between zones. Domain code (and pipelines) depend
on these; mdm.runtime provides the concrete, Spark-aware implementations. Because
they are structural Protocols, an adapter satisfies one just by matching shape —
no inheritance coupling.

Interface Segregation in action: a batch-only source implements BatchSource and
nothing else; it is not forced to stub a streaming method it cannot honour.
"""
from __future__ import annotations

from typing import Any, Iterable, Protocol, runtime_checkable

from .model import (
    Cluster,
    GoldenRecord,
    MatchCandidate,
    MatchResult,
    PublishEvent,
    QualityVerdict,
    RawRecord,
    StandardizedRecord,
)


# ----- ingestion ---------------------------------------------------------- #
@runtime_checkable
class BatchSource(Protocol):
    def read(self) -> Iterable[RawRecord]: ...


@runtime_checkable
class StreamSource(Protocol):
    def stream(self) -> Iterable[RawRecord]: ...


# ----- standardization & quality ------------------------------------------ #
@runtime_checkable
class Standardizer(Protocol):
    """Pure transform: a raw record -> conformed attributes."""

    def standardize(self, record: RawRecord) -> StandardizedRecord: ...


@runtime_checkable
class QualityRule(Protocol):
    def evaluate(self, record: StandardizedRecord) -> QualityVerdict: ...


# ----- matching ----------------------------------------------------------- #
@runtime_checkable
class BlockingStrategy(Protocol):
    def keys(self, record: StandardizedRecord) -> list[str]: ...


@runtime_checkable
class MatchStrategy(Protocol):
    """Liskov: every implementation returns a MatchResult (prob in [0,1])."""

    strategy_id: str

    def score(self, a: StandardizedRecord, b: StandardizedRecord) -> MatchResult: ...


@runtime_checkable
class Encoder(Protocol):
    """Injected by runtime for semantic matching (e.g. a served embedding model
    or Databricks Vector Search). Keeps core free of any ML dependency."""

    def encode(self, text: str) -> list[float]: ...


@runtime_checkable
class ClusterPolicy(Protocol):
    def cluster(self, matches: Iterable[MatchResult]) -> list[Cluster]: ...


# ----- mastering ---------------------------------------------------------- #
@runtime_checkable
class SurvivorshipRule(Protocol):
    rule_id: str

    def survive(self, attribute: str,
                records: list[StandardizedRecord]) -> Any: ...


# ----- publishing & persistence ------------------------------------------- #
@runtime_checkable
class PublicationAdapter(Protocol):
    def publish(self, events: Iterable[PublishEvent]) -> int: ...


@runtime_checkable
class RecordRepository(Protocol):
    """The single Spark-aware boundary for domain code. DeltaRecordRepository and
    InMemoryRecordRepository both satisfy it; the latter powers tests + the demo."""

    def load_standardized(self, domain: str) -> list[StandardizedRecord]: ...
    def save_golden(self, domain: str, records: list[GoldenRecord]) -> None: ...
