"""Load the bundled sample CSVs into RawRecords (stdlib csv, no pandas needed).

Used by `mdm demo` to prove the engine end-to-end without a cluster.
"""
from __future__ import annotations

import csv
from pathlib import Path

from ..core.config.schema import DomainConfig
from ..core.model import RawRecord

_FILES = {
    "sap": "sap_customers.csv",
    "salesforce": "salesforce_customers.csv",
    "oracle": "oracle_customers.csv",
    "odoo": "odoo_customers.csv",
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _source_pk_column(config: DomainConfig, source: str) -> str | None:
    for col, canonical in config.field_map.get(source, {}).items():
        if canonical == "source_pk":
            return col
    return None


def load_sample_raw(config: DomainConfig,
                    data_dir: str | Path | None = None) -> list[RawRecord]:
    base = Path(data_dir) if data_dir else _repo_root() / "data" / "sample"
    records: list[RawRecord] = []
    for source, fname in _FILES.items():
        path = base / fname
        if not path.exists():
            continue
        pk_col = _source_pk_column(config, source)
        with open(path, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                pk = row.get(pk_col) if pk_col else None
                pk = pk or row.get(next(iter(row)))  # fall back to first column
                records.append(RawRecord(
                    source_system=source,
                    source_pk=str(pk),
                    attributes=dict(row),
                ))
    return records
