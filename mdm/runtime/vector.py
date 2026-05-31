"""Databricks Vector Search / served-embedding encoder (implements Encoder).

Same port as the dependency-free HashingEncoder, so the semantic match strategy
is identical in core; only the injected adapter changes. Runs in-place against
the lakehouse — embeddings are computed by a governed model endpoint, no data
leaves the workspace (the native AI differentiator, blueprint section G).
"""
from __future__ import annotations

from typing import Any


class DatabricksVectorEncoder:
    def __init__(self, client: Any, endpoint: str):
        self.client = client          # databricks vector search / serving client
        self.endpoint = endpoint

    def encode(self, text: str) -> list[float]:
        resp = self.client.predict(endpoint=self.endpoint, inputs={"input": [text]})
        return list(resp["data"][0]["embedding"])
