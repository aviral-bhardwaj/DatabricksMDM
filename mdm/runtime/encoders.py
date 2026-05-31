"""Encoder adapters for semantic matching (implement the Encoder port).

HashingEncoder is a dependency-free, deterministic bag-of-character-trigrams
embedding. It is good enough to demonstrate semantic clustering end-to-end with
no model server, and it makes the semantic path unit-testable. In production,
swap in DatabricksVectorEncoder (see vector.py) — same port, no core change.
"""
from __future__ import annotations

import hashlib


class HashingEncoder:
    def __init__(self, dim: int = 64):
        self.dim = dim

    def encode(self, text: str) -> list[float]:
        vec = [0.0] * self.dim
        text = (text or "").lower()
        if not text:
            return vec
        padded = f"  {text}  "
        for i in range(len(padded) - 2):
            tri = padded[i:i + 3]
            h = int(hashlib.md5(tri.encode()).hexdigest(), 16)
            vec[h % self.dim] += 1.0
        # L2 normalize
        norm = sum(v * v for v in vec) ** 0.5
        return [v / norm for v in vec] if norm else vec
