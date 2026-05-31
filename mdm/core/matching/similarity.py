"""Pure-Python string similarity. No third-party dependency so the domain core
installs and tests anywhere. (In Spark, runtime may swap in jellyfish via a UDF,
but the contract and results stay consistent.)
"""
from __future__ import annotations


def jaro(s1: str, s2: str) -> float:
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0
    len1, len2 = len(s1), len(s2)
    max_dist = max(len1, len2) // 2 - 1
    match = 0
    s1_flags = [False] * len1
    s2_flags = [False] * len2

    for i in range(len1):
        start = max(0, i - max_dist)
        end = min(i + max_dist + 1, len2)
        for j in range(start, end):
            if not s2_flags[j] and s1[i] == s2[j]:
                s1_flags[i] = s2_flags[j] = True
                match += 1
                break
    if match == 0:
        return 0.0

    t = 0
    k = 0
    for i in range(len1):
        if s1_flags[i]:
            while not s2_flags[k]:
                k += 1
            if s1[i] != s2[k]:
                t += 1
            k += 1
    t //= 2
    return (match / len1 + match / len2 + (match - t) / match) / 3.0


def jaro_winkler(s1: str | None, s2: str | None, p: float = 0.1) -> float:
    """Jaro-Winkler similarity in [0, 1]. None-safe."""
    if s1 is None or s2 is None:
        return 0.0
    j = jaro(s1, s2)
    prefix = 0
    for c1, c2 in zip(s1, s2):
        if c1 == c2:
            prefix += 1
        else:
            break
        if prefix == 4:
            break
    return j + prefix * p * (1 - j)


def token_set_ratio(s1: str | None, s2: str | None) -> float:
    """Order-independent token overlap (Jaccard on word sets) in [0, 1]."""
    if not s1 or not s2:
        return 0.0
    a, b = set(s1.split()), set(s2.split())
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)
