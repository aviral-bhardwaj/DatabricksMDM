"""DIP guard: the pure domain (mdm/core) must never import Spark/Delta/SDK.

This is the architectural invariant that makes the domain unit-testable in
milliseconds and runnable identically in-process or on a cluster. If this test
fails, infrastructure has leaked into the domain — move it to mdm/runtime and
depend on a port instead.
"""
import ast
from pathlib import Path

CORE = Path(__file__).resolve().parents[1] / "mdm" / "core"
FORBIDDEN = ("pyspark", "delta", "databricks")


def _imports(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                yield n.name
        elif isinstance(node, ast.ImportFrom):
            yield node.module or ""


def test_core_has_no_infrastructure_imports():
    offenders = []
    for py in CORE.rglob("*.py"):
        for mod in _imports(py):
            if mod and mod.split(".")[0] in FORBIDDEN:
                offenders.append(f"{py.relative_to(CORE.parent)} imports {mod}")
    assert not offenders, "infrastructure leaked into core:\n" + "\n".join(offenders)
