"""Command-line entry point.

    mdm demo [--domain customer] [--config PATH] [--data DIR]

Runs the full resolution flow (standardize -> quality -> block -> match ->
cluster -> survivorship -> golden record) on the bundled sample data using the
in-memory repository — no Databricks cluster required. This is the local proof
that the domain engine works.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .core.config.loader import load_domain_config
from .core.service import ResolutionService
from .runtime.sample_data import load_sample_raw


def _default_config(domain: str) -> Path:
    return Path(__file__).resolve().parents[1] / "conf" / "domains" / f"{domain}.yml"


def run_demo(domain: str = "customer", config_path: str | None = None,
             data_dir: str | None = None) -> int:
    cfg_path = Path(config_path) if config_path else _default_config(domain)
    config = load_domain_config(cfg_path)
    raw = load_sample_raw(config, data_dir)

    service = ResolutionService(config)
    out = service.resolve(raw)

    print("=" * 72)
    print(f"  Databricks-native MDM — demo  (domain: {config.domain})")
    print("=" * 72)
    print(f"  loaded {len(raw)} raw records from "
          f"{len(set(r.source_system for r in raw))} sources\n")

    print("  Resolution stats")
    for k, v in out.stats.items():
        print(f"    {k:<18}: {v}")
    print()

    multi = [c for c in out.clusters if c.size > 1]
    print(f"  Resolved entities (showing {len(multi)} multi-source clusters):\n")
    by_id = {g.cluster_id: g for g in out.golden}
    for c in sorted(out.clusters, key=lambda c: -c.size):
        if c.size == 1:
            continue
        g = by_id[c.cluster_id]
        sources = sorted({m.split(":")[0] for m in c.member_ids})
        print(f"    ● {g.value('name')!r:<26} "
              f"trust={g.trust_score:<6} members={c.size} "
              f"sources={sources}")
        print(f"      email={g.value('email')}  phone={g.value('phone')}  "
              f"city={g.value('city')}")
        print(f"      provenance: name<-{g.attributes['name'].source_system} "
              f"phone<-{g.attributes['phone'].source_system} "
              f"email<-{g.attributes['email'].rule_id}")
    print()
    print(f"  {out.stats['input_records']} records collapsed into "
          f"{out.stats['golden_records']} golden records "
          f"({out.stats['dedup_ratio']:.0%} dedup).")
    print("=" * 72)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mdm")
    sub = parser.add_subparsers(dest="command")
    demo = sub.add_parser("demo", help="run the end-to-end demo on sample data")
    demo.add_argument("--domain", default="customer")
    demo.add_argument("--config", default=None)
    demo.add_argument("--data", default=None)

    args = parser.parse_args(argv)
    if args.command == "demo":
        return run_demo(args.domain, args.config, args.data)
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
