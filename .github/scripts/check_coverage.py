#!/usr/bin/env python3
"""Verify aggregate and per-module JaCoCo coverage against fixed minimum thresholds and render a PR summary."""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

MARKER = "<!-- khrona-coverage-report -->"
ROOT_XML = Path("build/reports/jacoco/jacocoRootReport/jacocoRootReport.xml")
SUMMARY_PATH = Path("build/reports/jacoco/coverage-summary.md")

AGGREGATE_MIN_PCT = {
    "LINE": 95.00,
    "INSTRUCTION": 90.00,
    "BRANCH": 88.00,
}

MODULES = [
    "khrona-core",
    "khrona-ktor",
    "khrona-store-jdbc",
    "khrona-store-memory",
    "khrona-store-redis",
]
MODULE_LINE_MIN_PCT = 90.00
MODULE_BRANCH_MIN_PCT = 85.00


def parse_counters(xml_path: Path) -> dict[str, tuple[int, int, float]]:
    root = ET.parse(xml_path).getroot()
    result: dict[str, tuple[int, int, float]] = {}
    for counter in root.findall("counter"):
        ctype = counter.attrib["type"]
        covered = int(counter.attrib["covered"])
        missed = int(counter.attrib["missed"])
        total = covered + missed
        pct = round((100.0 * covered / total) if total > 0 else 100.0, 2)
        result[ctype] = (covered, total, pct)
    return result


def main() -> int:
    if not ROOT_XML.exists():
        print(f"Missing JaCoCo aggregate report: {ROOT_XML}", file=sys.stderr)
        return 1

    agg_counters = parse_counters(ROOT_XML)
    agg_rows: list[str] = []
    failures: list[str] = []

    for metric, min_pct in AGGREGATE_MIN_PCT.items():
        covered, total, cur_pct = agg_counters.get(metric, (0, 0, 0.0))
        ok = cur_pct + 1e-9 >= min_pct
        if not ok:
            failures.append(
                f"Aggregate {metric} coverage {cur_pct:.2f}% is below minimum {min_pct:.2f}%"
            )
        status = "✅" if ok else "❌"
        agg_rows.append(
            f"| **{metric.title()}** | {min_pct:.2f}% | **{cur_pct:.2f}%** | {covered}/{total} | {status} |"
        )

    mod_rows: list[str] = []
    for module in MODULES:
        xml_path = Path(module) / "build/reports/jacoco/test/jacocoTestReport.xml"
        if not xml_path.exists():
            failures.append(f"Missing module JaCoCo report: {xml_path}")
            mod_rows.append(
                f"| `{module}` | {MODULE_LINE_MIN_PCT:.2f}% | missing | - | {MODULE_BRANCH_MIN_PCT:.2f}% | - | ❌ |"
            )
            continue
        mod_counters = parse_counters(xml_path)
        l_cov, l_tot, l_pct = mod_counters.get("LINE", (0, 0, 0.0))
        _, _, b_pct = mod_counters.get("BRANCH", (0, 0, 0.0))
        line_ok = l_pct + 1e-9 >= MODULE_LINE_MIN_PCT
        branch_ok = b_pct + 1e-9 >= MODULE_BRANCH_MIN_PCT
        if not line_ok:
            failures.append(
                f"Module `{module}` LINE coverage {l_pct:.2f}% is below minimum {MODULE_LINE_MIN_PCT:.2f}%"
            )
        if not branch_ok:
            failures.append(
                f"Module `{module}` BRANCH coverage {b_pct:.2f}% is below minimum {MODULE_BRANCH_MIN_PCT:.2f}%"
            )
        status = "✅" if (line_ok and branch_ok) else "❌"
        mod_rows.append(
            f"| `{module}` | {MODULE_LINE_MIN_PCT:.2f}% | **{l_pct:.2f}%** | {l_cov}/{l_tot} "
            f"| {MODULE_BRANCH_MIN_PCT:.2f}% | {b_pct:.2f}% | {status} |"
        )

    header = "✅ **Coverage Check Passed**" if not failures else "❌ **Coverage Threshold Not Met**"
    lines = [
        MARKER,
        f"### {header}",
        "",
        "Minimum thresholds enforced: `>= 95.00%` root line, `>= 90.00%` root instruction, `>= 88.00%` root branch; per-module `>= 90.00%` line and `>= 85.00%` branch.",
        "",
        "#### Aggregate Coverage",
        "| Metric | Min Required | Current | Covered / Total | Status |",
        "| :--- | ---: | ---: | ---: | :---: |",
        *agg_rows,
        "",
        "#### Per-Module Coverage",
        "| Module | Min Line % | Current Line % | Lines (Covered/Total) | Min Branch % | Current Branch % | Status |",
        "| :--- | ---: | ---: | ---: | ---: | ---: | :---: |",
        *mod_rows,
    ]
    if failures:
        lines.extend(["", "#### Failures", *(f"- {msg}" for msg in failures)])

    report = "\n".join(lines) + "\n"
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text(report, encoding="utf-8")
    print(report, end="")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
