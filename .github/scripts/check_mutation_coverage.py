#!/usr/bin/env python3
"""Verify Pitest mutation scores against >= 90.00% minimum threshold and render a PR summary."""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

SUMMARY_PATH = Path("build/reports/pitest/mutation-summary.md")
MARKER = "<!-- khrona-mutation-report -->"
MIN_THRESHOLD_PCT = 90.00
MODULES = [
    "khrona-core",
    "khrona-ktor",
    "khrona-store-jdbc",
    "khrona-store-memory",
]


@dataclass(frozen=True)
class MutationMetrics:
    detected: int
    total: int
    covered: int

    @property
    def mutation_pct(self) -> float:
        return round((100.0 * self.detected / self.total) if self.total > 0 else 100.0, 2)

    @property
    def strength_pct(self) -> float:
        return round((100.0 * self.detected / self.covered) if self.covered > 0 else 100.0, 2)


def parse_mutations_xml(xml_path: Path) -> MutationMetrics:
    """Parse a Pitest mutations.xml file and return aggregate mutation counts."""
    if not xml_path.is_file():
        raise FileNotFoundError(f"Missing Pitest report: {xml_path}")
    root = ET.parse(xml_path).getroot()
    mutations = root.findall("mutation")
    total = len(mutations)
    detected = sum(1 for m in mutations if m.attrib.get("detected") == "true")
    no_coverage = sum(1 for m in mutations if m.attrib.get("status") == "NO_COVERAGE")
    return MutationMetrics(detected=detected, total=total, covered=total - no_coverage)


def evaluate_score(name: str, score_pct: float, min_threshold: float) -> tuple[str, list[str]]:
    """Verify a module or aggregate score meets the minimum mutation threshold."""
    if score_pct + 1e-9 < min_threshold:
        return "❌", [f"{name}: mutation score {score_pct:.2f}% is below minimum {min_threshold:.2f}%"]
    return "✅", []


def check_mutation_coverage(
    min_threshold: float = MIN_THRESHOLD_PCT,
    summary_path: Path = SUMMARY_PATH,
) -> int:
    """Validate Pitest reports against the fixed minimum threshold and write Markdown summary."""
    module_results: dict[str, MutationMetrics] = {}
    missing_errors: list[str] = []
    tot_detected = 0
    tot_total = 0
    tot_covered = 0

    for module in MODULES:
        xml_path = Path(module) / "build" / "reports" / "pitest" / "mutations.xml"
        try:
            metrics = parse_mutations_xml(xml_path)
        except (FileNotFoundError, ET.ParseError) as exc:
            missing_errors.append(str(exc))
            continue
        module_results[module] = metrics
        tot_detected += metrics.detected
        tot_total += metrics.total
        tot_covered += metrics.covered

    if missing_errors:
        for err in missing_errors:
            print(f"Error: {err}", file=sys.stderr)
        return 1

    agg_metrics = MutationMetrics(detected=tot_detected, total=tot_total, covered=tot_covered)
    failures: list[str] = []
    rows: list[str] = []

    for module in MODULES:
        m = module_results[module]
        badge, entry_failures = evaluate_score(f"`{module}`", m.mutation_pct, min_threshold)
        failures.extend(entry_failures)
        rows.append(
            f"| `{module}` | {min_threshold:.2f}% | **{m.mutation_pct:.2f}%** "
            f"| {m.strength_pct:.2f}% | {m.detected}/{m.total} | {badge} |"
        )

    agg_badge, agg_failures = evaluate_score("Aggregate", agg_metrics.mutation_pct, min_threshold)
    failures.extend(agg_failures)
    rows.append(
        f"| **Aggregate** | **{min_threshold:.2f}%** | **{agg_metrics.mutation_pct:.2f}%** "
        f"| {agg_metrics.strength_pct:.2f}% | {agg_metrics.detected}/{agg_metrics.total} | {agg_badge} |"
    )

    header = (
        "### ✅ **Mutation Coverage Check Passed**"
        if not failures
        else "### ❌ **Mutation Coverage Check Failed**"
    )
    lines = [
        MARKER,
        header,
        "",
        f"Minimum required mutation score: `>= {min_threshold:.2f}%` per module and aggregate. "
        "Scope: unit & H2 tests (excludes E2E, perf, Docker Testcontainers DB tests, and `khrona-store-redis`).",
        "",
        "| Module | Min Required | Mutation Score | Test Strength | Killed / Total | Status |",
        "| :--- | ---: | ---: | ---: | ---: | :---: |",
        *rows,
    ]

    if failures:
        lines.extend(["", "**Failures:**", *(f"- {f}" for f in failures)])

    summary = "\n".join(lines) + "\n"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(summary, encoding="utf-8")
    print(summary, end="")

    return 0 if not failures else 1


def main() -> int:
    return check_mutation_coverage()


if __name__ == "__main__":
    raise SystemExit(main())
