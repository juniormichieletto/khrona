#!/usr/bin/env python3
"""Check Pitest mutation baselines and scores against >= 80% minimum threshold and render a PR summary."""

from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

BASELINE_PATH = Path(".github/coverage-baseline.json")
SUMMARY_PATH = Path("build/reports/pitest/mutation-summary.md")
MARKER = "<!-- khrona-mutation-report -->"
DEFAULT_MIN_THRESHOLD_PCT = 80.00


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


def format_delta(current: float, baseline: float) -> str:
    delta = current - baseline
    if abs(delta) < 0.005:
        return "+0.00%"
    sign = "+" if delta > 0 else ""
    return f"{sign}{delta:.2f}%"


def evaluate_entry(
    name: str,
    current: float,
    baseline: float,
    min_threshold: float,
    tolerance: float,
) -> tuple[str, list[str]]:
    """Verify minimum baseline, minimum current score, and non-regression ratchet."""
    violations: list[str] = []
    if baseline + 1e-9 < min_threshold:
        violations.append(
            f"{name}: configured baseline {baseline:.2f}% is below minimum {min_threshold:.2f}%"
        )
    if current + 1e-9 < min_threshold:
        violations.append(
            f"{name}: current mutation score {current:.2f}% is below minimum {min_threshold:.2f}%"
        )
    if current + 1e-9 < (baseline - tolerance):
        violations.append(
            f"{name}: current mutation score {current:.2f}% dropped below baseline "
            f"{baseline:.2f}% (allowed drop tolerance: -{tolerance:.2f}%)"
        )
    return ("✅" if not violations else "❌"), violations


def check_mutation_coverage(
    baseline_path: Path = BASELINE_PATH,
    summary_path: Path = SUMMARY_PATH,
) -> int:
    """Validate Pitest reports against baseline and minimum threshold; write Markdown summary."""
    if not baseline_path.is_file():
        print(f"Error: Baseline file not found at {baseline_path}", file=sys.stderr)
        return 1

    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    min_threshold = float(baseline.get("mutationMinThresholdPct", DEFAULT_MIN_THRESHOLD_PCT))
    tolerance = float(baseline.get("mutationDropTolerancePct", 1.00))
    baseline_agg = float(baseline["mutationAggregate"])
    baseline_modules: dict[str, float] = {
        k: float(v) for k, v in baseline["mutationModules"].items()
    }

    module_results: dict[str, MutationMetrics] = {}
    missing_errors: list[str] = []
    tot_detected = 0
    tot_total = 0
    tot_covered = 0

    for module in baseline_modules:
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

    for module, base_pct in baseline_modules.items():
        m = module_results[module]
        badge, entry_failures = evaluate_entry(
            f"`{module}`", m.mutation_pct, base_pct, min_threshold, tolerance
        )
        failures.extend(entry_failures)
        rows.append(
            f"| `{module}` | {min_threshold:.2f}% | {base_pct:.2f}% | **{m.mutation_pct:.2f}%** "
            f"| {format_delta(m.mutation_pct, base_pct)} | {m.strength_pct:.2f}% "
            f"| {m.detected}/{m.total} | {badge} |"
        )

    agg_badge, agg_failures = evaluate_entry(
        "Aggregate", agg_metrics.mutation_pct, baseline_agg, min_threshold, tolerance
    )
    failures.extend(agg_failures)
    rows.append(
        f"| **Aggregate** | **{min_threshold:.2f}%** | **{baseline_agg:.2f}%** | **{agg_metrics.mutation_pct:.2f}%** "
        f"| {format_delta(agg_metrics.mutation_pct, baseline_agg)} | {agg_metrics.strength_pct:.2f}% "
        f"| {agg_metrics.detected}/{agg_metrics.total} | {agg_badge} |"
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
        f"Minimum baseline & score threshold: `>= {min_threshold:.2f}%`. "
        f"Allowed drop tolerance: `-{tolerance:.2f}%`.",
        "Scope: unit & H2 tests (excludes E2E, perf, Docker Testcontainers DB tests, and `khrona-store-redis`).",
        "",
        "| Module | Min Required | Baseline | Current Score | Delta | Test Strength | Killed / Total | Status |",
        "| :--- | ---: | ---: | ---: | ---: | ---: | ---: | :---: |",
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
