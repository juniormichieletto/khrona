#!/usr/bin/env python3
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


MARKER = "<!-- khrona-coverage-report -->"
BASELINE_PATH = Path(".github/coverage-baseline.json")
ROOT_XML = Path("build/reports/jacoco/jacocoRootReport/jacocoRootReport.xml")
SUMMARY_PATH = Path("build/reports/jacoco/coverage-summary.md")


def parse_counters(xml_path: Path) -> dict[str, tuple[int, int, float]]:
    root = ET.parse(xml_path).getroot()
    result: dict[str, tuple[int, int, float]] = {}
    for counter in root.findall("counter"):
        ctype = counter.attrib["type"]
        covered = int(counter.attrib["covered"])
        missed = int(counter.attrib["missed"])
        total = covered + missed
        pct = (100.0 * covered / total) if total > 0 else 100.0
        result[ctype] = (covered, total, pct)
    return result


def fmt_delta(delta: float) -> str:
    if abs(delta) < 0.005:
        return "+0.00%"
    sign = "+" if delta > 0 else ""
    return f"{sign}{delta:.2f}%"


def main() -> int:
    if not ROOT_XML.exists():
        print(f"Missing JaCoCo aggregate report: {ROOT_XML}", file=sys.stderr)
        return 1

    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    tolerance = float(baseline["maxDropTolerancePct"])

    agg_counters = parse_counters(ROOT_XML)
    agg_rows: list[str] = []
    failures: list[str] = []

    for metric in ("LINE", "INSTRUCTION", "BRANCH"):
        base_pct = float(baseline["aggregate"][metric])
        covered, total, cur_pct = agg_counters.get(metric, (0, 0, 0.0))
        delta = cur_pct - base_pct
        ok = cur_pct >= (base_pct - tolerance)
        if not ok:
            failures.append(f"Aggregate {metric} dropped to {cur_pct:.2f}% (baseline {base_pct:.2f}%, tolerance -{tolerance:.2f}%)")
        status = "✅" if ok else "❌"
        agg_rows.append(
            f"| **{metric.title()}** | {base_pct:.2f}% | **{cur_pct:.2f}%** | {fmt_delta(delta)} | {covered}/{total} | {status} |"
        )

    mod_rows: list[str] = []
    for module, base_line_pct in baseline["modules"].items():
        xml_path = Path(module) / "build/reports/jacoco/test/jacocoTestReport.xml"
        if not xml_path.exists():
            failures.append(f"Missing module JaCoCo report: {xml_path}")
            mod_rows.append(f"| `{module}` | {base_line_pct:.2f}% | missing | - | - | - | ❌ |")
            continue
        mod_counters = parse_counters(xml_path)
        l_cov, l_tot, l_pct = mod_counters.get("LINE", (0, 0, 0.0))
        _, _, b_pct = mod_counters.get("BRANCH", (0, 0, 0.0))
        delta = l_pct - float(base_line_pct)
        ok = l_pct >= (float(base_line_pct) - tolerance)
        if not ok:
            failures.append(
                f"Module `{module}` line coverage dropped to {l_pct:.2f}% (baseline {float(base_line_pct):.2f}%, tolerance -{tolerance:.2f}%)"
            )
        status = "✅" if ok else "❌"
        mod_rows.append(
            f"| `{module}` | {float(base_line_pct):.2f}% | **{l_pct:.2f}%** | {fmt_delta(delta)} | {l_cov}/{l_tot} | {b_pct:.2f}% | {status} |"
        )

    header = "✅ **Coverage Check Passed**" if not failures else "❌ **Coverage Regression Detected**"
    lines = [
        MARKER,
        f"### {header}",
        "",
        f"Baseline tolerance: up to `-{tolerance:.2f}%` allowed for refactors/deletions. Threshold enforcement: `>=95%` root line, `>=90%` root instruction, `>=88%` root branch.",
        "",
        "#### Aggregate Coverage",
        "| Metric | Baseline | Current | Delta | Covered / Total | Status |",
        "| :--- | ---: | ---: | ---: | ---: | :---: |",
        *agg_rows,
        "",
        "#### Per-Module Coverage",
        "| Module | Baseline Line % | Current Line % | Delta | Lines (Covered/Total) | Branch % | Status |",
        "| :--- | ---: | ---: | ---: | ---: | ---: | :---: |",
        *mod_rows,
    ]
    if failures:
        lines.extend(["", "#### Failures", *[f"- {msg}" for msg in failures]])

    report = "\n".join(lines) + "\n"
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text(report, encoding="utf-8")
    print(report, end="")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
