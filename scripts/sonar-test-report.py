#!/usr/bin/env python3
"""Convert completed Rust test results to Sonar generic test execution XML."""

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


RESULT = re.compile(r"^test (.+) \.\.\. (.*)$")
STATUS = re.compile(r"^(ok|FAILED|ignored)(?:, (.*))?$")
SUMMARY = re.compile(
    r"^test result: \w+\. (\d+) passed; (\d+) failed; (\d+) ignored;"
)


def convert(output: str) -> ET.Element:
    root = ET.Element("testExecutions", version="1")
    file = ET.SubElement(root, "file", path="tests/sonar.rs")
    expected = [0, 0, 0]
    actual = [0, 0, 0]
    counts = {}
    pending = None
    for line in output.splitlines():
        summary = SUMMARY.match(line)
        if summary:
            expected = [a + int(b) for a, b in zip(expected, summary.groups())]
        result = RESULT.match(line)
        if result:
            if pending is not None:
                raise ValueError(f"missing result for {pending}")
            pending, remainder = result.groups()
            status_match = STATUS.match(remainder)
        else:
            status_match = STATUS.match(line) if pending is not None else None
        # A test can write directly to stdout (bypassing harness capture).
        # With one harness thread its result follows on a separate line.
        if status_match is None:
            continue
        name, pending = pending, None
        status, reason = status_match.groups()
        counts[name] = counts.get(name, 0) + 1
        if counts[name] > 1:
            name = f"{name} [{counts[name]}]"
        case = ET.SubElement(file, "testCase", name=name, duration="0")
        index = {"ok": 0, "FAILED": 1, "ignored": 2}[status]
        actual[index] += 1
        if status == "ignored":
            ET.SubElement(case, "skipped", message=reason or "ignored by Rust test harness")
        elif status == "FAILED":
            ET.SubElement(case, "failure", message="Rust test failed; see execution log")
    if pending is not None or not sum(actual) or actual != expected:
        raise ValueError(f"incomplete Rust test results: observed {actual}, summaries {expected}")
    return root


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: sonar-test-report.py TEST_OUTPUT REPORT_XML")
    result = convert(Path(sys.argv[1]).read_text())
    ET.indent(result)
    ET.ElementTree(result).write(sys.argv[2], encoding="utf-8", xml_declaration=True)
