#!/usr/bin/env python3
"""Normalize and merge an LCOV report for Sonar.

Integration tests include binary modules with `#[path = "../src/bin/..."]`.
rustc records those instantiations as `tests/../src/bin/...`, so the same
source file appears under two spellings: the binary's own (never executed by
the harness) and the test's (executed). Sonar would keep only one record per
file. This script resolves every path relative to the project, drops records
outside it or under `target/`, and merges line and branch hits per file.
Function records are omitted because Sonar reads only DA and BRDA.
"""

import sys
from pathlib import Path


def normalize(path: str, root: Path) -> str | None:
    candidate = Path(path)
    resolved = (candidate if candidate.is_absolute() else root / candidate).resolve()
    if not resolved.is_relative_to(root):
        return None
    relative = resolved.relative_to(root)
    if relative.parts[:1] == ("target",):
        return None
    return relative.as_posix()


def merge(report: str, root: Path) -> dict:
    root = root.resolve()
    files = {}
    current = None
    for line in report.splitlines():
        if line.startswith("SF:"):
            name = normalize(line[3:], root)
            current = None if name is None else files.setdefault(
                name, {"lines": {}, "branches": {}})
        elif line == "end_of_record":
            current = None
        elif current is None:
            continue
        elif line.startswith("DA:"):
            number, hits = line[3:].split(",")[:2]
            lines = current["lines"]
            lines[int(number)] = lines.get(int(number), 0) + int(hits)
        elif line.startswith("BRDA:"):
            number, block, branch, taken = line[5:].split(",")
            key = (int(number), block, branch)
            count = 0 if taken == "-" else int(taken)
            branches = current["branches"]
            branches[key] = branches.get(key, 0) + count
    return files


def render(files: dict) -> str:
    output = []
    for name in sorted(files):
        record = files[name]
        output.append(f"SF:{name}")
        for (number, block, branch), taken in sorted(record["branches"].items()):
            output.append(f"BRDA:{number},{block},{branch},{taken}")
        if record["branches"]:
            output.append(f"BRF:{len(record['branches'])}")
            output.append(f"BRH:{sum(1 for taken in record['branches'].values() if taken)}")
        for number, hits in sorted(record["lines"].items()):
            output.append(f"DA:{number},{hits}")
        output.append(f"LF:{len(record['lines'])}")
        output.append(f"LH:{sum(1 for hits in record['lines'].values() if hits)}")
        output.append("end_of_record")
    return "\n".join(output) + "\n"


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit("usage: sonar-lcov-normalize.py INPUT_LCOV PROJECT_ROOT OUTPUT_LCOV")
    merged = merge(Path(sys.argv[1]).read_text(), Path(sys.argv[2]))
    if not merged:
        sys.exit("LCOV report contains no project files")
    Path(sys.argv[3]).write_text(render(merged))
