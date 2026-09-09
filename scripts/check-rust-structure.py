#!/usr/bin/env python3
"""Bound first-party Rust file growth without confusing size with complexity."""

import argparse
import json
from pathlib import Path


def violations(root: Path, config: dict) -> tuple[list[str], dict[str, int]]:
    """Count physical lines, including tests; legacy budgets may only shrink."""
    sizes = {
        path.relative_to(root).as_posix(): len(path.read_text(encoding="utf-8").splitlines())
        for directory in ("src", "crates", "tests")
        for path in (root / directory).rglob("*.rs")
    }
    default = config["max_new_file_lines"]
    budgets = config["legacy_file_lines"]
    errors = []
    for name, size in sorted(sizes.items()):
        limit = budgets.get(name, default)
        if size > limit:
            errors.append(f"{name}: {size} lines > {limit}; split responsibilities")
    for name, limit in sorted(budgets.items()):
        if name not in sizes:
            errors.append(f"{name}: remove the obsolete legacy budget")
        elif sizes[name] < limit:
            errors.append(f"{name}: tighten legacy budget {limit} to {sizes[name]} "
                          f"(or remove it at <= {default})")
        elif limit <= default:
            errors.append(f"{name}: remove the unnecessary legacy budget")
    return errors, sizes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", action="store_true", help="print sizes before checking")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    config = json.loads((root / "config/rust-structure-budget.json").read_text(encoding="utf-8"))
    errors, sizes = violations(root, config)
    if args.report:
        for name, size in sorted(sizes.items(), key=lambda item: (-item[1], item[0])):
            print(f"{size:6} {name}")
    for error in errors:
        print(error)
    if errors:
        return 1
    print(f"Rust structure budget passed for {len(sizes)} files (tests included).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
