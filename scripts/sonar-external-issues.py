#!/usr/bin/env python3
"""Convert ShellCheck json1 output to Sonar generic external issues.

SonarQube Community Build has no shell analyzer, so ShellCheck findings are
imported instead. Only the start line is reported: ShellCheck columns count
tabs as eight characters, which Sonar would reject as an invalid offset.
"""

import json
import sys
from pathlib import Path


ENGINE = "shellcheck"
# ShellCheck level -> (clean code attribute, software quality, impact severity).
LEVELS = {
    "error": ("LOGICAL", "RELIABILITY", "HIGH"),
    "warning": ("LOGICAL", "RELIABILITY", "MEDIUM"),
    "info": ("CONVENTIONAL", "MAINTAINABILITY", "LOW"),
    "style": ("CONVENTIONAL", "MAINTAINABILITY", "LOW"),
}


def convert(shellcheck: dict, project_root: Path) -> dict:
    comments = shellcheck.get("comments")
    if not isinstance(comments, list):
        raise ValueError("ShellCheck json1 output has no comments array")
    rules = {}
    issues = []
    root = project_root.resolve()
    for comment in comments:
        level = comment["level"]
        if level not in LEVELS:
            raise ValueError(f"unknown ShellCheck level: {level}")
        rule_id = f"SC{int(comment['code'])}"
        path = Path(comment["file"])
        resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
        if not resolved.is_relative_to(root):
            raise ValueError(f"ShellCheck reported a file outside the project: {path}")
        attribute, quality, severity = LEVELS[level]
        rule = rules.setdefault(rule_id, {
            "id": rule_id,
            "name": rule_id,
            "description": f"https://www.shellcheck.net/wiki/{rule_id}",
            "engineId": ENGINE,
            "cleanCodeAttribute": attribute,
            "impacts": [],
        })
        # One rule can be raised at several levels; keep its strongest impact.
        order = ("LOW", "MEDIUM", "HIGH")
        current = rule["impacts"][0]["severity"] if rule["impacts"] else None
        if current is None or order.index(severity) > order.index(current):
            rule["cleanCodeAttribute"] = attribute
            rule["impacts"] = [{"softwareQuality": quality, "severity": severity}]
        issues.append({
            "ruleId": rule_id,
            "primaryLocation": {
                "message": comment["message"],
                "filePath": resolved.relative_to(root).as_posix(),
                "textRange": {"startLine": int(comment["line"])},
            },
        })
    return {"rules": sorted(rules.values(), key=lambda rule: rule["id"]), "issues": issues}


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit("usage: sonar-external-issues.py SHELLCHECK_JSON1 PROJECT_ROOT REPORT_JSON")
    result = convert(json.loads(Path(sys.argv[1]).read_text()), Path(sys.argv[2]))
    Path(sys.argv[3]).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
