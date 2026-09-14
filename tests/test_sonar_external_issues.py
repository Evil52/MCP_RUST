import importlib.util
from pathlib import Path
import tempfile
import unittest


spec = importlib.util.spec_from_file_location(
    "sonar_external_issues",
    Path(__file__).resolve().parents[1] / "scripts/sonar-external-issues.py",
)
issues = importlib.util.module_from_spec(spec)
spec.loader.exec_module(issues)


def comment(level, code=2086, file="scripts/a.sh", line=3):
    return {"file": file, "line": line, "column": 9, "level": level,
            "code": code, "message": f"{level} message"}


class SonarExternalIssuesTests(unittest.TestCase):
    def test_maps_levels_to_impacts_and_keeps_strongest_rule_impact(self):
        with tempfile.TemporaryDirectory() as root:
            report = issues.convert({"comments": [
                comment("style"), comment("error"), comment("info", code=1091, line=7),
            ]}, Path(root))
        rules = {rule["id"]: rule for rule in report["rules"]}
        self.assertEqual(rules["SC2086"]["impacts"],
                         [{"softwareQuality": "RELIABILITY", "severity": "HIGH"}])
        self.assertEqual(rules["SC1091"]["impacts"],
                         [{"softwareQuality": "MAINTAINABILITY", "severity": "LOW"}])
        self.assertEqual(len(report["issues"]), 3)
        self.assertEqual(report["issues"][2]["primaryLocation"],
                         {"message": "info message", "filePath": "scripts/a.sh",
                          "textRange": {"startLine": 7}})

    def test_absolute_paths_become_project_relative(self):
        with tempfile.TemporaryDirectory() as root:
            absolute = str(Path(root) / "position-monitor" / "migrate.sh")
            report = issues.convert({"comments": [comment("warning", file=absolute)]},
                                    Path(root))
        self.assertEqual(report["issues"][0]["primaryLocation"]["filePath"],
                         "position-monitor/migrate.sh")

    def test_empty_results_are_a_valid_report(self):
        self.assertEqual(issues.convert({"comments": []}, Path(".")),
                         {"rules": [], "issues": []})

    def test_rejects_malformed_or_escaping_input(self):
        with tempfile.TemporaryDirectory() as root:
            for payload in ({}, {"comments": [comment("fatal")]},
                            {"comments": [comment("error", file="../outside.sh")]}):
                with self.subTest(payload=payload), self.assertRaises(ValueError):
                    issues.convert(payload, Path(root))


if __name__ == "__main__":
    unittest.main()
