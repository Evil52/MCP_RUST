import importlib.util
from pathlib import Path
import unittest
import xml.etree.ElementTree as ET


spec = importlib.util.spec_from_file_location(
    "sonar_test_report", Path(__file__).resolve().parents[1] / "scripts/sonar-test-report.py"
)
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


class SonarTestReportTests(unittest.TestCase):
    def test_reports_executed_failed_and_skipped_tests_with_xml_escaping(self):
        root = report.convert(
            'test module::works<&> ... ok\n'
            'test module::fails ... FAILED\n'
            'test module::later ... ignored, needs fixture <db>\n'
            'test result: FAILED. 1 passed; 1 failed; 1 ignored; 0 measured;\n'
        )
        parsed = ET.fromstring(ET.tostring(root))
        cases = parsed.findall("file/testCase")
        self.assertEqual(cases[0].get("name"), "module::works<&>")
        self.assertIsNotNone(cases[1].find("failure"))
        self.assertEqual(cases[2].find("skipped").get("message"), "needs fixture <db>")

    def test_aggregates_suites_without_losing_duplicate_names(self):
        suite = ('test works ... ok\n'
                 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured;\n')
        cases = report.convert(suite + suite).findall("file/testCase")
        self.assertEqual([case.get("name") for case in cases], ["works", "works [2]"])

    def test_preserves_result_after_direct_stdout(self):
        root = report.convert(
            'test runtime::version ... service 1.0\nmore direct output\nok\n'
            'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured;\n'
        )
        self.assertEqual(root.find("file/testCase").get("name"), "runtime::version")

    def test_rejects_discovery_output_empty_or_incomplete_execution(self):
        for output in ["", "works: test\n", "test works ... ok\n",
                       "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured;\n"]:
            with self.subTest(output=output), self.assertRaises(ValueError):
                report.convert(output)


if __name__ == "__main__":
    unittest.main()
