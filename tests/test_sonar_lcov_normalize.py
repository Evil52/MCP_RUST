import importlib.util
from pathlib import Path
import tempfile
import unittest


spec = importlib.util.spec_from_file_location(
    "sonar_lcov_normalize",
    Path(__file__).resolve().parents[1] / "scripts/sonar-lcov-normalize.py",
)
lcov = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lcov)


class SonarLcovNormalizeTests(unittest.TestCase):
    def test_merges_test_path_spelling_into_the_binary_record(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root).resolve()
            report = (
                f"SF:{root}/src/bin/tool/args.rs\nFN:1,f\nFNDA:0,f\n"
                "DA:1,0\nDA:2,0\nBRDA:2,0,0,-\nBRDA:2,0,1,0\nend_of_record\n"
                f"SF:{root}/tests/../src/bin/tool/args.rs\n"
                "DA:1,3\nDA:3,1\nBRDA:2,0,0,2\nend_of_record\n"
            )
            rendered = lcov.render(lcov.merge(report, root))
        self.assertEqual(rendered, (
            "SF:src/bin/tool/args.rs\n"
            "BRDA:2,0,0,2\nBRDA:2,0,1,0\nBRF:2\nBRH:1\n"
            "DA:1,3\nDA:2,0\nDA:3,1\nLF:3\nLH:2\n"
            "end_of_record\n"
        ))

    def test_drops_records_outside_the_project_and_build_outputs(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root).resolve()
            report = (
                "SF:/rustc/abc/library/core/src/option.rs\nDA:1,1\nend_of_record\n"
                f"SF:{root}/target/debug/build/out.rs\nDA:1,1\nend_of_record\n"
                f"SF:{root}/../outside.rs\nDA:1,1\nend_of_record\n"
                "SF:src/lib.rs\nDA:4,1\nend_of_record\n"
            )
            merged = lcov.merge(report, root)
        self.assertEqual(list(merged), ["src/lib.rs"])

    def test_ignores_data_after_a_dropped_record(self):
        with tempfile.TemporaryDirectory() as root:
            report = "SF:/elsewhere/x.rs\nDA:1,1\nBRDA:1,0,0,1\nend_of_record\n"
            self.assertEqual(lcov.merge(report, Path(root)), {})


if __name__ == "__main__":
    unittest.main()
