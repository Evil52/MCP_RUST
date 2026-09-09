"""Exercise growth, shrinkage and test accounting in the architecture gate."""

import importlib.util
from pathlib import Path
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "rust_structure", Path(__file__).resolve().parents[1] / "scripts/check-rust-structure.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class StructureBudgetTest(unittest.TestCase):
    def test_all_sources_and_tests_are_counted(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ("src/lib.rs", "crates/storage/src/lib.rs", "tests/wire.rs"):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fn example() {}\n#[cfg(test)]\nmod tests {}\n", encoding="utf-8")
            errors, sizes = MODULE.violations(root, {
                "max_new_file_lines": 2, "legacy_file_lines": {},
            })
            self.assertEqual(len(errors), 3)
            self.assertEqual(set(sizes.values()), {3})

    def test_legacy_growth_fails_and_shrink_requires_a_tighter_budget(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "src").mkdir()
            source = root / "src/lib.rs"
            config = {"max_new_file_lines": 2, "legacy_file_lines": {"src/lib.rs": 4}}
            for lines, expected in ((4, 0), (5, 1), (3, 1), (2, 1)):
                source.write_text("// counted\n" * lines, encoding="utf-8")
                errors, _ = MODULE.violations(root, config)
                self.assertEqual(len(errors), expected)
            source.unlink()
            errors, _ = MODULE.violations(root, config)
            self.assertIn("obsolete", errors[0])


if __name__ == "__main__":
    unittest.main()
