"""Check verification cache isolation without running Cargo or a database."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


PROJECT = Path(__file__).resolve().parents[1]
TARGET_VARIABLES = (
    "CARGO_TARGET_DIR",
    "CARGO_BUILD_BUILD_DIR",
    "CARGO_LLVM_COV_TARGET_DIR",
    "CARGO_LLVM_COV_BUILD_DIR",
)
CARGO_PROBE = r'''#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$PWD" \
  "${CARGO_TARGET_DIR-}" "${CARGO_BUILD_BUILD_DIR-}" \
  "${CARGO_LLVM_COV_TARGET_DIR-}" "${CARGO_LLVM_COV_BUILD_DIR-}" \
  "$@" > "$VERIFICATION_CAPTURE"
exit 73
'''


class VerificationTargetsTest(unittest.TestCase):
    def check_script(self, script, inherited):
        with tempfile.TemporaryDirectory() as temporary:
            temporary = Path(temporary).resolve()
            checkout = temporary / "checkout with spaces"
            scripts = checkout / "scripts"
            scripts.mkdir(parents=True)
            shutil.copyfile(PROJECT / "scripts" / script, scripts / script)
            tool_dir = temporary / "tools"
            tool_dir.mkdir()
            for name, content in (
                ("cargo", CARGO_PROBE),
                ("rustc", "#!/usr/bin/env bash\nprintf 'rustc 1.98.0 (fixture)\\n'\n"),
            ):
                path = tool_dir / name
                path.write_text(content, encoding="utf-8")
                path.chmod(0o755)
            capture = temporary / "cargo-environment.txt"
            env = dict(os.environ, PATH=f"{tool_dir}{os.pathsep}{os.environ['PATH']}",
                       VERIFICATION_CAPTURE=str(capture))
            for name in TARGET_VARIABLES:
                env.pop(name, None)
                if inherited:
                    env[name] = str(temporary / "shared cache" / name)
            result = subprocess.run(
                ["bash", str(scripts / script)], cwd=temporary, env=env,
                text=True, capture_output=True, check=False, timeout=10,
            )
            self.assertEqual(result.returncode, 73, result.stdout + result.stderr)
            observed = capture.read_text(encoding="utf-8").splitlines()
            self.assertEqual(observed[0], str(checkout))
            self.assertEqual(observed[5:], ["fmt", "--all", "--", "--check"])
            normal_target = str(checkout / "target/verification/cargo")
            coverage_target = str(checkout / "target/verification/coverage")
            self.assertEqual(observed[1:5], [normal_target, normal_target,
                                            coverage_target, coverage_target])

    def test_sonar_targets_are_checkout_local(self):
        for inherited in (False, True):
            with self.subTest(inherited=inherited):
                self.check_script("sonar-reports.sh", inherited)

    def test_local_ci_targets_are_checkout_local(self):
        for inherited in (False, True):
            with self.subTest(inherited=inherited):
                self.check_script("local-ci.sh", inherited)


if __name__ == "__main__":
    unittest.main()
