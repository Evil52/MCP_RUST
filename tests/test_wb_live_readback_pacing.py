"""Exercise the live wrapper's request order without marketplace access."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class LiveReadbackPacing(unittest.TestCase):
    def run_wrapper(self, outcome, sleep_status=0, readback_status=0):
        script = Path(__file__).resolve().parents[1] / 'scripts/run-wb-automation-live.sh'
        with tempfile.TemporaryDirectory(prefix='wb-readback-') as temporary:
            root = Path(temporary)
            commands = root / 'bin'
            commands.mkdir()
            policy = dict(policy_version='wb_ads_robot.v1', account_id='fixture_wb',
                          campaign_id=123, allow_budget_top_up=False)
            for name, enabled in [('shadow.json', False), ('live.json', True)]:
                (root / name).write_text(json.dumps(dict(policy, write_enabled=enabled,
                                                        bid_writes_enabled=enabled)))
            for name, content in [('db.env', 'WB_AUTOMATION_DB_PASSWORD=' + 'x' * 24),
                                  ('read.token', 'fixture'), ('write.token', 'fixture'),
                                  ('legacy.json', '{}'), ('access.json', '{}'),
                                  ('compose.wb-automation-live.yaml', '{}')]:
                (root / name).write_text(content)
                (root / name).chmod(0o600)
            (commands / 'docker').write_text('''#!/bin/bash
set -eu
if [[ -f "$TEST_ROOT/first" ]]; then
  echo readback >>"$TEST_ROOT/events"
  printf '%s\\n' '{"outcome":"reconciled"}'
  exit "$TEST_READBACK_STATUS"
fi
touch "$TEST_ROOT/first"
echo cycle >>"$TEST_ROOT/events"
printf '{"outcome":"%s"}\\n' "$TEST_OUTCOME"
''')
            (commands / 'sleep').write_text('''#!/bin/bash
set -eu
printf 'wait:%s\\n' "$*" >>"$TEST_ROOT/events"
exit "$TEST_SLEEP_STATUS"
''')
            for command in commands.iterdir():
                command.chmod(0o700)
            environment = dict(os.environ, PATH=str(commands) + ':' + os.environ['PATH'],
                               TMPDIR=str(root), TEST_ROOT=str(root), TEST_OUTCOME=outcome,
                               TEST_SLEEP_STATUS=str(sleep_status),
                               TEST_READBACK_STATUS=str(readback_status),
                               WB_AUTOMATION_PROJECT_DIR=str(root),
                               WB_AUTOMATION_POSITION_ENV=str(root / 'db.env'),
                               WB_AUTOMATION_SHADOW_POLICY=str(root / 'shadow.json'),
                               WB_AUTOMATION_LIVE_POLICY=str(root / 'live.json'),
                               WB_AUTOMATION_ACCESS_CONFIG=str(root / 'access.json'),
                               WB_AUTOMATION_READ_TOKEN_FILE=str(root / 'read.token'),
                               WB_AUTOMATION_WRITE_TOKEN_FILE=str(root / 'write.token'),
                               WB_AUTOMATION_LEGACY_STATE=str(root / 'legacy.json'),
                               WB_AUTOMATION_BID_WRITES_ENABLED='true',
                               WB_AUTOMATION_EXPECTED_ACCOUNT_ID='fixture_wb',
                               WB_AUTOMATION_EXPECTED_CAMPAIGN_ID='123',
                               WB_AUTOMATION_RUNTIME_ID='fixture')
            result = subprocess.run(['bash', str(script)], env=environment,
                                    capture_output=True, timeout=5)
            return result, (root / 'events').read_text().splitlines()

    def test_write_waits_before_one_readback(self):
        result, events = self.run_wrapper('write_sent_reconciliation_required')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(events, ['cycle', 'wait:20', 'readback'])

    def test_observation_does_not_wait_or_start_another_cycle(self):
        result, events = self.run_wrapper('observed')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(events, ['cycle'])

    def test_interrupted_wait_does_not_send_readback(self):
        result, events = self.run_wrapper('write_sent_reconciliation_required', sleep_status=1)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(events, ['cycle', 'wait:20'])

    def test_readback_failure_is_preserved_without_retrying_write(self):
        result, events = self.run_wrapper('write_sent_reconciliation_required', readback_status=42)
        self.assertEqual(result.returncode, 42)
        self.assertEqual(events, ['cycle', 'wait:20', 'readback'])
