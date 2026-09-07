"""Offline operations delivery, tunnel evidence, and core-heartbeat regressions."""

import fcntl
import http.server
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import operations_notify as notify
import operations_heartbeat as heartbeat


def hook(path, source):
    path.write_text("#!" + sys.executable + "\n" + source)
    path.chmod(0o700)
    return str(path)


class DeliveryTests(unittest.TestCase):
    def test_dedup_changed_findings_and_recovery_follow_successful_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = root / "events"
            command = hook(root / "hook", "import os,sys\nfrom pathlib import Path\n"
                           f"with Path({str(events)!r}).open('a') as output:\n"
                           " output.write(os.environ['MCP_HEALTH_EVENT']+'\\n')\n"
                           "sys.stdin.read()\n")
            state = root / "state"
            self.assertEqual(notify.deliver(command, state, [], b"clean", 1), "initial_clean")
            self.assertFalse(events.exists())
            self.assertEqual(notify.deliver(command, state, ["a"], b"age1", 1), "alert")
            self.assertEqual(notify.deliver(command, state, ["a", "a"], b"age2", 1), "unchanged")
            self.assertEqual(notify.deliver(command, state, ["a", "b"], b"changed", 1), "alert")
            self.assertEqual(notify.deliver(command, state, [], b"clean", 1), "recovery")
            self.assertEqual(notify.deliver(command, state, [], b"clean again", 1), "unchanged")
            self.assertEqual(events.read_text().splitlines(), ["alert", "alert", "recovery"])
            self.assertEqual((state / "delivered.json").stat().st_mode & 0o777, 0o600)

    def test_failed_hook_is_visible_redacted_and_does_not_advance_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            command = hook(root / "hook", "import sys\nprint('PRIVATE-HOOK-SECRET',file=sys.stderr)\nsys.exit(7)\n")
            args = [sys.executable, str(ROOT / "scripts/operations_notify.py"),
                    "--hook", command, "--state-dir", str(root / "state"), "--finding", "a"]
            failed = subprocess.run(args, input="alert", text=True, capture_output=True, check=False)
            self.assertEqual(failed.returncode, 1)
            self.assertIn("notification delivery failed", failed.stderr)
            self.assertNotIn("PRIVATE-HOOK-SECRET", failed.stdout + failed.stderr)
            self.assertFalse((root / "state/delivered.json").exists())
            hook(root / "hook", "import sys\nsys.stdin.read()\n")
            retried = subprocess.run(args, input="alert", text=True, capture_output=True, check=False)
            self.assertEqual(retried.returncode, 0, retried.stderr)
            self.assertTrue((root / "state/delivered.json").exists())

    def test_replacing_hook_path_or_contents_resends_an_ongoing_alert(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = hook(root / "first", "import sys\nsys.stdin.read()\n")
            second = hook(root / "second", "import sys\nsys.stdin.read()\n")
            state = root / "state"
            self.assertEqual(notify.deliver(first, state, ["incident"], b"body", 1), "alert")
            self.assertEqual(notify.deliver(second, state, ["incident"], b"body", 1), "alert")
            self.assertEqual(notify.deliver(second, state, ["incident"], b"body", 1), "unchanged")
            hook(root / "second", "import sys\nsys.stdin.read()\n# new receiver\n")
            self.assertEqual(notify.deliver(second, state, ["incident"], b"body", 1), "alert")

    def test_timeout_terminates_hook_descendants_before_late_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = root / "late-delivery"
            child = "import time;from pathlib import Path;time.sleep(.4);Path(" + repr(str(marker)) + ").touch()"
            command = hook(root / "hook", "import subprocess,sys,time\n"
                           f"subprocess.Popen([sys.executable,'-c',{child!r}])\n"
                           "time.sleep(10)\n")
            started = time.monotonic()
            with self.assertRaisesRegex(ValueError, "timed out"):
                notify.run_hook(command, b"event", "alert", 0.1)
            self.assertLess(time.monotonic() - started, 1)
            time.sleep(0.5)
            self.assertFalse(marker.exists())

    def test_term_and_int_terminate_hook_descendants_without_delivery_state(self):
        for selected in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signal=selected), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                ready, marker = root / "ready", root / "late-delivery"
                child = "import time;from pathlib import Path;time.sleep(.5);Path(" + repr(str(marker)) + ").touch()"
                command = hook(root / "hook", "import subprocess,sys,time\nfrom pathlib import Path\n"
                               f"subprocess.Popen([sys.executable,'-c',{child!r}])\n"
                               f"Path({str(ready)!r}).touch()\n"
                               "time.sleep(10)\n")
                process = subprocess.Popen([sys.executable, str(ROOT / "scripts/operations_notify.py"),
                                            "--hook", command, "--state-dir", str(root / "state"),
                                            "--finding", "incident"], stdin=subprocess.DEVNULL,
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    deadline = time.monotonic() + 3
                    while not ready.exists() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertTrue(ready.exists(), "fixture hook must have started")
                    process.send_signal(selected)
                    process.communicate(timeout=3)
                    self.assertNotEqual(process.returncode, 0)
                    self.assertFalse((root / "state/delivered.json").exists())
                    time.sleep(0.6)
                    self.assertFalse(marker.exists())
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.communicate(timeout=3)

    def test_locked_or_symlinked_state_fails_without_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state"
            state.mkdir(mode=0o700)
            lock = state / "delivery.lock"
            lock.touch(mode=0o600)
            command = hook(root / "hook", "raise RuntimeError('must not run')\n")
            with lock.open("r+") as held:
                fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaisesRegex(ValueError, "in progress"):
                    notify.deliver(command, state, ["a"], b"body", 1)
            target = root / "sensitive"
            target.write_text("not state")
            (state / "delivered.json").symlink_to(target)
            with self.assertRaises(OSError):
                notify.deliver(command, state, ["a"], b"body", 1)
            self.assertEqual(target.read_text(), "not state")
            (state / "delivered.json").unlink()
            target.chmod(0o600)
            os.link(target, state / "delivered.json")
            with self.assertRaisesRegex(ValueError, "private regular file"):
                notify.deliver(command, state, ["a"], b"body", 1)
            (state / "delivered.json").unlink()
            os.mkfifo(state / "delivered.json", mode=0o600)
            with self.assertRaisesRegex(ValueError, "private regular file"):
                notify.deliver(command, state, ["a"], b"body", 1)


class TunnelTests(unittest.TestCase):
    def test_launch_agent_result_never_invents_success_and_rejects_ambiguous_status(self):
        for output in ("state = not running\nruns = 0\n", "last exit code = (never exited)\n",
                       "last exit code = never exited\n"):
            self.assertIsNone(heartbeat.parse_launch_agent_result(output))
        for code in (0, 1, 78, -15):
            self.assertEqual(heartbeat.parse_launch_agent_result(
                "gui/501/example = {\n\tstate = not running\n\tlast exit code = " + str(code) + "\n}"), code)
        for output in ("last exit code = PRIVATE\n", "last exit code = 0\nlast exit code = 7\n"):
            with self.assertRaises(ValueError) as caught:
                heartbeat.parse_launch_agent_result(output)
            self.assertNotIn("PRIVATE", str(caught.exception))

    def test_poll_metric_requires_exactly_one_finite_recent_nonfuture_sample(self):
        metric = heartbeat.POLL_METRIC
        self.assertEqual(heartbeat.poll_age(metric + '{runtime="test"} 990\n', 1000, 90), 10)
        self.assertEqual(heartbeat.poll_age(metric + ' 9.1e2\n', 1000, 90), 90)
        for text in ("", metric + ' 909\n', metric + ' 1001\n', metric + ' NaN\n',
                     metric + ' 0\n', metric + ' +Inf\n', metric + ' PRIVATE\n',
                     metric + ' 990\n' + metric + ' 991\n'):
            with self.subTest(text=text), self.assertRaises(ValueError) as caught:
                heartbeat.poll_age(text, 1000, 90)
            self.assertNotIn("PRIVATE", str(caught.exception))

    def test_tunnel_validates_loopback_and_all_three_independent_endpoints(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "health.url"
            path.write_text("http://127.0.0.1:12345\n")
            calls = []
            def fetch(url):
                calls.append(url.rsplit("/", 1)[-1])
                return heartbeat.POLL_METRIC + " 990\n" if url.endswith("/metrics") else "ready"
            self.assertEqual(heartbeat.probe(path, 90, fetch=fetch, now=1000), 10)
            self.assertEqual(calls, ["healthz", "readyz", "metrics"])
            for value in ("https://remote.invalid", "http://127.0.0.1:0", "http://127.0.0.1:1234/private"):
                path.write_text(value)
                with self.assertRaises(ValueError):
                    heartbeat.probe(path, 90, fetch=fetch, now=1000)
            self.assertEqual(len(calls), 3)

    def test_local_probe_cannot_follow_redirects_or_read_unbounded_bodies(self):
        for stdout, code in ((b"ready\n200", 0), (b"PRIVATE\n302", 0), (b"x" * 1_048_582, 0), (b"", 7)):
            with patch.object(heartbeat.subprocess, "run", return_value=subprocess.CompletedProcess([], code, stdout, b"")) as call:
                if stdout == b"ready\n200":
                    self.assertEqual(heartbeat.fetch_local("http://127.0.0.1:1234/readyz"), "ready")
                else:
                    with self.assertRaises(ValueError):
                        heartbeat.fetch_local("http://127.0.0.1:1234/readyz")
                self.assertIn("--noproxy", call.call_args.args[0])
                self.assertIn("--max-filesize", call.call_args.args[0])
                self.assertNotIn("--location", call.call_args.args[0])
                self.assertEqual(call.call_args.kwargs["timeout"], 4)


class HealthIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        backup = self.root / "backups/20260907T000000Z"
        backup.mkdir(parents=True)
        for name in ("offsite-complete.json", "restore-verified.json"):
            (backup / name).touch()
        for name in ("position.env", "ready"):
            (self.root / name).touch()
        docker = self.root / "docker"
        docker.write_text('''#!/bin/bash
set -euo pipefail
case "$1" in
 info) exit "${FAKE_DOCKER_DOWN:-0}" ;;
 ps) printf '%s\\n' "${FAKE_COMPOSE_STATE:-running|Up (healthy)}" ;;
 container) printf '%s\\n' "${FAKE_MAIN_STATE:-running|healthy}" ;;
 run) cat >/dev/null; printf 'cycle_age|0\\n'; printf '%s\\n' "${FAKE_ROWS:-}"; exit "${FAKE_DB_STATUS:-0}" ;;
 *) exit 2 ;;
esac
''')
        docker.chmod(0o700)
        self.events = self.root / "events"
        self.command = hook(self.root / "hook", "import os,sys\nfrom pathlib import Path\n"
                            f"with Path({str(self.events)!r}).open('a') as output:\n"
                            " output.write(os.environ['MCP_HEALTH_EVENT']+'\\n')\n"
                            "sys.stdin.read()\n")
        self.env = {key: value for key, value in os.environ.items() if not key.startswith("MCP_")}
        self.env.update(DOCKER_BIN=str(docker), MCP_HEALTH_POSITION_ENV=str(self.root / "position.env"),
                        MCP_BACKUP_DIR=str(self.root / "backups"), MCP_HEALTH_SKIP_LAUNCH_AGENT_CHECK="true",
                        MCP_HEALTH_MCP_READY_URL=(self.root / "ready").as_uri(),
                        MCP_HEALTH_EVENT_STATE_DIR=str(self.root / "state"),
                        MCP_HEALTH_NOTIFY_COMMAND=self.command, MCP_HEALTH_HEARTBEAT_COMMAND=self.command)

    def tearDown(self):
        self.temporary.cleanup()

    def run_health(self, **values):
        result = subprocess.run([str(ROOT / "scripts/check-runtime-health.sh")],
                                env=dict(self.env, **values), text=True, capture_output=True, timeout=20, check=False)
        return result.returncode, result.stdout + result.stderr

    def event_list(self):
        return self.events.read_text().splitlines() if self.events.exists() else []

    def test_guard_lock_notifies_once_but_core_heartbeat_continues_and_clean_recovers(self):
        for _ in range(2):
            status, output = self.run_health(FAKE_ROWS="incident|test|1|locked")
            self.assertEqual(status, 1, output)
        self.assertEqual(self.event_list(), ["heartbeat", "alert", "heartbeat"])
        status, output = self.run_health()
        self.assertEqual(status, 0, output)
        self.assertEqual(self.event_list()[-2:], ["heartbeat", "recovery"])
        for values in ({"FAKE_DB_STATUS": "9"}, {"FAKE_MAIN_STATE": "exited|"},
                       {"FAKE_COMPOSE_STATE": "running|Up (health: starting)"},
                       {"FAKE_DOCKER_DOWN": "1"}, {"MCP_HEALTH_CHECK_TUNNEL": "true",
                       "MCP_HEALTH_TUNNEL_URL_FILE": str(self.root / "missing-url")}):
            before = self.event_list().count("heartbeat")
            status, output = self.run_health(**values)
            self.assertEqual(status, 1, output)
            self.assertEqual(self.event_list().count("heartbeat"), before)

    def test_delivery_failures_are_findings_and_notify_failure_does_not_falsify_core(self):
        failing = hook(self.root / "fail", "import sys\nprint('PRIVATE-HOOK-SECRET')\nsys.exit(7)\n")
        status, output = self.run_health(MCP_HEALTH_NOTIFY_COMMAND=failing, FAKE_ROWS="incident|test|1|locked")
        self.assertEqual(status, 1)
        self.assertIn("health notification delivery failed", output)
        self.assertNotIn("PRIVATE-HOOK-SECRET", output)
        self.assertEqual(self.event_list(), ["heartbeat"])
        status, output = self.run_health(MCP_HEALTH_HEARTBEAT_COMMAND=failing)
        self.assertEqual(status, 1)
        self.assertIn("external core heartbeat delivery failed", output)
        self.assertEqual(self.event_list()[-1], "alert")

    def test_real_loopback_tunnel_probe_controls_heartbeat_and_recovers(self):
        responses = {"/healthz": (200, "live"), "/readyz": (200, "ready"),
                     "/metrics": (200, heartbeat.POLL_METRIC + " " + str(time.time() - 3))}
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                status, text = responses.get(self.path, (404, ""))
                body = text.encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url_file = self.root / "tunnel.url"
        url_file.write_text("http://127.0.0.1:" + str(server.server_port))
        values = {"MCP_HEALTH_CHECK_TUNNEL": "true", "MCP_HEALTH_TUNNEL_URL_FILE": str(url_file)}
        try:
            status, output = self.run_health(**values)
            self.assertEqual(status, 0, output)
            self.assertEqual(self.event_list(), ["heartbeat"])
            responses["/metrics"] = (200, heartbeat.POLL_METRIC + " " + str(time.time() - 120))
            status, output = self.run_health(**values)
            self.assertEqual(status, 1, output)
            self.assertIn("tunnel control-plane poll is stale", output)
            self.assertEqual(self.event_list(), ["heartbeat", "alert"])
            responses["/metrics"] = (200, heartbeat.POLL_METRIC + " " + str(time.time() - 3))
            responses["/readyz"] = (503, "unready")
            status, output = self.run_health(**values)
            self.assertEqual(status, 1, output)
            self.assertEqual(self.event_list(), ["heartbeat", "alert"])
            responses["/readyz"] = (200, "ready")
            status, output = self.run_health(**values)
            self.assertEqual(status, 0, output)
            self.assertEqual(self.event_list()[-2:], ["heartbeat", "recovery"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
