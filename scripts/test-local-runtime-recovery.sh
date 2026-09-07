#!/usr/bin/env bash
# Exercise the actual watchdog with isolated fake clients, never real Docker,
# marketplace credentials, a tunnel profile, LaunchAgents or production data.
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python3 - "$project_root/scripts/ensure-local-runtime.sh" <<'PY'
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

WATCHDOG = str(Path(sys.argv[1]).resolve())
FAKE = r'''
import json, os, pathlib, sys, time
root = pathlib.Path(os.environ['RECOVERY_TEST_CASE'])
state_path = root / 'state.json'
state = json.loads(state_path.read_text())
args = sys.argv[1:]
kind = pathlib.Path(sys.argv[0]).name
with (root / 'calls.jsonl').open('a') as stream:
    stream.write(json.dumps([kind, args]) + '\n')
def save():
    state_path.write_text(json.dumps(state))
if kind == 'docker':
    if args == ['info']:
        if state.get('block_info'):
            (root / 'blocked').write_text(str(os.getpid()))
            deadline = time.monotonic() + 30
            while not (root / 'release').exists() and time.monotonic() < deadline:
                time.sleep(0.02)
        sys.exit(0)
    if args[:2] == ['container', 'inspect']:
        assert args[-1] == 'test-recovery-server'
        if '--format' not in args:
            print('{}')
        elif '.Mounts' in args[args.index('--format') + 1]:
            print(root / 'runtime/access.json')
        elif '.State.Running' in args[args.index('--format') + 1]:
            print(str(state['running']).lower())
        else:
            raise AssertionError('unexpected inspect')
    elif args in [['start', 'test-recovery-server'], ['restart', 'test-recovery-server']]:
        state.update(running=True, healthy=True)
        save()
    else:
        raise AssertionError('unexpected Docker command')
elif kind == 'curl':
    assert '--connect-timeout' in args and '--max-time' in args
    assert 0 < float(args[args.index('--connect-timeout') + 1]) <= 3
    deadlines = [float(args[i + 1]) for i, arg in enumerate(args) if arg == '--max-time']
    assert all(0 < deadline <= 6 for deadline in deadlines)
    url = args[-1]
    if url == 'http://127.0.0.1:18787/readyz':
        sys.exit(0 if state['healthy'] and state['running'] else 28)
    if url == 'https://api.openai.com/v1/models':
        assert '--noproxy' in args and args[args.index('--noproxy') + 1] == '*'
        print(state.get('control_plane', '401'))
        sys.exit(0)
    assert url.startswith('http://127.0.0.1:18989/')
    mode = state['tunnel']
    if url.endswith('/healthz') or url.endswith('/readyz'):
        sys.exit(0 if mode != 'dead' else 7)
    assert url.endswith('/metrics')
    now = int(time.time())
    poll = now if mode == 'fresh' else 0
    started = now if mode == 'starting' else now - 600
    print('commands_poll_last_successful_timestamp_seconds ' + str(poll))
    print('process_start_time_seconds ' + str(started))
elif kind == 'tunnel-client':
    assert args[0] in ['doctor', 'runtimes']
    assert 'test-recovery-tunnel' in args
    if args[0] == 'doctor' and state.get('block_doctor'):
        (root / 'doctor-blocked').write_text(str(os.getpid()))
        time.sleep(60)
        (root / 'doctor-finished').write_text('unexpected')
    if args[:2] == ['runtimes', 'connect']:
        assert args[args.index('--profile-dir') + 1] == str(root / 'profile')
        assert args[args.index('--runtime-api-key') + 1] == 'file:' + str(root / 'profile/runtime-api-key')
        assert args[args.index('--mcp-server-url') + 1] == 'http://127.0.0.1:18787/mcp'
        if state.get('spawn_daemon'):
            if os.fork() == 0:
                os.setsid()
                # A daemon may intentionally close stdio but retain every
                # other inherited FD. The watchdog must provide the boundary.
                null = os.open(os.devnull, os.O_RDWR)
                for descriptor in (0, 1, 2):
                    os.dup2(null, descriptor)
                if null > 2:
                    os.close(null)
                (root / 'daemon-ready').write_text(str(os.getpid()))
                deadline = time.monotonic() + 15
                while not (root / 'daemon-release').exists() and time.monotonic() < deadline:
                    time.sleep(0.02)
                (root / 'daemon-done').write_text('done')
                os._exit(0)
        state['tunnel'] = 'fresh'
        save()
else:
    raise AssertionError('unknown fake client')
'''


def check(condition, message):
    if not condition:
        raise AssertionError(message)


with tempfile.TemporaryDirectory(prefix='mcp-runtime-recovery-test-') as temporary:
    root = Path(temporary)
    root.chmod(0o700)
    binaries = root / 'bin'
    binaries.mkdir()
    for name in ['docker', 'curl', 'tunnel-client']:
        path = binaries / name
        path.write_text('#!' + sys.executable + '\n' + FAKE)
        path.chmod(0o700)
    sequence = 0
    passed = []

    def new_case(**overrides):
        global sequence
        sequence += 1
        case = root / str(sequence)
        case.mkdir(mode=0o700)
        runtime = case / 'runtime'
        runtime.mkdir(mode=0o700)
        (runtime / 'access.json').write_text('{}')
        (runtime / 'access.json').chmod(0o644)
        profile = case / 'profile'
        profile.mkdir(mode=0o700)
        (profile / 'test-recovery-tunnel.yaml').write_text('{"tunnel_id":"tunnel_' + '0' * 32 + '"}')
        (profile / 'runtime-api-key').write_text('synthetic-test-key-never-used')
        (profile / 'runtime-api-key').chmod(0o600)
        (case / 'health.url').write_text('http://127.0.0.1:18989')
        (case / 'state.json').write_text(json.dumps(dict(running=True, healthy=True, tunnel='fresh', **overrides)))
        environment = {
            'PATH': str(Path(sys.executable).parent) + ':/usr/bin:/bin',
            'HOME': os.environ['HOME'],
            'TMPDIR': str(case),
            'MCP_RUNTIME_DIR': str(runtime),
            'MCP_CONTAINER_NAME': 'test-recovery-server',
            'MCP_HEALTH_URL': 'http://127.0.0.1:18787/readyz',
            'MCP_SERVER_URL': 'http://127.0.0.1:18787/mcp',
            'MCP_RUNTIME_CURL_BIN': str(binaries / 'curl'),
            'DOCKER_BIN': str(binaries / 'docker'),
            'TUNNEL_CLIENT_BIN': str(binaries / 'tunnel-client'),
            'TUNNEL_CLIENT_PROFILE': 'test-recovery-tunnel',
            'TUNNEL_CLIENT_PROFILE_DIR': str(profile),
            'TUNNEL_CLIENT_HEALTH_URL_FILE': str(case / 'health.url'),
            'RECOVERY_TEST_CASE': str(case),
            'SECRET_TEST_SENTINEL': 'must-never-appear-in-output',
        }
        return case, environment

    def invoke(environment, expected=0):
        result = subprocess.run(['/bin/bash', WATCHDOG], env=environment, text=True,
                                capture_output=True, timeout=15, check=False)
        check(result.returncode == expected, 'watchdog exit code differs: ' + result.stderr)
        check(environment['SECRET_TEST_SENTINEL'] not in result.stdout + result.stderr,
              'environment data leaked')
        return result

    def calls(case):
        path = case / 'calls.jsonl'
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def changed(case, **updates):
        path = case / 'state.json'
        state = json.loads(path.read_text())
        state.update(updates)
        path.write_text(json.dumps(state))

    case, environment = new_case()
    legacy = case / 'mcp-ozon-runtime-agent.lock'
    legacy.mkdir()
    invoke(environment)
    check(not any(kind == 'tunnel-client' or args[0] in ['start', 'restart'] for kind, args in calls(case)),
          'healthy runtime was restarted')
    check(legacy.exists(), 'legacy directory should not be removed or used')
    check((case / 'runtime/watchdog.lock').stat().st_mode & 0o777 == 0o600, 'kernel lock is not private')
    passed.append('healthy runtime and fresh tunnel require no restart; stale legacy directory is harmless')

    for running, action in [(False, 'start'), (True, 'restart')]:
        case, environment = new_case()
        changed(case, running=running, healthy=False)
        invoke(environment)
        check(sum(kind == 'docker' and args == [action, 'test-recovery-server'] for kind, args in calls(case)) == 1,
              'MCP recovery did not perform exactly the expected action')
    passed.append('stopped MCP starts; unhealthy running MCP restarts; every HTTP call has both timeout bounds')

    case, environment = new_case()
    changed(case, tunnel='stale')
    invoke(environment)
    operations = [args for kind, args in calls(case) if kind == 'tunnel-client']
    check([args[:2] for args in operations] == [['runtimes', 'stop'], ['doctor', '--profile'], ['runtimes', 'connect']],
          'stale tunnel supervision sequence differs')
    restart_state = case / 'runtime/tunnel-restart.state'
    check(restart_state.stat().st_mode & 0o777 == 0o600, 'tunnel cooldown state is not private')
    changed(case, tunnel='stale')
    invoke(environment, expected=1)
    check(len([1 for kind, args in calls(case) if kind == 'tunnel-client']) == 3,
          'cooldown allowed a second tunnel restart')
    passed.append('stale tunnel reconnects through the fake client and cooldown suppresses a second restart')

    for mode, control, expected in [('starting', '401', 0), ('stale', '403', 1), ('dead', '000', 1)]:
        case, environment = new_case()
        changed(case, tunnel=mode, control_plane=control)
        invoke(environment, expected=expected)
        check(not any(kind == 'tunnel-client' for kind, _ in calls(case)), 'unsafe tunnel restart')
        check(not (case / 'runtime/tunnel-restart.state').exists(), 'suppressed restart recorded a cooldown')
    passed.append('startup grace, forbidden control plane and unreachable control plane suppress restart')

    case, environment = new_case(spawn_daemon=True)
    changed(case, tunnel='stale')
    try:
        invoke(environment)
        deadline = time.monotonic() + 5
        while not (case / 'daemon-ready').exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        check((case / 'daemon-ready').exists() and not (case / 'daemon-done').exists(),
              'fake tunnel daemon did not remain alive after its CLI exited')
        count = len(calls(case))
        invoke(environment)
        check(len(calls(case)) > count and not (case / 'daemon-done').exists(),
              'tunnel daemon inherited the watchdog lock and blocked the next tick')
    finally:
        (case / 'daemon-release').touch()
        deadline = time.monotonic() + 5
        while not (case / 'daemon-done').exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        check((case / 'daemon-done').exists(), 'fake tunnel daemon did not clean up')
    passed.append('orphan tunnel daemon stays alive without retaining the watchdog lock; next tick acquires')

    case, environment = new_case(block_doctor=True)
    changed(case, tunnel='stale')
    first = subprocess.Popen(['/bin/bash', WATCHDOG], env=environment, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, start_new_session=True)
    try:
        deadline = time.monotonic() + 5
        while not (case / 'doctor-blocked').exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        check((case / 'doctor-blocked').exists(), 'supervised fake doctor did not start')
        count = len(calls(case))
        first.kill()
        first.wait(timeout=5)
        invoke(environment)
        check(len(calls(case)) == count, 'owner SIGKILL released an active supervised tunnel operation')
        lock_path = case / 'runtime/watchdog.lock'
        deadline = time.monotonic() + 35
        while time.monotonic() < deadline:
            with lock_path.open('r+') as stream:
                try:
                    fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    time.sleep(0.05)
        else:
            raise AssertionError('tunnel supervisor did not release the lock at its command deadline')
        check(not (case / 'doctor-finished').exists(), 'blocked doctor escaped its deadline')
        changed(case, block_doctor=False, tunnel='fresh')
        invoke(environment)
        check(len(calls(case)) > count, 'next tick did not recover after the supervised CLI timed out')
    finally:
        try:
            os.killpg(first.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        first.wait(timeout=5)
    passed.append('owner SIGKILL leaves active tunnel supervisor holding lock until its timeout, then recovery succeeds')

    case, environment = new_case(block_info=True)
    first = subprocess.Popen(['/bin/bash', WATCHDOG], env=environment, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, start_new_session=True)
    try:
        deadline = time.monotonic() + 5
        while not (case / 'blocked').exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        check((case / 'blocked').exists(), 'first watchdog did not reach the isolated blocked client')
        lock_path = case / 'runtime/watchdog.lock'
        original = (lock_path.stat().st_ino, lock_path.read_bytes())
        call_count = len(calls(case))
        invoke(environment)
        check(len(calls(case)) == call_count, 'concurrent watchdog entered its clients')
        check((lock_path.stat().st_ino, lock_path.read_bytes()) == original, 'contender changed active lock ownership')
        # Killing the owner alone must not release an operation still running
        # in its child; no contender may unlink another holder's inode.
        first.kill()
        first.wait(timeout=5)
        invoke(environment)
        check(len(calls(case)) == call_count, 'live child lost inherited lock after owner SIGKILL')
        os.killpg(first.pid, signal.SIGKILL)
        changed(case, block_info=False)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with lock_path.open('r+') as stream:
                try:
                    fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    time.sleep(0.02)
        else:
            raise AssertionError('kernel did not release the killed watchdog lock')
        invoke(environment)
        check(len(calls(case)) > call_count, 'watchdog did not recover after SIGKILL')
        check(lock_path.stat().st_ino == original[0], 'lock inode was replaced during recovery')
    finally:
        try:
            os.killpg(first.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        first.wait(timeout=5)
    passed.append('active owner survives contention; SIGKILL recovery preserves the lock inode and live child ownership')

    case, environment = new_case()
    lock_path = case / 'runtime/watchdog.lock'
    lock_path.write_text(json.dumps({'pid': os.getpid(), 'script': 'unrelated-live-process'}))
    lock_path.chmod(0o600)
    invoke(environment)
    check(calls(case), 'PID reuse metadata prevented kernel-lock acquisition')
    passed.append('an unrelated live PID in stale metadata cannot cause permanent lockout')

    case, environment = new_case()
    target = case / 'sentinel'
    target.write_text('unchanged')
    (case / 'runtime/watchdog.lock').symlink_to(target)
    invoke(environment, expected=1)
    check(target.read_text() == 'unchanged' and not calls(case), 'unsafe lock path was followed')
    case, environment = new_case()
    environment['MCP_RUNTIME_CURL_BIN'] = 'curl'
    invoke(environment, expected=1)
    check(not calls(case), 'relative curl injection reached a client')
    passed.append('symlink lock and relative curl override fail closed before any client call')

    for message in passed:
        print('PASS: ' + message)
    print(str(len(passed)) + ' recovery scenario groups passed; production artifacts and clients were never used.')
PY
