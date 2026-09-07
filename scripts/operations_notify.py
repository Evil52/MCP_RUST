#!/usr/bin/env python3
"""Deliver provider-neutral health events with bounded hooks and durable dedup."""

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import tempfile

MAX_REPORT_BYTES = 65_536


def install_signal_handlers():
    def interrupted(_signal, _frame):
        raise ValueError("hook delivery was interrupted")
    for selected in (signal.SIGTERM, signal.SIGINT):
        signal.signal(selected, interrupted)


def hook_identity(command):
    path = Path(command)
    if not path.is_absolute() or not path.is_file() or not os.access(path, os.X_OK):
        raise ValueError("hook must be one absolute executable file")
    fingerprint = hashlib.sha256(str(path).encode())
    with path.open("rb") as executable:
        for block in iter(lambda: executable.read(65_536), b""):
            fingerprint.update(block)
    return fingerprint.hexdigest()


def run_hook(command, body, event, timeout):
    """Exit zero acknowledges delivery; never print the hook's output or secrets."""
    path = Path(command)
    if not path.is_absolute() or not path.is_file() or not os.access(path, os.X_OK):
        raise ValueError("hook must be one absolute executable file")
    environment = dict(os.environ, MCP_HEALTH_EVENT=event)
    process = subprocess.Popen([str(path)], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, env=environment, start_new_session=True)
    try:
        process.communicate(body, timeout=timeout)
    except BaseException as error:
        # Kill the whole hook group, including a shell wrapper's HTTP client.
        # A notification timeout must not leave a late background delivery.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.communicate(timeout=1)
        if isinstance(error, subprocess.TimeoutExpired):
            raise ValueError("hook delivery timed out") from None
        raise
    if process.returncode:
        raise ValueError("hook delivery returned a failure")


def private_file(path, flags):
    descriptor = os.open(path, flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    info = os.fstat(descriptor)
    if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_uid != os.getuid() or info.st_nlink != 1):
        os.close(descriptor)
        raise ValueError("notification state must be a private regular file")
    return descriptor


def deliver(command, state_dir, findings, report, timeout):
    directory = Path(state_dir)
    if directory.is_symlink():
        raise ValueError("notification state directory cannot be a symlink")
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    info = directory.stat()
    if stat.S_IMODE(info.st_mode) != 0o700 or info.st_uid != os.getuid():
        raise ValueError("notification state directory must be private")
    keys = sorted(set(findings))
    destination = hook_identity(command)
    fingerprint = hashlib.sha256(json.dumps(keys, separators=(",", ":")).encode()).hexdigest()
    with os.fdopen(private_file(directory / "delivery.lock", os.O_CREAT | os.O_RDWR), "r+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("notification delivery is already in progress") from None
        state_path = directory / "delivered.json"
        previous = None
        if state_path.exists() or state_path.is_symlink():
            with os.fdopen(private_file(state_path, os.O_RDONLY)) as state:
                previous = json.loads(state.read(4097))
            if (not isinstance(previous, dict) or previous.get("version") != 1
                    or not isinstance(previous.get("fingerprint"), str)
                    or not re.fullmatch("[0-9a-f]{64}", previous["fingerprint"])
                    or type(previous.get("finding_count")) is not int
                    or not 0 <= previous["finding_count"] <= 1000):
                raise ValueError("notification state has an invalid contract")
            if previous.get("hook_identity") != destination:
                # A replacement receiver must see ongoing incidents. An old
                # receiver's alert is not evidence of delivery to this one.
                previous = None
        if previous and previous["fingerprint"] == fingerprint:
            return "unchanged"
        if keys:
            event = "alert"
        elif previous and previous["finding_count"]:
            event = "recovery"
        else:
            return "initial_clean"
        body = ("MCP_OZON " + event + "\n\n").encode() + report
        run_hook(command, body, event, timeout)
        # A failed delivery never advances dedup state. A crash between the
        # external acknowledgement and this replace can repeat an event.
        with tempfile.NamedTemporaryFile(mode="w", dir=directory, prefix=".delivered-", delete=False) as output:
            temporary = Path(output.name)
            json.dump({"version": 1, "fingerprint": fingerprint, "finding_count": len(keys),
                       "hook_identity": destination}, output)
            output.flush()
            os.fsync(output.fileno())
        try:
            temporary.chmod(0o600)
            temporary.replace(state_path)
        finally:
            temporary.unlink(missing_ok=True)
        return event


def timeout_seconds(value):
    seconds = int(value)
    if not 1 <= seconds <= 30:
        raise argparse.ArgumentTypeError("hook timeout must be from 1 to 30 seconds")
    return seconds


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hook", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--timeout", type=timeout_seconds, default=10)
    parser.add_argument("--finding", action="append", default=[])
    arguments = parser.parse_args()
    report = sys.stdin.buffer.read(MAX_REPORT_BYTES + 1)
    if len(report) > MAX_REPORT_BYTES or len(arguments.finding) > 1000:
        raise ValueError("notification report exceeds its fixed bound")
    deliver(arguments.hook, arguments.state_dir, arguments.finding, report, arguments.timeout)


if __name__ == "__main__":
    install_signal_handlers()
    try:
        main()
    except (OSError, ValueError, subprocess.SubprocessError):
        print("health notification delivery failed; inspect the configured hook privately", file=sys.stderr)
        sys.exit(1)
