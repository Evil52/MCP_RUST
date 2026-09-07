#!/usr/bin/env python3
"""Probe the local tunnel or deliver a provider-neutral core-availability heartbeat."""

import argparse
import datetime as dt
import json
import math
from pathlib import Path
import re
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from operations_notify import install_signal_handlers, run_hook, timeout_seconds

POLL_METRIC = "commands_poll_last_successful_timestamp_seconds"


def parse_launch_agent_result(output):
    values = re.findall(r"^\s*last exit code\s*=\s*(.*?)\s*$", output, re.M)
    if not values or values == ["(never exited)"] or values == ["never exited"]:
        return None
    if len(values) != 1 or not re.fullmatch(r"-?[0-9]+", values[0]):
        raise ValueError("scheduled backup exit evidence is invalid")
    return int(values[0])


def poll_age(metrics, now, stale_seconds):
    samples = [line for line in metrics.splitlines()
               if re.match(r"^" + POLL_METRIC + r"(?:\{|\s)", line)]
    if len(samples) != 1:
        raise ValueError("tunnel poll evidence is missing or ambiguous")
    match = re.fullmatch(POLL_METRIC + r'(?:\{[^}\n]*\})?\s+(\S+)(?:\s+[0-9]+)?\s*', samples[0])
    if match is None:
        raise ValueError("tunnel poll evidence is invalid")
    try:
        timestamp = float(match[1])
    except ValueError:
        raise ValueError("tunnel poll timestamp is invalid") from None
    if not math.isfinite(timestamp) or timestamp <= 0 or timestamp > now:
        raise ValueError("tunnel poll timestamp is invalid")
    age = now - timestamp
    if age > stale_seconds:
        raise ValueError("tunnel control-plane poll is stale")
    return age


def fetch_local(url):
    result = subprocess.run([
        "/usr/bin/curl", "--noproxy", "*", "--proto", "=http", "--connect-timeout", "2",
        "--max-time", "3", "--max-filesize", "1048576", "--fail", "--silent",
        "--write-out", "\n%{http_code}", url,
    ], capture_output=True, timeout=4, check=False)
    if result.returncode or len(result.stdout) > 1_048_581:
        raise ValueError("tunnel local endpoint is unavailable")
    body, _, status = result.stdout.rpartition(b"\n")
    if status != b"200":
        raise ValueError("tunnel local endpoint is unavailable")
    try:
        return body.decode("utf-8")
    except UnicodeError:
        raise ValueError("tunnel local endpoint returned invalid text") from None


def probe(url_file, stale_seconds, fetch=fetch_local, now=None):
    path = Path(url_file)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 256:
        raise ValueError("tunnel health URL file is unavailable")
    try:
        base = path.read_text().strip()
    except UnicodeError:
        raise ValueError("tunnel health URL file is invalid") from None
    match = re.fullmatch(r"http://127\.0\.0\.1:([0-9]{1,5})", base)
    if match is None or not 1 <= int(match[1]) <= 65535:
        raise ValueError("tunnel health URL must be a loopback endpoint")
    fetch(base + "/healthz")
    fetch(base + "/readyz")
    metrics = fetch(base + "/metrics")
    return poll_age(metrics, time.time() if now is None else now, stale_seconds)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    tunnel = modes.add_parser("probe-tunnel")
    tunnel.add_argument("--url-file", required=True)
    tunnel.add_argument("--stale-seconds", type=int, default=90)
    heartbeat = modes.add_parser("send")
    heartbeat.add_argument("--hook", required=True)
    heartbeat.add_argument("--timeout", type=timeout_seconds, default=10)
    modes.add_parser("agent-result")
    arguments = parser.parse_args()
    if arguments.mode == "probe-tunnel":
        if not 1 <= arguments.stale_seconds <= 600:
            raise ValueError("tunnel stale threshold must be from 1 to 600 seconds")
        probe(arguments.url_file, arguments.stale_seconds)
    elif arguments.mode == "send":
        body = json.dumps({"version": 1, "event": "heartbeat", "core_available": True,
                           "observed_at": dt.datetime.now(dt.timezone.utc).isoformat()}).encode()
        run_hook(arguments.hook, body, "heartbeat", arguments.timeout)
    else:
        output = sys.stdin.read(262_145)
        if len(output) > 262_144:
            raise ValueError("scheduled backup exit evidence exceeds its bound")
        status = parse_launch_agent_result(output)
        print("unknown" if status is None else "exit:" + str(status))


if __name__ == "__main__":
    install_signal_handlers()
    try:
        main()
    except ValueError as error:
        # ValueError messages are fixed classifications, never metrics, a URL,
        # hook output, headers, credentials, or upstream response bodies.
        print(str(error), file=sys.stderr)
        sys.exit(1)
    except (OSError, subprocess.SubprocessError):
        print("tunnel probe or heartbeat delivery failed", file=sys.stderr)
        sys.exit(1)
