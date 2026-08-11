#!/usr/bin/env python3
"""Safe loopback-only MCP session load smoke test.

This harness deliberately exposes no generic tool-name or request-body option.
It can either list the MCP schema or call the process-local ``list_members``
tool. It never calls a marketplace-backed tool.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import http.client
import json
import math
import re
import socket
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import SplitResult, urlsplit


MCP_PROTOCOL_VERSION = "2025-06-18"
MAX_CLIENTS = 256
MAX_CALLS_PER_CLIENT = 100
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_SESSION_ID_BYTES = 256
MAX_CLEANUP_SECONDS = 30.0
CLEANUP_ATTEMPTS = 5
START_BARRIER_TIMEOUT_SECONDS = 10.0
SESSION_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]+")
SAFE_LOCAL_TOOL = "list_members"


class LoadFailure(Exception):
    """A failure represented only by a non-sensitive aggregate class."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class ControlledOverload(Exception):
    """An expected workload rejection that must still clean up its session."""


@dataclass(frozen=True)
class Endpoint:
    host: str
    port: int
    path: str


@dataclass
class WorkerResult:
    outcome: str
    failure_class: str | None
    session_created: bool
    session_id: str | None = field(default=None, repr=False)
    delete_acknowledged: bool = False
    requests_completed: int = 0
    http_503_responses: int = 0
    latency_ms: list[float] = field(default_factory=list, repr=False)


@dataclass
class LifecycleOperation:
    successful: bool
    failure_class: str | None
    requests_completed: int
    http_503_responses: int
    latency_ms: list[float] = field(default_factory=list, repr=False)


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[index]


def bounded_int(name: str, minimum: int, maximum: int):
    def parse(raw: str) -> int:
        try:
            value = int(raw)
        except ValueError as error:
            raise argparse.ArgumentTypeError(f"{name} must be an integer") from error
        if not minimum <= value <= maximum:
            raise argparse.ArgumentTypeError(
                f"{name} must be between {minimum} and {maximum}"
            )
        return value

    return parse


def bounded_float(name: str, minimum: float, maximum: float):
    def parse(raw: str) -> float:
        try:
            value = float(raw)
        except ValueError as error:
            raise argparse.ArgumentTypeError(f"{name} must be a number") from error
        if not minimum <= value <= maximum:
            raise argparse.ArgumentTypeError(
                f"{name} must be between {minimum:g} and {maximum:g}"
            )
        return value

    return parse


def parse_loopback_endpoint(raw_url: str) -> Endpoint:
    parsed: SplitResult = urlsplit(raw_url)
    if parsed.scheme != "http":
        raise argparse.ArgumentTypeError("URL scheme must be http")
    if parsed.username is not None or parsed.password is not None:
        raise argparse.ArgumentTypeError("URL credentials are not allowed")
    if parsed.query or parsed.fragment:
        raise argparse.ArgumentTypeError("URL query and fragment are not allowed")
    if parsed.path != "/mcp":
        raise argparse.ArgumentTypeError("URL path must be exactly /mcp")

    host = parsed.hostname
    if host != "127.0.0.1":
        raise argparse.ArgumentTypeError("URL host must be exactly 127.0.0.1")
    try:
        parsed_port = parsed.port
    except ValueError as error:
        raise argparse.ArgumentTypeError("URL port is invalid") from error
    port = 80 if parsed_port is None else parsed_port
    if not 1 <= port <= 65_535:
        raise argparse.ArgumentTypeError("URL port must be between 1 and 65535")

    return Endpoint(host=host, port=port, path=parsed.path)


def json_body(method: str, request_id: int | None, params: dict[str, Any] | None) -> bytes:
    message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
    if request_id is not None:
        message["id"] = request_id
    if params is not None:
        message["params"] = params
    return json.dumps(message, separators=(",", ":")).encode("utf-8")


def request(
    connection: http.client.HTTPConnection,
    endpoint: Endpoint,
    method: str,
    payload: bytes | None,
    session_id: str | None,
) -> tuple[int, str, bytes, float, str | None, str | None]:
    headers = {
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": MCP_PROTOCOL_VERSION,
    }
    if payload is not None:
        headers["Content-Type"] = "application/json"
    if session_id is not None:
        headers["Mcp-Session-Id"] = session_id

    started = time.perf_counter()
    connection.request(method, endpoint.path, body=payload, headers=headers)
    response = connection.getresponse()
    content_length = response.getheader("Content-Length")
    if content_length is not None:
        try:
            declared_length = int(content_length)
            if declared_length < 0:
                response.close()
                raise LoadFailure("invalid_content_length")
            if declared_length > MAX_RESPONSE_BYTES:
                response.close()
                raise LoadFailure("response_too_large")
        except ValueError:
            response.close()
            raise LoadFailure("invalid_content_length")

    body = response.read(MAX_RESPONSE_BYTES + 1)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    if len(body) > MAX_RESPONSE_BYTES:
        response.close()
        raise LoadFailure("response_too_large")
    content_type = response.getheader("Content-Type", "")
    response_session_id = response.getheader("Mcp-Session-Id")
    retry_after = response.getheader("Retry-After")
    return (
        response.status,
        content_type,
        body,
        elapsed_ms,
        response_session_id,
        retry_after,
    )


def parse_rpc_message(content_type: str, body: bytes) -> dict[str, Any]:
    if not body:
        raise LoadFailure("invalid_mcp_response")
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise LoadFailure("invalid_mcp_response") from error

    candidates: list[str]
    if "text/event-stream" in content_type.lower():
        candidates = []
        event_data: list[str] = []
        for line in text.splitlines():
            if not line:
                if event_data:
                    candidates.append("\n".join(event_data))
                    event_data.clear()
                continue
            if line.startswith("data:"):
                event_data.append(line[5:].lstrip())
        if event_data:
            candidates.append("\n".join(event_data))
    else:
        candidates = [text]

    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    raise LoadFailure("invalid_mcp_response")


def require_status(status: int, expected: set[int], failure_class: str) -> None:
    if status not in expected:
        raise LoadFailure(failure_class)


def validate_session_id(raw_session_id: str | None) -> str:
    if raw_session_id is None:
        raise LoadFailure("missing_session_id")
    try:
        encoded = raw_session_id.encode("ascii")
    except UnicodeEncodeError as error:
        raise LoadFailure("invalid_session_id") from error
    if not 1 <= len(encoded) <= MAX_SESSION_ID_BYTES:
        raise LoadFailure("invalid_session_id")
    if SESSION_ID_PATTERN.fullmatch(raw_session_id) is None:
        raise LoadFailure("invalid_session_id")
    return raw_session_id


def validate_initialize(content_type: str, body: bytes) -> None:
    message = parse_rpc_message(content_type, body)
    result = message.get("result")
    if not isinstance(result, dict) or not isinstance(result.get("serverInfo"), dict):
        raise LoadFailure("invalid_initialize_response")


def validate_tools_list(content_type: str, body: bytes) -> None:
    message = parse_rpc_message(content_type, body)
    result = message.get("result")
    tools = result.get("tools") if isinstance(result, dict) else None
    if not isinstance(tools, list) or not tools:
        raise LoadFailure("invalid_tools_list_response")
    names = {
        item.get("name")
        for item in tools
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }
    if SAFE_LOCAL_TOOL not in names:
        raise LoadFailure("invalid_tools_list_response")


def validate_local_tool(content_type: str, body: bytes) -> None:
    message = parse_rpc_message(content_type, body)
    result = message.get("result")
    if not isinstance(result, dict) or result.get("isError") is True:
        raise LoadFailure("local_tool_failed")
    if not isinstance(result.get("content"), list):
        raise LoadFailure("invalid_local_tool_response")


def worker(
    endpoint: Endpoint,
    ordinal: int,
    calls: int,
    mode: str,
    timeout_seconds: float,
    expect_overload: bool,
    start_barrier: threading.Barrier | None,
) -> WorkerResult:
    session_id: str | None = None
    session_created = False
    delete_acknowledged = False
    controlled_overload = False
    failure_class: str | None = None
    latencies: list[float] = []
    requests_completed = 0
    http_503_responses = 0

    def perform(
        connection: http.client.HTTPConnection,
        method: str,
        payload: bytes | None,
        active_session_id: str | None,
    ) -> tuple[int, str, bytes, str | None, str | None]:
        nonlocal requests_completed, http_503_responses
        (
            status,
            content_type,
            body,
            elapsed_ms,
            response_session_id,
            retry_after,
        ) = request(connection, endpoint, method, payload, active_session_id)
        requests_completed += 1
        latencies.append(elapsed_ms)
        if status == 503:
            http_503_responses += 1
        return status, content_type, body, response_session_id, retry_after

    def reject_or_control_overload(status: int, retry_after: str | None) -> None:
        if status != 503:
            return
        if expect_overload and retry_after == "1":
            raise ControlledOverload
        if expect_overload:
            raise LoadFailure("invalid_overload_retry_after")
        raise LoadFailure("unexpected_http_503")

    if start_barrier is not None:
        try:
            start_barrier.wait(timeout=START_BARRIER_TIMEOUT_SECONDS)
        except threading.BrokenBarrierError:
            return WorkerResult(
                outcome="failed",
                failure_class="start_barrier_broken",
                session_created=False,
            )

    connection = http.client.HTTPConnection(
        endpoint.host, endpoint.port, timeout=timeout_seconds
    )
    try:
        initialize_id = ordinal * 1_000 + 1
        status, content_type, body, response_session_id, retry_after = perform(
            connection,
            "POST",
            json_body(
                "initialize",
                initialize_id,
                {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {
                        "name": "mcp-session-load-smoke",
                        "version": "1.0",
                    },
                },
            ),
            None,
        )
        if response_session_id is not None:
            session_id = validate_session_id(response_session_id)
            session_created = True
        reject_or_control_overload(status, retry_after)
        require_status(status, {200}, "initialize_http_error")
        validate_initialize(content_type, body)
        if session_id is None:
            raise LoadFailure("missing_session_id")

        (
            status,
            _content_type,
            _body,
            _response_session_id,
            retry_after,
        ) = perform(
            connection,
            "POST",
            json_body("notifications/initialized", None, None),
            session_id,
        )
        reject_or_control_overload(status, retry_after)
        require_status(status, {200, 202, 204}, "initialized_http_error")

        for call_index in range(calls):
            request_id = ordinal * 1_000 + 100 + call_index
            if mode == "schema":
                payload = json_body("tools/list", request_id, None)
            else:
                payload = json_body(
                    "tools/call",
                    request_id,
                    {"name": SAFE_LOCAL_TOOL, "arguments": {}},
                )
            (
                status,
                content_type,
                body,
                _response_session_id,
                retry_after,
            ) = perform(connection, "POST", payload, session_id)
            reject_or_control_overload(status, retry_after)
            require_status(
                status,
                {200},
                "tools_list_http_error" if mode == "schema" else "local_tool_http_error",
            )
            if mode == "schema":
                validate_tools_list(content_type, body)
            else:
                validate_local_tool(content_type, body)
    except ControlledOverload:
        controlled_overload = True
    except LoadFailure as error:
        failure_class = error.code
    except socket.timeout:
        failure_class = "request_timeout"
    except (ConnectionError, http.client.HTTPException, OSError):
        failure_class = "connection_error"
    except Exception:
        failure_class = "internal_error"
    finally:
        connection.close()

    if failure_class is not None:
        outcome = "failed"
    elif controlled_overload:
        outcome = "controlled_overload"
    elif session_created:
        outcome = "successful"
    else:
        outcome = "failed"
        failure_class = "session_lifecycle_incomplete"

    return WorkerResult(
        outcome=outcome,
        failure_class=failure_class,
        session_created=session_created,
        session_id=session_id,
        delete_acknowledged=delete_acknowledged,
        requests_completed=requests_completed,
        http_503_responses=http_503_responses,
        latency_ms=latencies,
    )


def cleanup_session(
    endpoint: Endpoint,
    session_id: str,
    timeout_seconds: float,
    cleanup_deadline: float,
) -> LifecycleOperation:
    latencies: list[float] = []
    requests_completed = 0
    http_503_responses = 0
    last_failure = "delete_failed"

    for attempt in range(CLEANUP_ATTEMPTS):
        remaining = cleanup_deadline - time.monotonic()
        if remaining <= 0:
            return LifecycleOperation(
                False,
                "delete_deadline_exhausted",
                requests_completed,
                http_503_responses,
                latencies,
            )
        cleanup_connection = http.client.HTTPConnection(
            endpoint.host,
            endpoint.port,
            timeout=min(timeout_seconds, remaining, 5.0),
        )
        retryable = False
        try:
            (
                status,
                _content_type,
                _body,
                elapsed_ms,
                _response_session_id,
                retry_after,
            ) = request(cleanup_connection, endpoint, "DELETE", None, session_id)
            requests_completed += 1
            latencies.append(elapsed_ms)
            if status == 503:
                http_503_responses += 1
                if retry_after == "1":
                    last_failure = "delete_http_503"
                    retryable = True
                else:
                    last_failure = "delete_invalid_overload_retry_after"
            elif 200 <= status < 300:
                return LifecycleOperation(
                    True,
                    None,
                    requests_completed,
                    http_503_responses,
                    latencies,
                )
            else:
                last_failure = "delete_http_error"
        except LoadFailure as error:
            last_failure = f"delete_{error.code}"
        except socket.timeout:
            last_failure = "delete_timeout"
            retryable = True
        except (ConnectionError, http.client.HTTPException, OSError):
            last_failure = "delete_connection_error"
            retryable = True
        except Exception:
            last_failure = "delete_internal_error"
        finally:
            cleanup_connection.close()

        if not retryable or attempt == CLEANUP_ATTEMPTS - 1:
            break
        delay = min(0.05 * (2**attempt), max(0.0, cleanup_deadline - time.monotonic()))
        if delay > 0:
            time.sleep(delay)

    return LifecycleOperation(
        False,
        last_failure,
        requests_completed,
        http_503_responses,
        latencies,
    )


def finalize_session(
    result: WorkerResult,
    endpoint: Endpoint,
    timeout_seconds: float,
    cleanup_deadline: float,
) -> None:
    if result.session_id is None:
        return
    cleanup = cleanup_session(
        endpoint, result.session_id, timeout_seconds, cleanup_deadline
    )
    result.delete_acknowledged = cleanup.successful
    result.requests_completed += cleanup.requests_completed
    result.http_503_responses += cleanup.http_503_responses
    result.latency_ms.extend(cleanup.latency_ms)
    if not cleanup.successful:
        result.outcome = "failed"
        if result.failure_class is None:
            result.failure_class = cleanup.failure_class or "delete_failed"


def health_probe(
    endpoint: Endpoint, timeout_seconds: float, phase: str
) -> LifecycleOperation:
    health_endpoint = Endpoint(endpoint.host, endpoint.port, "/health")
    connection = http.client.HTTPConnection(
        endpoint.host, endpoint.port, timeout=timeout_seconds
    )
    try:
        (
            status,
            _content_type,
            _body,
            elapsed_ms,
            _response_session_id,
            _retry_after,
        ) = request(connection, health_endpoint, "GET", None, None)
        if status == 200:
            return LifecycleOperation(True, None, 1, int(status == 503), [elapsed_ms])
        return LifecycleOperation(
            False, f"health_{phase}_http_error", 1, int(status == 503), [elapsed_ms]
        )
    except LoadFailure as error:
        return LifecycleOperation(
            False, f"health_{phase}_{error.code}", 0, 0, []
        )
    except socket.timeout:
        return LifecycleOperation(False, f"health_{phase}_timeout", 0, 0, [])
    except (ConnectionError, http.client.HTTPException, OSError):
        return LifecycleOperation(
            False, f"health_{phase}_connection_error", 0, 0, []
        )
    except Exception:
        return LifecycleOperation(
            False, f"health_{phase}_internal_error", 0, 0, []
        )
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Load MCP session/schema handling on loopback without marketplace calls"
        )
    )
    parser.add_argument(
        "--url",
        type=parse_loopback_endpoint,
        default=parse_loopback_endpoint("http://127.0.0.1:8787/mcp"),
        help="numeric 127.0.0.1 MCP URL; path must be /mcp",
    )
    parser.add_argument(
        "--mode",
        choices=("schema", "local"),
        default="schema",
        help="schema lists tools; local calls only list_members",
    )
    parser.add_argument(
        "--clients",
        type=bounded_int("clients", 1, MAX_CLIENTS),
        default=32,
    )
    parser.add_argument(
        "--calls",
        type=bounded_int("calls", 1, MAX_CALLS_PER_CLIENT),
        default=5,
        help="schema or local calls per client",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=bounded_float("timeout-seconds", 1.0, 60.0),
        default=10.0,
        help="per-request timeout",
    )
    parser.add_argument(
        "--expect-overload",
        action="store_true",
        help="require mixed success and controlled HTTP 503 with Retry-After: 1",
    )
    args = parser.parse_args()

    started = time.perf_counter()
    health_before = health_probe(args.url, args.timeout_seconds, "before")
    if not health_before.successful:
        wall_seconds = time.perf_counter() - started
        summary = {
            "mode": args.mode,
            "clients": args.clients,
            "calls_per_client": args.calls,
            "expect_overload": args.expect_overload,
            "overload_observed": False,
            "successful_workload_observed": False,
            "response_cap_bytes": MAX_RESPONSE_BYTES,
            "aborted_before_load": True,
            "health": {"before": False, "after": None},
            "outcomes": {"successful": 0, "controlled_overload": 0, "failed": 0},
            "sessions": {
                "created": 0,
                "delete_acknowledged": 0,
                "delete_failed": 0,
                "duplicates": 0,
            },
            "recovery": {
                "attempted": False,
                "successful": False,
                "delete_acknowledged": False,
            },
            "requests": {
                "completed": health_before.requests_completed,
                "http_503": health_before.http_503_responses,
                "rps": round(health_before.requests_completed / wall_seconds, 2)
                if wall_seconds > 0
                else 0.0,
            },
            "wall_seconds": round(wall_seconds, 3),
            "latency_ms": {
                "p50": round(percentile(health_before.latency_ms, 0.50), 3),
                "p95": round(percentile(health_before.latency_ms, 0.95), 3),
                "p99": round(percentile(health_before.latency_ms, 0.99), 3),
                "max": round(max(health_before.latency_ms), 3)
                if health_before.latency_ms
                else 0.0,
            },
            "failure_classes": {
                health_before.failure_class or "health_before_failed": 1
            },
        }
        print(json.dumps(summary, ensure_ascii=True, sort_keys=True, indent=2))
        return 1

    start_barrier = threading.Barrier(args.clients)
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as executor:
        futures = [
            executor.submit(
                worker,
                args.url,
                ordinal,
                args.calls,
                args.mode,
                args.timeout_seconds,
                args.expect_overload,
                start_barrier,
            )
            for ordinal in range(args.clients)
        ]
        results = [future.result() for future in futures]

    cleanup_deadline = time.monotonic() + MAX_CLEANUP_SECONDS
    for result in results:
        finalize_session(result, args.url, args.timeout_seconds, cleanup_deadline)

    recovery = worker(
        args.url,
        MAX_CLIENTS + 1,
        1,
        "schema",
        args.timeout_seconds,
        False,
        None,
    )
    finalize_session(
        recovery,
        args.url,
        args.timeout_seconds,
        time.monotonic() + min(MAX_CLEANUP_SECONDS, 10.0),
    )
    health_after = health_probe(args.url, args.timeout_seconds, "after")
    wall_seconds = time.perf_counter() - started

    failure_classes = Counter(
        result.failure_class for result in results if result.failure_class is not None
    )
    if recovery.failure_class is not None:
        failure_classes[recovery.failure_class] += 1
    if health_after.failure_class is not None:
        failure_classes[health_after.failure_class] += 1
    outcomes = Counter(result.outcome for result in results)
    session_ids = [
        result.session_id for result in results if result.session_id is not None
    ]
    if recovery.session_id is not None:
        session_ids.append(recovery.session_id)
    duplicate_sessions = len(session_ids) - len(set(session_ids))
    latencies = (
        health_before.latency_ms
        + [value for result in results for value in result.latency_ms]
        + recovery.latency_ms
        + health_after.latency_ms
    )
    requests_completed = (
        health_before.requests_completed
        + sum(result.requests_completed for result in results)
        + recovery.requests_completed
        + health_after.requests_completed
    )
    http_503_responses = (
        health_before.http_503_responses
        + sum(result.http_503_responses for result in results)
        + recovery.http_503_responses
        + health_after.http_503_responses
    )
    overload_observed = (
        outcomes["controlled_overload"] > 0 and http_503_responses > 0
    )
    successful_workload_observed = outcomes["successful"] > 0
    if args.expect_overload and not overload_observed:
        failure_classes["expected_overload_not_observed"] += 1
    if args.expect_overload and not successful_workload_observed:
        failure_classes["successful_workload_not_observed"] += 1

    summary = {
        "mode": args.mode,
        "clients": args.clients,
        "calls_per_client": args.calls,
        "expect_overload": args.expect_overload,
        "overload_observed": overload_observed,
        "successful_workload_observed": successful_workload_observed,
        "response_cap_bytes": MAX_RESPONSE_BYTES,
        "aborted_before_load": False,
        "health": {
            "before": health_before.successful,
            "after": health_after.successful,
        },
        "outcomes": {
            "successful": outcomes["successful"],
            "controlled_overload": outcomes["controlled_overload"],
            "failed": outcomes["failed"],
        },
        "sessions": {
            "created": sum(result.session_created for result in results),
            "delete_acknowledged": sum(
                result.delete_acknowledged for result in results
            ),
            "delete_failed": sum(
                result.session_created and not result.delete_acknowledged
                for result in results
            ),
            "duplicates": duplicate_sessions,
        },
        "recovery": {
            "attempted": True,
            "successful": recovery.outcome == "successful",
            "delete_acknowledged": recovery.delete_acknowledged,
        },
        "requests": {
            "completed": requests_completed,
            "http_503": http_503_responses,
            "rps": round(requests_completed / wall_seconds, 2)
            if wall_seconds > 0
            else 0.0,
        },
        "wall_seconds": round(wall_seconds, 3),
        "latency_ms": {
            "p50": round(percentile(latencies, 0.50), 3),
            "p95": round(percentile(latencies, 0.95), 3),
            "p99": round(percentile(latencies, 0.99), 3),
            "max": round(max(latencies), 3) if latencies else 0.0,
        },
        "failure_classes": dict(sorted(failure_classes.items())),
    }
    print(json.dumps(summary, ensure_ascii=True, sort_keys=True, indent=2))

    clean = (
        outcomes["failed"] == 0
        and duplicate_sessions == 0
        and summary["sessions"]["delete_failed"] == 0
        and recovery.outcome == "successful"
        and recovery.delete_acknowledged
        and health_after.successful
        and (
            not args.expect_overload
            or (overload_observed and successful_workload_observed)
        )
    )
    return 0 if clean else 1


if __name__ == "__main__":
    sys.exit(main())
