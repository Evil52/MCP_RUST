#!/usr/bin/env python3
"""Exercise verified images in a sterile Docker stack; never target live resources.

The caller supplies previously verified release locks and a locally available
initialized-schema PostgreSQL image. No vendor credentials, production volumes,
or production tunnel processes are used. Evidence explicitly marks the tunnel
as a local protocol fixture, not a control-plane recovery test.
"""

import argparse
import http.server
import json
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request


def run(*args, timeout=90, **kwargs):
    return subprocess.run(args, check=True, capture_output=True, text=True,
                          timeout=timeout, **kwargs).stdout.strip()


def image_from_lock(path):
    lock = json.loads(Path(path).read_text())
    image = lock["images"]["server"]["reference"]
    if not re.fullmatch(r"[a-z0-9./_-]+@sha256:[0-9a-f]{64}", image):
        raise ValueError("server image must be pinned by digest")
    revision = lock["git_sha"]
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("release revision must be a full Git SHA")
    actual = json.loads(run("docker", "image", "inspect", image))[0]
    if actual["Config"]["Labels"]["org.opencontainers.image.revision"] != revision:
        raise ValueError("local image revision differs from release lock")
    return image, revision


OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def request(url, payload=None, session=None):
    headers = {"Accept": "application/json, text/event-stream"}
    if session:
        headers["Mcp-Session-Id"] = session
        headers["MCP-Protocol-Version"] = "2025-03-26"
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=None if payload is None else
                                 json.dumps(payload).encode(), headers=headers)
    try:
        with OPENER.open(req, timeout=8) as result:
            return result.status, result.read().decode(), result.headers
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode(), error.headers


def eventually(check, seconds=90):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        try:
            if check():
                return
        except (OSError, subprocess.SubprocessError):
            pass
        time.sleep(1)
    raise RuntimeError("recovery condition did not become true within its deadline")


def rpc_document(body):
    if body.lstrip().startswith("{"):
        return json.loads(body)
    for event in body.replace("\r\n", "\n").split("\n\n"):
        data = "\n".join(line[5:].lstrip() for line in event.splitlines()
                         if line.startswith("data:"))
        if data:
            document = json.loads(data)
            if document.get("id") == 2:
                return document
    raise ValueError("MCP response has no JSON-RPC result")


class TunnelFixture(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = (f"commands_poll_last_successful_timestamp_seconds {int(time.time())}\n"
                f"process_start_time_seconds {int(time.time()) - 600}\n"
                if self.path == "/metrics" else "ok")
        self.send_response(200)
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, *_args):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current-lock", required=True)
    parser.add_argument("--rollback-lock", required=True)
    parser.add_argument("--postgres-image", required=True)
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args()
    def interrupted(_signum, _frame):
        raise RuntimeError("recovery drill interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    os.umask(0o077)
    output = Path(args.evidence)
    if output.exists() or output.is_symlink():
        raise ValueError("evidence path must be new")
    current = image_from_lock(args.current_lock)
    rollback = image_from_lock(args.rollback_lock)
    if not re.fullmatch(r"(?:[a-z0-9./_-]+@)?sha256:[0-9a-f]{64}", args.postgres_image):
        raise ValueError("PostgreSQL image must be an immutable local ID or digest")
    run("docker", "image", "inspect", args.postgres_image)
    prefix = "mcp-recovery-drill-" + secrets.token_hex(6)
    db, server, network = prefix + "-db", prefix + "-server", prefix + "-network"
    publish_network = prefix + "-loopback-publish"
    volume = prefix + "-data"
    ownership_label = "mcp.ozon.recovery-drill=" + prefix
    baseline = run("docker", "ps", "-aq").splitlines()
    baseline_state = {cid: json.loads(run("docker", "inspect", cid))[0]["State"]["StartedAt"]
                      for cid in baseline}
    evidence = {"status": "running", "current_sha": current[1],
                "rollback_sha": rollback[1], "steps": [],
                "tunnel": "local fixture; real control-plane recovery not exercised"}
    started = time.monotonic()
    fixture = http.server.ThreadingHTTPServer(("127.0.0.1", 0), TunnelFixture)
    threading.Thread(target=fixture.serve_forever, daemon=True).start()
    resources = []
    try:
        with tempfile.TemporaryDirectory(prefix=prefix + "-") as directory:
            root = Path(directory).resolve()
            root.chmod(0o700)
            registry = root / "access.json"
            registry.write_text(json.dumps({"version": 1, "actors": [
                {"id": "test_manager", "name": "Recovery test", "role": "manager"}],
                "accounts": [{"id": "test_account", "organization": "Recovery test",
                    "marketplace": "ozon", "seller_client_id": "test",
                    "manager_id": "test_manager", "ozon": {"store_id": "test_store",
                        "client_id_env": "TEST_OZON_ID", "api_key_env": "TEST_OZON_KEY"}}]}))
            registry.chmod(0o644)  # Container UID 10001; owned private parent.
            password = secrets.token_hex(24)
            db_env = root / "db.env"
            db_env.write_text("\n".join([
                "POSTGRES_DB=ozon_positions", "POSTGRES_USER=position_admin",
                "POSTGRES_PASSWORD=" + secrets.token_hex(24),
                "POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 --auth-local=scram-sha-256",
                *[name + "=" + (password if name == "POSITION_READER_DB_PASSWORD"
                                 else secrets.token_hex(24)) for name in (
                    "POSITION_COLLECTOR_DB_PASSWORD", "POSITION_READER_DB_PASSWORD",
                    "REPORT_WORKER_DB_PASSWORD", "REPORT_COLLECTOR_DB_PASSWORD",
                    "REPORT_REFRESH_REQUESTER_DB_PASSWORD", "CONTROL_WRITER_DB_PASSWORD",
                    "OZON_CONTROL_PLANNER_DB_PASSWORD", "OZON_CONTROL_EXECUTOR_DB_PASSWORD",
                    "WB_AUTOMATION_DB_PASSWORD")]]) + "\n")
            run("docker", "network", "create", "--internal", "--label", ownership_label, network)
            resources.append(("network", network))
            # Docker does not publish ports from an internal-only network.
            # This separate test bridge publishes only the synthetic server on
            # loopback. It has no production peers or vendor credentials.
            run("docker", "network", "create", "--label", ownership_label,
                "--opt", "com.docker.network.bridge.host_binding_ipv4=127.0.0.1",
                "--opt", "com.docker.network.bridge.enable_icc=false", publish_network)
            resources.append(("network", publish_network))
            run("docker", "volume", "create", "--label", ownership_label, volume)
            resources.append(("volume", volume))
            resources.append(("container", db))
            run("docker", "run", "-d", "--pull", "never", "--name", db,
                "--label", ownership_label,
                "--network", network, "--network-alias", "test-db", "--env-file", str(db_env),
                "--volume", volume + ":/var/lib/postgresql/data", "--memory", "512m",
                args.postgres_image)
            def database_ready():
                if run("docker", "inspect", "--format", "{{.State.Status}}", db) == "exited":
                    raise RuntimeError("synthetic database exited during initialization")
                return run("docker", "exec", db, "/usr/local/bin/position-db-healthcheck") == ""
            eventually(database_ready)
            env_file = root / "server.env"
            env_file.write_text("\n".join([
                "MCP_TRANSPORT=http", "MCP_BIND=0.0.0.0:8787", "MCP_AUTH_MODE=dev",
                "MCP_ACTOR_ID=test_manager", "MCP_DEV_ALLOW_NON_LOOPBACK=true",
                "MCP_ACCESS_CONFIG=/etc/mcp-ozon/access.json",
                f"MCP_REPORTING_DATABASE_URL=postgresql://position_reader:{password}@test-db:5432/ozon_positions"
            ]) + "\n")
            resources.append(("container", server))

            def start_image(image):
                with socket.socket() as reservation:
                    reservation.bind(("127.0.0.1", 0))
                    published_port = reservation.getsockname()[1]
                run("docker", "run", "-d", "--pull", "never", "--name", server,
                    "--label", ownership_label,
                    "--network", publish_network, "--network", network, "--env-file", str(env_file),
                    "-p", f"127.0.0.1:{published_port}:8787", "--read-only", "--cap-drop", "ALL",
                    "--security-opt", "no-new-privileges:true", "--memory", "512m",
                    "--pids-limit", "128", "--tmpfs", "/tmp:size=16m,mode=1777",
                    "--mount", f"type=bind,src={registry},dst=/etc/mcp-ozon/access.json,readonly",
                    "--health-cmd", "wget -q -T 3 -O /dev/null http://127.0.0.1:8787/readyz",
                    "--health-interval", "2s", "--health-timeout", "3s", image)
                address = run("docker", "port", server, "8787/tcp")
                if not re.fullmatch(r"127\.0\.0\.1:[0-9]+", address):
                    raise ValueError("unexpected Docker port mapping")
                base = "http://" + address
                eventually(lambda: request(base + "/readyz")[0] == 200 and
                    run("docker", "inspect", "--format", "{{.State.Health.Status}}", server) == "healthy")
                return base

            def mcp_session(base):
                code, _body, headers = request(base + "/mcp", {"jsonrpc": "2.0", "id": 1,
                    "method": "initialize", "params": {"protocolVersion": "2025-03-26",
                        "capabilities": {}, "clientInfo": {"name": "recovery-drill", "version": "1"}}})
                if code != 200:
                    raise RuntimeError("MCP initialize failed")
                session = headers.get("Mcp-Session-Id")
                request(base + "/mcp", {"jsonrpc": "2.0", "method": "notifications/initialized"}, session)
                return session

            def collection_result(base, session):
                code, body, _ = request(base + "/mcp", {"jsonrpc": "2.0", "id": 2,
                    "method": "tools/call", "params": {"name": "ofk_collection_status",
                        "arguments": {"account": "test_account", "limit": 1}}}, session)
                if code != 200:
                    raise RuntimeError("MCP tool request did not return a protocol response")
                document = rpc_document(body)
                if document.get("jsonrpc") != "2.0" or document.get("id") != 2:
                    raise ValueError("unexpected JSON-RPC response identity")
                return document

            def read_collections(base):
                document = collection_result(base, mcp_session(base))
                if "error" in document:
                    return False
                result = document["result"]
                if result.get("isError", False):
                    return False
                data = result.get("structuredContent")
                if data is None:
                    texts = [item["text"] for item in result["content"] if item["type"] == "text"]
                    if len(texts) != 1:
                        raise ValueError("expected one structured collection status")
                    data = json.loads(texts[0])
                return data == {"account_id": "test_account", "marketplace": "ozon", "items": []}

            def reporting_unavailable(base, session):
                document = collection_result(base, session)
                if "error" in document:
                    return "REPORTING_TEMPORARILY_UNAVAILABLE" in document["error"].get("message", "")
                result = document["result"]
                texts = [item["text"] for item in result.get("content", []) if item["type"] == "text"]
                return result.get("isError") is True and any("REPORTING_TEMPORARILY_UNAVAILABLE" in s for s in texts)

            base = start_image(current[0])
            eventually(lambda: read_collections(base))
            evidence["steps"].append({"name": "current_image_ready_and_mcp_read", "passed": True})
            before = run("docker", "inspect", "--format", "{{.State.StartedAt}}", server)
            outage_session = mcp_session(base)
            outage_started = time.monotonic()
            run("docker", "stop", "--time", "5", db)
            eventually(lambda: request(base + "/readyz")[0] == 503, seconds=45)
            if request(base + "/livez")[0] != 200:
                raise RuntimeError("database outage must not break process liveness")
            if not reporting_unavailable(base, outage_session):
                raise RuntimeError("database outage must produce REPORTING_TEMPORARILY_UNAVAILABLE")
            run("docker", "start", db)
            eventually(lambda: request(base + "/readyz")[0] == 200 and read_collections(base))
            if run("docker", "inspect", "--format", "{{.State.StartedAt}}", server) != before:
                raise RuntimeError("MCP restarted instead of recovering its database connection")
            evidence["steps"].append({"name": "database_disconnect_reconnect_same_process",
                "passed": True, "seconds": round(time.monotonic() - outage_started, 2)})
            for name, text in [("recovery.yaml", '{}'), ("runtime-api-key", "test-only"),
                ("tunnel.url", f"http://127.0.0.1:{fixture.server_port}")]:
                (root / name).write_text(text)
            curl_wrapper = root / "loopback-curl"
            curl_wrapper.write_text("#!/usr/bin/env python3\n"
                "import os, sys, urllib.parse\n"
                "url = urllib.parse.urlsplit(sys.argv[-1])\n"
                "if url.scheme != 'http' or url.hostname != '127.0.0.1': sys.exit(22)\n"
                "os.execv('/usr/bin/curl', ['/usr/bin/curl', *sys.argv[1:]])\n")
            curl_wrapper.chmod(0o700)
            watchdog_env = dict(os.environ, MCP_RUNTIME_DIR=str(root), MCP_CONTAINER_NAME=server,
                MCP_HEALTH_URL=base + "/readyz", MCP_SERVER_URL=base + "/mcp",
                TUNNEL_CLIENT_PROFILE="recovery", TUNNEL_CLIENT_PROFILE_DIR=str(root),
                TUNNEL_CLIENT_HEALTH_URL_FILE=str(root / "tunnel.url"),
                TUNNEL_CLIENT_BIN="/usr/bin/true", TMPDIR=str(root),
                MCP_RUNTIME_CURL_BIN=str(curl_wrapper))
            # Exclude developer overrides that could point the watchdog at a live dependency.
            watchdog_env.pop("DOCKER_BIN", None)
            run("docker", "stop", "--time", "5", server)
            outage_started = time.monotonic()
            run("bash", str(Path(__file__).resolve().parent / "ensure-local-runtime.sh"),
                env=watchdog_env, timeout=100)
            eventually(lambda: request(base + "/readyz")[0] == 200 and read_collections(base))
            evidence["steps"].append({"name": "watchdog_restarts_stopped_container",
                "passed": True, "seconds": round(time.monotonic() - outage_started, 2)})
            for label, release in [("rollback", rollback), ("return_to_current", current)]:
                run("docker", "rm", "-f", server)
                base = start_image(release[0])
                eventually(lambda: read_collections(base))
                evidence["steps"].append({"name": label, "sha": release[1], "passed": True})
            evidence["status"] = "passed"
    except BaseException as error:
        evidence["status"] = "failed"
        if isinstance(error, subprocess.CalledProcessError):
            output.with_name(output.stem + "-command-error.log").write_text(
                (error.stdout or "") + (error.stderr or ""))
        for kind, name in resources:
            if kind == "container":
                try:
                    log = subprocess.run(["docker", "logs", "--tail", "60", name],
                        capture_output=True, text=True, timeout=10, check=False)
                    output.with_name(output.stem + "-" + name + ".log").write_text(log.stdout + log.stderr)
                except OSError:
                    pass
        raise
    finally:
        fixture.shutdown()
        fixture.server_close()
        errors = []
        for kind, name in reversed(resources):
            try:
                record = json.loads(run("docker", kind, "inspect", name))[0]
                labels = record["Config"]["Labels"] if kind == "container" else record["Labels"]
                if labels.get("mcp.ozon.recovery-drill") != prefix:
                    errors.append("resource ownership mismatch: " + name)
                    continue
                if kind == "container":
                    run("docker", "rm", "-f", name)
                else:
                    run("docker", kind, "rm", name)
            except subprocess.SubprocessError:
                errors.append(name)
        for cid, started_at in baseline_state.items():
            try:
                if json.loads(run("docker", "inspect", cid))[0]["State"]["StartedAt"] != started_at:
                    errors.append("preexisting container changed: " + cid[:12])
            except subprocess.SubprocessError:
                errors.append("preexisting container disappeared: " + cid[:12])
        evidence["cleanup_errors"] = errors
        evidence["preexisting_containers_unchanged"] = not errors
        evidence["seconds"] = round(time.monotonic() - started, 2)
        if errors:
            evidence["status"] = "failed"
        with output.open("x") as target:
            json.dump(evidence, target, indent=2)
            target.write("\n")
        print(json.dumps(evidence, indent=2))
        if errors:
            raise RuntimeError("drill cleanup or preexisting-container verification failed")


if __name__ == "__main__":
    main()
