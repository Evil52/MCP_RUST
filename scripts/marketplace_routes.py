#!/usr/bin/env python3
"""macOS marketplace IPv4 bypass: inspect, reconcile narrow routes, or roll back.

The fixed allowlist is transport policy, not marketplace write authorization.
This tool never reads API credentials or changes the VPN/default route/firewall.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import contextlib
import datetime as dt
import fcntl
import ipaddress
import json
import os
from pathlib import Path
import re
import signal
import socket
import stat
import subprocess
import sys
import tempfile

MARKETPLACE_HOSTS = (
    "api-seller.ozon.ru", "api-performance.ozon.ru",
    "seller-analytics-api.wildberries.ru", "statistics-api.wildberries.ru",
    "content-api.wildberries.ru", "discounts-prices-api.wildberries.ru",
    "common-api.wildberries.ru", "advert-api.wildberries.ru",
    "marketplace-api.wildberries.ru",
)
VPN_HOSTS = ("api.openai.com", "chatgpt.com")
STATE_DIR = Path("/private/var/db/mcp-ozon-marketplace-routing")
MAX_IPS = 64


class RoutingError(Exception):
    """A bounded diagnostic, without credentials or HTTP response bodies."""


def run(args, timeout=5):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout,
                            env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "LC_ALL": "C"})
    if result.returncode or len(result.stdout) > 65536:
        raise RoutingError("command_failed: " + args[0])
    return result.stdout


def read_json(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 32768:
        raise RoutingError("unsafe_or_missing_json_file")
    try:
        return json.loads(path.read_text())
    except (ValueError, UnicodeError):
        raise RoutingError("invalid_json") from None


def config_document(value):
    if not isinstance(value, dict) or set(value) != {"version", "interface", "gateway"}:
        raise RoutingError("invalid_config_keys")
    if type(value["version"]) is not int or value["version"] != 1:
        raise RoutingError("unsupported_config_version")
    if not isinstance(value["interface"], str) or not re.fullmatch(r"en[0-9]{1,3}", value["interface"]):
        raise RoutingError("interface_must_be_physical_en_device")
    if not isinstance(value["gateway"], str):
        raise RoutingError("invalid_ipv4_gateway")
    try:
        address = ipaddress.IPv4Address(value["gateway"])
    except (ValueError, TypeError):
        raise RoutingError("invalid_ipv4_gateway") from None
    private = ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
    if not any(address in ipaddress.IPv4Network(net) for net in private):
        raise RoutingError("gateway_must_be_a_private_lan_address")
    return dict(value)


def addresses(host):
    values = sorted({row[4][0] for row in socket.getaddrinfo(host, 443, 0, socket.SOCK_STREAM)})
    if not values or len(values) > MAX_IPS:
        raise RoutingError("dns_address_count_invalid: " + host)
    for value in values:
        address = ipaddress.ip_address(value)
        if not address.is_global or address.is_multicast:
            raise RoutingError("dns_address_not_public: " + host)
        if address.version != 4:
            # Never report complete bypass while Happy Eyeballs can select IPv6.
            raise RoutingError("ipv6_requires_a_separate_reviewed_policy: " + host)
    return values


def parse_route(output):
    fields = {}
    for line in output.splitlines():
        key, separator, value = line.strip().partition(":")
        if separator:
            fields[key] = value.strip()
    if not fields.get("interface") or not fields.get("destination"):
        raise RoutingError("route_readback_incomplete")
    fields["flags"] = fields.get("flags", "").strip("<>").split(",")
    return fields


class System:
    resolve = staticmethod(addresses)

    @staticmethod
    def lan_default(interface):
        return parse_route(run(["/sbin/route", "-n", "get", "-inet", "-ifscope", interface, "default"]))

    @staticmethod
    def route(address):
        return parse_route(run(["/sbin/route", "-n", "get", "-inet", address]))

    @staticmethod
    def add(address, config):
        run(route_command("add", address, config))

    @staticmethod
    def delete(address, config):
        run(route_command("delete", address, config))


def route_command(action, address, config):
    args = ["/sbin/route", "-n", action, "-inet", "-host", address, config["gateway"]]
    if action == "add":
        args += ["-static", "-proto2"]
    return args


def direct(route, config):
    return (route.get("interface") == config["interface"]
            and route.get("gateway") == config["gateway"]
            and not {"REJECT", "BLACKHOLE"}.intersection(route["flags"]))


def marked_route(route, address, config):
    return (direct(route, config) and route["destination"] == address
            and {"HOST", "STATIC", "PROTO2"}.issubset(route["flags"]))


def journaled_route(route, address, config):
    # An owned route can acquire the wrong interface after a network change.
    # Its journal + explicit host marker + gateway still identify it for rollback.
    return (route["destination"] == address and route.get("gateway") == config["gateway"]
            and {"HOST", "STATIC", "PROTO2"}.issubset(route["flags"]))


def snapshot(system, config):
    # Resolve everything before a single mutation; DNS failure preserves routes.
    resolved = {host: system.resolve(host) for host in MARKETPLACE_HOSTS + VPN_HOSTS}
    market_ips = {ip for host in MARKETPLACE_HOSTS for ip in resolved[host]}
    vpn_ips = {ip for host in VPN_HOSTS for ip in resolved[host]}
    if len(market_ips | vpn_ips) > MAX_IPS or market_ips & vpn_ips:
        raise RoutingError("dns_policy_overlap_or_excessive_addresses")
    if not direct(system.lan_default(config["interface"]), config):
        raise RoutingError("configured_gateway_does_not_match_lan_default")
    if system.route(config["gateway"])["interface"] != config["interface"]:
        raise RoutingError("lan_gateway_is_not_on_configured_interface")
    routes = {ip: system.route(ip) for ip in sorted(market_ips | vpn_ips)}
    for ip in vpn_ips:
        route = routes[ip]
        if (not re.fullmatch(r"utun[0-9]+", route["interface"])
                or {"REJECT", "BLACKHOLE"}.intersection(route["flags"])):
            raise RoutingError("openai_vpn_route_missing")
    return resolved, routes


def make_plan(system, config):
    resolved, routes = snapshot(system, config)
    wanted = sorted({ip for host in MARKETPLACE_HOSTS for ip in resolved[host]})
    return {
        "resolved": resolved, "routes": routes,
        "add": [ip for ip in wanted if not direct(routes[ip], config)],
    }


def empty_state(config):
    return {"version": 1, "config": dict(config), "owned": []}


def validate_state(state, config):
    if (not isinstance(state, dict) or set(state) != {"version", "config", "owned"}
            or state["version"] != 1 or state["config"] != config
            or not isinstance(state["owned"], list) or len(state["owned"]) > MAX_IPS):
        raise RoutingError("invalid_state_or_config_changed_rollback_first")
    if not all(isinstance(value, str) for value in state["owned"]):
        raise RoutingError("invalid_state_address")
    if len(state["owned"]) != len(set(state["owned"])):
        raise RoutingError("duplicate_state_addresses")
    for value in state["owned"]:
        try:
            ip = ipaddress.IPv4Address(value)
        except (ValueError, TypeError):
            raise RoutingError("invalid_state_address") from None
        if not ip.is_global or ip.is_multicast:
            raise RoutingError("invalid_state_address")


def remove_owned(system, config, state, save, addresses_to_remove):
    for address in addresses_to_remove:
        if journaled_route(system.route(address), address, config):
            system.delete(address, config)
            if journaled_route(system.route(address), address, config):
                raise RoutingError("route_delete_readback_failed")
        # A replaced route belongs to its new owner: never delete it.
        state["owned"].remove(address)
        save(state)


def reconcile(system, config, state, save):
    validate_state(state, config)
    plan = make_plan(system, config)
    wanted = {ip for host in MARKETPLACE_HOSTS for ip in plan["resolved"][host]}
    # An existing foreign /32 must not be overwritten, even if it is on the VPN.
    for ip in plan["add"]:
        flags = set(plan["routes"][ip]["flags"])
        # macOS creates scoped HOST/WASCLONED cache entries during normal traffic.
        # Those are not administrator-owned explicit host routes.
        if "HOST" in flags and "WASCLONED" not in flags:
            raise RoutingError("foreign_host_route_conflict: " + ip)
    stale = [ip for ip in state["owned"] if ip not in wanted]
    remove_owned(system, config, state, save, stale)
    created = []
    try:
        for ip in plan["add"]:
            # Write-ahead journal plus PROTO2 marker permits crash recovery.
            if ip not in state["owned"]:
                state["owned"].append(ip)
                save(state)
            created.append(ip)
            system.add(ip, config)
            if not marked_route(system.route(ip), ip, config):
                raise RoutingError("route_add_readback_failed")
        after = make_plan(system, config)
        if after["add"]:
            raise RoutingError("marketplace_route_readback_failed")
    except Exception:
        remove_owned(system, config, state, save, created)
        raise
    return {"added": created, "removed": stale, "owned": list(state["owned"])}


def require_root_path(path, directory=False):
    path = Path(path).absolute()
    for entry in (path, *path.parents):
        metadata = entry.lstat()
        if (stat.S_ISLNK(metadata.st_mode) or metadata.st_uid != 0
                or metadata.st_mode & 0o022):
            raise RoutingError("root_path_is_not_protected")
    if directory and not path.is_dir():
        raise RoutingError("state_directory_is_not_a_directory")


@contextlib.contextmanager
def locked_state(config):
    require_root_path(STATE_DIR, directory=True)
    descriptor = os.open(STATE_DIR / "lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != 0
                or metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) != 0o600):
            raise RoutingError("unsafe_lock_file")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        path = STATE_DIR / "state.json"
        if path.exists() or path.is_symlink():
            require_root_path(path)
            state = read_json(path)
            if not isinstance(state, dict):
                raise RoutingError("invalid_state_document")
            validate_state(state, config_document(state.get("config")))
            if not state["owned"]:
                state = empty_state(config)
        else:
            state = empty_state(config)

        def save(value):
            fd, name = tempfile.mkstemp(dir=STATE_DIR, prefix=".state-")
            try:
                with os.fdopen(fd, "w") as stream:
                    json.dump(value, stream, sort_keys=True)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(name, path)
            finally:
                if os.path.exists(name):
                    os.unlink(name)

        yield state, save
    finally:
        os.close(descriptor)


def https_probe(item):
    host, address = item
    path = "/v1/models" if host == "api.openai.com" else (
        "/ping" if host.endswith(".wildberries.ru") else "/")
    output = run([
        "/usr/bin/curl", "--noproxy", "*", "--proto", "=https", "--ipv4",
        "--connect-timeout", "3", "--max-time", "5", "--silent", "--output", "/dev/null",
        "--resolve", host + ":443:" + address,
        "--write-out", "%{http_code} %{time_appconnect} %{time_total}",
        "https://" + host + path,
    ], timeout=6).split()
    if len(output) != 3 or not re.fullmatch(r"[1-5][0-9]{2}", output[0]):
        raise RoutingError("tls_http_probe_failed: " + host)
    if host == "api.openai.com" and output[0] != "401":
        raise RoutingError("openai_control_plane_expected_401")
    return {"host": host, "address": address, "http_status": int(output[0]),
            "tls_seconds": float(output[1]), "total_seconds": float(output[2])}


def check(system, config, probes=False, tunnel_url_file=None):
    plan = make_plan(system, config)
    if plan["add"]:
        raise RoutingError("marketplace_direct_route_missing")
    result = {"route_check": "passed", "resolved": plan["resolved"], "routes": plan["routes"]}
    if probes:
        items = [(host, ip) for host in MARKETPLACE_HOSTS + ("api.openai.com",)
                 for ip in plan["resolved"][host]]
        with ThreadPoolExecutor(max_workers=3) as pool:
            result["https_transport"] = list(pool.map(https_probe, items))
        # Detect route changes while probes were in flight.
        after = make_plan(system, config)
        if after["add"]:
            raise RoutingError("marketplace_route_changed_during_probe")
        if after["resolved"] != plan["resolved"]:
            raise RoutingError("dns_changed_during_probe_retry_check")
    if tunnel_url_file:
        # Available in the repository/user runtime, never imported by root apply.
        from operations_heartbeat import probe
        result["tunnel_poll_age_seconds"] = probe(tunnel_url_file, 90)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("plan", "check", "apply", "rollback"))
    parser.add_argument("--config", required=True)
    parser.add_argument("--probe", action="store_true", help="bounded HTTPS probes; check only")
    parser.add_argument("--tunnel-url-file", help="verify MCP tunnel poll freshness; check only")
    args = parser.parse_args()
    if sys.platform != "darwin":
        raise RoutingError("this_policy_supports_macos_only")
    if args.mode != "check" and (args.probe or args.tunnel_url_file):
        raise RoutingError("probe_arguments_require_check_mode")
    config = config_document(read_json(args.config))
    system = System()
    if args.mode == "plan":
        result = make_plan(system, config)
        result["commands"] = [route_command("add", ip, config) for ip in result["add"]]
    elif args.mode == "check":
        result = check(system, config, args.probe, args.tunnel_url_file)
    else:
        if os.geteuid() != 0:
            raise RoutingError("root_required_for_route_changes")
        require_root_path(args.config)
        with locked_state(config) as (state, save):
            validate_state(state, config)
            if args.mode == "apply":
                result = reconcile(system, config, state, save)
            else:
                removed = list(state["owned"])
                remove_owned(system, config, state, save, removed)
                result = {"rolled_back": removed}
    print(json.dumps({"ok": True, "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                      "mode": args.mode, **result}, sort_keys=True))


def deadline(_signum, _frame):
    raise RoutingError("routing_operation_deadline_exceeded")


if __name__ == "__main__":
    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(50)
    try:
        main()
    except (RoutingError, OSError, ValueError, subprocess.SubprocessError) as error:
        message = str(error) if isinstance(error, RoutingError) else type(error).__name__
        print(json.dumps({"ok": False, "error": message}), file=sys.stderr)
        sys.exit(1)
