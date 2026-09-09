"""Routing safety/failure tests; no real network, sudo, or route modifications."""

import copy
import importlib.util
from pathlib import Path
import socket
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "marketplace_routes.py"
SPEC = importlib.util.spec_from_file_location("marketplace_routes", SCRIPT)
routes = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(routes)
CONFIG = {"version": 1, "interface": "en0", "gateway": "192.168.0.1"}
WB_IP = "194.1.214.253"
OZON_IP = "185.73.194.107"
NEW_IP = "185.73.193.82"
VPN_IP = "162.159.140.245"


class FakeSystem:
    def __init__(self):
        self.dns = {h: [WB_IP if h.endswith("wildberries.ru") else OZON_IP]
                    for h in routes.MARKETPLACE_HOSTS}
        self.dns.update({h: [VPN_IP] for h in routes.VPN_HOSTS})
        self.table = {}
        self.events = []
        self.fail_add = None
        self.fail_delete = None
        self.drop_vpn_on_add = False
        self.direct_default = False

    def resolve(self, host):
        value = self.dns[host]
        if isinstance(value, Exception):
            raise value
        return value

    def lan_default(self, interface):
        return {"interface": interface, "gateway": CONFIG["gateway"],
                "destination": "default", "flags": ["UP", "GATEWAY"]}

    def route(self, address):
        if address == CONFIG["gateway"]:
            return {"interface": "en0", "destination": address, "flags": ["HOST", "LLINFO"]}
        if address in self.table:
            return copy.deepcopy(self.table[address])
        if self.direct_default and address != VPN_IP:
            return {"interface": "en0", "gateway": CONFIG["gateway"],
                    "destination": "default", "flags": ["GATEWAY", "STATIC"]}
        return {"interface": "utun4", "destination": "128.0.0.0", "flags": ["UP"]}

    def add(self, address, config):
        self.events.append(("add", address))
        if address == self.fail_add:
            raise routes.RoutingError("injected_add_failure")
        self.table[address] = {"interface": config["interface"], "gateway": config["gateway"],
                               "destination": address, "flags": ["HOST", "STATIC", "PROTO2"]}
        if self.drop_vpn_on_add:
            self.table[VPN_IP] = {"interface": "en0", "destination": "default", "flags": []}

    def delete(self, address, _config):
        self.events.append(("delete", address))
        if address == self.fail_delete:
            raise routes.RoutingError("injected_delete_failure")
        del self.table[address]


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.system = FakeSystem()
        self.state = routes.empty_state(CONFIG)
        self.journal = []

    def save(self, value):
        self.journal.append(copy.deepcopy(value))

    def apply(self):
        return routes.reconcile(self.system, CONFIG, self.state, self.save)

    def test_working_provider_routes_are_preserved_and_not_claimed(self):
        self.system.direct_default = True
        self.assertEqual(self.apply()["added"], [])
        self.assertEqual(self.system.events, [])
        self.assertEqual(self.state["owned"], [])

    def test_vpn_up_missing_exceptions_are_added_and_idempotent(self):
        self.assertEqual(set(self.apply()["added"]), {WB_IP, OZON_IP})
        self.assertEqual(self.apply()["added"], [])
        self.assertEqual(self.system.route(VPN_IP)["interface"], "utun4")
        self.assertEqual(len(self.system.events), 2)

    def test_vpn_reconnect_losing_owned_routes_restores_them(self):
        self.apply()
        self.system.table.clear()
        self.assertEqual(set(self.apply()["added"]), {WB_IP, OZON_IP})

    def test_dns_change_removes_only_old_owned_route(self):
        self.apply()
        for host in ("api-seller.ozon.ru", "api-performance.ozon.ru"):
            self.system.dns[host] = [NEW_IP]
        result = self.apply()
        self.assertEqual(result["removed"], [OZON_IP])
        self.assertEqual(result["added"], [NEW_IP])
        self.assertNotIn(OZON_IP, self.state["owned"])

    def test_dns_error_changes_nothing_and_preserves_journal(self):
        self.apply()
        old = copy.deepcopy(self.state)
        events = list(self.system.events)
        self.system.dns[routes.MARKETPLACE_HOSTS[-1]] = socket.gaierror("offline")
        with self.assertRaises(socket.gaierror):
            self.apply()
        self.assertEqual(self.system.events, events)
        self.assertEqual(self.state, old)

    def test_openai_overlap_or_vpn_down_prevents_any_mutation(self):
        for overlap in (True, False):
            with self.subTest(overlap=overlap):
                self.system = FakeSystem()
                if overlap:
                    self.system.dns[routes.VPN_HOSTS[0]] = [WB_IP]
                else:
                    self.system.table[VPN_IP] = {"interface": "en0", "destination": "default", "flags": []}
                with self.assertRaises(routes.RoutingError):
                    self.apply()
                self.assertEqual(self.system.events, [])

    def test_foreign_host_route_is_not_overwritten(self):
        self.system.table[WB_IP] = {"interface": "utun4", "destination": WB_IP, "flags": ["HOST"]}
        with self.assertRaisesRegex(routes.RoutingError, "foreign_host_route"):
            self.apply()
        self.assertEqual(self.system.events, [])

    def test_macos_cloned_host_cache_is_not_a_foreign_static_route(self):
        self.system.table[WB_IP] = {"interface": "utun4", "destination": WB_IP,
                                    "flags": ["HOST", "WASCLONED", "IFSCOPE"]}
        self.assertIn(WB_IP, self.apply()["added"])

    def test_partial_add_failure_rolls_back_only_this_attempt(self):
        self.system.fail_add = WB_IP
        with self.assertRaisesRegex(routes.RoutingError, "injected_add_failure"):
            self.apply()
        self.assertEqual(self.state["owned"], [])
        self.assertEqual(self.system.table, {})
        self.assertIn(("delete", OZON_IP), self.system.events)

    def test_lost_openai_route_after_add_triggers_rollback(self):
        self.system.drop_vpn_on_add = True
        with self.assertRaisesRegex(routes.RoutingError, "openai_vpn_route_missing"):
            self.apply()
        self.assertEqual(self.state["owned"], [])
        self.assertNotIn(WB_IP, self.system.table)

    def test_crash_journal_recovers_marked_route_without_duplication(self):
        self.state["owned"].append(WB_IP)
        self.system.add(WB_IP, CONFIG)
        self.assertEqual(self.apply()["added"], [OZON_IP])
        self.assertEqual(self.state["owned"].count(WB_IP), 1)

    def test_rollback_preserves_routes_replaced_by_vpn(self):
        self.apply()
        self.system.table[WB_IP]["flags"].remove("PROTO2")
        routes.remove_owned(self.system, CONFIG, self.state, self.save, list(self.state["owned"]))
        self.assertIn(WB_IP, self.system.table)
        self.assertNotIn(OZON_IP, self.system.table)
        self.assertEqual(self.state["owned"], [])

    def test_delete_failure_retains_journal_for_retry(self):
        self.apply()
        self.system.fail_delete = OZON_IP
        with self.assertRaises(routes.RoutingError):
            routes.remove_owned(self.system, CONFIG, self.state, self.save, list(self.state["owned"]))
        self.assertIn(OZON_IP, self.state["owned"])

    def test_rollback_handles_owned_route_whose_interface_changed(self):
        self.apply()
        self.system.table[WB_IP]["interface"] = "en1"
        routes.remove_owned(self.system, CONFIG, self.state, self.save, list(self.state["owned"]))
        self.assertEqual(self.system.table, {})

    def test_readback_failure_does_not_leave_a_new_misdirected_route(self):
        original_add = self.system.add

        def misdirect(address, config):
            original_add(address, config)
            self.system.table[address]["interface"] = "en1"

        self.system.add = misdirect
        with self.assertRaisesRegex(routes.RoutingError, "add_readback_failed"):
            self.apply()
        self.assertEqual(self.state["owned"], [])
        self.assertEqual(self.system.table, {})

    def test_changed_gateway_requires_explicit_rollback(self):
        self.state["config"]["gateway"] = "192.168.0.2"
        with self.assertRaisesRegex(routes.RoutingError, "config_changed"):
            self.apply()
        self.assertEqual(self.system.events, [])

    def test_lan_gateway_change_stops_before_writing_old_gateway(self):
        self.system.lan_default = lambda interface: {
            "interface": interface, "gateway": "192.168.0.2", "flags": []}
        with self.assertRaisesRegex(routes.RoutingError, "does_not_match_lan_default"):
            self.apply()
        self.assertEqual(self.system.events, [])

    def test_dns_change_during_probe_does_not_claim_all_addresses_tested(self):
        self.system.direct_default = True

        def probe(_item):
            self.system.dns["api-seller.ozon.ru"] = [NEW_IP]
            return {"http_status": 200}

        with patch.object(routes, "https_probe", side_effect=probe):
            with self.assertRaisesRegex(routes.RoutingError, "dns_changed_during_probe"):
                routes.check(self.system, CONFIG, probes=True)

    def test_config_rejects_network_wide_or_shell_like_inputs(self):
        for patch_value in ({"interface": "utun4"}, {"gateway": "0.0.0.0"},
                            {"gateway": "192.168.0.1;id"}, {"interface": "en0;id"},
                            {"hosts": ["example.com"]}, {"version": True}):
            with self.subTest(patch_value=patch_value):
                with self.assertRaises(routes.RoutingError):
                    routes.config_document(dict(CONFIG, **patch_value))

    def test_dns_rejects_ipv6_private_loopback_and_empty_results(self):
        for values in (["::1"], ["2606:4700::1111"], ["127.0.0.1"], ["10.0.0.1"], []):
            answer = [(0, 0, 0, "", (value, 443)) for value in values]
            with self.subTest(values=values), patch.object(socket, "getaddrinfo", return_value=answer):
                with self.assertRaises(routes.RoutingError):
                    routes.addresses("content-api.wildberries.ru")

    def test_route_command_is_a_single_host_without_shell_or_default_route(self):
        command = routes.route_command("add", WB_IP, CONFIG)
        self.assertEqual(command, ["/sbin/route", "-n", "add", "-inet", "-host", WB_IP,
                                   "192.168.0.1", "-static", "-proto2"])

    def test_check_detects_missing_bypass_without_writes(self):
        with self.assertRaisesRegex(routes.RoutingError, "direct_route_missing"):
            routes.check(self.system, CONFIG)
        self.assertEqual(self.system.events, [])

    def test_openai_403_is_not_a_successful_probe(self):
        with patch.object(routes, "run", return_value="403 0.1 0.2"):
            with self.assertRaisesRegex(routes.RoutingError, "expected_401"):
                routes.https_probe(("api.openai.com", VPN_IP))

    def test_https_uses_verified_tls_pinned_dns_and_no_ambient_proxy(self):
        with patch.object(routes, "run", return_value="401 0.1 0.2") as command:
            result = routes.https_probe(("content-api.wildberries.ru", WB_IP))
        argv = command.call_args[0][0]
        self.assertIn("--resolve", argv)
        self.assertIn("--noproxy", argv)
        self.assertNotIn("--insecure", argv)
        self.assertNotIn("--location", argv)
        self.assertEqual(result["http_status"], 401)


if __name__ == "__main__":
    unittest.main()
