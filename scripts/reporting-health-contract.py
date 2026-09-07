#!/usr/bin/env python3
"""Read only reporting scope metadata; never load marketplace credentials."""

import json
import os
import re
import stat
import sys

MAX_BYTES = 1024 * 1024
IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")


def require(condition):
    if not condition:
        raise ValueError("invalid reporting health contract")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


def load_document(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as source:
        require(stat.S_ISREG(os.fstat(source.fileno()).st_mode))
        data = source.read(MAX_BYTES + 1)
    require(len(data) <= MAX_BYTES)
    return json.loads(data, object_pairs_hook=unique_object)


def identifiers(values):
    require(isinstance(values, list) and 1 <= len(values) <= 64)
    require(all(isinstance(value, str) and IDENTIFIER.fullmatch(value) for value in values))
    require(len(values) == len(set(values)))
    return values


def build_scope(policy, registry):
    require(isinstance(policy, dict) and isinstance(registry, dict))
    require(type(policy.get("version")) is int and policy["version"] == 1)
    require(type(policy.get("enabled")) is bool)
    require(policy.get("timezone") == "Asia/Yekaterinburg")
    require(type(registry.get("version")) is int and registry["version"] == 1)
    require(isinstance(registry.get("accounts"), list))
    accounts = {}
    for account in registry["accounts"]:
        require(isinstance(account, dict))
        account_id = account.get("id")
        identifiers([account_id])
        require(account_id not in accounts)
        require(account.get("marketplace") in ("ozon", "wildberries"))
        accounts[account_id] = account

    if "account_ids" in policy:
        require(set(policy) == {"version", "enabled", "timezone", "account_ids"})
        selected = identifiers(policy["account_ids"])
    else:
        require(set(policy) == {"version", "enabled", "timezone", "sender_email_env", "audiences"})
        audiences = policy["audiences"]
        require(isinstance(audiences, list) and 1 <= len(audiences) <= 64)
        selected = []
        for audience in audiences:
            require(isinstance(audience, dict) and set(audience) == {"id", "email_env", "managers"})
            managers = audience["managers"]
            require(isinstance(managers, list) and 1 <= len(managers) <= 64)
            for manager in managers:
                require(isinstance(manager, dict) and set(manager) == {"actor_id", "account_ids"})
                identifiers([manager["actor_id"]])
                for account_id in identifiers(manager["account_ids"]):
                    require(account_id in accounts)
                    require(accounts[account_id].get("manager_id") == manager["actor_id"])
                    selected.append(account_id)
        identifiers(selected)
    require(all(account_id in accounts for account_id in selected))
    if not policy["enabled"]:
        return []
    return [
        {"account_id": account_id, "marketplace": accounts[account_id]["marketplace"]}
        for account_id in sorted(selected)
    ]


def main():
    try:
        require(len(sys.argv) == 3)
        scope = build_scope(load_document(sys.argv[1]), load_document(sys.argv[2]))
    except (OSError, ValueError, TypeError, KeyError, RecursionError):
        # Do not echo either input, which may include private registry fields.
        print("reporting health policy or registry is unavailable or invalid", file=sys.stderr)
        return 2
    print(json.dumps(scope, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
