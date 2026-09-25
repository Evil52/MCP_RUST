#!/usr/bin/env python3
"""Install one shop's WB Promotion writer from the shared private .env."""

import argparse
import base64
import binascii
import json
import os
import re
import stat
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ACCOUNT = re.compile(r"[a-z][a-z0-9_]{1,63}\Z")
SID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")


def value(path: Path, key: str) -> str:
    entries = [line.split("=", 1)[1] for line in path.read_text().splitlines()
               if line.startswith(key + "=")]
    if len(entries) != 1 or not entries[0]:
        raise ValueError(f"ожидается одна непустая строка {key}=...")
    token = entries[0]
    if (len(token) > 16_384 or not token.isascii()
            or not re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", token)):
        raise ValueError(f"{key}: нужен JWT без кавычек и пробелов")
    return token


def claims(token: str) -> dict:
    payload = token.split(".")[1]
    try:
        result = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    except (ValueError, UnicodeDecodeError, binascii.Error) as exc:
        raise ValueError("не удалось прочитать JWT") from exc
    if not isinstance(result, dict):
        raise ValueError("JWT должен содержать объект claims")
    if (result.get("acc") != 3 or result.get("for") != "self"
            or result.get("t") is not False or not isinstance(result.get("exp"), int)
            or result["exp"] <= time.time() + 300
            or not isinstance(result.get("sid"), str) or not SID.fullmatch(result["sid"])):
        raise ValueError("нужен действующий Personal JWT продавца")
    return result


def account_from_registry(path: Path, account_id: str) -> dict:
    registry = json.loads(path.read_text())
    matches = [account for account in registry.get("accounts", [])
               if account.get("id") == account_id]
    if len(matches) != 1 or matches[0].get("marketplace") != "wildberries":
        raise ValueError("кабинет WB отсутствует в access registry")
    return matches[0]


def install(token: str, output: Path) -> str:
    if not output.parent.is_dir() or output.parent.stat().st_mode & 0o077:
        raise ValueError("каталог ключей отсутствует или доступен другим пользователям")
    if output.is_symlink():
        raise ValueError("символическая ссылка вместо файла ключа")
    if output.exists():
        if not output.is_file() or output.stat().st_mode & 0o077:
            raise ValueError("существующий файл ключа имеет небезопасные права")
        if output.read_text().strip() != token:
            raise ValueError("другой ключ уже установлен; замена требует отдельной процедуры")
        return "уже установлен"
    fd, temporary = tempfile.mkstemp(prefix=".wb-promotion-write-", dir=output.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="ascii") as stream:
            stream.write(token + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, output)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return "создан"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", required=True, help="id WB-кабинета из access registry")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--registry", type=Path, default=ROOT / "config/access.json")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        if not ACCOUNT.fullmatch(args.account):
            raise ValueError("недопустимый id кабинета")
        if args.env_file.is_symlink() or stat.S_IMODE(args.env_file.stat().st_mode) & 0o077:
            raise ValueError("общий .env должен быть обычным приватным файлом 0600/0400")
        account = account_from_registry(args.registry, args.account)
        prefix = args.account.upper()
        reader = value(args.env_file, prefix + "_API_TOKEN")
        writer = value(args.env_file, prefix + "_PROMOTION_WRITE_TOKEN")
        r, w = claims(reader), claims(writer)
        if r["s"] & ((1 << 6) | (1 << 30)) != ((1 << 6) | (1 << 30)):
            raise ValueError("ключ чтения не имеет прав Promotion/read-only")
        if w["s"] != 1 << 6 or w["sid"] != r["sid"]:
            raise ValueError("ключ записи должен иметь только Promotion/read-write того же продавца")
        bound_sid = (account.get("wildberries") or {}).get("seller_sid")
        if bound_sid and bound_sid != w["sid"]:
            raise ValueError("seller SID не совпадает с access registry")
        output = args.output or (Path.home() / ".local/share/mcp-ozon-runtime"
                                 / (args.account.replace("_", "-") + "-promotion-write.token"))
        result = install(writer, output)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f"Ключ не установлен: {exc}", file=sys.stderr)
        return 1
    print(f"Файл ключа {result}: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
