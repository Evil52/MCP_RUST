#!/usr/bin/env python3
"""Install the IP Usovik Promotion writer from a private env file as a 0600 secret."""

import argparse
import base64
import binascii
import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path

KEY = "IP_USOVIK_WB_PROMOTION_WRITE_TOKEN"
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV = ROOT / ".env.ip-usovik-promotion"
DEFAULT_OUTPUT = Path.home() / ".local/share/mcp-ozon-runtime/ip-usovik-wb-promotion-write.token"


def read_token(path: Path) -> str:
    if path.stat().st_mode & 0o077:
        raise ValueError("env-файл должен иметь права 0600 или 0400")
    entries = [line.split("=", 1)[1] for line in path.read_text().splitlines()
               if line.startswith(KEY + "=")]
    if len(entries) != 1 or not entries[0]:
        raise ValueError(f"ожидается одна непустая строка {KEY}=...")
    token = entries[0]
    if len(token) > 16384 or not token.isascii() or not re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", token):
        raise ValueError("ключ должен быть JWT без кавычек, пробелов и переносов")
    return token


def validate_token(token: str) -> None:
    payload = token.split(".")[1]
    try:
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    except (ValueError, UnicodeDecodeError, binascii.Error) as exc:
        raise ValueError("не удалось прочитать JWT") from exc
    if not isinstance(claims, dict):
        raise ValueError("JWT должен содержать объект claims")
    if (claims.get("acc") != 3 or claims.get("for") != "self"
            or claims.get("t") is not False or claims.get("s") != 1 << 6
            or not isinstance(claims.get("exp"), int)
            or claims["exp"] <= time.time() + 300
            or not isinstance(claims.get("sid"), str)
            or not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", claims["sid"])):
        raise ValueError("нужен действующий Personal JWT только с правом Продвижение: чтение и запись")


def install(token: str, output: Path) -> None:
    if not output.parent.is_dir():
        raise ValueError("каталог назначения отсутствует")
    if output.exists() or output.is_symlink():
        raise ValueError("файл ключа уже существует; автоматическая замена запрещена")
    fd, temporary = tempfile.mkstemp(prefix=".wb-promotion-write-", dir=output.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="ascii") as stream:
            stream.write(token + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, output)  # atomic create; never overwrite an existing secret
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    try:
        token = read_token(args.env_file)
        validate_token(token)
        install(token, args.output)
    except (OSError, ValueError) as exc:
        print(f"Ключ не установлен: {exc}", file=sys.stderr)
        return 1
    print(f"Файл ключа создан с правами 0600: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
