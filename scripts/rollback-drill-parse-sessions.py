#!/usr/bin/env python3
"""Stdlib [[sessions]] parser for the UAT rollback drill. No tomllib."""
from __future__ import annotations

import json
import re
import sys
from typing import Iterable, List, Optional, Tuple

# stderr on fail: "{code}\t{code}" (logical LLD §5.5). Exit 0 ok, 2 parse/usage, 1 assert fail.


def _fail(code: str, message: str, exit_code: int = 2) -> None:
    sys.stderr.write(f"{code}\t{code}\n")
    sys.stderr.write(f"ERROR: {message}\n")
    raise SystemExit(exit_code)


def strip_comment(line: str) -> str:
    """Strip an unquoted # comment. Quotes are " or '."""
    out: List[str] = []
    quote: Optional[str] = None
    i = 0
    while i < len(line):
        ch = line[i]
        if quote:
            out.append(ch)
            if ch == quote and (i == 0 or line[i - 1] != "\\"):
                quote = None
            i += 1
            continue
        if ch in ("'", '"'):
            quote = ch
            out.append(ch)
            i += 1
            continue
        if ch == "#":
            break
        out.append(ch)
        i += 1
    return "".join(out).rstrip()


_KV = re.compile(r"^([A-Za-z0-9_]+)\s*=\s*(.*)$")
_TABLE = re.compile(r"^\[\[([A-Za-z0-9_]+)\]\]$")
_STD_TABLE = re.compile(r"^\[([^\]]+)\]$")


def _unquote(raw: str) -> str:
    s = raw.strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        return s[1:-1]
    return s


def iter_session_tables(text: str) -> List[List[str]]:
    """Return each [[sessions]] table as a list of original lines (including header)."""
    sessions: List[List[str]] = []
    current: Optional[List[str]] = None
    for line in text.splitlines(keepends=True):
        stripped = strip_comment(line).strip()
        m_arr = _TABLE.match(stripped)
        m_std = _STD_TABLE.match(stripped) if not m_arr else None
        if m_arr and m_arr.group(1) == "sessions":
            current = [line]
            sessions.append(current)
            continue
        if m_arr or (m_std and not stripped.startswith("[[")):
            current = None
            continue
        if current is not None:
            current.append(line)
    return sessions


def _header_and_rest(text: str) -> Tuple[str, str]:
    """Everything before the first [[sessions]] , plus non-session tables after."""
    lines = text.splitlines(keepends=True)
    header: List[str] = []
    rest: List[str] = []
    seen_session = False
    in_session = False
    for line in lines:
        stripped = strip_comment(line).strip()
        m_arr = _TABLE.match(stripped)
        m_std = _STD_TABLE.match(stripped) if not m_arr else None
        if m_arr and m_arr.group(1) == "sessions":
            seen_session = True
            in_session = True
            continue
        if in_session and (m_arr or (m_std and not stripped.startswith("[["))):
            in_session = False
        if in_session:
            continue
        if not seen_session:
            header.append(line)
        else:
            rest.append(line)
    return "".join(header), "".join(rest)


def _session_kv(lines: Iterable[str]) -> dict:
    kv = {}
    for line in lines:
        stripped = strip_comment(line).strip()
        m = _KV.match(stripped)
        if not m:
            continue
        kv[m.group(1)] = _unquote(m.group(2))
    return kv


def parse_sessions_identity(text: str) -> dict:
    tables = iter_session_tables(text)
    sender: List[str] = []
    hosts: List[str] = []
    creds: List[str] = []
    seen_s, seen_h, seen_c = set(), set(), set()
    for tbl in tables:
        kv = _session_kv(tbl)
        sc = kv.get("sender_comp_id")
        if sc and sc not in seen_s:
            seen_s.add(sc)
            sender.append(sc)
        host = kv.get("host")
        if host and host not in seen_h:
            seen_h.add(host)
            hosts.append(host)
        cr = kv.get("credentials_ref")
        if cr and cr not in seen_c:
            seen_c.add(cr)
            creds.append(cr)
    return {
        "n_sessions": len(tables),
        "sender_comp_ids": sender,
        "hosts": hosts,
        "credentials_refs": creds,
    }


def rewrite_case1(text: str) -> str:
    """HFT-042 empty sessions: `sessions = []`. Other tables preserved. Not a key strip."""
    header, rest = _header_and_rest(text)
    if header and not header.endswith("\n"):
        header += "\n"
    block = "sessions = []\n"
    if rest and not rest.startswith("\n") and not block.endswith("\n"):
        block += "\n"
    return header + block + rest


def rewrite_case3(text: str) -> str:
    """Every [[sessions]] host=127.0.0.1 port=1 only. No other host rewrite."""
    lines = text.splitlines(keepends=True)
    out: List[str] = []
    in_session = False
    seen_host = False
    seen_port = False

    def flush_missing(indent: str = "") -> None:
        nonlocal seen_host, seen_port
        if in_session:
            if not seen_host:
                out.append(f'{indent}host             = "127.0.0.1"\n')
            if not seen_port:
                out.append(f"{indent}port             = 1\n")

    for line in lines:
        stripped = strip_comment(line).strip()
        m_arr = _TABLE.match(stripped)
        m_std = _STD_TABLE.match(stripped) if not m_arr else None
        if m_arr and m_arr.group(1) == "sessions":
            flush_missing()
            in_session = True
            seen_host = False
            seen_port = False
            out.append(line)
            continue
        if in_session and (m_arr or (m_std and not stripped.startswith("[["))):
            flush_missing()
            in_session = False
            out.append(line)
            continue
        if in_session:
            m = _KV.match(stripped)
            indent = line[: len(line) - len(line.lstrip())]
            if m and m.group(1) == "host":
                out.append(f'{indent}host             = "127.0.0.1"\n')
                seen_host = True
                continue
            if m and m.group(1) == "port":
                out.append(f"{indent}port             = 1\n")
                seen_port = True
                continue
        out.append(line)
    if in_session:
        flush_missing()
    return "".join(out)


def assert_case3(text: str) -> None:
    tables = iter_session_tables(text)
    if not tables:
        _fail("identity_n_sessions", "assert-case3: no [[sessions]] tables", 1)
    for tbl in tables:
        kv = _session_kv(tbl)
        if kv.get("host") != "127.0.0.1" or kv.get("port") != "1":
            _fail("identity_fix_host", "assert-case3: session is not host=127.0.0.1 port=1", 1)


def _read(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()
    except OSError as exc:
        _fail("usage", f"cannot read {path}: {exc.__class__.__name__}")
        raise  # pragma: no cover


def _usage() -> None:
    _fail(
        "usage",
        "usage: rollback-drill-parse-sessions.py "
        "{identity|rewrite-case1|rewrite-case3|--assert-case3|--config} PATH",
    )


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        _usage()
    cmd = argv[0]
    # Logical flags and physical subcommands.
    if cmd in ("--help", "-h"):
        sys.stdout.write(
            "rollback-drill-parse-sessions.py identity|rewrite-case1|rewrite-case3 PATH\n"
        )
        return 0
    path: Optional[str] = None
    mode: Optional[str] = None
    if cmd == "--config":
        mode = "identity"
        path = argv[1] if len(argv) > 1 else None
    elif cmd == "--rewrite-case1":
        mode = "rewrite-case1"
        path = argv[1] if len(argv) > 1 else None
    elif cmd == "--rewrite-case3":
        mode = "rewrite-case3"
        path = argv[1] if len(argv) > 1 else None
    elif cmd == "--assert-case3":
        mode = "assert-case3"
        path = argv[1] if len(argv) > 1 else None
    elif cmd in ("identity", "rewrite-case1", "rewrite-case3"):
        mode = cmd
        path = argv[1] if len(argv) > 1 else None
    else:
        _fail("usage", f"unknown command: {cmd}")
    if not path:
        _usage()
    text = _read(path)
    if mode == "identity":
        ident = parse_sessions_identity(text)
        sys.stdout.write(json.dumps(ident, separators=(",", ":")) + "\n")
        return 0
    if mode == "rewrite-case1":
        sys.stdout.write(rewrite_case1(text))
        if not rewrite_case1(text).endswith("\n"):
            sys.stdout.write("\n")
        return 0
    if mode == "rewrite-case3":
        sys.stdout.write(rewrite_case3(text))
        return 0
    if mode == "assert-case3":
        assert_case3(text)
        return 0
    _usage()
    return 2


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except BrokenPipeError:
        raise SystemExit(0)
