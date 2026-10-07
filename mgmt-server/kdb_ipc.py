"""
kdb+ IPC client — little-endian wire protocol (kdb+ 3.x)

Message header (8 bytes, little-endian):
  [0]   endianness  = 1 (LE)
  [1]   msg_type    = 1 (sync) | 0 (async)
  [2]   compression = 0
  [3]   reserved    = 0
  [4:8] total_len   u32 LE  (includes 8-byte header)

Wire encoding for a table (type 98 / 0x62):
  [98][0][sym_vec of col names][mixed_list of col arrays]
  (kdb+ wraps this as an inner dict-99 in its own serialiser)

Wire encoding for a dict (type 99 / 0x63):
  [99][key_object][value_object]
  A keyed table is dict(table, table).
"""

import socket
import struct
from typing import Any, Optional


class KdbIPCClient:
    def __init__(self, host: str = "localhost", port: int = 5001, timeout: float = 10.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None

    def connect(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(self.timeout)
            self.sock.connect((self.host, self.port))
            # kdb+ capability handshake
            self.sock.sendall(b":\x03\x00")
            cap = self.sock.recv(1)
            if not cap:
                raise ConnectionError("kdb+ closed connection during handshake")
        except Exception as e:
            self.close()
            raise ConnectionError(f"Failed to connect to kdb+ at {self.host}:{self.port}: {e}")

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    # ── public ────────────────────────────────────────────────────────────────

    def execute(self, query: str) -> Any:
        if not self.sock:
            self.connect()
        try:
            self._send_sync(query)
            return self._read_response()
        except socket.timeout:
            raise TimeoutError(f"Timeout waiting for kdb+ response")
        except Exception as e:
            raise RuntimeError(f"kdb+ query failed: {e}")

    # ── send ──────────────────────────────────────────────────────────────────

    def _send_sync(self, query: str):
        qb = query.encode("utf-8")
        n = len(qb)
        total = 8 + 1 + 1 + 4 + n
        hdr = struct.pack("<BBBBI", 1, 1, 0, 0, total)
        payload = struct.pack("<BBi", 10, 0, n) + qb
        self.sock.sendall(hdr + payload)

    # ── receive ───────────────────────────────────────────────────────────────

    def _read_response(self) -> Any:
        hdr = self._recv_exact(8)
        total_len = struct.unpack_from("<I", hdr, 4)[0]
        body = self._recv_exact(total_len - 8)
        val, _ = self._deserialize(memoryview(body), 0)
        return val

    # ── deserialise ───────────────────────────────────────────────────────────

    def _deserialize(self, buf: memoryview, pos: int):
        t = buf[pos]; pos += 1

        # ── kdb+ error ────────────────────────────────────────────────────────
        if t == 128:
            end = bytes(buf[pos:]).index(b'\x00')
            raise RuntimeError(f"kdb+ error: {bytes(buf[pos:pos+end]).decode()}")

        # ── atoms ─────────────────────────────────────────────────────────────
        if t == 0xFF:  return bool(buf[pos]),                          pos + 1
        if t == 0xFB:  return struct.unpack_from("<h", buf, pos)[0],   pos + 2
        if t == 0xFA:  return struct.unpack_from("<i", buf, pos)[0],   pos + 4
        if t == 0xF9:  return struct.unpack_from("<q", buf, pos)[0],   pos + 8
        if t == 0xF8:  return struct.unpack_from("<f", buf, pos)[0],   pos + 4
        if t == 0xF7:  return struct.unpack_from("<d", buf, pos)[0],   pos + 8
        if t == 0xF6:  return chr(buf[pos]),                           pos + 1
        if t == 0xF5:  # symbol atom
            end = bytes(buf[pos:]).index(b'\x00')
            return bytes(buf[pos:pos+end]).decode("utf-8", errors="replace"), pos+end+1

        # ── table (flip) — type 98 ────────────────────────────────────────────
        # Wire: [98][0-pad][col_names: sym_vec][col_arrays: mixed_list]
        # kdb+ nests col_names+col_arrays inside an inner dict-99 marker
        if t == 98:
            pos += 1  # skip pad byte (0x00)
            inner = buf[pos]; pos += 1  # should be 99 (inner dict marker)
            col_names, pos = self._deserialize(buf, pos)   # sym vec
            col_arrays, pos = self._deserialize(buf, pos)  # mixed list
            return self._flip_to_rows(col_names, col_arrays), pos

        # ── dict — type 99 ────────────────────────────────────────────────────
        if t == 99:
            keys, pos = self._deserialize(buf, pos)
            vals, pos = self._deserialize(buf, pos)
            # Keyed table: both are row-lists produced by type-98 above
            if (isinstance(keys, list) and keys and isinstance(keys[0], dict) and
                    isinstance(vals, list) and vals and isinstance(vals[0], dict)):
                return [{**k, **v} for k, v in zip(keys, vals)], pos
            # Plain dict
            if isinstance(keys, list) and isinstance(vals, list):
                return dict(zip(keys, vals)), pos
            return {"keys": keys, "values": vals}, pos

        # ── vectors ───────────────────────────────────────────────────────────
        _attrs = buf[pos]; pos += 1
        count  = struct.unpack_from("<i", buf, pos)[0]; pos += 4

        if t == 1:   # bool vec
            v = [bool(buf[pos+i]) for i in range(count)]; return v, pos+count
        if t == 5:   # short vec
            v = list(struct.unpack_from(f"<{count}h", buf, pos)); return v, pos+count*2
        if t == 6:   # int vec
            v = list(struct.unpack_from(f"<{count}i", buf, pos)); return v, pos+count*4
        if t == 7:   # long vec
            v = list(struct.unpack_from(f"<{count}q", buf, pos)); return v, pos+count*8
        if t == 8:   # real vec
            v = list(struct.unpack_from(f"<{count}f", buf, pos)); return v, pos+count*4
        if t == 9:   # float vec
            v = list(struct.unpack_from(f"<{count}d", buf, pos)); return v, pos+count*8
        if t == 10:  # char vec (string)
            s = bytes(buf[pos:pos+count]).decode("utf-8", errors="replace"); return s, pos+count
        if t == 11:  # symbol vec
            syms = []; raw = bytes(buf[pos:])
            for _ in range(count):
                end = raw.index(b'\x00')
                syms.append(raw[:end].decode("utf-8", errors="replace"))
                raw = raw[end+1:]; pos += end+1
            return syms, pos
        if t == 14:  # date vec (days since 2000-01-01)
            v = list(struct.unpack_from(f"<{count}i", buf, pos)); return v, pos+count*4
        if t == 12:  # timestamp vec
            v = list(struct.unpack_from(f"<{count}q", buf, pos)); return v, pos+count*8
        if t == 19:  # time vec (ms since midnight)
            v = list(struct.unpack_from(f"<{count}i", buf, pos)); return v, pos+count*4

        # ── mixed list — type 0 ───────────────────────────────────────────────
        if t == 0:
            items = []
            for _ in range(count):
                item, pos = self._deserialize(buf, pos)
                items.append(item)
            return items, pos

        # ── fallback ──────────────────────────────────────────────────────────
        return bytes(buf[pos:]), pos

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _flip_to_rows(col_names, col_arrays):
        if not (isinstance(col_names, list) and isinstance(col_arrays, list)):
            return []
        if not col_arrays:
            return []
        # strings are kdb+ char vectors — treat each character as a row value
        normalized = [list(cd) if isinstance(cd, str) else cd for cd in col_arrays]
        first = normalized[0]
        nrows = len(first) if isinstance(first, list) else 1
        rows = []
        for i in range(nrows):
            row = {}
            for cn, cd in zip(col_names, normalized):
                row[cn] = cd[i] if isinstance(cd, list) else cd
            rows.append(row)
        return rows

    def _recv_exact(self, n: int) -> bytes:
        data = bytearray()
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError(f"Connection closed after {len(data)}/{n} bytes")
            data += chunk
        return bytes(data)

    def __enter__(self):
        self.connect(); return self

    def __exit__(self, *_):
        self.close()
