"""
common.py - helpers shared by the tracker and the peers.

Message format (our application-layer protocol, carried over TCP):

    +----------------+----------------+-------------------+------------------+
    | header length  | payload length | JSON header       | binary payload   |
    | 4 bytes (uint) | 4 bytes (uint) | (header length B) | (payload len B)  |
    +----------------+----------------+-------------------+------------------+

Both lengths are in network byte order (big-endian). TCP is a byte stream,
so the length prefix tells the receiver where one message ends and the next
begins (message framing). The JSON header is human-readable, which makes the
protocol easy to verify in Wireshark ("Follow TCP Stream").
"""

import hashlib
import json
import os
import socket
import struct
import zlib

CHUNK_SIZE = 64 * 1024          # 64 KB per chunk
FRAME_FMT = "!II"               # header length, payload length
FRAME_LEN = struct.calcsize(FRAME_FMT)


# ---------------------------------------------------------------- framing
def send_msg(sock, header, payload=b""):
    h = json.dumps(header).encode("utf-8")
    sock.sendall(struct.pack(FRAME_FMT, len(h), len(payload)) + h + payload)


def recv_exact(sock, n):
    """Read exactly n bytes (recv() may return fewer than asked)."""
    buf = bytearray()
    while len(buf) < n:
        part = sock.recv(n - len(buf))
        if not part:
            raise ConnectionError("connection closed by remote side")
        buf += part
    return bytes(buf)


def recv_msg(sock):
    hlen, plen = struct.unpack(FRAME_FMT, recv_exact(sock, FRAME_LEN))
    header = json.loads(recv_exact(sock, hlen).decode("utf-8"))
    payload = recv_exact(sock, plen) if plen else b""
    return header, payload


# -------------------------------------------------------- error detection
def crc32(data):
    """CRC-32 of a chunk (same polynomial as Ethernet, Unit II)."""
    return zlib.crc32(data) & 0xFFFFFFFF


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def build_metadata(path, chunk_size=CHUNK_SIZE):
    """Split a file into fixed-size chunks and describe it (like a .torrent)."""
    crcs, sha, size = [], hashlib.sha256(), 0
    with open(path, "rb") as f:
        while True:
            block = f.read(chunk_size)
            if not block:
                break
            crcs.append(crc32(block))
            sha.update(block)
            size += len(block)
    digest = sha.hexdigest()
    return {
        "file_id": digest[:16],
        "name": os.path.basename(path),
        "size": size,
        "chunk_size": chunk_size,
        "num_chunks": len(crcs),
        "chunk_crc32": crcs,
        "sha256": digest,
    }


# ------------------------------------------------------------- tracker I/O
def parse_addr(text, default_port=9000):
    host, _, port = text.partition(":")
    return host or "127.0.0.1", int(port or default_port)


def tracker_request(tracker_addr, header, timeout=5):
    """One request/response exchange with the tracker (new TCP connection)."""
    with socket.create_connection(tracker_addr, timeout=timeout) as s:
        send_msg(s, header)
        reply, _ = recv_msg(s)
        return reply
