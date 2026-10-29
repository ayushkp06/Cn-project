"""
tracker.py - central tracker for the P2P file distribution system.

The tracker NEVER stores file data. It only remembers:
    file_id -> metadata  +  which peer (ip:port) has which chunks

Usage:
    python tracker.py --port 9000
"""

import argparse
import socket
import threading
import time

from common import recv_msg, send_msg


class Tracker:
    def __init__(self, verbose=False):
        self.files = {}            # file_id -> {"meta": {...}, "peers": {peer_id: set(chunks)}}
        self.lock = threading.Lock()
        self.verbose = verbose

    def log(self, text):
        print(f"[{time.strftime('%H:%M:%S')}] {text}", flush=True)

    # ------------------------------------------------------------ protocol
    def process(self, msg, client_ip):
        kind = msg.get("type")
        with self.lock:
            if kind == "REGISTER":
                # a peer announces it is sharing (or about to download) a file
                peer_id = f"{msg.get('host') or client_ip}:{msg['port']}"
                meta = msg["meta"]
                entry = self.files.setdefault(meta["file_id"], {"meta": meta, "peers": {}})
                entry["peers"][peer_id] = set(msg.get("chunks", []))
                self.log(f"REGISTER {peer_id} file={meta['name']} "
                         f"chunks={len(entry['peers'][peer_id])}/{meta['num_chunks']}")
                return {"type": "OK", "peer_id": peer_id}

            if kind == "ANNOUNCE":
                # a downloader got a new chunk and can now seed it
                entry = self.files.get(msg["file_id"])
                if not entry:
                    return {"type": "ERROR", "reason": "unknown file"}
                entry["peers"].setdefault(msg["peer_id"], set()).add(msg["chunk"])
                if self.verbose:
                    self.log(f"ANNOUNCE {msg['peer_id']} has chunk {msg['chunk']}")
                return {"type": "OK"}

            if kind == "GET_PEERS":
                entry = self.files.get(msg["file_id"])
                if not entry:
                    return {"type": "ERROR", "reason": "unknown file"}
                return {"type": "PEERS", "meta": entry["meta"],
                        "peers": {p: sorted(c) for p, c in entry["peers"].items()}}

            if kind == "LIST":
                return {"type": "FILES", "files": [
                    {"file_id": fid, "name": e["meta"]["name"], "size": e["meta"]["size"],
                     "num_chunks": e["meta"]["num_chunks"], "peers": len(e["peers"])}
                    for fid, e in self.files.items()]}

            if kind in ("LEAVE", "REPORT_DEAD"):
                # LEAVE: graceful exit. REPORT_DEAD: another peer could not reach it.
                peer_id = msg["peer_id"]
                for entry in self.files.values():
                    entry["peers"].pop(peer_id, None)
                self.log(f"{kind} {peer_id}")
                return {"type": "OK"}

        return {"type": "ERROR", "reason": f"unknown message type {kind!r}"}

    # -------------------------------------------------------------- server
    def handle(self, conn, addr):
        with conn:
            try:
                msg, _ = recv_msg(conn)
                send_msg(conn, self.process(msg, addr[0]))
            except (OSError, ConnectionError, ValueError, KeyError) as e:
                self.log(f"bad request from {addr}: {e}")

    def serve(self, port):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", port))
        srv.listen(64)
        self.log(f"Tracker listening on TCP port {port}")
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self.handle, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="P2P tracker")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--verbose", action="store_true", help="log every ANNOUNCE")
    args = ap.parse_args()
    try:
        Tracker(args.verbose).serve(args.port)
    except KeyboardInterrupt:
        print("\nTracker stopped.")
