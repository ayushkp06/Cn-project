"""
peer.py - a peer in the P2P file distribution system.

Every peer is BOTH a client (downloads chunks from other peers) and a
server (uploads chunks it already has). As soon as a downloader receives a
chunk, it tells the tracker, and other peers can fetch that chunk from it.

Usage:
    python peer.py seed     <file>     --port 9001
    python peer.py download <file_id>  --port 9002 --out downloads
    python peer.py list

Common options:
    --tracker 127.0.0.1:9000   address of the tracker
    --host <ip>                IP other peers should use to reach me (LAN demos)
    --upload-kbps N            limit my upload speed (emulates a slow link)
    --corrupt-rate R           (testing) corrupt R fraction of chunks I send
"""

import argparse
import json
import os
import random
import socket
import statistics
import sys
import threading
import time

from common import (build_metadata, crc32, parse_addr, recv_msg, send_msg,
                    sha256_file, tracker_request)

MAX_CRC_FAILURES = 3      # after this many corrupt chunks a peer is blacklisted
STALL_TIMEOUT = 20        # seconds without progress -> give up


class Peer:
    def __init__(self, port, tracker, host=None, upload_kbps=0, corrupt_rate=0.0):
        self.port = port
        self.tracker = tracker
        self.host = host
        self.upload_kbps = upload_kbps
        self.corrupt_rate = corrupt_rate
        self.files = {}                 # file_id -> {"meta", "path", "have": set()}
        self.io_lock = threading.Lock()
        self.upload_lock = threading.Lock()
        self.peer_id = None

    # =========================================================== upload side
    def start_server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.port))
        srv.listen(32)
        threading.Thread(target=self._accept_loop, args=(srv,), daemon=True).start()
        print(f"[peer] serving chunks on TCP port {self.port}", flush=True)

    def _accept_loop(self, srv):
        while True:
            conn, _ = srv.accept()
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn):
        """Answer GET_CHUNK requests on one persistent TCP connection."""
        with conn:
            try:
                while True:
                    msg, _ = recv_msg(conn)
                    fid, idx = msg.get("file_id"), msg.get("index")
                    entry = self.files.get(fid)
                    if msg.get("type") != "GET_CHUNK" or not entry or idx not in entry["have"]:
                        send_msg(conn, {"type": "NOT_FOUND", "file_id": fid, "index": idx})
                        continue
                    data = self.read_chunk(fid, idx)
                    checksum = crc32(data)                 # CRC of the CORRECT data
                    if self.upload_kbps:                   # emulate limited uplink bandwidth
                        with self.upload_lock:
                            time.sleep(len(data) / (self.upload_kbps * 1024))
                    if self.corrupt_rate and random.random() < self.corrupt_rate:
                        data = bytes([data[0] ^ 0xFF]) + data[1:]   # flip bits in transit
                    send_msg(conn, {"type": "CHUNK", "file_id": fid, "index": idx,
                                    "crc32": checksum}, data)
            except (OSError, ConnectionError, ValueError):
                pass

    def read_chunk(self, fid, idx):
        e = self.files[fid]
        cs = e["meta"]["chunk_size"]
        with self.io_lock, open(e["path"], "rb") as f:
            f.seek(idx * cs)
            return f.read(cs)

    def write_chunk(self, fid, idx, data):
        e = self.files[fid]
        with self.io_lock, open(e["path"], "r+b") as f:
            f.seek(idx * e["meta"]["chunk_size"])
            f.write(data)

    # ============================================================ commands
    def register(self, meta, chunks):
        reply = tracker_request(self.tracker, {"type": "REGISTER", "port": self.port,
                                               "host": self.host, "meta": meta,
                                               "chunks": sorted(chunks)})
        self.peer_id = reply["peer_id"]

    def leave(self):
        if self.peer_id:
            try:
                tracker_request(self.tracker, {"type": "LEAVE", "peer_id": self.peer_id})
            except OSError:
                pass

    def seed(self, path):
        meta = build_metadata(path)
        self.files[meta["file_id"]] = {"meta": meta, "path": path,
                                       "have": set(range(meta["num_chunks"]))}
        self.register(meta, range(meta["num_chunks"]))
        print(f"[peer] seeding '{meta['name']}'  size={meta['size']} B  "
              f"chunks={meta['num_chunks']}", flush=True)
        print(f"[peer] FILE ID = {meta['file_id']}   (give this to downloaders)", flush=True)

    def download(self, file_id, out_dir):
        info = tracker_request(self.tracker, {"type": "GET_PEERS", "file_id": file_id})
        if info.get("type") != "PEERS":
            raise RuntimeError(f"tracker: {info.get('reason')}")
        meta = info["meta"]
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, meta["name"])
        with open(path, "wb") as f:              # pre-allocate the output file
            f.truncate(meta["size"])
        self.files[file_id] = {"meta": meta, "path": path, "have": set()}
        self.register(meta, [])                  # join the swarm with 0 chunks
        print(f"[peer] downloading '{meta['name']}' ({meta['num_chunks']} chunks) "
              f"from {len(info['peers'])} peer(s)", flush=True)
        return Downloader(self, meta, path).run()


class Downloader:
    """Downloads one file from many peers in parallel (one thread per peer)."""

    def __init__(self, peer, meta, path):
        self.peer, self.meta, self.path = peer, meta, path
        self.fid, self.n = meta["file_id"], meta["num_chunks"]
        self.have = peer.files[self.fid]["have"]
        self.in_flight = set()
        self.peer_chunks = {}          # peer_id -> set of chunks it has
        self.dead = set()              # peers that crashed or sent bad data
        self.crc_failures = {}
        self.workers = {}
        self.records = []              # one record per chunk request (for metrics)
        self.lock = threading.Lock()
        self.done = threading.Event()

    # ----------------------------------------------------- tracker refresh
    def refresh(self):
        info = tracker_request(self.peer.tracker, {"type": "GET_PEERS", "file_id": self.fid})
        with self.lock:
            self.peer_chunks = {p: set(c) for p, c in info.get("peers", {}).items()
                                if p != self.peer.peer_id and p not in self.dead}

    def mark_dead(self, p, reason):
        with self.lock:
            if p in self.dead:
                return
            self.dead.add(p)
            self.peer_chunks.pop(p, None)
        print(f"\n[peer] dropping peer {p}: {reason}", flush=True)
        try:
            tracker_request(self.peer.tracker, {"type": "REPORT_DEAD", "peer_id": p})
        except OSError:
            pass

    # ------------------------------------------------- chunk selection
    def pick_chunk(self, p):
        """Rarest-first: among chunks this peer has and we still need,
        pick the one held by the fewest peers (ties broken randomly)."""
        with self.lock:
            avail = self.peer_chunks.get(p, set()) - self.have - self.in_flight
            if not avail:
                return None
            def rarity(i):
                return sum(1 for cs in self.peer_chunks.values() if i in cs)
            idx = min(avail, key=lambda i: (rarity(i), random.random()))
            self.in_flight.add(idx)
            return idx

    def release(self, idx):
        with self.lock:
            self.in_flight.discard(idx)

    def record(self, p, idx, status, ms, size=0):
        with self.lock:
            self.records.append({"peer": p, "chunk": idx, "status": status,
                                 "ms": ms, "bytes": size, "t": time.perf_counter()})

    # --------------------------------------------------------- worker
    def worker(self, p):
        host, port = p.rsplit(":", 1)
        try:
            sock = socket.create_connection((host, int(port)), timeout=5)
        except OSError as e:
            self.mark_dead(p, f"cannot connect ({e})")
            return
        sock.settimeout(10)
        with sock:
            while not self.done.is_set():
                if p in self.dead:
                    return
                idx = self.pick_chunk(p)
                if idx is None:                  # peer has nothing new for us yet
                    time.sleep(0.2)
                    continue
                t0 = time.perf_counter()
                try:
                    send_msg(sock, {"type": "GET_CHUNK", "file_id": self.fid, "index": idx})
                    hdr, data = recv_msg(sock)
                except (OSError, ConnectionError, ValueError) as e:
                    self.release(idx)
                    self.mark_dead(p, f"connection lost ({e.__class__.__name__})")
                    return
                ms = (time.perf_counter() - t0) * 1000

                if hdr.get("type") != "CHUNK":
                    self.release(idx)
                    self.record(p, idx, "not_found", ms)
                    continue

                # ---- error detection: compare CRC with the trusted metadata
                if crc32(data) != self.meta["chunk_crc32"][idx]:
                    self.release(idx)
                    self.record(p, idx, "crc_fail", ms, len(data))
                    fails = self.crc_failures[p] = self.crc_failures.get(p, 0) + 1
                    print(f"\n[peer] CRC mismatch on chunk {idx} from {p} -> re-requesting",
                          flush=True)
                    if fails >= MAX_CRC_FAILURES:
                        self.mark_dead(p, f"{fails} corrupt chunks (blacklisted)")
                        return
                    continue

                self.peer.write_chunk(self.fid, idx, data)
                with self.lock:
                    self.in_flight.discard(idx)
                    self.have.add(idx)
                    complete = len(self.have) == self.n
                self.record(p, idx, "ok", ms, len(data))
                try:   # we can now SEED this chunk to others
                    tracker_request(self.peer.tracker, {"type": "ANNOUNCE", "file_id": self.fid,
                                                        "peer_id": self.peer.peer_id, "chunk": idx})
                except OSError:
                    pass
                if complete:
                    self.done.set()

    # ------------------------------------------------------------ main loop
    def run(self):
        start = time.perf_counter()
        last_progress, last_count = start, -1
        while not self.done.is_set():
            try:
                self.refresh()
            except OSError:
                print("\n[peer] tracker unreachable, continuing with known peers", flush=True)
            with self.lock:
                peers = list(self.peer_chunks)
            for p in peers:
                t = self.workers.get(p)
                if (t is None or not t.is_alive()) and p not in self.dead:
                    t = threading.Thread(target=self.worker, args=(p,), daemon=True)
                    self.workers[p] = t
                    t.start()
            count = len(self.have)
            now = time.perf_counter()
            if count != last_count:
                last_progress, last_count = now, count
                bar = "#" * (30 * count // self.n)
                print(f"\r[peer] [{bar:<30}] {count}/{self.n} chunks", end="", flush=True)
            if now - last_progress > STALL_TIMEOUT:
                raise RuntimeError("download stalled: no peer can supply the missing chunks")
            self.done.wait(0.5)
        elapsed = time.perf_counter() - start
        print(f"\r[peer] [{'#' * 30}] {self.n}/{self.n} chunks", flush=True)
        return self.report(elapsed)

    # --------------------------------------------------------------- metrics
    def report(self, elapsed):
        ok = sorted((r for r in self.records if r["status"] == "ok"), key=lambda r: r["t"])
        lat = [r["ms"] for r in ok]
        jitter = (statistics.mean(abs(a - b) for a, b in zip(lat[1:], lat[:-1]))
                  if len(lat) > 1 else 0.0)
        per_peer = {}
        for r in ok:
            per_peer[r["peer"]] = per_peer.get(r["peer"], 0) + 1
        crc_fail = sum(1 for r in self.records if r["status"] == "crc_fail")
        verified = sha256_file(self.path) == self.meta["sha256"]
        m = {
            "file": self.meta["name"],
            "size_bytes": self.meta["size"],
            "chunks": self.n,
            "time_s": round(elapsed, 3),
            "throughput_MBps": round(self.meta["size"] / elapsed / 1e6, 3),
            "throughput_Mbps": round(self.meta["size"] * 8 / elapsed / 1e6, 2),
            "avg_chunk_latency_ms": round(statistics.mean(lat), 2) if lat else 0,
            "jitter_ms": round(jitter, 2),
            "requests": len(self.records),
            "crc_failures": crc_fail,
            "corrupt_rate_pct": round(100 * crc_fail / max(1, len(self.records)), 2),
            "chunks_per_peer": per_peer,
            "dead_peers": sorted(self.dead),
            "sha256_ok": verified,
        }
        print("\n=========== DOWNLOAD REPORT ===========")
        for k, v in m.items():
            print(f"  {k:<22}: {v}")
        print("=======================================", flush=True)
        return m


# ===================================================================== CLI
def main():
    ap = argparse.ArgumentParser(description="P2P peer")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("seed", "download", "list"):
        p = sub.add_parser(name)
        p.add_argument("--tracker", default="127.0.0.1:9000")
        if name == "list":
            continue
        p.add_argument("target", help="file path (seed) or file id (download)")
        p.add_argument("--port", type=int, required=True, help="port to serve chunks on")
        p.add_argument("--host", default=None, help="IP other peers should use to reach me")
        p.add_argument("--upload-kbps", type=float, default=0, help="upload limit in KB/s")
        p.add_argument("--corrupt-rate", type=float, default=0.0, help="testing only")
        if name == "download":
            p.add_argument("--out", default="downloads")
            p.add_argument("--linger", type=float, default=None,
                           help="seconds to keep seeding after download (default: forever)")
            p.add_argument("--metrics", default=None, help="write report to this JSON file")
    args = ap.parse_args()
    tracker = parse_addr(args.tracker)

    if args.cmd == "list":
        try:
            reply = tracker_request(tracker, {"type": "LIST"})
        except OSError as e:
            sys.exit(f"Tracker unreachable at {args.tracker}: {e}")
        print(f"{'FILE ID':<18}{'NAME':<24}{'SIZE':>12}{'CHUNKS':>8}{'PEERS':>7}")
        for f in reply["files"]:
            print(f"{f['file_id']:<18}{f['name']:<24}{f['size']:>12}{f['num_chunks']:>8}{f['peers']:>7}")
        return

    peer = Peer(args.port, tracker, args.host, args.upload_kbps, args.corrupt_rate)
    peer.start_server()
    try:
        if args.cmd == "seed":
            peer.seed(args.target)
            print("[peer] seeding... press Ctrl+C to stop", flush=True)
            while True:
                time.sleep(1)
        else:
            try:
                metrics = peer.download(args.target, args.out)
            except ConnectionRefusedError:
                print(f"[peer] ERROR: tracker unreachable at {args.tracker}", flush=True)
                sys.exit(2)
            except (RuntimeError, OSError) as e:
                print(f"\n[peer] ERROR: {e}", flush=True)
                peer.leave()
                sys.exit(1)
            if args.metrics:
                with open(args.metrics, "w") as f:
                    json.dump(metrics, f, indent=2)
            if not metrics["sha256_ok"]:
                sys.exit(1)
            if args.linger is None:
                print("[peer] download complete - now seeding. Ctrl+C to stop", flush=True)
                while True:
                    time.sleep(1)
            time.sleep(args.linger)
    except KeyboardInterrupt:
        print("\n[peer] stopping")
    finally:
        peer.leave()


if __name__ == "__main__":
    main()
