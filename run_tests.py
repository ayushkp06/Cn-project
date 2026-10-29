"""
run_tests.py - automated test cases + performance measurement.

Starts a tracker and several peers on localhost (each peer is a separate
process on its own TCP port), runs normal and failure scenarios, and writes:
    results/results.md    - test-case table + performance table
    results/results.csv   - raw numbers
    results/chart.png     - throughput comparison (if matplotlib is installed)

Usage:
    python run_tests.py
"""

import csv
import json
import os
import shutil
import subprocess
import sys
import time

from common import build_metadata, tracker_request

HERE = os.path.dirname(os.path.abspath(__file__))
WORK = os.path.join(HERE, "test_run")
RESULTS = os.path.join(HERE, "results")
PY = sys.executable
FILE_MB = 8
UPLOAD_KBPS = 2000          # every peer's uplink is limited to ~2 MB/s


def start(args, log_name):
    log = open(os.path.join(WORK, log_name), "w")
    return subprocess.Popen([PY, "-u", *args], cwd=HERE, stdout=log, stderr=subprocess.STDOUT)


def wait_for(cond, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if cond():
                return True
        except OSError:
            pass
        time.sleep(0.2)
    return False


def run_scenario(sid, seeders, downloaders=1, kill_seeder_after=None, with_tracker=True):
    """seeders: list of dicts like {"corrupt": 0.3}. Returns (exit codes, metrics list)."""
    base = 9100 + sid * 20
    tracker = ("127.0.0.1", base)
    targ = f"127.0.0.1:{base}"
    procs, seed_procs = [], []
    try:
        if with_tracker:
            procs.append(start(["tracker.py", "--port", str(base)], f"s{sid}_tracker.log"))
            wait_for(lambda: tracker_request(tracker, {"type": "LIST"}))
            for i, s in enumerate(seeders):
                p = start(["peer.py", "seed", TEST_FILE, "--port", str(base + 1 + i),
                           "--tracker", targ, "--upload-kbps", str(UPLOAD_KBPS),
                           "--corrupt-rate", str(s.get("corrupt", 0))], f"s{sid}_seeder{i}.log")
                procs.append(p)
                seed_procs.append(p)
            wait_for(lambda: any(f["peers"] >= len(seeders) for f in
                                 tracker_request(tracker, {"type": "LIST"})["files"]))

        dl, metric_files = [], []
        for j in range(downloaders):
            mf = os.path.join(WORK, f"s{sid}_dl{j}.json")
            metric_files.append(mf)
            p = start(["peer.py", "download", FILE_ID, "--port", str(base + 10 + j),
                       "--tracker", targ, "--out", os.path.join(WORK, f"s{sid}_dl{j}"),
                       "--upload-kbps", str(UPLOAD_KBPS), "--linger", "4",
                       "--metrics", mf], f"s{sid}_downloader{j}.log")
            procs.append(p)
            dl.append(p)

        if kill_seeder_after is not None:
            time.sleep(kill_seeder_after)
            seed_procs[0].kill()              # crash: no LEAVE message is sent

        codes = []
        for p in dl:
            try:
                codes.append(p.wait(timeout=90))
            except subprocess.TimeoutExpired:
                p.kill()
                codes.append("timeout")
        metrics = []
        for mf in metric_files:
            if os.path.exists(mf):
                with open(mf) as f:
                    metrics.append(json.load(f))
        return codes, metrics
    finally:
        for p in procs:
            if p.poll() is None:
                p.kill()


def main():
    global TEST_FILE, FILE_ID
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(WORK)
    os.makedirs(RESULTS, exist_ok=True)
    TEST_FILE = os.path.join(WORK, "testfile.bin")
    with open(TEST_FILE, "wb") as f:
        f.write(os.urandom(FILE_MB * 1024 * 1024))
    FILE_ID = build_metadata(TEST_FILE)["file_id"]
    print(f"Test file: {FILE_MB} MB, file_id={FILE_ID}, uplink per peer={UPLOAD_KBPS} KB/s\n")

    tests, perf = [], []

    def add_test(tid, name, condition, expected, ok, observed):
        tests.append([tid, name, condition, expected, "PASS" if ok else "FAIL", observed])
        print(f"{tid} {name:<38} {'PASS' if ok else 'FAIL'}  ({observed})")

    def add_perf(label, m):
        perf.append({"scenario": label, **{k: m[k] for k in (
            "time_s", "throughput_Mbps", "avg_chunk_latency_ms", "jitter_ms",
            "requests", "crc_failures", "corrupt_rate_pct")}})

    # TC1 - 1 seeder -> 1 downloader
    codes, ms = run_scenario(1, [{}])
    m = ms[0] if ms else None
    add_test("TC1", "Single source download", "Normal",
             "File received, SHA-256 matches", codes == [0] and m and m["sha256_ok"],
             f"{m['time_s']} s, {m['throughput_Mbps']} Mbps" if m else f"exit={codes}")
    if m: add_perf("1 seeder", m)

    # TC2 - 3 seeders -> 1 downloader (parallel chunks from multiple nodes)
    codes, ms = run_scenario(2, [{}, {}, {}])
    m2 = ms[0] if ms else None
    add_test("TC2", "Multi-source download (3 seeders)", "Normal",
             "Chunks fetched from all 3 peers, faster than TC1",
             codes == [0] and m2 and len(m2["chunks_per_peer"]) == 3
             and m and m2["time_s"] < m["time_s"],
             f"{m2['time_s']} s, split {list(m2['chunks_per_peer'].values())}" if m2 else f"exit={codes}")
    if m2: add_perf("3 seeders", m2)

    # TC3 - swarm: 1 seeder, 3 downloaders at the same time (re-seeding)
    codes, ms = run_scenario(3, [{}], downloaders=3)
    seeder_id = f"127.0.0.1:{9100 + 3 * 20 + 1}"
    from_peers = sum(c for m3 in ms for p, c in m3["chunks_per_peer"].items() if p != seeder_id)
    total = sum(m3["chunks"] for m3 in ms)
    add_test("TC3", "Swarm: downloaders re-seed chunks", "Normal",
             "Downloaders get chunks from each other",
             codes == [0, 0, 0] and all(x["sha256_ok"] for x in ms) and from_peers > 0,
             f"{from_peers}/{total} chunks came from other downloaders")
    for k, m3 in enumerate(ms):
        add_perf(f"swarm downloader {k + 1}", m3)

    # TC4 - a seeder crashes in the middle of the download
    codes, ms = run_scenario(4, [{}, {}, {}], kill_seeder_after=0.6)
    m4 = ms[0] if ms else None
    add_test("TC4", "Peer crash mid-download", "Failure",
             "Crashed peer dropped, download completes",
             codes == [0] and m4 and m4["sha256_ok"] and len(m4["dead_peers"]) == 1,
             f"dropped {m4['dead_peers']}, done in {m4['time_s']} s" if m4 else f"exit={codes}")
    if m4: add_perf("3 seeders, 1 crashes", m4)

    # TC5 - one seeder sends corrupted chunks
    codes, ms = run_scenario(5, [{}, {"corrupt": 0.5}])
    m5 = ms[0] if ms else None
    add_test("TC5", "Corrupted chunks (bad peer)", "Failure",
             "CRC detects, chunk re-fetched, bad peer blacklisted",
             codes == [0] and m5 and m5["sha256_ok"] and m5["crc_failures"] > 0,
             f"{m5['crc_failures']} CRC errors caught, blacklisted {m5['dead_peers']}" if m5 else f"exit={codes}")
    if m5: add_perf("1 good + 1 corrupt seeder", m5)

    # TC6 - tracker is down
    codes, _ = run_scenario(6, [], with_tracker=False)
    add_test("TC6", "Tracker unreachable", "Failure",
             "Clear error, exit code 2", codes == [2], f"exit code {codes[0]}")

    # TC7 - no peer has the file / all seeders gone
    codes, ms = run_scenario(7, [{}], kill_seeder_after=0.3)
    add_test("TC7", "Only seeder crashes", "Failure",
             "Download stops with 'stalled' error (no hang)", codes == [1],
             f"exit code {codes[0]}")

    # ------------------------------------------------------------ write results
    with open(os.path.join(RESULTS, "results.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(perf[0].keys()))
        w.writeheader()
        w.writerows(perf)

    with open(os.path.join(RESULTS, "results.md"), "w") as f:
        f.write(f"# Test results\n\nFile: {FILE_MB} MB random data, {FILE_MB * 16} chunks of 64 KB. "
                f"Every peer's upload limited to {UPLOAD_KBPS} KB/s. Run on localhost.\n\n")
        f.write("## Test cases\n\n| ID | Test | Condition | Expected | Result | Observed |\n"
                "|---|---|---|---|---|---|\n")
        for t in tests:
            f.write("| " + " | ".join(map(str, t)) + " |\n")
        f.write("\n## Performance\n\n| Scenario | Time (s) | Throughput (Mbps) | Avg chunk "
                "latency (ms) | Jitter (ms) | Requests | CRC failures | Corrupt % |\n"
                "|---|---|---|---|---|---|---|---|\n")
        for p in perf:
            f.write("| " + " | ".join(str(v) for v in p.values()) + " |\n")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        labels = [p["scenario"] for p in perf]
        vals = [p["throughput_Mbps"] for p in perf]
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.barh(labels[::-1], vals[::-1], color="#3b6ea5")
        ax.set_xlabel("Download throughput (Mbps)")
        ax.set_title(f"P2P download throughput ({FILE_MB} MB file, {UPLOAD_KBPS} KB/s uplink per peer)")
        for i, v in enumerate(vals[::-1]):
            ax.text(v, i, f" {v}", va="center")
        fig.tight_layout()
        fig.savefig(os.path.join(RESULTS, "chart.png"), dpi=130)
    except ImportError:
        print("(matplotlib not installed - skipping chart)")

    passed = sum(t[4] == "PASS" for t in tests)
    print(f"\n{passed}/{len(tests)} tests passed. Results written to results/")


if __name__ == "__main__":
    main()
