# Peer-to-Peer File Distribution System

CN Lab Mini Project — CSE20110 (Computer Networks Laboratory), MIT-WPU

A file is split into 64 KB **chunks**. A downloader fetches different chunks from
**several peers at the same time** over TCP, checks every chunk with **CRC-32**, and
as soon as it has a chunk it **seeds** it to other peers. A small **tracker** keeps
track of which peer has which chunk.

Pure Python 3 (standard library only) — runs the same on Windows and macOS.
`matplotlib` is optional (only for the results chart).

## Files

| File | What it does |
|---|---|
| `common.py` | Message framing over TCP, CRC-32 / SHA-256, splitting a file into chunks |
| `tracker.py` | Tracker server: which peer has which chunks (stores no file data) |
| `peer.py` | Peer: chunk server + parallel multi-peer downloader + re-seeding |
| `run_tests.py` | Runs all 7 test cases automatically and records performance |
| `docs/architecture.png` | Architecture diagram |
| `results/` | Test-case table, performance table, chart (generated) |
| `REPORT.md` | Project report (problem, objectives, protocol, tests, results, contributions) |

## Run it (each command in its own terminal)

```bash
# 1. tracker
python tracker.py --port 9000

# 2. seeder - shares a file and prints its FILE ID
python peer.py seed notes.pdf --port 9001

# 3. see what is shared
python peer.py list

# 4. downloaders (each on a different port) - they keep seeding after finishing
python peer.py download <FILE_ID> --port 9002 --out dl_B
python peer.py download <FILE_ID> --port 9003 --out dl_C
```

Use `python3` instead of `python` on macOS.

**Make the demo visible on localhost** (localhost is too fast to see anything):
add `--upload-kbps 500` to every peer — each peer's upload is then limited to
~0.5 MB/s, like a real slow link, and you can watch the progress bar fill from
several peers.

**Across laptops on the same Wi-Fi:** run the tracker on laptop 1, then on each
laptop add `--tracker <laptop1-IP>:9000 --host <this-laptop-IP>`.
(Find your IP with `ipconfig` on Windows or `ipconfig getifaddr en0` on Mac. Allow
Python through the Windows firewall when asked.)

**Failure options for the demo:**
- Kill a seeder with Ctrl+C mid-download → the downloader drops it and continues.
- Start a "bad" seeder: `python peer.py seed notes.pdf --port 9005 --corrupt-rate 0.5`
  → the downloader shows `CRC mismatch ... re-requesting`, then blacklists it.

## Run all test cases

```bash
python run_tests.py
```

Takes about 1 minute. Writes `results/results.md`, `results/results.csv`, `results/chart.png`.

## Wireshark verification

1. Capture on the **loopback** interface (`Loopback: lo0` on Mac; on Windows,
   `Adapter for loopback traffic capture` — installed with Npcap). For a
   multi-laptop demo, capture on Wi-Fi instead.
2. Display filter: `tcp.port == 9000 || (tcp.port >= 9001 && tcp.port <= 9010)`
3. Things to show:
   - **3-way handshake** (SYN, SYN-ACK, ACK) when a peer connects.
   - Right-click a packet → **Follow → TCP Stream**: the readable JSON headers
     (`"type": "GET_PEERS"`, `"type": "GET_CHUNK"`, `"crc32": ...`) followed by the binary chunk.
   - Tracker connections are short (one request, then FIN); peer-to-peer
     connections stay open and carry many chunks.
   - A ~64 KB chunk is split into many TCP segments (look at `tcp.len` / MSS).
   - Kill a seeder → RST or FIN on its connection.
