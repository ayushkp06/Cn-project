# Test results

File: 8 MB random data, 128 chunks of 64 KB. Every peer's upload limited to 2000 KB/s. Run on localhost.

## Test cases

| ID | Test | Condition | Expected | Result | Observed |
|---|---|---|---|---|---|
| TC1 | Single source download | Normal | File received, SHA-256 matches | PASS | 4.303 s, 15.6 Mbps |
| TC2 | Multi-source download (3 seeders) | Normal | Chunks fetched from all 3 peers, faster than TC1 | PASS | 1.453 s, split [43, 43, 42] |
| TC3 | Swarm: downloaders re-seed chunks | Normal | Downloaders get chunks from each other | PASS | 230/384 chunks came from other downloaders |
| TC4 | Peer crash mid-download | Failure | Crashed peer dropped, download completes | PASS | dropped ['127.0.0.1:9181'], done in 1.881 s |
| TC5 | Corrupted chunks (bad peer) | Failure | CRC detects, chunk re-fetched, bad peer blacklisted | PASS | 3 CRC errors caught, blacklisted ['127.0.0.1:9202'] |
| TC6 | Tracker unreachable | Failure | Clear error, exit code 2 | PASS | exit code 2 |
| TC7 | Only seeder crashes | Failure | Download stops with 'stalled' error (no hang) | PASS | exit code 1 |

## Performance

| Scenario | Time (s) | Throughput (Mbps) | Avg chunk latency (ms) | Jitter (ms) | Requests | CRC failures | Corrupt % |
|---|---|---|---|---|---|---|---|
| 1 seeder | 4.303 | 15.6 | 32.69 | 0.15 | 128 | 0 | 0.0 |
| 3 seeders | 1.453 | 46.19 | 32.83 | 0.42 | 128 | 0 | 0.0 |
| swarm downloader 1 | 4.941 | 13.58 | 65.19 | 27.21 | 128 | 0 | 0.0 |
| swarm downloader 2 | 4.93 | 13.61 | 66.42 | 30.72 | 128 | 0 | 0.0 |
| swarm downloader 3 | 4.875 | 13.76 | 67.63 | 29.07 | 128 | 0 | 0.0 |
| 3 seeders, 1 crashes | 1.881 | 35.68 | 32.67 | 0.24 | 128 | 0 | 0.0 |
| 1 good + 1 corrupt seeder | 4.236 | 15.84 | 32.58 | 0.09 | 131 | 3 | 2.29 |
