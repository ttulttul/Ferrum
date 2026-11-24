# Performance results

The Rust server against the pinned reference, measured by scripts/perf_compare.py on each push to main (.github/workflows/perf.yml); docs/performance.md on main explains the method. One file per run is under results/. This file is generated: do not edit it.

## Latest

Commit `4b8d262f08a2`, reference `fd0331ca`, 2026-11-24T22:06:16Z, AMD EPYC 9V74 80-Core Processor (4 CPUs), Linux 7.0.0-1017-azure.

Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 69.6 MiB | 205 MiB | **3.0×** |
| Start to first RPC | 86.0 ms <sub>(72.2–309)</sub> | 2301 ms <sub>(2258–2377)</sub> | **26.8×** |
| Memory at idle (RSS) | 40.0 MiB <sub>(39.3–40.5)</sub> | 452 MiB <sub>(452–457)</sub> | **11.3×** |
| CPU while idle (per 10 s) | 90.0 ms <sub>(80.0–100.0)</sub> | 20.0 ms <sub>(20.0–240)</sub> | **0.2×** |
| Peak memory, Codex turns | 59.8 MiB <sub>(59.2–60.8)</sub> | 644 MiB <sub>(643–651)</sub> | **10.8×** |
| Peak memory, Claude turns | 59.3 MiB <sub>(58.9–59.9)</sub> | 659 MiB <sub>(656–663)</sub> | **11.1×** |
| Codex turns per second | 5.63 /s <sub>(4.67–5.66)</sub> | 2.28 /s <sub>(2.17–2.36)</sub> | **2.5×** |
| Claude turns per second | 4.88 /s <sub>(4.87–4.89)</sub> | 2.11 /s <sub>(2.01–2.17)</sub> | **2.3×** |
| CPU per Codex turn | 192 ms <sub>(191–193)</sub> | 599 ms <sub>(596–658)</sub> | **3.1×** |
| CPU per Claude turn | 262 ms <sub>(260–265)</sub> | 692 ms <sub>(688–730)</sub> | **2.6×** |
| server.getConfig p50 | 10.6 ms <sub>(10.2–10.6)</sub> | 3.61 ms <sub>(3.58–3.62)</sub> | **0.3×** |
| server.getConfig p95 | 11.2 ms <sub>(10.9–11.5)</sub> | 4.25 ms <sub>(4.18–4.70)</sub> | **0.4×** |
| server.getSettings p50 | 0.46 ms <sub>(0.42–0.46)</sub> | 0.52 ms <sub>(0.51–0.52)</sub> | **1.1×** |
| server.getSettings p95 | 0.48 ms <sub>(0.44–0.48)</sub> | 0.65 ms <sub>(0.64–0.69)</sub> | **1.4×** |
| orchestration.subscribeShell p50 | 1.23 ms <sub>(1.20–1.24)</sub> | 3.22 ms <sub>(3.12–3.26)</sub> | **2.6×** |
| orchestration.subscribeShell p95 | 1.27 ms <sub>(1.26–1.28)</sub> | 4.67 ms <sub>(4.56–4.95)</sub> | **3.7×** |
| orchestration.subscribeThread p50 | 3.98 ms <sub>(3.98–4.00)</sub> | 13.2 ms <sub>(12.7–13.4)</sub> | **3.3×** |
| orchestration.subscribeThread p95 | 4.17 ms <sub>(4.16–4.23)</sub> | 15.2 ms <sub>(14.6–15.5)</sub> | **3.6×** |
| orchestration.getThreadProjection p50 | 1.97 ms <sub>(1.91–2.00)</sub> | 10.7 ms <sub>(10.3–10.8)</sub> | **5.4×** |
| orchestration.getThreadProjection p95 | 2.01 ms <sub>(1.96–2.04)</sub> | 12.0 ms <sub>(11.9–12.3)</sub> | **5.9×** |
| orchestration.getTurnDiff p50 | 2.49 ms <sub>(2.45–2.56)</sub> | 44.0 ms <sub>(42.0–44.3)</sub> | **17.7×** |
| orchestration.getTurnDiff p95 | 2.63 ms <sub>(2.62–2.73)</sub> | 52.9 ms <sub>(48.4–55.0)</sub> | **20.1×** |

Installed size: Rust is one binary (69.6 MiB). The reference is Node (143.4 MiB), its bundle (22.6 MiB) and 23 external packages (145.2 MiB), 311.2 MiB in all; the table leaves out the 105.8 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

## History

Ratios (Rust against the reference, higher is better for Rust) by commit, newest first.

| Date | Commit | Installed size | Start to first RPC | Memory at idle (RSS) | CPU while idle (per 10 s) | Peak memory, Codex turns | Peak memory, Claude turns | Codex turns per second | Claude turns per second | CPU per Codex turn | CPU per Claude turn |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-11-24 | `4b8d262f` | 3.0× | 26.8× | 11.3× | 0.2× | 10.8× | 11.1× | 2.5× | 2.3× | 3.1× | 2.6× |
| 2026-11-24 | `c68bda6c` | 2.9× | 35.7× | 11.1× | 2.8× | 11.3× | 11.3× | 1.8× | 1.6× | 2.2× | 1.9× |
| 2026-11-24 | `480c4073` | 2.9× | 35.3× | 11.2× | 3.9× | 10.9× | 11.3× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `52b62924` | 3.0× | 36.2× | 11.1× | 3.1× | 11.0× | 11.2× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `e10f06d2` | 3.0× | 36.4× | 11.1× | 3.2× | 11.0× | 11.2× | 1.8× | 1.7× | 2.2× | 1.9× |
| 2026-11-24 | `00b0e0d7` | 3.0× | 31.2× | 11.2× | 0.3× | 11.0× | 11.2× | 2.7× | 2.8× | 3.1× | 3.0× |
| 2026-11-24 | `ec02f406` | 3.0× | 31.1× | 11.3× | 2.6× | 11.0× | 11.3× | 3.2× | 3.0× | 3.7× | 3.1× |
