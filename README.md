# Performance results

The Rust server against the pinned reference, measured by scripts/perf_compare.py on each push to main (.github/workflows/perf.yml); docs/performance.md on main explains the method. One file per run is under results/. This file is generated: do not edit it.

## Latest

Commit `52b62924fc2f`, reference `fd0331ca`, 2026-11-24T21:18:19Z, Intel(R) Xeon(R) 6975P-C (4 CPUs), Linux 7.0.0-1014-aws.

Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 69.1 MiB | 205 MiB | **3.0×** |
| Start to first RPC | 59.4 ms <sub>(54.8–62.6)</sub> | 2149 ms <sub>(2098–2160)</sub> | **36.2×** |
| Memory at idle (RSS) | 40.7 MiB <sub>(39.9–41.2)</sub> | 452 MiB <sub>(451–452)</sub> | **11.1×** |
| CPU while idle (per 10 s) | 90.0 ms <sub>(90.0–100)</sub> | 280 ms <sub>(20.0–290)</sub> | **3.1×** |
| Peak memory, Codex turns | 59.7 MiB <sub>(59.3–60.0)</sub> | 656 MiB <sub>(653–675)</sub> | **11.0×** |
| Peak memory, Claude turns | 59.3 MiB <sub>(58.4–59.5)</sub> | 665 MiB <sub>(663–665)</sub> | **11.2×** |
| Codex turns per second | 6.96 /s <sub>(6.95–6.97)</sub> | 4.08 /s <sub>(3.95–4.09)</sub> | **1.7×** |
| Claude turns per second | 5.85 /s <sub>(5.80–5.91)</sub> | 3.57 /s <sub>(3.56–3.61)</sub> | **1.6×** |
| CPU per Codex turn | 174 ms <sub>(174–174)</sub> | 366 ms <sub>(363–377)</sub> | **2.1×** |
| CPU per Claude turn | 235 ms <sub>(233–236)</sub> | 442 ms <sub>(437–442)</sub> | **1.9×** |
| server.getConfig p50 | 7.75 ms <sub>(7.70–8.16)</sub> | 3.54 ms <sub>(3.52–3.56)</sub> | **0.5×** |
| server.getConfig p95 | 8.33 ms <sub>(8.20–9.26)</sub> | 4.20 ms <sub>(4.14–4.22)</sub> | **0.5×** |
| server.getSettings p50 | 0.41 ms <sub>(0.37–0.44)</sub> | 0.61 ms <sub>(0.59–0.62)</sub> | **1.5×** |
| server.getSettings p95 | 0.47 ms <sub>(0.42–0.48)</sub> | 0.76 ms <sub>(0.75–0.79)</sub> | **1.6×** |
| orchestration.subscribeShell p50 | 1.20 ms <sub>(1.15–1.25)</sub> | 3.04 ms <sub>(3.01–3.08)</sub> | **2.5×** |
| orchestration.subscribeShell p95 | 1.28 ms <sub>(1.28–1.46)</sub> | 4.54 ms <sub>(4.47–4.67)</sub> | **3.5×** |
| orchestration.subscribeThread p50 | 4.08 ms <sub>(4.00–4.17)</sub> | 11.8 ms <sub>(11.5–11.9)</sub> | **2.9×** |
| orchestration.subscribeThread p95 | 4.41 ms <sub>(4.27–4.94)</sub> | 13.3 ms <sub>(13.2–13.3)</sub> | **3.0×** |
| orchestration.getThreadProjection p50 | 1.93 ms <sub>(1.81–1.96)</sub> | 10.0 ms <sub>(9.97–10.1)</sub> | **5.2×** |
| orchestration.getThreadProjection p95 | 2.14 ms <sub>(2.13–2.24)</sub> | 10.7 ms <sub>(10.6–11.0)</sub> | **5.0×** |
| orchestration.getTurnDiff p50 | 1.87 ms <sub>(1.83–1.96)</sub> | 26.1 ms <sub>(25.9–26.5)</sub> | **13.9×** |
| orchestration.getTurnDiff p95 | 2.09 ms <sub>(2.01–2.20)</sub> | 28.1 ms <sub>(27.9–28.4)</sub> | **13.5×** |

Installed size: Rust is one binary (69.1 MiB). The reference is Node (143.4 MiB), its bundle (22.6 MiB) and 23 external packages (145.2 MiB), 311.2 MiB in all; the table leaves out the 105.8 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

## History

Ratios (Rust against the reference, higher is better for Rust) by commit, newest first.

| Date | Commit | Installed size | Start to first RPC | Memory at idle (RSS) | CPU while idle (per 10 s) | Peak memory, Codex turns | Peak memory, Claude turns | Codex turns per second | Claude turns per second | CPU per Codex turn | CPU per Claude turn |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-11-24 | `52b62924` | 3.0× | 36.2× | 11.1× | 3.1× | 11.0× | 11.2× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `e10f06d2` | 3.0× | 36.4× | 11.1× | 3.2× | 11.0× | 11.2× | 1.8× | 1.7× | 2.2× | 1.9× |
| 2026-11-24 | `00b0e0d7` | 3.0× | 31.2× | 11.2× | 0.3× | 11.0× | 11.2× | 2.7× | 2.8× | 3.1× | 3.0× |
| 2026-11-24 | `ec02f406` | 3.0× | 31.1× | 11.3× | 2.6× | 11.0× | 11.3× | 3.2× | 3.0× | 3.7× | 3.1× |
