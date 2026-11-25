# Performance results

The Rust server against the pinned reference, measured by scripts/perf_compare.py on each push to main (.github/workflows/perf.yml); docs/performance.md on main explains the method. One file per run is under results/. This file is generated: do not edit it.

## Latest

Commit `f2145255066a`, reference `fd0331ca`, 2026-11-25T00:15:00Z, AMD EPYC 7763 64-Core Processor (4 CPUs), Linux 7.0.0-1017-azure.

Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 70.1 MiB | 205 MiB | **2.9×** |
| Start to first RPC | 86.8 ms <sub>(82.9–94.3)</sub> | 3126 ms <sub>(3007–3205)</sub> | **36.0×** |
| Memory at idle (RSS) | 40.9 MiB <sub>(40.5–41.7)</sub> | 456 MiB <sub>(452–457)</sub> | **11.1×** |
| CPU while idle (per 10 s) | 10.00 ms <sub>(0.00–20.0)</sub> | 30.0 ms <sub>(10.00–290)</sub> | **3.0×** |
| Peak memory, Codex turns | 59.4 MiB <sub>(59.3–59.9)</sub> | 691 MiB <sub>(652–691)</sub> | **11.6×** |
| Peak memory, Claude turns | 59.8 MiB <sub>(59.3–60.2)</sub> | 667 MiB <sub>(655–668)</sub> | **11.2×** |
| Codex turns per second | 4.78 /s <sub>(4.74–4.78)</sub> | 1.79 /s <sub>(1.74–1.83)</sub> | **2.7×** |
| Claude turns per second | 4.02 /s <sub>(4.01–4.06)</sub> | 1.65 /s <sub>(1.65–1.68)</sub> | **2.4×** |
| CPU per Codex turn | 255 ms <sub>(253–256)</sub> | 811 ms <sub>(785–840)</sub> | **3.2×** |
| CPU per Claude turn | 344 ms <sub>(342–348)</sub> | 906 ms <sub>(890–908)</sub> | **2.6×** |
| server.getConfig p50 | 3.33 ms <sub>(3.31–3.60)</sub> | 5.27 ms <sub>(5.10–5.43)</sub> | **1.6×** |
| server.getConfig p95 | 3.44 ms <sub>(3.42–3.89)</sub> | 6.63 ms <sub>(6.56–7.05)</sub> | **1.9×** |
| server.getSettings p50 | 0.60 ms <sub>(0.58–0.61)</sub> | 0.69 ms <sub>(0.68–0.70)</sub> | **1.2×** |
| server.getSettings p95 | 0.62 ms <sub>(0.61–0.63)</sub> | 0.95 ms <sub>(0.92–0.95)</sub> | **1.5×** |
| orchestration.subscribeShell p50 | 1.65 ms <sub>(1.64–1.71)</sub> | 4.74 ms <sub>(4.24–4.82)</sub> | **2.9×** |
| orchestration.subscribeShell p95 | 1.73 ms <sub>(1.71–1.87)</sub> | 6.63 ms <sub>(5.84–6.76)</sub> | **3.8×** |
| orchestration.subscribeThread p50 | 5.30 ms <sub>(5.27–5.35)</sub> | 17.9 ms <sub>(17.4–18.2)</sub> | **3.4×** |
| orchestration.subscribeThread p95 | 5.68 ms <sub>(5.45–5.71)</sub> | 20.2 ms <sub>(19.4–20.7)</sub> | **3.6×** |
| orchestration.getThreadProjection p50 | 2.64 ms <sub>(2.64–2.67)</sub> | 14.5 ms <sub>(14.2–14.6)</sub> | **5.5×** |
| orchestration.getThreadProjection p95 | 2.71 ms <sub>(2.68–2.90)</sub> | 16.2 ms <sub>(16.1–16.9)</sub> | **6.0×** |
| orchestration.getTurnDiff p50 | 3.17 ms <sub>(3.01–3.40)</sub> | 63.2 ms <sub>(60.4–65.9)</sub> | **20.0×** |
| orchestration.getTurnDiff p95 | 3.36 ms <sub>(3.13–3.82)</sub> | 74.4 ms <sub>(71.3–78.4)</sub> | **22.1×** |

Installed size: Rust is one binary (70.1 MiB). The reference is Node (143.4 MiB), its bundle (22.6 MiB) and 23 external packages (145.2 MiB), 311.2 MiB in all; the table leaves out the 105.8 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

## History

Ratios (Rust against the reference, higher is better for Rust) by commit, newest first.

| Date | Commit | Installed size | Start to first RPC | Memory at idle (RSS) | CPU while idle (per 10 s) | Peak memory, Codex turns | Peak memory, Claude turns | Codex turns per second | Claude turns per second | CPU per Codex turn | CPU per Claude turn |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-11-25 | `f2145255` | 2.9× | 36.0× | 11.1× | 3.0× | 11.6× | 11.2× | 2.7× | 2.4× | 3.2× | 2.6× |
| 2026-11-24 | `8c0f3e67` | 2.9× | 31.7× | 11.2× | 0.3× | 10.6× | 11.2× | 2.4× | 1.9× | 3.0× | 2.5× |
| 2026-11-24 | `1bd449a3` | 2.9× | 33.5× | 11.2× | 0.2× | 11.0× | 11.1× | 2.4× | 2.2× | 2.9× | 2.4× |
| 2026-11-24 | `e2933272` | 3.0× | 35.5× | 11.1× | 3.1× | 10.8× | 11.2× | 1.7× | 1.8× | 2.1× | 2.0× |
| 2026-11-24 | `4b8d262f` | 3.0× | 26.8× | 11.3× | 0.2× | 10.8× | 11.1× | 2.5× | 2.3× | 3.1× | 2.6× |
| 2026-11-24 | `c68bda6c` | 2.9× | 35.7× | 11.1× | 2.8× | 11.3× | 11.3× | 1.8× | 1.6× | 2.2× | 1.9× |
| 2026-11-24 | `480c4073` | 2.9× | 35.3× | 11.2× | 3.9× | 10.9× | 11.3× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `52b62924` | 3.0× | 36.2× | 11.1× | 3.1× | 11.0× | 11.2× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `e10f06d2` | 3.0× | 36.4× | 11.1× | 3.2× | 11.0× | 11.2× | 1.8× | 1.7× | 2.2× | 1.9× |
| 2026-11-24 | `00b0e0d7` | 3.0× | 31.2× | 11.2× | 0.3× | 11.0× | 11.2× | 2.7× | 2.8× | 3.1× | 3.0× |
| 2026-11-24 | `ec02f406` | 3.0× | 31.1× | 11.3× | 2.6× | 11.0× | 11.3× | 3.2× | 3.0× | 3.7× | 3.1× |
