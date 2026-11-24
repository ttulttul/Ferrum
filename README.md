# Performance results

The Rust server against the pinned reference, measured by scripts/perf_compare.py on each push to main (.github/workflows/perf.yml); docs/performance.md on main explains the method. One file per run is under results/. This file is generated: do not edit it.

## Latest

Commit `8c0f3e6717f9`, reference `fd0331ca`, 2026-11-24T23:38:11Z, AMD EPYC 9V74 80-Core Processor (4 CPUs), Linux 7.0.0-1017-azure.

Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 70.0 MiB | 205 MiB | **2.9×** |
| Start to first RPC | 82.7 ms <sub>(78.5–144)</sub> | 2618 ms <sub>(2534–2782)</sub> | **31.7×** |
| Memory at idle (RSS) | 40.3 MiB <sub>(40.2–40.8)</sub> | 452 MiB <sub>(451–457)</sub> | **11.2×** |
| CPU while idle (per 10 s) | 90.0 ms <sub>(90.0–100.0)</sub> | 30.0 ms <sub>(20.0–260)</sub> | **0.3×** |
| Peak memory, Codex turns | 60.6 MiB <sub>(59.8–60.9)</sub> | 645 MiB <sub>(642–673)</sub> | **10.6×** |
| Peak memory, Claude turns | 59.0 MiB <sub>(58.4–59.8)</sub> | 662 MiB <sub>(658–686)</sub> | **11.2×** |
| Codex turns per second | 5.60 /s <sub>(5.43–5.61)</sub> | 2.36 /s <sub>(2.20–2.52)</sub> | **2.4×** |
| Claude turns per second | 4.06 /s <sub>(3.93–4.34)</sub> | 2.13 /s <sub>(2.11–2.30)</sub> | **1.9×** |
| CPU per Codex turn | 202 ms <sub>(196–210)</sub> | 611 ms <sub>(532–647)</sub> | **3.0×** |
| CPU per Claude turn | 264 ms <sub>(263–264)</sub> | 653 ms <sub>(630–689)</sub> | **2.5×** |
| server.getConfig p50 | 11.5 ms <sub>(10.4–12.0)</sub> | 4.87 ms <sub>(4.67–4.91)</sub> | **0.4×** |
| server.getConfig p95 | 13.1 ms <sub>(12.0–14.3)</sub> | 6.11 ms <sub>(5.70–6.17)</sub> | **0.5×** |
| server.getSettings p50 | 0.42 ms <sub>(0.40–0.42)</sub> | 0.53 ms <sub>(0.52–0.54)</sub> | **1.3×** |
| server.getSettings p95 | 0.45 ms <sub>(0.45–0.48)</sub> | 0.72 ms <sub>(0.71–0.74)</sub> | **1.6×** |
| orchestration.subscribeShell p50 | 1.30 ms <sub>(1.28–1.32)</sub> | 3.99 ms <sub>(3.96–4.16)</sub> | **3.1×** |
| orchestration.subscribeShell p95 | 1.34 ms <sub>(1.32–1.40)</sub> | 5.48 ms <sub>(5.14–5.89)</sub> | **4.1×** |
| orchestration.subscribeThread p50 | 4.39 ms <sub>(4.08–4.48)</sub> | 14.7 ms <sub>(14.0–15.3)</sub> | **3.3×** |
| orchestration.subscribeThread p95 | 4.78 ms <sub>(4.61–4.99)</sub> | 17.0 ms <sub>(16.2–18.1)</sub> | **3.6×** |
| orchestration.getThreadProjection p50 | 2.02 ms <sub>(1.95–2.02)</sub> | 11.9 ms <sub>(11.8–13.3)</sub> | **5.9×** |
| orchestration.getThreadProjection p95 | 2.09 ms <sub>(2.01–2.18)</sub> | 13.9 ms <sub>(13.1–14.9)</sub> | **6.6×** |
| orchestration.getTurnDiff p50 | 2.61 ms <sub>(2.58–2.71)</sub> | 62.1 ms <sub>(48.9–68.2)</sub> | **23.8×** |
| orchestration.getTurnDiff p95 | 3.08 ms <sub>(2.98–3.08)</sub> | 70.7 ms <sub>(65.2–73.1)</sub> | **22.9×** |

Installed size: Rust is one binary (70.0 MiB). The reference is Node (143.4 MiB), its bundle (22.6 MiB) and 23 external packages (145.2 MiB), 311.2 MiB in all; the table leaves out the 105.8 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

## History

Ratios (Rust against the reference, higher is better for Rust) by commit, newest first.

| Date | Commit | Installed size | Start to first RPC | Memory at idle (RSS) | CPU while idle (per 10 s) | Peak memory, Codex turns | Peak memory, Claude turns | Codex turns per second | Claude turns per second | CPU per Codex turn | CPU per Claude turn |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
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
