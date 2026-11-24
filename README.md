# Performance results

The Rust server against the pinned reference, measured by scripts/perf_compare.py on each push to main (.github/workflows/perf.yml); docs/performance.md on main explains the method. One file per run is under results/. This file is generated: do not edit it.

## Latest

Commit `1bd449a32b4d`, reference `fd0331ca`, 2026-11-24T23:15:31Z, AMD EPYC 7763 64-Core Processor (4 CPUs), Linux 7.0.0-1017-azure.

Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 70.0 MiB | 205 MiB | **2.9×** |
| Start to first RPC | 88.5 ms <sub>(85.7–95.5)</sub> | 2961 ms <sub>(2893–3104)</sub> | **33.5×** |
| Memory at idle (RSS) | 40.6 MiB <sub>(40.2–40.9)</sub> | 455 MiB <sub>(453–457)</sub> | **11.2×** |
| CPU while idle (per 10 s) | 110 ms <sub>(100.0–120)</sub> | 20.0 ms <sub>(20.0–160)</sub> | **0.2×** |
| Peak memory, Codex turns | 59.2 MiB <sub>(58.8–60.6)</sub> | 653 MiB <sub>(653–690)</sub> | **11.0×** |
| Peak memory, Claude turns | 59.5 MiB <sub>(58.8–59.9)</sub> | 658 MiB <sub>(657–660)</sub> | **11.1×** |
| Codex turns per second | 4.91 /s <sub>(4.80–4.94)</sub> | 2.05 /s <sub>(2.00–2.12)</sub> | **2.4×** |
| Claude turns per second | 4.13 /s <sub>(4.09–4.15)</sub> | 1.85 /s <sub>(1.85–1.86)</sub> | **2.2×** |
| CPU per Codex turn | 250 ms <sub>(248–256)</sub> | 723 ms <sub>(708–737)</sub> | **2.9×** |
| CPU per Claude turn | 340 ms <sub>(336–340)</sub> | 826 ms <sub>(821–826)</sub> | **2.4×** |
| server.getConfig p50 | 11.4 ms <sub>(11.3–12.2)</sub> | 4.78 ms <sub>(4.76–4.82)</sub> | **0.4×** |
| server.getConfig p95 | 12.8 ms <sub>(12.8–13.1)</sub> | 5.61 ms <sub>(5.46–6.03)</sub> | **0.4×** |
| server.getSettings p50 | 0.60 ms <sub>(0.59–0.60)</sub> | 0.67 ms <sub>(0.67–0.68)</sub> | **1.1×** |
| server.getSettings p95 | 0.63 ms <sub>(0.62–0.64)</sub> | 0.90 ms <sub>(0.83–0.96)</sub> | **1.4×** |
| orchestration.subscribeShell p50 | 1.66 ms <sub>(1.65–1.66)</sub> | 4.28 ms <sub>(4.19–4.29)</sub> | **2.6×** |
| orchestration.subscribeShell p95 | 1.74 ms <sub>(1.73–1.74)</sub> | 5.82 ms <sub>(5.55–6.87)</sub> | **3.3×** |
| orchestration.subscribeThread p50 | 5.32 ms <sub>(5.27–5.32)</sub> | 16.5 ms <sub>(16.2–16.7)</sub> | **3.1×** |
| orchestration.subscribeThread p95 | 5.44 ms <sub>(5.43–5.52)</sub> | 19.3 ms <sub>(18.2–19.5)</sub> | **3.6×** |
| orchestration.getThreadProjection p50 | 2.64 ms <sub>(2.62–2.67)</sub> | 13.6 ms <sub>(13.1–13.6)</sub> | **5.2×** |
| orchestration.getThreadProjection p95 | 2.70 ms <sub>(2.69–2.88)</sub> | 15.0 ms <sub>(14.3–15.6)</sub> | **5.5×** |
| orchestration.getTurnDiff p50 | 3.00 ms <sub>(2.97–3.02)</sub> | 51.8 ms <sub>(51.2–52.7)</sub> | **17.3×** |
| orchestration.getTurnDiff p95 | 3.10 ms <sub>(3.10–3.17)</sub> | 58.6 ms <sub>(57.7–59.6)</sub> | **18.9×** |

Installed size: Rust is one binary (70.0 MiB). The reference is Node (143.4 MiB), its bundle (22.6 MiB) and 23 external packages (145.2 MiB), 311.2 MiB in all; the table leaves out the 105.8 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

## History

Ratios (Rust against the reference, higher is better for Rust) by commit, newest first.

| Date | Commit | Installed size | Start to first RPC | Memory at idle (RSS) | CPU while idle (per 10 s) | Peak memory, Codex turns | Peak memory, Claude turns | Codex turns per second | Claude turns per second | CPU per Codex turn | CPU per Claude turn |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-11-24 | `1bd449a3` | 2.9× | 33.5× | 11.2× | 0.2× | 11.0× | 11.1× | 2.4× | 2.2× | 2.9× | 2.4× |
| 2026-11-24 | `e2933272` | 3.0× | 35.5× | 11.1× | 3.1× | 10.8× | 11.2× | 1.7× | 1.8× | 2.1× | 2.0× |
| 2026-11-24 | `4b8d262f` | 3.0× | 26.8× | 11.3× | 0.2× | 10.8× | 11.1× | 2.5× | 2.3× | 3.1× | 2.6× |
| 2026-11-24 | `c68bda6c` | 2.9× | 35.7× | 11.1× | 2.8× | 11.3× | 11.3× | 1.8× | 1.6× | 2.2× | 1.9× |
| 2026-11-24 | `480c4073` | 2.9× | 35.3× | 11.2× | 3.9× | 10.9× | 11.3× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `52b62924` | 3.0× | 36.2× | 11.1× | 3.1× | 11.0× | 11.2× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `e10f06d2` | 3.0× | 36.4× | 11.1× | 3.2× | 11.0× | 11.2× | 1.8× | 1.7× | 2.2× | 1.9× |
| 2026-11-24 | `00b0e0d7` | 3.0× | 31.2× | 11.2× | 0.3× | 11.0× | 11.2× | 2.7× | 2.8× | 3.1× | 3.0× |
| 2026-11-24 | `ec02f406` | 3.0× | 31.1× | 11.3× | 2.6× | 11.0× | 11.3× | 3.2× | 3.0× | 3.7× | 3.1× |
