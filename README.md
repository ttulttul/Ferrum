# Performance results

The Rust server against the pinned reference, measured by scripts/perf_compare.py on each push to main (.github/workflows/perf.yml); docs/performance.md on main explains the method. One file per run is under results/. This file is generated: do not edit it.

## Latest

Commit `c68bda6c0d48`, reference `fd0331ca`, 2026-11-24T21:54:17Z, Intel(R) Xeon(R) 6975P-C (4 CPUs), Linux 7.0.0-1014-aws.

Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 69.7 MiB | 205 MiB | **2.9×** |
| Start to first RPC | 58.7 ms <sub>(56.6–61.9)</sub> | 2097 ms <sub>(2088–2103)</sub> | **35.7×** |
| Memory at idle (RSS) | 40.7 MiB <sub>(39.6–41.1)</sub> | 451 MiB <sub>(450–452)</sub> | **11.1×** |
| CPU while idle (per 10 s) | 100 ms <sub>(90.0–100)</sub> | 280 ms <sub>(30.0–290)</sub> | **2.8×** |
| Peak memory, Codex turns | 58.8 MiB <sub>(58.8–59.4)</sub> | 662 MiB <sub>(656–663)</sub> | **11.3×** |
| Peak memory, Claude turns | 59.0 MiB <sub>(58.3–60.1)</sub> | 666 MiB <sub>(665–669)</sub> | **11.3×** |
| Codex turns per second | 6.75 /s <sub>(6.66–6.86)</sub> | 3.77 /s <sub>(3.75–3.95)</sub> | **1.8×** |
| Claude turns per second | 5.79 /s <sub>(5.77–5.81)</sub> | 3.51 /s <sub>(3.51–3.61)</sub> | **1.6×** |
| CPU per Codex turn | 181 ms <sub>(177–182)</sub> | 394 ms <sub>(373–395)</sub> | **2.2×** |
| CPU per Claude turn | 238 ms <sub>(235–239)</sub> | 444 ms <sub>(436–447)</sub> | **1.9×** |
| server.getConfig p50 | 8.01 ms <sub>(8.01–8.12)</sub> | 3.57 ms <sub>(3.51–3.60)</sub> | **0.4×** |
| server.getConfig p95 | 8.44 ms <sub>(8.43–8.48)</sub> | 4.21 ms <sub>(4.14–4.25)</sub> | **0.5×** |
| server.getSettings p50 | 0.38 ms <sub>(0.37–0.38)</sub> | 0.59 ms <sub>(0.56–0.64)</sub> | **1.6×** |
| server.getSettings p95 | 0.40 ms <sub>(0.40–0.41)</sub> | 0.79 ms <sub>(0.69–0.79)</sub> | **1.9×** |
| orchestration.subscribeShell p50 | 1.17 ms <sub>(1.16–1.21)</sub> | 3.15 ms <sub>(3.11–3.20)</sub> | **2.7×** |
| orchestration.subscribeShell p95 | 1.28 ms <sub>(1.26–1.34)</sub> | 4.86 ms <sub>(4.74–4.99)</sub> | **3.8×** |
| orchestration.subscribeThread p50 | 4.19 ms <sub>(4.15–4.19)</sub> | 11.9 ms <sub>(11.9–11.9)</sub> | **2.8×** |
| orchestration.subscribeThread p95 | 4.55 ms <sub>(4.46–4.56)</sub> | 13.6 ms <sub>(13.4–13.9)</sub> | **3.0×** |
| orchestration.getThreadProjection p50 | 1.91 ms <sub>(1.87–2.01)</sub> | 10.1 ms <sub>(9.90–10.1)</sub> | **5.3×** |
| orchestration.getThreadProjection p95 | 2.15 ms <sub>(1.91–2.29)</sub> | 10.9 ms <sub>(10.9–11.1)</sub> | **5.1×** |
| orchestration.getTurnDiff p50 | 1.95 ms <sub>(1.92–1.95)</sub> | 26.8 ms <sub>(26.7–27.3)</sub> | **13.8×** |
| orchestration.getTurnDiff p95 | 2.11 ms <sub>(2.10–2.14)</sub> | 29.2 ms <sub>(28.6–29.5)</sub> | **13.9×** |

Installed size: Rust is one binary (69.7 MiB). The reference is Node (143.4 MiB), its bundle (22.6 MiB) and 23 external packages (145.2 MiB), 311.2 MiB in all; the table leaves out the 105.8 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

## History

Ratios (Rust against the reference, higher is better for Rust) by commit, newest first.

| Date | Commit | Installed size | Start to first RPC | Memory at idle (RSS) | CPU while idle (per 10 s) | Peak memory, Codex turns | Peak memory, Claude turns | Codex turns per second | Claude turns per second | CPU per Codex turn | CPU per Claude turn |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-11-24 | `c68bda6c` | 2.9× | 35.7× | 11.1× | 2.8× | 11.3× | 11.3× | 1.8× | 1.6× | 2.2× | 1.9× |
| 2026-11-24 | `480c4073` | 2.9× | 35.3× | 11.2× | 3.9× | 10.9× | 11.3× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `52b62924` | 3.0× | 36.2× | 11.1× | 3.1× | 11.0× | 11.2× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `e10f06d2` | 3.0× | 36.4× | 11.1× | 3.2× | 11.0× | 11.2× | 1.8× | 1.7× | 2.2× | 1.9× |
| 2026-11-24 | `00b0e0d7` | 3.0× | 31.2× | 11.2× | 0.3× | 11.0× | 11.2× | 2.7× | 2.8× | 3.1× | 3.0× |
| 2026-11-24 | `ec02f406` | 3.0× | 31.1× | 11.3× | 2.6× | 11.0× | 11.3× | 3.2× | 3.0× | 3.7× | 3.1× |
