# Performance results

The Rust server against the pinned reference, measured by scripts/perf_compare.py on each push to main (.github/workflows/perf.yml); docs/performance.md on main explains the method. One file per run is under results/. This file is generated: do not edit it.

## Latest

Commit `480c40738827`, reference `fd0331ca`, 2026-11-24T21:30:09Z, Intel(R) Xeon(R) 6975P-C (4 CPUs), Linux 7.0.0-1014-aws.

Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 69.9 MiB | 205 MiB | **2.9×** |
| Start to first RPC | 57.9 ms <sub>(56.5–64.6)</sub> | 2045 ms <sub>(2029–2076)</sub> | **35.3×** |
| Memory at idle (RSS) | 40.4 MiB <sub>(40.2–41.1)</sub> | 451 MiB <sub>(450–452)</sub> | **11.2×** |
| CPU while idle (per 10 s) | 70.0 ms <sub>(70.0–80.0)</sub> | 270 ms <sub>(200–270)</sub> | **3.9×** |
| Peak memory, Codex turns | 60.1 MiB <sub>(58.3–60.3)</sub> | 655 MiB <sub>(650–692)</sub> | **10.9×** |
| Peak memory, Claude turns | 59.0 MiB <sub>(59.0–60.4)</sub> | 665 MiB <sub>(664–711)</sub> | **11.3×** |
| Codex turns per second | 6.94 /s <sub>(6.94–6.96)</sub> | 4.15 /s <sub>(4.14–4.19)</sub> | **1.7×** |
| Claude turns per second | 5.85 /s <sub>(5.78–5.87)</sub> | 3.62 /s <sub>(3.58–3.71)</sub> | **1.6×** |
| CPU per Codex turn | 174 ms <sub>(174–175)</sub> | 359 ms <sub>(354–362)</sub> | **2.1×** |
| CPU per Claude turn | 235 ms <sub>(234–238)</sub> | 435 ms <sub>(419–437)</sub> | **1.9×** |
| server.getConfig p50 | 7.74 ms <sub>(7.67–7.76)</sub> | 3.47 ms <sub>(3.43–3.59)</sub> | **0.4×** |
| server.getConfig p95 | 8.16 ms <sub>(8.14–8.17)</sub> | 4.01 ms <sub>(4.00–4.28)</sub> | **0.5×** |
| server.getSettings p50 | 0.43 ms <sub>(0.35–0.44)</sub> | 0.61 ms <sub>(0.61–0.63)</sub> | **1.4×** |
| server.getSettings p95 | 0.52 ms <sub>(0.45–0.53)</sub> | 0.74 ms <sub>(0.71–0.77)</sub> | **1.4×** |
| orchestration.subscribeShell p50 | 1.24 ms <sub>(1.23–1.26)</sub> | 3.04 ms <sub>(2.93–3.05)</sub> | **2.4×** |
| orchestration.subscribeShell p95 | 1.29 ms <sub>(1.26–1.29)</sub> | 4.44 ms <sub>(4.09–4.53)</sub> | **3.4×** |
| orchestration.subscribeThread p50 | 4.08 ms <sub>(4.05–4.10)</sub> | 11.7 ms <sub>(11.7–11.9)</sub> | **2.9×** |
| orchestration.subscribeThread p95 | 4.38 ms <sub>(4.35–4.43)</sub> | 13.3 ms <sub>(12.8–13.4)</sub> | **3.0×** |
| orchestration.getThreadProjection p50 | 1.88 ms <sub>(1.84–1.99)</sub> | 9.80 ms <sub>(9.63–9.98)</sub> | **5.2×** |
| orchestration.getThreadProjection p95 | 2.13 ms <sub>(2.05–2.16)</sub> | 10.7 ms <sub>(10.7–10.7)</sub> | **5.0×** |
| orchestration.getTurnDiff p50 | 1.88 ms <sub>(1.87–1.89)</sub> | 26.1 ms <sub>(25.9–26.4)</sub> | **13.9×** |
| orchestration.getTurnDiff p95 | 2.03 ms <sub>(2.02–2.04)</sub> | 28.2 ms <sub>(27.6–28.6)</sub> | **13.9×** |

Installed size: Rust is one binary (69.9 MiB). The reference is Node (143.4 MiB), its bundle (22.6 MiB) and 23 external packages (145.2 MiB), 311.2 MiB in all; the table leaves out the 105.8 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

## History

Ratios (Rust against the reference, higher is better for Rust) by commit, newest first.

| Date | Commit | Installed size | Start to first RPC | Memory at idle (RSS) | CPU while idle (per 10 s) | Peak memory, Codex turns | Peak memory, Claude turns | Codex turns per second | Claude turns per second | CPU per Codex turn | CPU per Claude turn |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-11-24 | `480c4073` | 2.9× | 35.3× | 11.2× | 3.9× | 10.9× | 11.3× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `52b62924` | 3.0× | 36.2× | 11.1× | 3.1× | 11.0× | 11.2× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `e10f06d2` | 3.0× | 36.4× | 11.1× | 3.2× | 11.0× | 11.2× | 1.8× | 1.7× | 2.2× | 1.9× |
| 2026-11-24 | `00b0e0d7` | 3.0× | 31.2× | 11.2× | 0.3× | 11.0× | 11.2× | 2.7× | 2.8× | 3.1× | 3.0× |
| 2026-11-24 | `ec02f406` | 3.0× | 31.1× | 11.3× | 2.6× | 11.0× | 11.3× | 3.2× | 3.0× | 3.7× | 3.1× |
