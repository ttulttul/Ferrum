# Performance results

The Rust server against the pinned reference, measured by scripts/perf_compare.py on each push to main (.github/workflows/perf.yml); docs/performance.md on main explains the method. One file per run is under results/. This file is generated: do not edit it.

## Latest

Commit `00b0e0d7bd09`, reference `fd0331ca`, 2026-11-24T20:53:53Z, AMD EPYC 9V45 96-Core Processor (4 CPUs), Linux 7.0.0-1017-azure.

Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 68.1 MiB | 205 MiB | **3.0×** |
| Start to first RPC | 63.9 ms <sub>(60.2–76.8)</sub> | 1994 ms <sub>(1957–6043)</sub> | **31.2×** |
| Memory at idle (RSS) | 40.4 MiB <sub>(39.7–40.8)</sub> | 451 MiB <sub>(450–457)</sub> | **11.2×** |
| CPU while idle (per 10 s) | 80.0 ms <sub>(70.0–80.0)</sub> | 20.0 ms <sub>(10.00–230)</sub> | **0.3×** |
| Peak memory, Codex turns | 59.4 MiB <sub>(59.1–60.2)</sub> | 652 MiB <sub>(640–674)</sub> | **11.0×** |
| Peak memory, Claude turns | 59.1 MiB <sub>(59.0–59.5)</sub> | 660 MiB <sub>(659–708)</sub> | **11.2×** |
| Codex turns per second | 7.50 /s <sub>(7.28–7.72)</sub> | 2.74 /s <sub>(2.35–2.74)</sub> | **2.7×** |
| Claude turns per second | 6.32 /s <sub>(6.32–6.33)</sub> | 2.28 /s <sub>(2.18–2.38)</sub> | **2.8×** |
| CPU per Codex turn | 161 ms <sub>(157–164)</sub> | 500 ms <sub>(496–601)</sub> | **3.1×** |
| CPU per Claude turn | 216 ms <sub>(214–221)</sub> | 645 ms <sub>(620–677)</sub> | **3.0×** |
| server.getConfig p50 | 9.24 ms <sub>(8.84–9.30)</sub> | 2.60 ms <sub>(2.59–2.69)</sub> | **0.3×** |
| server.getConfig p95 | 10.4 ms <sub>(10.3–10.6)</sub> | 3.06 ms <sub>(3.01–3.60)</sub> | **0.3×** |
| server.getSettings p50 | 0.30 ms <sub>(0.28–0.31)</sub> | 0.40 ms <sub>(0.38–0.40)</sub> | **1.3×** |
| server.getSettings p95 | 0.32 ms <sub>(0.31–0.33)</sub> | 0.55 ms <sub>(0.50–1.01)</sub> | **1.7×** |
| orchestration.subscribeShell p50 | 0.89 ms <sub>(0.87–0.91)</sub> | 2.30 ms <sub>(2.29–2.45)</sub> | **2.6×** |
| orchestration.subscribeShell p95 | 0.94 ms <sub>(0.92–0.95)</sub> | 3.82 ms <sub>(3.63–4.13)</sub> | **4.1×** |
| orchestration.subscribeThread p50 | 3.00 ms <sub>(2.97–3.08)</sub> | 9.22 ms <sub>(9.19–9.59)</sub> | **3.1×** |
| orchestration.subscribeThread p95 | 3.24 ms <sub>(3.19–3.26)</sub> | 12.3 ms <sub>(11.5–13.0)</sub> | **3.8×** |
| orchestration.getThreadProjection p50 | 1.38 ms <sub>(1.38–1.40)</sub> | 7.53 ms <sub>(7.24–8.29)</sub> | **5.5×** |
| orchestration.getThreadProjection p95 | 1.46 ms <sub>(1.43–1.55)</sub> | 9.10 ms <sub>(8.64–9.45)</sub> | **6.2×** |
| orchestration.getTurnDiff p50 | 2.44 ms <sub>(2.43–2.45)</sub> | 46.1 ms <sub>(45.3–46.7)</sub> | **18.9×** |
| orchestration.getTurnDiff p95 | 2.63 ms <sub>(2.56–2.67)</sub> | 56.7 ms <sub>(53.2–58.1)</sub> | **21.6×** |

Installed size: Rust is one binary (68.1 MiB). The reference is Node (143.4 MiB), its bundle (22.6 MiB) and 23 external packages (145.2 MiB), 311.2 MiB in all; the table leaves out the 105.8 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

## History

Ratios (Rust against the reference, higher is better for Rust) by commit, newest first.

| Date | Commit | Installed size | Start to first RPC | Memory at idle (RSS) | CPU while idle (per 10 s) | Peak memory, Codex turns | Peak memory, Claude turns | Codex turns per second | Claude turns per second | CPU per Codex turn | CPU per Claude turn |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-11-24 | `00b0e0d7` | 3.0× | 31.2× | 11.2× | 0.3× | 11.0× | 11.2× | 2.7× | 2.8× | 3.1× | 3.0× |
| 2026-11-24 | `ec02f406` | 3.0× | 31.1× | 11.3× | 2.6× | 11.0× | 11.3× | 3.2× | 3.0× | 3.7× | 3.1× |
