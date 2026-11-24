# Performance results

The Rust server against the pinned reference, measured by scripts/perf_compare.py on each push to main (.github/workflows/perf.yml); docs/performance.md on main explains the method. One file per run is under results/. This file is generated: do not edit it.

## Latest

Commit `e10f06d228a6`, reference `fd0331ca`, 2026-11-24T21:06:21Z, Intel(R) Xeon(R) 6975P-C (4 CPUs), Linux 7.0.0-1014-aws.

Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 68.1 MiB | 205 MiB | **3.0×** |
| Start to first RPC | 59.8 ms <sub>(56.8–62.4)</sub> | 2175 ms <sub>(2160–2188)</sub> | **36.4×** |
| Memory at idle (RSS) | 40.6 MiB <sub>(40.3–40.9)</sub> | 451 MiB <sub>(450–452)</sub> | **11.1×** |
| CPU while idle (per 10 s) | 90.0 ms <sub>(90.0–100.0)</sub> | 290 ms <sub>(20.0–290)</sub> | **3.2×** |
| Peak memory, Codex turns | 59.8 MiB <sub>(59.2–60.9)</sub> | 655 MiB <sub>(653–656)</sub> | **11.0×** |
| Peak memory, Claude turns | 59.3 MiB <sub>(57.5–60.1)</sub> | 662 MiB <sub>(662–667)</sub> | **11.2×** |
| Codex turns per second | 6.86 /s <sub>(6.76–6.93)</sub> | 3.85 /s <sub>(3.80–3.87)</sub> | **1.8×** |
| Claude turns per second | 5.80 /s <sub>(5.78–5.80)</sub> | 3.35 /s <sub>(3.33–3.36)</sub> | **1.7×** |
| CPU per Codex turn | 178 ms <sub>(176–180)</sub> | 386 ms <sub>(377–391)</sub> | **2.2×** |
| CPU per Claude turn | 239 ms <sub>(238–240)</sub> | 464 ms <sub>(461–472)</sub> | **1.9×** |
| server.getConfig p50 | 8.13 ms <sub>(8.04–8.32)</sub> | 3.60 ms <sub>(3.52–3.67)</sub> | **0.4×** |
| server.getConfig p95 | 8.54 ms <sub>(8.39–8.66)</sub> | 4.25 ms <sub>(4.08–4.43)</sub> | **0.5×** |
| server.getSettings p50 | 0.44 ms <sub>(0.37–0.45)</sub> | 0.61 ms <sub>(0.60–0.66)</sub> | **1.4×** |
| server.getSettings p95 | 0.47 ms <sub>(0.46–0.58)</sub> | 0.83 ms <sub>(0.79–0.83)</sub> | **1.7×** |
| orchestration.subscribeShell p50 | 1.19 ms <sub>(1.17–1.21)</sub> | 3.11 ms <sub>(3.10–3.12)</sub> | **2.6×** |
| orchestration.subscribeShell p95 | 1.29 ms <sub>(1.28–1.29)</sub> | 4.83 ms <sub>(4.58–4.84)</sub> | **3.8×** |
| orchestration.subscribeThread p50 | 4.09 ms <sub>(4.07–4.12)</sub> | 12.2 ms <sub>(12.2–12.3)</sub> | **3.0×** |
| orchestration.subscribeThread p95 | 4.45 ms <sub>(4.43–4.54)</sub> | 13.9 ms <sub>(13.8–14.3)</sub> | **3.1×** |
| orchestration.getThreadProjection p50 | 1.95 ms <sub>(1.79–1.97)</sub> | 10.2 ms <sub>(9.97–10.4)</sub> | **5.2×** |
| orchestration.getThreadProjection p95 | 2.13 ms <sub>(2.05–2.16)</sub> | 11.3 ms <sub>(11.1–11.4)</sub> | **5.3×** |
| orchestration.getTurnDiff p50 | 1.95 ms <sub>(1.94–1.99)</sub> | 28.1 ms <sub>(28.0–28.1)</sub> | **14.4×** |
| orchestration.getTurnDiff p95 | 2.12 ms <sub>(2.07–2.12)</sub> | 30.3 ms <sub>(30.2–30.3)</sub> | **14.3×** |

Installed size: Rust is one binary (68.1 MiB). The reference is Node (143.4 MiB), its bundle (22.6 MiB) and 23 external packages (145.2 MiB), 311.2 MiB in all; the table leaves out the 105.8 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

## History

Ratios (Rust against the reference, higher is better for Rust) by commit, newest first.

| Date | Commit | Installed size | Start to first RPC | Memory at idle (RSS) | CPU while idle (per 10 s) | Peak memory, Codex turns | Peak memory, Claude turns | Codex turns per second | Claude turns per second | CPU per Codex turn | CPU per Claude turn |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-11-24 | `e10f06d2` | 3.0× | 36.4× | 11.1× | 3.2× | 11.0× | 11.2× | 1.8× | 1.7× | 2.2× | 1.9× |
| 2026-11-24 | `00b0e0d7` | 3.0× | 31.2× | 11.2× | 0.3× | 11.0× | 11.2× | 2.7× | 2.8× | 3.1× | 3.0× |
| 2026-11-24 | `ec02f406` | 3.0× | 31.1× | 11.3× | 2.6× | 11.0× | 11.3× | 3.2× | 3.0× | 3.7× | 3.1× |
