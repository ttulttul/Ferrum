# Performance results

The Rust server against the pinned reference, measured by scripts/perf_compare.py on each push to main (.github/workflows/perf.yml); docs/performance.md on main explains the method. One file per run is under results/. This file is generated: do not edit it.

## Latest

Commit `e2933272a70d`, reference `fd0331ca`, 2026-11-24T22:39:59Z, Intel(R) Xeon(R) 6975P-C (4 CPUs), Linux 7.0.0-1014-aws.

Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 69.6 MiB | 205 MiB | **3.0×** |
| Start to first RPC | 60.0 ms <sub>(52.5–61.1)</sub> | 2127 ms <sub>(2094–2153)</sub> | **35.5×** |
| Memory at idle (RSS) | 40.7 MiB <sub>(39.6–40.9)</sub> | 452 MiB <sub>(450–452)</sub> | **11.1×** |
| CPU while idle (per 10 s) | 90.0 ms <sub>(80.0–100)</sub> | 280 ms <sub>(20.0–290)</sub> | **3.1×** |
| Peak memory, Codex turns | 60.5 MiB <sub>(59.8–60.8)</sub> | 656 MiB <sub>(655–664)</sub> | **10.8×** |
| Peak memory, Claude turns | 59.4 MiB <sub>(59.0–59.5)</sub> | 667 MiB <sub>(664–693)</sub> | **11.2×** |
| Codex turns per second | 6.74 /s <sub>(6.71–6.82)</sub> | 3.90 /s <sub>(3.89–4.03)</sub> | **1.7×** |
| Claude turns per second | 5.71 /s <sub>(5.69–5.71)</sub> | 3.19 /s <sub>(3.11–3.32)</sub> | **1.8×** |
| CPU per Codex turn | 182 ms <sub>(178–182)</sub> | 381 ms <sub>(366–384)</sub> | **2.1×** |
| CPU per Claude turn | 242 ms <sub>(242–243)</sub> | 488 ms <sub>(469–490)</sub> | **2.0×** |
| server.getConfig p50 | 8.06 ms <sub>(8.06–8.21)</sub> | 3.54 ms <sub>(3.40–3.55)</sub> | **0.4×** |
| server.getConfig p95 | 8.50 ms <sub>(8.37–8.53)</sub> | 4.08 ms <sub>(4.03–4.19)</sub> | **0.5×** |
| server.getSettings p50 | 0.39 ms <sub>(0.36–0.44)</sub> | 0.60 ms <sub>(0.57–0.62)</sub> | **1.5×** |
| server.getSettings p95 | 0.41 ms <sub>(0.39–0.46)</sub> | 0.73 ms <sub>(0.72–0.78)</sub> | **1.8×** |
| orchestration.subscribeShell p50 | 1.25 ms <sub>(1.21–1.26)</sub> | 3.09 ms <sub>(3.02–3.09)</sub> | **2.5×** |
| orchestration.subscribeShell p95 | 1.31 ms <sub>(1.30–1.31)</sub> | 4.74 ms <sub>(4.71–5.18)</sub> | **3.6×** |
| orchestration.subscribeThread p50 | 4.12 ms <sub>(4.02–4.23)</sub> | 12.2 ms <sub>(11.9–12.3)</sub> | **3.0×** |
| orchestration.subscribeThread p95 | 4.55 ms <sub>(4.46–4.57)</sub> | 13.9 ms <sub>(13.9–14.0)</sub> | **3.1×** |
| orchestration.getThreadProjection p50 | 1.86 ms <sub>(1.82–1.88)</sub> | 10.2 ms <sub>(9.98–10.3)</sub> | **5.5×** |
| orchestration.getThreadProjection p95 | 1.90 ms <sub>(1.88–2.19)</sub> | 11.2 ms <sub>(11.1–11.2)</sub> | **5.9×** |
| orchestration.getTurnDiff p50 | 1.97 ms <sub>(1.93–1.98)</sub> | 27.6 ms <sub>(27.5–28.3)</sub> | **14.0×** |
| orchestration.getTurnDiff p95 | 2.18 ms <sub>(2.13–2.19)</sub> | 29.9 ms <sub>(29.6–31.2)</sub> | **13.7×** |

Installed size: Rust is one binary (69.6 MiB). The reference is Node (143.4 MiB), its bundle (22.6 MiB) and 23 external packages (145.2 MiB), 311.2 MiB in all; the table leaves out the 105.8 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

## History

Ratios (Rust against the reference, higher is better for Rust) by commit, newest first.

| Date | Commit | Installed size | Start to first RPC | Memory at idle (RSS) | CPU while idle (per 10 s) | Peak memory, Codex turns | Peak memory, Claude turns | Codex turns per second | Claude turns per second | CPU per Codex turn | CPU per Claude turn |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-11-24 | `e2933272` | 3.0× | 35.5× | 11.1× | 3.1× | 10.8× | 11.2× | 1.7× | 1.8× | 2.1× | 2.0× |
| 2026-11-24 | `4b8d262f` | 3.0× | 26.8× | 11.3× | 0.2× | 10.8× | 11.1× | 2.5× | 2.3× | 3.1× | 2.6× |
| 2026-11-24 | `c68bda6c` | 2.9× | 35.7× | 11.1× | 2.8× | 11.3× | 11.3× | 1.8× | 1.6× | 2.2× | 1.9× |
| 2026-11-24 | `480c4073` | 2.9× | 35.3× | 11.2× | 3.9× | 10.9× | 11.3× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `52b62924` | 3.0× | 36.2× | 11.1× | 3.1× | 11.0× | 11.2× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `e10f06d2` | 3.0× | 36.4× | 11.1× | 3.2× | 11.0× | 11.2× | 1.8× | 1.7× | 2.2× | 1.9× |
| 2026-11-24 | `00b0e0d7` | 3.0× | 31.2× | 11.2× | 0.3× | 11.0× | 11.2× | 2.7× | 2.8× | 3.1× | 3.0× |
| 2026-11-24 | `ec02f406` | 3.0× | 31.1× | 11.3× | 2.6× | 11.0× | 11.3× | 3.2× | 3.0× | 3.7× | 3.1× |
