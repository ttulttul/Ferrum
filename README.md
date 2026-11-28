# Performance results

The Rust server against the pinned reference, measured by scripts/perf_compare.py on each push to main (.github/workflows/perf.yml); docs/performance.md on main explains the method. One file per run is under results/. This file is generated: do not edit it.

## Latest

Commit `26a4602cb20d`, reference `2e830d77`, 2026-11-28T08:25:24Z, AMD EPYC 7763 64-Core Processor (4 CPUs), Linux 7.0.0-1020-azure.

Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 74.7 MiB | 203 MiB | **2.7×** |
| Start to first RPC | 91.0 ms <sub>(84.2–101)</sub> | 3045 ms <sub>(2969–3115)</sub> | **33.5×** |
| Memory at idle (RSS) | 40.8 MiB <sub>(40.7–41.1)</sub> | 512 MiB <sub>(512–515)</sub> | **12.6×** |
| CPU while idle (per 10 s) | 10.0 ms <sub>(10.00–10.0)</sub> | 30.0 ms <sub>(20.0–290)</sub> | **3.0×** |
| CPU while idle, large database with clients (per 10 s) | 15.0 ms <sub>(10.0–20.0)</sub> | 10.0 ms <sub>(10.00–10.0)</sub> | **0.7×** |
| Memory at idle, large database | 163 MiB <sub>(146–180)</sub> | 518 MiB <sub>(516–520)</sub> | **3.2×** |
| Peak memory, 3 clients opening long threads | 396 MiB | 1062 MiB | **2.7×** |
| GET / p99 while 3 clients open long threads | 9.84 ms | 96.9 ms | **9.8×** |
| Peak memory, Codex turns | 59.1 MiB <sub>(58.1–59.2)</sub> | 640 MiB <sub>(638–656)</sub> | **10.8×** |
| Peak memory, Claude turns | 58.2 MiB <sub>(57.4–58.3)</sub> | 672 MiB <sub>(649–676)</sub> | **11.5×** |
| Codex turns per second | 4.39 /s <sub>(4.33–4.40)</sub> | 4.80 /s <sub>(4.49–4.80)</sub> | **0.9×** |
| Claude turns per second | 3.62 /s <sub>(3.56–3.70)</sub> | 3.87 /s <sub>(3.86–4.06)</sub> | **0.9×** |
| CPU per Codex turn | 274 ms <sub>(273–281)</sub> | 330 ms <sub>(329–360)</sub> | **1.2×** |
| CPU per Claude turn | 379 ms <sub>(364–384)</sub> | 432 ms <sub>(414–432)</sub> | **1.1×** |
| server.getConfig p50 | 3.51 ms <sub>(3.49–3.54)</sub> | 5.21 ms <sub>(5.16–5.53)</sub> | **1.5×** |
| server.getConfig p95 | 3.59 ms <sub>(3.59–3.64)</sub> | 6.45 ms <sub>(5.99–6.61)</sub> | **1.8×** |
| server.getSettings p50 | 0.57 ms <sub>(0.52–0.57)</sub> | 0.67 ms <sub>(0.66–0.67)</sub> | **1.2×** |
| server.getSettings p95 | 0.59 ms <sub>(0.54–0.61)</sub> | 0.89 ms <sub>(0.83–0.89)</sub> | **1.5×** |
| orchestration.subscribeShell p50 | 1.86 ms <sub>(1.82–1.87)</sub> | 4.04 ms <sub>(3.98–4.26)</sub> | **2.2×** |
| orchestration.subscribeShell p95 | 1.90 ms <sub>(1.90–1.92)</sub> | 5.89 ms <sub>(5.77–6.12)</sub> | **3.1×** |
| orchestration.subscribeThread p50 | 4.46 ms <sub>(4.45–4.53)</sub> | 16.1 ms <sub>(15.2–16.2)</sub> | **3.6×** |
| orchestration.subscribeThread p95 | 4.65 ms <sub>(4.64–4.72)</sub> | 18.7 ms <sub>(17.5–18.8)</sub> | **4.0×** |
| orchestration.getThreadProjection p50 | 2.85 ms <sub>(2.81–2.89)</sub> | 12.9 ms <sub>(12.8–13.1)</sub> | **4.5×** |
| orchestration.getThreadProjection p95 | 2.97 ms <sub>(2.95–3.03)</sub> | 14.3 ms <sub>(14.0–14.9)</sub> | **4.8×** |
| orchestration.getTurnDiff p50 | 1.74 ms <sub>(1.74–1.75)</sub> | 22.1 ms <sub>(22.1–23.5)</sub> | **12.7×** |
| orchestration.getTurnDiff p95 | 1.81 ms <sub>(1.78–1.82)</sub> | 25.7 ms <sub>(24.0–26.5)</sub> | **14.2×** |

Installed size: Rust is one binary (74.7 MiB). The reference is Node (143.4 MiB), its bundle (23.1 MiB) and 21 external packages (143.2 MiB), 309.6 MiB in all; the table leaves out the 106.7 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

## History

Ratios (Rust against the reference, higher is better for Rust) by commit, newest first.

| Date | Commit | Installed size | Start to first RPC | Memory at idle (RSS) | CPU while idle (per 10 s) | CPU while idle, large database with clients (per 10 s) | Memory at idle, large database | Peak memory, 3 clients opening long threads | GET / p99 while 3 clients open long threads | Peak memory, Codex turns | Peak memory, Claude turns | Codex turns per second | Claude turns per second | CPU per Codex turn | CPU per Claude turn |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-11-28 | `26a4602c` | 2.7× | 33.5× | 12.6× | 3.0× | 0.7× | 3.2× | 2.7× | 9.8× | 10.8× | 11.5× | 0.9× | 0.9× | 1.2× | 1.1× |
| 2026-11-27 | `ee1ce42a` | 2.7× | 35.0× | 12.4× | 3.0× | 1.5× | 3.3× | 2.7× | 23.2× | 10.5× | 11.4× | 0.8× | 0.8× | 1.1× | 1.0× |
| 2026-11-26 | `efb86860` | 2.8× | 33.4× | 11.1× | 2.0× | 1.3× | 3.2× | 2.7× | 22.5× | 11.0× | 11.7× | 2.4× | 2.3× | 2.9× | 2.5× |
| 2026-11-25 | `f2145255` | 2.9× | 36.0× | 11.1× | 3.0× | – | – | – | – | 11.6× | 11.2× | 2.7× | 2.4× | 3.2× | 2.6× |
| 2026-11-24 | `8c0f3e67` | 2.9× | 31.7× | 11.2× | 0.3× | – | – | – | – | 10.6× | 11.2× | 2.4× | 1.9× | 3.0× | 2.5× |
| 2026-11-24 | `1bd449a3` | 2.9× | 33.5× | 11.2× | 0.2× | – | – | – | – | 11.0× | 11.1× | 2.4× | 2.2× | 2.9× | 2.4× |
| 2026-11-24 | `e2933272` | 3.0× | 35.5× | 11.1× | 3.1× | – | – | – | – | 10.8× | 11.2× | 1.7× | 1.8× | 2.1× | 2.0× |
| 2026-11-24 | `4b8d262f` | 3.0× | 26.8× | 11.3× | 0.2× | – | – | – | – | 10.8× | 11.1× | 2.5× | 2.3× | 3.1× | 2.6× |
| 2026-11-24 | `c68bda6c` | 2.9× | 35.7× | 11.1× | 2.8× | – | – | – | – | 11.3× | 11.3× | 1.8× | 1.6× | 2.2× | 1.9× |
| 2026-11-24 | `480c4073` | 2.9× | 35.3× | 11.2× | 3.9× | – | – | – | – | 10.9× | 11.3× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `52b62924` | 3.0× | 36.2× | 11.1× | 3.1× | – | – | – | – | 11.0× | 11.2× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `e10f06d2` | 3.0× | 36.4× | 11.1× | 3.2× | – | – | – | – | 11.0× | 11.2× | 1.8× | 1.7× | 2.2× | 1.9× |
| 2026-11-24 | `00b0e0d7` | 3.0× | 31.2× | 11.2× | 0.3× | – | – | – | – | 11.0× | 11.2× | 2.7× | 2.8× | 3.1× | 3.0× |
| 2026-11-24 | `ec02f406` | 3.0× | 31.1× | 11.3× | 2.6× | – | – | – | – | 11.0× | 11.3× | 3.2× | 3.0× | 3.7× | 3.1× |
