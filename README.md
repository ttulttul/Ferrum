# Performance results

The Rust server against the pinned reference, measured by scripts/perf_compare.py on each push to main (.github/workflows/perf.yml); docs/performance.md on main explains the method. One file per run is under results/. This file is generated: do not edit it.

## Latest

Commit `efb86860023a`, reference `fd0331ca`, 2026-11-26T03:58:37Z, AMD EPYC 7763 64-Core Processor (4 CPUs), Linux 7.0.0-1017-azure.

Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 73.2 MiB | 205 MiB | **2.8×** |
| Start to first RPC | 90.5 ms <sub>(88.6–94.2)</sub> | 3019 ms <sub>(2874–3172)</sub> | **33.4×** |
| Memory at idle (RSS) | 41.0 MiB <sub>(40.6–41.1)</sub> | 453 MiB <sub>(452–456)</sub> | **11.1×** |
| CPU while idle (per 10 s) | 10.00 ms <sub>(0.00–20.0)</sub> | 20.0 ms <sub>(10.00–40.0)</sub> | **2.0×** |
| CPU while idle, large database with clients (per 10 s) | 15.0 ms <sub>(10.0–20.0)</sub> | 20.0 ms <sub>(20.0–20.0)</sub> | **1.3×** |
| Memory at idle, large database | 162 MiB <sub>(155–169)</sub> | 519 MiB <sub>(516–522)</sub> | **3.2×** |
| Peak memory, 3 clients opening long threads | 428 MiB | 1158 MiB | **2.7×** |
| GET / p99 while 3 clients open long threads | 3.07 ms | 68.9 ms | **22.5×** |
| Peak memory, Codex turns | 59.5 MiB <sub>(58.6–59.6)</sub> | 654 MiB <sub>(653–654)</sub> | **11.0×** |
| Peak memory, Claude turns | 56.8 MiB <sub>(56.6–57.1)</sub> | 667 MiB <sub>(661–711)</sub> | **11.7×** |
| Codex turns per second | 4.53 /s <sub>(4.48–4.59)</sub> | 1.90 /s <sub>(1.81–2.10)</sub> | **2.4×** |
| Claude turns per second | 3.89 /s <sub>(3.87–3.91)</sub> | 1.72 /s <sub>(1.70–1.76)</sub> | **2.3×** |
| CPU per Codex turn | 264 ms <sub>(263–266)</sub> | 762 ms <sub>(702–802)</sub> | **2.9×** |
| CPU per Claude turn | 356 ms <sub>(354–359)</sub> | 874 ms <sub>(854–895)</sub> | **2.5×** |
| server.getConfig p50 | 3.49 ms <sub>(3.39–3.50)</sub> | 4.85 ms <sub>(4.73–5.03)</sub> | **1.4×** |
| server.getConfig p95 | 3.64 ms <sub>(3.61–3.69)</sub> | 5.91 ms <sub>(5.50–6.21)</sub> | **1.6×** |
| server.getSettings p50 | 0.59 ms <sub>(0.59–0.60)</sub> | 0.68 ms <sub>(0.67–0.68)</sub> | **1.2×** |
| server.getSettings p95 | 0.62 ms <sub>(0.62–0.63)</sub> | 0.86 ms <sub>(0.84–0.92)</sub> | **1.4×** |
| orchestration.subscribeShell p50 | 1.86 ms <sub>(1.85–1.87)</sub> | 4.28 ms <sub>(4.14–4.41)</sub> | **2.3×** |
| orchestration.subscribeShell p95 | 1.91 ms <sub>(1.90–2.00)</sub> | 5.77 ms <sub>(5.44–7.21)</sub> | **3.0×** |
| orchestration.subscribeThread p50 | 4.43 ms <sub>(4.39–4.47)</sub> | 16.8 ms <sub>(16.7–17.4)</sub> | **3.8×** |
| orchestration.subscribeThread p95 | 4.61 ms <sub>(4.58–4.62)</sub> | 19.6 ms <sub>(18.6–19.7)</sub> | **4.3×** |
| orchestration.getThreadProjection p50 | 2.92 ms <sub>(2.86–2.99)</sub> | 14.0 ms <sub>(13.9–14.1)</sub> | **4.8×** |
| orchestration.getThreadProjection p95 | 3.06 ms <sub>(3.05–3.09)</sub> | 15.2 ms <sub>(15.2–15.4)</sub> | **5.0×** |
| orchestration.getTurnDiff p50 | 1.75 ms <sub>(1.74–1.75)</sub> | 55.8 ms <sub>(52.0–56.3)</sub> | **32.0×** |
| orchestration.getTurnDiff p95 | 1.80 ms <sub>(1.79–1.84)</sub> | 66.0 ms <sub>(60.6–71.0)</sub> | **36.7×** |

Installed size: Rust is one binary (73.2 MiB). The reference is Node (143.4 MiB), its bundle (22.6 MiB) and 23 external packages (145.2 MiB), 311.2 MiB in all; the table leaves out the 105.8 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

## History

Ratios (Rust against the reference, higher is better for Rust) by commit, newest first.

| Date | Commit | Installed size | Start to first RPC | Memory at idle (RSS) | CPU while idle (per 10 s) | CPU while idle, large database with clients (per 10 s) | Memory at idle, large database | Peak memory, 3 clients opening long threads | GET / p99 while 3 clients open long threads | Peak memory, Codex turns | Peak memory, Claude turns | Codex turns per second | Claude turns per second | CPU per Codex turn | CPU per Claude turn |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
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
