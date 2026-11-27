# Performance results

The Rust server against the pinned reference, measured by scripts/perf_compare.py on each push to main (.github/workflows/perf.yml); docs/performance.md on main explains the method. One file per run is under results/. This file is generated: do not edit it.

## Latest

Commit `ee1ce42a4f57`, reference `44a53337`, 2026-11-27T05:48:10Z, Intel(R) Xeon(R) 6975P-C (4 CPUs), Linux 7.0.0-1018-aws.

Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 74.1 MiB | 203 MiB | **2.7×** |
| Start to first RPC | 58.3 ms <sub>(55.7–61.9)</sub> | 2040 ms <sub>(2006–2068)</sub> | **35.0×** |
| Memory at idle (RSS) | 41.6 MiB <sub>(41.2–41.9)</sub> | 514 MiB <sub>(513–518)</sub> | **12.4×** |
| CPU while idle (per 10 s) | 10.0 ms <sub>(0.00–10.0)</sub> | 30.0 ms <sub>(30.0–40.0)</sub> | **3.0×** |
| CPU while idle, large database with clients (per 10 s) | 20.0 ms <sub>(20.0–20.0)</sub> | 30.0 ms <sub>(30.0–30.0)</sub> | **1.5×** |
| Memory at idle, large database | 155 MiB <sub>(151–159)</sub> | 517 MiB <sub>(517–517)</sub> | **3.3×** |
| Peak memory, 3 clients opening long threads | 403 MiB | 1090 MiB | **2.7×** |
| GET / p99 while 3 clients open long threads | 1.92 ms | 44.4 ms | **23.2×** |
| Peak memory, Codex turns | 60.4 MiB <sub>(58.7–61.0)</sub> | 636 MiB <sub>(636–654)</sub> | **10.5×** |
| Peak memory, Claude turns | 57.7 MiB <sub>(55.2–57.8)</sub> | 655 MiB <sub>(653–681)</sub> | **11.4×** |
| Codex turns per second | 5.72 /s <sub>(5.65–5.76)</sub> | 7.18 /s <sub>(7.13–7.19)</sub> | **0.8×** |
| Claude turns per second | 4.64 /s <sub>(4.15–4.69)</sub> | 5.86 /s <sub>(5.76–5.92)</sub> | **0.8×** |
| CPU per Codex turn | 209 ms <sub>(208–212)</sub> | 227 ms <sub>(226–230)</sub> | **1.1×** |
| CPU per Claude turn | 289 ms <sub>(286–328)</sub> | 294 ms <sub>(292–300)</sub> | **1.0×** |
| server.getConfig p50 | 2.83 ms <sub>(2.72–2.95)</sub> | 3.84 ms <sub>(3.81–3.91)</sub> | **1.4×** |
| server.getConfig p95 | 3.14 ms <sub>(3.00–3.22)</sub> | 4.60 ms <sub>(4.45–4.89)</sub> | **1.5×** |
| server.getSettings p50 | 0.34 ms <sub>(0.34–0.44)</sub> | 0.60 ms <sub>(0.57–0.63)</sub> | **1.8×** |
| server.getSettings p95 | 0.38 ms <sub>(0.36–0.47)</sub> | 0.82 ms <sub>(0.76–0.83)</sub> | **2.2×** |
| orchestration.subscribeShell p50 | 1.47 ms <sub>(1.37–1.64)</sub> | 2.92 ms <sub>(2.91–2.96)</sub> | **2.0×** |
| orchestration.subscribeShell p95 | 1.58 ms <sub>(1.51–1.84)</sub> | 5.32 ms <sub>(4.79–5.45)</sub> | **3.4×** |
| orchestration.subscribeThread p50 | 3.85 ms <sub>(3.80–3.87)</sub> | 11.6 ms <sub>(11.5–11.6)</sub> | **3.0×** |
| orchestration.subscribeThread p95 | 4.32 ms <sub>(4.20–4.42)</sub> | 13.7 ms <sub>(13.4–13.9)</sub> | **3.2×** |
| orchestration.getThreadProjection p50 | 2.24 ms <sub>(2.19–2.25)</sub> | 9.54 ms <sub>(9.21–9.62)</sub> | **4.3×** |
| orchestration.getThreadProjection p95 | 2.43 ms <sub>(2.41–2.46)</sub> | 10.8 ms <sub>(10.7–11.0)</sub> | **4.4×** |
| orchestration.getTurnDiff p50 | 1.03 ms <sub>(1.02–1.03)</sub> | 14.3 ms <sub>(14.3–14.4)</sub> | **14.0×** |
| orchestration.getTurnDiff p95 | 1.20 ms <sub>(1.20–1.20)</sub> | 16.1 ms <sub>(15.8–16.2)</sub> | **13.4×** |

Installed size: Rust is one binary (74.1 MiB). The reference is Node (143.4 MiB), its bundle (23.1 MiB) and 21 external packages (143.2 MiB), 309.6 MiB in all; the table leaves out the 106.7 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

## History

Ratios (Rust against the reference, higher is better for Rust) by commit, newest first.

| Date | Commit | Installed size | Start to first RPC | Memory at idle (RSS) | CPU while idle (per 10 s) | CPU while idle, large database with clients (per 10 s) | Memory at idle, large database | Peak memory, 3 clients opening long threads | GET / p99 while 3 clients open long threads | Peak memory, Codex turns | Peak memory, Claude turns | Codex turns per second | Claude turns per second | CPU per Codex turn | CPU per Claude turn |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
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
