# Performance results

The Rust server against the pinned reference, measured by scripts/perf_compare.py on each push to main (.github/workflows/perf.yml); docs/performance.md on main explains the method. One file per run is under results/. This file is generated: do not edit it.

## Latest

Commit `ec02f4069944`, reference `fd0331ca`, 2026-11-24T20:30:59Z, AMD EPYC 9V45 96-Core Processor (4 CPUs), Linux 7.0.0-1017-azure.

Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 67.7 MiB | 205 MiB | **3.0×** |
| Start to first RPC | 67.8 ms <sub>(61.9–84.3)</sub> | 2112 ms <sub>(2044–2156)</sub> | **31.1×** |
| Memory at idle (RSS) | 39.9 MiB <sub>(39.2–40.4)</sub> | 451 MiB <sub>(450–451)</sub> | **11.3×** |
| CPU while idle (per 10 s) | 90.0 ms <sub>(80.0–100)</sub> | 230 ms <sub>(230–240)</sub> | **2.6×** |
| Peak memory, Codex turns | 59.5 MiB <sub>(59.1–59.6)</sub> | 655 MiB <sub>(641–686)</sub> | **11.0×** |
| Peak memory, Claude turns | 58.4 MiB <sub>(58.2–58.8)</sub> | 660 MiB <sub>(657–662)</sub> | **11.3×** |
| Codex turns per second | 6.88 /s <sub>(6.70–6.91)</sub> | 2.13 /s <sub>(2.09–2.17)</sub> | **3.2×** |
| Claude turns per second | 5.77 /s <sub>(5.76–5.96)</sub> | 1.92 /s <sub>(1.82–2.05)</sub> | **3.0×** |
| CPU per Codex turn | 177 ms <sub>(175–184)</sub> | 659 ms <sub>(644–673)</sub> | **3.7×** |
| CPU per Claude turn | 244 ms <sub>(234–246)</sub> | 757 ms <sub>(711–797)</sub> | **3.1×** |
| server.getConfig p50 | 10.2 ms <sub>(9.79–10.6)</sub> | 2.65 ms <sub>(2.65–3.04)</sub> | **0.3×** |
| server.getConfig p95 | 11.4 ms <sub>(10.7–11.7)</sub> | 3.77 ms <sub>(3.33–4.33)</sub> | **0.3×** |
| server.getSettings p50 | 0.28 ms <sub>(0.27–0.30)</sub> | 0.40 ms <sub>(0.40–0.42)</sub> | **1.4×** |
| server.getSettings p95 | 0.32 ms <sub>(0.30–0.34)</sub> | 0.56 ms <sub>(0.53–0.60)</sub> | **1.8×** |
| orchestration.subscribeShell p50 | 0.89 ms <sub>(0.87–0.90)</sub> | 2.47 ms <sub>(2.36–2.51)</sub> | **2.8×** |
| orchestration.subscribeShell p95 | 0.94 ms <sub>(0.91–0.95)</sub> | 4.07 ms <sub>(3.98–4.54)</sub> | **4.3×** |
| orchestration.subscribeThread p50 | 3.01 ms <sub>(3.00–3.17)</sub> | 10.7 ms <sub>(10.1–10.9)</sub> | **3.6×** |
| orchestration.subscribeThread p95 | 4.27 ms <sub>(3.16–4.51)</sub> | 13.2 ms <sub>(12.9–13.5)</sub> | **3.1×** |
| orchestration.getThreadProjection p50 | 1.36 ms <sub>(1.35–1.41)</sub> | 8.76 ms <sub>(8.35–9.33)</sub> | **6.4×** |
| orchestration.getThreadProjection p95 | 1.46 ms <sub>(1.45–2.32)</sub> | 10.1 ms <sub>(9.94–10.9)</sub> | **7.0×** |
| orchestration.getTurnDiff p50 | 2.51 ms <sub>(2.50–2.57)</sub> | 53.7 ms <sub>(52.1–57.9)</sub> | **21.4×** |
| orchestration.getTurnDiff p95 | 2.64 ms <sub>(2.59–3.06)</sub> | 62.4 ms <sub>(61.3–64.8)</sub> | **23.7×** |

Installed size: Rust is one binary (67.7 MiB). The reference is Node (143.4 MiB), its bundle (22.6 MiB) and 23 external packages (145.2 MiB), 311.2 MiB in all; the table leaves out the 105.8 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

## History

Ratios (Rust against the reference, higher is better for Rust) by commit, newest first.

| Date | Commit | Installed size | Start to first RPC | Memory at idle (RSS) | CPU while idle (per 10 s) | Peak memory, Codex turns | Peak memory, Claude turns | Codex turns per second | Claude turns per second | CPU per Codex turn | CPU per Claude turn |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-11-24 | `ec02f406` | 3.0× | 31.1× | 11.3× | 2.6× | 11.0× | 11.3× | 3.2× | 3.0× | 3.7× | 3.1× |
