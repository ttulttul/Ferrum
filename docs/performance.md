# Performance

How the Rust server compares with the TypeScript reference (`t3 serve` at the [pinned commit](../README.md#parity-status)) on the same machine and the same workload. [`scripts/perf_compare.py`](../scripts/perf_compare.py) takes the measurements. CI runs it on every push to `main` and records the results on the [`perf-data` branch](https://github.com/ttulttul/Ferrum/tree/perf-data), whose README shows the latest numbers and the trend.

## Headline

The first results, from a local run on a 16-core desktop (AMD Ryzen 9 7950X, Linux, Node 26.13.1) that other work was also loading. The [`perf-data` branch](https://github.com/ttulttul/Ferrum/tree/perf-data) has the numbers from CI. A ratio above 1× favors Rust.

- **About 3× smaller to install** (67 MiB against 205 MiB for comparable features; 311 MiB for everything the reference installs).
- **About 17× faster to start** (0.11 s against 1.85 s to the first RPC).
- **About 11× less memory at idle** (40 MiB against 452 MiB), and **about 8.5× less at peak** during turns.
- **2–3× the turn throughput, for about a third of the CPU per turn**, with 8 threads running fake Codex or Claude turns at once.
- **2–13× lower latency for the thread and project RPCs**. `server.getConfig` is the exception: it is about 5.7× slower in Rust (see below).
- **More CPU at idle** in Rust: about 60 ms per 10 s against 10 ms.

Commit `560dcd4d`, reference `fd0331ca`, 2026-11-24. Medians, with the range in brackets:

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 67.2 MiB | 205 MiB | **3.1×** |
| Start to first RPC | 112 ms <sub>(78.5–227)</sub> | 1851 ms <sub>(1515–2592)</sub> | **16.6×** |
| Memory at idle (RSS) | 39.5 MiB <sub>(39.1–39.8)</sub> | 452 MiB <sub>(449–455)</sub> | **11.4×** |
| CPU while idle (per 10 s) | 60.0 ms <sub>(60.0–70.0)</sub> | 10.00 ms <sub>(10.00–10.0)</sub> | **0.2×** |
| Peak memory, Codex turns | 76.9 MiB <sub>(73.3–80.7)</sub> | 655 MiB <sub>(654–681)</sub> | **8.5×** |
| Peak memory, Claude turns | 76.7 MiB <sub>(74.5–78.3)</sub> | 665 MiB <sub>(663–669)</sub> | **8.7×** |
| Codex turns per second | 9.59 /s <sub>(6.95–9.72)</sub> | 3.44 /s <sub>(3.43–3.81)</sub> | **2.8×** |
| Claude turns per second | 6.47 /s <sub>(5.96–7.10)</sub> | 3.01 /s <sub>(2.92–3.01)</sub> | **2.1×** |
| CPU per Codex turn | 130 ms <sub>(130–133)</sub> | 396 ms <sub>(356–396)</sub> | **3.0×** |
| CPU per Claude turn | 182 ms <sub>(180–185)</sub> | 495 ms <sub>(491–510)</sub> | **2.7×** |
| server.getConfig p50 | 14.6 ms <sub>(14.1–15.3)</sub> | 2.57 ms <sub>(2.38–2.77)</sub> | **0.2×** |
| server.getConfig p95 | 16.2 ms <sub>(16.2–16.7)</sub> | 3.30 ms <sub>(2.82–3.85)</sub> | **0.2×** |
| server.getSettings p50 | 0.26 ms <sub>(0.25–0.31)</sub> | 0.29 ms <sub>(0.27–0.35)</sub> | **1.1×** |
| server.getSettings p95 | 0.32 ms <sub>(0.31–0.34)</sub> | 0.35 ms <sub>(0.35–0.47)</sub> | **1.1×** |
| orchestration.subscribeShell p50 | 0.75 ms <sub>(0.71–0.76)</sub> | 1.82 ms <sub>(1.78–1.93)</sub> | **2.4×** |
| orchestration.subscribeShell p95 | 0.84 ms <sub>(0.77–1.02)</sub> | 2.69 ms <sub>(2.52–2.80)</sub> | **3.2×** |
| orchestration.subscribeThread p50 | 2.77 ms <sub>(2.73–2.82)</sub> | 8.24 ms <sub>(7.42–9.30)</sub> | **3.0×** |
| orchestration.subscribeThread p95 | 3.79 ms <sub>(3.16–3.89)</sub> | 10.0 ms <sub>(8.47–10.5)</sub> | **2.6×** |
| orchestration.getThreadProjection p50 | 1.33 ms <sub>(1.25–1.39)</sub> | 7.03 ms <sub>(6.42–7.04)</sub> | **5.3×** |
| orchestration.getThreadProjection p95 | 1.45 ms <sub>(1.41–2.09)</sub> | 7.52 ms <sub>(7.29–8.19)</sub> | **5.2×** |
| orchestration.getTurnDiff p50 | 2.66 ms <sub>(2.54–2.67)</sub> | 33.0 ms <sub>(28.0–33.9)</sub> | **12.4×** |
| orchestration.getTurnDiff p95 | 2.92 ms <sub>(2.91–3.08)</sub> | 38.4 ms <sub>(37.9–38.7)</sub> | **13.1×** |

Installed size: Rust is one binary (67.2 MiB). The reference is Node (143.4 MiB), its bundle (22.6 MiB) and 23 external packages (145.2 MiB), 311.2 MiB in all; the table leaves out the 105.8 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

## What is measured

Each server runs alone, on a free loopback port, in its own temporary home. Both get the environment the parity suites use (`parity_harness.server_env`): no real provider CLI on `PATH` (and never `~/.local/bin` or an nvm directory), a login shell that reads only the temporary home's profile, no telemetry, and all HTTP(S) sent to the harness's refusing proxy. The script fails if any server ran without that proxy. Provider turns use the fake `codex` and `claude` CLIs from `tests/fixtures/providers`, so no model is called and nothing leaves the machine.

- **Installed size.** Rust: the release binary (`cargo build --release`, as built). Reference: what `t3 serve` needs to run: the Node binary, the production bundle (`apps/server/dist`, built with the reference's own `build:bundle`, without source maps), and the packages the bundle leaves external (the reference's own list in `scripts/lib/cli-external-packages.ts`) with every package they depend on. Native prebuilds for other platforms are not counted. The web client is left out on both sides, since the Rust server does not ship one. The table also leaves out the external packages for features Rust does not have (local speech-to-text and Cursor turns); the full figure is in the notes below the table.
- **Startup.** The time from launching the process to its first protocol answer (`GET /ws` returning 426), and to the first successful RPC: browser sign-in, a WebSocket, and `server.getConfig`. The first launch of each server warms the page cache and is not recorded.
- **Memory and CPU at idle.** Resident memory of the server and the helper processes it keeps (the fake CLIs are not counted), and CPU used over 10 seconds of idling, after a 3-second settle.
- **RPC latency.** p50 and p95 of client RPCs on one WebSocket, against a project with a thread that has run three Codex turns: `server.getConfig` (which carries the provider status), `server.getSettings`, `orchestration.subscribeShell` to its first snapshot (the project and thread lists), `orchestration.subscribeThread` to `synchronized` (opening a thread), `orchestration.getThreadProjection`, and `orchestration.getTurnDiff` for the first turn. Each RPC gets 20 unrecorded calls and then 200 timed ones.
- **Turn throughput.** 8 threads at once, each sending 5 messages one after another (each once the previous run has completed). A Claude thread runs its own CLI. Each Codex thread runs on its own Codex instance, so each has its own app-server, as each Claude thread has its own CLI. The fakes replay `codex_turn.json` and `claude_turn.json` (reasoning, streamed text, a command, a file change and token usage), with fresh ids for every turn, and without the half-second pause the parity fixtures leave before a tool call ends, so the server's work is what is timed. Reported: turns per second, the median time from sending a message to its run completing, CPU time (the server process and every helper it started, such as git, but not the fake CLIs), and the server's peak resident memory (`VmHWM`).

Every measurement runs several times: 5 startups, 3 latency servers, and 3 throughput runs per provider, alternating which server goes first. The tables give medians with the range. CI runners are noisy and differ from one another, so the headline is the ratio of the two medians from the same run: Rust's against the reference's, so a result above 1× is in Rust's favor. The absolute numbers come second.

### What it does not show

- The fake CLIs answer at once. Real turns are dominated by the model, so in practice the turn throughput ratio matters most when many threads run at once on a small machine.
- The reference also ships a native resource monitor helper in its release archive. It is not counted here.
- The measurements cover the server, not the web client.

### Where Rust is slower

`server.getConfig` takes longer in Rust (see the latency rows). On every call Rust runs `git rev-parse` to decide whether to offer a scratch folder, and looks for editors and remote targets again (`src/config_rpc.rs`). The reference caches its answer to the git check once it has one (`ManagedProjectFolders.ts`). Caching it in Rust should close most of the gap; that is left for a separate change.

Rust also uses more CPU while idle: about 60 ms in 10 seconds against the reference's 10 ms (the measurement's resolution is 10 ms). That is still under 1% of one core, but it is a difference worth finding the cause of.

## Tracking on main

[`.github/workflows/perf.yml`](../.github/workflows/perf.yml) runs on pushes to `main` that change more than `README.md` or `docs/`, and on demand. It never runs on pull requests, and it is a separate workflow, so it cannot slow or block the parity shards. It:

1. fetches the reference at the commit `parity.yml` pins, installs it as the parity jobs do, and builds its production bundle;
2. builds the Rust server in release mode;
3. runs `scripts/perf_compare.py`, and shows the summary on the run's page;
4. uploads `perf.json` and `perf.md` as the run's artifact;
5. adds `perf.json` to the `perf-data` branch as `results/<date>-<commit>.json`, and regenerates that branch's `README.md` (`perf_compare.py --history`): the latest results and a table of the headline ratios for the last 30 runs.

The results live on their own branch, so CI never commits to `main`. A push to `perf-data` triggers no workflow, so there is no loop. Runs are queued, not cancelled, and a run whose push loses a race fetches the branch again and retries. The JSON keeps every sample, so a later change to the report can be applied to old runs.

The headline numbers in this file are updated by hand from a run. CI does not keep them current. Keeping this file current automatically would need CI to write to `main`, for example by opening a pull request that updates the headline after each run. That is not enabled.

## Running it locally

Build both servers, then run the script with a `PATH` that has neither `~/.local/bin` nor an nvm directory on it:

```bash
cargo build --locked --release
(cd REFERENCE/apps/server && node --run build:bundle)
python3 scripts/perf_compare.py \
  --original-command "node REFERENCE/apps/server/dist/bin.mjs" \
  --rust-command target/release/t3code-server \
  --reference-root REFERENCE \
  --json perf.json --markdown perf.md
```

`REFERENCE` is a checkout of the pinned commit, installed as in `parity.yml`. A full run takes about 7 minutes. `--only startup latency` (or `size`, `codex`, `claude`) runs part of it, the `--*-runs`, `--samples`, `--lanes` and `--turns` options change the workload, and `--keep` keeps each server's home and logs. `--history DIR` writes the trend table for a directory of result files.
