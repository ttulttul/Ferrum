# Performance

How Ferrum (the Rust server) compares with the TypeScript reference (`t3 serve` at the [pinned commit](../README.md#parity-status)) on the same machine and the same workload. [`scripts/perf_compare.py`](../scripts/perf_compare.py) takes the measurements. CI runs it on every push to `main` and records the results on the [`perf-data` branch](https://github.com/ttulttul/Ferrum/tree/perf-data), whose README shows the latest numbers and the trend.

## Headline

CI writes this section from its measurements. It refreshes the section at most once a day (see [Tracking on main](#tracking-on-main)); the full record is on the [`perf-data` branch](https://github.com/ttulttul/Ferrum/tree/perf-data).

<!-- perf:begin (written by scripts/perf_compare.py --update-doc; edits here are replaced) -->

From the latest CI run, on a GitHub-hosted runner. A ratio above 1× favors Ferrum.

- Installed size: **2.9×** in Ferrum's favor (70.1 MiB against 205 MiB).
- Start to first RPC: **36.0×** in Ferrum's favor (86.8 ms <sub>(82.9–94.3)</sub> against 3126 ms <sub>(3007–3205)</sub>).
- Memory at idle (RSS): **11.1×** in Ferrum's favor (40.9 MiB <sub>(40.5–41.7)</sub> against 456 MiB <sub>(452–457)</sub>).
- CPU while idle (per 10 s): **3.0×** in Ferrum's favor (10.00 ms <sub>(0.00–20.0)</sub> against 30.0 ms <sub>(10.00–290)</sub>).
- Peak memory, Codex turns: **11.6×** in Ferrum's favor (59.4 MiB <sub>(59.3–59.9)</sub> against 691 MiB <sub>(652–691)</sub>).
- Peak memory, Claude turns: **11.2×** in Ferrum's favor (59.8 MiB <sub>(59.3–60.2)</sub> against 667 MiB <sub>(655–668)</sub>).
- Codex turns per second: **2.7×** in Ferrum's favor (4.78 /s <sub>(4.74–4.78)</sub> against 1.79 /s <sub>(1.74–1.83)</sub>).
- Claude turns per second: **2.4×** in Ferrum's favor (4.02 /s <sub>(4.01–4.06)</sub> against 1.65 /s <sub>(1.65–1.68)</sub>).
- CPU per Codex turn: **3.2×** in Ferrum's favor (255 ms <sub>(253–256)</sub> against 811 ms <sub>(785–840)</sub>).
- CPU per Claude turn: **2.6×** in Ferrum's favor (344 ms <sub>(342–348)</sub> against 906 ms <sub>(890–908)</sub>).
- RPC latency (p50 and p95 of 6 RPCs): from **1.2×** to **22.1×** (above 1× favors Ferrum).

Commit `f2145255066a`, reference `fd0331ca`, 2026-11-25T00:15:00Z, AMD EPYC 7763 64-Core Processor (4 CPUs), Linux 7.0.0-1017-azure.

Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 70.1 MiB | 205 MiB | **2.9×** |
| Start to first RPC | 86.8 ms <sub>(82.9–94.3)</sub> | 3126 ms <sub>(3007–3205)</sub> | **36.0×** |
| Memory at idle (RSS) | 40.9 MiB <sub>(40.5–41.7)</sub> | 456 MiB <sub>(452–457)</sub> | **11.1×** |
| CPU while idle (per 10 s) | 10.00 ms <sub>(0.00–20.0)</sub> | 30.0 ms <sub>(10.00–290)</sub> | **3.0×** |
| Peak memory, Codex turns | 59.4 MiB <sub>(59.3–59.9)</sub> | 691 MiB <sub>(652–691)</sub> | **11.6×** |
| Peak memory, Claude turns | 59.8 MiB <sub>(59.3–60.2)</sub> | 667 MiB <sub>(655–668)</sub> | **11.2×** |
| Codex turns per second | 4.78 /s <sub>(4.74–4.78)</sub> | 1.79 /s <sub>(1.74–1.83)</sub> | **2.7×** |
| Claude turns per second | 4.02 /s <sub>(4.01–4.06)</sub> | 1.65 /s <sub>(1.65–1.68)</sub> | **2.4×** |
| CPU per Codex turn | 255 ms <sub>(253–256)</sub> | 811 ms <sub>(785–840)</sub> | **3.2×** |
| CPU per Claude turn | 344 ms <sub>(342–348)</sub> | 906 ms <sub>(890–908)</sub> | **2.6×** |
| server.getConfig p50 | 3.33 ms <sub>(3.31–3.60)</sub> | 5.27 ms <sub>(5.10–5.43)</sub> | **1.6×** |
| server.getConfig p95 | 3.44 ms <sub>(3.42–3.89)</sub> | 6.63 ms <sub>(6.56–7.05)</sub> | **1.9×** |
| server.getSettings p50 | 0.60 ms <sub>(0.58–0.61)</sub> | 0.69 ms <sub>(0.68–0.70)</sub> | **1.2×** |
| server.getSettings p95 | 0.62 ms <sub>(0.61–0.63)</sub> | 0.95 ms <sub>(0.92–0.95)</sub> | **1.5×** |
| orchestration.subscribeShell p50 | 1.65 ms <sub>(1.64–1.71)</sub> | 4.74 ms <sub>(4.24–4.82)</sub> | **2.9×** |
| orchestration.subscribeShell p95 | 1.73 ms <sub>(1.71–1.87)</sub> | 6.63 ms <sub>(5.84–6.76)</sub> | **3.8×** |
| orchestration.subscribeThread p50 | 5.30 ms <sub>(5.27–5.35)</sub> | 17.9 ms <sub>(17.4–18.2)</sub> | **3.4×** |
| orchestration.subscribeThread p95 | 5.68 ms <sub>(5.45–5.71)</sub> | 20.2 ms <sub>(19.4–20.7)</sub> | **3.6×** |
| orchestration.getThreadProjection p50 | 2.64 ms <sub>(2.64–2.67)</sub> | 14.5 ms <sub>(14.2–14.6)</sub> | **5.5×** |
| orchestration.getThreadProjection p95 | 2.71 ms <sub>(2.68–2.90)</sub> | 16.2 ms <sub>(16.1–16.9)</sub> | **6.0×** |
| orchestration.getTurnDiff p50 | 3.17 ms <sub>(3.01–3.40)</sub> | 63.2 ms <sub>(60.4–65.9)</sub> | **20.0×** |
| orchestration.getTurnDiff p95 | 3.36 ms <sub>(3.13–3.82)</sub> | 74.4 ms <sub>(71.3–78.4)</sub> | **22.1×** |

Installed size: Rust is one binary (70.1 MiB). The reference is Node (143.4 MiB), its bundle (22.6 MiB) and 23 external packages (145.2 MiB), 311.2 MiB in all; the table leaves out the 105.8 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

### Trend

Ratios by commit for the last 10 runs, newest first. The [`perf-data` branch](https://github.com/ttulttul/Ferrum/tree/perf-data) has every run.

| Date | Commit | Installed size | Start to first RPC | Memory at idle (RSS) | CPU while idle (per 10 s) | Peak memory, Codex turns | Peak memory, Claude turns | Codex turns per second | Claude turns per second | CPU per Codex turn | CPU per Claude turn |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-11-25 | `f2145255` | 2.9× | 36.0× | 11.1× | 3.0× | 11.6× | 11.2× | 2.7× | 2.4× | 3.2× | 2.6× |
| 2026-11-24 | `8c0f3e67` | 2.9× | 31.7× | 11.2× | 0.3× | 10.6× | 11.2× | 2.4× | 1.9× | 3.0× | 2.5× |
| 2026-11-24 | `1bd449a3` | 2.9× | 33.5× | 11.2× | 0.2× | 11.0× | 11.1× | 2.4× | 2.2× | 2.9× | 2.4× |
| 2026-11-24 | `e2933272` | 3.0× | 35.5× | 11.1× | 3.1× | 10.8× | 11.2× | 1.7× | 1.8× | 2.1× | 2.0× |
| 2026-11-24 | `4b8d262f` | 3.0× | 26.8× | 11.3× | 0.2× | 10.8× | 11.1× | 2.5× | 2.3× | 3.1× | 2.6× |
| 2026-11-24 | `c68bda6c` | 2.9× | 35.7× | 11.1× | 2.8× | 11.3× | 11.3× | 1.8× | 1.6× | 2.2× | 1.9× |
| 2026-11-24 | `480c4073` | 2.9× | 35.3× | 11.2× | 3.9× | 10.9× | 11.3× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `52b62924` | 3.0× | 36.2× | 11.1× | 3.1× | 11.0× | 11.2× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `e10f06d2` | 3.0× | 36.4× | 11.1× | 3.2× | 11.0× | 11.2× | 1.8× | 1.7× | 2.2× | 1.9× |
| 2026-11-24 | `00b0e0d7` | 3.0× | 31.2× | 11.2× | 0.3× | 11.0× | 11.2× | 2.7× | 2.8× | 3.1× | 3.0× |

<!-- perf:end -->

## What is measured

Each server runs alone, on a free loopback port, in its own temporary home. Both get the environment the parity suites use (`parity_harness.server_env`): no real provider CLI on `PATH` (and never `~/.local/bin` or an nvm directory), a login shell that reads only the temporary home's profile, no telemetry, and all HTTP(S) sent to the harness's refusing proxy. The script fails if any server ran without that proxy. Provider turns use the fake `codex` and `claude` CLIs from `tests/fixtures/providers`, so no model is called and nothing leaves the machine.

- **Installed size.** Rust: the release binary (`cargo build --release`, as built). Reference: what `t3 serve` needs to run: the Node binary, the production bundle (`apps/server/dist`, built with the reference's own `build:bundle`, without source maps), and the packages the bundle leaves external (the reference's own list in `scripts/lib/cli-external-packages.ts`) with every package they depend on. Native prebuilds for other platforms are not counted. The web client is left out on both sides, since the Rust server does not ship one. The table also leaves out the external packages for features Rust does not have (local speech-to-text and Cursor turns); the full figure is in the notes below the table.
- **Startup.** The time from launching the process to its first protocol answer (`GET /ws` returning 426), and to the first successful RPC: browser sign-in, a WebSocket, and `server.getConfig`. The first launch of each server warms the page cache and is not recorded.
- **Memory and CPU at idle.** Resident memory of the server and the helper processes it keeps (the fake CLIs are not counted), and the CPU the server process itself uses while idling (10 seconds by default, after a 3-second settle), given per 10 seconds. Processes it starts and reaps while idle are left out: they are the provider status probes, here the fake CLIs.
- **RPC latency.** p50 and p95 of client RPCs on one WebSocket, against a project with a thread that has run three Codex turns: `server.getConfig` (which carries the provider status), `server.getSettings`, `orchestration.subscribeShell` to its first snapshot (the project and thread lists), `orchestration.subscribeThread` to `synchronized` (opening a thread), `orchestration.getThreadProjection`, and `orchestration.getTurnDiff` for the first turn. Each RPC gets 20 unrecorded calls and then 200 timed ones.
- **Turn throughput.** 8 threads at once, each sending 5 messages one after another (each once the previous run has completed). A Claude thread runs its own CLI. Each Codex thread runs on its own Codex instance, so each has its own app-server, as each Claude thread has its own CLI. The fakes replay `codex_turn.json` and `claude_turn.json` (reasoning, streamed text, a command, a file change and token usage), with fresh ids for every turn, and without the half-second pause the parity fixtures leave before a tool call ends, so the server's work is what is timed. Reported: turns per second, the median time from sending a message to its run completing, CPU time (the server process and every helper it started, such as git, but not the fake CLIs), and the server's peak resident memory (`VmHWM`).

Every measurement runs several times: 5 startups, 3 latency servers, and 3 throughput runs per provider, alternating which server goes first. The tables give medians with the range. CI runners are noisy and differ from one another, so the headline is the ratio of the two medians from the same run: Rust's against the reference's, so a result above 1× is in Rust's favor. The absolute numbers come second.

### What it does not show

- The fake CLIs answer at once. Real turns are dominated by the model, so in practice the turn throughput ratio matters most when many threads run at once on a small machine.
- The reference also ships a native resource monitor helper in its release archive. It is not counted here.
- The measurements cover the server, not the web client.

### Where Rust is slower

`server.getConfig` used to take longer in Rust: on every call it ran `git rev-parse` to decide whether to offer a scratch folder, and looked for editors and remote targets again. It now keeps those answers as the reference does: editors and remote targets for a minute, served stale while a refresh runs (`makeStaleWhileRevalidate`), and the git check for good once git has answered (`ManagedProjectFolders.ts`). On one machine, Rust's p50 went from 15.8 ms to 1.78 ms, against the reference's 2.6 ms.

Idle CPU used to be counted with the processes the server reaps while idle, which are the fake CLIs that stand in for provider status probes. Counted as the server's own time, Rust used about as much as the reference (80 ms against 83 ms per 10 s). Nearly all of it was the provider status scheduler: four times a second it re-read and parsed `settings.json`, filled in its secrets, and rebuilt every provider's row, to notice a change. It now skips a tick when the settings file, the secret files and the model catalog are unchanged and no check is due, which leaves about 7 ms per 10 s.

## Tracking on main

[`.github/workflows/perf.yml`](../.github/workflows/perf.yml) runs on pushes to `main` that change more than `README.md` or `docs/`, and on demand. It never runs on pull requests, and it is a separate workflow, so it cannot slow or block the parity shards.

It measures at most once a day. Its first step reads when `docs/performance.md` last changed on `main` (`git log -1 --format=%ct -- docs/performance.md`). If that was less than 24 hours ago, the job ends there, successfully, and says why. Because it uses the document's own commit time, a hand edit also restarts the clock. A manual run can pass `force` to measure anyway. Otherwise it:

1. fetches the reference at the commit `parity.yml` pins, installs it as the parity jobs do, and builds its production bundle;
2. builds the Rust server in release mode;
3. runs `scripts/perf_compare.py`, and shows the summary on the run's page;
4. uploads `perf.json` and `perf.md` as the run's artifact;
5. adds `perf.json` to the `perf-data` branch as `results/<date>-<commit>.json`, and regenerates that branch's `README.md` (`perf_compare.py --history`): the latest results and a table of the headline ratios for the last 30 runs;
6. rewrites the [Headline](#headline) section of this file from those results (`perf_compare.py --history … --update-doc`), and brings the change onto `main` through a pull request from the `perf-docs-update` branch, which the workflow merges itself.

Why a pull request: `main`'s ruleset requires every change to arrive through one, so the workflow cannot commit to it directly. The repository does not allow auto-merge, so the workflow merges the pull request itself as soon as it is open. The pull request changes only `docs/performance.md`, and nothing needs to check it. The parity and workers workflows create no run for a pull request that changes only that file. Without that filter, GitHub creates one for the bot's pull request, holds it for approval before any job starts, and it ends red, even though those workflows skip docs-only changes (#283). The merge is pushed with the workflow's `GITHUB_TOKEN`, and GitHub starts no workflow run for such a push. `perf.yml` also ignores `docs/**`. So the update cannot start another measurement, and there is no loop. This uses only the `GITHUB_TOKEN`; it needs the repository setting "Allow GitHub Actions to create and approve pull requests" (Settings → Actions → General). Without it, the measurement and the `perf-data` record still happen, and the step only warns.

The results live on their own branch. A push to `perf-data` triggers no workflow. Runs are queued, not cancelled, and a run whose push loses a race fetches the branch again and retries. The JSON keeps every sample, so a later change to the report can be applied to old runs.

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
