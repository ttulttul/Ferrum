# Performance

How Ferrum (the Rust server) compares with the TypeScript reference (`t3 serve` at the [pinned commit](../README.md#parity-status)) on the same machine and the same workload. [`scripts/perf_compare.py`](../scripts/perf_compare.py) takes the measurements. CI runs it on every push to `main` and records the results on the [`perf-data` branch](https://github.com/ttulttul/Ferrum/tree/perf-data), whose README shows the latest numbers and the trend.

## Headline

CI writes this section from its measurements. It refreshes the section at most once a day (see [Tracking on main](#tracking-on-main)); the full record is on the [`perf-data` branch](https://github.com/ttulttul/Ferrum/tree/perf-data).

<!-- perf:begin (written by scripts/perf_compare.py --update-doc; edits here are replaced) -->

From the latest CI run, on a GitHub-hosted runner. A ratio above 1× favors Ferrum.

- Installed size: **2.9×** in Ferrum's favor (70.0 MiB against 205 MiB).
- Start to first RPC: **31.7×** in Ferrum's favor (82.7 ms <sub>(78.5–144)</sub> against 2618 ms <sub>(2534–2782)</sub>).
- Memory at idle (RSS): **11.2×** in Ferrum's favor (40.3 MiB <sub>(40.2–40.8)</sub> against 452 MiB <sub>(451–457)</sub>).
- CPU while idle (per 10 s): **3.0×** in the reference's favor (90.0 ms <sub>(90.0–100.0)</sub> against 30.0 ms <sub>(20.0–260)</sub>).
- Peak memory, Codex turns: **10.6×** in Ferrum's favor (60.6 MiB <sub>(59.8–60.9)</sub> against 645 MiB <sub>(642–673)</sub>).
- Peak memory, Claude turns: **11.2×** in Ferrum's favor (59.0 MiB <sub>(58.4–59.8)</sub> against 662 MiB <sub>(658–686)</sub>).
- Codex turns per second: **2.4×** in Ferrum's favor (5.60 /s <sub>(5.43–5.61)</sub> against 2.36 /s <sub>(2.20–2.52)</sub>).
- Claude turns per second: **1.9×** in Ferrum's favor (4.06 /s <sub>(3.93–4.34)</sub> against 2.13 /s <sub>(2.11–2.30)</sub>).
- CPU per Codex turn: **3.0×** in Ferrum's favor (202 ms <sub>(196–210)</sub> against 611 ms <sub>(532–647)</sub>).
- CPU per Claude turn: **2.5×** in Ferrum's favor (264 ms <sub>(263–264)</sub> against 653 ms <sub>(630–689)</sub>).
- RPC latency (p50 and p95 of 6 RPCs): from **0.4×** to **23.8×** (above 1× favors Ferrum).

Commit `8c0f3e6717f9`, reference `fd0331ca`, 2026-11-24T23:38:11Z, AMD EPYC 9V74 80-Core Processor (4 CPUs), Linux 7.0.0-1017-azure.

Ratio: how many times smaller, faster or lighter the Rust server is (the reference's median over Rust's, or Rust's over the reference's for turns per second). Medians, with the range in brackets.

| Measure | Rust | Reference | Ratio |
| --- | ---: | ---: | ---: |
| Installed size | 70.0 MiB | 205 MiB | **2.9×** |
| Start to first RPC | 82.7 ms <sub>(78.5–144)</sub> | 2618 ms <sub>(2534–2782)</sub> | **31.7×** |
| Memory at idle (RSS) | 40.3 MiB <sub>(40.2–40.8)</sub> | 452 MiB <sub>(451–457)</sub> | **11.2×** |
| CPU while idle (per 10 s) | 90.0 ms <sub>(90.0–100.0)</sub> | 30.0 ms <sub>(20.0–260)</sub> | **0.3×** |
| Peak memory, Codex turns | 60.6 MiB <sub>(59.8–60.9)</sub> | 645 MiB <sub>(642–673)</sub> | **10.6×** |
| Peak memory, Claude turns | 59.0 MiB <sub>(58.4–59.8)</sub> | 662 MiB <sub>(658–686)</sub> | **11.2×** |
| Codex turns per second | 5.60 /s <sub>(5.43–5.61)</sub> | 2.36 /s <sub>(2.20–2.52)</sub> | **2.4×** |
| Claude turns per second | 4.06 /s <sub>(3.93–4.34)</sub> | 2.13 /s <sub>(2.11–2.30)</sub> | **1.9×** |
| CPU per Codex turn | 202 ms <sub>(196–210)</sub> | 611 ms <sub>(532–647)</sub> | **3.0×** |
| CPU per Claude turn | 264 ms <sub>(263–264)</sub> | 653 ms <sub>(630–689)</sub> | **2.5×** |
| server.getConfig p50 | 11.5 ms <sub>(10.4–12.0)</sub> | 4.87 ms <sub>(4.67–4.91)</sub> | **0.4×** |
| server.getConfig p95 | 13.1 ms <sub>(12.0–14.3)</sub> | 6.11 ms <sub>(5.70–6.17)</sub> | **0.5×** |
| server.getSettings p50 | 0.42 ms <sub>(0.40–0.42)</sub> | 0.53 ms <sub>(0.52–0.54)</sub> | **1.3×** |
| server.getSettings p95 | 0.45 ms <sub>(0.45–0.48)</sub> | 0.72 ms <sub>(0.71–0.74)</sub> | **1.6×** |
| orchestration.subscribeShell p50 | 1.30 ms <sub>(1.28–1.32)</sub> | 3.99 ms <sub>(3.96–4.16)</sub> | **3.1×** |
| orchestration.subscribeShell p95 | 1.34 ms <sub>(1.32–1.40)</sub> | 5.48 ms <sub>(5.14–5.89)</sub> | **4.1×** |
| orchestration.subscribeThread p50 | 4.39 ms <sub>(4.08–4.48)</sub> | 14.7 ms <sub>(14.0–15.3)</sub> | **3.3×** |
| orchestration.subscribeThread p95 | 4.78 ms <sub>(4.61–4.99)</sub> | 17.0 ms <sub>(16.2–18.1)</sub> | **3.6×** |
| orchestration.getThreadProjection p50 | 2.02 ms <sub>(1.95–2.02)</sub> | 11.9 ms <sub>(11.8–13.3)</sub> | **5.9×** |
| orchestration.getThreadProjection p95 | 2.09 ms <sub>(2.01–2.18)</sub> | 13.9 ms <sub>(13.1–14.9)</sub> | **6.6×** |
| orchestration.getTurnDiff p50 | 2.61 ms <sub>(2.58–2.71)</sub> | 62.1 ms <sub>(48.9–68.2)</sub> | **23.8×** |
| orchestration.getTurnDiff p95 | 3.08 ms <sub>(2.98–3.08)</sub> | 70.7 ms <sub>(65.2–73.1)</sub> | **22.9×** |

Installed size: Rust is one binary (70.0 MiB). The reference is Node (143.4 MiB), its bundle (22.6 MiB) and 23 external packages (145.2 MiB), 311.2 MiB in all; the table leaves out the 105.8 MiB of packages for features Rust lacks (local speech-to-text, Cursor turns).

Startup: 5 runs per server after one warm-up; idle measured over 10 s after 3 s. Latency: 3 servers each, 200 calls per RPC after 20 warm-up calls. Turns: 8 threads at once, 5 turns each, 3 runs.

### Trend

Ratios by commit for the last 10 runs, newest first. The [`perf-data` branch](https://github.com/ttulttul/Ferrum/tree/perf-data) has every run.

| Date | Commit | Installed size | Start to first RPC | Memory at idle (RSS) | CPU while idle (per 10 s) | Peak memory, Codex turns | Peak memory, Claude turns | Codex turns per second | Claude turns per second | CPU per Codex turn | CPU per Claude turn |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-11-24 | `8c0f3e67` | 2.9× | 31.7× | 11.2× | 0.3× | 10.6× | 11.2× | 2.4× | 1.9× | 3.0× | 2.5× |
| 2026-11-24 | `1bd449a3` | 2.9× | 33.5× | 11.2× | 0.2× | 11.0× | 11.1× | 2.4× | 2.2× | 2.9× | 2.4× |
| 2026-11-24 | `e2933272` | 3.0× | 35.5× | 11.1× | 3.1× | 10.8× | 11.2× | 1.7× | 1.8× | 2.1× | 2.0× |
| 2026-11-24 | `4b8d262f` | 3.0× | 26.8× | 11.3× | 0.2× | 10.8× | 11.1× | 2.5× | 2.3× | 3.1× | 2.6× |
| 2026-11-24 | `c68bda6c` | 2.9× | 35.7× | 11.1× | 2.8× | 11.3× | 11.3× | 1.8× | 1.6× | 2.2× | 1.9× |
| 2026-11-24 | `480c4073` | 2.9× | 35.3× | 11.2× | 3.9× | 10.9× | 11.3× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `52b62924` | 3.0× | 36.2× | 11.1× | 3.1× | 11.0× | 11.2× | 1.7× | 1.6× | 2.1× | 1.9× |
| 2026-11-24 | `e10f06d2` | 3.0× | 36.4× | 11.1× | 3.2× | 11.0× | 11.2× | 1.8× | 1.7× | 2.2× | 1.9× |
| 2026-11-24 | `00b0e0d7` | 3.0× | 31.2× | 11.2× | 0.3× | 11.0× | 11.2× | 2.7× | 2.8× | 3.1× | 3.0× |
| 2026-11-24 | `ec02f406` | 3.0× | 31.1× | 11.3× | 2.6× | 11.0× | 11.3× | 3.2× | 3.0× | 3.7× | 3.1× |

<!-- perf:end -->

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

`server.getConfig` used to take longer in Rust: on every call it ran `git rev-parse` to decide whether to offer a scratch folder, and looked for editors and remote targets again. It now keeps those answers as the reference does: editors and remote targets for a minute, served stale while a refresh runs (`makeStaleWhileRevalidate`), and the git check for good once git has answered (`ManagedProjectFolders.ts`). On one machine, Rust's p50 went from 15.8 ms to 1.78 ms, against the reference's 2.6 ms.

Rust also uses more CPU while idle: about 60 ms in 10 seconds against the reference's 10 ms (the measurement's resolution is 10 ms). That is still under 1% of one core, but it is a difference worth finding the cause of.

## Tracking on main

[`.github/workflows/perf.yml`](../.github/workflows/perf.yml) runs on pushes to `main` that change more than `README.md` or `docs/`, and on demand. It never runs on pull requests, and it is a separate workflow, so it cannot slow or block the parity shards.

It measures at most once a day. Its first step reads when `docs/performance.md` last changed on `main` (`git log -1 --format=%ct -- docs/performance.md`). If that was less than 24 hours ago, the job ends there, successfully, and says why. Because it uses the document's own commit time, a hand edit also restarts the clock. A manual run can pass `force` to measure anyway. Otherwise it:

1. fetches the reference at the commit `parity.yml` pins, installs it as the parity jobs do, and builds its production bundle;
2. builds the Rust server in release mode;
3. runs `scripts/perf_compare.py`, and shows the summary on the run's page;
4. uploads `perf.json` and `perf.md` as the run's artifact;
5. adds `perf.json` to the `perf-data` branch as `results/<date>-<commit>.json`, and regenerates that branch's `README.md` (`perf_compare.py --history`): the latest results and a table of the headline ratios for the last 30 runs;
6. rewrites the [Headline](#headline) section of this file from those results (`perf_compare.py --history … --update-doc`), and brings the change onto `main` through a pull request from the `perf-docs-update` branch, which the workflow merges itself.

Why a pull request: `main`'s ruleset requires every change to arrive through one, so the workflow cannot commit to it directly. The repository does not allow auto-merge, so the workflow merges the pull request itself as soon as it is open. The pull request changes only `docs/performance.md`, and nothing needs to check it: the parity and workers workflows skip a docs-only change (#283), and the merge is pushed with the workflow's `GITHUB_TOKEN`, which by GitHub's rules starts no other workflow run. So the update cannot start another measurement, and there is no loop. This uses only the `GITHUB_TOKEN`; it needs the repository setting "Allow GitHub Actions to create and approve pull requests" (Settings → Actions → General). Without it, the measurement and the `perf-data` record still happen, and the step only warns.

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
