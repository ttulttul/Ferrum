# Performance

How Ferrum (the Rust server) compares with the TypeScript reference (`t3 serve` at the [pinned commit](../README.md#parity-status)) on the same machine and the same workload. [`scripts/perf_compare.py`](../scripts/perf_compare.py) takes the measurements. CI runs it on every push to `main` and records the results on the [`perf-data` branch](https://github.com/ttulttul/Ferrum/tree/perf-data), whose README shows the latest numbers and the trend.

## Headline

CI writes this section from its measurements. It refreshes the section at most once a day (see [Tracking on main](#tracking-on-main)); the full record is on the [`perf-data` branch](https://github.com/ttulttul/Ferrum/tree/perf-data).

<!-- perf:begin (written by scripts/perf_compare.py --update-doc; edits here are replaced) -->

From the latest CI run, on a GitHub-hosted runner. A ratio above 1× favors Ferrum.

- Installed size: **2.8×** in Ferrum's favor (73.2 MiB against 205 MiB).
- Start to first RPC: **33.4×** in Ferrum's favor (90.5 ms <sub>(88.6–94.2)</sub> against 3019 ms <sub>(2874–3172)</sub>).
- Memory at idle (RSS): **11.1×** in Ferrum's favor (41.0 MiB <sub>(40.6–41.1)</sub> against 453 MiB <sub>(452–456)</sub>).
- CPU while idle (per 10 s): **2.0×** in Ferrum's favor (10.00 ms <sub>(0.00–20.0)</sub> against 20.0 ms <sub>(10.00–40.0)</sub>).
- CPU while idle, large database with clients (per 10 s): **1.3×** in Ferrum's favor (15.0 ms <sub>(10.0–20.0)</sub> against 20.0 ms <sub>(20.0–20.0)</sub>).
- Memory at idle, large database: **3.2×** in Ferrum's favor (162 MiB <sub>(155–169)</sub> against 519 MiB <sub>(516–522)</sub>).
- Peak memory, 3 clients opening long threads: **2.7×** in Ferrum's favor (428 MiB against 1158 MiB).
- GET / p99 while 3 clients open long threads: **22.5×** in Ferrum's favor (3.07 ms against 68.9 ms).
- Peak memory, Codex turns: **11.0×** in Ferrum's favor (59.5 MiB <sub>(58.6–59.6)</sub> against 654 MiB <sub>(653–654)</sub>).
- Peak memory, Claude turns: **11.7×** in Ferrum's favor (56.8 MiB <sub>(56.6–57.1)</sub> against 667 MiB <sub>(661–711)</sub>).
- Codex turns per second: **2.4×** in Ferrum's favor (4.53 /s <sub>(4.48–4.59)</sub> against 1.90 /s <sub>(1.81–2.10)</sub>).
- Claude turns per second: **2.3×** in Ferrum's favor (3.89 /s <sub>(3.87–3.91)</sub> against 1.72 /s <sub>(1.70–1.76)</sub>).
- CPU per Codex turn: **2.9×** in Ferrum's favor (264 ms <sub>(263–266)</sub> against 762 ms <sub>(702–802)</sub>).
- CPU per Claude turn: **2.5×** in Ferrum's favor (356 ms <sub>(354–359)</sub> against 874 ms <sub>(854–895)</sub>).
- RPC latency (p50 and p95 of 6 RPCs): from **1.2×** to **36.7×** (above 1× favors Ferrum).

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

### Trend

Ratios by commit for the last 10 runs, newest first. The [`perf-data` branch](https://github.com/ttulttul/Ferrum/tree/perf-data) has every run.

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

<!-- perf:end -->

## Live trials on a real database

CI measures fake workloads on a small database. Five times, Ferrum also took over from the reference for 40 minutes on a 33 GB production database with about 7,000 threads, with real desktop clients connected and real agent threads at work, while an acceptance suite drove it. Each trial sampled the server process every 2 seconds (CPU as % of one core, resident memory, and `GET /` timed against a 2 s timeout). The reference was sampled on the same machine just before each one.

| Trial | Fixes it had | CPU mean / p95 | RSS mean / max | `GET /` mean / max, failures | What it found |
| --- | --- | ---: | ---: | ---: | --- |
| 1 | – | 38.8% / 50.0% | 2.08 / 2.23 GB | 18.7 ms / 2.0 s, 1 of 1,200 | 40% of a core while idle: every git, `ps` and `lsof` was started with `fork`, which copied the server's page tables (#311). Every git status stream polled git every 2 s on its own (#313). Threads read whole were never released (#316). |
| 2 | #311, #313, #316 | 1.97% / 8.5% | 1.19 / 2.18 GB | 1.4 ms / 1.2 s, 0 | Idle CPU fixed. |
| 3 | as 2 | 12.3% / 63.5% | 2.47 / 5.15 GB | 11.3 ms / 2.0 s, 5 of 1,199 | A 10 s stall and a 5 GB peak while clients opened long threads: each caller read the thread again, on the runtime workers, under the store lock (#337). A delta to a long thread cost tens of milliseconds, copying the whole thread several times (#340). |
| 4 | #337, #340 | 6.5% / 25.5% | 8.5 / 21.8 GB | 26.9 ms / 2.0 s, 13 of 1,053 | `t3_thread_list` across all projects read every thread whole before paging: 21.6 GB and a 5-minute stall of writes (#347). Every request now releases what it reads whole past 64 threads (#349). |
| 5 | #347, #349 | **1.64% / 6.5%** | **0.77 / 1.22 GB** | **0.65 ms / 0.41 s, 0 of 1,200** | Eight unbounded thread lists over about 7,150 threads, alongside attachment sends, held. |

The reference, on the same database just before each trial, averaged 1.3–6.7% of a core and 0.62–0.79 GB. In the fifth trial Ferrum used about a quarter of its CPU (1.6% against 6.7%), and its resident memory settled at 675 MB after the trial's peaks.

What each fix changed, as measured in its pull request:
- #311: idle CPU on a copy of that database, with two clients, went from 87–91% to 1.3–1.4% of a core.
- #313: two clients' git status streams on five folders: the server's own CPU went from 5.8–7.6% to 0.1–0.8%, and its git processes' from 17.7–21.3% to 0.1–3.1%.
- #316: RSS after opening and closing 300 long threads went from 2.96 GB (kept) to 0.77 GB once they had been idle.
- #337: three clients opening the eight longest threads: the peak went from 3.1 to 1.9 GB, and the slowest `GET /` from 5 s (timed out) to 12 ms.
- #340: a streamed delta to a 4.5 MB thread went from 87 ms to 1.9 ms.
- #347 and #349: listing 2,000 lazily read threads went from 1.9 s and 472 MB to 111 ms and 1 MB.

## What is measured

Each server runs alone, on a free loopback port, in its own temporary home. Both get the environment the parity suites use (`parity_harness.server_env`): no real provider CLI on `PATH` (and never `~/.local/bin` or an nvm directory), a login shell that reads only the temporary home's profile, no telemetry, and all HTTP(S) sent to the harness's refusing proxy. The script fails if any server ran without that proxy. Provider turns use the fake `codex` and `claude` CLIs from `tests/fixtures/providers`, so no model is called and nothing leaves the machine.

- **Installed size.** Rust: the release binary (`cargo build --release`, as built). Reference: what `t3 serve` needs to run: the Node binary, the production bundle (`apps/server/dist`, built with the reference's own `build:bundle`, without source maps), and the packages the bundle leaves external (the reference's own list in `scripts/lib/cli-external-packages.ts`) with every package they depend on. Native prebuilds for other platforms are not counted. The web client is left out on both sides, since the Rust server does not ship one. The table also leaves out the external packages for features Rust does not have (local speech-to-text and Cursor turns); the full figure is in the notes below the table.
- **Startup.** The time from launching the process to its first protocol answer (`GET /ws` returning 426), and to the first successful RPC: browser sign-in, a WebSocket, and `server.getConfig`. The first launch of each server warms the page cache and is not recorded.
- **Memory and CPU at idle.** Resident memory of the server and the helper processes it keeps (the fake CLIs are not counted), and the CPU the server process itself uses while idling (10 seconds by default, after a 3-second settle), given per 10 seconds. Processes it starts and reaps while idle are left out: they are the provider status probes, here the fake CLIs.
- **Idle CPU on a large database.** Each server seeds 2,000 threads through its own commands, stops, and starts again on that database. Two clients then hold open what a desktop window does: the shell, three threads each, VCS status and branch streams on the project's checkout, and local server discovery. Reported: the server process's own CPU while they idle (per 10 seconds) and its resident memory. CI fails the run if Rust's median is over 3% of one core (`--max-large-idle-cpu 3`). The results are still recorded first. This guards against work that grows with the database while clients are connected. Starting each git, `ps` or `lsof` with `fork` once cost a 33 GB database's server about 40% of a core at idle, because each fork copied its page tables.
- **Opening long threads.** Each server grows 8 threads with 8 turns each of 1 MiB messages (and, where the fake replays it, as much command output), and restarts on that database. Three clients then subscribe to all 8 at once, as desktop windows opening them do, while a sampler reads the server's resident memory and times `GET /` every 100 ms. Reported: the peak RSS and the `GET /` p99. CI fails the run if Rust's `GET /` p99 is over 300 ms (`--max-open-get-p99-ms 300`) or its peak RSS over 1.5 times the reference's (`--max-open-rss-ratio 1.5`). A long thread was once read from the database, and its snapshot built, on a runtime worker, once for every client that asked at the same time; on a 33 GB database that held up every request, a health check included, for ten seconds, and took the server past 5 GB. At CI's size each thread loads within a second from a warm page cache, so this section tracks memory and responsiveness; the stall itself is guarded by `stored_projection`'s unit tests.
- **Commit cost on a long thread.** `cargo test --release --lib commit_cost -- --ignored --nocapture` commits 200 streamed deltas to a thread of about 4.5 MB of turn items, with a live subscriber and a shell listener, and prints each phase's time (`commit_timing`). CI fails if the median delta takes more than 6 ms. A commit that changes a few records of a thread read from the database is planned, written and sent to live subscribers from those records alone (`shared_store::Partial`), and the thread's stored records are patched in place; whole projections were copied and diffed four times a commit before, 87 ms a delta against 1.9 ms now. A debug build plans every such commit both ways and checks they agree.
- **Listing the threads of a large database.** `cargo test --release --lib thread_list_on_a_large_database -- --ignored --nocapture` loads 2,000 threads lazily from a `statev2.sqlite` (32 command items of 4 KB each per thread) and calls `t3_thread_list` across all projects, with no limit, through the MCP tool call. CI fails if it takes a second or more, or grows the process's memory by 200 MB or more: a list once read every thread whole (1.9 s and 472 MB here, 21 GB on a real database). Every MCP call, RPC and HTTP request also releases, as it returns, the threads it read whole past 64 (`projection_eviction::bounded_reads`).
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

It measures at most once a day. Its first step (`scripts/perf_gate.sh`) reads when this workflow's bot last refreshed the numbers in `docs/performance.md` on `main`: the last commit by `github-actions[bot]` whose message starts with "Refresh the performance numbers". If that was less than 24 hours ago, the job ends there, successfully, and says why. If there has never been one, or it was 24 hours ago or more, it measures. A hand edit of this file does not restart the clock, so a pull request that documents a performance change in it is still measured once it merges. A manual run can pass `force` to measure anyway. `scripts/perf_gate_test.sh` checks the decision both ways on a throwaway repository, and the parity workflow runs it on every pull request. Otherwise it:

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
