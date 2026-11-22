# Making server parity CI fast

Research report, November 2026. The goal is to get the "Server parity" workflow
(`.github/workflows/parity.yml`) well under 10 minutes of wall-clock time per PR
without weakening what it verifies. Measurements come from recent successful runs on
main and PRs (44959255057, 44959226903, 44959569083), a profiled local run of
`client_thread_parity.py`, and an experimental matrix run on this branch (draft PR #87).

## 1. Where the time goes

Main's runs take 29–30 minutes on one `ubuntu-24.04` runner. Everything runs in one
job, and the parity scripts run one after another in one step.

### Job steps (run 44959255057, main)

| Step | Time |
|---|---:|
| Checkout, set up job, setup-node | 10 s |
| Fetch pinned reference (shallow) | 17 s |
| Prepare reference: apt libfaketime, pnpm install, Playwright Chromium + deps, venv | 59 s |
| Check upstream inventory | 3 s |
| Validate Rust: fmt, clippy (45 s compile), test (78 s compile, ~7 s run), schema corpus, build | 138 s |
| Build and test t3-agentd | 63 s |
| **Compare both servers** (21 commands, serial) | **1479 s (24.6 min)** |
| **Total** | **~29.5 min** |

There is no cargo cache (every run compiles from scratch) and no `concurrency:` group,
so superseded PR pushes keep running. On Nov 21, two cancelled runs went for 35 min each.

### Parity scripts, ranked (mean of three runs)

| # | Script | Time | Share of compare step | Where the time goes |
|---|---|---:|---:|---|
| 1 | `client_thread_parity.py` | **979 s** | 66 % | 44 provider scenarios plus the seeded-thread checks, each on a fresh pair of servers (see below) |
| 2 | `relay_provisioning_parity.py` | 165 s | 11 % | 6 scenarios × 2 servers. The reference waits a random 0–30 s (`MANAGED_TUNNEL_FIRST_REGISTRATION_JITTER`) before its first T3 Connect call. Each run is also a SIGTERM + wait. |
| 3 | `relay_client_parity.py` | 68 s | 5 % | 6 scenarios × 2. Four use a fixed `time.sleep(4)` that checks no connector starts (a negative check). |
| 4 | `config_parity.py` | 62 s | 4 % | Settings frames, persisted JSON, secret migrations, restarts |
| 5 | `client_rpc_parity.py` | 43 s | 3 % | 179 client steps per server, run one server after the other (13.6 s for the reference, 22.8 s for Rust) |
| 6 | `preview_provider_parity.py` | 13–72 s | 1–5 % | Chromium first paint. One run took 72 s (retries), others 13–15 s. |
| 7 | `auth_parity.py` + `cors_parity.py` | 20 s | 1 % | |
| 8 | `client_initial_sync_parity.py` | 19 s | 1 % | |
| 9 | `peer_link_parity.py` | 16 s | 1 % | |
| 10 | `git_workflow_parity.py` | 10–26 s | 1 % | |
| — | 11 others (orchestration, rpc_edge, harness, config_snapshot, mcp, github, linear, source_control_hosts, relay_parity, codex schemas) | 2–8 s each, ~60 s total | 4 % | |

### Inside `client_thread_parity.py`

Profiled locally with the CI command line (wrapper timing `time.sleep`, `wait_for`,
server launch, and client runs). The full local run took 918 s; CI takes 979 s, so local
numbers carry over.

| Component | Time | Notes |
|---|---:|---|
| 18 restart scenarios (36 `restart_turn` calls) | **686 s** | 75 % of the script |
| …of which: 8 s minimum settle after each restart (`time.monotonic() - started > 8`) | 288 s | Fixed floor on every restart, on both servers |
| …of which: the reference's 60 s continuation retry in `claude restart cli restored` | 2 × ~60 s | Hard-coded `CONTINUATION_START_RETRY_DELAYS_MS = [60_000, 180_000]` in the reference's `RestartBackgroundNote.ts`. Makes this one scenario take ~150 s. |
| …of which: fixed `time.sleep(1)`/`sleep(2)` before stop, after settle, between queued messages | ~90 s | |
| 26 non-restart scenarios | ~218 s | ~8 s per scenario: ~2.5 s client run, ~3 s reference startup, the rest seeding and teardown |
| Server startup (126 launches) | 164 s | Node reference ~2.9 s each, Rust debug ~0.1 s |
| Seeded-thread checks | 14 s | Includes a fixed 2 s sleep per server and a 1.5 s `Effect.sleep` in `client_thread.mjs` |
| Actual comparisons (Python) | < 2 s | |

About 78 % of the script (717 s across 3,753 `time.sleep` calls) is spent sleeping:
fixed sleeps plus 0.5 s `wait_for` polling. Most of this is waiting on the servers'
own timers rather than on CPU, which is why both sharding and concurrency help so much.
The 20 s Codex provider-status timeout from the brief does not show up as a cost:
`provider_status` took 0.1 s in total, because Rust returns immediately and the reference
probe settles fast against the fakes.

## 2. Options

Wall-time savings are measured against today's ~29.5 min.

| Option | Est. wall-time saving | Effort | Fidelity | Flakiness / risk |
|---|---|---|---|---|
| **A. Job matrix: separate Rust validation job + 7 parity shards** (prototyped, see §3) | 29.5 → **~7–8 min** | Low (workflow + `--shard` flag + one shell script) | Unchanged: same scripts, same commands | Unchanged per test. Uses ~8 runners' worth of setup minutes (free on a public repo; 20 concurrent jobs allowed). Required-check names change (`parity (core)`, …); main isn't protected today. |
| **B. `concurrency:` cancel-in-progress on PRs** | No per-run saving; frees runners, ends 35-min zombie runs | Trivial | None | None |
| **C. `Swatinem/rust-cache`** | ~1 min per shard on a warm cache (build ~60 s → ~10–20 s); Rust job 3.5 → ~1.5 min | Trivial | None | Cache poisoning is unlikely; cache is keyed on Cargo.lock and toolchain |
| **D. Readiness instead of the fixed 8 s restart settle floor** (wait for the continuation run to appear and settle, or for "no continuation" to be decided from a server signal) | −288 s serial, about −70 s per thread shard | Medium: needs a reliable "restart recovery finished" signal from both servers (e.g. a trace-log event in `server.trace.ndjson` or a projection field) | Same, if the signal is right. Wrong signals risk false passes on "nothing continued" scenarios. | Reduces timing sensitivity if done well |
| **E. libfaketime jump for the 60 s continuation retry** (as `orchestration_parity.py` and `github_provider_parity.py` already do) | −~120 s serial. This one scenario sets the minimum length of a thread shard. | Medium: start the restarted servers under faketime with a controllable offset, then advance it. Rust timers must use the faked clock too (tokio uses CLOCK_MONOTONIC, which libfaketime fakes by default). | Keeps the real 60 s contract while testing it faster | Faketime with Node and tokio can be finicky. Keep a real-time run nightly. |
| **F. Run the original and Rust servers concurrently within each scenario** | client_thread ~−45 %; relay_* and client_rpc ~−45 % | Medium: thread pool per scenario. Homes and ports are already isolated. | Same comparisons | CPU contention can move timing checks (`continuation timing` −1.5..3 s, relay "immediate" < N s). With sharding on, fewer parallel processes per runner keeps this safe. |
| **G. Run independent scenarios concurrently within a script** | Like F, and larger | Medium–high: `relay_provisioning` shares one `FakeConnect` (`fake.reset()`), so it needs one fake per scenario | Same | Same contention concern. Mostly redundant once sharded. |
| **H. Faster `relay_provisioning`** | ~165 s → ~40 s | Low–medium: run scenarios concurrently (per-scenario fake), or give Rust's `first_registration_jitter` a test override and keep the reference under faketime | Same | The reference's jitter is random. Today the suite already tolerates 0–30 s. |
| **I. Shorten `relay_client`'s 4 s negative waits** | ~−16 s | Trivial | Slightly weaker ("no connector within 2 s") | Low |
| **J. Reuse one server pair per suite** | client_thread: at most the 164 s of startup; less once sharded | High | **Weaker**: fresh homes are part of what each scenario asserts (restart, missing CLI, login-shell PATH) | Cross-scenario leakage. **Not recommended.** |
| **K. Cache pnpm store / `node_modules` keyed on the pinned commit; cache Playwright browsers** | ~20–30 s per shard (prep 33–56 s → ~15 s) | Low | None | Low. Note that pnpm 12's first `exec` completes the workspace install (see §3), so the cache must cover the post-`exec` state. |
| **L. Prebuilt reference artifact** (tarball of the reference + node_modules for the pinned commit, built once and stored as a release asset or cache) | Same as K, slightly more | Medium | None | Must be rebuilt when the pin moves |
| **M. Release build of the Rust server** | Negative: compiles slower, and the debug server already starts in 0.1 s | — | — | Not recommended |
| **N. Path filters**: skip docs-only PRs (`paths-ignore: ['**/*.md', 'docs/**']`); optionally run only affected suites, with full runs on main and nightly | 100 % on docs PRs (e.g. #86); affected-suite selection is hard to get right here because almost everything goes through the same server crate | Low (paths-ignore); high (affected-suite mapping) | paths-ignore: none. Suite selection: real risk of missing regressions. | Only do paths-ignore. |
| **O. Remove duplicated scenarios** | Small. The restart scenarios share the same first phase but each asserts a different recovery path. `config_parity` and `config_snapshot_parity` overlap a little. | Medium | Risk of losing coverage | Not worth it before A/D/E |
| **P. Self-hosted runner** | Local hardware ran client_thread in 918 s vs 979 s on CI: the suite is wait-bound, so a big box gains little per shard | Medium + ops | None | **Security:** this is a public repo that takes PRs, so self-hosted runners would run untrusted fork code. Not recommended. |
| **Q. Larger GitHub runners** | Only the compile gets faster (~1 min); tests are wait-bound | Trivial, but costs money | None | Not worth it |

## 3. Prototype: matrix split (draft PR #87, branch `experiment/parity-speed`)

Changes, all on the experiment branch only:

- `client_thread_parity.py --shard I/N` splits the provider scenarios into N shards with a
  greedy cost balance (restart ≈ 30 s, `cli restored` ≈ 150 s, others ≈ 10 s). The seeded
  thread checks run on shard 1. All 44 scenarios are covered exactly once (checked).
- `scripts/ci_parity_shard.sh <shard>` holds the commands for each shard: `thread-1-4` …
  `thread-4-4`, `core` (initial sync, codex schemas, client RPC, harness, edges,
  orchestration, config, config snapshot, auth, CORS, MCP, peer link), `integrations`
  (GitHub, Linear, source control hosts, git workflow, preview, relay, relay client;
  the only shard that installs Chromium) and `relay-provisioning`. Every command from
  the old serial step appears exactly once.
- A separate `rust` job runs fmt, clippy, tests, the schema corpus and t3-agentd.
- `Swatinem/rust-cache`, and `concurrency` with cancel-in-progress for PRs.

**Finding while prototyping:** the first matrix run failed in every shard without
Chromium with `Cannot find package 'effect'`. `pnpm --filter … install --frozen-lockfile`
under pnpm 12 does not leave the reference runnable. The later
`pnpm --filter t3 exec playwright-core install` triggers a second, workspace-wide install
(+366 packages, "Scope: 34 of 43 workspace projects"), and today's workflow depends on
that side effect without saying so. The fix on the branch is an explicit
`pnpm --dir reference --filter t3 exec node --version` in each job. Main should make
this explicit too, whatever else it adopts.

### Measured matrix run (run 44968097002, head 3d4823f, cold cargo cache)

All 8 jobs passed. **Wall time: 7 min 45 s**, compared with ~29.5 min on main.

| Job | Fetch | Prep | Cache restore | Build | Compare | Job total |
|---|---:|---:|---:|---:|---:|---:|
| rust (fmt, clippy, test, corpus, agentd) | 16 s | 32 s | 19 s | — | 158 s + 66 s | 314 s |
| parity (thread-1-4), incl. thread checks | 12 s | 44 s | 17 s | 63 s | 245 s | 394 s |
| parity (thread-2-4) | 17 s | 41 s | 17 s | 68 s | 264 s | 417 s |
| parity (thread-3-4) | 17 s | 33 s | 12 s | 55 s | 225 s | 353 s |
| **parity (thread-4-4)** | 17 s | 33 s | 19 s | 86 s | **292 s** | **460 s** (critical path) |
| parity (core) | 17 s | 38 s | 20 s | 84 s | 255 s | 429 s |
| parity (integrations), with Chromium | 17 s | 56 s | 16 s | 70 s | 126 s | 296 s |
| parity (relay-provisioning) | 16 s | 32 s | 13 s | 58 s | 177 s | 305 s |

Each shard spends ~2.5–3 min on setup before its first comparison, and about 1 min of
that is the cold `cargo build`. Locally, `--shard 2/4` took 3 min 55 s, against 4 min
24 s on CI, so the cost model carries over. The shards are uneven (225–292 s), and
`core` is now nearly as long as the slowest thread shard.

### Local check of option D (settle floor)

With the 8 s floor set to 0 or 2 s through a scratch copy (not committed), the
restart scenarios `claude restart mid-turn`, `codex restart idle`,
`codex restart continuation off` and `codex restart unaccepted` all still passed.
Their time fell from ~121 s with the 8 s floor to 54 s (floor 0) and 70 s (floor 2).
That the tests pass is not proof the floor is unneeded. For a "nothing continues"
scenario, an early snapshot passes either way, so a Rust server that continued late
would slip through. The floor should be replaced by a positive signal, not deleted.

## 4. Recommended plan

Ordered steps. Times are expected PR wall time after each step.

1. **Adopt the matrix** from this branch, cleaned up: a separate `rust` job, a 7-shard
   parity matrix via `scripts/ci_parity_shard.sh`, `client_thread_parity.py --shard`,
   `Swatinem/rust-cache`, `concurrency` with cancel-in-progress for PRs, and the explicit
   `pnpm … exec node --version` that completes the reference install.
   **~7.5 min measured cold; ~6.5 min expected warm.** Set `timeout-minutes` per job to
   ~15. Rebalance from measured times: give `thread-4` one fewer restart scenario, and
   move `config_parity` (62 s) and `client_rpc_parity` (43 s) out of `core` into
   `integrations`, which has the most room.
2. **Remove fixed waits in `client_thread_parity.py` restart scenarios** (option D).
   Replace the 8 s settle floor with a positive "restart recovery finished" signal from
   both servers (a trace event, or the projection showing the continuation decision),
   and the 1–2 s sleeps with `wait_for` on the state they are standing in for. Also apply
   the libfaketime jump to the reference's 60 s continuation retry (option E), as
   `orchestration_parity.py` already does for its clocks. Expected: thread shards
   ~250 s → ~150 s. **~5 min.** At that point 3 thread shards are enough, which saves
   a runner.
3. **Trim setup** (option K): cache the reference `node_modules` (post-`exec` state)
   keyed on the pinned commit and `pnpm-lock.yaml`, and the Playwright browser for
   `integrations`. **~4.5 min.**
4. **Tighten the remaining long suites** (options H, I): run `relay_provisioning`'s
   scenarios concurrently, each with its own fake T3 Connect. Shorten `relay_client`'s
   4 s negative waits to 2 s only if that stays reliable. Optionally run the original and
   Rust servers concurrently within each scenario (option F) for `client_rpc_parity` and
   `relay_client_parity`, which have no timing assertions between the two servers.
   **~4 min.**
5. **`paths-ignore`** for docs-only changes (`**/*.md`, `docs/**`). Don't try to pick
   suites per change: nearly every change goes through the same server crate.

Not recommended: reusing one server per suite (option J, weakens isolation that the
scenarios assert), release builds (M), self-hosted runners (P, untrusted fork PRs on a
public repo), larger runners (Q, the suite waits on timers rather than CPU).

**Keep on a nightly full run** (`schedule:` on main, plus every push to main):

- The full matrix with no path filters, run twice to surface flakes.
- The real-time variants that steps 2 and 4 speed up: the 60 s continuation retry in
  `claude restart cli restored` without faketime, the 8 s settle floor kept as a
  belt-and-braces check of "nothing continues late", and `relay_client`'s 4 s negative
  waits.
- Optionally, one serial unsharded run each week, to catch suites that only pass because
  of what an earlier suite left behind (none known; every script uses temporary homes).

**Open questions for the maintainer:**

- Whether a positive "restart recovery done" signal exists in both servers' logs, or
  whether one should be added to Rust and read from the reference's
  `server.trace.ndjson`.
- Whether PRs may run ~8 runners each. That is free on a public repository and within
  the 20-job concurrency limit, but two pushes in quick succession will queue unless
  cancel-in-progress is on.
