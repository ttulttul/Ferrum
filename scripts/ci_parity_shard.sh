#!/usr/bin/env bash
# Run one shard of the server parity suite (see .github/workflows/parity.yml).
# Usage: scripts/ci_parity_shard.sh <shard> <reference checkout> <rust server binary>
#
# Every scripts/*_parity.py that CI runs must appear in exactly one shard below;
# scripts/check_parity_shards.py enforces that.
set -euo pipefail
shard=$1
checkout=$2
server=$3
reference="node $checkout/apps/server/src/bin.ts"
faketime=${FAKETIME_LIB:-/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1}
both=(--original-command "$reference" --rust-command "$server")

# Run one suite and print how long it took, for balancing the shards.
run() {
  local started=$SECONDS
  "$@"
  echo "::notice title=parity ${shard}::$(basename "$2") took $((SECONDS - started)) s"
}

case "$shard" in
  thread-*)
    # thread-I-N: provider scenario shard I of N (shard 1 also runs the seeded thread checks).
    IFS=- read -r _ index count <<<"$shard"
    # PARITY_REAL_TIME=1 waits out the restart timers that faketime otherwise skips.
    real_time=()
    if [ "${PARITY_REAL_TIME:-0}" = 1 ]; then
      real_time=(--real-time)
    fi
    run python3 scripts/client_thread_parity.py "${both[@]}" --reference-root "$checkout" --shard "$index/$count" \
      --faketime-lib "$faketime" "${real_time[@]}"
    ;;
  core)
    run python3 scripts/client_initial_sync_parity.py "${both[@]}" --reference-root "$checkout"
    run node scripts/codex_fixture_schemas.mjs "$checkout"
    run python3 scripts/parity_harness.py --managed --check-required-fields "${both[@]}"
    run python3 scripts/rpc_edge_parity.py "${both[@]}"
    run python3 scripts/orchestration_parity.py "${both[@]}" --faketime-lib "$faketime"
    run python3 scripts/config_snapshot_parity.py "${both[@]}" --reference-root "$checkout"
    run python3 scripts/auth_parity.py "${both[@]}"
    run python3 scripts/cors_parity.py "${both[@]}"
    run python3 scripts/mcp_parity.py "${both[@]}"
    run python3 scripts/peer_link_parity.py "${both[@]}"
    run python3 scripts/client_sessions_parity.py "${both[@]}"
    run python3 scripts/peer_link_outgoing_parity.py "${both[@]}"
    run python3 scripts/shared_store_parity.py "${both[@]}" --reference-root "$checkout"
    run python3 scripts/peer_forwarding_parity.py "${both[@]}" --reference-root "$checkout"
    run python3 scripts/login_shell_refresh_parity.py "${both[@]}"
    ;;
  integrations)
    run python3 scripts/storage_cleanup_parity.py "${both[@]}"
    run python3 scripts/config_parity.py "${both[@]}"
    run python3 scripts/github_provider_parity.py "${both[@]}" --faketime-lib "$faketime"
    run python3 scripts/linear_provider_parity.py "${both[@]}"
    run python3 scripts/source_control_hosts_parity.py "${both[@]}"
    run python3 scripts/git_workflow_parity.py "${both[@]}"
    run python3 scripts/source_control_clone_parity.py "${both[@]}"
    run python3 scripts/source_control_lookup_parity.py "${both[@]}"
    run python3 scripts/project_clone_parity.py "${both[@]}"
    run python3 scripts/project_favicon_parity.py "${both[@]}"
    run python3 scripts/bitbucket_pull_requests_parity.py "${both[@]}"
    run python3 scripts/bitbucket_settings_parity.py "${both[@]}"
    run python3 scripts/bitbucket_pull_request_parity.py "${both[@]}"
    run python3 scripts/acp_install_parity.py "${both[@]}"
    run python3 scripts/preview_provider_parity.py "${both[@]}" --browser-cache "$HOME/.cache/ms-playwright" --reference-root "$checkout"
    run python3 scripts/html_render_parity.py "${both[@]}" --browser-cache "$HOME/.cache/ms-playwright" --reference-root "$checkout"
    run python3 scripts/preview_session_parity.py "${both[@]}" --browser-cache "$HOME/.cache/ms-playwright" --reference-root "$checkout"
    run python3 scripts/relay_parity.py --checkout "$checkout" --rust "$server"
    run python3 scripts/link_request_parity.py "${both[@]}"
    run python3 scripts/secret_request_parity.py "${both[@]}"
    ;;
  status-relay)
    # Suites that run their servers side by side, so mostly wait on the servers' timers.
    # provider_status: one scenario per driver and case, several of which wait out the providers' status timeouts.
    run python3 scripts/provider_status_parity.py "${both[@]}"
    run python3 scripts/relay_provisioning_parity.py "${both[@]}"
    run python3 scripts/client_rpc_parity.py "${both[@]}" --reference-root "$checkout"
    run python3 scripts/relay_client_parity.py "${both[@]}"
    ;;
  *)
    echo "unknown shard: $shard" >&2
    exit 2
    ;;
esac
