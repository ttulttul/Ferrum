#!/usr/bin/env bash
# Run one shard of the server parity suite (see .github/workflows/parity.yml).
# Usage: scripts/ci_parity_shard.sh <shard> <reference checkout> <rust server binary>
set -euo pipefail
shard=$1
checkout=$2
server=$3
reference="node $checkout/apps/server/src/bin.ts"
faketime=/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1
both=(--original-command "$reference" --rust-command "$server")

case "$shard" in
  thread-*)
    # thread-I-N: provider scenario shard I of N (shard 1 also runs the seeded thread checks).
    IFS=- read -r _ index count <<<"$shard"
    python3 scripts/client_thread_parity.py "${both[@]}" --reference-root "$checkout" --shard "$index/$count"
    ;;
  core)
    python3 scripts/client_initial_sync_parity.py "${both[@]}" --reference-root "$checkout"
    node scripts/codex_fixture_schemas.mjs "$checkout"
    python3 scripts/client_rpc_parity.py "${both[@]}" --reference-root "$checkout"
    python3 scripts/parity_harness.py --managed --check-required-fields "${both[@]}"
    python3 scripts/rpc_edge_parity.py "${both[@]}"
    python3 scripts/orchestration_parity.py "${both[@]}" --faketime-lib "$faketime"
    python3 scripts/config_parity.py "${both[@]}"
    python3 scripts/config_snapshot_parity.py "${both[@]}" --reference-root "$checkout"
    python3 scripts/auth_parity.py "${both[@]}"
    python3 scripts/cors_parity.py "${both[@]}"
    python3 scripts/mcp_parity.py "${both[@]}"
    python3 scripts/peer_link_parity.py "${both[@]}"
    ;;
  integrations)
    python3 scripts/github_provider_parity.py "${both[@]}" --faketime-lib "$faketime"
    python3 scripts/linear_provider_parity.py "${both[@]}"
    python3 scripts/source_control_hosts_parity.py "${both[@]}"
    python3 scripts/git_workflow_parity.py "${both[@]}"
    python3 scripts/preview_provider_parity.py "${both[@]}" --browser-cache "$HOME/.cache/ms-playwright"
    python3 scripts/relay_parity.py --checkout "$checkout" --rust "$server"
    python3 scripts/relay_client_parity.py "${both[@]}"
    ;;
  relay-provisioning)
    python3 scripts/relay_provisioning_parity.py "${both[@]}"
    ;;
  *)
    echo "unknown shard: $shard" >&2
    exit 2
    ;;
esac
