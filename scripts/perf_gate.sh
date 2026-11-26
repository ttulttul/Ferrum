#!/usr/bin/env bash
# Whether the Performance workflow measures now (perf.yml's gate).
#
#   scripts/perf_gate.sh REPO REF [NOW] [FORCE]
#
# It measures when no bot refresh of docs/performance.md is on REF, or the last one is at least 24
# hours old (by commit time), or FORCE is "true". The clock is the refresh bot's own commits
# ("Refresh the performance numbers from ..." by github-actions[bot]), not any edit of the file: a
# pull request that documents a performance change in it does not hold the measurement back.
# Prints "measure=true" or "measure=false" last, after a line saying why.
set -euo pipefail
repo=$1
ref=$2
now=${3:-$(date +%s)}
force=${4:-false}
if [ "$force" = true ]; then
  echo "Measuring: asked to (force)."
  echo "measure=true"
  exit 0
fi
refreshed=$(git -C "$repo" log -1 --format=%ct --author='github-actions\[bot\]' \
  --grep='^Refresh the performance numbers' "$ref" -- docs/performance.md)
if [ -z "$refreshed" ]; then
  echo "Measuring: no bot refresh of docs/performance.md on $ref yet."
  echo "measure=true"
  exit 0
fi
age=$(( now - refreshed ))
if [ "$age" -ge 86400 ]; then
  echo "Measuring: the numbers in docs/performance.md were last refreshed $((age / 3600)) hours ago."
  echo "measure=true"
else
  echo "::notice title=Performance::Not measuring: the numbers in docs/performance.md were refreshed $((age / 3600)) h $((age % 3600 / 60)) min ago, less than 24 hours."
  echo "measure=false"
fi
