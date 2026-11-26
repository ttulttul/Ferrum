#!/usr/bin/env bash
# Checks scripts/perf_gate.sh's decision on a throwaway repository with fixed commit times.
set -euo pipefail
gate=$(cd "$(dirname "$0")" && pwd)/perf_gate.sh
repo=$(mktemp -d)
trap 'rm -rf "$repo"' EXIT
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
git -C "$repo" init -qb main
day=86400
t0=1800000000
commit() { # commit AUTHOR_NAME AUTHOR_EMAIL TIME MESSAGE
  echo "$4 $3" >> "$repo/docs/performance.md"
  git -C "$repo" add docs/performance.md
  GIT_AUTHOR_DATE="@$3 +0000" GIT_COMMITTER_DATE="@$3 +0000" \
    git -C "$repo" -c user.name="$1" -c user.email="$2" commit -qm "$4"
}
human() { commit "Dev" "dev@example.test" "$1" "$2"; }
bot() { commit "github-actions[bot]" "41898282+github-actions[bot]@users.noreply.github.com" "$1" "$2"; }
decide() { "$gate" "$repo" main "$1" "${2:-false}" | tail -n 1; }
expect() { # expect WANT GOT WHAT
  if [ "$2" != "$1" ]; then echo "FAIL $3: got $2, want $1"; exit 1; fi
  echo "PASS $3"
}
mkdir -p "$repo/docs"
human "$t0" "Add the performance doc"
expect measure=true "$(decide $((t0 + 60)))" "no bot refresh yet: measure"
bot $((t0 + 100)) "Refresh the performance numbers from 0123456789ab (#1)"
expect measure=false "$(decide $((t0 + 100 + day - 1)))" "bot refresh under 24 h ago: skip"
expect measure=true "$(decide $((t0 + 100 + day)))" "bot refresh 24 h ago: measure"
human $((t0 + 100 + day + 10)) "Document a faster commit (#2)"
expect measure=true "$(decide $((t0 + 100 + day + 20)))" "a hand edit after an old refresh does not hold it back"
bot $((t0 + 2 * day)) "Refresh the performance numbers from 1234567890ab (#3)"
human $((t0 + 2 * day + 10)) "Document another change (#4)"
expect measure=false "$(decide $((t0 + 2 * day + 20)))" "a hand edit does not restart the clock either way"
commit "Dev" "dev@example.test" $((t0 + 2 * day + 30)) "Refresh the performance numbers by hand"
expect measure=true "$(decide $((t0 + 3 * day)))" "a human commit with the bot's subject is not a refresh"
expect measure=true "$(decide $((t0 + 2 * day + 40)) true)" "force: measure"
echo "perf gate: all checks passed"
