#!/bin/sh
# with_system_policy.sh COMMAND...: runs COMMAND with the file named by
# $T3CODE_DEV_MANAGED_POLICY_PATH at the system policy path, /etc/t3code/policy.json,
# and that variable unset, so a release build (which reads only the system path)
# is tested too. /etc is an overlay in a private mount namespace: the host's /etc
# is untouched and no root is needed. A nested user namespace maps the caller's
# uid back, so COMMAND runs as the caller. Needs unprivileged user namespaces and
# overlayfs in them (Linux).
set -e
work=$(mktemp -d)
mkdir -p "$work/up/t3code" "$work/w"
cp "$T3CODE_DEV_MANAGED_POLICY_PATH" "$work/up/t3code/policy.json"
uid=$(id -u) gid=$(id -g)
exec unshare -rm sh -c '
  mount -t overlay overlay -o "lowerdir=/etc,upperdir=$0/up,workdir=$0/w" /etc
  unset T3CODE_DEV_MANAGED_POLICY_PATH
  uid=$1 gid=$2
  shift 2
  exec unshare -U --map-user="$uid" --map-group="$gid" -- "$@"' "$work" "$uid" "$gid" "$@"
