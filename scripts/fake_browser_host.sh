#!/bin/sh
# fake_browser_host.sh ROLE RESTRICTED COMMAND...: runs COMMAND as a fake Linux host for
# `browser setup`, so its root path runs without root and without changing this machine.
#   ROLE        root (uid 0 in a user namespace) or user (the caller's uid).
#   RESTRICTED  0 or 1: what /proc/sys/kernel/apparmor_restrict_unprivileged_userns reads.
# /etc is an overlay whose writes land in $FAKE_HOST_WORK/up (the host's /etc is untouched);
# with FAKE_PROFILE_ON_DISK=1 the AppArmor profile is already there. Needs unprivileged user
# namespaces and overlayfs in them.
set -e
role=$1 restricted=$2
shift 2
work=$FAKE_HOST_WORK
mkdir -p "$work/up/apparmor.d" "$work/w"
printf '%s\n' "$restricted" > "$work/restrict"
if [ "${FAKE_PROFILE_ON_DISK:-0}" = 1 ]; then
  printf '# An administrator edited this profile.\n' > "$work/up/apparmor.d/t3-chrome-headless-shell"
fi
uid=$(id -u) gid=$(id -g)
exec unshare -rm sh -c '
  mount -t overlay overlay -o "lowerdir=/etc,upperdir=$0/up,workdir=$0/w" /etc
  mount --bind "$0/restrict" /proc/sys/kernel/apparmor_restrict_unprivileged_userns
  role=$1 uid=$2 gid=$3
  shift 3
  if [ "$role" = root ]; then exec "$@"; fi
  exec unshare -U --map-user="$uid" --map-group="$gid" -- "$@"' "$work" "$role" "$uid" "$gid" "$@"
