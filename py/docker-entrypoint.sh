#!/bin/sh
# docker run IMAGE                          mount the filesystem at $DFS_MOUNTPOINT (needs --device /dev/fuse --cap-add SYS_ADMIN)
# docker run IMAGE days                     list the trading days on the ssh host
# docker run IMAGE symbols [DAY]            list the symbols of a day
# docker run IMAGE prefetch DAY SYMBOL...   extract into the cache without mounting
set -e
command=${1:-mount}
[ $# -gt 0 ] && shift
if [ ! -f "$DFS_CONFIG" ]; then
    echo "no config at $DFS_CONFIG: mount one (see /app/config.docker.toml for an example)" >&2
    exit 2
fi
if [ "$command" != mount ]; then
    cd /app/py && exec python -m datafeedfs "$command" "$DFS_CONFIG" "$@"
fi
mkdir -p "$DFS_MOUNTPOINT"
(cd /app/py && exec python -m datafeedfs mount "$DFS_CONFIG" "$DFS_MOUNTPOINT" --allow-other) &
child=$!
# unmount on docker stop so a bind-propagated mount does not stay behind on the host as "transport endpoint is not connected"
trap 'fusermount -uz "$DFS_MOUNTPOINT" 2>/dev/null; kill "$child" 2>/dev/null' TERM INT
wait "$child" || true
fusermount -uz "$DFS_MOUNTPOINT" 2>/dev/null || true
