# Execution Groups

CraneSched carries execution groups as one ordered list on every job and step
request:

```text
gids[0] = effective (primary) GID
gids[1:] = supplementary GIDs
```

The FrontEnd collects the effective GID and the process supplementary groups,
removes duplicates, and always places the effective GID first. For an explicit
container `--user uid:gid`, groups are resolved through NSS for the requested
user before submission.

The Backend performs the final authorization on each execution node. It
requires `gids[0]` to exist in that node's NSS group set. A missing primary GID
fails the step before its payload starts. Supplementary groups are intersected
with the node's groups; missing supplementary values are dropped with a
bounded warning and execution continues. Groups present on the node but not
requested by the FrontEnd are never added automatically.

For a multi-node step, every node must pass the primary-GID check before the
allocation is acknowledged. A node failure therefore prevents a partial
payload start and the scheduler performs the normal status, cgroup, and
resource cleanup.

This is a breaking protocol change. Backend, FrontEnd, Craned, Cfored, and
client binaries must be upgraded as one version window. Do not run an old
scalar-GID client with a new Backend or mix versions during deployment.
