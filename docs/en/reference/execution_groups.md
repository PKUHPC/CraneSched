# Execution Groups

CraneSched carries execution groups as one ordered list on every job and step
request:

```text
gids[0] = effective (primary) GID
gids[1:] = supplementary GIDs
```

The FrontEnd collects the effective GID and the process supplementary groups,
removes duplicates, and always places the effective GID first. The job/step
`uid + gids` always identifies the submitter on the host, including a container
job's batch script and child steps submitted through `ccon run`.

The Backend performs the final authorization for the job/step UID on each
execution node. It requires `gids[0]` to exist in that user's NSS group set.
A missing primary GID fails the step before its payload starts. Supplementary
groups are intersected with the node's groups; missing values are dropped with a
bounded warning and execution continues. Groups present on the node but not
requested by the FrontEnd are never added automatically.

For a multi-node step, every node must pass the primary-GID check before the
allocation is acknowledged. A node failure therefore prevents a partial
payload start and the scheduler performs the normal status, cgroup, and
resource cleanup.

## Container identity

`job/step.uid + gids` identifies the submitter on the host, while
`pod_meta.run_as_user/run_as_group` identifies the user inside the container.
Neither `ccon --user` nor `cbatch --pod-user` overwrites the submitter's groups
or resolves the container UID through host NSS.

- **Without a user namespace:** the container runs with the submitter's UID,
  effective GID, and node-validated supplementary groups. Explicit UID/GID
  values must match the submitter's UID/EGID, including for root submitters.
- **With a user namespace:** the default container identity is `0:0`;
  `--user` / `--pod-user` may select any UID/GID covered by the node's namespace
  mapping.
  Submission fails if any requested group differs from the EGID; groups are
  never silently discarded to enable userns. Repeated EGIDs are not extra
  groups. Container IDs do not have to exist in host NSS.

A UID without a GID keeps the mode's default GID: 0 with userns, or the
submitter's EGID without userns. Both modes set CRI
`SupplementalGroupsPolicy=Strict` to prevent image-defined groups from being
added. The container runtime must support this policy.

For standalone jobs, the FrontEnd validates identity as soon as submission
credentials and the userns options from the script and CLI are available.
Container steps inherit their parent job's Pod configuration and are checked
when the controller obtains the parent job. The controller and execution
nodes also validate identity; nodes reject extra userns groups before NSS
intersection can hide unsupported groups.

Userns UID/GID mappings use the account's existing SubUID/SubGID ranges.
Managed SubGID allocation still selects the range using the account's NSS
primary GID; changing a submission's EGID does not reallocate the range.
The namespace mapping always starts at container ID 0, independently of the
requested run-as identity. Nodes check UID and GID against their respective
actual ranges before starting the payload.
Native idmapped mounts and bindfs map the submitter's UID/EGID to the selected
container UID/GID (default `0:0`). Mount translation and new-file group ownership
no longer assume that the EGID equals the NSS primary GID. Preserving host
supplementary-group mount permissions with userns is currently unsupported.

This is a breaking protocol change. Backend, FrontEnd, Craned, Cfored, and
client binaries must be upgraded as one version window. Do not run an old
scalar-GID client with a new Backend or mix versions during deployment.
