# Execution Groups

CraneSched carries execution groups as one ordered list on every job and step
request:

```text
gids[0] = effective (primary) GID
gids[1:] = supplementary GIDs
```

The FrontEnd collects the effective GID and the process supplementary groups,
removes duplicates, and always places the effective GID first. These execution
groups always describe the submitter's permissions on the host, including a
container job's batch script and child steps submitted through `ccon run`.

The Backend performs the final authorization for the submitter on each
execution node. It requires the effective GID to exist in that user's NSS group set.
A missing primary GID fails the step before its payload starts. Supplementary
groups are intersected with the node's groups. If any requested supplementary
group is removed, the step logs a warning and execution continues. The warning
does not list group IDs or counts. Groups present on the node but not requested
by the FrontEnd are never added automatically and do not cause a warning.

For a multi-node step, every node must pass the primary-GID check before the
allocation is acknowledged. A node failure therefore prevents a partial
payload start and the scheduler performs the normal status, cgroup, and
resource cleanup.

## Container identity

The submitter's host identity and the identity used inside the container are
independent. `ccon --user` and `cbatch --pod-user` select only the identity
inside the container; they do not change the submitter's host group permissions.

- **Without a user namespace:** the container runs with the submitter's UID,
  effective GID, and node-validated supplementary groups. Explicit UID/GID
  values must match the submitter's UID/EGID, including for root submitters.
- **With a user namespace:** the default container identity is `0:0`;
  `--user` / `--pod-user` may select any UID/GID covered by the node's namespace
  mapping.
  The job uses only the submitter's effective GID; supplementary groups are
  omitted. Users belonging to multiple host groups can submit userns jobs,
  but those jobs do not retain supplementary-group permissions.
  Container IDs do not have to exist in host NSS.

A UID without a GID keeps the mode's default GID: 0 with userns, or the
submitter's EGID without userns. Both modes set CRI
`SupplementalGroupsPolicy=Strict` to prevent image-defined groups from being
added. The container runtime must support this policy.

For standalone jobs, the FrontEnd keeps only the effective GID for userns and
validates identity once submission credentials and the final userns options
from the script and CLI are available. Container steps inherit their parent
job's Pod configuration; for userns, the controller also replaces their group
list with the parent job's effective GID before validation. This applies when
the submitting process still has supplementary groups or a different EGID.
The controller and execution nodes retain validation of the single-group
userns restriction.

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
