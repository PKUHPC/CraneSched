# CraneTestKit privileged CI contract

`.github/workflows/build.yaml` is the trusted dispatcher for the K3s system
suite. Pull requests use `pull_request_target` only to authorize and resolve
immutable revisions on a GitHub-hosted runner. The hosted job never checks out
or executes pull-request code. A privileged job is dispatched only when all of
the following are true:

- the workflow was loaded from `master`;
- the pull request is from this repository, not a fork;
- `github.triggering_actor` currently has `maintain` or `admin` permission;
- Backend and FrontEnd refs resolve to full commit SHAs.

The current cutover uses the existing repository-scoped runner and selects it
with the `cranesystemtest` label. This temporary mode does not isolate the
runner from other workflows in `PKUHPC/CraneSched`: labels route jobs, but do
not authorize workflows. Keep the runner stopped except for controlled CI runs
until an organization owner completes the runner-group migration below.

The target state is an organization runner group that allows only
`PKUHPC/CraneSched/.github/workflows/build.yaml@refs/heads/master`. Its runner
must keep the `cranesystemtest` label and runner name. After migration, restore
the workflow's `runs-on.group` selector using the
`CRANESCHED_PRIVILEGED_RUNNER_GROUP` repository variable so the group name is
not duplicated in source.

## Repository variables

The privileged job consumes no repository or organization secrets. Install
the restricted ServiceAccount kubeconfig on `cranesystemtest` and configure
these repository variables:

| Variable | Contract |
| --- | --- |
| `CRANESCHED_PRIVILEGED_RUNNER_GROUP` | Target-state organization runner group; `Default` is only a temporary cutover marker and is not consumed by label-only routing |
| `CRANETESTKIT_RELEASE_DIR` | Absolute path to a root-owned, recursively read-only AutoTest Git release |
| `CRANETESTKIT_AUTOTEST_SHA` | Exact 40-character commit deployed at the release path |
| `CRANETESTKIT_EXECUTABLE_SHA256` | SHA-256 of the prepared `<release>/.venv/bin/crane_testkit` entry point |
| `CRANETESTKIT_PROFILE_PATH` | Absolute path to the tracked profile inside that release |
| `CRANETESTKIT_PROFILE_SHA256` | SHA-256 of the deployed profile |
| `CRANETESTKIT_BUNDLE_ID` | Imported and admission-approved image bundle ID |
| `CRANETESTKIT_IMAGE_RUNTIME_SHA256` | SHA-256 of `<nfs-root>/images/<bundle-id>/runtime.json` |
| `CRANETESTKIT_K3S_EXECUTABLE` | Absolute root-owned K3s client path; digest and version must match `images.lock.yaml` |
| `CRANETESTKIT_KUBECONFIG` | Absolute path to the mode `0600` client-certificate kubeconfig for the restricted CI identity |
| `CRANETESTKIT_NFS_ROOT` | Mounted NFS root matching `storage.nfs` in the profile |
| `CRANETESTKIT_CCACHE_DIR` | Persistent ccache directory on the runner's local SSD |
| `CRANETESTKIT_FETCHCONTENT_DIR` | Persistent CMake FetchContent directory on the runner's local SSD |

The cache variables are fixed to `/opt/crane-testkit/cache/ccache` and
`/opt/crane-testkit/cache/fetchcontent`. The maintenance policy owns those exact
paths; alternate descendants are not supported.

The release must contain a prepared `.venv/bin/crane_testkit`. Remove every
owner/group/other write bit recursively after deployment and publish the entry
point checksum with the release SHA. Preflight validates the checksum, shebang,
isolated `pyvenv.cfg`, editable source pointer and `.pth` startup set before it
loads the release interpreter. The runner's trusted `/usr/bin/python3` (Python
3.9 or newer) must provide PyYAML for preflight parsing; `/usr/bin/findmnt` and
`/usr/bin/openssl` must also be root-owned system binaries. The helpers do not
require `uv` or Python 3.12. CI never pulls or updates the release. Deploy a new
root-owned release, image bundle, profile policy and repository-variable
checksums as one administrator operation. Do not point these variables at a
mutable developer checkout.

The kubeconfig must target the profile API address, embed its cluster CA and
use `crane-testkit` as its current namespace. Authenticate with a client
certificate whose CN is exactly
`system:serviceaccount:crane-testkit:crane-testkit-ci`; token, exec,
auth-provider and password credentials are rejected. Certificate/key data may
be embedded, or referenced through root-owned absolute non-symlink paths (the
private key must be mode `0600` or stricter). Preflight validates certificate
identity, at least four hours of remaining validity, and key matching without
printing credential material. Rotate and redeploy the certificate before it
enters that four-hour window; a near-expiry certificate fails before source
checkout.

Every run verifies the complete positive permission set for Jobs, Pods,
create-only Secrets and Leases and an expanded negative matrix for Secret
reads, Pod create/exec/log, Namespace, Node, ServiceAccount and RBAC operations.
Pod subresources are checked with kubectl's explicit form, for example
`auth can-i get pods --subresource=log --namespace crane-testkit`; do not use
`pods/log`, which kubectl interprets as a Pod named `log`.
It then renders canonical Jobs from the validated release/profile/runtime and
uses server-side dry-run to require that approved Jobs pass while forbidden
nodeName, lifecycle, host-network, image and NFS mutations are denied by
`crane-testkit-ci-jobs`. This verifies the admission boundary without creating
resources. Permission or policy drift fails before pull-request code is checked
out.

## Runner setup

The current migration baseline is the dedicated root-owned Backend service
`actions.runner.PKUHPC-CraneSched.cranesystemtest`. Root is currently required
to collect and clean NFS run artifacts written by privileged Pods, so the
hosted authorization, organization runner-group restriction and Kubernetes
RBAC checks are mandatory compensating controls. The separate AutoTest service
`actions.runner.PKUHPC-CraneSched-AutoTest.cranesystemtest-autotest` must not
receive the Backend runner group or `cranesystemtest` label. A future non-root
migration requires an explicit NFS ownership/ACL validation first. The
preflight accepts a private kubeconfig owned by root or the effective runner
account. Install Actions Runner `2.327.1` or newer; the pinned
`upload-artifact@v6` action uses Node.js 24. The following state belongs on
local SSD:

- Actions workspace;
- ccache and FetchContent/CMake dependency caches;
- temporary Compose build output.

Only content-addressed `builds/` and immutable `runs/` are published under the
configured NFS root. Preflight uses `findmnt` to require `nfs`/`nfs4`, the exact
profile `server:export`, and the exact `rootRelative` path; a writable local
directory at the same pathname is rejected. Do not clear ccache, CMake, Go,
FetchContent or frontend caches at job start. Apply capacity and retention
outside the active build and run directories. Both build caches use the fixed
paths under `/opt/crane-testkit/cache` listed above. The build holds a shared
flock on `/opt/crane-testkit/cache/.maintenance.lock` for the complete Compose
build; host retention must hold the same lock exclusively and fail closed when
it cannot acquire it.

Configure the runner service `.env` with the run-scoped hooks from the same
exact release:

```text
ACTIONS_RUNNER_HOOK_JOB_STARTED=<release>/github-runner/action_job_started.sh
ACTIONS_RUNNER_HOOK_JOB_COMPLETED=<release>/github-runner/action_job_completed.sh
CRANE_TESTKIT_HOOK_STATE_DIR=/run/crane-testkit-runner
CRANE_TESTKIT_RELEASE_ROOT=<release>
CRANE_TESTKIT_PROFILE=<release>/profiles/wrl-lab.yaml
CRANE_TESTKIT_K3S_EXECUTABLE=<pinned-k3s-client>
KUBECONFIG=<restricted-mode-0600-kubeconfig>
```

The start hook derives exactly `gh-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}` and
writes only that value to
`<state-dir>/<GITHUB_RUN_ID>-<GITHUB_RUN_ATTEMPT>.run-id`. The workflow verifies
the inherited hook paths, release/profile/client/kubeconfig values, marker and
absence of legacy agent environment before any source checkout or build. The
completion hook may clean only that exact run. It must never dump the
environment to `ci.lock`, clean global Podman names, or launch the old
failure-analysis agent.

The AutoTest release remote must not contain a PAT. Rotate any previously
embedded PAT, then use a short-lived read-only GitHub App or deploy credential
only during the administrator deployment. CI itself performs no AutoTest Git
network operation.

## Organization runner-group migration

An organization owner must perform this migration before the Backend runner is
left online between runs:

1. Create a PKUHPC organization runner group with selected-repository access
   limited to `PKUHPC/CraneSched` and selected-workflow access limited to
   `PKUHPC/CraneSched/.github/workflows/build.yaml@refs/heads/master`.
2. Stop the repository-scoped Backend runner and confirm it is idle, no
   CraneTestKit Lease is held, and no matching Actions job is queued.
3. Remove the repository-scoped registration and register the same runner at
   `https://github.com/PKUHPC` in the new group. Keep runner name and custom
   label `cranesystemtest`; preserve the verified release hooks and restricted
   kubeconfig in the service environment. Registration and removal tokens must
   be short-lived, passed without logging, and never stored on disk.
4. Set `CRANESCHED_PRIVILEGED_RUNNER_GROUP` to the new group name and restore
   the workflow selector to:

   ```yaml
   runs-on:
     group: ${{ vars.CRANESCHED_PRIVILEGED_RUNNER_GROUP }}
     labels: cranesystemtest
   ```

5. Start only the Backend runner, dispatch `build.yaml` from `master`, and
   verify the `test` job reports the new organization group before leaving the
   service enabled. The AutoTest maintenance runner remains a separate
   registration and group.

## Artifacts and result contract

`env build --publish-root` writes an exact package manifest under
`builds/<build-id>/`. `system execute` creates
`runs/<run-id>/{input,plan,results,logs,result.json,state.json}` and owns Lease,
collection and exact run cleanup.

The workflow uploads only an allowlisted copy of plan, state, aggregate result,
build/image manifests and checkpoints. Shard logs are uploaded only on failure,
are capped at 4 MiB per file and 128 MiB in total, and pass through credential
redaction. Every value written to the GitHub step summary uses the same
redaction and Markdown escaping. Full NFS directories, environment files,
kubeconfigs and Kubernetes Secrets are never uploaded. GitHub artifacts are
retained for 14 days.

CraneTestKit exit codes remain authoritative:

- `0`: complete coverage and all tests passed;
- `1`: complete coverage with test failures or errors;
- `2`: infrastructure error or incomplete coverage.

The workflow replaces the serial implementation in
`.github/workflows/build.yaml` in place, and the privileged job keeps both the
job ID and display name `test`. It therefore takes over the existing required
check context `test`; do not delete or rename that context during cutover.
Verify the first production run before making any additional branch-protection
change, and remove only separately named obsolete contexts that an administrator
has confirmed actually exist.

If two consecutive production runs return infrastructure exit code `2`,
disable this workflow and restore the previous serial workflow from Git
history. Exit code `1` is a product/test failure and does not trigger rollback.
