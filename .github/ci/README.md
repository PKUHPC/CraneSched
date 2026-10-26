# CraneTestKit privileged CI contract

`.github/workflows/pr-ci.yaml` is a minimal `pull_request` entry point. It
runs only a no-checkout GitHub-hosted job after GitHub's native fork-workflow
approval allows it. Configure **Settings > Actions > General > Approval for
running fork pull request workflows from contributors > Require approval for
first-time contributors**. GitHub decides which authors and event actors need
approval; a repository user with write access selects **Approve workflows to
run**. Existing contributors run according to the same GitHub policy, without
an additional repository-role check.

`.github/workflows/build.yaml` receives successful gate runs through
`workflow_run: completed`. Its authorization and reporting jobs execute only
trusted controls from `master`; proposed-merge workflow files are inspected as
data. Only after hosted validation does the privileged job build the exact PR
head. Master push, scheduled runs and maintainer-only manual dispatch retain
their existing entry points. `pull_request_target` is deliberately absent:
GitHub runs it regardless of native fork approval settings.

For every authorized Backend pull-request event or approved rerun, the hosted
job looks for an exact, case-sensitive branch with the same name in
`PKUHPC/CraneSched-FrontEnd`. It uses GitHub's branch-reference API, so a tag or
other commit-ish value with the same name does not match. Only an explicit 404
for that branch permits fallback to FrontEnd `master`; permission, rate-limit,
timeout and malformed-response failures stop authorization. The selected branch
head is immediately verified and locked to its full commit SHA. The privileged
runner checks out only that SHA and never falls back during checkout if the
branch subsequently moves or is deleted.

Approved fork pull requests use the same central FrontEnd branch lookup; no
FrontEnd fork is queried. Backend `push` and `schedule` events always select the
exact FrontEnd `master` branch, while `workflow_dispatch` retains its explicit
`frontend_ref` commit-ish input. FrontEnd pushes do not trigger Backend CI.
Selection is recalculated only for a new Backend event or a manual workflow
rerun, then recorded as `matching_branch`, `master_fallback`, `master_default`
or `manual_ref` in the job summary and sanitized artifact manifest.

The privileged job uses the dedicated repository-scoped runner selected by the
exact `cranesystemtest` label. Repository registration prevents other
repositories from dispatching to it, but GitHub does not restrict which
workflow in `PKUHPC/CraneSched` may request a repository runner. This is an
accepted boundary of the deployment.

## Native fork approval and PR identity

Before GitHub approves a required fork run, the PR gate cannot complete and
cannot dispatch the repository runner. The trusted dispatcher rereads the
source run and verifies its repository, canonical workflow ID, name and path,
`pull_request` event, completed/success result, and exact attempt. The one
`Request system tests` job must also have actually succeeded on its hosted
runner; a skipped job cannot serve as approval evidence. The dispatcher never
downloads PR artifacts or consumes PR-produced caches, code, or credentials
as authorization inputs.

Fork runs can have an empty `workflow_run.pull_requests` list. Resolve the
open PR by the source head owner and branch, then require exactly one match
for the full head repository, ref and SHA and the target repository/master.
Reread that PR before authorization and reject drafts, closed PRs and changed
heads. A stale gate or stale rerun cannot approve a new commit. The same
validation runs again when the trusted dispatcher is manually rerun.

The former comment-request workflow and bot attestation protocol have been
removed. Approve or rerun the native **PR CI Gate** workflow through GitHub.
For infrastructure retries, rerunning the linked trusted system workflow is
also supported while its source gate attempt and PR head remain current.

The hosted authorization job applies a compensating routing guard before it
dispatches `test`. For a pull request, it resolves the exact PR head SHA for
the build, then obtains GitHub's proposed merge commit only for this static
routing check. The merge commit must have exactly two full-SHA parents:
the live proposed base followed by the gate's current PR head. The API merge
SHA and merge ref must agree. The proposed base can advance beyond the source
gate's original base while master moves; it does not change the tested head.
These current values are checked twice with bounded retries before dispatch.
It requires `.github/workflows/build.yaml:test` to use exactly
`[self-hosted, cranesystemtest]` and every other workflow job to use the fixed
`ubuntu-latest` hosted runner. Expressions, matrix-selected runners, other
self-hosted labels and job-level reusable workflows are rejected. The check
runs trusted code from `master` and treats the proposed workflow files only as
data. The privileged job still builds and tests the PR head SHA, not the
temporary merge commit. Protect
`.github/workflows/**` and `.github/ci/**` through branch protection and
required maintainer review because this static guard is an accidental-change
check, not a GitHub-enforced workflow authorization boundary.
The sanitized artifact manifest and summary retain the base, head, routing and
authorized FrontEnd revisions so reviewers can distinguish tested source from
the inspected merge tree. If a generated plan or aggregate result omits the
FrontEnd SHA or disagrees with the authorized SHA, artifact collection reports
an infrastructure error with exit code `2`.

## Repository variables

The privileged job consumes no repository or organization secrets. Install
the restricted ServiceAccount kubeconfig on `cranesystemtest` and configure
these repository variables:

| Variable | Contract |
| --- | --- |
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
resources. Permission or policy drift fails before pull-request source is
checked out on the privileged runner.

## Runner setup

The dedicated Backend service is
`actions.runner.PKUHPC-CraneSched.cranesystemtest`. Root is currently required
to collect and clean NFS run artifacts written by privileged Pods, so the
hosted authorization, static routing guard and Kubernetes RBAC checks are
mandatory compensating controls. The separate AutoTest service
`actions.runner.PKUHPC-CraneSched-AutoTest.cranesystemtest-autotest` must not
receive the Backend `cranesystemtest` label. A future non-root migration
requires an explicit NFS ownership/ACL validation first. The preflight accepts
a private kubeconfig owned by root or the effective runner account. Install
Actions Runner `2.327.1` or newer; the pinned
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

## Repository runner boundary

Keep the Backend runner registered directly to `PKUHPC/CraneSched`, with
runner name and custom label both set to `cranesystemtest`. Keep the AutoTest
maintenance runner registered directly to `PKUHPC/CraneSched-AutoTest`, with
its distinct `cranesystemtest-autotest` label. Do not give either service the
other service's label or environment.

Only the `test` job in `build.yaml` may select the Backend runner. The hosted
routing guard scans every `.yaml` and `.yml` file directly under
`.github/workflows` at the authorized proposed-merge SHA and fails closed on
symlinked or noncanonical workflows, a missing privileged job, any other
self-hosted route, dynamic runner selection, or job-level reusable workflows.
The checkout also verifies the merge commit's HEAD and parent SHAs before
scanning.

The Clang Format check is intentionally read-only. It checks the exact PR head
on a hosted runner, never pushes a formatting commit, and uploads a short-lived
binary patch when formatting changes are required. Same-repository and fork
pull requests have the same behavior; a contributor applies the patch and
pushes a normal commit, which then triggers the normal PR events.

Required maintainer review, branch protection, the trusted
`workflow_run` dispatcher and the restricted Kubernetes identity remain
part of the repository-level security boundary.

## RPM repository publishing

After the system-test exit contract succeeds, CI uploads the exact tested RPM
set to Nexus. Publishing runs for `master` pushes and schedules, and manual
runs loaded from `master` with both source inputs set to `master`. Pull-request
runs and manual builds of other refs do not publish packages.

The upload step alone receives the repository Actions secrets `CCREPO_URL`,
`CCREPO_USERNAME` and `CCREPO_PASSWORD`. It skips publishing when any is missing.
The uploader comes from the pinned trusted CI controls checkout, so restoring
this feature requires only a Backend change and no AutoTest release deployment.
Before uploading, the validated AutoTest release rechecks the build manifest,
RPM metadata, sizes, digests and exact Backend/FrontEnd revisions.

The restored Nexus uploader is `.github/ci/upload-packages.sh`. The workflow
uses `UPLOAD_MODE=RPM`, repository `crane`, distro `8`, architecture `x86_64`,
and version channel `master`, preserving the historical URL layout:

```text
<CCREPO_URL>/repository/crane/master/8/x86_64/RPMS/<RPM filename>
```

Cache hits also attempt publishing, allowing a workflow rerun to retry an
earlier upload failure. Each file has at most three attempts, a 15-second
connection timeout and a 300-second transfer timeout; the entire step has a
20-minute limit. Missing secrets or upload failures do not change the system
test result; upload errors remain visible in the optional step's log.

## Artifacts and result contract

`env build --publish-root` writes an exact package manifest under
`builds/<build-id>/`. `system execute` creates
`runs/<run-id>/{input,plan,results,logs,result.json,state.json}` and owns Lease,
collection and exact run cleanup.

The workflow uploads only an allowlisted copy of plan, state, aggregate result,
build/image manifests and checkpoints. Shard logs are uploaded only on failure,
are capped at 4 MiB per file and 128 MiB in total, and pass through credential
redaction. The GitHub step summary renders the aggregate outcome, affected
cases, infrastructure errors, per-shard balance, phase timings, slowest cases
and folded provenance. Dynamic values use the same redaction and Markdown
escaping, and report rows are capped before rendering. Full NFS directories,
environment files, kubeconfigs and Kubernetes Secrets are never uploaded.
GitHub artifacts are retained for the repository maximum of 3 days; durable
run evidence remains under the configured NFS retention policy.

The collector cross-checks the workflow-captured process exit code against the
aggregate result, run state and terminal shard evidence. Any mismatch or
incomplete evidence resolves to infrastructure exit code `2`, and the final
required check uses that resolved value.

CraneTestKit exit codes remain authoritative:

- `0`: complete coverage and all tests passed;
- `1`: complete coverage with test failures or errors;
- `2`: infrastructure error or incomplete coverage.

The privileged job keeps YAML job ID `test` and display name `system-test`.
The always-running hosted `final-test` job still enforces complete authorization
and system-test success for master/manual runs.

`workflow_run` is associated with master, so it cannot by itself satisfy a PR's
required check. After resolving a valid source gate, the hosted preparation
step creates a `test` check on the exact PR head with status `in_progress`.
The hosted final job completes that check as successful only if both hosted
authorization (including routing validation) and `system-test` succeeded.
Skipped, failed, missing or cancelled jobs never become green. Checks are
bound to the trusted run ID and attempt, and the reporter verifies their check
ID, head, name and external ID before updating them. GitHub Actions rewrites
`details_url`, so that display field is not used to verify check identity. The
check summary links directly to the trusted workflow attempt. Only the hosted
jobs receive `checks: write`; the repository runner keeps `contents: read`.
Do not remove or rename the existing required `test` check during cutover.

Merge the gate and trusted dispatcher together. PR events before this change
reaches master use the previous workflow definition and cannot validate the
new dispatch path. After merge, reopen or update a suitable PR (or rerun a
native gate run created after cutover) and verify approval, dispatch, and the
exact-head `test` result. Repository/organization approval policy and branch
protection must be checked with an account that can read those settings;
HTTP 403 from a limited PAT does not prove the configured policy.

If two consecutive production runs return infrastructure exit code `2`,
disable this workflow and restore the previous serial workflow from Git
history. Exit code `1` is a product/test failure and does not trigger rollback.
