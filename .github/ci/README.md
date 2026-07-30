# CraneTestKit privileged CI contract

`.github/workflows/build.yaml` is the trusted dispatcher for the K3s system
suite. Pull requests use `pull_request_target` only to authorize and resolve
immutable revisions on a GitHub-hosted runner. The hosted job executes only
trusted controls from `master`; after authorization, it checks out proposed-merge
workflow files for static inspection but never executes candidate code. A
privileged job is dispatched only when all of the following are true:

- the workflow was loaded from `master`;
- a same-repository pull request was triggered by a user whose current
  permission is `maintain` or `admin`; or
- a fork pull request author or current `maintain`/`admin` user recorded an
  exact-head `/request-ci` request and a current `maintain` or `admin` user
  reran that same workflow run;
- Backend and FrontEnd refs resolve to full commit SHAs.

The privileged job uses the dedicated repository-scoped runner selected by the
exact `cranesystemtest` label. Repository registration prevents other
repositories from dispatching to it, but GitHub does not restrict which
workflow in `PKUHPC/CraneSched` may request a repository runner. This is an
accepted boundary of the deployment.

## Fork pull request approval

The first `pull_request_target` run for a fork always fails in the hosted
authorization job and never queues `cranesystemtest`. The fork author or a
current `maintain`/`admin` user can then comment exactly `/request-ci`. The
hosted-only `request-ci.yaml` workflow requires an open, non-draft fork PR
targeting `master`, verifies a non-author requester's current permission, binds
the request to the current head repository, ref and SHA, and verifies that
attempt 1 of the latest matching `build.yaml` run failed authorization without
assigning or starting `system-test`.

The bot reply records a canonical hidden attestation containing the PR number,
head SHA, original request comment ID and workflow run ID. A maintainer approves
that exact request by following the reply and selecting **Re-run all jobs**.
Every rerun checks the rerun actor's current permission and, for a request made
by someone other than the PR author, the requester's current permission. It
rereads the original request comment, resolves the current PR and proposed
merge, and rejects any run, head, base, comment, permission or merge drift. A
new head requires a new `/request-ci` comment. A maintainer may rerun the same
attested run again after an infrastructure failure; all checks execute again.

The request workflow has only hosted-runner access and read-only repository and
Actions permissions plus permission to create or update its issue comment. It
checks out only the exact default-branch SHA and never checks out or executes PR
code. Fork authors can request a run but cannot approve or dispatch the
self-hosted job themselves.

The hosted authorization job applies a compensating routing guard before it
dispatches `test`. For a pull request, it resolves the exact PR head SHA for
the build, then obtains GitHub's proposed merge commit only for this static
routing check. The merge commit must have exactly the event's base and head
commits as parents, in that order; a stale or conflicting snapshot fails
closed. GitHub can retain stale base and proposed-merge SHAs in a `reopened`
event payload after `master` advances, so those payload values are syntax
checked but are not authorization inputs. The trusted base is the
`pull_request_target` workflow SHA; the current PR API base and the proposed
merge's first parent must both equal it. The current API head and second parent
must equal the event head, while the API merge SHA and merge ref must agree.
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
The sanitized artifact manifest and summary retain the base, head, and routing
SHAs so reviewers can distinguish tested source from the inspected merge tree.

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
`pull_request_target` dispatcher and the restricted Kubernetes identity remain
part of the repository-level security boundary.

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

The privileged job keeps YAML job ID `test` but uses display name
`system-test`. A hosted `final-test` job uses display name `test`, always runs,
and succeeds only when authorization and the complete K3s system job both
succeed. This preserves the existing required check context `test` while making
an unapproved fork, failed authorization or skipped system job explicitly fail
instead of appearing as a skipped required check. Do not delete or rename that
context during cutover.

If two consecutive production runs return infrastructure exit code `2`,
disable this workflow and restore the previous serial workflow from Git
history. Exit code `1` is a product/test failure and does not trigger rollback.
