# Build, Test and Publish

`ci.yaml` uses repository-scoped ARC scale sets with explicit container images.
Automatic triggers remain disabled until full workflow acceptance is complete.

| Job | Runner | Responsibility |
| --- | --- | --- |
| `build` | `crane-build` | In one pinned Dev image: freeze product/suite commits, check formatting and vet, build and run native tests, package RPMs, prepare full-suite assets and push Test |
| `integration` | `crane-ci` | Consume Test digest, prepared assets and frozen suite commit; create environments, run the full suite, collect evidence and reclaim |
| `publish` | `crane-ci` | Upload the build's RPM directory in parallel with integration |
| `summary` | `crane-ci` | Enforce successful build, complete integration and requested publication |

Dev inherits its matching Base toolchain and adds formatting, workload and CI
client tools. The same Dev digest runs build and is the parent of the Test image.
Integration's Actions container uses CI tools; product Pods in test environments
use Test. Integration does not compile attachments, unpack RPMs or push images.

## Source selection and checks

`source_ref` optionally selects a product branch, tag or commit; blank uses the
workflow commit. The checkout SHA is frozen before checks and used by the persistent
build tree. An explicit `harness_ref` selects the test suite ref. Otherwise the
launcher selects the suite branch matching the selected product branch, then
`master` only if that branch does not exist. For a PR, the product branch is the
head branch; for manual dispatch it is the selected workflow branch. An explicit
`source_ref` branch overrides it. A tag or detached commit has no branch to match.
Access errors and invalid explicit refs fail preparation rather than fall back.

Build freezes the suite once. Test cases, executor, CLI, fixtures, attachment
sources and Test recipe come from that commit. Downstream jobs do not resolve the
branch again, and the next build sees new commits without workflow edits.
Changes from the deployed Operator source revision produce a warning in the
build log/summary; selected cases still run against the existing Operator.
An unknown deployment revision also warns. Results involving Operator changes
require manual review and infrastructure testing; CI never upgrades shared services.

C++/protocol formatting runs in build using the Dev image's clang-format. It
checks `src/`, `test/` and shared `.proto` files, preserves their separate style
and uploads an applicable `format-diff` on failure. `.clang-format` explicitly sets
`DerivePointerAlignment` to preserve existing style with clang-format 21.
Go fmt reports unformatted files; protocol generation precedes `go vet -mod=readonly
./...`, including tests with default tags. These checks do not run `go mod tidy`.
Any check/native-test failure stops build and its downstream jobs.

## Outputs and integration

Build outputs are `rpm_dir`, `harness_sha`, `asset_dir` and
`test_image` (a repository digest). Job dependencies and outputs join stages;
there is no custom build manifest or general site-configuration file.

`integration_shards` selects one or four environments. The executor discovers every
top-level `TestTC...` in the frozen suite and checks the prepared binaries expose
the same inventory. It does not consume a separately maintained list or subset.
Cases run serially within a Run and concurrently across Runs. Panic, timeout or
abort stops that shard; other shards finish and all results are collected.
Actions reports case names, terminal results and counts; detailed assertions,
private source and diagnostic evidence stay in internal storage.

The shared integration concurrency group admits one job at a time. Kubernetes
labels record each resource's execution Pod UID, attempt and shard; API discovery
and UID-checked deletion drive cleanup. CI does not scan local CLI receipts to
find resources. Unfinished reclamation blocks new integration admission. Each
attempt writes independent results and preserves previous failures.
Integration writes `complete`, `expected`, and `passed` to `GITHUB_OUTPUT`;
summary requires a nonempty complete pass plus successful cleanup.

## Publication

`publish_packages` defaults to false. Required secrets are `CCREPO_URL`,
`CCREPO_USERNAME`, and `CCREPO_PASSWORD`. Missing credentials or upload failure
fails publish/summary while integration continues. The uploader uses the existing
`crane` RPM repository, distro `10`, `x86_64` and version-directory convention.
Only the final RPM directory is passed to it; CPack intermediate RPMs, DEBs and
the retired bin path are excluded. DEB support awaits a matching build target.
Uploads to the shared destination queue without cancelling an in-progress upload.
Internal package DNS is maintained by cluster DNS, without Pod host aliases.

## Scripts and images

| Script | Responsibility |
| --- | --- |
| `check_format.py` | Check C++/proto style and produce an applicable patch |
| `build-product.sh` | Hold the persistent tree lock through Go checks, native tests, builds, packaging and RPM validation |
| `prepare-go-package-inputs.sh` | Remove obsolete generated Go package inputs before rebuild |
| `collect-rpm-packages.sh` | Collect final CPack RPMs without intermediate files |
| `upload-packages.sh` | Upload the final RPM directory |
| `summarize_ci.py` | Enforce job results and integration completeness |

Python entrypoints use `snake_case`. Shell entrypoints use command-style hyphenated names.

| Repository variable | Purpose |
| --- | --- |
| `CRANE_CI_DEV_IMAGE` | Immutable Dev digest with the inherited toolchain, clang-format, Open MPI, kubectl and Docker CLI/Buildx |
| `CRANE_CI_TOOLS_IMAGE` | Immutable CI execution-tools digest for downstream jobs |
| `CRANE_CI_TEST_IMAGE` | Stable Test repository/tag to push; consumers use the returned digest |
| `CRANE_CI_GOPROXY` | Optional module proxy, default `https://goproxy.cn,direct` |

Hooks provide UID/GID 1001, shared storage, Pod identity, CA and narrowly scoped
API credentials. Only build mounts source-read and registry-push credentials.
The launcher is mounted at `/etc/ci-launcher/launch.py`; build calls
`--select-suite` then `--prepare`, and integration calls `--integration`.
Run templates and platform metadata are maintained through Kubernetes objects.
No private repository URL or source is stored in the product workflow.

## Persistent build state

`/ci/workspaces/native-rhel10-amd64/<dev-digest>/ci-debug` preserves the source and
complete CMake/Ninja tree across product commits. One flock covers source update,
generation, native tests, build and packaging. Runner checkout is independent;
cleaning it does not remove this tree. ccache is shared and Go caches are local
to the build Pod. Only `go/build/bin`, `plugin`, and `tool` are cleared before
rebuilding package inputs, preventing removed modules from reusing stale files.

Each attempt owns `/ci/attempts/<run-id>-<attempt>/build`. Final RPMs go under
`packages/rpm/`; CPack's temporary tree is elsewhere. Build validates five package
identities, architecture and product `VERSION` before test assembly/publication.
GoReleaser disables snapshot mode and skips publish/tag validation; the product
Makefile supplies the version. Its read-only GitHub token is scoped to that
packaging command. RPM payload extraction and Test push run in the same Dev job.
BuildKit handles layer caching; prepared test assets use compatible immutable
cache directories on shared storage.

## Validation

Run script regressions with `python3 -m unittest discover -s .github/ci -p
 'test_*.py'` and check workflow syntax with actionlint configured for ARC labels.
The pinned actionlint predates `concurrency.queue`; only that field is omitted
from a temporary lint copy. A real workflow attempt must still validate hooks,
identities, shared storage and the complete DAG before automatic triggers.
