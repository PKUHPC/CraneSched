# Build, Test and Publish

`ci.yaml` runs all five jobs through repository-scoped ARC scale sets. Each job
has an explicit digest-pinned container image: Dev for C++ formatting, Base for
Go checks and product builds, and CI tools for downstream execution. The runner image receives work;
the job container provides the product toolchain or execution tools.

| Job | Runner | Responsibility |
| --- | --- | --- |
| `format` | `crane-ci` | Use the pinned Dev image's clang-format for C/C++ sources, tests and shared protocols with their separate style |
| `build` | `crane-build` | Check Go formatting and run vet, then C++/Go tests, build and package the same checked product commit |
| `integration` | `crane-ci` | Assemble a test image from those RPMs, discover and run every TestTC in the selected suite revision, collect internal evidence and reclaim environments |
| `publish` | `crane-ci` | Upload those RPMs immediately after a successful build, independently of integration |
| `summary` | `crane-ci` | Fail if formatting or any requested phase failed, was cancelled, skipped or incomplete |

The initial workflow supports manual dispatch. Automatic PR, push and scheduled
execution is enabled only after concurrency, failure handling and full-suite
acceptance. GitHub's repository permissions govern manual dispatch. The retired
root runner, external FrontEnd checkout, release validators and PR dispatcher
are no longer part of this workflow.

## Formatting and Go vet

Formatting is the first stage of this workflow; there is no separate formatting
workflow. The job runs in the existing Dev image and prints its packaged
`clang-format` version. The image digest fixes that toolchain; the job installs
no packages. It checks `src/`, `test/` and shared `.proto` files, and uploads
`format-diff` when corrections are needed. Its resolved product SHA is the input
to the build job, so the checked and built revisions cannot diverge.

`.clang-format` explicitly enables `DerivePointerAlignment` to retain the
existing per-file pointer/reference style. Google changed that default in
clang-format 21; upgrading the Dev formatter must not silently change the
repository's formatting policy.

Inside the persistent Base container, `gofmt -l go` reports unformatted source
without rewriting it. The workflow generates shared protocols with the existing
`make -C go protos` target, then runs `go vet -mod=readonly ./...` in the Go module,
including test packages with the default build tags. Both checks precede product
compilation and reuse the build job's caches. These commands also work when
`source_ref` selects a revision without new CI helper targets. They do not run
`go mod tidy`. A failure stops packaging and is reflected in the workflow summary. Like the rest of the pipeline, these checks currently run on
manual dispatch; automatic PR execution is deferred until the planned acceptance.

## Inputs and site configuration

`source_ref` optionally selects a product branch, tag or commit. An empty value
uses this workflow's commit. Checkout resolves it to a full SHA; C++ and Go share
that source and the same `ci-debug` target. `harness_ref` optionally selects the
integration suite's branch, tag or commit, defaulting to `master`. The site
entry point resolves it once and records the SHA. Missing inputs or a version
mismatch fail preparation instead of selecting an older cached suite.

`integration_shards` selects one or four independent environments. The
integration entrypoint discovers every top-level `TestTC...` function from the
suite revision selected by `harness_ref`; the site does not maintain a second
test list or a representative subset. Cases run serially within each
environment. Each case has a timeout, and the site enforces an overall
execution deadline. Panic, timeout or execution abort stops that environment
while the other shards finish; all results are collected before reclamation.
Automatic triggers remain disabled during acceptance.

`publish_packages` defaults to false. Enable it only when the existing `CCREPO_*`
secrets are configured. A requested upload with missing secrets or a failed
transfer fails the publish job and the summary without stopping integration.
The existing uploader retains its authentication, path and retry behavior;
this phase publishes only the five final RPMs to the `crane` repository, using
distro `10` and architecture `x86_64`. The package directory contains only the
final RPM files, so CPack's temporary staging tree and the retired `bin` path
cannot be uploaded accidentally. DEB publishing is deferred until a matching
build target and installation validation are added; `cranedeb` is not used by
this phase.
Publish copies only the five manifest-selected final packages into the current
attempt's upload directory.
The publish job serializes uploads to that shared package destination without
cancelling an in-progress publish; integration remains independent. Internal-only
package hostnames are maintained in the site's cluster DNS, such as CoreDNS
custom extensions. Job pods use that shared resolution instead of individual
host aliases. The operator terminal's `/etc/hosts` entries are not automatically
inherited by Kubernetes job pods.

Repository variables:

| Variable | Contract |
| --- | --- |
| `CRANE_CI_DEV_IMAGE` | Published Dev image with an immutable digest and the project's clang-format |
| `CRANE_CI_BASE_IMAGE` | Published RHEL 10/amd64 build image with an immutable `@sha256:` digest |
| `CRANE_CI_TOOLS_IMAGE` | Published execution-tools image with an immutable digest |
| `CRANE_CI_GOPROXY` | Optional Go module proxy override; defaults to `https://goproxy.cn,direct` |
| `CRANE_CI_INTEGRATION_ENTRYPOINT` | Versioned internal entry point under the read-only `/ci/assets/` mount |
| `CRANE_CI_INTEGRATION_SHA256` | SHA-256 of that prepared entry point |

The Kubernetes hook templates provide shared storage, resource limits, UID/GID
1001, registry pull authentication and the execution identity. API, registry and
source-read credentials stay in Kubernetes Secrets; they are not embedded in
workflow files. The build container has no Kubernetes API token.

## Persistent inputs and build outputs

`/ci/workspaces/native-rhel10-amd64/<base-digest>/ci-debug` retains the source,
complete CMake/Ninja tree and dependencies across source revisions. One flock
covers source update, generation, native tests, build and packaging. Checkout
uses an independent runner work volume; cleaning it never removes the hot
build tree. ccache is shared; Go caches are local to the build pod. Before rebuilding Go package
inputs, the locked CI step clears `go/build/bin`, `go/build/plugin` and
`go/build/tool`. This makes a removed command/plugin/tool fail packaging if its
package declaration still names the old output. Compiler and dependency caches
and the CMake/Ninja tree remain available.

Every run attempt owns `/ci/attempts/<run-id>-<attempt>/build`. It contains five
RPMs under `packages/rpm/` and a manifest with source SHA, Base digest, target,
preset, package identities and SHA-256 checksums. CPack works in a separate
`cpack-backend/` directory; only its two final RPMs are copied into that shared
package directory alongside the three Go RPMs. Actions passes the manifest checksum to both
consumers. Integration verifies it and assembles its image from the RPM payload;
it does not rebuild product binaries. Publishing verifies the same output.

For branch and pull request builds, the Go package target disables GoReleaser snapshot mode
and supplies the product `VERSION` as `GORELEASER_CURRENT_TAG`; it skips only the
tag validation that requires a release tag. Snapshot mode hardcodes package version
`0.0.0`, which does not identify the selected product source. GoReleaser receives the
workflow's read-only GitHub token only in its packaging process. The manifest gate checks
all five RPM versions against that source revision and `VERSION` before downstream use.
CI skips GoReleaser validation that requires a release tag and skips publishing; RPMs
remain local workflow outputs and are not uploaded as GitHub Releases.

## Integration entry-point contract

The site entry point receives `CI_ARTIFACT_DIR`, `CI_MANIFEST_SHA256`,
`CI_SOURCE_SHA` and `CI_TEST_REF`. It uses only the successful build attempt's
RPMs, records the resolved suite revision and actual asset inputs, and invokes
the existing builder without replacing or deleting it. Suite selection is
independent of the deployed platform version and never upgrades shared services.

All test processes run in the integration container. Each environment has its
own UID receipt and result directory. Tests are serial within an environment;
independent environments can use separate processes. The fixed repository-wide
integration concurrency group admits one integration job at a time. Retrying
requires a new attempt; missing UID receipts or unfinished cleanup block new
admission. Cancellation stops test processes before environment reclamation.

The entry point retains packages, complete logs, test identities and diagnostic
evidence internally. Public Actions receives case names, terminal results, aggregate counts and phase
outcomes. Success requires nonempty, complete execution and successful cleanup.
It writes `complete=true`, `expected=<count>` and `passed=<count>` to
`GITHUB_OUTPUT`; a failed or incomplete execution returns nonzero. The summary
only checks the declared job results and those aggregate outputs; it does not
wait for or reclaim remote resources.

## Validation

Run `actionlint` from the product repository and the retained uploader/format
regressions with `python3 -m unittest discover -s .github/ci -p 'test_*.py'`.
Validate the prepared runner, hooks, identities and storage with a real manual
workflow attempt before enabling automatic triggers. An online idle runner is
not evidence that a container job or product test has passed.
