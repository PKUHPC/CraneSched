# Build, Test and Publish

`ci.yaml` runs manually through `workflow_dispatch`. It checks formatting and native tests,
builds RPMs and test assets, then runs full integration tests and optional package publication
in parallel. The final job reports the combined result. Automatic triggers remain disabled.

## Inputs and outputs

- `source_ref`: product revision; blank uses the selected workflow revision.
- `harness_ref`: optional test-suite revision; blank uses the matching branch, then master.
- `integration_shards`: number of independent integration environments.
- `publish_packages`: enable RPM publication; default is false.

Build returns `rpm_dir`, `harness_sha`, `asset_dir` and `test_image` for downstream jobs.
Integration reports completion and expected/passed counts.

## Configuration

The configured runner pools use the pinned `CRANE_CI_DEV_IMAGE`; `CRANE_CI_TEST_IMAGE`
selects the Test push target. `CRANE_CI_GOPROXY` optionally overrides the Go module proxy.
The optional `HTTPS_PROXY` secret is passed by the workflow for CMake dependency downloads;
compilation and native tests run without HTTP proxy variables. Publication requires
`CCREPO_URL`, `CCREPO_USERNAME` and `CCREPO_PASSWORD`. Deployment configuration is maintained separately.

## Script entrypoints

| Script | Purpose |
| --- | --- |
| `check_format.py` | Check C++/proto formatting and produce a correction patch |
| `build_product.sh` | Check, build, test and package the selected product revision |
| `build_golden_tree.sh` | Produce a checked C++ build baseline |
| `upload_packages.sh` | Upload this build's final RPMs |
| `summarize_ci.py` | Report required job results and integration completeness |

CI helper filenames use underscores. Check shell changes with `bash -n` and workflow
changes with a compatible `actionlint` version.
