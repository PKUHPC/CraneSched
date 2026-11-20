# Supervisor OOM regression tests

The supervisor records OOM independently of the main process's exit code. For
example, a batch shell can print `python_rc=137` after its child is OOM-killed and
then exit successfully. Its local step result must be `OutOfMemory` with the
shell's actual exit code, zero. No synthetic signal or exit code is introduced.

Within one supervisor, an OOM result survives later ordinary task success or
failure. Explicit cancellation, time limit, deadline, and Cfored disconnect
retain precedence. This does not change the controller's existing policy for
combining results from different execution nodes.

Terminal dependencies use both status and exit code: only `Completed` with exit
code zero satisfies `afterok`. An `OutOfMemory` result with exit code zero
satisfies `afternotok` and `afterany`. Live completion and recovery from archived
job results use the same rule.

## Detection and scope

| Interface | Evidence retained before cgroup removal |
| --- | --- |
| cgroup v2 | Positive `oom_kill` delta in the step's hierarchical `memory.events` |
| cgroup v1 with `oom_kill` | Positive delta in either the step or a task's local `memory.oom_control` |
| cgroup v1 without `oom_kill` | An eventfd notification registered through `cgroup.event_control` before workload launch |

The interface is probed; a distribution name or kernel version does not select
the implementation. A missing counter is distinct from a zero count, malformed
contents, and an I/O error. `memory.failcnt`, transient `under_oom`, an ordinary
exit code of 137, or SIGKILL alone are not OOM evidence.

Old v1 event notifications indicate an OOM condition, not an exact kill count.
An OOM at a parent limit can notify sibling steps. The monitor registers a job
watch before registering workload watches and retains ancestor notifications:

- A workload notification without an ancestor notification is attributed to the
  step. A missing or unreadable ancestor watch prevents this attribution.
- An ancestor notification records that the victim step is unknown. A batch
  primary reports `OutOfMemory` for its allocation; a common step retains its
  ordinary result with the ancestor reason instead of claiming a local kill.
- Once an ancestor event has occurred, later event-only notifications remain
  ambiguous for that monitor. This conservative behavior can miss attribution
  to an individual victim step on old kernels.

Task watches are sampled and closed before deleting their cgroups, and the step
watch is closed before deleting the step cgroup. A cgroup-removal notification
must not become OOM evidence. Eventfd retains events until read, so exit-time
classification does not require a background polling thread. This change does
not proactively kill the whole step when an OOM notification arrives.

## Unit tests

With `CRANE_ENABLE_TESTS=ON`, build `oom_monitor_test`, `task_manager_test`, and
`ctld_public_defs_test`.
From the repository root, substituting the private build directory as needed:

```sh
build-shared/test/Craned/oom_monitor_test
build-shared/test/Craned/task_manager_test \
  '--gtest_filter=OomTerminationTest.*'
build-shared/test/CraneCtld/ctld_public_defs_test \
  '--gtest_filter=CtldOomDependencyTest.*:StepLifecycleTest.ZeroExitOomSurvivesPrimaryAndDaemonCompletion'
```

These tests use temporary cgroup-like files and real eventfd descriptors with
controlled notification ordering. They cover capability detection, counter
deltas, task-cgroup removal, parent-event attribution, shell exit zero, real
exit-code preservation, and final-result precedence. They do not validate an
old kernel's event registration implementation.

The controller tests exercise terminal dependency evaluation and pass an OOM
result with exit code zero through primary-step and daemon-step completion to
the final job result. They use the production state machine and a temporary
embedded database; they do not run the scheduler service or recovery from a
MongoDB instance.

## Opt-in kernel tests

`OomCgroupIntegrationTest` skips unless `CRANE_TEST_MEMORY_CGROUP_ROOT` is set.
Supply a writable, delegated memory-controller directory. For v2, the supplied
directory must already have memory enabled in `cgroup.subtree_control`. Root
privileges with permission to lower `oom_score_adj`, Bash, Python 3, and GNU
`timeout` are needed.

```sh
CRANE_TEST_MEMORY_CGROUP_ROOT=/sys/fs/cgroup \
  build-shared/test/Craned/task_manager_test \
  '--gtest_filter=OomCgroupIntegrationTest.*'
```

For a CentOS 7 memory-controller mount, use
`CRANE_TEST_MEMORY_CGROUP_ROOT=/sys/fs/cgroup/memory` instead, or an appropriately
delegated child directory. The supplied directory is not reconfigured. The
tests create and remove only a fresh `crane-oom-test-*` subtree.

The four cases independently enforce a 32 MiB limit at the task, step, or job
level, and compare these with a manual SIGKILL control. A 256 MiB ancestor cap
bounds the whole test. The protected shell survives a Python child allocating
128 MiB, prints `python_rc=137`, and exits zero. The production wait-status
handler, OOM monitor, and local result aggregation must report `OutOfMemory`
with code zero for the three OOM cases, and `Completed` for manual SIGKILL.

These tests exercise the host kernel and supervisor result handling; they do
not start Crane services or verify RPC delivery, controller aggregation, or
accounting. Passing on a v2 host does not establish CentOS 7.4 / Linux 3.10
compatibility. Run them on the target v1 kernel and retain `uname -r` and the
actual `memory.oom_control` fields with the test report.
