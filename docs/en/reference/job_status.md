# Job Status

CraneSched jobs have the following statuses:

| Status Name | Description |
| :--- | :--- |
| **Pending** | The job is waiting for resources to be allocated. |
| **Running** | The job has been allocated resources and is currently executing. |
| **Completed** | The job has finished execution successfully (exit code 0). |
| **Failed** | The job finished execution with a non-zero exit code. |
| **ExceedTimeLimit** | The job was terminated because it exceeded the requested time limit. |
| **Cancelled** | The job was cancelled by the user or an administrator. |
| **OutOfMemory** | The workload encountered an OOM condition. A batch shell can still exit with code 0 after a child is OOM-killed. |
| **Configuring** | The job resources are being configured (e.g. prolog script running). |
| **Starting** | The job resources are configured and ready. |
| **Completing** | The job is finishing up (e.g. epilog script running). |

Job status and exit code are separate. OOM detection preserves the main
process's actual exit code, including zero; it does not synthesize an exit code
of 137.

On older cgroup v1 kernels without an OOM-kill counter, notifications identify
an OOM condition rather than a particular victim. A job-level notification can
mark a batch primary as `OutOfMemory`; it does not identify every sibling step
as an OOM victim. OOM classification is skipped when the supervisor has recorded
an explicit cancellation, timeout, deadline, or Cfored disconnect; those task
results retain the existing aggregation behavior. Results from different
execution nodes continue to use the controller's existing aggregation policy.
