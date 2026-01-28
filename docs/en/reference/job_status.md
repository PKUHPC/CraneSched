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
| **OutOfMemory** | The job was killed by the OOM killer because it used more memory than requested. |
| **Configuring** | The job resources are being configured (e.g. prolog script running). |
| **Starting** | The job resources are configured and ready. |
| **Completing** | The job is finishing up (e.g. epilog script running). |
