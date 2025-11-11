# Job Exit Code

The `EXITCODE` column in the `cacct` command records the reason for a user's job termination.  
For example, in `0:15`, the **first code** (`0`) is the **primary code**, and the **second code** (`15`) is the **secondary code**.

---

## Task-Level Exit Code Reporting (for crun Jobs)

For interactive crun jobs with multiple tasks (using `--ntasks-per-node > 1`), the system provides detailed exit code information when any task fails:

**Information Provided:**
- **Task ID**: Identifies which specific task failed
- **Exit Code**: The actual exit code (0-255) or signal number  
- **Signal Status**: Indicates if the task was terminated by a signal

**When Triggered:**
- Automatically triggered when any task in a crun job exits with non-zero exit code
- Reports exit codes for ALL tasks in the job, not just failed ones
- Helps users quickly identify problematic tasks in multi-task jobs

This granular reporting enables more effective debugging and diagnosis of multi-task job failures.

---

## Primary Code

Value of 0-255 for Program `exit` return value


## Secondary Code

Program exit signal:

- 0-63: Signal value

Crane-defined codes:

- 64: Terminated
- 65: Permission Denied
- 66: Cgroup Error
- 67: File Not Found
- 68: Spawn Process Failed
- 69: Exceeded Time Limit
- 70: Crane Daemon Down
- 71: Execution Error
- 72: RPC Failure

---

## JSON Format Explanation

- Values **0–255** represent the **exit return value**.  
- Values **256–320** represent **program exit signals**.  
- Values **above 320** represent **Crane-defined codes**.
