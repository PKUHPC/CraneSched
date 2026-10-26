# Job Exit Code

The `EXITCODE` column in the `cacct` command records the reason for a user's job termination.  
For example, in `0:15`, the **first code** (`0`) is the **primary code**, and the **second code** (`15`) is the **secondary code**.

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

## Normal calloc Completion

Normally releasing a `calloc` allocation records the job as `Completed` with exit code `0:0` (raw JSON value `0`). The allocation session does not use an individual `crun` step's exit code as its result; query step results separately. Cancellation, timeout, and startup failure are not normal release.

## JSON Format Explanation

- Values **0–255** represent the **exit return value**.  
- Values **256–319** represent **program exit signals**.
- Values **greater than or equal to 320** represent **Crane-defined codes**; for example, `320` means terminated and is displayed as `0:64`.
