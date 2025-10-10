# Job Exit Code Reference

The `EXITCODE` column in the `cacct` command records the reason for a user's job termination.  
For example, in `0:15`, the **first code** (`0`) is the **primary code**, and the **second code** (`15`) is the **secondary code**.

---

## Primary Code

Value of 0-255 for Program `exit` return value

---

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
