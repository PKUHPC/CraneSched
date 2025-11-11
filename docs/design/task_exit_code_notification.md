# Task Exit Code Notification for crun Jobs - Design Document

## Overview

This document describes the design and implementation of the task exit code notification mechanism for crun jobs in CraneSched. When any task in a crun job fails (exits with non-zero exit code), the supervisor will proactively notify crun of ALL task exit codes for detailed status synchronization.

## Background

### Current Behavior
Currently, when a crun job with multiple tasks completes, only the final job-level status is reported to the crun client. If individual tasks fail with different exit codes, the crun client doesn't receive detailed information about which tasks failed and with what exit codes.

### Requirements
1. Supervisor must monitor all task exit codes in crun jobs
2. When any task exits with non-zero exit code, supervisor must notify crun of ALL task exit codes
3. The notification must include task_id, exit_code, and whether the task was terminated by signal
4. The mechanism must be compatible with existing crun-cfored-supervisor communication

## Architecture

### Communication Flow
```
crun client (FrontEnd) 
    ↕ CrunStream
Cfored (FrontEnd)
    ↕ TaskIOStream  
Supervisor (BackEnd)
    ↓ monitors
Tasks (processes on compute nodes)
```

### Components Modified

#### 1. Protocol Buffers (protos/Crane.proto)

**Added to StreamTaskIORequest:**
- New request type: `TASK_EXIT_CODE_NOTIFICATION = 4`
- New message: `TaskExitCodeNotificationReq`
  - Contains repeated `TaskExitInfo` with:
    - `task_id`: uint32
    - `exit_code`: uint32
    - `is_terminated_by_signal`: bool

**Added to StreamCrunReply:**
- New reply type: `TASK_EXIT_CODE_NOTIFICATION = 7`
- New message: `TaskExitCodeNotification`
  - Contains repeated `TaskExitInfo` (same structure as above)

#### 2. Supervisor (src/Craned/Supervisor/)

**TaskManager.h - StepInstance class:**
- Added: `m_task_exit_codes_` - Map to store all task exit codes
- Added: `m_task_exit_codes_mtx_` - Mutex for thread-safe access
- Added methods:
  - `RecordTaskExitCode(task_id, exit_info)` - Store task exit code
  - `HasAnyTaskFailed()` - Check if any task has non-zero exit code
  - `GetAllTaskExitCodes()` - Retrieve all recorded exit codes

**TaskManager.cpp:**
- Modified `EvCleanSigchldQueueCb_()`:
  - Records exit code when task exits
  - Detects non-zero exit codes
  - Triggers notification with all exit codes

**CforedClient.h/cpp:**
- Added: `SendTaskExitCodeNotification(exit_codes)` method
  - Serializes task exit codes into protobuf message
  - Sends via TaskIOStream to Cfored

## Implementation Details

### Exit Code Recording
When a task process exits:
1. `HandleSigchld()` captures the exit status from waitpid()
2. Exit code is extracted (WEXITSTATUS or WTERMSIG)
3. For crun tasks, `RecordTaskExitCode()` stores the information
4. If exit code is non-zero, notification is triggered

### Notification Trigger
```cpp
if (m_step_.IsCrun()) {
  m_step_.RecordTaskExitCode(task_id, exit_info.value());
  
  if (exit_info->value != 0) {
    auto all_exit_codes = m_step_.GetAllTaskExitCodes();
    if (!all_exit_codes.empty()) {
      m_step_.GetCforedClient()->SendTaskExitCodeNotification(all_exit_codes);
    }
  }
}
```

### Message Serialization
The notification is sent as a delimited protobuf message via the existing TaskIOStream:
1. Create `StreamTaskIORequest` with type `TASK_EXIT_CODE_NOTIFICATION`
2. Populate `TaskExitCodeNotificationReq` with all task exit information
3. Serialize using `SerializeDelimitedToCodedStream()`
4. Forward to Cfored via `TaskOutPutForward()`

## Frontend Requirements

**Note:** The backend implementation in this repository is complete. The frontend (CraneSched-FrontEnd repository) needs to implement the corresponding handler to complete the feature.

### In Cfored (FrontEnd Repository)
1. Receive `StreamTaskIORequest` with `TASK_EXIT_CODE_NOTIFICATION` type
2. Extract task exit code information from the request
3. Create `StreamCrunReply` with `TASK_EXIT_CODE_NOTIFICATION` type
4. Forward to the connected crun client via CrunStream

### In crun Client (FrontEnd Repository)
1. Receive `StreamCrunReply` with `TASK_EXIT_CODE_NOTIFICATION` type
2. Parse task exit codes
3. Display detailed exit code information to user or handle appropriately

## Testing Strategy

### Unit Tests
- Test `RecordTaskExitCode()` with multiple tasks
- Test `HasAnyTaskFailed()` with various exit code combinations
- Test `GetAllTaskExitCodes()` returns correct information

### Integration Tests
1. **Single task failure**: Submit crun job with 1 task, verify exit code notification
2. **Multiple task success**: Submit crun job with multiple tasks, all succeed, no notification
3. **Mixed results**: Submit crun job with multiple tasks, some fail, verify all exit codes sent
4. **Signal termination**: Kill task with signal, verify is_terminated_by_signal flag

### Test Scenario Example
```bash
# Submit crun job with 4 tasks per node on 2 nodes
crun -N 2 --ntasks-per-node 4 -c 1 test_script.sh

# test_script.sh exits with different codes based on task_id
# Expected: When first task fails, crun receives notification with all 8 task exit codes
```

## Backward Compatibility

- The changes are backward compatible
- New message types are additions, not modifications
- Existing behavior is preserved when no tasks fail
- Frontend can ignore new message types if not yet implemented

## Security Considerations

- No sensitive data in exit codes
- Exit codes are already observable through job accounting
- No authentication/authorization changes needed
- Message serialization uses standard protobuf, no injection risks

## Performance Impact

- Minimal: Only triggered when tasks fail
- Memory: O(n) storage for n tasks' exit codes
- Network: One additional message per failing job
- No impact on task execution performance

## Documentation Updates Required

### User Documentation
1. Update `docs/en/command/crun.md` - Add section on exit code notification
2. Update `docs/zh/command/crun.md` - Chinese version
3. Update `docs/en/reference/exit_code.md` - Explain detailed exit code reporting

### Developer Documentation
1. Add this design document
2. Update API documentation with new protobuf messages
3. Add comments in code explaining the mechanism

## Example Documentation Addition

### For docs/en/command/crun.md:

```markdown
## Exit Code Reporting

When running multi-task crun jobs (with --ntasks-per-node > 1), if any task fails, crun will receive detailed exit code information for ALL tasks in the job. This helps diagnose which specific tasks failed and why.

Example output when tasks fail:
```
Task #0: exit code 0 (success)
Task #1: exit code 1 (failure)
Task #2: exit code 0 (success)  
Task #3: exit code 139 (terminated by signal 11 - SIGSEGV)
```
```

### For docs/en/reference/exit_code.md:

```markdown
## Task-Level Exit Code Reporting

For interactive crun jobs with multiple tasks, the system provides detailed exit code information when any task fails:

- **Task ID**: Identifies which specific task failed
- **Exit Code**: The actual exit code (0-255) or signal number
- **Signal Status**: Indicates if the task was terminated by a signal

This granular reporting helps users quickly identify problematic tasks in multi-task jobs and diagnose issues more effectively.
```

## Code Changes Summary

### Files Modified

1. **protos/Crane.proto**
   - Added `TASK_EXIT_CODE_NOTIFICATION` enum values
   - Added `TaskExitCodeNotification` and `TaskExitCodeNotificationReq` messages
   - Added `TaskExitInfo` nested messages

2. **src/Craned/Supervisor/TaskManager.h**
   - Added exit code tracking members to `StepInstance`
   - Added public methods for exit code management

3. **src/Craned/Supervisor/TaskManager.cpp**
   - Implemented exit code recording and retrieval methods
   - Modified SIGCHLD handler to trigger notifications

4. **src/Craned/Supervisor/CforedClient.h**
   - Added `SendTaskExitCodeNotification()` declaration

5. **src/Craned/Supervisor/CforedClient.cpp**
   - Implemented notification sending logic

### Lines of Code Changed
- Added: ~100 lines
- Modified: ~20 lines
- Total: ~120 lines

## Future Enhancements

1. **Configurable Notification**: Add option to enable/disable notifications
2. **Threshold-based Notification**: Only notify if failure rate exceeds threshold
3. **Aggregated Statistics**: Include min/max/avg exit codes in summary
4. **Persistent Logging**: Store detailed exit codes in job database

## References

- [Exit Code Documentation](../en/reference/exit_code.md)
- [crun Command Documentation](../en/command/crun.md)
- [Protobuf Language Guide](https://protobuf.dev/)
- [CraneSched Architecture](../../README.md)

## Version History

- v1.0 (2025-11) - Initial design and implementation
