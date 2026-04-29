# cattach - Attach to a Job Step

cattach attaches the current terminal to a running interactive job step, providing real-time forwarding of standard input and output. User input is forwarded to the tasks of the job step, and task output is forwarded back to the user terminal. cattach must be run on a node where cfored is running.

## Usage

```
cattach [flags] <jobid>.<stepid>
```

`<jobid>.<stepid>` is the identifier of the target job step. Use `cqueue --step` to query running job steps.

## Options

**-h, --help**

:   Display help information for cattach.

**-v, --version**

:   Display cattach version information.

**-C, --config=&lt;path&gt;**

:   Path to configuration file. Default: `/etc/crane/config.yaml`.

**--output-filter=&lt;task_id&gt;**

:   Show stdout only from the specified task (0-based task id). Default: `-1` (show output from all tasks).

**--error-filter=&lt;task_id&gt;**

:   Show stderr only from the specified task (0-based task id). Default: `-1` (show stderr from all tasks).

**--input-filter=&lt;task_id&gt;**

:   Send stdin to the specified task only (0-based task id). Default: `-1` (broadcast to all tasks).

**--label**

:   Prepend task number to each line of stdout and stderr, in the format `<task_id>: <line>`.

**--layout**

:   Print task layout information and exit without attaching to tasks.

**--quiet**

:   Quiet mode; suppress informational messages.

## Usage Examples

### Basic Usage

Query running job steps:

```bash
$ cqueue --step -j 42
```

Attach to step 0 of job 42:

```bash
$ cattach 42.0
Task io forward ready, waiting input.
hello
hello from task 0
```

Press `Ctrl+C` to disconnect cattach. The job step itself continues running.

### Display Task Layout

Print the node and task distribution of a job step and exit without attaching:

```bash
$ cattach --layout 42.0
Job step layout:
        1 tasks, 1 nodes (crane01)
```

### I/O Filtering for Multi-Task Steps

For job steps with multiple tasks, filter output from a specific task or route stdin to a specific task:

```bash
# Show stdout from task 0 only
$ cattach --output-filter 0 42.0

# Show stdout from task 1 only
$ cattach --output-filter 1 42.0

# Send stdin to task 0 only
$ cattach --input-filter 0 42.0

# Filter output and route input simultaneously
$ cattach --output-filter 0 --input-filter 0 42.0
```

### Labeled Output

Prepend task numbers to each output line, useful for debugging and monitoring multi-task steps:

```bash
$ cattach --label 42.0
0: Hello from task 0
1: Hello from task 1
0: Done
```

### Quiet Mode

Attach without printing informational messages (e.g., "Task io forward ready, waiting input."):

```bash
$ cattach --quiet 42.0
```

## Notes

- cattach must be executed on a node where cfored is running.
- **PTY mode** is determined automatically from the job step's configuration and does not need to be specified manually. If the target job step was started with `crun --pty`, cattach will automatically run in PTY mode.
- **Read-only mode**: When the target job step was started with `crun --input=<task_id>` (i.e., stdin is exclusively routed to a specific task), cattach automatically enters read-only mode — task output is displayed but no terminal input is forwarded.
- In non-PTY mode, pressing `Ctrl+C` disconnects cattach without terminating the job step.
- The `--error-filter` option is currently a no-op because stderr messages do not carry a task ID at the protocol level. Per-task stderr filtering will be supported in a future release.

## Related Commands

- [crun](crun.md) - Submit interactive tasks
- [calloc](calloc.md) - Allocate resources for interactive use
- [cqueue](cqueue.md) - View job queue and steps
- [ccontrol](ccontrol.md) - Control and query jobs/steps
- [ccancel](ccancel.md) - Cancel jobs and steps
