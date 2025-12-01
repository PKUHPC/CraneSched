# Prolog/Epilog Configuration Guide

Crane supports multiple `prolog` and `epilog` programs. Note that for security reasons, these programs do **not** have a search path set. You must either specify fully qualified paths in the programs or set the `PATH` environment variable. The table below explains the prolog and epilog programs available during job allocation, including when and where they run.

| Parameter               | Location              | Invoked by | User                        | Execution Timing                                                                 |
|-------------------------|-----------------------|-------------|------------------------------|----------------------------------------------------------------------------------|
| Prolog (config.yaml)    | Compute node          | craned      | CranedUser (usually root)    | When a job or job step first starts on the node (default); `PrologFlags=Alloc` forces execution at allocation time |
| PrologCtld (config.yaml)| Controller node       | cranectld   | CranectldUser                | At job allocation                                                                 |
| Epilog (config.yaml)    | Compute node          | cranectld   | CranedUser (usually root)    | At job completion                                                                 |
| EpilogCtld (config.yaml)| Controller node       | cranectld   | CranectldUser                | At job completion                                                                 |

The table below describes the prolog and epilog programs available during job step execution, including when and where they run.

| Parameter                                     | Location        | Invoked by  | User                        | Execution Timing            |
|-----------------------------------------------|-----------------|-------------|-----------------------------|-----------------------------|
| CrunProlog (config.yaml or `crun --prolog`)   | crun launch node| crun        | User running crun           | Before job step launch      |
| TaskProlog (config.yaml)                      | Compute node    | cranestepd  | User running crun           | Before job step launch      |
| `crun --task-prolog`                          | Compute node    | cranestepd  | User running crun           | Before job step launch      |
| TaskEpilog (config.yaml)                      | Compute node    | cranestepd  | User running crun           | When job step completes     |
| `crun --task-epilog`                          | Compute node    | cranestepd  | User running crun           | When job step completes     |
| CrunEpilog (config.yaml or `crun --epilog`)   | crun launch node| crun        | User running crun           | When job step completes     |

By default, the `Prolog` script only runs on a node when it receives its first job step from a new allocation. It does not run at the moment the allocation is granted. If no job step from an allocation ever runs on a node, that node will not run the `Prolog` for that allocation. This behavior can be changed with the `PrologFlags` parameter.  
`Epilog` always runs on each node when the allocation is released.

If multiple `prolog` or `epilog` scripts are specified (e.g., `/etc/crane/prolog.d/*`), they will run in **reverse alphabetical order** (z→a → Z→A → 9→0).

Prolog and Epilog scripts should be short and must **not** call Crane commands such as `cqueue`, `ccontrol`, `cacctmgr`. Long-running scripts slow down scheduling. Calling Crane commands may also cause performance issues.

`TaskProlog` runs with the same environment as the user’s task. Its standard output is interpreted as:

- `export name=value` : set environment variable
- `unset name` : unset environment variable
- `print ...` : write to task stdout

Example `TaskProlog`:

```bash
#!/bin/bash
echo "export VARIABLE_1=HelloWorld"
echo "unset MANPATH"
echo "print This message has been printed with TaskProlog"
```

# Failure Handling

- **If a Prolog fails (non-zero exit)** → the node is set to **DRAIN** and the job is **requeued**.
- **If an Epilog fails** → the node is set to **DRAIN**.
- **If PrologCraneCtld fails** → the job is **requeued**. Interactive jobs (`calloc`, `crun`) are **canceled**.
- **If EpilogCraneCtld fails** → a **log is written**.
- **If task prolog fails** → the **task is canceled**.
- **If crun prolog fails** → the **step is canceled**.
- **If task epilog or crun epilog fails** → a **log is written**.

---

# Prolog/Epilog Configuration

```yaml
JobLogHook:
  Prolog: prolog1.sh,prolog2.sh
  PrologTimeout: 60
  # PrologFlags: Alloc  # Alloc, Contain, NoHold, RunInJob, Serial
  Epilog: epilog1.sh,epilog2.sh
  EpilogTimeout: 60
  PrologEpilogTimeout: 120
  PrologCranectld: prologctld.sh
  EpilogCranectld: epilogctld.sh
  CrunProlog: srun_prolog.sh
  CrunEpilog: srun_epilog.sh
  TaskProlog: task_prolog.sh
  TaskEpilog: task_epilog.sh
```

---

# Prolog Flags

### **Alloc**
Runs Prolog at allocation time. Increases startup time. Required on some Cray systems.

### **Contain**
Runs Prolog inside job cgroup at allocation time.  
Implies **Alloc**.

### **NoHold**
Must be used with **Alloc**.  
Allows `calloc` to proceed without waiting for all Prologs.  
Faster with `crun`.  
Incompatible with **Contain** and **X11**.

### **ForceRequeueOnFail**
Requeue batch jobs that fail due to Prolog errors, even if not requested.  
Implies **Alloc**.

### **RunInJob**
Runs Prolog/Epilog inside extern cranestepd, included in job cgroup.  
Implies **Contain** and **Alloc**.

### **Serial**
Runs Prolog/Epilog serially per node.  
Reduces throughput.  
Incompatible with **RunInJob**.
