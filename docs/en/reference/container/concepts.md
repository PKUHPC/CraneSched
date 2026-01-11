# Core Concepts

This page introduces the object model and lifecycle of CraneSched Container Support. After reading, you will understand the relationship between container jobs and container steps, the roles of Pod and container metadata, and resource allocation and inheritance mechanisms.

## Basic Terminology

| Term | Description |
| --- | --- |
| Container Job | A job with `TaskType=Container`, creates a Pod at startup, hosts container and non-container steps |
| Container Step | A step executed within a container job, carries container metadata, corresponds to a container in the Pod |
| Pod Metadata | Job-level configuration defining Pod name, namespace options, port mappings, etc. |
| Container Metadata | Step-level configuration defining image, command, environment variables, mounts, etc. |
| CRI | Container Runtime Interface, CraneSched interacts with runtimes like containerd via CRI |
| CNI | Container Network Interface, CraneSched configures container networks via CNI plugins |

---

## Container Job

A **Container Job** is the resource allocation unit for CraneSched Container Support. After submitting a container job, the scheduler completes resource allocation, and the node creates and maintains the Pod until the job ends.

Container jobs have the following characteristics:

- **Resource Hosting**: Job-level requests for CPU, memory, GPU, etc.; subsequent steps run within this allocation.
- **Pod Lifecycle**: Pod is created when the job starts and destroyed when it ends. All container steps run within the Pod.
- **Mixed Steps**: Container jobs can include both container steps and non-container steps (e.g., batch scripts, interactive commands).

When creating a container job, you can use ccon to submit a container step as the Primary Step, or use cbatch --pod to submit a batch script as the Primary Step, then use ccon within the script to append container steps.

!!! note "Job Type Recognition"
    Container jobs display type as `Container`. Other job types do not allow calling ccon to submit container steps.

---

## Container Step

A **Container Step** is the execution unit within a container job, corresponding to a container in the Pod. Each container step carries independent container metadata and can specify different images, commands, and mount configurations.

Container steps are similar to interactive steps submitted by crun. If you specify multiple nodes during submission, a corresponding container step instance will be created on each node.

Container steps follow the general step types:

| Type | Step ID | Description |
| --- | --- | --- |
| Daemon Step | 0 | Daemon step, creates Pod and runs continuously |
| Primary Step | 1 | The first step produced by the job entry |
| Common Step | ≥2 | Appended steps, can be dynamically created during job execution |

!!! note "Role of Pod"
    - Pod is created during Daemon Step at job startup and destroyed when the job ends.
    - Pod provides unified network namespace and resource isolation environment for containers, without performing any actual computation tasks.
    - Users neither need to nor can directly operate Pod.

---

## Container-Related Metadata

CraneSched Container Support separates configuration into two layers:

```mermaid
flowchart LR
    subgraph Job Level
        PM[Pod Metadata]
    end
    subgraph Step Level
        CM1[Container Metadata 1]
        CM2[Container Metadata 2]
    end
    PM --> CM1
    PM --> CM2
```

### Pod Metadata

Pod Metadata is **job-level** configuration specified when submitting a container job, defining the Pod's overall runtime environment.

| Field | Description |
| --- | --- |
| `name` | Pod name, used to generate container hostname |
| `namespace` | Namespace options (network, PID, IPC, etc.) |
| `userns` | Whether to enable user namespace (Fake Root) |
| `run_as_user` / `run_as_group` | User/Group ID to run containers as |
| `ports` | Port mapping configuration |

### Container Metadata

Container Metadata is **step-level** configuration specified when submitting a container step, defining the container's specific runtime behavior.

| Field | Description |
| --- | --- |
| `image` | Container image and pull policy |
| `command` / `args` | Container startup command and arguments |
| `workdir` | Working directory inside the container |
| `env` | Environment variables |
| `mounts` | Directory mount mappings |
| `tty` / `stdin` | Terminal and stdin configuration |
| `detached` | Whether to run in background |

### Configuration Timing

| Entry Point | Pod Metadata | Container Metadata |
| --- | --- | --- |
| `cbatch --pod` | Specified at job submission | Not needed for Primary Step; specified when appending steps |
| `ccon run` (new job) | Specified at job submission | Specified at job submission |
| `ccon run` (append step) | Inherited from job | Specified at step submission |

---

## Resource Model

Container jobs follow a "job-level allocation, step-level inheritance" resource model, consistent with non-container jobs.

### Job-Level Allocation

When submitting a container job, request resources using these parameters:

- Node count (`-N`)
- CPU (`-c` / `--cpus-per-task`)
- Memory (`--mem`)
- GPU and other devices (`--gres`)
- Time limit (`-t`)

The scheduler allocates resources based on partition, account, QoS, and other policies.

### Step-Level Inheritance

When appending container steps, resource handling follows these rules:

| Scenario | Behavior |
| --- | --- |
| Resources not specified | Inherit job-level request |
| Resource subset specified | Use specified values, must not exceed job allocation |
| Node list specified | Must be within the job's allocated node set |

### Constraints

- Container step resource requests must not exceed job allocation (returns `ERR_STEP_RES_BEYOND`)
- Node selection must be within job allocation range (returns `ERR_NO_ENOUGH_NODE`)
- Container steps must maintain the same user identity as the job

---

## Lifecycle

The container job lifecycle includes the following phases:

```mermaid
stateDiagram-v2
    state "Failed" as FailedStartup
    state "Failed" as FailedRuntime

    [*] --> Pending: Submit
    Pending --> Configuring: Scheduled
    Configuring --> Starting: Pod startup
    Configuring --> FailedStartup: Pod startup failed
    Starting --> Running: Container startup
    Starting --> FailedStartup: Container startup failed
    Running --> Completing: Steps finished
    Completing --> Completed: Pod cleanup
    Running --> FailedRuntime: Execution failed
    Running --> Cancelled: User cancelled
    Running --> ETL: Time limit exceeded
    Completed --> [*]
    FailedStartup --> [*]
    FailedRuntime --> [*]
    Cancelled --> [*]
    ETL --> [*]
```

**Lifecycle Phase Description:**

1. **Pending**: Job enters queue awaiting scheduling.
2. **Configuring**: Scheduling complete, node is creating Pod and performing necessary configuration (network, mounts, namespaces, etc.).
3. **Starting**: Pod created, container runtime is pulling image and starting container; image pull may take some time.
4. **Running**: Resource allocation complete, container started and executing, Primary Step begins running.
5. **Completing**: All steps finished, awaiting Pod cleanup.
6. **Completed / Failed / Cancelled / ExceedTimeLimit**: Job terminal states.

---

## Runtime Interaction

Container steps support runtime interaction operations:

| Operation | Command | Description |
| --- | --- | --- |
| Attach | `ccon attach JOBID.STEPID` | Connect to container's stdin/stdout |
| Exec | `ccon exec JOBID.STEPID COMMAND` | Execute command inside container |
| Logs | `ccon logs JOBID.STEPID` | View container logs |

These operations are forwarded through CraneCtld to the Craned node running the container, which then interacts with the container runtime via CRI.

---

## Mixed Steps

Container jobs allow mixing different types of steps within the same job:

```mermaid
flowchart TD
    Job[Container Job] --> Pod[Pod]
    Pod --> PS[Primary Step: Batch Script]
    PS --> CS1[Container Step: Training]
    PS --> CS2[Container Step: Inference]
    PS --> NS[Non-container Step: Data Processing]
```

Use cases:

- Run core computation in containers while using host environment for pre/post-processing
- Complete containerized training and bare-metal debugging within the same resource allocation
- Use scripts to orchestrate the execution order of multiple container tasks

---

## Architecture Overview

Container Support is implemented through coordination between the scheduling control plane and node execution plane:

```mermaid
flowchart LR
    subgraph User Side
        CLI[ccon / cbatch]
    end
    subgraph Control Plane
        Ctld[CraneCtld]
    end
    subgraph Execution Plane
        Craned[Craned]
        CSuper[CSupervisor]
        CRI[CRI Runtime]
    end
    CLI -->|gRPC| Ctld
    Ctld -->|Task Dispatch| Craned
    Craned -->|Dispatch & Coordinate| CSuper
    CSuper -->|CRI Calls / cgroup Management| CRI
```

| Component | Responsibility |
| --- | --- |
| CraneCtld | Receives submission requests, schedules resources, validates permissions and parameters |
| Craned | Node-side agent, receives task dispatch and cooperates with CSupervisor, manages node-level resources and CSupervisor creation |
| CSupervisor | Monitoring and management component running on nodes, monitors lifecycle and cgroup of each Step, communicates with CRI to execute container operations |
| CRI Runtime | Container runtime (e.g., containerd), executes container operations upon CSupervisor's invocation |

---

## Related Documentation

- [Container Support Overview](index.md)
- [Quick Start](quickstart.md)
- [Examples](examples.md)
- [Troubleshooting](troubleshooting.md)
