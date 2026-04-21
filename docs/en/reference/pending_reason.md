# Job Pending Reasons

## Overview

When a job is in PENDING (queued) state, the system displays the reason why the job cannot run immediately. You can view the pending reason using `cqueue` or `ccontrol show job` commands to understand why the job is waiting.

## Viewing Pending Reasons

### Using cqueue

```bash
cqueue
```

Example output:
```
JOBID    PARTITION  NAME     USER   ST   TIME     NODES  NODELIST(REASON)
101      CPU        job1     user1  PD   0:00     2      (Priority)
102      CPU        job2     user1  PD   0:00     4      (Resource)
103      GPU        job3     user2  PD   0:00     1      (Dependency)
104      CPU        job4     user1  PD   0:00     2      (Held)
```

### Using ccontrol show job

```bash
ccontrol show job 101
```

Example output:
```
JobId=101
...
State=PENDING
Reason=Priority
```

## Pending Reason Descriptions

Pending reasons are listed in judgment order from top to bottom. If a job satisfies multiple conditions simultaneously, the reason that appears first will be displayed.

| Reason | Description | When It Appears |
|--------|-------------|-----------------|
| `Held` | Job is held | Job was submitted in held state or set to held, requires manual release |
| `BeginTime` | Start time not reached | Job has a delayed start time (`--begin` parameter), waiting for specified time |
| `DependencyNeverSatisfied` | Dependency can never be satisfied | Required dependent job to succeed, but it actually failed, dependency conditions cannot be met |
| `Dependency` | Waiting for dependency | Job dependencies have not been satisfied (dependent jobs not completed, not started, etc.) |
| `Resource changed` | Resource configuration changed | Node resources changed during job scheduling, waiting for rescheduling |
| `Reservation deleted` | Reservation was deleted | Reservation originally allocated to the job has been deleted |
| `Reservation changed` | Reservation was changed | Reservation changed during scheduling, waiting for rescheduling |
| `License` | Insufficient licenses | Currently insufficient license resources requested by the job |
| `Resource` | Insufficient resources | Cluster does not have enough resources (CPU, memory, GPU, etc.) to satisfy job requirements |
| `Resource Reserved` | Resources are reserved | Resources needed by the job are reserved by other reservations in future time periods |
| `Priority` | Insufficient priority | Job priority is lower than other queued jobs, or concurrent job limit reached |

## Resource Limit Pending Reasons

When a job cannot be scheduled due to QoS or Partition resource limits, the following pending reasons will be displayed. These reasons appear during the scheduling phase (job has been submitted but is waiting to run).

### QoS Resource Limits

| Reason | Description | Corresponding Limit |
|--------|-------------|---------------------|
| `QosEntryNotFound` | QoS statistics entry not found (internal error) | QoS statistics state anomaly during scheduling, contact administrator |
| `QosCpuResourceLimit` | CPU usage exceeds QoS user CPU limit | QoS `max_cpus_per_user` exceeded |
| `QosJobsResourceLimit` | Running job count exceeds QoS limit | QoS `max_jobs_per_user` or `max_jobs_per_account` exceeded |
| `QosWallTimeLimit` | Cumulative wall time exceeds QoS limit | QoS `max_wall` exceeded |
| `QosCpuResourceLimit` | CPU usage exceeds QoS TRES limit | CPU exceeded in QoS `max_tres_per_user` or `max_tres_per_account` |
| `QosMemResourceLimit` | Memory usage exceeds QoS TRES limit | Memory exceeded in QoS `max_tres_per_user` or `max_tres_per_account` |
| `QosGresResourceLimit` | GRES usage exceeds QoS TRES limit | GRES exceeded in QoS `max_tres_per_user` or `max_tres_per_account` |

### Partition Resource Limits

| Reason | Description | Corresponding Limit |
|--------|-------------|---------------------|
| `PartitionCpuResourceLimit` | CPU usage exceeds Partition TRES limit | CPU exceeded in Partition `max_tres` |
| `PartitionMemResourceLimit` | Memory usage exceeds Partition TRES limit | Memory exceeded in Partition `max_tres` |
| `PartitionGresResourceLimit` | GRES usage exceeds Partition TRES limit | GRES exceeded in Partition `max_tres` |
| `UserPartitionJobsLimit` | User's running job count in this Partition exceeded | Partition `max_jobs` exceeded (user dimension) |
| `AccPartitionJobsLimit` | Account's running job count in this Partition exceeded | Partition `max_jobs` exceeded (account dimension) |
| `UserPartitionWallTimeLimit` | User's cumulative wall time in this Partition exceeded | Partition `max_wall` exceeded (user dimension) |
| `AccPartitionWallTimeLimit` | Account's cumulative wall time in this Partition exceeded | Partition `max_wall` exceeded (account dimension) |
| `PartitionResourceLimit` | Partition statistics entry not found (internal error) | Partition statistics state anomaly during scheduling, contact administrator |

### Internal State Errors

| Reason | Description |
|--------|-------------|
| `UserMetaNotFound` | User statistics entry not found (internal error), contact administrator |
| `AccountMetaNotFound` | Account statistics entry not found (internal error), contact administrator |
| `QosMetaNotFound` | QoS statistics entry not found (internal error), contact administrator |
