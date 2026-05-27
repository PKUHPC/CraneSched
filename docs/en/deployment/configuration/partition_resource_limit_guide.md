# Resource Limit Configuration Guide

CraneSched supports configuring resource usage limits for accounts and users at two levels:

- **QoS level**: Limits are bound to the QoS itself and apply to all jobs using that QoS, suitable for tiered resource quota management by service quality.
- **Partition level**: Limits apply to a specific partition and can be set separately for accounts and users, suitable for isolating resource quotas per partition.

Both mechanisms are checked as **peer-level dimensions** for each entity (user/account): the system first checks QoS-dimension limits, then partition-dimension limits. Both must pass before a job is allowed to submit or be scheduled. Additionally, partition limits only take effect when the QoS does not already cover the corresponding dimension (e.g., if QoS has set `MaxJobs`, the partition's `MaxJobs` is not checked again to avoid double enforcement).

## Prerequisites

- CraneSched cluster is running normally
- The operating user has Admin or Operator privileges
- Target accounts/users have been created via `cacctmgr`

---

## 1. QoS Resource Limits

QoS (Quality of Service) resource limits apply globally, regardless of partition. Configure them with `cacctmgr add qos` or `cacctmgr modify qos`.

### 1.1 QoS Limit Fields

| Field | Description | Scope |
|-------|-------------|-------|
| `MaxJobsPerUser` | Maximum concurrent running jobs per user | Per user |
| `MaxSubmitJobsPerUser` | Maximum submitted (including queued) jobs per user | Per user |
| `MaxCpusPerUser` | Maximum CPU usage per user | Per user |
| `MaxTresPerUser` | Maximum TRES usage per user | Per user |
| `MaxJobsPerAccount` | Maximum concurrent running jobs per account | Per account |
| `MaxSubmitJobsPerAccount` | Maximum submitted jobs per account | Per account |
| `MaxTresPerAccount` | Maximum TRES usage per account | Per account |
| `MaxJobs` | Global maximum concurrent running jobs for this QoS | QoS global |
| `MaxSubmitJobs` | Global maximum submitted jobs for this QoS | QoS global |
| `MaxTres` | Global maximum TRES usage for this QoS | QoS global |
| `MaxWall` | Global cumulative wall-clock time limit for this QoS (seconds) | QoS global |
| `MaxTimeLimitPerJob` | Maximum run time per job | Per job |
| `Priority` | QoS priority (higher value = higher priority) | — |
| `Flags` | QoS flags (`DenyOnLimit` or `None`) | — |

### 1.2 Create QoS

**Syntax:**

```bash
cacctmgr add qos <name> [options]
```

**Examples:**

Create a standard QoS:

```bash
cacctmgr add qos normal Description="Standard QoS" Priority=1000 \
  MaxJobsPerUser=10 MaxCpusPerUser=100
```

Create a high-priority QoS with a 24-hour per-job time limit:

```bash
cacctmgr add qos high Description="High Priority" Priority=5000 \
  MaxJobsPerUser=20 MaxCpusPerUser=200 MaxTimeLimitPerJob=86400
```

Create a QoS with TRES limits:

```bash
cacctmgr add qos gpu_qos Description="GPU Queue" Priority=2000 \
  MaxTresPerUser=cpu:64,mem:128G \
  MaxTresPerAccount=cpu:256,mem:512G \
  MaxTimeLimitPerJob=172800
```

### 1.3 Modify QoS

**Syntax:**

```bash
cacctmgr modify qos where Name=<qos_name> set <field>=<value>
```

**Modifiable fields:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `Description=<desc>` | string | Set description |
| `Priority=<num>` | uint32 | Set priority |
| `MaxCpusPerUser=<num>` | uint64 | Set max CPUs per user |
| `MaxJobsPerUser=<num>` | uint32 | Set max jobs per user |
| `MaxSubmitJobsPerUser=<num>` | uint32 | Set max submit jobs per user |
| `MaxTresPerUser=<tres>` | TRES string | Set max TRES per user |
| `MaxJobsPerAccount=<num>` | uint32 | Set max jobs per account |
| `MaxSubmitJobsPerAccount=<num>` | uint32 | Set max submit jobs per account |
| `MaxTresPerAccount=<tres>` | TRES string | Set max TRES per account |
| `MaxJobs=<num>` | uint32 | Set global max jobs for this QoS |
| `MaxSubmitJobs=<num>` | uint32 | Set global max submit jobs for this QoS |
| `MaxTres=<tres>` | TRES string | Set global max TRES for this QoS |
| `MaxWall=<sec>` | uint64 (seconds) | Set global cumulative wall-clock time limit |
| `MaxTimeLimitPerJob=<duration\|sec>` | duration or seconds | Set max run time per job |
| `Flags=<DenyOnLimit\|None>` | enum | Set QoS flags |

**Examples:**

Modify QoS priority:

```bash
cacctmgr modify qos where Name=normal set Priority=2000
```

Update per-user resource limits:

```bash
cacctmgr modify qos where Name=high set MaxJobsPerUser=50 MaxCpusPerUser=500
```

Set TRES limits:

```bash
cacctmgr modify qos where Name=gpu_qos set MaxTresPerUser=cpu:128,mem:256G
```

Set per-job time limit (supports duration format `days-hours:minutes:seconds` or seconds):

```bash
# Using seconds
cacctmgr modify qos where Name=normal set MaxTimeLimitPerJob=3600

# Using duration format (1 day, 2 hours, 30 minutes)
cacctmgr modify qos where Name=high set MaxTimeLimitPerJob=1-2:30:0
```

### 1.4 Show QoS

```bash
# Show all QoS
cacctmgr show qos

# Show a specific QoS
cacctmgr show qos normal

# Show with custom format
cacctmgr show qos format=name,MaxJobsPerUser,MaxCpusPerUser
```

### 1.5 Delete QoS

```bash
cacctmgr delete qos low
```

### 1.6 QoS Limit Enforcement

QoS limits are enforced at both the **submit stage** and the **scheduling stage**:

**Submit stage (immediate error):**

The following limits are checked at job submission. If exceeded, the job is rejected immediately:

| Error Code | Description | Corresponding Limit |
|------------|-------------|---------------------|
| `ERR_MAX_JOB_COUNT_PER_USER` | User submit job count exceeded | `MaxSubmitJobsPerUser` exceeded |
| `ERR_MAX_JOB_COUNT_PER_ACCOUNT` | Account submit job count exceeded | `MaxSubmitJobsPerAccount` exceeded |
| `ERR_QOS_JOB_COUNT_EXCEEDED` | QoS global submit job count exceeded | `MaxSubmitJobs` exceeded |
| `ERR_CPUS_PER_TASK_BEYOND` | Job CPU request exceeds QoS limit | `MaxCpusPerUser` exceeded |
| `ERR_TRES_PER_JOB_BEYOND` | Job TRES request exceeds QoS limit | `MaxTresPerUser`/`MaxTresPerAccount`/`MaxTres` exceeded |
| `ERR_TIME_TIMIT_BEYOND` | Job time limit exceeds QoS limit | `MaxTimeLimitPerJob` exceeded |

**Scheduling stage (job remains pending):**

The following limits are checked at scheduling time. If exceeded, the job stays in the pending queue with a corresponding reason:

| Pending Reason | Description | Corresponding Limit |
|----------------|-------------|---------------------|
| `QosCpuResourceLimit` | CPU usage exceeds QoS limit | `MaxCpusPerUser` or CPU in `MaxTresPerUser/Account` exceeded |
| `QosMemResourceLimit` | Memory usage exceeds QoS limit | Mem in `MaxTresPerUser` or `MaxTresPerAccount` exceeded |
| `QosGresResourceLimit` | GRES usage exceeds QoS limit | GRES in `MaxTresPerUser` or `MaxTresPerAccount` exceeded |
| `QosJobsResourceLimit` | Running job count exceeds QoS limit | `MaxJobsPerUser` or `MaxJobsPerAccount` exceeded |
| `QosWallTimeLimit` | Cumulative wall-clock time exceeds QoS limit | `MaxWall` exceeded |

---

## 2. Partition Resource Limits

Partition resource limits apply to a specific partition and can be set separately for accounts and users. Configure them with `cacctmgr modify account/user` by specifying `Partition=<partition_name>` in the `where` clause.

### 2.1 Partition Limit Fields

| Field | Description |
|-------|-------------|
| `MaxJobs` | Maximum concurrent running jobs for this account/user in the partition |
| `MaxSubmitJobs` | Maximum submitted (including queued) jobs for this account/user in the partition |
| `MaxTres` | Maximum total TRES usage for this account/user in the partition |
| `MaxTresPerJob` | Maximum TRES per job in the partition |
| `MaxWall` | Cumulative wall-clock time limit for this account/user in the partition (seconds) |
| `MaxWallPerJob` | Maximum wall-clock time per job in the partition (seconds) |

### 2.2 Set Partition Limits for an Account

**Syntax:**

```bash
cacctmgr modify account where Name=<account_name> Partition=<partition_name> set <field>=<value>
```

> **Note**: All partition resource limit fields **require** `Partition=<partition_name>` in the `where` clause. Omitting it will result in an error.

**Parameter reference:**

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `MaxJobs=<num>` | uint32 | Max concurrent running jobs | `10` |
| `MaxSubmitJobs=<num>` | uint32 | Max submitted jobs (including queued) | `50` |
| `MaxTres=<tres>` | TRES string | Max total TRES usage | `cpu:100,mem:200G` |
| `MaxTresPerJob=<tres>` | TRES string | Max TRES per job | `cpu:32,mem:64G` |
| `MaxWall=<sec>` | uint64 (seconds) | Cumulative wall-clock time limit | `86400` |
| `MaxWallPerJob=<sec>` | uint64 (seconds) | Max wall-clock time per job | `3600` |

**Examples:**

Set max running jobs to 20 and max submit jobs to 100 for account `PKU` in partition `GPU`:

```bash
cacctmgr modify account where Name=PKU Partition=GPU set MaxJobs=20 MaxSubmitJobs=100
```

Set max total TRES for account `PKU` in partition `CPU` (no more than 200 CPUs and 400G memory):

```bash
cacctmgr modify account where Name=PKU Partition=CPU set MaxTres=cpu:200,mem:400G
```

Set per-job TRES limit and wall-clock time for account `PKU` in partition `CPU`:

```bash
cacctmgr modify account where Name=PKU Partition=CPU \
  set MaxTresPerJob=cpu:32,mem:64G MaxWallPerJob=3600
```

Set multiple limits at once:

```bash
cacctmgr modify account where Name=PKU Partition=GPU \
  set MaxJobs=20 MaxSubmitJobs=100 MaxTresPerJob=cpu:8,mem:32G MaxWallPerJob=7200
```

### 2.3 Set Partition Limits for a User

**Syntax:**

```bash
cacctmgr modify user where Name=<username> [Account=<account_name>] Partition=<partition_name> set <field>=<value>
```

> **Note**: All partition resource limit fields **require** `Partition=<partition_name>` in the `where` clause. Omitting it will result in an error.

**Examples:**

Set max running jobs to 5 for user `alice` in partition `GPU`:

```bash
cacctmgr modify user where Name=alice Partition=GPU set MaxJobs=5
```

Set max submit jobs to 30 for user `alice` under account `PKU` in partition `CPU`:

```bash
cacctmgr modify user where Name=alice Account=PKU Partition=CPU set MaxSubmitJobs=30
```

Set per-job TRES limit and wall-clock time for user `alice` in partition `GPU`:

```bash
cacctmgr modify user where Name=alice Partition=GPU \
  set MaxTresPerJob=cpu:8,mem:32G MaxWallPerJob=7200
```

### 2.4 Show Partition Resource Limits

Use the `--partition-limit` (short: `-P`) global flag to display partition resource limit tables alongside `show account` or `show user` output.

Show partition limits for all accounts:

```bash
cacctmgr show account --partition-limit
```

Show partition limits for a specific account:

```bash
cacctmgr show account PKU -P
```

Sample output:

```
+------+-----------+---------+---------------+---------+---------------+-----------+--------------+
| NAME | PARTITION | MAXTRES | MAXTRESPERJOB | MAXJOBS | MAXSUBMITJOBS | MAXWALL   | MAXWALLPERJOB|
+------+-----------+---------+---------------+---------+---------------+-----------+--------------+
| PKU  | GPU       |         | cpu:8,mem:32G | 20      | 100           | unlimited | 02:00:00     |
| PKU  | CPU       | cpu:200 | cpu:32,mem:64G| unlimited| unlimited    | unlimited | 01:00:00     |
+------+-----------+---------+---------------+---------+---------------+-----------+--------------+
```

Show partition limits for a specific user:

```bash
cacctmgr show user alice -P
```

Sample output:

```
+-------+----------+------+-----------+---------+---------------+---------+---------------+-----------+--------------+
|ACCOUNT| USERNAME | UID  | PARTITION | MAXTRES | MAXTRESPERJOB | MAXJOBS | MAXSUBMITJOBS | MAXWALL   | MAXWALLPERJOB|
+-------+----------+------+-----------+---------+---------------+---------+---------------+-----------+--------------+
| PKU   | alice    | 1001 | GPU       |         | cpu:8,mem:32G | 5       | 30            | unlimited | 02:00:00     |
+-------+----------+------+-----------+---------+---------------+---------+---------------+-----------+--------------+
```

> **Note**: `unlimited` means no limit is configured for that field. Time fields are displayed in `HH:MM:SS` format.

### 2.5 Partition Limit Enforcement

Partition resource limits are enforced at both the submit stage and the scheduling stage:

**Submit stage (immediate error):**

The following limits are checked at job submission. If exceeded, the job is rejected immediately:

| Error Code | Description |
|------------|-------------|
| `ERR_PARTITION_TRES_PER_JOB_BEYOND` (106) | Job TRES exceeds partition `MaxTresPerJob` limit |
| `ERR_PARTITION_TIME_BEYOND` (107) | Job time limit exceeds partition `MaxWallPerJob` limit |
| `ERR_PARTITION_MAX_SUBMIT_JOBS_PER_USER` (108) | User submit job count in partition exceeds `MaxSubmitJobs` limit |
| `ERR_PARTITION_MAX_SUBMIT_JOBS_PER_ACCOUNT` (109) | Account submit job count in partition exceeds `MaxSubmitJobs` limit |

**Scheduling stage (job remains pending):**

The following limits are checked at scheduling time. If exceeded, the job stays in the pending queue with a corresponding reason:

| Pending Reason | Description |
|----------------|-------------|
| `UserPartitionJobsLimit` | User running job count in partition exceeds `MaxJobs` limit |
| `AccPartitionJobsLimit` | Account running job count in partition exceeds `MaxJobs` limit |
| `UserPartitionWallTimeLimit` | User cumulative wall-clock time in partition exceeds `MaxWall` limit |
| `AccPartitionWallTimeLimit` | Account cumulative wall-clock time in partition exceeds `MaxWall` limit |
| `PartitionCpuResourceLimit` | CPU usage exceeds CPU limit in partition `MaxTres` |
| `PartitionMemResourceLimit` | Memory usage exceeds memory limit in partition `MaxTres` |
| `PartitionGresResourceLimit` | GRES usage exceeds GRES limit in partition `MaxTres` |

---

## 3. Complete Configuration Example

The following example demonstrates a typical workflow using both QoS limits and partition limits:

```bash
# 1. Create a QoS policy with global resource limits
cacctmgr add qos gpu_qos Description="GPU Queue" Priority=2000 \
  MaxJobsPerUser=20 \
  MaxTresPerUser=cpu:64,mem:128G \
  MaxTimeLimitPerJob=172800

# 2. Create an account and associate it with the QoS
cacctmgr add account PKU Description="Peking University" Partition=CPU,GPU QosList=gpu_qos

# 3. Set partition-level limits for account PKU in partition GPU
cacctmgr modify account where Name=PKU Partition=GPU \
  set MaxJobs=50 \
  set MaxSubmitJobs=200 \
  set MaxTresPerJob=cpu:8,mem:32G \
  set MaxWallPerJob=7200

# 4. Set partition-level limits for account PKU in partition CPU
cacctmgr modify account where Name=PKU Partition=CPU \
  set MaxTres=cpu:200,mem:400G \
  set MaxTresPerJob=cpu:32,mem:64G \
  set MaxWallPerJob=3600

# 5. Set stricter personal limits for user alice in partition GPU
cacctmgr modify user where Name=alice Account=PKU Partition=GPU \
  set MaxJobs=5 \
  set MaxSubmitJobs=20 \
  set MaxWallPerJob=3600

# 6. Show QoS configuration
cacctmgr show qos gpu_qos

# 7. Show account partition limits
cacctmgr show account PKU -P

# 8. Show user partition limits
cacctmgr show user alice -P
```

---

## 4. Notes

1. **Peer-level enforcement**: For each user and account, the system checks QoS-dimension limits first, then partition-dimension limits. Both must pass. Partition limits only take effect when the QoS does not already cover the corresponding dimension: if QoS has set `MaxJobs`/`MaxWall`/`MaxTres`, the partition's corresponding fields are not checked again to avoid double enforcement.

2. **Partition must be specified in the where clause**: When setting partition resource limits, `Partition=<partition_name>` must be included in the `where` clause, otherwise the command will return an error.

3. **Account vs. user limits**: Account-level partition limits apply to the total usage of all users under that account. User-level partition limits apply only to that individual user. When both are configured, a job must satisfy both.

4. **TRES format**: TRES strings use the format `resource_type:amount`, with multiple resources separated by commas, e.g., `cpu:32,mem:64G`. Memory supports unit suffixes `K`, `M`, `G`, `T`. GRES format is `gres/type[:name]:num`, e.g., `gres/gpu:4`.

5. **Time units**:
   - QoS `MaxTimeLimitPerJob` supports duration format (`days-hours:minutes:seconds`, e.g., `1-2:30:0`) or seconds.
   - Partition `MaxWall` and `MaxWallPerJob` are in **seconds**, e.g., 1 hour = `3600`.

6. **Meaning of unlimited**: When a field is not configured, it displays as `unlimited`, meaning no constraint applies for that dimension.

## See Also

- [cacctmgr - Account Manager](../../command/cacctmgr.md)
- [Pending Reasons](../../reference/pending_reason.md)
- [Error Code Reference](../../reference/error_code.md)
