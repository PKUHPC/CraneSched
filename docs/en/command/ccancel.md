# ccancel - Cancel Jobs

**ccancel terminates running or pending jobs or individual steps within jobs.**

You can cancel jobs by job ID, cancel individual steps using the `jobid.stepid` format, or use filter conditions to cancel multiple jobs at once.

## Command Syntax

```bash
ccancel [job_id[.step_id][,job_id[.step_id]...]] [OPTIONS]
```

## Command Line Options

### Job Selection
- **job_id[.step_id][,job_id[.step_id]...]**: Job ID(s) or step ID(s) to cancel (comma-separated list).
  - Format for jobs: `<job_id>` or `<job_id>,<job_id>,<job_id>...`
  - Format for steps: `<job_id>.<step_id>` or `<job_id>.<step_id>,<job_id>.<step_id>...`
  - Mixed format: `<job_id>,<job_id>.<step_id>,<job_id>.<step_id>...`

### Filter Options
- **-n, --name string**: Cancel jobs with the specified job name
- **-u, --user string**: Cancel jobs submitted by the specified user
- **-A, --account string**: Cancel jobs under the specified account
- **-p, --partition string**: Cancel jobs in the specified partition
- **-t, --state string**: Cancel jobs in the specified state. Valid states: `PENDING` (P), `RUNNING` (R), `ALL` (case-insensitive)
- **-w, --nodes strings**: Cancel jobs running on the specified nodes (comma-separated list)

### Output Options
- **--json**: Output in JSON format

### Miscellaneous
- **-C, --config string**: Configuration file path (default: `/etc/crane/config.yaml`)
- **-h, --help**: Display help information
- **-v, --version**: Display version number

## Filter Rules

!!! important
    At least one condition must be provided: either job ID(s) or at least one filter option.

When using multiple filters, jobs matching **all** specified conditions will be cancelled (AND logic).

## Usage Examples

### Cancel by Job ID

Cancel a single job:
```bash
ccancel 30686
```

**Result:**

```text
[cranetest@crane01 ~]$ cqueue
JOBID  PARTITION  NAME     USER        ACCOUNT    STATUS  TYPE   TIME     TIMELIMIT  NODES  NODELIST/REASON
30686  CPU        Test_Job cranetest   CraneTest  Running Batch  00:00:19  00:30:01   2      crane[02-03]
30685  CPU        Test_Job cranetest   CraneTest  Running Batch  00:00:19  00:25:25   2      crane[02-03]
```
```text
[cranetest@crane01 ~]$ cqueue
JOBID  PARTITION  NAME     USER        ACCOUNT    STATUS  TYPE   TIME     TIMELIMIT  NODES  NODELIST/REASON
30686  CPU        Test_Job cranetest   CraneTest  Running Batch  00:00:19  00:30:01   2      crane[02-03]
30685  CPU        Test_Job cranetest   CraneTest  Running Batch  00:00:19  00:25:25   2      crane[02-03]
```
```text
[cranetest@crane01 ~]$ cqueue
JOBID  PARTITION  NAME     USER        ACCOUNT    STATUS  TYPE   TIME     TIMELIMIT  NODES  NODELIST/REASON
30685  CPU        Test_Job cranetest   CraneTest  Running Batch  00:00:37  00:25:25   2      crane[02-03]
```

Cancel multiple jobs:
```bash
ccancel 30686,30687,30688
```

### Cancel by Job Name

Cancel all jobs named "test1":
```bash
ccancel -n test1
```

**Result:**

```text
[cranetest@crane01 ~]$ cqueue
JOBID  PARTITION  NAME   USER        ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT  NODES  NODELIST/REASON
30743  CPU        test5  cranetest   CraneTest  Pending  Batch  -         00:30:01   1      Priority
30739  CPU        test1  cranetest   CraneTest  Running  Batch  00:05:38  00:30:01   1      crane03
30742  CPU        test4  cranetest   CraneTest  Running  Batch  00:02:28  00:30:01   1      crane02
30740  CPU        test2  cranetest   CraneTest  Running  Batch  00:05:34  00:30:01   1      crane02
30741  CPU        test3  cranetest   CraneTest  Running  Batch  00:05:33  00:30:01   1      crane03
```
```text
[cranetest@crane01 ~]$ ccancel -n test1
Job 30739 cancelled successfully.
```
```text
[cranetest@crane01 ~]$ cqueue
JOBID  PARTITION  NAME   USER        ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT  NODES  NODELIST/REASON
30742  CPU        test4  cranetest   CraneTest  Running  Batch  00:03:26  00:30:01   1      crane02
30743  CPU        test5  cranetest   CraneTest  Running  Batch  00:00:04  00:30:01   1      crane03
30740  CPU        test2  cranetest   CraneTest  Running  Batch  00:06:32  00:30:01   1      crane02
30741  CPU        test3  cranetest   CraneTest  Running  Batch  00:06:31  00:30:01   1      crane03
```

### Cancel by Partition

Cancel all jobs in GPU partition:
```bash
ccancel -p GPU
```

**Result:**

```text
[cranetest@crane01 ~]$ cqueue
JOBID  PARTITION  NAME     USER        ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT  NODES  NODELIST/REASON
30766  GPU        Test_Job cranetest   CraneTest  Pending  Batch  -         00:30:01   1      Priority
30767  GPU        Test_Job cranetest   CraneTest  Pending  Batch  -         00:30:01   1      Priority
30764  CPU        Test_Job cranetest   CraneTest  Running  Batch  00:00:12  00:30:01   1      crane03
30765  GPU        Test_Job cranetest   CraneTest  Running  Batch  00:00:03  00:30:01   1      crane03
30763  CPU        Test_Job cranetest   CraneTest  Running  Batch  00:00:13  00:30:01   1      crane02
```
```text
[cranetest@crane01 ~]$ ccancel -p GPU
Job 30766,30767,30765 cancelled successfully.
```
```text
[cranetest@crane01 ~]$ cqueue
JOBID  PARTITION  NAME     USER        ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT  NODES  NODELIST/REASON
30764  CPU        Test_Job cranetest   CraneTest  Running  Batch  00:00:50  00:30:01   1      crane03
30763  CPU        Test_Job cranetest   CraneTest  Running  Batch  00:00:51  00:30:01   1      crane02
```

### Cancel by Node

Cancel all jobs running on crane02:
```bash
ccancel -w crane02
```

```text
[cranetest@crane01 ~]$ ccancel -w crane02
Job 30683,30684 cancelled successfully.
```

### Cancel by State

Cancel all pending jobs:
```bash
ccancel -t Pending
```


```text
[cranetest@crane01 ~]$ ccancel -t Pending
Job 30687 cancelled successfully.
```

Cancel all running jobs:
```bash
ccancel -t Running
```

Cancel all jobs (pending and running):
```bash
ccancel -t All
```

### Cancel by Account

Cancel all jobs under PKU account:
```bash
ccancel -A PKU
```

```text
[root@cranetest-rocky01 zhouhao]# ccancel -A ROOT --json
{"cancelled_tasks":[128088,128089,128090,128091,128092,128093,128094,128095,128096,128097,128098,128099,128100,128101,128102,128103,128104,128105,128106,128107,128108,128109,128110,128111,128112,128113,128114,128115,128116,128117,128118,128119,128120,128121,128122,128123,128124,128125,128126,128127,128128,128129,128130,128131,128132,128133,128134,128135,128136,128137,128138,128139,128140,128141,128142,128143,128144,128145,128146,128147,128148,128149,128150,128151,128152,128153,128154,128155,128156,128157,128158,128159,128160,128161,128162,128163,128164,128165,128166,128167,128168,128169,128170,128171,128172,128173,128174,128175,128176,128177],"not_cancelled_tasks":[],"not_cancelled_reasons":{}}
```

### Cancel by User

Cancel all jobs submitted by user ROOT:
```bash
ccancel -u ROOT
```

```text
[root@cranetest-rocky01 zhouhao]# ccancel -u root
1279986 1279987 1279988 1279989 1279990 1279991 1279992 1279993 1279994 1279995 1279996 1279997 1279998 1279999 1280000 1280001 1280002 1280003 1280004 1280005
1280006 1280007 1280008 1280009 1280010 1280011 1280012 1280013 1280014 1280015 1280016 1280017 1280018 1280019 1280020 1280021 1280022 1280023 1280024 1280025
1280026 1280027 1280028 1280029 1280030 1280031 1280032 1280033 1280034 1280035 1280036 1280037 1280038 1280039 1280040 1280041 1280042 1280043 1280044 1280045
1280046 1280047 1280048 1280049 1280050 1280051 1280052 1280053 1280054 1280055 1280056 1280057 1280058 1280059 1280060 1280061 1280062 1280063 1280064 1280065
1280066 1280067 1280068 1280069 1280070 1280071 1280072 1280073 1280074 1280075 1280076 1280077 1280078 1280079 1280080 1280081 1280082 1280083 1280084 1280085
1280086 1280087 1280088 1280089 1280090 1280091 1280092 1280093 1280094 1280095 1280096 1280097 1280098 1280099 1280100 1280101 1280102 1280103 1280104 1280105
1280106 1280107 1280108 1280109 1280110 1280111 1280112 1280113 1280114 1280115 1280116 1280117 1280118 1280119 1280120 1280121 1280122 1280123 1280124 1280125
1280126 1280127 1280128 1280129 1280130 1280131 1280132 1280133 1280134 1280135 1280136 1280137 1280138 1280139 1280140 1280141 1280142 1280143 1280144 1280145
1280146 1280147 1280148 1280149 1280150 1280151 1280152 1280153 1280154 1280155 1280156 1280157 1280158 1280159 1280160 1280161 1280162 1280163 1280164 1280165
1280166 1280167 1280168 1280169 1280170 1280171 1280172 1280173 1280174 1280175 1280176 1280177 1280178 1280179 1280180 1280181 1280182 1280183 1280184 1280185
1280186 1280187 1280188 1280189 1280190 1280191 1280192 1280193 1280194 1280195 1280196 1280197 1280198 1280199 1280200 1280201 1280202 1280203 1280204 1280205
1280206 1280207 1280208 1280209 1280210 1280211 1280212 1280213 1280214 1280215 1280216 1280217 1280218 1280219 1280220 1280221 1280222 1280223 1280224 1280225
1280226 1280227 1280228 1280229 1280230 1280231 1280232 1280233 1280234 1280235 1280236 1280237 1280238 1280239 1280240 1280241 1280242 1280243 1280244 1280245
1280246 1280247 1280248 1280249 1280250 1280251 1280252 1280253 1280254 1280255 1280256 1280257 1280258 1280259 1280260 1280261 1280262 1280263 1280264 1280265
1280266 1280267 1280268 1280269 1280270 1280271 1280272 1280273 1280274 1280275 1280276 1280277 1280278 1280279 1280280 1280281 1280282 1280283 1280284 1280285
1280286 1280287 1280288 1280289 1280290 1280291 1280292 1280293 1280294 1280295 1280296 1280297 1280298 1280299 1280300 1280301 1280302 1280303 1280304 1280305
1280306 1280307 1280308 1280309 1280310 1280311 1280312 1280313 1280314 1280315 1280316 1280317 1280318 1280319 1280320 1280321 1280322 1280323 1280324 1280325
1280326 1280327 1280328 1280329 1280330 1280331 1280332 1280333 1280334 1280335 1280336 1280337 1280338 1280339 1280340 1280341 1280342 1280343 1280344 1280345
1280346 1280347 1280348 1280349 1280350 1280351 1280352 1280353
```

### Combined Filters

Cancel all pending jobs in CPU partition:
```bash
ccancel -t Pending -p CPU
```

Cancel all jobs by user alice in GPU partition:
```bash
ccancel -u alice -p GPU
```

Cancel running jobs on specific nodes:
```bash
ccancel -t Running -w crane01,crane02
```

### JSON Output

Get cancellation results in JSON format:
```bash
ccancel 30686 --json
```

Cancel with filters and JSON output:
```bash
ccancel -p GPU -t Pending --json
```

## Examples Overview

```text
[cranetest@crane01 ~]$ cqueue
JOBID  PARTITION  NAME     USER        ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT  NODES  NODELIST/REASON
30685  CPU        Test_Job cranetest   CraneTest  Pending  Batch  -         00:25:25   2      Priority
30686  CPU        Test_Job cranetest   CraneTest  Pending  Batch  -         00:30:01   2      Priority
30687  CPU        Test_Job cranetest   CraneTest  Pending  Batch  -         00:30:01   2      Priority
30683  CPU        Test_Job cranetest   CraneTest  Running  Batch  00:07:43  00:30:01   2      crane[02-03]
30684  CPU        Test_Job cranetest   CraneTest  Running  Batch  00:07:41  00:30:01   2      crane[02-03]
```

## Canceling Job Steps

CraneSched supports canceling individual steps within a job using the step ID format `jobid.stepid`. This allows you to terminate specific steps while keeping the parent job and other steps running.

### Step Cancellation Syntax

**Cancel a specific step:**
```bash
ccancel 123.1      # Cancel step 1 of job 123
```

**Cancel multiple steps:**
```bash
ccancel 123.1,123.2,456.3
```

**Cancel entire job (all steps):**
```bash
ccancel 123        # Cancels job 123 and all its steps
```

### Step Cancellation Behavior

When canceling steps:

- **Single Step Cancellation**: Canceling a specific step (`jobid.stepid`) only affects that step
  - The parent job continues running
  - Other steps in the same job are not affected
  - Resources allocated to the step are released back to the parent job

- **Full Job Cancellation**: Canceling a job without specifying a step ID (`jobid`) cancels:
  - All steps within the job
  - The parent job itself
  - All allocated resources are released

### Step Cancellation Examples

**Cancel only step 2 of job 100:**
```bash
ccancel 100.2
```

**Cancel steps 1 and 2 of job 100 (step 3 continues if exists):**
```bash
ccancel 100.1,100.2
```

**Mixed cancellation - entire job 100 and step 3 of job 200:**
```bash
ccancel 100,200.3
```

**Query steps before canceling:**
```bash
# View all steps for a job
cqueue --step -j 123

# Cancel specific steps based on status
ccancel 123.2,123.4
```

### Step Cancellation Permissions

- **Regular Users**: Can only cancel steps belonging to their own jobs
- **Coordinators**: Can cancel steps within jobs under their account
- **Operators/Admins**: Can cancel any step in the system

## Behavior After Cancellation

After a job is cancelled:

1. **Process Termination**: If there are no other jobs from the user on the allocated nodes, the job scheduling system will terminate all user processes on those nodes

2. **SSH Access Revocation**: SSH access to the allocated nodes will be revoked for the user

3. **Resource Release**: All allocated resources (CPUs, memory, GPUs) are immediately released and become available for other jobs

4. **Job State Update**: The job state changes to `CANCELLED` in the job history

## Permission Requirements

- **Regular Users**: Can only cancel their own jobs
- **Coordinators**: Can cancel jobs within their account
- **Operators/Admins**: Can cancel any job in the system

## Important Notes

1. **Immediate Effect**: Job cancellation takes effect immediately. Running jobs are terminated without grace period by default

2. **Multiple Jobs**: You can cancel multiple jobs at once using comma-separated job IDs or filter conditions

3. **No Confirmation**: There is no confirmation prompt. Jobs are cancelled immediately upon command execution

4. **State Filtering**: Use `-t` to target specific job states to avoid accidentally cancelling jobs in unintended states

5. **Job/Step ID Format**: IDs must follow these formats with no spaces:
   - Jobs: `<job_id>` or `<job_id>,<job_id>,<job_id>...`
   - Steps: `<job_id>.<step_id>` or `<job_id>.<step_id>,<job_id>.<step_id>...`
   - Mixed: `<job_id>,<job_id>.<step_id>...`

## Error Handling

Common errors:

- **Invalid Job ID**: Returns error if job ID doesn't exist or you don't have permission to cancel it
- **No Matching Jobs**: If filter conditions match no jobs, returns success with zero jobs cancelled
- **Invalid State**: State must be one of: PENDING, RUNNING, ALL (case-insensitive)

## See Also

- [cbatch](cbatch.md) - Submit batch jobs
- [crun](crun.md) - Run interactive tasks
- [calloc](calloc.md) - Allocate interactive resources
- [cqueue](cqueue.md) - View job queue
- [cacct](cacct.md) - View job accounting information
- [ccontrol](ccontrol.md) - Control jobs and system resources
