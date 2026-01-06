# Lua Script Configuration Guide

This document describes Crane’s Lua script configuration and its APIs.

## Lua Callback Functions

When writing Lua scripts, **all callback functions must be implemented**.  
Any function you do not use must still provide a stub.

### crane_job_submit

This function is called by the **cranectld daemon** when it receives job submission parameters from the user, regardless of the command used (e.g., `calloc`, `cbatch`).

Only parameters explicitly provided by the user will appear in `job_desc`.  
Fields not defined at submission time will contain `0` or `nil`.

This function can be used to log and/or modify user-supplied job submission parameters.

Note: This function has access to global cranectld data structures, for example to inspect available partitions, reservations, etc.

```lua
function crane_job_submit(job_desc, part_list, submit_uid)
```

**Parameters:**

- **job_desc**: Job allocation request specification.
- **part_list**: List of partitions (pointers) that the user is authorized to use.
- **submit_uid**: User ID initiating the request.

**Returns:**

- **crane.SUCCESS** — Job submission accepted.
- **crane.ERROR** — Job submission rejected due to error.
- **crane.\*** (Crane error codes) — Reject submission according to defined error codes.

#### Note
Since `job_desc` **contains only values explicitly specified by the user**, undefined fields can be detected by checking whether they are `nil` or `0`.  
This allows sites to enforce policies, such as *requiring the user to specify the number of nodes*.

### crane_job_modify

This function is called by the **cranectld daemon** when it receives job modification parameters from the user, regardless of the command used (e.g., `ccontrol`).  
It can be used to log and/or modify job modification parameters supplied by the user.

Like `crane_job_submit`, this function can also access global cranectld data structures, such as partitions and reservations.

```lua
function crane_job_modify(job_desc, job_ptr, part_list, submit_uid)
```

**Parameters:**

- **job_desc**  
  Specification of the job modification request.

- **job_ptr**  
  The current data structure maintained by cranectld for the job being modified.

- **part_list**  
  List of partitions the user is authorized to use.

- **modify_uid**  
  User ID performing the modification.

**Returns:**  
Same as the return values of `crane_job_submit`.

## Lua Script Availability

### Available Functions

| Function Name                                               | Description                                   |
|------------------------------------------------------------|-----------------------------------------------|
| crane.get_job_env_field(job_desc, "env_name")              | Query the value of a job's environment field  |
| crane.set_job_env_field(job_desc, "env_name", "env_value") | Set the value of a job's environment field    |
| crane.log_error("msg") / log_info("msg") / log_debug("msg")| Logging functions                             |
| crane.log_user("msg")                                      | User-visible log message                     |

---

### Input Parameter Attributes

#### job_desc

| Attribute Name             | Type    | Description                                   |
|---------------------------|---------|-----------------------------------------------|
| time_limit                | number  | Time limit                                    |
| partition_id              | string  | Partition to which the job belongs            |
| requested_node_res_view   | table   | Requested resource information                |
| type                      | number  | Job type                                      |
| uid                       | number  | UID of the job owner                          |
| gid                       | number  | GID of the job owner                          |
| account                   | string  | Account associated with the job               |
| name                      | string  | Job name                                      |
| qos                       | string  | QoS associated with the job                   |
| node_num                  | number  | Number of nodes                               |
| ntasks_per_node           | number  | Number of tasks per node                      |
| cpus_per_task             | number  | Number of CPUs per task                       |
| included_nodes            | string  | Nodes explicitly included                    |
| excluded_nodes            | string  | Nodes explicitly excluded                    |
| requeue_if_failed         | boolean | Whether the job can be requeued on failure    |
| get_user_env              | boolean | Whether to load user environment variables    |
| cmd_line                  | string  | Job command line                              |
| env                       | table   | Environment variables                         |
| cwd                       | string  | Working directory of the job                  |
| extra_attr                | string  | Extra attributes                              |
| reservation               | string  | Reservation information                      |
| begin_time                | number  | Job start time                                |
| exclusive                 | boolean | Whether to run in exclusive node mode         |
| licenses_count            | table   | License information                           |


---

#### part_list

| Attribute Name          | Type                 | Description                     |
|-------------------------|----------------------|---------------------------------|
| hostlist                | string               | List of node hostnames           |
| state                   | number               | Partition state                  |
| name                    | string               | Partition name                   |
| total_nodes             | number               | Total number of nodes            |
| alive_nodes             | number               | Number of alive nodes            |
| res_total               | ResourceView         | Total resource information       |
| res_avail               | ResourceView         | Available resource information  |
| res_alloc               | ResourceView         | Allocated (in-use) resource info |
| allowed_accounts        | table(string list)   | Allowed accounts                 |
| denied_accounts         | table(string list)   | Denied accounts                  |
| default_mem_per_cpu     | number               | Default memory per CPU           |
| max_mem_per_cpu         | number               | Maximum memory per CPU           |

---

### Global Variables

#### crane.jobs or job_ptr

| Attribute Name | Type           | Description                              |
|----------------|----------------|------------------------------------------|
| type           | number         | Job type                                 |
| task_id        | number         | Job ID                                   |
| name           | string         | Job name                                 |
| partition      | string         | Partition to which the job belongs       |
| uid            | number         | UID of the job owner                     |
| time_limit     | number         | Time limit                               |
| end_time       | number         | Job end time                             |
| submit_time    | number         | Job submission time                     |
| account        | string         | Account associated with the job          |
| node_num       | number         | Number of nodes                          |
| cmd_line       | string         | Submission command line                  |
| cwd            | string         | Job working directory                    |
| username       | string         | Username who submitted the job           |
| qos            | string         | QoS associated with the job              |
| req_res_view   | ResourceView   | Requested resource information           |
| licenses_count | table(map)     | License information                     |
| req_nodes      | string         | Requested nodes                          |
| exclude_nodes  | string         | Excluded nodes                           |
| extra_attr     | string         | Extra attributes                         |
| reservation    | string         | Reservation information                 |
| held           | boolean        | Whether the job is held                  |
| status         | number         | Job status                               |
| exit_code      | number         | Job exit code                            |
| priority       | number         | Job priority                             |
| pending_reason | string         | Reason for being pending                 |
| craned_list    | string         | List of nodes allocated to the job       |
| elapsed_time   | number         | Elapsed execution time                   |
| execution_node | string         | Job execution node                       |
| exclusive      | boolean        | Whether the job runs in exclusive mode   |
| alloc_res_view | ResourceView   | Allocated (in-use) resource information |
| env            | table(map)     | Environment variables                   |

---

#### crane.reservations

| Attribute Name   | Type                 | Description                               |
|------------------|----------------------|-------------------------------------------|
| reservation_name | string               | Reservation name                          |
| start_time       | number               | Reservation start time                   |
| duration         | number               | Reservation duration                     |
| partition        | string               | Reservation partition                    |
| craned_regex     | string               | Reserved nodes (regex)                   |
| res_total        | ResourceView         | Total reserved resource information      |
| res_avail        | ResourceView         | Available reserved resource information  |
| res_alloc        | ResourceView         | Allocated reserved resource information  |
| allowed_accounts | table(string list)   | Accounts allowed for the reservation     |
| denied_accounts  | table(string list)   | Accounts denied for the reservation      |
| allowed_users    | table(string list)   | Users allowed for the reservation        |
| denied_users     | table(string list)   | Users denied for the reservation         |


## Lua Script Configuration

The system needs to have Lua version 5.x and lua-devel installed.

### Crane compilation
```shell
# Check the packaging guide for details
cmake -G Ninja .. -DCRANE_WITH_LUA=true
```

### /etc/crane/config.yaml

```yaml
JobSubmitLuaScript: /path/to/your/job_submit.lua
```

## Lua Script Example

```lua
function crane_job_submit(job_desc, part_list, uid)
    crane.log_info("==== crane_job_submit ====")
    crane.log_info("job_desc.name: %s", job_desc.name)
    crane.log_info("job_desc.uid: %d", job_desc.uid)
    crane.log_info("job_desc.gid: %d", job_desc.gid)
    crane.log_info("job_desc.account: %s", job_desc.account)
    crane.log_info("job_desc.node_num: %d", job_desc.node_num)
    crane.log_info("job_desc.qos: %s", job_desc.qos)
    crane.log_info("job_desc.type: %d", job_desc.type)
    crane.log_info("job_desc.time_limit: %d", job_desc.time_limit)
    crane.log_info("job_desc.partition_id: %s", job_desc.partition_id)
    crane.log_info("job_desc.cmd_line: %s", job_desc.cmd_line)
    crane.log_info("job_desc.cwd: %s", job_desc.cwd)
    crane.log_info("job_desc.extra_attr: %s", job_desc.extra_attr)
    crane.log_info("job_desc.reservation: %s", job_desc.reservation)
    crane.log_info("job_desc.begin_time: %d", job_desc.begin_time)
    crane.log_info("job_desc.exclusive: %s", tostring(job_desc.exclusive))
    crane.log_info("job_desc.requeue_if_failed: %s", tostring(job_desc.requeue_if_failed))

    crane.log_info("job_desc.included_nodes: %s", table.concat(job_desc.included_nodes, ", "))
    crane.log_info("job_desc.excluded_nodes: %s", table.concat(job_desc.excluded_nodes, ", "))

    crane.log_info("job_desc.env:")
    for k, v in pairs(job_desc.env) do
        crane.log_info("  %s = %s", k, v)
    end

    crane.log_info("job_desc.licenses_count:")
    for name, value in pairs(job_desc.licenses_count) do
        crane.log_info("  %s %s", name, tostring(value))
    end

    crane.log_info("job_desc.requested_node_res_view:")
    local rv = job_desc.requested_node_res_view
    crane.log_info("  cpu_count: %d", rv.cpu_count)
    crane.log_info("  memory_bytes: %d", rv.memory_bytes)
    crane.log_info("  device_map:")
    for dev, entry in pairs(rv.device_map) do
        crane.log_info("    %s: untyped=%d", dev, entry.untyped_count)
        crane.log_info("      typed:")
        for tname, tcount in pairs(entry.typed) do
            crane.log_info("        %s: %d", tname, tcount)
        end
    end

    crane.log_info("part_list:")
    for i, part in ipairs(part_list) do
        crane.log_info("  [%d] %s", i, part.name)
        crane.log_info("    hostlist: %s", part.hostlist)
        crane.log_info("    state: %d", part.state)
        crane.log_info("    total_nodes: %d", part.total_nodes)
        crane.log_info("    alive_nodes: %d", part.alive_nodes)
        crane.log_info("    allowed_accounts: %s", table.concat(part.allowed_accounts, ", "))
        crane.log_info("    denied_accounts: %s", table.concat(part.denied_accounts, ", "))
        crane.log_info("    default_mem_per_cpu: %d", part.default_mem_per_cpu)
        crane.log_info("    max_mem_per_cpu: %d", part.max_mem_per_cpu)
        crane.log_info("    res_total.cpu_count: %d", part.res_total.cpu_count)
        crane.log_info("    res_total.memory_bytes: %d", part.res_total.memory_bytes)
    end

    crane.log_info("uid: %d", uid)

    -- crane.get_qos_priority
    local prio = crane.get_qos_priority(job_desc.qos)
    crane.log_info("get_qos_priority(job_desc.qos): %s", tostring(prio))

    -- crane.get_job_env_field
    local env_test = crane.get_job_env_field(job_desc, "PATH")
    crane.log_info("get_job_env_field(job_desc, 'PATH'): %s", tostring(env_test))

    -- crane.set_job_env_field
    crane.set_job_env_field("TEST_VAR", "test_value", job_desc)
    crane.log_info("After set_job_env_field, job_desc.env.TEST_VAR: %s", tostring(job_desc.env.TEST_VAR))

    -- crane.get_resv
    local resv = crane.get_resv(job_desc.reservation)
    if resv then
        crane.log_info("get_resv(job_desc.reservation): %s", resv.reservation_name)
        crane.log_info("  start_time: %d", resv.start_time)
        crane.log_info("  duration: %d", resv.duration)
        crane.log_info("  partition: %s", resv.partition)
        crane.log_info("  craned_regex: %s", resv.craned_regex)
        crane.log_info("  allowed_accounts: %s", table.concat(resv.allowed_accounts, ", "))
        crane.log_info("  allowed_users: %s", table.concat(resv.allowed_users, ", "))
        crane.log_info("  res_total.cpu_count: %d", resv.res_total.cpu_count)
        crane.log_info("  res_total.memory_bytes: %d", resv.res_total.memory_bytes)
    else
        crane.log_info("get_resv(job_desc.reservation): nil")
    end

    -- crane.jobs
    crane.log_info("crane.jobs:")
    for id, job in pairs(crane.jobs) do
        crane.log_info("  Job ID: %d", id)
        crane.log_info("    name: %s", job.name)
        crane.log_info("    username: %s", job.username)
        crane.log_info("    account: %s", job.account)
        crane.log_info("    partition: %s", job.partition)
        crane.log_info("    status: %d", job.status)
        crane.log_info("    node_num: %d", job.node_num)
        crane.log_info("    time_limit: %d", job.time_limit)
        crane.log_info("    submit_time: %d", job.submit_time)
        crane.log_info("    cmd_line: %s", job.cmd_line)
        crane.log_info("    env:")
        for k, v in pairs(job.env) do
            crane.log_info("      %s %s", k, v)
        end
        crane.log_info("    req_res_view.cpu_count: %d", job.req_res_view.cpu_count)
        crane.log_info("    req_res_view.memory_bytes: %d", job.req_res_view.memory_bytes)
    end

    -- crane.reservations
    crane.log_info("crane.reservations:")
    for name, resv in pairs(crane.reservations) do
        crane.log_info("  Reservation Name: %s", resv.reservation_name)
        crane.log_info("    start_time: %d", resv.start_time)
        crane.log_info("    duration: %d", resv.duration)
        crane.log_info("    partition: %s", resv.partition)
        crane.log_info("    craned_regex: %s", resv.craned_regex)
        crane.log_info("    allowed_accounts: %s", table.concat(resv.allowed_accounts, ", "))
        crane.log_info("    allowed_users: %s", table.concat(resv.allowed_users, ", "))
        crane.log_info("    res_total.cpu_count: %d", resv.res_total.cpu_count)
        crane.log_info("    res_total.memory_bytes: %d", resv.res_total.memory_bytes)
    end

    return crane.SUCCESS
end

function crane_job_modify(job_desc, job_rec, part_list, uid)
    crane.log_info("==== crane_job_modify ====")

    return crane.SUCCESS
end
```