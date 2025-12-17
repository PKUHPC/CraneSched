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

| Function Name                                                    | Description                          |
|------------------------------------------------------------------|--------------------------------------|
| crane._get_job_env_field_value(job_desc, "env_name")            | Get the value of a job's `env` field |
| crane._get_job_req_field_name(job_desc, "name")                 | Get the value of a job's `req` field |
| crane._set_job_env_field(job_desc, "env_name", "env_value")     | Set a job's `env` field              |
| crane._set_job_req_field(job_desc, "name", "new_job_name")      | Set a job's `req` field              |
| crane._get_part_rec_field(part, "field_name")                   | Get the `rec` field of a partition   |
| crane.log_error("msg") / log_info("msg") / log_debug("msg")     | Logging functions                    |
| crane.log_user("msg")                                           | Log message to user                  |

---

### Input Parameter Attributes

#### job_desc

| Property Name              | Type    | Description                      |
|----------------------------|---------|----------------------------------|
| time_limit                 | number  | Time limit                       |
| partition                  | string  | Job partition                    |
| requested_node_res_view    | table   | Requested resource information   |
| type                       | number  | Job type                         |
| uid                        | number  | Job owner UID                    |
| account                    | string  | Job owner account                |
| name                       | string  | Job name                         |
| qos                        | string  | Job QOS                          |
| node_num                   | number  | Number of nodes                  |
| ntasks_per_node            | number  | Number of tasks per node         |
| cpus_per_task              | number  | Number of CPUs per task          |
| requeue_if_failed          | boolean | Allow requeue on failure         |
| get_user_env               | boolean | Get user environment variables   |
| gid                        | number  | Job owner GID                    |
| batch_meta                 | table   | Batch job information            |
| interactive_meta           | table   | Interactive job information      |
| extra_attr                 | string  | Extra attributes                 |
| cmd_line                   | string  | Submission command               |
| cwd                        | string  | Working directory                |
| env                        | table   | Environment variables            |
| excludes                   | string  | Excluded nodes                   |
| nodelist                   | string  | Node list                        |
| reservation                | string  | Reservation information          |
| begin_time                 | number  | Job start time                   |
| exclusive                  | boolean | Exclusive node usage             |
| hold                       | boolean | Hold job                         |

---

#### part_list

| Attribute Name        | Type                 | Description                  |
|-----------------------|----------------------|------------------------------|
| name                  | string               | Partition name               |
| node_list             | string               | Node list                    |
| total_nodes           | number               | Total number of nodes        |
| alive_nodes           | number               | Number of alive nodes        |
| state                 | number               | Partition state              |
| default_mem_per_cpu   | number               | Default memory per CPU       |
| max_mem_per_cpu       | number               | Max memory per CPU           |
| allowed_accounts      | table(string list)   | Allowed accounts             |
| denied_accounts       | table(string list)   | Denied accounts              |
| res_total             | table                | Total resource information    |
| res_avail             | table                | Available resource info       |
| res_in_used           | table                | In-use resource info          |

**Resource table fields:**

- cpu_core_limit: CPU limit
- memory_limit_bytes: Memory limit
- memory_sw_limit_bytes: Swap limit

---

### Global Variables

#### crane.jobs or job_ptr

| Attribute Name    | Type    | Description                 |
|-------------------|---------|-----------------------------|
| job_id            | number  | Job ID                      |
| job_name          | string  | Job name                    |
| type              | number  | Job type                    |
| partition         | string  | Job partition               |
| uid               | number  | User ID                     |
| account           | string  | Account                     |
| qos               | string  | QoS                         |
| gid               | number  | Group ID                    |
| time_limit        | number  | Time limit                  |
| start_time        | number  | Job start time              |
| end_time          | number  | Job end time                |
| submit_time       | number  | Job submission time         |
| node_num          | number  | Number of nodes             |
| cmd_line          | string  | Command line                |
| cwd               | string  | Working directory           |
| username          | string  | User name                   |
| req_res_view      | table   | Requested resource view     |
| req_nodes         | string  | Requested nodes             |
| exclude_nodes     | string  | Excluded nodes              |
| extra_attr        | string  | Extra attributes            |
| held              | boolean | Whether the job is held     |
| status            | number  | Job status                  |
| exit_code         | number  | Exit code                   |
| execution_node    | string  | Execution node              |
| exclusive         | boolean | Whether job is exclusive    |

---

#### crane.reservations

| Attribute Name      | Type                | Description                 |
|---------------------|---------------------|-----------------------------|
| reservation_name    | string              | Reservation name            |
| start_time          | number              | Start time                  |
| duration            | number              | Duration                    |
| partition           | string              | Partition                   |
| craned_regex        | string              | Node regex                  |
| res_total           | table               | Total resource info         |
| res_avail           | table               | Available resource info     |
| res_alloc           | table               | Allocated resource info     |
| allowed_accounts    | table(string list)  | Allowed accounts            |
| denied_accounts     | table(string list)  | Denied accounts             |
| allowed_users       | table(string list)  | Allowed users               |
| denied_users        | table(string list)  | Denied users                |

**Resource table fields:**

- cpu_core_limit: CPU limit
- memory_limit_bytes: Memory limit
- memory_sw_limit_bytes: Swap limit


## Lua Script Configuration

### /etc/crane/config.yaml

```yaml
JobSubmitLuaScript: /path/to/your/job_submit.lua
```

## Lua Script Example

```lua
function crane_job_submit(job_desc, part_list, uid)
    -- 日志函数演示
    crane.log_info("提交作业: %s, uid: %d", job_desc.name, uid)
    crane.log_debug("作业类型: %d, 分区: %s", job_desc.type, job_desc.partition)
    crane.log_error("这是一个错误日志示例")
    crane.log_user("用户消息: Crane Lua 测试")

    -- 访问 job_desc 所有字段（只读）
    crane.log_info("time_limit: %s", tostring(job_desc.time_limit))
    crane.log_info("partition: %s", tostring(job_desc.partition))
    crane.log_info("requested_node_res_view: %s", tostring(job_desc.requested_node_res_view))
    crane.log_info("type: %s", tostring(job_desc.type))
    crane.log_info("uid: %s", tostring(job_desc.uid))
    crane.log_info("account: %s", tostring(job_desc.account))
    crane.log_info("name: %s", tostring(job_desc.name))
    crane.log_info("qos: %s", tostring(job_desc.qos))
    crane.log_info("node_num: %s", tostring(job_desc.node_num))
    crane.log_info("ntasks_per_node: %s", tostring(job_desc.ntasks_per_node))
    crane.log_info("cpus_per_task: %s", tostring(job_desc.cpus_per_task))
    crane.log_info("requeue_if_failed: %s", tostring(job_desc.requeue_if_failed))
    crane.log_info("get_user_env: %s", tostring(job_desc.get_user_env))
    crane.log_info("gid: %s", tostring(job_desc.gid))
    crane.log_info("extra_attr: %s", tostring(job_desc.extra_attr))
    crane.log_info("cmd_line: %s", tostring(job_desc.cmd_line))
    crane.log_info("cwd: %s", tostring(job_desc.cwd))
    crane.log_info("container: %s", tostring(job_desc.container))
    crane.log_info("reservation: %s", tostring(job_desc.reservation))

    -- env 环境变量
    if job_desc.env then
        for k, v in pairs(job_desc.env) do
            crane.log_debug("环境变量: %s=%s", k, v)
        end
    end

    -- meta（batch_meta / interactive_meta）
    if job_desc.bash_meta then
        crane.log_info("bash_meta.sh_script: %s", job_desc.bash_meta.sh_script or "")
        crane.log_info("bash_meta.output_file_pattern: %s", job_desc.bash_meta.output_file_pattern or "")
        crane.log_info("bash_meta.error_file_pattern: %s", job_desc.bash_meta.error_file_pattern or "")
        crane.log_info("bash_meta.interpreter: %s", job_desc.bash_meta.interpreter or "")
    end
    if job_desc.interactive_meta then
        crane.log_info("interactive_meta.interactive_type: %s", tostring(job_desc.interactive_meta.interactive_type or -1))
    end

    -- requested_node_res_view
    crane.log_info("requested_node_res_view: %s", tostring(job_desc.requested_node_res_view))

    -- 分区列表 part_list
    for pname, part in pairs(part_list) do
        crane.log_info("分区: %s, 节点总数: %s, 存活节点: %s", part.name, tostring(part.total_nodes), tostring(part.alive_nodes))
        crane.log_debug("分区状态: %s, 默认mem/cpu: %s, 最大mem/cpu: %s", tostring(part.state), tostring(part.default_mem_per_cpu), tostring(part.max_mem_per_cpu))
        if part.allowed_accounts then
            for i, acc in ipairs(part.allowed_accounts) do
                crane.log_debug("允许账号: %s", acc)
            end
        end
        if part.denied_accounts then
            for i, acc in ipairs(part.denied_accounts) do
                crane.log_debug("禁止账号: %s", acc)
            end
        end
        crane.log_debug("nodelist: %s", tostring(part.nodelist))
        crane.log_debug("res_total: %s", tostring(part.res_total))
        crane.log_debug("res_avail: %s", tostring(part.res_avail))
        crane.log_debug("res_in_use: %s", tostring(part.res_in_use))
    end

    -- 全局 jobs
    if crane.jobs then
        for job_id, job in crane.jobs:iter() do
            crane.log_debug("已存在作业: %s, 用户: %s, 状态: %s, 优先级: %s", job.job_name, job.username, tostring(job.status), tostring(job.priority))
            crane.log_debug("作业ID: %s, 分区: %s", tostring(job.job_id), job.partition)
            crane.log_debug("time_limit: %s, start_time: %s, end_time: %s, submit_time: %s", tostring(job.time_limit), tostring(job.start_time), tostring(job.end_time), tostring(job.submit_time))
            crane.log_debug("cmd_line: %s, cwd: %s, qos: %s, extra_attr: %s, reservation: %s, container: %s", job.cmd_line, job.cwd, job.qos, job.extra_attr, job.reservation, job.container)
            crane.log_debug("held: %s, exclusive: %s", tostring(job.held), tostring(job.exclusive))
            crane.log_debug("req_nodes: %s, exclude_nodes: %s, execution_node: %s", tostring(job.req_nodes), tostring(job.exclude_nodes), tostring(job.execution_node))
            crane.log_debug("exit_code: %s", tostring(job.exit_code))
        end
    end

    -- 全局 reservations
    if crane.reservations then
        for resv_name, resv in crane.reservations:iter() do
            crane.log_debug("预约: %s, 分区: %s, 开始时间: %s, 时长: %s", resv.reservation_name, resv.partition, tostring(resv.start_time), tostring(resv.duration))
            if resv.allowed_accounts then
                for i, acc in ipairs(resv.allowed_accounts) do
                    crane.log_debug("预约允许账号: %s", acc)
                end
            end
            if resv.denied_accounts then
                for i, acc in ipairs(resv.denied_accounts) do
                    crane.log_debug("预约禁止账号: %s", acc)
                end
            end
            if resv.allowed_users then
                for i, user in ipairs(resv.allowed_users) do
                    crane.log_debug("预约允许用户: %s", user)
                end
            end
            if resv.denied_users then
                for i, user in ipairs(resv.denied_users) do
                    crane.log_debug("预约禁止用户: %s", user)
                end
            end
            crane.log_debug("craned_regex: %s", tostring(resv.craned_regex))
            crane.log_debug("res_total: %s", tostring(resv.res_total))
            crane.log_debug("res_avail: %s", tostring(resv.res_avail))
            crane.log_debug("res_alloc: %s", tostring(resv.res_alloc))
        end
    end

    -- 使用常量
    crane.log_debug("CraneErrCode.ERROR: %s", tostring(crane.ERROR))
    crane.log_debug("CraneErrCode.SUCCESS: %s", tostring(crane.SUCCESS))
    crane.log_debug("TaskStatus.Pending: %s, Running: %s, Completed: %s", tostring(crane.Pending), tostring(crane.Running), tostring(crane.Completed))
    crane.log_debug("TaskType.Batch: %s, Interactive: %s", tostring(crane.Batch), tostring(crane.Interactive))

    -- 字段辅助函数（只读演示，不修改）
    local mt = getmetatable(job_desc)
    local env_val = _get_job_env_field_name(mt._job_desc, "PATH")
    crane.log_debug("_get_job_env_field_name PATH: %s", env_val or "NIL")
    local req_val = _get_job_req_field_name(mt._job_desc, "name")
    crane.log_debug("_get_job_req_field_name name: %s", req_val or "NIL")
    _set_job_env_field(job_desc, "MYVAR", "hello_lua")
    local myvar_val = _get_job_env_field_name(mt._job_desc, "MYVAR")
    crane.log_info("验证 MYVAR: %s", myvar_val or "NIL")
    _set_job_req_field(job_desc, "name", "new_job_name")
    local name_val2 = _get_job_req_field_name(mt._job_desc, "name")
    crane.log_info("验证 name: %s", name_val2 or "NIL")
    

    -- 返回成功
    return crane.SUCCESS
end

function crane_job_modify(job_desc, job_rec, part_list, uid)
    crane.log_info("修改作业（只读访问）: %s, 原分区: %s", job_desc.name, job_desc.partition)
    -- 访问 job_rec 字段（只读）
    if job_rec then
        crane.log_info("原作业状态: %s, 优先级: %s, 用户: %s", tostring(job_rec.status), tostring(job_rec.priority), job_rec.username)
        crane.log_info("原作业ID: %s, 分区: %s", tostring(job_rec.job_id), job_rec.partition)
    end

    -- CraneErrCode 演示
    crane.log_debug("修改作业，错误码: %s", tostring(crane.ERROR))

    -- 返回成功
    return crane.SUCCESS
end
```