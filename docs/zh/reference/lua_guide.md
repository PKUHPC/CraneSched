# Lua 脚本配置指南

本文描述 Crane 的 Lua 脚本配置以及它们的API。

## Lua实现函数
编写lua脚本时，以下所有函数均为必需项。未实现的函数必须提供 stub。

### crane_job_submit
该函数由 **cranectld 守护进程**在接收到用户的作业提交参数时调用，
无论使用的是何种命令（例如 `calloc`、`cbatch`）。
只有用户明确指定的参数会出现在 `job_desc` 中；对于提交时未定义的字段，
会使用 `0` 或 `nil`。

该函数可用于记录（log）和/或修改用户提交的作业参数。  

注意：此函数可以访问 cranectld 的全局数据结构，例如用于检查可用的分区、预留等。

```lua
function crane_job_submit(job_desc, part_list, submit_uid)
```
**参数：**

* job_desc: 作业分配请求参数。
* part_list: 用户有权限使用的分区列表（指针的列表）。
* submit_uid: 发起请求的用户 ID。

**返回：**

* crane.SUCCESS  — 作业提交被接受。
* crane.ERROR — 作业提交因错误被拒绝。
* crane.*（使用Crane错误码）根据定义的错误码拒绝提交。

#### 注意
由于 `job_desc` **只包含用户指定的值**，未定义的字段可通过检查是否为 `nil` 或 `0` 来识别。
这允许站点实施策略，例如 *要求用户必须指定节点数*。

### crane_job_modify
该函数由 **cranectld 守护进程** 在接收到用户修改作业的参数时调用，
无论使用的是何种命令（例如 ccontrol）。
可用于记录和/或修改用户提交的作业修改参数。

与 job_submit 类似，此函数也可以访问 cranectld 的全局数据结构，例如分区、预留等。
```lua
function crane_job_modify(job_desc, job_ptr, part_list, submit_uid)
```
**参数：**

- job_desc
作业修改请求的参数规格。
- job_ptr
cranectld 当前保存的被修改作业的数据结构。
- part_list
该用户有权限使用的分区列表。
- modify_uid
发起作业修改操作的用户 ID。

**返回值：**  与 `crane_job_submit` 一致。


## Lua脚本可用

### 可用函数

| 函数名                                                        | 解释                |
|------------------------------------------------------------|-------------------|
| crane.get_job_env_field(job_desc, "env_name")              | 查询job的env字段的值     |
| crane.set_job_env_field(job_desc, "env_name", "env_value") | 设置job的env字段       |
| crane.log_error("msg")/log_info("msg")/log_debug("msg")    | 日志函数              |
| crane.log_user("msg")                                      | 用户日志              |

### 传入参数属性

#### job_desc

用户请求的作业参数。

| 属性名                     | 类型      | 解释           |
|-------------------------|---------|--------------|
| time_limit              | number  | 时间限制         | 
| partition_id            | string  | 作业所属分区       | 
| requested_node_res_view | table   | 需求资源信息       | 
| type                    | number  | 作业类型         |
| uid                     | number  | 作业所属uid      |
| gid                     | number  | 作业所属gid      |
| account                 | string  | 作业所属账户       |
| name                    | string  | 作业名          |
| qos                     | string  | 作业所属qos      |
| node_num                | number  | 节点数目         |
| ntasks_per_node         | number  | 每个节点的task数目  |
| cpus_per_task           | number  | 每个task的cpu数目 |
| included_nodes          | string  | 包含的节点        |
| excluded_nodes          | string  | 排除的节点        |
| requeue_if_failed       | boolean | 是否允许失败重试     |
| get_user_env            | boolean | 是否获取用户环境变量   |
| cmd_line                | string  | 作业命令行        |
| env                     | table   | 环境变量         |
| cwd                     | string  | 作业执行目录       |
| extra_attr              | string  | 额外的属性        |
| reservation             | string  | 预约信息         |
| begin_time              | number  | 作业开始时间       |
| exclusive               | boolean | 是否独占节点       |
| licenses_count          | table   | 许可证信息        |

#### part_list
该用户有权限使用的分区

| 属性名                 | 类型                 | 解释     |
|---------------------|--------------------|--------|
| hostlist            | string             | 节点主机列表 |
| state               | number             | 分区状态   |
| name                | string             | 分区名    |
| total_nodes         | number             | 节点数目   |
| alive_nodes         | number             | 存活节点数目 |
| res_total           | ResourceView       | 资源信息   |
| res_avail           | ResourceView       | 可用资源信息 |
| res_alloc           | ResourceView       | 已使用资源信息 |
| allowed_accounts    | table(string list) | 允许的账户  |
| denied_accounts     | table(string list) | 拒绝的账户  |
| default_mem_per_cpu | number             | 默认内存   |
| max_mem_per_cpu     | number             | 最大内存   |

### 全局变量

#### crane.jobs or job_ptr

| 属性名            | 类型           | 解释       |
|----------------|--------------|----------|
| type           | number       | 作业类型     |
| task_id        | number       | 作业id     |
| name           | string       | 作业名      |
| partition      | string       | 作业所属分区   |
| uid            | number       | 作业所属uid  |
| time_limit     | number       | 时间限制     |
| end_time       | number       | 作业结束时间   |
| submit_time    | number       | 作业提交时间   |
| account        | string       | 作业所属账户   |
| node_num       | number       | 节点数目     |
| cmd_line       | string       | 提交命令     |
| cwd            | string       | 作业执行目录   |
| username       | string       | 作业提交用户名  |
| qos            | string       | 作业所属qos  |
| req_res_view   | ResourceView | 需求资源信息   |
| license_count  | table(map)   | 许可证信息    |
| req_nodes      | string       | 需求节点信息   |
| exclude_nodes  | string       | 排除节点     |
| extra_attr     | string       | 额外的属性    |
| reservation    | string       | 预约信息     |
| held           | boolean      | 作业是否被挂起  |
| status         | number       | 作业状态     |
| exit_code      | number       | 作业退出码    |
| priority       | number       | 作业优先级    |
| pending_reason | string       | 排队原因     |
| craned_list    | string       | 作业节点列表   |
| elapsed_time   | number       | 已用时间     |
| execution_node | string       | 作业执行节点   |
| exclusive      | boolean      | 作业是否独占节点 |
| alloc_res_view | ResourceView | 已使用资源信息  |
| env            | table(map)   | 环境变量     |

#### crane.reservations

| 属性名              | 类型                 | 解释 |
|------------------|--------------------|-----|
| reservation_name | string             | 预约名称 |
| start_time       | number             | 预约开始时间 |
| duration         | number             | 预约时长 |
| partiton         | string             | 预约分区 |
| craned_regex     | string             | 预约节点 |
| res_total        | ResourceView       | 预约资源信息 |
| res_avail        | ResourceView       | 预约可用资源信息 |
| res_alloc        | ResourceView       | 预约已使用资源信息 |
| allowed_accounts | table(string list) | 预约允许的账户 |
| denied_accounts  | table(string list) | 预约拒绝的账户 |
| allowed_users    | table(string list) | 预约允许的用户 |
| denied_users     | table(string list) | 预约拒绝的用户 |

## Lua 脚本配置

### /etc/crane/config.yaml

```yaml
JobSubmitLuaScript: /path/to/your/job_submit.lua
```

## Lua脚本样例

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