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

| 函数名                                                         | 解释                |
|-------------------------------------------------------------|-------------------|
| crane._get_job_env_field_value(job_desc, "env_name")        | 查询job的env字段的值     |
| crane._get_job_req_field_name(job_desc, "name")             | 获取job的req字段的值     |
| crane._set_job_env_field(job_desc, "env_name", "env_value") | 设置job的env字段       |
| crane._set_job_req_field(job_desc, "name", "new_job_name")  | 设置job的req字段       |
| crane._get_part_rec_field(part, "field_name")               | 获取partition的rec字段 |
| crane.log_error("msg")/log_info("msg")/log_debug("msg")     | 日志函数              |
| crane.log_user("msg")                                       | 用户日志              |

### 传入参数属性

#### job_desc

用户请求的作业参数。

| 属性名                     | 类型      | 解释                  | 是否可修改 |
|---------------------------|-----------|-----------------------|------------|
| time_limit                | number    | 时间限制              | ✔️         |
| partition                 | string    | 作业所属分区          | ✔️         |
| requested_node_res_view   | table     | 需求资源信息          | ❌         |
| type                      | number    | 作业类型              | ❌         |
| uid                       | number    | 作业所属uid           | ❌         |
| account                   | string    | 作业所属账户          | ✔️         |
| name                      | string    | 作业名                | ✔️         |
| qos                       | string    | 作业所属qos           | ✔️         |
| node_num                  | number    | 节点数目              | ✔️         |
| ntasks_per_node           | number    | 每个节点的task数目    | ✔️         |
| cpus_per_task             | number    | 每个task的cpu数目     | ✔️         |
| requeue_if_failed         | boolean   | 是否允许失败重试      | ✔️         |
| get_user_env              | boolean   | 是否获取用户环境变量  | ❌         |
| gid                       | number    | 作业所属gid           | ❌         |
| batch_meta                | table     | 批量作业信息          | ❌         |
| interactive_meta          | table     | 交互式作业信息        | ❌         |
| extra_attr                | string    | 额外的属性            | ✔️         |
| cmd_line                  | string    | 提交命令              | ✔️         |
| cwd                       | string    | 作业执行目录          | ✔️         |
| env                       | table     | 环境变量              | ❌         |
| excludes                  | string    | 排他节点              | ✔️         |
| nodelist                  | string    | 节点列表              | ✔️         |
| reservation               | string    | 预约信息              | ✔️         |
| begin_time                | number    | 作业开始时间          | ✔️         |
| exclusive                 | boolean   | 是否独占节点          | ✔️         |
| hold                      | boolean   | 是否保持作业          | ❌         |



#### part_list
该用户有权限使用的分区

| 属性名                 | 类型                 | 解释 |
|---------------------|--------------------|--|
| name                | string             | 分区名 |
| node_list           | string             | 节点列表 |
| total_nodes         | number             | 节点数目 |
| alive_nodes         | number             | 存活节点数目 |
| state               | number             | 分区状态 |
| default_mem_per_cpu | number             | 默认内存|
| max_mem_per_cpu     | number             | 最大内存|
| allowd_accounts     | table(string list) | 允许的账户|
| denied_accounts     | table(string list) | 拒绝的账户|
| res_total           | table              | 资源信息|
| res_avail           | table              | 可用资源信息|
| res_in_used         | table              | 已使用资源信息|

**资源table**

- cpu_core_limit: cpu限制
- memory_limit_bytes: 内存限制
- memory_sw_limit_bytes: swap限制

### 全局变量

#### crane.jobs or job_ptr

| 属性名       | 类型 | 解释 |
|-----------|----|-----|
| job_id    | number | 作业id |
| job_name  | string | 作业名 |
| type      | number | 作业类型 |
| partition | string | 作业所属分区 |
| uid       | number | 作业所属uid |
| account   | string | 作业所属账户 |
| qos       | string | 作业所属qos |
| gid       | number | 作业所属gid |
| time_limit | number | 时间限制 |
| start_time | number | 作业开始时间 |
| end_time   | number | 作业结束时间 |
| submit_time | number | 作业提交时间 |
| node_num  | number | 节点数目 |
| cmd_line| string | 提交命令 |
| cwd | string | 作业执行目录 |
| username| string | 作业提交用户名 |
| req_res_view| table | 需求资源信息 |
| req_nodes| string | 需求节点信息 |
| exclude_nodes | string | 排除节点 |
| extra_attr | string | 额外的属性 |
| held| boolean | 作业是否被挂起 |
| status | number | 作业状态 |
| exit_code | number | 作业退出码 |
| execution_node | string | 作业执行节点|
| exclusive | boolean | 作业是否独占节点 |

#### crane.reservations

| 属性名              | 类型 | 解释 |
|------------------|----|-----|
| reservation_name | string | 预约名称 |
| start_time       | number | 预约开始时间 |
| duration         | number | 预约时长 |
| partiton         | string | 预约分区 |
| craned_regex     | string | 预约节点 |
| res_total        | table | 预约资源信息 |
| res_avail        | table | 预约可用资源信息 |
| res_alloc        | table | 预约已使用资源信息 |
| allowed_accounts | table(string list) | 预约允许的账户 |
| denied_accounts  | table(string list) | 预约拒绝的账户 |
| allowed_users |  table(string list) | 预约允许的用户 |
| denied_users | table(string list) | 预约拒绝的用户 |

**资源table**

- cpu_core_limit: cpu限制
- memory_limit_bytes: 内存限制
- memory_sw_limit_bytes: swap限制

## Lua 脚本配置

### /etc/crane/config.yaml

```yaml
JobSubmitLuaScript: /path/to/your/job_submit.lua
```

## Lua脚本样例

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