# cacctmgr - 账户管理器

**cacctmgr 使用 SQL 风格的命令管理鹤思系统中的账户、用户和服务质量（QoS）设置。**

!!! note
    此命令使用 SQL 风格的语法来管理集群资源。语法为：`cacctmgr <操作> <实体> [选项]`

## 用户角色

鹤思系统有四个用户权限级别:

- **Admin（系统管理员）**：通常是 root 用户，对任何账户或用户拥有完全的创建、读取、更新和删除权限
- **Operator（平台管理员）**：对账户系统拥有完全权限
- **Coordinator（账户调度员）**：对同一账户内的用户和子账户拥有权限，包括添加用户
- **None（普通用户）**：只能查询自己账户内的信息，不能修改用户或账户信息

## 命令语法

```bash
cacctmgr <操作> <实体> [ID] [选项]
```

### 全局选项

- **-h, --help**: 显示帮助信息
- **-C, --config string**: 配置文件路径（默认：`/etc/crane/config.yaml`）
- **-v, --version**: 显示 cacctmgr 版本
- **-J, --json**: 以 JSON 格式输出
- **-f, --force**: 强制执行操作，不需要确认

## 操作

- **add**: 创建新的账户、用户或 QoS
- **delete**: 删除账户、用户或 QoS
- **block**: 阻止账户或用户使用系统
- **unblock**: 解除先前阻止的账户或用户
- **modify**: 使用 SQL 风格的 SET 子句修改属性
- **show**: 显示实体信息
- **reset**: 重置用户证书

## 实体

- **account**: 系统中的用户账户
- **user**: 个人用户
- **qos**: 服务质量设置
- **transaction**: 事务日志（仅用于 show 操作）

---

## 1. 账户管理

### 1.1 添加账户

**语法：**
```bash
cacctmgr add account <名称> [选项]
```

**选项：**

- **Description=<描述>**: 账户描述
- **Parent=<父账户>**: 父账户名称
- **DefaultQos=<qos>**: 账户的默认 QoS
- **Partition=<分区1,分区2,...>**: 允许的分区（逗号分隔）
- **QosList=<qos1,qos2,...>**: 允许的 QoS 列表（逗号分隔）
- **Name=<名称1,名称2,...>**: 批量创建多个账户（逗号分隔）

**示例：**

创建简单账户：
```bash
cacctmgr add account PKU Description="北京大学" Partition=CPU,GPU QosList=normal,high
```

创建子账户：
```bash
cacctmgr add account ComputingCentre Description="计算中心" Parent=PKU
```

批量创建账户：
```bash
cacctmgr add account dept Name=CS,Math,Physics Parent=PKU
```

### 1.2 删除账户

**语法：**
```bash
cacctmgr delete account <名称>
```

**注意：** 不能删除还有子账户或用户的账户。需要先删除它们。

**示例：**
```bash
cacctmgr delete account ComputingCentre
```

### 1.3 修改账户

**语法：**
```bash
cacctmgr modify account where Name=<账户> set <属性>=<值>
```

**属性：**

- **Description=<描述>**: 设置描述
- **DefaultQos=<qos>**: 设置默认 QoS
- **AllowedPartition=<分区>**: 设置允许的分区（覆盖）
- **AllowedPartition+=<分区>**: 向允许列表添加分区
- **AllowedPartition-=<分区>**: 从允许列表移除分区
- **AllowedQos=<qos列表>**: 设置允许的 QoS 列表（覆盖）
- **AllowedQos+=<qos列表>**: 向允许列表添加 QoS
- **AllowedQos-=<qos列表>**: 从允许列表移除 QoS

**示例：**

修改账户描述：
```bash
cacctmgr modify account where Name=ComputingCentre set Description="HPC计算中心"
```

向允许列表添加分区：
```bash
cacctmgr modify account where Name=PKU set AllowedPartition+=GPU2,GPU3
```

从允许列表移除分区：
```bash
cacctmgr modify account where Name=PKU set AllowedPartition-=GPU
```

设置允许的分区（替换现有）：
```bash
cacctmgr modify account where Name=PKU set AllowedPartition=CPU,GPU
```

### 1.4 显示账户

**语法：**
```bash
cacctmgr show account [名称] [选项]
```

**选项：**

- **Name=<名称1,名称2,...>**: 仅显示特定账户

**示例：**

显示所有账户：
```bash
cacctmgr show account
```

显示特定账户：
```bash
cacctmgr show account PKU
```

显示多个账户：
```bash
cacctmgr show account Name=PKU,ComputingCentre
```

### 1.5 阻止/解除阻止账户

**语法：**
```bash
cacctmgr block account <名称>
cacctmgr unblock account <名称>
```

**示例：**

阻止账户：
```bash
cacctmgr block account ComputingCentre
```

解除阻止账户：
```bash
cacctmgr unblock account ComputingCentre
```

---

## 2. 用户管理

### 2.1 添加用户

**语法：**
```bash
cacctmgr add user <名称> Account=<账户> [选项]
```

**必需：**

- **Account=<账户>**: 用户所属的账户（必需）

**选项：**

- **Coordinator=true|false**: 将用户设置为账户调度员
- **Level=<级别>**: 用户管理级别（none/operator/admin，默认：none）
- **Partition=<分区1,分区2,...>**: 允许的分区（逗号分隔）
- **Name=<名称1,名称2,...>**: 批量创建多个用户（逗号分隔）

**注意：** 用户必须作为 Linux 系统用户存在（先使用 `useradd` 创建）。

**示例：**

创建简单用户：
```bash
useradd alice
cacctmgr add user alice Account=PKU
```

创建具有特定权限的用户：
```bash
useradd bob
cacctmgr add user bob Account=PKU Level=operator Partition=CPU,GPU
```

创建调度员用户：
```bash
useradd charlie
cacctmgr add user charlie Account=PKU Coordinator=true
```

批量创建用户：
```bash
useradd user1 && useradd user2 && useradd user3
cacctmgr add user batch Account=PKU Name=user1,user2,user3
```

### 2.2 删除用户

**语法：**
```bash
cacctmgr delete user <名称> [Account=<账户>]
```

**选项：**

- **Account=<账户>**: 指定账户上下文（可选）
- **Name=<名称1,名称2,...>**: 删除多个用户（逗号分隔）

**特殊情况：**
- 如果名称是 `ALL` 且设置了 `--force`，将删除指定账户的所有用户

**示例：**

删除用户：
```bash
cacctmgr delete user alice
```

从特定账户删除用户：
```bash
cacctmgr delete user bob Account=PKU
```

删除账户的所有用户（需要强制）：
```bash
cacctmgr delete user ALL Account=PKU --force
```

### 2.3 修改用户

**语法：**
```bash
cacctmgr modify user where Name=<用户> [Account=<账户>] [Partition=<分区>] set <属性>=<值>
```

**Where 子句：**

- **Name=<用户>**: 要修改的用户（必需）
- **Account=<账户>**: 账户上下文（可选）
- **Partition=<分区>**: 分区上下文（可选）

**属性：**

- **AdminLevel=<级别>**: 设置管理级别（none/operator/admin）
- **DefaultAccount=<账户>**: 设置默认账户
- **DefaultQos=<qos>**: 设置默认 QoS
- **AllowedPartition=<分区>**: 设置允许的分区（覆盖）
- **AllowedPartition+=<分区>**: 添加分区
- **AllowedPartition-=<分区>**: 移除分区
- **AllowedQos=<qos列表>**: 设置允许的 QoS（覆盖）
- **AllowedQos+=<qos列表>**: 添加 QoS
- **AllowedQos-=<qos列表>**: 移除 QoS

**示例：**

修改用户管理级别：
```bash
cacctmgr modify user where Name=alice Account=PKU set AdminLevel=operator
```

从用户移除分区：
```bash
cacctmgr modify user where Name=bob set AllowedPartition-=GPU
```

向用户添加 QoS：
```bash
cacctmgr modify user where Name=charlie set AllowedQos+=high
```

### 2.4 显示用户

**语法：**
```bash
cacctmgr show user [名称] [选项]
```

**选项：**

- **Accounts=<账户>**: 仅显示特定账户的用户
- **Name=<名称1,名称2,...>**: 仅显示特定用户

**示例：**

显示所有用户：
```bash
cacctmgr show user
```

显示特定用户：
```bash
cacctmgr show user alice
```

显示账户中的用户：
```bash
cacctmgr show user Accounts=PKU
```

### 2.5 阻止/解除阻止用户

**语法：**
```bash
cacctmgr block user <名称> Account=<账户>
cacctmgr unblock user <名称> Account=<账户>
```

**必需：**

- **Account=<账户>**: 用户阻止/解除阻止需要账户上下文

**示例：**

阻止用户：
```bash
cacctmgr block user alice Account=PKU
```

解除阻止用户：
```bash
cacctmgr unblock user alice Account=PKU
```

### 2.6 重置用户证书

**语法：**
```bash
cacctmgr reset <名称>
```

**特殊情况：**
- 如果名称是 `all`，将重置所有用户的证书

**示例：**

重置单个用户证书：
```bash
cacctmgr reset alice
```

重置所有用户证书：
```bash
cacctmgr reset all
```

---

## 3. QoS 管理

### 3.1 添加 QoS

**语法：**
```bash
cacctmgr add qos <名称> [选项]
```

**选项：**

- **Description=<描述>**: QoS 描述
- **Priority=<优先级>**: 优先级值（值越高，优先级越高）
- **MaxJobsPerUser=<数量>**: 每个用户的最大并发作业数
- **MaxCpusPerUser=<数量>**: 每个用户的最大 CPU 数
- **MaxTimeLimitPerTask=<秒数>**: 每个任务的最大运行时间（秒）
- **Name=<名称1,名称2,...>**: 批量创建多个 QoS（逗号分隔）

**示例：**

创建 QoS：
```bash
cacctmgr add qos normal Description="普通QoS" Priority=1000 MaxJobsPerUser=10 MaxCpusPerUser=100
```

创建高优先级 QoS：
```bash
cacctmgr add qos high Description="高优先级" Priority=5000 MaxJobsPerUser=20 MaxCpusPerUser=200 MaxTimeLimitPerTask=86400
```

批量创建 QoS：
```bash
cacctmgr add qos batch Name=low,medium,high Priority=500
```

### 3.2 删除 QoS

**语法：**
```bash
cacctmgr delete qos <名称>
```

**选项：**

- **Name=<名称1,名称2,...>**: 删除多个 QoS（逗号分隔）

**示例：**
```bash
cacctmgr delete qos low
```

### 3.3 修改 QoS

**语法：**
```bash
cacctmgr modify qos where Name=<qos> set <属性>=<值>
```

**属性：**

- **Description=<描述>**: 设置描述
- **MaxCpusPerUser=<数量>**: 设置每个用户的最大 CPU 数
- **MaxJobsPerUser=<数量>**: 设置每个用户的最大作业数
- **MaxTimeLimitPerTask=<秒数>**: 设置每个任务的最大时间（秒）
- **Priority=<优先级>**: 设置优先级

**示例：**

修改 QoS 优先级：
```bash
cacctmgr modify qos where Name=normal set Priority=2000
```

更新资源限制：
```bash
cacctmgr modify qos where Name=high set MaxJobsPerUser=50 MaxCpusPerUser=500
```

### 3.4 显示 QoS

**语法：**
```bash
cacctmgr show qos [名称] [选项]
```

**选项：**

- **Name=<名称1,名称2,...>**: 仅显示特定 QoS

**示例：**

显示所有 QoS：
```bash
cacctmgr show qos
```

显示特定 QoS：
```bash
cacctmgr show qos normal
```

---

## 4. 事务日志

### 4.1 显示事务日志

**语法：**
```bash
cacctmgr show transaction where [选项]
```

**Where 选项：**

- **Actor=<用户名>**: 按执行操作的用户过滤
- **Target=<目标>**: 按目标实体过滤
- **Action=<操作>**: 按操作类型过滤
- **Info=<信息>**: 按附加信息过滤
- **StartTime=<时间戳>**: 按开始时间过滤

**示例：**

显示所有事务：
```bash
cacctmgr show transaction
```

显示特定用户的事务：
```bash
cacctmgr show transaction where Actor=admin
```

显示特定账户的事务：
```bash
cacctmgr show transaction where Target=PKU
```

---

## 5. Wckey 管理

### 5.1 添加 Wckey

**语法：**
```bash
cacctmgr add wckey <名称> [选项]
```

**选项：**

- **User=<用户名>**: 用户名 （必需）
- **Cluster=<集群名>**: 集群名 （必需）

**示例：**

添加 Wckey
```bash
cacctmgr add wckey hpcgroup User=zhangsan Cluster=pkuhpc_test
```
### 5.2 删除 Wckey

**语法：**
```bash
cacctmgr delete wckey <名称> [选项]
```

**选项：**

- **User=<用户名>**: 用户名 （必需）
- **Cluster=<集群名>**: 集群名 （必需）

**示例：**
```bash
cacctmgr delete wckey hpcgroup User=zhangsan Cluster=pkuhpc_test
```

### 5.3 修改默认Wckey

**语法：**
```bash
cacctmgr modify wckey where User=<name> Cluster=<cluster> set <DefaultWckey>=<wckey>
```

**属性：**

- **User=<用户名>**: 用户名 （必需）
- **Cluster=<集群名>**: 集群名 （必需）
- **DefaultWckey=<Wckey名>**: 存在的wckey名 （必需）

**示例：**

修改默认Wckey
```bash
cacctmgr modify wckey where User=zhangsan Cluster=pkuhpc_test set DefaultWckey=hpcgroup
```

### 5.4 显示 Wckey

**语法：**
```bash
cacctmgr show wckey [名称]
```

**选项：**

- **Name=<名称1,名称2,...>**: 仅显示特定Wckey（逗号分隔）

**示例：**

显示所有 Wckey
```bash
cacctmgr show wckey
```

## 使用示例

### 完整工作流示例

```bash
# 1. 创建 QoS 策略
cacctmgr add qos standard Description="标准队列" Priority=1000 MaxJobsPerUser=10

# 2. 创建根账户并设置分区和 QoS
cacctmgr add account University Description="大学账户" Partition=CPU,GPU QosList=standard

# 3. 在 University 下创建院系账户
cacctmgr add account CS Description="计算机科学系" Parent=University

# 4. 创建 Linux 用户
useradd professor

# 5. 将用户添加到账户并赋予调度员权限
cacctmgr add user professor Account=CS Level=operator Coordinator=true

# 6. 创建学生用户
useradd student1 && useradd student2
cacctmgr add user batch Account=CS Name=student1,student2

# 7. 限制 student1 只能使用 CPU 分区
cacctmgr modify user where Name=student1 Account=CS set AllowedPartition=CPU

# 8. 查看 CS 账户的所有用户
cacctmgr show user Accounts=CS

# 9. 临时阻止用户
cacctmgr block user student2 Account=CS

# 10. 解除阻止用户
cacctmgr unblock user student2 Account=CS

# 11. 查看事务历史
cacctmgr show transaction where Actor=professor
```

### JSON 输出示例

以 JSON 格式获取结果用于脚本：
```bash
cacctmgr show account --json
cacctmgr show user Accounts=PKU --json
cacctmgr show qos --json
```

---

## 权限矩阵

| 操作 | Admin | Operator | Coordinator | User |
|------|-------|----------|-------------|------|
| 添加账户 | ✓ | ✓ | ✗ | ✗ |
| 删除账户 | ✓ | ✓ | ✗ | ✗ |
| 修改账户 | ✓ | ✓ | 自己的账户 | ✗ |
| 添加用户 | ✓ | ✓ | 同账户 | ✗ |
| 删除用户 | ✓ | ✓ | 同账户 | ✗ |
| 修改用户 | ✓ | ✓ | 同账户 | ✗ |
| 添加 QoS | ✓ | ✓ | ✗ | ✗ |
| 删除 QoS | ✓ | ✓ | ✗ | ✗ |
| 修改 QoS | ✓ | ✓ | ✗ | ✗ |
| 显示（查询） | ✓ | ✓ | ✓ | ✓（自己的账户） |
| 阻止/解除阻止 | ✓ | ✓ | 同账户 | ✗ |

---

## 重要说明

1. **Linux 用户要求**：在将用户添加到 cacctmgr 之前，该用户必须作为 Linux 系统用户存在（使用 `useradd` 创建）

2. **账户层次结构**：删除账户时，请确保该账户下没有子账户或用户

3. **继承**：用户从其父账户继承分区和 QoS 设置，除非明确覆盖

4. **调度员权限**：调度员可以管理其账户内的用户，但不能修改自己账户的父账户

5. **SQL 风格语法**：新语法使用类似 SQL 的 `where` 和 `set` 子句进行修改操作，使用 `+=` 和 `-=` 运算符进行列表修改

## 另请参阅

- [cbatch](cbatch.md) - 提交批处理作业
- [cqueue](cqueue.md) - 查看作业队列
- [cacct](cacct.md) - 查看作业统计信息
