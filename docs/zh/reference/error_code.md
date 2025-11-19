# 错误码参考

本文档提供 CraneSched 系统中所有错误码的详细说明。

## 错误码列表

| 错误ID | 错误码 | 预期上报错误文本 | 触发条件/错误原因 | 提示普通用户解决方案 | 备注 |
|--------|--------|------------------|-------------------|---------------------|------|
| 1 | ERR_INVALID_UID | The user UID being operated on does not exist in the system | 尝试操作的用户 UID 在系统中未注册或已被删除 | 确认账户状态,检查UID | |
| 2 | ERR_INVALID_OP_USER | You are not a user of Crane | 非鹤思用户执行鹤思命令 | 联系管理员将您添加到 Crane 系统中 | 在开启加密的情况下,目前暂不会有这个错误码上报。直接证书鉴权失败,没有json结果,基于错误流上报错误信息 |
| 3 | ERR_INVALID_USER | The entered user is not a user of Crane | 输入的用户不是Crane的用户 | 检查用户名拼写是否正确 | |
| 4 | ERR_PERMISSION_USER | Your permission is insufficient | 低权限用户操作高权限用户 | 联系管理员申请相应权限或请管理员代为执行操作 | |
| 5 | ERR_BLOCKED_USER | The user has been blocked | 该用户已被封禁 | 联系管理员了解封禁原因并申请解封 | |
| 6 | ERR_USER_ALREADY_EXISTS | The user already exists in this account | 添加重复用户 | \ | |
| 7 | ERR_USER_ACCESS_TO_ACCOUNT_DENIED | The user is not allowed to access account | 用户无权访增删改查账号 | 查看自己可用的账户列表 | 建议修改为"ERR_USER_OPERATE_ACCOUNT_DENIED":"The user has no permission to operate the account." |
| 8 | ERR_INVALID_ADMIN_LEVEL | Unknown admin level | 设置用户权限非法,不识别 | \ | |
| 9 | ERR_USER_ACCOUNT_MISMATCH | The user does not belong to this account | 被操作用户和账号不匹配 | 确认使用正确的账户名称 | |
| 10 | ERR_NO_ACCOUNT_SPECIFIED | No account is specified for the user | 未指定有效账号 | 联系管理员处理 | |
| 11 | ERR_INVALID_ACCOUNT | The entered account does not exist | 指定的账号不存在 | 查看可用账户列表 | |
| 12 | ERR_ACCOUNT_ALREADY_EXISTS | The parent account of the entered account does not exist | 账号已存在 | 联系管理员处理 | |
| 13 | ERR_INVALID_PARENT_ACCOUNT | The account already exists in the crane | 账号的父账号不存在 | 联系管理员处理 | |
| 14 | ERR_ACCOUNT_HAS_CHILDREN | The account has child account or users, unable to delete. | 账号存在子账号或用户 | 联系管理员处理 | |
| 15 | ERR_BLOCKED_ACCOUNT | The account has been blocked | 账号被禁 | 联系管理员了解账户封禁原因 | |
| 16 | ERR_INVALID_PARTITION | The entered partition does not exist | 分区不存在 | 使用 cinfo 或 sinfo 查看可用分区列表 | |
| 17 | ERR_PARTITION_MISSING | The entered account or user does not include this partition | 账号或用户没有对应分区 | 使用已授权的分区,或联系管理员申请分区访问权限 | |
| 18 | ERR_PARTITION_ALREADY_EXISTS | The partition already exists in the account or user | 分区在用户或账号中已存在 | \ | |
| 19 | ERR_PARENT_ACCOUNT_PARTITION_MISSING | Parent account does not include the partition | 父账号不包含这个分区 | \ | |
| 20 | ERR_USER_EMPTY_PARTITION | The user does not contain any partitions, operation cannot be performed. | 用户缺少分区,无法添加qos | 联系管理员先为用户分配至少一个分区 | 建议修改为:"Add QoS dependent partition. This user does not have a partition yet. You need to add a partition for this user first." |
| 21 | ERR_CHILD_HAS_PARTITION | The partition is currently being used by the child accounts or users of the account, operation cannot be performed. You can use a forced operation to ignore this constraint | 该分区当前正被子账户或账户用户使用,无法执行操作。您可以使用强制操作来忽略此限制 | 联系管理员 | |
| 22 | ERR_HAS_NO_QOS_IN_PARTITION | The user has no QoS available for this partition to be used | 提交作业时指定的分区无可用 QoS | 联系管理员为该分区分配 QoS | |
| 23 | ERR_HAS_ALLOWED_QOS_IN_PARTITION | The qos you set is not in partition's allowed qos list | 指定的 QoS 未被分区允许使用 | 选择分区允许的 QoS | |
| 24 | ERR_INVALID_QOS | The entered qos does not exist | qos不存在 | 使用 cacctmgr show qos 查看可用 QoS 列表 | |
| 25 | ERR_DB_QOS_ALREADY_EXISTS | Qos already exists in the crane | qos已存在 | \ | |
| 26 | ERR_QOS_REFERENCES_EXIST | QoS is still being used by accounts or users, unable to delete | qos正在被用户或账号使用 | \ | |
| 27 | ERR_CONVERT_TO_INTEGER | Failed to convert value to integer | 参数值格式错误,期望整数但提供了非数字值 | 检查参数值格式,确保提供有效整数 | |
| 28 | ERR_TIME_LIMIT | Invalid time limit value | 时间格式非法 | 使用正确的时间格式(如 HH:MM:SS 或 days-HH:MM:SS) | |
| 29 | ERR_QOS_MISSING | The entered account or user does not include this qos | 账号不包含该qos | \ | |
| 30 | ERR_QOS_ALREADY_EXISTS | The Qos already exists in the account or user | qos已经在账号或用户中 | \ | |
| 31 | ERR_PARENT_ACCOUNT_QOS_MISSING | Parent account does not include the qos | 父账号没有该qos | \ | |
| 32 | ERR_SET_ALLOWED_QOS | The entered QoS list does not include the default QoS for this user. Ignoring this constraint with forced operation, the default QoS is randomly replaced with one of the items in the new QoS list | 输入的QoS列表中未包含此用户的默认QoS。若强制操作忽略此约束,则默认QoS将被新QoS列表中的一项随机替换 | \ | 建议修改为:"The entered QoS list does not include the current default QoS of the user. The default QoS will be randomly selected again, which will overwrite the original default QoS. Ignoring this constraint with --force/-f" |
| 33 | ERR_DEFAULT_QOS_NOT_INHERITED | The entered default_qos is not allowed | 尝试设置的默认 QoS 不在允许 QoS 列表中 | \ | |
| 34 | ERR_DUPLICATE_DEFAULT_QOS | The QoS is already the default QoS for the account or specified partition of the user | 该QoS已经是用户账户或指定分区的默认QoS | \ | |
| 35 | ERR_CHILD_HAS_DEFAULT_QOS | Some child accounts or users is using the QoS as the default QoS. By ignoring this constraint with forced deletion, the deleted default QoS is randomly replaced with one of the remaining items in the QoS list | 输入的QoS列表中未包含此账户或某些子节点的默认QoS。您可以使用强制操作来忽略此限制 | \ | 建议改成"QOS is being used as the default QOS for sub accounts or users. Deleting it will cause the default QOS to be randomly reselected. Ignoring this constraint with --force/-f" |
| 36 | ERR_SET_ACCOUNT_QOS | The entered QoS list does not include the default QoS for this account or some descendant node. You can use a forced operation to ignore this constraint | 某些子账户或用户正在使用该QoS作为默认QoS。如果强制删除时忽略此约束,则被删除的默认QoS将被QoS列表中的剩余项之一随机替换 | \ | 建议修改为:"The entered QoS list does not include the current default QoS of the account. The default QoS will be randomly selected again, which will overwrite the original default QoS. Ignoring this constraint with --force/-f" |
| 37 | ERR_SET_DEFAULT_QOS | The Qos not allowed or is already the default qos | 设置的默认qos已经是当前默认qos | \ | |
| 38 | ERR_DEFAULT_QOS_MODIFICATION_DENIED | The QoS is the default QoS for the current user/Account and cannot be modified. Ignoring this constraint with forced operation, the default QoS is randomly replaced with one of the items in the new QoS list | QoS是当前用户/账户的默认QoS,且无法修改。若强制操作忽略此限制,则默认QoS将随机替换为新QoS列表中的一项 | \ | 建议改成"QOS is being used as the default QOS for current accounts or users. Deleting it will cause the default QOS to be randomly reselected. Ignoring this constraint with --force/-f" |
| 39 | ERR_UPDATE_DATABASE | Fail to update data in database | 更新数据库中的数据失败 | | |
| 40 | ERR_GENERIC_FAILURE | Generic failure | | | 需要明确 |
| 41 | ERR_NO_RESOURCE | Resource not enough for task | 提交任务,但任务所需运行资源不足 | 自行修改任务所需资源或联系管理员根据实际情况调整 | 和ERR_NO_ENOUGH_NODE的错误文本写反了,需要改过来 |
| 42 | ERR_NON_EXISTENT | The object doesn't exist | 对象不存在 | | |
| 43 | ERR_INVALID_NODE_NUM | Nodes partition not enough for task | 任务需要的节点数多于分区的节点数 | 联系管理员根据实际情况修改分区节点或者自行修改任务要求的节点数 | |
| 44 | ERR_INVALID_NODE_LIST | Invalid node list | 节点的名称非法 | 联系管理员修改节点名称 | |
| 45 | ERR_INVALID_EX_NODE_LIST | Invalid exclude node list | 节点的名称非法 | 联系管理员修改节点名称 | |
| 46 | ERR_TIME_TIMIT_BEYOND | Time-limit reached the user's limit | 设定的时间限制超出了用户的配置所规定的范围 | 修改时间限制数值或是联系管理员修改用户配置 | |
| 47 | ERR_CPUS_PER_TASK_BEYOND | cpus-per-task reached the user's limit | 当前运行任务使用cpu核数达到用户配置上限 | 联系管理员根据实际情况调整用户配置或自行修改任务配置 | 建议改成"new task and the currently running tasks have used the total count of CPU cores, reach the limit of user in QOS" |
| 48 | ERR_NO_ENOUGH_NODE | Nodes num not enough for task | 提交任务申请节点或核心数或内存数受限 | 联系管理员根据实际情况调整节点配置 | 和ERR_NO_RESOURCE的错误文本写反了,需要改过来 |
| 49 | ERR_SYSTEM_ERR | Linux Error | 系统错误 | | |
| 50 | ERR_EXISTING_TASK | | | | 暂无用途 |
| 51 | ERR_BEYOND_TASK_ID | System error occurred or the number of pending tasks exceeded maximum value | 等待中的任务数超过了设定的最大数或是系统错误 | 如果是任务数超出限制,考虑取消一部分任务或者是等待任务执行完毕 | |
| 52 | ERR_INVALID_PARAM | Invalid Parameter | 目前是非reservation指定用户,提交了reservation作业会报这个错误,还有其他场景吗? | | 待明确触发条件和场景。需要补充错误描述信息 |
| 53 | ERR_STOP | | | | 暂无用途 |
| 54 | ERR_PERMISSION_DENIED | | | | 暂无用途 |
| 55 | ERR_CONNECTION_TIMEOUT | | | | 暂无用途 |
| 56 | ERR_CONNECTION_ABORTED | | | | 暂无用途 |
| 57 | ERR_RPC_FAILURE | RPC call failed | rpc call 出错 | | |
| 58 | ERR_TOKEN_REQUEST_FAILURE | | | | 暂无用途 |
| 59 | ERR_STREAM_BROKEN | | | | 暂无用途 |
| 60 | ERR_INVALID_STUB | | | | 暂无用途 |
| 61 | ERR_CGROUP | Error when manipulating cgroup | 使用cgroup出错 | | |
| 62 | ERR_PROTOBUF | Error when using protobuf | protobuf出错 | | |
| 63 | ERR_LIB_EVENT | | | | 暂无用途 |
| 64 | ERR_NO_AVAIL_NODE | | | | 暂无用途 |
| 65 | ERR_MAX_JOB_COUNT_PER_USER | job max count is empty or exceeds the limit | 当前运行作业数量达到qos中用户允许运行作业数量限制 | 联系管理员提升限制或等待其中一个任务执行完毕 | 建议改成"new task and the currently running tasks number, reach the limit of user in QOS" |
| 66 | ERR_USER_NO_PRIVILEGE | User has insufficient privilege | 用户无权限创建reservation | 联系管理员设置权限 | 是否可以共用ERR_PERMISSION_USER |
| 67 | ERR_NOT_IN_ALLOWED_LIST | The account does not have permission to run jobs in this partition. Please contact the administrator to add it to the allowed list | 用户所在账号不在分区允许的账号列表中 | 联系管理员将用户添加进分区允许的账号列表中 | 建议改成:"The user's account is not in 'AllowedAccounts' of the partition. Please contact the administrator if you need." |
| 68 | ERR_IN_DENIED_LIST | The account has been denied access to this partition. Please contact the security administrator if access is required | 用户所在账号在分区禁止的账号列表中 | 联系管理员将用户从分区禁止的账号列表中移除 | 建议改成:"The user's account is in 'DeniedAccounts' of the partition. Please contact the administrator if you need." |
| 69 | ERR_EBPF | EBPF syscall error | EBPF syscall出错 | | |
| 70 | ERR_SUPERVISOR | Supervisor error | Supervisor内部出错 | | |
| 71 | ERR_SHUTTING_DOWN | | | | 暂无用途 |
| 72 | ERR_SIGN_CERTIFICATE | The user failed to issue the certificate, please contact the administrator for assistance | 签发证书失败 | 联系管理员 | |
| 73 | ERR_DUPLICATE_CERTIFICATE | The certificate has already been issued to the user. If the certificate is lost and needs to be reissued, please contact the administrator for assistance | 已经拥有证书时,尝试为其重新签发证书。 | 联系管理员 | 看起来实际套了两层,报了两次,需要优化成一个。 |
| 74 | ERR_REVOKE_CERTIFICATE | Revocation of the certificate failed, Please check the logs | 吊销用户的证书时失败 | 检查log以查看具体错误 | |
| 75 | ERR_IDENTITY_MISMATCH | User information does not match, unable to submit the task. | 用户身份信息不匹配 | 修改身份信息 | |
| 76 | ERR_NOT_FORCE | You need to set --force for this operation. | 在删除全体用户时没有显式标注--force | 显式标注--force | |
| 77 | ERR_INVALID_USERNAME | Invalid username | 创建新用户时使用"ALL"作为名字 | 取消使用"ALL"作为新用户名字 | |

## 错误码分类

### 用户相关错误 (1-10)
涉及用户身份验证、权限和账户状态的错误。

### 账户相关错误 (11-15)
与账户管理和账户层级结构相关的错误。

### 分区相关错误 (16-21)
分区配置和访问权限相关的错误。

### QoS 相关错误 (22-38)
服务质量配置和管理相关的错误。

### 数据库和系统错误 (39-49)
数据库操作和系统资源相关的错误。

### 任务相关错误 (50-52)
任务提交和执行相关的错误。

### 连接和 RPC 错误 (53-60)
网络连接和远程过程调用相关的错误。

### 组件错误 (61-70)
各系统组件(cgroup、protobuf、eBPF等)相关的错误。

### 证书和安全错误 (71-77)
证书管理和用户身份验证相关的错误。
