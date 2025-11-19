# 错误码参考

| 错误ID | 错误码 | 触发条件/错误原因 | 提示普通用户解决方案 | 预期上报错误文本 |
|--------|--------|-------------------|---------------------|------------------|
| 1 | `ERR_INVALID_UID` | 尝试操作的用户 UID 在系统中未注册或已被删除 | 确认账户状态,检查UID | The user UID being operated on does not exist in the system |
| 2 | `ERR_INVALID_OP_USER` | 非鹤思用户执行鹤思命令 | 联系管理员将您添加到 Crane 系统中 | You are not a user of Crane | 在开启加密的情况下,目前暂不会有这个错误码上报。直接证书鉴权失败,没有json结果,基于错误流上报错误信息 |
| 3 | `ERR_INVALID_USER` | 输入的用户不是Crane的用户 | 检查用户名拼写是否正确 | The entered user is not a user of Crane | |
| 4 | `ERR_PERMISSION_USER` | 低权限用户操作高权限用户 | 联系管理员申请相应权限或请管理员代为执行操作 | Your permission is insufficient | |
| 5 | `ERR_BLOCKED_USER` | 该用户已被封禁 | 联系管理员了解封禁原因并申请解封 | The user has been blocked | |
| 6 | `ERR_USER_ALREADY_EXISTS` | 添加重复用户 | \ | The user already exists in this account | |
| 7 | `ERR_USER_ACCESS_TO_ACCOUNT_DENIED` | 用户无权访增删改查账号 | 查看自己可用的账户列表 | The user is not allowed to access account |
| 8 | `ERR_INVALID_ADMIN_LEVEL` | 设置用户权限非法,不识别 | \ | Unknown admin level |
| 9 | `ERR_USER_ACCOUNT_MISMATCH` | 被操作用户和账号不匹配 | 确认使用正确的账户名称 | The user does not belong to this account | |
| 10 | `ERR_NO_ACCOUNT_SPECIFIED` | 未指定有效账号 | 联系管理员处理 | No account is specified for the user | |
| 11 | `ERR_INVALID_ACCOUNT` | 指定的账号不存在 | 查看可用账户列表 | The entered account does not exist | |
| 12 | `ERR_ACCOUNT_ALREADY_EXISTS` | 账号已存在 | 联系管理员处理 | The parent account of the entered account does not exist |
| 13 | `ERR_INVALID_PARENT_ACCOUNT` | 账号的父账号不存在 | 联系管理员处理 | The account already exists in the crane |
| 14 | `ERR_ACCOUNT_HAS_CHILDREN` | 账号存在子账号或用户 | 联系管理员处理 | The account has child account or users, unable to delete. |
| 15 | `ERR_BLOCKED_ACCOUNT` | 账号被禁 | 联系管理员了解账户封禁原因 | The account has been blocked |
| 16 | `ERR_INVALID_PARTITION` | 分区不存在 | 使用 cinfo 或 sinfo 查看可用分区列表 | The entered partition does not exist | |
| 17 | `ERR_PARTITION_MISSING` | 账号或用户没有对应分区 | 使用已授权的分区,或联系管理员申请分区访问权限 | The entered account or user does not include this partition | |
| 18 | `ERR_PARTITION_ALREADY_EXISTS` | 分区在用户或账号中已存在 | \ | The partition already exists in the account or user |
| 19 | `ERR_PARENT_ACCOUNT_PARTITION_MISSING` | 父账号不包含这个分区 | \ | Parent account does not include the partition |
| 20 | `ERR_USER_EMPTY_PARTITION` | 用户缺少分区,无法添加qos | 联系管理员先为用户分配至少一个分区 | The user does not contain any partitions, operation cannot be performed. |
| 21 | `ERR_CHILD_HAS_PARTITION` | 该分区当前正被子账户或账户用户使用,无法执行操作。您可以使用强制操作来忽略此限制 | 联系管理员 | The partition is currently being used by the child accounts or users of the account, operation cannot be performed. You can use a forced operation to ignore this constraint |
| 22 | `ERR_HAS_NO_QOS_IN_PARTITION` | 提交作业时指定的分区无可用 QoS | 联系管理员为该分区分配 QoS | The user has no QoS available for this partition to be used | |
| 23 | `ERR_HAS_ALLOWED_QOS_IN_PARTITION` | 指定的 QoS 未被分区允许使用 | 选择分区允许的 QoS | The qos you set is not in partition's allowed qos list |
| 24 | `ERR_INVALID_QOS` | qos不存在 | 使用 cacctmgr show qos 查看可用 QoS 列表 | The entered qos does not exist | |
| 25 | `ERR_DB_QOS_ALREADY_EXISTS` | qos已存在 | \ | Qos already exists in the crane |
| 26 | `ERR_QOS_REFERENCES_EXIST` | qos正在被用户或账号使用 | \ | QoS is still being used by accounts or users, unable to delete |
| 27 | `ERR_CONVERT_TO_INTEGER` | 参数值格式错误,期望整数但提供了非数字值 | 检查参数值格式,确保提供有效整数 | Failed to convert value to integer | |
| 28 | `ERR_TIME_LIMIT` | 时间格式非法 | 使用正确的时间格式(如 HH:MM:SS 或 days-HH:MM:SS) | Invalid time limit value | |
| 29 | `ERR_QOS_MISSING` | 账号不包含该qos | \ | The entered account or user does not include this qos |
| 30 | `ERR_QOS_ALREADY_EXISTS` | qos已经在账号或用户中 | \ | The Qos already exists in the account or user |
| 31 | `ERR_PARENT_ACCOUNT_QOS_MISSING` | 父账号没有该qos | \ | Parent account does not include the qos |
| 32 | `ERR_SET_ALLOWED_QOS` | 输入的QoS列表中未包含此用户的默认QoS。若强制操作忽略此约束,则默认QoS将被新QoS列表中的一项随机替换 | \ | The entered QoS list does not include the default QoS for this user. Ignoring this constraint with forced operation, the default QoS is randomly replaced with one of the items in the new QoS list |
| 33 | `ERR_DEFAULT_QOS_NOT_INHERITED` | 尝试设置的默认 QoS 不在允许 QoS 列表中 | \ | The entered default_qos is not allowed |
| 34 | `ERR_DUPLICATE_DEFAULT_QOS` | 该QoS已经是用户账户或指定分区的默认QoS | \ | The QoS is already the default QoS for the account or specified partition of the user |
| 35 | `ERR_CHILD_HAS_DEFAULT_QOS` | 输入的QoS列表中未包含此账户或某些子节点的默认QoS。您可以使用强制操作来忽略此限制 | \ | Some child accounts or users is using the QoS as the default QoS. By ignoring this constraint with forced deletion, the deleted default QoS is randomly replaced with one of the remaining items in the QoS list |
| 36 | `ERR_SET_ACCOUNT_QOS` | 某些子账户或用户正在使用该QoS作为默认QoS。如果强制删除时忽略此约束,则被删除的默认QoS将被QoS列表中的剩余项之一随机替换 | \ | The entered QoS list does not include the default QoS for this account or some descendant node. You can use a forced operation to ignore this constraint |
| 37 | `ERR_SET_DEFAULT_QOS` | 设置的默认qos已经是当前默认qos | \ | The Qos not allowed or is already the default qos |
| 38 | `ERR_DEFAULT_QOS_MODIFICATION_DENIED` | QoS是当前用户/账户的默认QoS,且无法修改。若强制操作忽略此限制,则默认QoS将随机替换为新QoS列表中的一项 | \ | The QoS is the default QoS for the current user/Account and cannot be modified. Ignoring this constraint with forced operation, the default QoS is randomly replaced with one of the items in the new QoS list |
| 39 | `ERR_UPDATE_DATABASE` | 更新数据库中的数据失败 | | Fail to update data in database |
| 40 | `ERR_GENERIC_FAILURE` | | | Generic failure |
| 41 | `ERR_NO_RESOURCE` | 提交任务,但任务所需运行资源不足 | 自行修改任务所需资源或联系管理员根据实际情况调整 | Resource not enough for task |
| 42 | `ERR_NON_EXISTENT` | 对象不存在 | | The object doesn't exist |
| 43 | `ERR_INVALID_NODE_NUM` | 任务需要的节点数多于分区的节点数 | 联系管理员根据实际情况修改分区节点或者自行修改任务要求的节点数 | Nodes partition not enough for task |
| 44 | `ERR_INVALID_NODE_LIST` | 节点的名称非法 | 联系管理员修改节点名称 | Invalid node list |
| 45 | `ERR_INVALID_EX_NODE_LIST` | 节点的名称非法 | 联系管理员修改节点名称 | Invalid exclude node list |
| 46 | `ERR_TIME_TIMIT_BEYOND` | 设定的时间限制超出了用户的配置所规定的范围 | 修改时间限制数值或是联系管理员修改用户配置 | Time-limit reached the user's limit |
| 47 | `ERR_CPUS_PER_TASK_BEYOND` | 当前运行任务使用cpu核数达到用户配置上限 | 联系管理员根据实际情况调整用户配置或自行修改任务配置 | cpus-per-task reached the user's limit |
| 48 | `ERR_NO_ENOUGH_NODE` | 提交任务申请节点或核心数或内存数受限 | 联系管理员根据实际情况调整节点配置 | Nodes num not enough for task |
| 49 | `ERR_SYSTEM_ERR` | 系统错误 | | Linux Error |
| 50 | `ERR_EXISTING_TASK` | | | |
| 51 | `ERR_BEYOND_TASK_ID` | 等待中的任务数超过了设定的最大数或是系统错误 | 如果是任务数超出限制,考虑取消一部分任务或者是等待任务执行完毕 | System error occurred or the number of pending tasks exceeded maximum value |
| 52 | `ERR_INVALID_PARAM` | 目前是非reservation指定用户,提交了reservation作业会报这个错误,还有其他场景吗? | | Invalid Parameter |
| 53 | `ERR_STOP` | | | |
| 54 | `ERR_PERMISSION_DENIED` | | | |
| 55 | `ERR_CONNECTION_TIMEOUT` | | | |
| 56 | `ERR_CONNECTION_ABORTED` | | | |
| 57 | `ERR_RPC_FAILURE` | rpc call 出错 | | RPC call failed |
| 58 | `ERR_TOKEN_REQUEST_FAILURE` | | | |
| 59 | `ERR_STREAM_BROKEN` | | | |
| 60 | `ERR_INVALID_STUB` | | | |
| 61 | `ERR_CGROUP` | 使用cgroup出错 | | Error when manipulating cgroup |
| 62 | `ERR_PROTOBUF` | protobuf出错 | | Error when using protobuf |
| 63 | `ERR_LIB_EVENT` | | | |
| 64 | `ERR_NO_AVAIL_NODE` | | | |
| 65 | `ERR_MAX_JOB_COUNT_PER_USER` | 当前运行作业数量达到qos中用户允许运行作业数量限制 | 联系管理员提升限制或等待其中一个任务执行完毕 | job max count is empty or exceeds the limit |
| 66 | `ERR_USER_NO_PRIVILEGE` | 用户无权限创建reservation | 联系管理员设置权限 | User has insufficient privilege |
| 67 | `ERR_NOT_IN_ALLOWED_LIST` | 用户所在账号不在分区允许的账号列表中 | 联系管理员将用户添加进分区允许的账号列表中 | The account does not have permission to run jobs in this partition. Please contact the administrator to add it to the allowed list |
| 68 | `ERR_IN_DENIED_LIST` | 用户所在账号在分区禁止的账号列表中 | 联系管理员将用户从分区禁止的账号列表中移除 | The account has been denied access to this partition. Please contact the security administrator if access is required |
| 69 | `ERR_EBPF` | EBPF syscall出错 | | EBPF syscall error |
| 70 | `ERR_SUPERVISOR` | Supervisor内部出错 | | Supervisor error |
| 71 | `ERR_SHUTTING_DOWN` | | | |
| 72 | `ERR_SIGN_CERTIFICATE` | 签发证书失败 | 联系管理员 | The user failed to issue the certificate, please contact the administrator for assistance |
| 73 | `ERR_DUPLICATE_CERTIFICATE` | 已经拥有证书时,尝试为其重新签发证书。 | 联系管理员 | The certificate has already been issued to the user. If the certificate is lost and needs to be reissued, please contact the administrator for assistance |
| 74 | `ERR_REVOKE_CERTIFICATE` | 吊销用户的证书时失败 | 检查log以查看具体错误 | Revocation of the certificate failed, Please check the logs |
| 75 | `ERR_IDENTITY_MISMATCH` | 用户身份信息不匹配 | 修改身份信息 | User information does not match, unable to submit the task. |
| 76 | `ERR_NOT_FORCE` | 在删除全体用户时没有显式标注--force | 显式标注--force | You need to set --force for this operation. |
| 77 | `ERR_INVALID_USERNAME` | 创建新用户时使用"ALL"作为名字 | 取消使用"ALL"作为新用户名字 | Invalid username |
