# Error Code Reference

| Error ID | Error Code | Trigger Condition/Error Cause | User Solution | Expected Error Message | Notes |
|----------|-----------|-------------------------------|---------------|------------------------|-------|
| 1 | `ERR_INVALID_UID` | The user UID being operated is not registered in the system or has been deleted | Confirm account status and check UID | The user UID being operated on does not exist in the system | |
| 2 | `ERR_INVALID_OP_USER` | Non-Crane user executes Crane commands | Contact the administrator to add you to the Crane system | You are not a user of Crane | When encryption is enabled, this error code is currently not reported. Certificate authentication fails directly without json results, error information is reported via error stream |
| 3 | `ERR_INVALID_USER` | The entered user is not a Crane user | Check if the username spelling is correct | The entered user is not a user of Crane | |
| 4 | `ERR_PERMISSION_USER` | Low-privilege user operates on high-privilege user | Contact administrator to apply for appropriate permissions or ask administrator to execute operations on your behalf | Your permission is insufficient | |
| 5 | `ERR_BLOCKED_USER` | The user has been blocked | Contact administrator to understand the reason for blocking and apply for unblocking | The user has been blocked | |
| 6 | `ERR_USER_ALREADY_EXISTS` | Adding duplicate user | \ | The user already exists in this account | |
| 7 | `ERR_USER_ACCESS_TO_ACCOUNT_DENIED` | User has no permission to create, delete, modify or query account | View your available account list | The user is not allowed to access account | Suggest changing to "ERR_USER_OPERATE_ACCOUNT_DENIED":"The user has no permission to operate the account." |
| 8 | `ERR_INVALID_ADMIN_LEVEL` | Setting illegal user permission, not recognized | \ | Unknown admin level | |
| 9 | `ERR_USER_ACCOUNT_MISMATCH` | The operated user and account do not match | Confirm using the correct account name | The user does not belong to this account | |
| 10 | `ERR_NO_ACCOUNT_SPECIFIED` | No valid account specified | Contact administrator | No account is specified for the user | |
| 11 | `ERR_INVALID_ACCOUNT` | The specified account does not exist | View available account list | The entered account does not exist | |
| 12 | `ERR_ACCOUNT_ALREADY_EXISTS` | Account already exists | Contact administrator | The parent account of the entered account does not exist | |
| 13 | `ERR_INVALID_PARENT_ACCOUNT` | Parent account of the account does not exist | Contact administrator | The account already exists in the crane | |
| 14 | `ERR_ACCOUNT_HAS_CHILDREN` | Account has child accounts or users | Contact administrator | The account has child account or users, unable to delete. | |
| 15 | `ERR_BLOCKED_ACCOUNT` | Account is blocked | Contact administrator to understand the reason for account blocking | The account has been blocked | |
| 16 | `ERR_INVALID_PARTITION` | Partition does not exist | Use cinfo or sinfo to view available partition list | The entered partition does not exist | |
| 17 | `ERR_PARTITION_MISSING` | Account or user does not have corresponding partition | Use authorized partitions, or contact administrator to apply for partition access permissions | The entered account or user does not include this partition | |
| 18 | `ERR_PARTITION_ALREADY_EXISTS` | Partition already exists in user or account | \ | The partition already exists in the account or user | |
| 19 | `ERR_PARENT_ACCOUNT_PARTITION_MISSING` | Parent account does not include this partition | \ | Parent account does not include the partition | |
| 20 | `ERR_USER_EMPTY_PARTITION` | User lacks partition, unable to add qos | Contact administrator to allocate at least one partition for the user first | The user does not contain any partitions, operation cannot be performed. | Suggest changing to: "Add QoS dependent partition. This user does not have a partition yet. You need to add a partition for this user first." |
| 21 | `ERR_CHILD_HAS_PARTITION` | The partition is currently being used by child accounts or account users, operation cannot be performed. You can use forced operation to ignore this constraint | Contact administrator | The partition is currently being used by the child accounts or users of the account, operation cannot be performed. You can use a forced operation to ignore this constraint | |
| 22 | `ERR_HAS_NO_QOS_IN_PARTITION` | No available QoS for the specified partition when submitting job | Contact administrator to allocate QoS for this partition | The user has no QoS available for this partition to be used | |
| 23 | `ERR_HAS_ALLOWED_QOS_IN_PARTITION` | The specified QoS is not allowed by the partition | Select QoS allowed by the partition | The qos you set is not in partition's allowed qos list | |
| 24 | `ERR_INVALID_QOS` | QoS does not exist | Use cacctmgr show qos to view available QoS list | The entered qos does not exist | |
| 25 | `ERR_DB_QOS_ALREADY_EXISTS` | QoS already exists | \ | Qos already exists in the crane | |
| 26 | `ERR_QOS_REFERENCES_EXIST` | QoS is being used by users or accounts | \ | QoS is still being used by accounts or users, unable to delete | |
| 27 | `ERR_CONVERT_TO_INTEGER` | Parameter value format error, expected integer but provided non-numeric value | Check parameter value format, ensure valid integer is provided | Failed to convert value to integer | |
| 28 | `ERR_TIME_LIMIT` | Time format is illegal | Use correct time format (e.g., HH:MM:SS or days-HH:MM:SS) | Invalid time limit value | |
| 29 | `ERR_QOS_MISSING` | Account does not include the qos | \ | The entered account or user does not include this qos | |
| 30 | `ERR_QOS_ALREADY_EXISTS` | QoS already exists in account or user | \ | The Qos already exists in the account or user | |
| 31 | `ERR_PARENT_ACCOUNT_QOS_MISSING` | Parent account does not have the qos | \ | Parent account does not include the qos | |
| 32 | `ERR_SET_ALLOWED_QOS` | The entered QoS list does not include the default QoS for this user. If forced operation ignores this constraint, the default QoS will be randomly replaced with one item in the new QoS list | \ | The entered QoS list does not include the default QoS for this user. Ignoring this constraint with forced operation, the default QoS is randomly replaced with one of the items in the new QoS list | Suggest changing to: "The entered QoS list does not include the current default QoS of the user. The default QoS will be randomly selected again, which will overwrite the original default QoS. Ignoring this constraint with --force/-f" |
| 33 | `ERR_DEFAULT_QOS_NOT_INHERITED` | The default QoS being set is not in the allowed QoS list | \ | The entered default_qos is not allowed | |
| 34 | `ERR_DUPLICATE_DEFAULT_QOS` | The QoS is already the default QoS for user's account or specified partition | \ | The QoS is already the default QoS for the account or specified partition of the user | |
| 35 | `ERR_CHILD_HAS_DEFAULT_QOS` | The entered QoS list does not include the default QoS for this account or some child nodes. You can use forced operation to ignore this constraint | \ | Some child accounts or users is using the QoS as the default QoS. By ignoring this constraint with forced deletion, the deleted default QoS is randomly replaced with one of the remaining items in the QoS list | Suggest changing to: "QOS is being used as the default QOS for sub accounts or users. Deleting it will cause the default QOS to be randomly reselected. Ignoring this constraint with --force/-f" |
| 36 | `ERR_SET_ACCOUNT_QOS` | Some child accounts or users are using this QoS as default QoS. If this constraint is ignored during forced deletion, the deleted default QoS will be randomly replaced with one of the remaining items in the QoS list | \ | The entered QoS list does not include the default QoS for this account or some descendant node. You can use a forced operation to ignore this constraint | Suggest changing to: "The entered QoS list does not include the current default QoS of the account. The default QoS will be randomly selected again, which will overwrite the original default QoS. Ignoring this constraint with --force/-f" |
| 37 | `ERR_SET_DEFAULT_QOS` | The default qos being set is already the current default qos | \ | The Qos not allowed or is already the default qos | |
| 38 | `ERR_DEFAULT_QOS_MODIFICATION_DENIED` | QoS is the default QoS for current user/account and cannot be modified. If forced operation ignores this constraint, the default QoS will be randomly replaced with one item in the new QoS list | \ | The QoS is the default QoS for the current user/Account and cannot be modified. Ignoring this constraint with forced operation, the default QoS is randomly replaced with one of the items in the new QoS list | Suggest changing to: "QOS is being used as the default QOS for current accounts or users. Deleting it will cause the default QOS to be randomly reselected. Ignoring this constraint with --force/-f" |
| 39 | `ERR_UPDATE_DATABASE` | Failed to update data in database | | Fail to update data in database | |
| 40 | `ERR_GENERIC_FAILURE` | | | Generic failure | Needs clarification |
| 41 | `ERR_NO_RESOURCE` | Submitted task but insufficient resources for task execution | Modify required task resources or contact administrator to adjust according to actual situation | Resource not enough for task | Error text is reversed with ERR_NO_ENOUGH_NODE, needs to be corrected |
| 42 | `ERR_NON_EXISTENT` | Object does not exist | | The object doesn't exist | |
| 43 | `ERR_INVALID_NODE_NUM` | Required node count exceeds partition's node count | Contact administrator to modify partition nodes according to actual situation or modify task's required node count | Nodes partition not enough for task | |
| 44 | `ERR_INVALID_NODE_LIST` | Node name is illegal | Contact administrator to modify node name | Invalid node list | |
| 45 | `ERR_INVALID_EX_NODE_LIST` | Node name is illegal | Contact administrator to modify node name | Invalid exclude node list | |
| 46 | `ERR_TIME_TIMIT_BEYOND` | Set time limit exceeds the range specified by user configuration | Modify time limit value or contact administrator to modify user configuration | Time-limit reached the user's limit | |
| 47 | `ERR_CPUS_PER_TASK_BEYOND` | Current running task CPU core usage reached user configuration limit | Contact administrator to adjust user configuration according to actual situation or modify task configuration | cpus-per-task reached the user's limit | Suggest changing to: "new task and the currently running tasks have used the total count of CPU cores, reach the limit of user in QOS" |
| 48 | `ERR_NO_ENOUGH_NODE` | Submitted task request limited by node count, core count or memory | Contact administrator to adjust node configuration according to actual situation | Nodes num not enough for task | Error text is reversed with ERR_NO_RESOURCE, needs to be corrected |
| 49 | `ERR_SYSTEM_ERR` | System error | | Linux Error | |
| 50 | `ERR_EXISTING_TASK` | | | | Currently unused |
| 51 | `ERR_BEYOND_TASK_ID` | Number of pending tasks exceeded the set maximum or system error | If task count exceeds limit, consider canceling some tasks or waiting for tasks to complete | System error occurred or the number of pending tasks exceeded maximum value | |
| 52 | `ERR_INVALID_PARAM` | Currently non-reservation specified user submitted reservation job will report this error, are there other scenarios? | | Invalid Parameter | Trigger conditions and scenarios need to be clarified. Error description needs to be supplemented |
| 53 | `ERR_STOP` | | | | Currently unused |
| 54 | `ERR_PERMISSION_DENIED` | | | | Currently unused |
| 55 | `ERR_CONNECTION_TIMEOUT` | | | | Currently unused |
| 56 | `ERR_CONNECTION_ABORTED` | | | | Currently unused |
| 57 | `ERR_RPC_FAILURE` | RPC call error | | RPC call failed | |
| 58 | `ERR_TOKEN_REQUEST_FAILURE` | | | | Currently unused |
| 59 | `ERR_STREAM_BROKEN` | | | | Currently unused |
| 60 | `ERR_INVALID_STUB` | | | | Currently unused |
| 61 | `ERR_CGROUP` | Error using cgroup | | Error when manipulating cgroup | |
| 62 | `ERR_PROTOBUF` | Protobuf error | | Error when using protobuf | |
| 63 | `ERR_LIB_EVENT` | | | | Currently unused |
| 64 | `ERR_NO_AVAIL_NODE` | | | | Currently unused |
| 65 | `ERR_MAX_JOB_COUNT_PER_USER` | Current running job count reached the limit of allowed running jobs in QoS | Contact administrator to raise limit or wait for one task to complete | job max count is empty or exceeds the limit | Suggest changing to: "new task and the currently running tasks number, reach the limit of user in QOS" |
| 66 | `ERR_USER_NO_PRIVILEGE` | User has no permission to create reservation | Contact administrator to set permissions | User has insufficient privilege | Can ERR_PERMISSION_USER be shared? |
| 67 | `ERR_NOT_IN_ALLOWED_LIST` | User's account is not in partition's allowed account list | Contact administrator to add user to partition's allowed account list | The account does not have permission to run jobs in this partition. Please contact the administrator to add it to the allowed list | Suggest changing to: "The user's account is not in 'AllowedAccounts' of the partition. Please contact the administrator if you need." |
| 68 | `ERR_IN_DENIED_LIST` | User's account is in partition's denied account list | Contact administrator to remove user from partition's denied account list | The account has been denied access to this partition. Please contact the security administrator if access is required | Suggest changing to: "The user's account is in 'DeniedAccounts' of the partition. Please contact the administrator if you need." |
| 69 | `ERR_EBPF` | EBPF syscall error | | EBPF syscall error | |
| 70 | `ERR_SUPERVISOR` | Supervisor internal error | | Supervisor error | |
| 71 | `ERR_SHUTTING_DOWN` | | | | Currently unused |
| 72 | `ERR_SIGN_CERTIFICATE` | Certificate signing failed | Contact administrator | The user failed to issue the certificate, please contact the administrator for assistance | |
| 73 | `ERR_DUPLICATE_CERTIFICATE` | Attempting to reissue certificate when already having one | Contact administrator | The certificate has already been issued to the user. If the certificate is lost and needs to be reissued, please contact the administrator for assistance | Appears to have two layers, reported twice, needs to be optimized to one |
| 74 | `ERR_REVOKE_CERTIFICATE` | Revoking user's certificate failed | Check logs to view specific error | Revocation of the certificate failed, Please check the logs | |
| 75 | `ERR_IDENTITY_MISMATCH` | User identity information mismatch | Modify identity information | User information does not match, unable to submit the task. | |
| 76 | `ERR_NOT_FORCE` | Did not explicitly mark --force when deleting all users | Explicitly mark --force | You need to set --force for this operation. | |
| 77 | `ERR_INVALID_USERNAME` | Using "ALL" as name when creating new user | Cancel using "ALL" as new username | Invalid username | |
