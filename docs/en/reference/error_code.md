# Error Code Reference

| Error ID | Error Code | Expected Error Message | Trigger Condition/Error Cause | User Solution | Notes |
|----------|------------|------------------------|-------------------------------|---------------|-------|
| 1 | ERR_INVALID_UID | The user UID being operated on does not exist in the system | The user UID being operated is not registered in the system or has been deleted | Confirm account status and check UID | |
| 2 | ERR_INVALID_OP_USER | You are not a user of Crane | Non-Crane user executes Crane commands | Contact the administrator to add you to the Crane system | When encryption is enabled, this error code is currently not reported. Certificate authentication fails directly without json results, error information is reported via error stream |
| 3 | ERR_INVALID_USER | The entered user is not a user of Crane | The entered user is not a Crane user | Check if the username spelling is correct | |
| 4 | ERR_PERMISSION_USER | Your permission is insufficient | Low-privilege user operates on high-privilege user | Contact administrator to apply for appropriate permissions or ask administrator to execute operations on your behalf | |
| 5 | ERR_BLOCKED_USER | The user has been blocked | The user has been blocked | Contact administrator to understand the reason for blocking and apply for unblocking | |
| 6 | ERR_USER_ALREADY_EXISTS | The user already exists in this account | Adding duplicate user | \ | |
| 7 | ERR_USER_ACCESS_TO_ACCOUNT_DENIED | The user is not allowed to access account | User has no permission to create, delete, modify or query account | View your available account list | Suggest changing to "ERR_USER_OPERATE_ACCOUNT_DENIED":"The user has no permission to operate the account." |
| 8 | ERR_INVALID_ADMIN_LEVEL | Unknown admin level | Setting illegal user permission, not recognized | \ | |
| 9 | ERR_USER_ACCOUNT_MISMATCH | The user does not belong to this account | The operated user and account do not match | Confirm using the correct account name | |
| 10 | ERR_NO_ACCOUNT_SPECIFIED | No account is specified for the user | No valid account specified | Contact administrator | |
| 11 | ERR_INVALID_ACCOUNT | The entered account does not exist | The specified account does not exist | View available account list | |
| 12 | ERR_ACCOUNT_ALREADY_EXISTS | The parent account of the entered account does not exist | Account already exists | Contact administrator | |
| 13 | ERR_INVALID_PARENT_ACCOUNT | The account already exists in the crane | Parent account of the account does not exist | Contact administrator | |
| 14 | ERR_ACCOUNT_HAS_CHILDREN | The account has child account or users, unable to delete. | Account has child accounts or users | Contact administrator | |
| 15 | ERR_BLOCKED_ACCOUNT | The account has been blocked | Account is blocked | Contact administrator to understand the reason for account blocking | |
| 16 | ERR_INVALID_PARTITION | The entered partition does not exist | Partition does not exist | Use cinfo or sinfo to view available partition list | |
| 17 | ERR_PARTITION_MISSING | The entered account or user does not include this partition | Account or user does not have corresponding partition | Use authorized partitions, or contact administrator to apply for partition access permissions | |
| 18 | ERR_PARTITION_ALREADY_EXISTS | The partition already exists in the account or user | Partition already exists in user or account | \ | |
| 19 | ERR_PARENT_ACCOUNT_PARTITION_MISSING | Parent account does not include the partition | Parent account does not include this partition | \ | |
| 20 | ERR_USER_EMPTY_PARTITION | The user does not contain any partitions, operation cannot be performed. | User lacks partition, unable to add qos | Contact administrator to allocate at least one partition for the user first | Suggest changing to: "Add QoS dependent partition. This user does not have a partition yet. You need to add a partition for this user first." |
| 21 | ERR_CHILD_HAS_PARTITION | The partition is currently being used by the child accounts or users of the account, operation cannot be performed. You can use a forced operation to ignore this constraint | The partition is currently being used by child accounts or account users, operation cannot be performed. You can use forced operation to ignore this constraint | Contact administrator | |
| 22 | ERR_HAS_NO_QOS_IN_PARTITION | The user has no QoS available for this partition to be used | No available QoS for the specified partition when submitting job | Contact administrator to allocate QoS for this partition | |
| 23 | ERR_HAS_ALLOWED_QOS_IN_PARTITION | The qos you set is not in partition's allowed qos list | The specified QoS is not allowed by the partition | Select QoS allowed by the partition | |
| 24 | ERR_INVALID_QOS | The entered qos does not exist | QoS does not exist | Use cacctmgr show qos to view available QoS list | |
| 25 | ERR_DB_QOS_ALREADY_EXISTS | Qos already exists in the crane | QoS already exists | \ | |
| 26 | ERR_QOS_REFERENCES_EXIST | QoS is still being used by accounts or users, unable to delete | QoS is being used by users or accounts | \ | |
| 27 | ERR_CONVERT_TO_INTEGER | Failed to convert value to integer | Parameter value format error, expected integer but provided non-numeric value | Check parameter value format, ensure valid integer is provided | |
| 28 | ERR_TIME_LIMIT | Invalid time limit value | Time format is illegal | Use correct time format (e.g., HH:MM:SS or days-HH:MM:SS) | |
| 29 | ERR_QOS_MISSING | The entered account or user does not include this qos | Account does not include the qos | \ | |
| 30 | ERR_QOS_ALREADY_EXISTS | The Qos already exists in the account or user | QoS already exists in account or user | \ | |
| 31 | ERR_PARENT_ACCOUNT_QOS_MISSING | Parent account does not include the qos | Parent account does not have the qos | \ | |
| 32 | ERR_SET_ALLOWED_QOS | The entered QoS list does not include the default QoS for this user. Ignoring this constraint with forced operation, the default QoS is randomly replaced with one of the items in the new QoS list | The entered QoS list does not include the default QoS for this user. If forced operation ignores this constraint, the default QoS will be randomly replaced with one item in the new QoS list | \ | Suggest changing to: "The entered QoS list does not include the current default QoS of the user. The default QoS will be randomly selected again, which will overwrite the original default QoS. Ignoring this constraint with --force/-f" |
| 33 | ERR_DEFAULT_QOS_NOT_INHERITED | The entered default_qos is not allowed | The default QoS being set is not in the allowed QoS list | \ | |
| 34 | ERR_DUPLICATE_DEFAULT_QOS | The QoS is already the default QoS for the account or specified partition of the user | The QoS is already the default QoS for user's account or specified partition | \ | |
| 35 | ERR_CHILD_HAS_DEFAULT_QOS | Some child accounts or users is using the QoS as the default QoS. By ignoring this constraint with forced deletion, the deleted default QoS is randomly replaced with one of the remaining items in the QoS list | The entered QoS list does not include the default QoS for this account or some child nodes. You can use forced operation to ignore this constraint | \ | Suggest changing to: "QOS is being used as the default QOS for sub accounts or users. Deleting it will cause the default QOS to be randomly reselected. Ignoring this constraint with --force/-f" |
| 36 | ERR_SET_ACCOUNT_QOS | The entered QoS list does not include the default QoS for this account or some descendant node. You can use a forced operation to ignore this constraint | Some child accounts or users are using this QoS as default QoS. If this constraint is ignored during forced deletion, the deleted default QoS will be randomly replaced with one of the remaining items in the QoS list | \ | Suggest changing to: "The entered QoS list does not include the current default QoS of the account. The default QoS will be randomly selected again, which will overwrite the original default QoS. Ignoring this constraint with --force/-f" |
| 37 | ERR_SET_DEFAULT_QOS | The Qos not allowed or is already the default qos | The default qos being set is already the current default qos | \ | |
| 38 | ERR_DEFAULT_QOS_MODIFICATION_DENIED | The QoS is the default QoS for the current user/Account and cannot be modified. Ignoring this constraint with forced operation, the default QoS is randomly replaced with one of the items in the new QoS list | QoS is the default QoS for current user/account and cannot be modified. If forced operation ignores this constraint, the default QoS will be randomly replaced with one item in the new QoS list | \ | Suggest changing to: "QOS is being used as the default QOS for current accounts or users. Deleting it will cause the default QOS to be randomly reselected. Ignoring this constraint with --force/-f" |
| 39 | ERR_UPDATE_DATABASE | Fail to update data in database | Failed to update data in database | | |
| 40 | ERR_GENERIC_FAILURE | Generic failure | | | Needs clarification |
| 41 | ERR_NO_RESOURCE | Resource not enough for task | Submitted task but insufficient resources for task execution | Modify required task resources or contact administrator to adjust according to actual situation | Error text is reversed with ERR_NO_ENOUGH_NODE, needs to be corrected |
| 42 | ERR_NON_EXISTENT | The object doesn't exist | Object does not exist | | |
| 43 | ERR_INVALID_NODE_NUM | Nodes partition not enough for task | Required node count exceeds partition's node count | Contact administrator to modify partition nodes according to actual situation or modify task's required node count | |
| 44 | ERR_INVALID_NODE_LIST | Invalid node list | Node name is illegal | Contact administrator to modify node name | |
| 45 | ERR_INVALID_EX_NODE_LIST | Invalid exclude node list | Node name is illegal | Contact administrator to modify node name | |
| 46 | ERR_TIME_TIMIT_BEYOND | Time-limit reached the user's limit | Set time limit exceeds the range specified by user configuration | Modify time limit value or contact administrator to modify user configuration | |
| 47 | ERR_CPUS_PER_TASK_BEYOND | cpus-per-task reached the user's limit | Current running task CPU core usage reached user configuration limit | Contact administrator to adjust user configuration according to actual situation or modify task configuration | Suggest changing to: "new task and the currently running tasks have used the total count of CPU cores, reach the limit of user in QOS" |
| 48 | ERR_NO_ENOUGH_NODE | Nodes num not enough for task | Submitted task request limited by node count, core count or memory | Contact administrator to adjust node configuration according to actual situation | Error text is reversed with ERR_NO_RESOURCE, needs to be corrected |
| 49 | ERR_SYSTEM_ERR | Linux Error | System error | | |
| 50 | ERR_EXISTING_TASK | | | | Currently unused |
| 51 | ERR_BEYOND_TASK_ID | System error occurred or the number of pending tasks exceeded maximum value | Number of pending tasks exceeded the set maximum or system error | If task count exceeds limit, consider canceling some tasks or waiting for tasks to complete | |
| 52 | ERR_INVALID_PARAM | Invalid Parameter | Currently non-reservation specified user submitted reservation job will report this error, are there other scenarios? | | Trigger conditions and scenarios need to be clarified. Error description needs to be supplemented |
| 53 | ERR_STOP | | | | Currently unused |
| 54 | ERR_PERMISSION_DENIED | | | | Currently unused |
| 55 | ERR_CONNECTION_TIMEOUT | | | | Currently unused |
| 56 | ERR_CONNECTION_ABORTED | | | | Currently unused |
| 57 | ERR_RPC_FAILURE | RPC call failed | RPC call error | | |
| 58 | ERR_TOKEN_REQUEST_FAILURE | | | | Currently unused |
| 59 | ERR_STREAM_BROKEN | | | | Currently unused |
| 60 | ERR_INVALID_STUB | | | | Currently unused |
| 61 | ERR_CGROUP | Error when manipulating cgroup | Error using cgroup | | |
| 62 | ERR_PROTOBUF | Error when using protobuf | Protobuf error | | |
| 63 | ERR_LIB_EVENT | | | | Currently unused |
| 64 | ERR_NO_AVAIL_NODE | | | | Currently unused |
| 65 | ERR_MAX_JOB_COUNT_PER_USER | job max count is empty or exceeds the limit | Current running job count reached the limit of allowed running jobs in QoS | Contact administrator to raise limit or wait for one task to complete | Suggest changing to: "new task and the currently running tasks number, reach the limit of user in QOS" |
| 66 | ERR_USER_NO_PRIVILEGE | User has insufficient privilege | User has no permission to create reservation | Contact administrator to set permissions | Can ERR_PERMISSION_USER be shared? |
| 67 | ERR_NOT_IN_ALLOWED_LIST | The account does not have permission to run jobs in this partition. Please contact the administrator to add it to the allowed list | User's account is not in partition's allowed account list | Contact administrator to add user to partition's allowed account list | Suggest changing to: "The user's account is not in 'AllowedAccounts' of the partition. Please contact the administrator if you need." |
| 68 | ERR_IN_DENIED_LIST | The account has been denied access to this partition. Please contact the security administrator if access is required | User's account is in partition's denied account list | Contact administrator to remove user from partition's denied account list | Suggest changing to: "The user's account is in 'DeniedAccounts' of the partition. Please contact the administrator if you need." |
| 69 | ERR_EBPF | EBPF syscall error | EBPF syscall error | | |
| 70 | ERR_SUPERVISOR | Supervisor error | Supervisor internal error | | |
| 71 | ERR_SHUTTING_DOWN | | | | Currently unused |
| 72 | ERR_SIGN_CERTIFICATE | The user failed to issue the certificate, please contact the administrator for assistance | Certificate signing failed | Contact administrator | |
| 73 | ERR_DUPLICATE_CERTIFICATE | The certificate has already been issued to the user. If the certificate is lost and needs to be reissued, please contact the administrator for assistance | Attempting to reissue certificate when already having one | Contact administrator | Appears to have two layers, reported twice, needs to be optimized to one |
| 74 | ERR_REVOKE_CERTIFICATE | Revocation of the certificate failed, Please check the logs | Revoking user's certificate failed | Check logs to view specific error | |
| 75 | ERR_IDENTITY_MISMATCH | User information does not match, unable to submit the task. | User identity information mismatch | Modify identity information | |
| 76 | ERR_NOT_FORCE | You need to set --force for this operation. | Did not explicitly mark --force when deleting all users | Explicitly mark --force | |
| 77 | ERR_INVALID_USERNAME | Invalid username | Using "ALL" as name when creating new user | Cancel using "ALL" as new username | |
