/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package util

import (
	"CraneFrontEnd/generated/protos"
	"errors"
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

type ExitCode = int

// Do not define error code > 127
const (
	ErrorSuccess ExitCode = iota
	ErrorGeneric
	ErrorCmdArg
	ErrorNetwork
	ErrorBackend
	ErrorInvalidFormat
	ErrorSystem
)

type CraneError struct {
	Code    ExitCode
	Message string
	cause   error
}

// CommandExitError carries a user command's exit status separately from
// CraneError so it is not interpreted as a Crane CLI error code.
type CommandExitError struct {
	Code ExitCode
}

func (e *CommandExitError) Error() string {
	return ""
}

func (e *CraneError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return e.Message
}

func (e *CraneError) Unwrap() error {
	return e.cause
}

func NewCraneErr(code ExitCode, message string) *CraneError {
	return &CraneError{
		Code:    code,
		Message: message,
	}
}

func WrapCraneErr(code ExitCode, format string, err error) *CraneError {
	return &CraneError{
		Code:    code,
		Message: fmt.Sprintf(format, err),
		cause:   err,
	}
}

func NewCraneErrFromGrpc(code ExitCode, err error, format string, a ...any) *CraneError {
	msg := fmt.Sprintf(format, a...)
	if rpcErr, ok := grpcstatus.FromError(err); ok {
		switch rpcErr.Code() {
		case grpccodes.Unavailable:
			msg = fmt.Sprintf("%s: Connection to CraneCtld is broken.", msg)
		case grpccodes.Unauthenticated:
			msg = fmt.Sprintf("%s: Access denied.", msg)
		case grpccodes.DeadlineExceeded: // timeout errors
			msg = fmt.Sprintf("%s: Request timeout, please try again or reduce the query scope.", msg)
		case grpccodes.ResourceExhausted: // resource exhaustion/oversized responses
			msg = fmt.Sprintf("%s: Response too large, please reduce the query scope.", msg)
		default:
			msg = fmt.Sprintf("%s: gRPC error code %s.", msg, rpcErr.String())
		}
	}
	return &CraneError{
		Code:    code,
		Message: msg,
		cause:   err,
	}
}

// Silence usage info output when RunE() returns a non-nil error
func RunEWrapperForLeafCommand(cmd *cobra.Command) {
	for _, c := range cmd.Commands() {
		RunEWrapperForLeafCommand(c) // bfs
	}

	if len(cmd.Commands()) == 0 {
		originalRunE := cmd.RunE
		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			if originalRunE != nil {
				err := originalRunE(cmd, args)
				var commandExitErr *CommandExitError
				if errors.As(err, &commandExitErr) {
					cmd.SilenceUsage = true
					cmd.SilenceErrors = true
					return err
				}
				var craneErr *CraneError
				if errors.As(err, &craneErr) {
					if craneErr != nil {
						if craneErr.Error() == "" {
							cmd.SilenceErrors = true
						}
						if craneErr.Code != ErrorCmdArg || craneErr.Message == "" {
							cmd.SilenceUsage = true // Silence usage info output
							cmd.SilenceErrors = true
						}
					}
				}
				return err
			} else if cmd.Run != nil {
				cmd.Run(cmd, args)
			}
			return nil
		}
	}
}

func RunAndHandleExit(cmd *cobra.Command) {
	if err := cmd.Execute(); err != nil {
		var commandExitErr *CommandExitError
		if errors.As(err, &commandExitErr) {
			os.Exit(commandExitErr.Code)
		}
		// Commands that opt out of Cobra's automatic error output use this
		// path as their single error reporting point.
		if cmd.SilenceErrors {
			printError := func(message string) {
				message = strings.TrimSpace(message)
				if message == "" {
					return
				}
				fmt.Fprintln(os.Stderr, fmt.Sprintf("%s: error: %s", cmd.CommandPath(), message))
			}
			var craneErr *CraneError
			if errors.As(err, &craneErr) && craneErr != nil {
				printError(craneErr.Message)
			} else if err.Error() != "" {
				printError(err.Error())
			}
			if craneErr != nil {
				os.Exit(craneErr.Code)
			}
			os.Exit(ErrorGeneric)
		}
		var craneErr *CraneError
		if errors.As(err, &craneErr) {
			if craneErr.Error() != "" && craneErr.Code != ErrorCmdArg {
				log.Error(craneErr)
			}
			os.Exit(craneErr.Code)
		} else {
			os.Exit(ErrorGeneric)
		}
	}
	os.Exit(ErrorSuccess)
}

// gRPC errors
var errMsgMap = map[protos.ErrCode]string{

	// User-related errors
	protos.ErrCode_ERR_INVALID_UID:                   "The user UID being operated on does not exist in the system",
	protos.ErrCode_ERR_INVALID_OP_USER:               "You are not a user of Crane",
	protos.ErrCode_ERR_INVALID_USER:                  "The entered user is not a user of Crane",
	protos.ErrCode_ERR_PERMISSION_USER:               "Your permission is insufficient",
	protos.ErrCode_ERR_BLOCKED_USER:                  "The user has been blocked",
	protos.ErrCode_ERR_USER_ALREADY_EXISTS:           "The user already exists in this account",
	protos.ErrCode_ERR_USER_ACCESS_TO_ACCOUNT_DENIED: "The user is not allowed to access account",
	protos.ErrCode_ERR_INVALID_ADMIN_LEVEL:           "Unknown admin level",
	protos.ErrCode_ERR_USER_ACCOUNT_MISMATCH:         "The user does not belong to this account",
	protos.ErrCode_ERR_NO_ACCOUNT_SPECIFIED:          "No account is specified for the user",
	protos.ErrCode_ERR_PERMISSION_DENIED:             "Permission denied for this operation",

	// Account-related errors
	protos.ErrCode_ERR_INVALID_ACCOUNT:        "The entered account does not exist",
	protos.ErrCode_ERR_INVALID_PARENT_ACCOUNT: "The parent account of the entered account does not exist",
	protos.ErrCode_ERR_ACCOUNT_ALREADY_EXISTS: "The account already exists in the crane",
	protos.ErrCode_ERR_ACCOUNT_HAS_CHILDREN:   "The account has child account or users, unable to delete.",
	protos.ErrCode_ERR_BLOCKED_ACCOUNT:        "The account has been blocked",

	// Partition-related errors
	protos.ErrCode_ERR_INVALID_PARTITION:                "The entered partition does not exist",
	protos.ErrCode_ERR_PARTITION_MISSING:                "The entered account or user does not include this partition",
	protos.ErrCode_ERR_PARENT_ACCOUNT_PARTITION_MISSING: "Parent account does not include the partition",
	protos.ErrCode_ERR_PARTITION_ALREADY_EXISTS:         "The partition already exists in the account or user",
	protos.ErrCode_ERR_USER_EMPTY_PARTITION:             "The user does not contain any partitions, operation cannot be performed.",
	protos.ErrCode_ERR_CHILD_HAS_PARTITION:              "The partition is currently being used by the child accounts or users of the account, operation cannot be performed. You can use a forced operation to ignore this constraint",
	protos.ErrCode_ERR_HAS_NO_QOS_IN_PARTITION:          "The user has no QoS available for this partition to be used",
	protos.ErrCode_ERR_HAS_ALLOWED_QOS_IN_PARTITION:     "The qos you set is not in partition's allowed qos list",

	// QoS-related errors
	protos.ErrCode_ERR_INVALID_QOS:                     "The entered qos does not exist",
	protos.ErrCode_ERR_DB_QOS_ALREADY_EXISTS:           "Qos already exists in the crane",
	protos.ErrCode_ERR_QOS_REFERENCES_EXIST:            "QoS is still being used by accounts or users, unable to delete",
	protos.ErrCode_ERR_CONVERT_TO_INTEGER:              "Failed to convert value to integer",
	protos.ErrCode_ERR_TIME_LIMIT:                      "Invalid time limit value",
	protos.ErrCode_ERR_QOS_MISSING:                     "The entered account or user does not include this qos",
	protos.ErrCode_ERR_QOS_ALREADY_EXISTS:              "The Qos already exists in the account or user",
	protos.ErrCode_ERR_PARENT_ACCOUNT_QOS_MISSING:      "Parent account does not include the qos",
	protos.ErrCode_ERR_SET_ALLOWED_QOS:                 "The entered QoS list does not include the default QoS for this user. Ignoring this constraint with forced operation, the default QoS is randomly replaced with one of the items in the new QoS list",
	protos.ErrCode_ERR_DEFAULT_QOS_NOT_INHERITED:       "The entered default_qos is not allowed",
	protos.ErrCode_ERR_DUPLICATE_DEFAULT_QOS:           "The QoS is already the default QoS for the account or specified partition of the user",
	protos.ErrCode_ERR_SET_ACCOUNT_QOS:                 "The entered QoS list does not include the default QoS for this account or some descendant node. You can use a forced operation to ignore this constraint",
	protos.ErrCode_ERR_CHILD_HAS_DEFAULT_QOS:           "some child accounts or users is using the QoS as the default QoS. By ignoring this constraint with forced deletion, the deleted default QoS is randomly replaced with one of the remaining items in the QoS list",
	protos.ErrCode_ERR_SET_DEFAULT_QOS:                 "The Qos not allowed or is already the default qos",
	protos.ErrCode_ERR_DEFAULT_QOS_MODIFICATION_DENIED: "The QoS is the default QoS for the current user/Account and cannot be modified. Ignoring this constraint with forced operation, the default QoS is randomly replaced with one of the items in the new QoS list",
	protos.ErrCode_ERR_QOS_JOB_COUNT_EXCEEDED:          "The number of jobs has reached the limit of this QOS",
	protos.ErrCode_ERR_TRES_PER_JOB_BEYOND:             "The tres of jobs has reached the limit of this QOS",
	protos.ErrCode_ERR_MAX_TRES_PER_USER_BEYOND:        "The tres of jobs for the current user has reached the limit of this QOS",
	protos.ErrCode_ERR_MAX_TRES_PER_ACCOUNT_BEYOND:     "The tres of jobs for the current account has reached the limit of this QOS",

	// LicenseResource errors
	protos.ErrCode_ERR_INVALID_ARGUMENT:       "Invalid argument provided",
	protos.ErrCode_ERR_RESOURCE_NOT_FOUND:     "The resource was not found in the crane",
	protos.ErrCode_ERR_RESOURCE_ALREADY_EXIST: "The resource already exists in the crane",

	// System-related errors
	protos.ErrCode_ERR_UPDATE_DATABASE:           "Fail to update data in database",
	protos.ErrCode_ERR_NO_RESOURCE:               "Resource not enough for job",
	protos.ErrCode_ERR_INVALID_NODE_NUM:          "Nodes partition not enough for job",
	protos.ErrCode_ERR_INVALID_NODE_LIST:         "Invalid node list",
	protos.ErrCode_ERR_INVALID_EX_NODE_LIST:      "Invalid exclude node list",
	protos.ErrCode_ERR_TIME_TIMIT_BEYOND:         "Time-limit reached the user's limit",
	protos.ErrCode_ERR_CPUS_PER_TASK_BEYOND:      "cpus-per-task reached the user's limit",
	protos.ErrCode_ERR_NO_ENOUGH_NODE:            "Nodes num not enough for job",
	protos.ErrCode_ERR_BEYOND_JOB_ID:             "The number of pending jobs exceeded maximum value",
	protos.ErrCode_ERR_CGROUP:                    "Error when manipulating cgroup",
	protos.ErrCode_ERR_SYSTEM_ERR:                "Linux Error",
	protos.ErrCode_ERR_RPC_FAILURE:               "RPC call failed",
	protos.ErrCode_ERR_GENERIC_FAILURE:           "Generic failure",
	protos.ErrCode_ERR_NON_EXISTENT:              "The object doesn't exist",
	protos.ErrCode_ERR_INVALID_PARAM:             "Invalid Parameter",
	protos.ErrCode_ERR_PROTOBUF:                  "Error when using protobuf",
	protos.ErrCode_ERR_NO_AVAIL_NODE:             "No available node meets the request",
	protos.ErrCode_ERR_MAX_JOB_COUNT_PER_USER:    "job max count is empty or exceeds the limit",
	protos.ErrCode_ERR_USER_NO_PRIVILEGE:         "User has insufficient privilege",
	protos.ErrCode_ERR_NOT_IN_ALLOWED_LIST:       "The account does not have permission to run jobs in this partition. Please contact the administrator to add it to the allowed list",
	protos.ErrCode_ERR_IN_DENIED_LIST:            "The account has been denied access to this partition. Please contact the security administrator if access is required",
	protos.ErrCode_ERR_EBPF:                      "EBPF syscall error",
	protos.ErrCode_ERR_SUPERVISOR:                "Supervisor error",
	protos.ErrCode_ERR_SHUTTING_DOWN:             "Service is shutting down, unable to process the request",
	protos.ErrCode_ERR_SIGN_CERTIFICATE:          "The user failed to issue the certificate, please contact the administrator for assistance",
	protos.ErrCode_ERR_DUPLICATE_CERTIFICATE:     "The certificate has already been issued to the user. If the certificate is lost and needs to be reissued, please contact the administrator for assistance",
	protos.ErrCode_ERR_REVOKE_CERTIFICATE:        "Revocation of the certificate failed, Please check the logs",
	protos.ErrCode_ERR_IDENTITY_MISMATCH:         "User information does not match, unable to submit the job.",
	protos.ErrCode_ERR_NOT_FORCE:                 "You need to set --force for this operation.",
	protos.ErrCode_ERR_INVALID_USERNAME:          "Invalid username",
	protos.ErrCode_ERR_LICENSE_LEGAL_FAILED:      "License request format is illegal or invalid",
	protos.ErrCode_ERR_INVALID_JOB_ID:            "Invalid job id",
	protos.ErrCode_ERR_CRI_GENERIC:               "CRI runtime returns error. Check logs for details.",
	protos.ErrCode_ERR_CRI_DISABLED:              "CRI support is disabled in the cluster.",
	protos.ErrCode_ERR_CRI_CONTAINER_NOT_READY:   "Job is pending or container is not ready.",
	protos.ErrCode_ERR_INVALID_MEM_FORMAT:        "Invalid memory format",
	protos.ErrCode_ERR_INVALID_WCKEY:             "The specified wckey does not exist",
	protos.ErrCode_ERR_WCKEY_ALREADY_EXISTS:      "The wckey already exists in crane",
	protos.ErrCode_ERR_INVALID_CLUSTER:           "The entered cluster does not exist",
	protos.ErrCode_ERR_DEL_DEFAULT_WCKEY:         "Cannot delete the default wckey. Please set a different default wckey first",
	protos.ErrCode_ERR_NO_DEFAULT_WCKEY:          "No default wckey is set. Please specify a wckey or set a default wckey",
	protos.ErrCode_ERR_STEP_RES_BEYOND:           "Step resource request exceeds the job's requested resources",
	protos.ErrCode_ERR_MISSING_DEPENDENCY:        "One or more dependency jobs may not exist or have ended",
	protos.ErrCode_ERR_DB_INSERT_FAILED:          "Database insertion failed due to internal error",
	protos.ErrCode_ERR_LUA_FAILED:                "Lua script validation failed",
	protos.ErrCode_ERR_MAX_JOB_COUNT_PER_ACCOUNT: "The number of jobs for the current account has reached its limit",
	protos.ErrCode_ERR_USER_HAS_JOB:              "The user has jobs pending or running, cannot be deleted",
	protos.ErrCode_ERR_INVALID_RESOURCE:          "Invalid resource specification for the partition",
	protos.ErrCode_ERR_INVALID_DEADLINE:          "Invalid deadline time(should be later than submit time)",
	protos.ErrCode_ERR_PMIX_ERR:                  "PMIx error",

	protos.ErrCode_ERR_PARTITION_TRES_PER_JOB_BEYOND:         "Resource (TRES) per job exceeds the partition limit",
	protos.ErrCode_ERR_PARTITION_TIME_BEYOND:                 "Time limit exceeds the partition's per-job wall time limit",
	protos.ErrCode_ERR_PARTITION_MAX_SUBMIT_JOBS_PER_USER:    "Partition max submit jobs per user exceeded",
	protos.ErrCode_ERR_PARTITION_MAX_SUBMIT_JOBS_PER_ACCOUNT: "Partition max submit jobs per account exceeded",
	protos.ErrCode_ERR_REQUESTED_NODES_NOT_IN_PARTITION:      "Requested nodes are not in the partition",
}

func ErrMsg(err_code protos.ErrCode) string {
	if msg, exists := errMsgMap[err_code]; exists {
		return msg
	}
	return fmt.Sprintf("Unknown error occurred: %s", err_code)
}
