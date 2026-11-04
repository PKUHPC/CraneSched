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

package cacctmgr

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"os/user"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
	"github.com/xlab/treeprint"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	userUid uint32
	stub    protos.CraneCtldClient
	config  *util.Config
)

type ServerAddr struct {
	ControlMachine      string `yaml:"ControlMachine"`
	ControlMachineAddr  string `yaml:"ControlMachineAddr"`
	CraneCtldListenPort string `yaml:"CraneCtldListenPort"`
}

type ResourceUsageRecord struct {
	ClusterName string
	NodeName    string
	Uid         uint64
	StartTime   int64
	EndTime     int64
	State       string
	Reason      string
	Timestamp   time.Time
}

type EventInfoJson struct {
	ClusterName string `json:"cluster_name"`
	NodeName    string `json:"node_name"`
	Uid         uint64 `json:"uid"`
	StartTime   string `json:"start_time"`
	EndTime     string `json:"end_time"`
	State       string `json:"state"`
	Reason      string `json:"reason"`
}

func PrintAccountTree(parentTreeRoot treeprint.Tree, account string, accountMap map[string]*protos.AccountInfo) {
	if account == "" {
		return
	}

	value, ok := accountMap[account]
	if !ok || len(value.ChildAccounts) == 0 {
		parentTreeRoot.AddNode(account)
	} else {
		branch := parentTreeRoot.AddBranch(account)
		for _, child := range accountMap[account].ChildAccounts {
			PrintAccountTree(branch, child, accountMap)
		}
	}
}

func PrintLicenseResource(resourceList []*protos.LicenseResourceInfo, hasWithClusters bool) {
	if len(resourceList) == 0 {
		return
	}

	sort.Slice(resourceList, func(i, j int) bool {
		return resourceList[i].ResourceName < resourceList[j].ResourceName
	})

	// Table format control
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	if !hasWithClusters {
		table.SetHeader([]string{"Name", "Server", "Type", "Count", "LastConsumed", "Allocated", "ServerType", "Flags"})
	} else {
		table.SetHeader([]string{"Name", "Server", "Type", "Count", "LastConsumed", "Allocated", "ServerType", "Clusters", "Allowed", "Flags"})
	}

	tableData := make([][]string, 0, len(resourceList))
	for _, info := range resourceList {
		var typeString string
		if info.Type == protos.LicenseResource_License {
			typeString = "License"
		}

		var allocatedString string
		allocatedString = fmt.Sprintf("%d", info.Allocated)

		var flagString string
		if info.Flags&(uint32(protos.LicenseResource_Absolute)) != 0 {
			flagString = "Absolute"
		} else if info.Flags == uint32(protos.LicenseResource_None) {
			allocatedString += "%"
		}

		if !hasWithClusters {
			tableData = append(tableData, []string{
				info.ResourceName,
				info.Server,
				typeString,
				strconv.Itoa(int(info.Count)),
				strconv.Itoa(int(info.LastConsumed)),
				allocatedString,
				info.ServerType,
				flagString,
			})
		} else {
			for cluster_name, allowed := range info.ClusterResourceInfo {
				var allowedString string
				allowedString = fmt.Sprintf("%d", allowed)
				if info.Flags == uint32(protos.LicenseResource_None) {
					allowedString += "%"
				}
				tableData = append(tableData, []string{
					info.ResourceName,
					info.Server,
					typeString,
					strconv.Itoa(int(info.Count)),
					strconv.Itoa(int(info.LastConsumed)),
					allocatedString,
					info.ServerType,
					cluster_name,
					allowedString,
					flagString,
				})
			}
		}
	}

	if !FlagFull && FlagFormat == "" {
		util.TrimTable(&tableData)
	}

	table.AppendBulk(tableData)
	table.Render()
}

func AddAccount(account *protos.AccountInfo) error {
	if FlagForce {
		log.Warning("The --force flag is ignored for add operations")
	}
	if err := util.CheckEntityName(account.Name); err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Failed to add account: invalid account name: %v\n", err)
	}

	req := new(protos.AddAccountRequest)
	req.Uid = userUid
	req.Account = account
	if account.DefaultQos == "" && len(account.AllowedQosList) > 0 {
		account.DefaultQos = account.AllowedQosList[0]
	}
	if account.DefaultQos != "" {
		find := false
		for _, qos := range account.AllowedQosList {
			if qos == account.DefaultQos {
				find = true
				break
			}
		}
		if !find {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Failed to add account: default QoS %s is not in allowed QoS list\n", account.DefaultQos))
		}
	}

	reply, err := stub.AddAccount(context.Background(), req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to add account: %v\n", err)
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Println("Account added successfully.")
		return nil
	} else {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to add account: %s.\n", util.ErrMsg(reply.GetCode())))
	}
}

func AddUser(user *protos.UserInfo, partition []string, level string, coordinator bool) error {
	if FlagForce {
		log.Warning("The --force flag is ignored for add operations")
	}
	var err error
	if err = util.CheckEntityName(user.Name); err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Failed to add user: invalid user name: %v\n", err)
	}

	user.Uid, err = util.GetUidByUserName(user.Name)
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Failed to add user: %v\n", err)
	}

	req := new(protos.AddUserRequest)
	req.Uid = userUid
	req.User = user
	for _, par := range partition {
		user.AllowedPartitionQosList = append(user.AllowedPartitionQosList, &protos.UserInfo_AllowedPartitionQos{PartitionName: par})
	}

	if level == "none" {
		user.AdminLevel = protos.UserInfo_None
	} else if level == "operator" {
		user.AdminLevel = protos.UserInfo_Operator
	} else if level == "admin" {
		user.AdminLevel = protos.UserInfo_Admin
	} else {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Failed to add user: unknown admin level, valid values: none, operator, admin."))
	}

	if coordinator {
		user.CoordinatorAccounts = append(user.CoordinatorAccounts, user.Account)
	}

	reply, err := stub.AddUser(context.Background(), req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to add user: %s\n", err)
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Println("User added successfully.")
		return nil
	} else {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to add user: %s.\n", util.ErrMsg(reply.GetCode())))
	}
}

func AddQos(qos *protos.QosInfo) error {
	if FlagForce {
		log.Warning("The --force flag is ignored for add operations")
	}
	if err := util.CheckEntityName(qos.Name); err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Failed to add QoS: invalid QoS name: %v\n", err)
	}

	req := new(protos.AddQosRequest)
	req.Uid = userUid
	req.Qos = qos

	reply, err := stub.AddQos(context.Background(), req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to add QoS: %v\n", err)
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Println("QoS added successfully.")
		return nil
	} else {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to add QoS: %s.\n", util.ErrMsg(reply.GetCode())))
	}
}

func AddLicenseResource(name string, server string, clusters string, operators map[protos.LicenseResource_Field]string) error {
	if FlagForce {
		log.Warning("The --force flag is ignored for add operations")
	}
	if err := util.CheckEntityName(name); err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Failed to add Resource: invalid Resource name: %v\n", err)
	}

	var clusterList []string
	if len(clusters) > 0 {
		var err error
		clusterList, err = util.ParseStringParamList(clusters, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid cluster list specified: %v.\n", err)
		}
	}

	req := protos.AddOrModifyLicenseResourceRequest{
		Uid:          userUid,
		ResourceName: name,
		Server:       server,
		Clusters:     clusterList,
		IsAdd:        true,
	}

	req.Operators = make([]*protos.AddOrModifyLicenseResourceRequest_Operator, 0)
	for field, value := range operators {
		req.Operators = append(req.Operators, &protos.AddOrModifyLicenseResourceRequest_Operator{OperatorField: field, Value: value})
	}

	reply, err := stub.AddOrModifyLicenseResource(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to add Resource: %v\n", err)
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Println("Resource added successfully.")
		return nil
	} else {
		msg := ""
		if len(reply.GetRichErr().GetDescription()) > 0 {
			msg = fmt.Sprintf("Failed to add Resource: %s.\n", reply.GetRichErr().GetDescription())
		} else {
			msg = fmt.Sprintf("Failed to add Resource: %s.\n", util.ErrMsg(reply.GetRichErr().Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

func DeleteAccount(value string) error {
	accountList, err := util.ParseStringParamList(value, ",")
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Invalid user list specified: %v.\n", err)
	}

	if slices.Contains(accountList, "ALL") {
		if !FlagForce {
			return util.NewCraneErr(util.ErrorCmdArg, "To delete all accounts, you must set --force.")
		}
		accountList = []string{"ALL"}
	}

	req := protos.DeleteAccountRequest{Uid: userUid, AccountList: accountList, Force: FlagForce}

	reply, err := stub.DeleteAccount(context.Background(), &req)
	if err != nil {
		return util.NewCraneErr(util.ErrorNetwork, fmt.Sprintf("Failed to delete account %s: %v\n", value, err))
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}
	if reply.GetOk() {
		fmt.Printf("Successfully deleted account '%s'.\n", value)
		return nil
	} else {
		msg := "Failed to delete account: \n"
		for _, richError := range reply.RichErrorList {
			msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

func DeleteUser(value string, account string) error {
	userList, err := util.ParseStringParamList(value, ",")
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Invalid user list specified: %v.\n", err)
	}

	if slices.Contains(userList, "ALL") {
		if !FlagForce {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("To delete all users in the account, you must set --force."))
		}
		userList = []string{"ALL"}
	}

	req := protos.DeleteUserRequest{Uid: userUid, UserList: userList, Account: account, Force: FlagForce}

	reply, err := stub.DeleteUser(context.Background(), &req)
	if err != nil {
		return util.NewCraneErr(util.ErrorNetwork, fmt.Sprintf("Failed to remove user %s: %v\n", value, err))
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}
	if reply.GetOk() {
		fmt.Printf("Successfully removed user '%s'.\n", value)
		return nil
	} else {
		msg := "Failed to remove user: \n"
		for _, richError := range reply.RichErrorList {
			msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

func DeleteQos(value string) error {
	if FlagForce {
		log.Warning("--force flag is ignored for delete operations")
	}

	qosList, err := util.ParseStringParamList(value, ",")
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Invalid user list specified: %v.\n", err)
	}

	if slices.Contains(qosList, "ALL") {
		if !FlagForce {
			return util.NewCraneErr(util.ErrorCmdArg, "To delete all QoS, you must set --force.")
		}
		qosList = []string{"ALL"}
	}

	req := protos.DeleteQosRequest{Uid: userUid, QosList: qosList, Force: FlagForce}

	reply, err := stub.DeleteQos(context.Background(), &req)
	if err != nil {
		return util.NewCraneErr(util.ErrorNetwork, fmt.Sprintf("Failed to delete QoS %s: %v\n", value, err))
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}
	if reply.GetOk() {
		fmt.Printf("Successfully deleted QoS '%s'.\n", value)
		return nil
	} else {
		msg := "Failed to delete QoS: \n"
		for _, richError := range reply.RichErrorList {
			msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

func DeleteLicenseResource(name string, server string, clusters string) error {
	if strings.ToUpper(name) == "ALL" {
		if !FlagForce {
			return util.NewCraneErr(util.ErrorCmdArg, "To delete all resources, you must set --force.")
		}
		req := protos.DeleteLicenseResourceRequest{
			Uid: userUid, ResourceName: "ALL", Force: FlagForce,
		}
		reply, err := stub.DeleteLicenseResource(context.Background(), &req)
		if err != nil {
			return util.WrapCraneErr(util.ErrorNetwork, "Failed to delete all resources: %v", err)
		}
		if FlagJson {
			fmt.Println(util.FmtJson.FormatReply(reply))
		}
		if reply.GetOk() {
			fmt.Println("All resources deleted successfully.")
			return nil
		}
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to delete all resources: %s\n", util.ErrMsg(reply.GetRichErr().GetCode())))
	}

	var clusterList []string
	if len(clusters) > 0 {
		var err error
		clusterList, err = util.ParseStringParamList(clusters, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid cluster list specified: %v.\n", err)
		}
	}

	req := protos.DeleteLicenseResourceRequest{Uid: userUid, ResourceName: name, Server: server, Clusters: clusterList}

	reply, err := stub.DeleteLicenseResource(context.Background(), &req)
	if err != nil {
		return util.NewCraneErr(util.ErrorNetwork, fmt.Sprintf("Failed to delete resource %s: %v\n", name, err))
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Printf("Successfully deleted Resource '%s@%s'.\n", name, server)
		return nil
	} else {
		msg := ""
		if len(reply.GetRichErr().GetDescription()) > 0 {
			msg = fmt.Sprintf("Failed to delete Resource: %s.\n", reply.GetRichErr().GetDescription())
		} else {
			msg = fmt.Sprintf("Failed to delete Resource: %s.\n", util.ErrMsg(reply.GetRichErr().Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

func ModifyAccount(params []ModifyParam, partition string, name string) error {
	var valueList []string
	var err error

	req := protos.ModifyAccountRequest{
		Uid:       userUid,
		Name:      name,
		Force:     FlagForce,
		Partition: partition,
	}

	for _, param := range params {
		valueList, err = util.ParseStringParamList(param.NewValue, ",")
		if err != nil {
			switch param.ModifyField {
			case protos.ModifyField_Qos:
				return util.WrapCraneErr(util.ErrorCmdArg, "Invalid qos list specified: %v.\n", err)
			case protos.ModifyField_Partition:
				return util.WrapCraneErr(util.ErrorCmdArg, "Invalid partition list specified: %v.\n", err)
			default:
				return util.WrapCraneErr(util.ErrorCmdArg, "Invalid value list specified: %v.\n", err)
			}
		}

		if param.ModifyField == protos.ModifyField_DefaultQos || param.ModifyField == protos.ModifyField_Description {
			if len(valueList) != 1 {
				return util.NewCraneErr(util.ErrorCmdArg, "Invalid value specified! Modify Description and DefaultQos, please provide only one value.")
			}
		}

		req.Operations = append(req.Operations, &protos.ModifyFieldOperation{
			ModifyField: param.ModifyField,
			ValueList:   valueList,
			Type:        param.RequestType,
		})
	}

	reply, err := stub.ModifyAccount(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to modify account information: %v\n", err)
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Println("Information was successfully modified.")
		return nil
	} else {
		msg := "Failed to modify information:\n"
		for _, richError := range reply.RichErrorList {
			if richError.Description == "" {
				msg += fmt.Sprintf("%s \n", util.ErrMsg(richError.Code))
			} else {
				msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
			}
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

func ModifyUser(params []ModifyParam, name string, account string, partition string) error {

	req := protos.ModifyUserRequest{
		Uid:       userUid,
		Name:      name,
		Partition: partition,
		Account:   account,
		Force:     FlagForce,
	}

	for _, param := range params {
		if param.ModifyField == protos.ModifyField_AdminLevel {
			if param.NewValue != "none" && param.NewValue != "operator" && param.NewValue != "admin" {
				return util.NewCraneErr(util.ErrorCmdArg, "Unknown admin level, valid values: none, operator, admin.")
			}
		}

		var valueList []string
		var err error

		valueList, err = util.ParseStringParamList(param.NewValue, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid value list specified: %v.\n", err)
		}

		if param.ModifyField == protos.ModifyField_AdminLevel || param.ModifyField == protos.ModifyField_DefaultQos || param.ModifyField == protos.ModifyField_DefaultAccount {
			if len(valueList) != 1 {
				return util.NewCraneErr(util.ErrorCmdArg, "Invalid value specified! Modify AdminLevel, DefaultAccount and DefaultQos, please provide only one value.")
			}
		}

		req.Operations = append(req.Operations, &protos.ModifyFieldOperation{
			ModifyField: param.ModifyField,
			ValueList:   valueList,
			Type:        param.RequestType,
		})
	}

	reply, err := stub.ModifyUser(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to modify user information: %s\n", err)
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Println("Information was successfully modified.")
		return nil
	} else {
		msg := fmt.Sprintln("Modify information failed:")
		for _, richError := range reply.RichErrorList {
			if richError.Description == "" {
				msg += fmt.Sprintf("%s \n", util.ErrMsg(richError.Code))
			} else {
				msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
			}
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

func ModifyQos(params []ModifyParam, name string) error {
	if FlagForce {
		log.Warning("--force flag is ignored for QoS modify operations")
	}
	req := protos.ModifyQosRequest{
		Uid:  userUid,
		Name: name,
	}

	for _, param := range params {
		var valueList []string
		if param.ModifyField == protos.ModifyField_Preempt {
			list, err := util.ParseStringParamListAllowEmpty(param.NewValue, ",")
			if err != nil {
				return util.NewCraneErr(util.ErrorCmdArg,
					fmt.Sprintf("invalid preempt list %q: %v\n", param.NewValue, err))
			}
			valueList = list
		} else {
			valueList = []string{param.NewValue}
		}
		req.Operations = append(req.Operations, &protos.ModifyFieldOperation{
			ModifyField: param.ModifyField,
			ValueList:   valueList,
		})
	}

	reply, err := stub.ModifyQos(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to modify QoS: %v\n", err)
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Println("Information was successfully modified.")
		return nil
	} else {
		msg := fmt.Sprintln("Failed to modify information:")
		for _, richError := range reply.RichErrorList {
			if richError.Description == "" {
				msg += fmt.Sprintf("%s \n", util.ErrMsg(richError.Code))
			} else {
				msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
			}
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

func ModifyResource(name string, server string, clusters string, operators map[protos.LicenseResource_Field]string) error {
	if FlagForce {
		log.Warning("--force flag is ignored for Resource modify operations")
	}

	var clusterList []string
	if len(clusters) > 0 {
		var err error
		clusterList, err = util.ParseStringParamList(clusters, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid cluster list specified: %v.\n", err)
		}
	}

	req := protos.AddOrModifyLicenseResourceRequest{
		Uid:          userUid,
		ResourceName: name,
		Server:       server,
		Clusters:     clusterList,
		IsAdd:        false,
	}

	req.Operators = make([]*protos.AddOrModifyLicenseResourceRequest_Operator, 0)
	for field, value := range operators {
		req.Operators = append(req.Operators, &protos.AddOrModifyLicenseResourceRequest_Operator{OperatorField: field, Value: value})
	}

	reply, err := stub.AddOrModifyLicenseResource(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to modify Resource: %v\n", err)
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Println("Information was successfully modified.")
		return nil
	} else {
		msg := ""
		if len(reply.GetRichErr().GetDescription()) > 0 {
			msg = fmt.Sprintf("Failed to modify information: %s.\n", reply.GetRichErr().GetDescription())
		} else {
			msg = fmt.Sprintf("Failed to modify information: %s.\n", util.ErrMsg(reply.GetRichErr().Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

func FindAccount(value string) error {
	if FlagForce {
		log.Warning("--force flag is ignored for show operations")
	}
	var accountList []string
	if value != "" {
		var err error
		accountList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid account list specified: %v.\n", err)
		}
	}

	req := protos.QueryAccountInfoRequest{Uid: userUid, AccountList: accountList}
	reply, err := stub.QueryAccountInfo(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to find account: %v\n", err)
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	PrintAccountList(reply.AccountList)

	if !reply.GetOk() {
		for _, richError := range reply.RichErrorList {
			if richError.Description == "" {
				fmt.Println(util.ErrMsg(richError.Code))
				break
			}
			fmt.Printf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return &util.CraneError{Code: util.ErrorBackend}
	}

	return nil
}

func ShowLicenseResources(name string, server string, clusters string, hasWithClusters bool) error {
	if FlagForce {
		log.Warning("--force flag is ignored for show operations")
	}
	var clusterList []string
	if clusters != "" {
		var err error
		clusterList, err = util.ParseStringParamList(clusters, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid cluster list specified: %v.\n", err)
		}
	}

	req := protos.QueryLicenseResourceRequest{Uid: userUid, ResourceName: name, Server: server, Clusters: clusterList}
	reply, err := stub.QueryLicenseResource(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to show resource: %v\n", err)
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	PrintLicenseResource(reply.LicenseResourceList, hasWithClusters)

	if !reply.GetOk() {
		if len(reply.GetRichErr().GetDescription()) > 0 {
			fmt.Printf("Failed to show resource: %s.\n", reply.GetRichErr().GetDescription())
		} else {
			fmt.Printf("Failed to show resource: %s.\n", util.ErrMsg(reply.GetRichErr().Code))
		}
		return &util.CraneError{Code: util.ErrorBackend}
	}

	return nil
}

func BlockAccountOrUser(value string, entityType protos.EntityType, account string) error {
	if FlagForce {
		log.Warning("--force flag is ignored for block operations")
	}

	var entityList []string
	if value != "all" {
		var err error
		entityList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid account/user list specified: %v.\n", err)
		}
	}

	req := protos.BlockAccountOrUserRequest{Uid: userUid, Block: true, EntityType: entityType, EntityList: entityList, Account: account}
	reply, err := stub.BlockAccountOrUser(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to block entity: %v\n", err)
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Printf("Block %s succeeded.\n", value)
		return nil
	} else {
		msg := ""
		for _, richError := range reply.RichErrorList {
			if richError.Description == "" {
				msg += fmt.Sprintln(util.ErrMsg(richError.Code))
				break
			}
			msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

func UnblockAccountOrUser(value string, entityType protos.EntityType, account string) error {
	if FlagForce {
		log.Warning("--force flag is ignored for unblock operations")
	}

	var entityList []string
	if value != "all" {
		var err error
		entityList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid account/user list specified: %v.\n", err)
		}
	}

	req := protos.BlockAccountOrUserRequest{Uid: userUid, Block: false, EntityType: entityType, EntityList: entityList, Account: account}
	reply, err := stub.BlockAccountOrUser(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to unblock entity: %v\n", err)
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Printf("Unblock %s succeeded.\n", value)
		return nil
	} else {
		msg := ""
		for _, richError := range reply.RichErrorList {
			if richError.Description == "" {
				msg += fmt.Sprintln(util.ErrMsg(richError.Code))
				break
			}
			msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

// GetPlugindClient connects to cplugind for querying event data via RPC
func GetPlugindClient(config *util.Config, pluginConfig *util.PluginConfig) (protos.PluginQueryServiceClient, *grpc.ClientConn, error) {
	if !pluginConfig.Enabled {
		return nil, nil, util.NewCraneErr(util.ErrorCmdArg, "Plugin is not enabled")
	}

	addr := pluginConfig.ListenAddress
	port := pluginConfig.ListenPort
	if addr == "" || port == "" {
		return nil, nil, util.NewCraneErr(util.ErrorCmdArg,
			"PlugindListenAddress and PlugindListenPort must be configured")
	}

	endpoint := net.JoinHostPort(addr, port)
	var creds credentials.TransportCredentials
	if config.TlsConfig.Enabled {
		certPath := config.TlsConfig.CaFilePath
		if certPath == "" {
			return nil, nil, util.NewCraneErr(util.ErrorCmdArg,
				"TLS is enabled for plugin client but no certificate file is configured")
		}
		var err error
		creds, err = credentials.NewClientTLSFromFile(certPath, "")
		if err != nil {
			return nil, nil, util.NewCraneErr(util.ErrorCmdArg,
				fmt.Sprintf("Failed to load TLS credentials: %v", err))
		}
	} else {
		creds = insecure.NewCredentials()
	}

	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(util.ClientKeepAliveParams),
		grpc.WithConnectParams(util.ClientConnectParams),
		grpc.WithIdleTimeout(time.Duration(math.MaxInt64)),
	)
	if err != nil {
		return nil, nil, util.NewCraneErr(util.ErrorNetwork,
			fmt.Sprintf("Failed to connect to cplugind at %s: %v", endpoint, err))
	}

	return protos.NewPluginQueryServiceClient(conn), conn, nil
}

func SortNodeEventRecords(records []*protos.NodeEventInfo, maxLines int) ([]*protos.NodeEventInfo, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("records list is empty")
	}

	// Sort the records by NodeName in ascending order
	sort.SliceStable(records, func(i, j int) bool {
		if records[i].NodeName == records[j].NodeName {
			return records[i].StartTime < records[j].StartTime
		}
		return records[i].NodeName < records[j].NodeName
	})

	drainMap := make(map[string]*protos.NodeEventInfo)
	var filteredRecords []*protos.NodeEventInfo
	for _, currentRecord := range records {
		if currentRecord.State == "Resume" {
			if previousRecord, exists := drainMap[currentRecord.NodeName]; exists {
				previousRecord.EndTime = currentRecord.StartTime
				continue
			}
		} else if currentRecord.State == "Drain" {
			drainMap[currentRecord.NodeName] = currentRecord
		}

		filteredRecords = append(filteredRecords, currentRecord)
	}

	// Sort the filteredRecords by StartTime
	sort.SliceStable(filteredRecords, func(i, j int) bool {
		return filteredRecords[i].StartTime > filteredRecords[j].StartTime
	})

	if maxLines > 0 && len(filteredRecords) > maxLines {
		filteredRecords = filteredRecords[:maxLines]
	}

	return filteredRecords, nil
}

func QueryEventInfoByNodes(nodeRegex string, maxLines int) error {
	if FlagForce {
		log.Warning("--force flag is ignored for query operations")
	}

	// Parse node names if provided
	nodeNames := []string{}
	var ok bool
	if len(nodeRegex) != 0 {
		nodeNames, ok = util.ParseHostList(nodeRegex)
		if !ok {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid node pattern: %s.\n", nodeRegex))
		}

		// Resolve configured aliases while keeping unknown nodes separate so the
		// command can report all invalid inputs together.
		aliases, err := util.BuildNodeAliasMap(config.CranedNodeList)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid input for nodes: %v\n", err)
		}
		resolvedNodes := make([]string, 0, len(nodeNames))
		missingList := make([]string, 0)
		for _, node := range nodeNames {
			canonical, exists := aliases[node]
			if !exists {
				missingList = append(missingList, node)
				continue
			}
			resolvedNodes = append(resolvedNodes, canonical)
		}
		if len(missingList) > 0 {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid input nodes: %v\n", missingList))
		}
		nodeNames = resolvedNodes
	}
	// If no nodes specified, nodeNames will be empty and query all nodes

	pluginConfig, err := util.ParsePluginConfig(FlagPluginConfigFilePath)
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg,
			fmt.Sprintf("Failed to parse plugin config from %s: %v", FlagPluginConfigFilePath, err))
	}

	// Connect to cplugind
	pluginClient, pluginConn, err := GetPlugindClient(config, pluginConfig)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to connect to cplugind: %v", err)
	}
	defer pluginConn.Close()

	// Get current user for authorization
	currentUser, err := user.Current()
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "Failed to get current user: %v", err)
	}

	uid, err := strconv.ParseUint(currentUser.Uid, 10, 32)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "Failed to parse user ID: %v", err)
	}

	// Query events via RPC
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &protos.QueryNodeEventsRequest{
		NodeNames: nodeNames,
		Uid:       uint32(uid),
	}

	reply, err := pluginClient.QueryNodeEvents(ctx, req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorBackend, "Failed to query node events: %v", err)
	}

	if !reply.Ok {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Query node events failed: %s\n", reply.ErrorMessage))
	}

	if len(reply.EventInfoList) == 0 {
		log.Info("No event data found")
		return nil
	}

	eventInfoList, err := SortNodeEventRecords(reply.EventInfoList, maxLines)
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Failed to sort records: %v\n", err)
	}

	if FlagJson {
		eventJsonList := []*EventInfoJson{}
		for _, event := range eventInfoList {
			startTime := FormatNanoTime(event.StartTime)
			endTime := FormatNanoTime(event.EndTime)
			eventJson := &EventInfoJson{
				ClusterName: event.ClusterName,
				NodeName:    event.NodeName,
				Uid:         event.Uid,
				StartTime:   startTime,
				EndTime:     endTime,
				State:       event.State,
				Reason:      event.Reason,
			}
			eventJsonList = append(eventJsonList, eventJson)
		}
		jsonData, err := json.MarshalIndent(eventJsonList, "", "  ")
		if err != nil {
			return util.WrapCraneErr(util.ErrorBackend, "Failed to marshal data to JSON: %v\n", err)
		}
		fmt.Println(string(jsonData))
		return nil
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(true)
	table.SetHeader([]string{"Node", "StartTime", "EndTime", "State", "Reason", "Uid"})

	for _, event := range eventInfoList {
		startTime := FormatNanoTime(event.StartTime)
		endTime := FormatNanoTime(event.EndTime)
		table.Append([]string{
			event.NodeName,
			startTime,
			endTime,
			event.State,
			event.Reason,
			strconv.FormatUint(event.Uid, 10),
		})
	}

	table.Render()
	return nil
}

func FormatNanoTime(ns int64) string {
	if ns == 0 || time.Unix(0, int64(ns)).Year() == 1970 {
		return "Unknown"
	}
	return time.Unix(0, int64(ns)).In(time.Local).Format("2006-01-02 15:04:05")
}

func ResetUserCredential(value string) error {
	var userList []string

	if value == "" {
		return util.NewCraneErr(util.ErrorCmdArg, "User is empty")
	}

	if value != "all" {
		var err error
		userList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid user list specified: %v.\n", err)
		}
	}

	req := protos.ResetUserCredentialRequest{Uid: userUid, UserList: userList}
	reply, err := stub.ResetUserCredential(context.Background(), &req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to reset user credential")
	}
	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if !reply.GetOk() {
		msg := ""
		for _, richError := range reply.RichErrorList {
			msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}

	fmt.Printf("reset user %s credential succeeded.\n", value)
	return nil
}

func AddWckey(wckey *protos.WckeyInfo) error {
	if err := util.CheckEntityName(wckey.Name); err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Failed to add wckey: invalid wckey name: %v", err)
	}

	req := new(protos.AddWckeyRequest)
	req.Uid = userUid
	req.Wckey = wckey

	reply, err := stub.AddWckey(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to add wckey")
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Println("Wckey added successfully.")
		return nil
	} else {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to add wckey: %s.\n", util.ErrMsg(reply.GetCode())))
	}
}

func DeleteWckey(name, userName string) error {
	if strings.ToUpper(name) == "ALL" {
		if !FlagForce {
			return util.NewCraneErr(util.ErrorCmdArg, "To delete all wckeys, you must set --force.")
		}
		req := protos.DeleteWckeyRequest{Uid: userUid, Name: "ALL", Force: true}
		reply, err := stub.DeleteWckey(context.Background(), &req)
		if err != nil {
			return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to delete all wckeys")
		}
		if FlagJson {
			fmt.Println(util.FmtJson.FormatReply(reply))
		}
		if reply.GetOk() {
			fmt.Println("All wckeys deleted successfully.")
			return nil
		}
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to delete all wckeys: %s", util.ErrMsg(reply.GetRichError().GetCode())))
	}

	req := protos.DeleteWckeyRequest{Uid: userUid, Name: name, UserName: userName, Force: false}

	if FlagForce {
		req.Force = true
	}

	reply, err := stub.DeleteWckey(context.Background(), &req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to delete wckey %s, user %s", name, userName)
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Printf("Successfully deleted wckey: %s, user: %s\n", name, userName)
		return nil
	} else {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to delete wckey: %s, user: %s: %s", name, userName, util.ErrMsg(reply.GetRichError().GetCode())))
	}
}

func ModifyDefaultWckey(name, userName string) error {
	req := protos.ModifyDefaultWckeyRequest{
		Uid:      userUid,
		Name:     name,
		UserName: userName,
	}

	reply, err := stub.ModifyDefaultWckey(context.Background(), &req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to modify default wckey")
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Println("Modify information succeeded.")
		return nil
	} else {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Modify information failed: %s.\n", util.ErrMsg(reply.GetCode())))
	}
}
