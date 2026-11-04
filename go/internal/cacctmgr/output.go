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
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
	"github.com/xlab/treeprint"
)

type Tableoutput struct {
	header    []string
	tableData [][]string
}

// init []int
func createSlice(length int, value int) []int {
	slice := make([]int, length)
	for i := range slice {
		slice[i] = value
	}
	return slice
}

// Txn
func PrintTxnLogList(txnLogList []*protos.QueryTxnLogReply_Txn) {
	if len(txnLogList) == 0 {
		return
	}
	var TxnActionToString = []string{
		"Add Account", "Modify Account", "Delete Account",
		"Add User", "Modify User", "Delete User",
		"Add QoS", "Modify QoS", "Delete QoS",
	}
	sort.Slice(txnLogList, func(i, j int) bool { return txnLogList[i].CreationTime < txnLogList[j].CreationTime })

	// Table format control
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"CreationTime", "Actor", "Action", "Target", "Info"})
	tableData := make([][]string, 0, len(txnLogList))
	for _, txnLog := range txnLogList {

		CreationTime := time.Unix(txnLog.CreationTime, 0)

		tableData = append(tableData, []string{
			CreationTime.Format("2006-01-02 15:04:05"),
			txnLog.Actor,
			TxnActionToString[txnLog.Action],
			txnLog.Target,
			txnLog.Info})
	}

	if !FlagFull && FlagFormat == "" {
		util.TrimTable(&tableData)
	}

	table.AppendBulk(tableData)
	table.Render()
}

func ShowTxn(actor string, target string, actionValue string, info string, startTimeValue string) error {
	if FlagForce {
		log.Warning("--force flag is ignored for show operations")
	}

	var action string
	if actionValue != "" {
		value, ok := util.StringToTxnAction(actionValue)
		if !ok {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid action specified: %v.\n", actionValue))
		}

		action = strconv.Itoa(int(value))
	}

	req := protos.QueryTxnLogRequest{
		Uid:    userUid,
		Actor:  actor,
		Target: target,
		Action: action,
		Info:   info,
	}
	if startTimeValue != "" {
		req.TimeInterval = &protos.TimeInterval{}
		err := util.ParseInterval(startTimeValue, req.TimeInterval)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Parse time interval failed:%s\n", err)
		}
	}

	reply, err := stub.QueryTxnLog(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to show txn: %s", err)
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
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintln(util.ErrMsg(reply.GetCode())))
	}

	PrintTxnLogList(reply.TxnLogList)

	return nil
}

// Account
func PrintAccountList(accountList []*protos.AccountInfo) error {
	if len(accountList) == 0 {
		return nil
	}
	sort.Slice(accountList, func(i, j int) bool {
		return accountList[i].Name < accountList[j].Name
	})

	// Slice to map and find the root account
	accountMap := make(map[string]*protos.AccountInfo)
	rootAccount := make([]string, 0)
	for _, accountInfo := range accountList {
		accountMap[accountInfo.Name] = accountInfo
	}
	for _, accountInfo := range accountList {
		if accountInfo.ParentAccount == "" || func() bool {
			_, ok := accountMap[accountInfo.ParentAccount]
			return !ok
		}() {
			rootAccount = append(rootAccount, accountInfo.Name)
		}
	}
	// Print account tree
	tree := treeprint.NewWithRoot("AccountTree")
	for _, account := range rootAccount {
		PrintAccountTree(tree, account, accountMap)
	}
	fmt.Println(tree.String())

	if FlagShowPartitionLimit {
		PrintAccountPartitionTable(accountList)
	}

	// Print account table
	return PrintAccountTable(accountList)
}

func PrintAccountPartitionTable(accountList []*protos.AccountInfo) {
	table := tablewriter.NewWriter(os.Stdout) //table format control
	util.SetBorderTable(table)
	table.SetHeader([]string{"Name", "Partition", "MaxTres", "MaxTresPerJob", "MaxJobs", "MaxSubmitJobs", "MaxWall", "MaxWallPerJob"})
	var tableData [][]string
	for _, accountInfo := range accountList {
		if accountInfo.PartitionResourceLimit != nil {
			for partition, partitionResourceLimit := range accountInfo.PartitionResourceLimit {
				var maxJobsStr string
				if partitionResourceLimit.MaxJobs == math.MaxUint32 {
					maxJobsStr = "unlimited"
				} else {
					maxJobsStr = strconv.FormatUint(uint64(partitionResourceLimit.MaxJobs), 10)
				}
				var maxSubmitJobsStr string
				if partitionResourceLimit.MaxSubmitJobs == math.MaxUint32 {
					maxSubmitJobsStr = "unlimited"
				} else {
					maxSubmitJobsStr = strconv.FormatUint(uint64(partitionResourceLimit.MaxSubmitJobs), 10)
				}
				var maxWallStr string
				if partitionResourceLimit.MaxWall == 0 {
					// MaxWall == 0 means no total wall time limit (ZeroDuration on backend)
					maxWallStr = "unlimited"
				} else {
					maxWallStr = util.SecondTimeFormat(int64(partitionResourceLimit.MaxWall))
				}
				var maxWallPerJobStr string
				if partitionResourceLimit.MaxWallDurationPerJob >= util.MaxJobTimeLimit {
					maxWallPerJobStr = "unlimited"
				} else {
					maxWallPerJobStr = util.SecondTimeFormat(int64(partitionResourceLimit.MaxWallDurationPerJob))
				}

				tableData = append(tableData, []string{
					accountInfo.Name,
					partition,
					util.ResourceViewToTres(partitionResourceLimit.MaxTres),
					util.ResourceViewToTres(partitionResourceLimit.MaxTresPerJob),
					maxJobsStr,
					maxSubmitJobsStr,
					maxWallStr,
					maxWallPerJobStr,
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

// Name Desciption AllowedPartition Users DefaultQos AllowedQosList Coordinators Blocked
func AccountFormatOutput(tableCtx *Tableoutput, accountList []*protos.AccountInfo) error {
	formatTableData := make([][]string, len(accountList))
	formatReq := util.SplitString(FlagFormat, []string{" ", ","})
	tableOutputWidth := createSlice(len(formatReq), -1)
	tableOutputHeader := make([]string, len(formatReq))

	for i := 0; i < len(formatReq); i++ {
		switch string(bytes.ToLower([]byte(formatReq[i]))) {
		case "name":
			tableOutputHeader[i] = "Name"
			for j := 0; j < len(accountList); j++ {
				formatTableData[j] = append(formatTableData[j], accountList[j].Name)
			}
		case "description":
			tableOutputHeader[i] = "Description"
			for j := 0; j < len(accountList); j++ {
				formatTableData[j] = append(formatTableData[j], accountList[j].Description)
			}
		case "allowedpartition":
			tableOutputHeader[i] = "AllowedPartition"
			for j := 0; j < len(accountList); j++ {
				formatTableData[j] = append(formatTableData[j], strings.Join(accountList[j].AllowedPartitions, ", "))
			}
		case "users":
			tableOutputHeader[i] = "Users"
			for j := 0; j < len(accountList); j++ {
				formatTableData[j] = append(formatTableData[j], strings.Join(accountList[j].Users, ", "))
			}
		case "defaultqos":
			tableOutputHeader[i] = "DefaultQos"
			for j := 0; j < len(accountList); j++ {
				formatTableData[j] = append(formatTableData[j], accountList[j].DefaultQos)
			}
		case "allowedqoslist":
			tableOutputHeader[i] = "AllowedQosList"
			for j := 0; j < len(accountList); j++ {
				formatTableData[j] = append(formatTableData[j], strings.Join(accountList[j].AllowedQosList, ", "))
			}
		case "coordinators":
			tableOutputHeader[i] = "Coordinators"
			for j := 0; j < len(accountList); j++ {
				formatTableData[j] = append(formatTableData[j], strings.Join(accountList[j].Coordinators, ", "))
			}
		case "blocked":
			tableOutputHeader[i] = "Blocked"
			for j := 0; j < len(accountList); j++ {
				formatTableData[j] = append(formatTableData[j], strconv.FormatBool(accountList[j].Blocked))
			}
		default:
			return util.NewCraneErr(util.ErrorInvalidFormat, fmt.Sprintf("Invalid format. You entered: '%s'", formatReq[i]))
		}
	}
	tableCtx.header, tableCtx.tableData = util.FormatTable(tableOutputWidth, tableOutputHeader, formatTableData)
	return nil
}

func AccountDefaultOutput(tableCtx *Tableoutput, accountList []*protos.AccountInfo) {
	tableCtx.header = []string{"Name", "Description", "AllowedPartition", "Users", "DefaultQos", "AllowedQosList", "Coordinators", "Blocked"}
	tableCtx.tableData = make([][]string, 0, len(accountList))
	for _, accountInfo := range accountList {
		tableCtx.tableData = append(tableCtx.tableData, []string{
			accountInfo.Name,
			accountInfo.Description,
			strings.Join(accountInfo.AllowedPartitions, ", "),
			strings.Join(accountInfo.Users, ", "),
			accountInfo.DefaultQos,
			strings.Join(accountInfo.AllowedQosList, ", "),
			strings.Join(accountInfo.Coordinators, ", "),
			strconv.FormatBool(accountInfo.Blocked),
		})
	}
}
func PrintAccountTable(accountList []*protos.AccountInfo) error {
	table := tablewriter.NewWriter(os.Stdout) //table format control
	util.SetBorderTable(table)

	var tableCtx Tableoutput // include header and datas
	if FlagFormat != "" {
		if err := AccountFormatOutput(&tableCtx, accountList); err != nil {
			return err
		}
	} else {
		AccountDefaultOutput(&tableCtx, accountList)
	}
	if !FlagNoHeader {
		table.SetHeader(tableCtx.header)
	}
	if !FlagFull && FlagFormat == "" {
		// The data in the fifth column is AllowedQosList, which is not trim
		util.TrimTableExcept(&tableCtx.tableData, 5)
	}

	table.AppendBulk(tableCtx.tableData)
	table.Render()
	return nil
}
func ShowAccounts() error {
	if FlagForce {
		log.Warning("--force flag is ignored for show operations")
	}
	req := protos.QueryAccountInfoRequest{Uid: userUid}
	reply, err := stub.QueryAccountInfo(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to show accounts: %s", err)
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
			if richError.Description == "" {
				msg += fmt.Sprintln(util.ErrMsg(richError.Code))
				break
			}
			msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
	return PrintAccountList(reply.AccountList)
}

// User
func PrintUserList(userList []*protos.UserInfo) error {
	if len(userList) == 0 {
		return nil
	}
	sort.Slice(userList, func(i, j int) bool {
		return userList[i].Uid < userList[j].Uid
	})
	// Slice to map
	userMap := make(map[string][]*protos.UserInfo)
	for _, userInfo := range userList {
		key := ""
		if userInfo.Account[len(userInfo.Account)-1] == '*' {
			key = userInfo.Account[:len(userInfo.Account)-1]
		} else {
			key = userInfo.Account
		}
		if list, ok := userMap[key]; ok {
			userMap[key] = append(list, userInfo)
		} else {
			var list = []*protos.UserInfo{userInfo}
			userMap[key] = list
		}
	}
	return PrintUserTable(userMap)
}

func PrintUserPartitionLimit(userList []*protos.UserInfo) {
	if len(userList) == 0 {
		return
	}

	sort.Slice(userList, func(i, j int) bool {
		return userList[i].Uid < userList[j].Uid
	})

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader([]string{"Account", "UserName", "Uid", "Partition", "MaxTres", "MaxTresPerJob", "MaxJobs", "MaxSubmitJobs", "MaxWall", "MaxWallPerJob"})
	var tableData [][]string
	for _, userInfo := range userList {
		if userInfo.AccountToPartitionLimit != nil {
			for account, partitionToLimitMap := range userInfo.AccountToPartitionLimit {
				if partitionToLimitMap != nil {
					for partition, partitionResourceLimit := range partitionToLimitMap.PartitionResourceLimit {
						var maxJobsStr string
						if partitionResourceLimit.MaxJobs == math.MaxUint32 {
							maxJobsStr = "unlimited"
						} else {
							maxJobsStr = strconv.FormatUint(uint64(partitionResourceLimit.MaxJobs), 10)
						}
						var maxSubmitJobsStr string
						if partitionResourceLimit.MaxSubmitJobs == math.MaxUint32 {
							maxSubmitJobsStr = "unlimited"
						} else {
							maxSubmitJobsStr = strconv.FormatUint(uint64(partitionResourceLimit.MaxSubmitJobs), 10)
						}
						var maxWallStr string
						if partitionResourceLimit.MaxWall == 0 {
							// MaxWall == 0 means no total wall time limit (ZeroDuration on backend)
							maxWallStr = "unlimited"
						} else {
							maxWallStr = util.SecondTimeFormat(int64(partitionResourceLimit.MaxWall))
						}
						var maxWallPerJobStr string
						if partitionResourceLimit.MaxWallDurationPerJob >= util.MaxJobTimeLimit {
							maxWallPerJobStr = "unlimited"
						} else {
							maxWallPerJobStr = util.SecondTimeFormat(int64(partitionResourceLimit.MaxWallDurationPerJob))
						}

						tableData = append(tableData, []string{
							account,
							userInfo.Name,
							strconv.FormatUint(uint64(userInfo.Uid), 10),
							partition,
							util.ResourceViewToTres(partitionResourceLimit.MaxTres),
							util.ResourceViewToTres(partitionResourceLimit.MaxTresPerJob),
							maxJobsStr,
							maxSubmitJobsStr,
							maxWallStr,
							maxWallPerJobStr,
						})
					}
				}
			}
		}
	}

	if !FlagFull && FlagFormat == "" {
		util.TrimTable(&tableData)
	}

	table.AppendBulk(tableData)
	table.Render()
}

func UserDefaultOutput(tableCtx *Tableoutput, userMap map[string][]*protos.UserInfo) {
	tableCtx.header = []string{"Account", "UserName", "Uid", "AllowedPartition", "AllowedQosList", "DefaultQos", "Coordinated", "AdminLevel", "Blocked"}
	for _, value := range userMap {
		for _, userInfo := range value {
			if len(userInfo.AllowedPartitionQosList) == 0 {
				tableCtx.tableData = append(tableCtx.tableData, []string{
					userInfo.Account,
					userInfo.Name,
					strconv.FormatUint(uint64(userInfo.Uid), 10),
					"",
					"",
					"",
					"",
					fmt.Sprintf("%v", userInfo.AdminLevel),
					strconv.FormatBool(userInfo.Blocked),
				})
			}
			for _, allowedPartitionQos := range userInfo.AllowedPartitionQosList {
				tableCtx.tableData = append(tableCtx.tableData, []string{
					userInfo.Account,
					userInfo.Name,
					strconv.FormatUint(uint64(userInfo.Uid), 10),
					allowedPartitionQos.PartitionName,
					strings.Join(allowedPartitionQos.QosList, ", "),
					allowedPartitionQos.DefaultQos,
					strings.Join(userInfo.CoordinatorAccounts, ", "),
					fmt.Sprintf("%v", userInfo.AdminLevel),
					strconv.FormatBool(userInfo.Blocked),
				})
			}
		}
	}
}

func CalcuTotalRows(userMap map[string][]*protos.UserInfo) int {
	totalRows := 0
	for _, users := range userMap {
		for _, userInfo := range users {
			if len(userInfo.AllowedPartitionQosList) == 0 {
				totalRows++
			} else {
				totalRows += len(userInfo.AllowedPartitionQosList)
			}
		}
	}
	return totalRows
}

func UserFormatOutput(tableCtx *Tableoutput, userMap map[string][]*protos.UserInfo) error {
	formatReq := util.SplitString(FlagFormat, []string{" ", ","})
	tableOutputWidth := createSlice(len(formatReq), -1)
	tableOutputHeader := make([]string, len(formatReq))

	// calculate the total Rows
	totalRows := CalcuTotalRows(userMap)
	formatTableData := make([][]string, totalRows)
	for i := range formatTableData {
		formatTableData[i] = make([]string, 0, len(formatReq))
	}

	// fill the Datas
	for i := 0; i < len(formatReq); i++ {
		currentRow := 0 // current Row index
		switch strings.ToLower(formatReq[i]) {
		case "account":
			tableOutputHeader[i] = "Account"
			for _, users := range userMap {
				for _, userInfo := range users {
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow], userInfo.Account)
						currentRow++
					} else {
						for range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow], userInfo.Account)
							currentRow++
						}
					}
				}
			}
		case "username":
			tableOutputHeader[i] = "UserName"
			for _, users := range userMap {
				for _, userInfo := range users {
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow], userInfo.Name)
						currentRow++
					} else {
						for range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow], userInfo.Name)
							currentRow++
						}
					}
				}
			}
		case "uid":
			tableOutputHeader[i] = "Uid"
			for _, users := range userMap {
				for _, userInfo := range users {
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow],
							strconv.FormatUint(uint64(userInfo.Uid), 10))
						currentRow++
					} else {
						for range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow],
								strconv.FormatUint(uint64(userInfo.Uid), 10))
							currentRow++
						}
					}
				}
			}
		case "allowedpartition":
			tableOutputHeader[i] = "AllowedPartition"
			for _, users := range userMap {
				for _, userInfo := range users {
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow], "")
						currentRow++
					} else {
						for _, allowedPartitionQos := range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow],
								allowedPartitionQos.PartitionName)
							currentRow++
						}
					}
				}
			}
		case "allowedqoslist":
			tableOutputHeader[i] = "AllowedQosList"
			for _, users := range userMap {
				for _, userInfo := range users {
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow], "")
						currentRow++
					} else {
						for _, allowedPartitionQos := range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow],
								strings.Join(allowedPartitionQos.QosList, ", "))
							currentRow++
						}
					}
				}
			}
		case "defaultqos":
			tableOutputHeader[i] = "DefaultQos"
			for _, users := range userMap {
				for _, userInfo := range users {
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow], "")
						currentRow++
					} else {
						for _, allowedPartitionQos := range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow],
								allowedPartitionQos.DefaultQos)
							currentRow++
						}
					}
				}
			}
		case "coordinated":
			tableOutputHeader[i] = "Coordinated"
			for _, users := range userMap {
				for _, userInfo := range users {
					coordinators := strings.Join(userInfo.CoordinatorAccounts, ", ")
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow], coordinators)
						currentRow++
					} else {
						for range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow], coordinators)
							currentRow++
						}
					}
				}
			}
		case "adminlevel":
			tableOutputHeader[i] = "AdminLevel"
			for _, users := range userMap {
				for _, userInfo := range users {
					adminLevelStr := fmt.Sprintf("%v", userInfo.AdminLevel)
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow], adminLevelStr)
						currentRow++
					} else {
						for range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow], adminLevelStr)
							currentRow++
						}
					}
				}
			}
		case "blocked":
			tableOutputHeader[i] = "Blocked"
			for _, users := range userMap {
				for _, userInfo := range users {
					blockedStr := strconv.FormatBool(userInfo.Blocked)
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow], blockedStr)
						currentRow++
					} else {
						for range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow], blockedStr)
							currentRow++
						}
					}
				}
			}
		default:
			return util.NewCraneErr(util.ErrorInvalidFormat, fmt.Sprintf("Invalid format. You entered: '%s'", formatReq[i]))
		}
	}
	tableCtx.header, tableCtx.tableData = util.FormatTable(tableOutputWidth, tableOutputHeader, formatTableData)
	return nil
}

func PrintUserTable(userMap map[string][]*protos.UserInfo) error {
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)

	var tableCtx Tableoutput
	if FlagFormat != "" {
		if err := UserFormatOutput(&tableCtx, userMap); err != nil {
			return err
		}
	} else {
		UserDefaultOutput(&tableCtx, userMap)
	}
	if !FlagFull && FlagFormat == "" {
		util.TrimTable(&tableCtx.tableData)
	}

	table.SetHeader(tableCtx.header)
	table.AppendBulk(tableCtx.tableData)
	table.Render()
	return nil
}

func ShowUser(value string, account string) error {

	if FlagForce {
		log.Warning("--force flag is ignored for show operations")
	}

	var userList []string
	if value != "" {
		var err error
		userList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid user list specified: %s.\n", err)
		}
	}
	req := protos.QueryUserInfoRequest{Uid: userUid, UserList: userList, Account: account}
	reply, err := stub.QueryUserInfo(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to show user: %s", err)
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
			if richError.Description == "" {
				msg += fmt.Sprintln(util.ErrMsg(richError.Code))
				break
			}
			msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}

	if FlagShowPartitionLimit {
		PrintUserPartitionLimit(reply.UserList)
	}

	return PrintUserList(reply.UserList)
}

// Qos
func PrintQosList(qosList []*protos.QosInfo) error {
	if len(qosList) == 0 {
		return nil
	}
	sort.Slice(qosList, func(i, j int) bool {
		return qosList[i].Name < qosList[j].Name
	})

	return PrintTable(qosList)
}

func QosFormatOutput(tableCtx *Tableoutput, qosList []*protos.QosInfo) error {
	formatTableData := make([][]string, len(qosList))
	formatReq := util.SplitString(FlagFormat, []string{" ", ","})
	tableOutputWidth := createSlice(len(formatReq), -1)
	tableOutputHeader := make([]string, len(formatReq))

	for i := 0; i < len(formatReq); i++ {
		currentRow := 0
		switch strings.ToLower(formatReq[i]) {
		case "name":
			tableOutputHeader[i] = "Name"
			for _, info := range qosList {
				formatTableData[currentRow] = append(formatTableData[currentRow], info.Name)
				currentRow++
			}
		case "description":
			tableOutputHeader[i] = "Description"
			for _, info := range qosList {
				formatTableData[currentRow] = append(formatTableData[currentRow], info.Description)
				currentRow++
			}
		case "priority":
			tableOutputHeader[i] = "Priority"
			for _, info := range qosList {
				formatTableData[currentRow] = append(formatTableData[currentRow], fmt.Sprint(info.Priority))
				currentRow++
			}
		case "maxjobsperuser":
			tableOutputHeader[i] = "MaxJobsPerUser"
			for _, info := range qosList {
				var jobsPerUserStr string
				if info.MaxJobsPerUser == math.MaxUint32 {
					jobsPerUserStr = "unlimited"
				} else {
					jobsPerUserStr = strconv.FormatUint(uint64(info.MaxJobsPerUser), 10)
				}
				formatTableData[currentRow] = append(formatTableData[currentRow], fmt.Sprint(jobsPerUserStr))
				currentRow++
			}
		case "maxcpusperuser":
			tableOutputHeader[i] = "MaxCpusPerUser"
			for _, info := range qosList {
				var cpusPerUserStr string
				if info.MaxCpusPerUser >= util.UnlimitedCpuThreshold {
					cpusPerUserStr = "unlimited"
				} else {
					cpusPerUserStr = strconv.FormatFloat(info.MaxCpusPerUser, 'f', -1, 64)
				}
				formatTableData[currentRow] = append(formatTableData[currentRow], fmt.Sprint(cpusPerUserStr))
				currentRow++
			}
		case "maxjobsperaccount":
			tableOutputHeader[i] = "MaxJobsPerAccount"
			for _, info := range qosList {
				var jobsPerAccountStr string
				if info.MaxJobsPerAccount == math.MaxUint32 {
					jobsPerAccountStr = "unlimited"
				} else {
					jobsPerAccountStr = strconv.FormatUint(uint64(info.MaxJobsPerAccount), 10)
				}
				formatTableData[currentRow] = append(formatTableData[currentRow], fmt.Sprint(jobsPerAccountStr))
				currentRow++
			}
		case "maxsubmitjobsperuser":
			tableOutputHeader[i] = "MaxSubmitJobsPerUser"
			for _, info := range qosList {
				var submitJobsPerUserStr string
				if info.MaxSubmitJobsPerUser == math.MaxUint32 {
					submitJobsPerUserStr = "unlimited"
				} else {
					submitJobsPerUserStr = strconv.FormatUint(uint64(info.MaxSubmitJobsPerUser), 10)
				}
				formatTableData[currentRow] = append(formatTableData[currentRow], submitJobsPerUserStr)
				currentRow++
			}
		case "maxsubmitjobsperaccount":
			tableOutputHeader[i] = "MaxSubmitJobsPerAccount"
			for _, info := range qosList {
				var submitJobsPerAccountStr string
				if info.MaxSubmitJobsPerAccount == math.MaxUint32 {
					submitJobsPerAccountStr = "unlimited"
				} else {
					submitJobsPerAccountStr = strconv.FormatUint(uint64(info.MaxSubmitJobsPerAccount), 10)
				}
				formatTableData[currentRow] = append(formatTableData[currentRow], submitJobsPerAccountStr)
				currentRow++
			}
		case "maxtresperuser":
			tableOutputHeader[i] = "MaxTresPerUser"
			for _, info := range qosList {
				formatTableData[currentRow] = append(formatTableData[currentRow], util.ResourceViewToTres(info.MaxTresPerUser))
				currentRow++
			}
		case "maxtresperaccount":
			tableOutputHeader[i] = "MaxTresPerAccount"
			for _, info := range qosList {
				formatTableData[currentRow] = append(formatTableData[currentRow], util.ResourceViewToTres(info.MaxTresPerAccount))
				currentRow++
			}
		case "maxtres":
			tableOutputHeader[i] = "MaxTres"
			for _, info := range qosList {
				formatTableData[currentRow] = append(formatTableData[currentRow], util.ResourceViewToTres(info.MaxTres))
				currentRow++
			}
		case "maxjobs":
			tableOutputHeader[i] = "MaxJobs"
			for _, info := range qosList {
				var maxJobsStr string
				if info.MaxJobs == math.MaxUint32 {
					maxJobsStr = "unlimited"
				} else {
					maxJobsStr = strconv.FormatUint(uint64(info.MaxJobs), 10)
				}
				formatTableData[currentRow] = append(formatTableData[currentRow], maxJobsStr)
				currentRow++
			}
		case "maxsubmitjobs":
			tableOutputHeader[i] = "MaxSubmitJobs"
			for _, info := range qosList {
				var maxSubmitJobsStr string
				if info.MaxSubmitJobs == math.MaxUint32 {
					maxSubmitJobsStr = "unlimited"
				} else {
					maxSubmitJobsStr = strconv.FormatUint(uint64(info.MaxSubmitJobs), 10)
				}
				formatTableData[currentRow] = append(formatTableData[currentRow], maxSubmitJobsStr)
				currentRow++
			}
		case "maxwall":
			tableOutputHeader[i] = "MaxWall"
			for _, info := range qosList {
				var maxWallStr string
				if info.MaxWall == 0 {
					maxWallStr = "unlimited"
				} else {
					maxWallStr = util.SecondTimeFormat(int64(info.MaxWall))
				}
				formatTableData[currentRow] = append(formatTableData[currentRow], maxWallStr)
				currentRow++
			}
		case "flags":
			tableOutputHeader[i] = "Flags"
			for _, info := range qosList {
				var qosFlagStr string
				if info.Flags == 0 {
					qosFlagStr = "None"
				} else {
					qosFlagStr = "DenyOnLimit"
				}
				formatTableData[currentRow] = append(formatTableData[currentRow], qosFlagStr)
				currentRow++
			}
		case "maxtimelimitperjob":
			tableOutputHeader[i] = "MaxTimeLimitPerJob"
			for _, info := range qosList {
				var timeLimitStr string
				if info.MaxTimeLimitPerJob >= util.MaxJobTimeLimit {
					timeLimitStr = "unlimited"
				} else {
					timeLimitStr = util.SecondTimeFormat(int64(info.MaxTimeLimitPerJob))
				}
				formatTableData[currentRow] = append(formatTableData[currentRow], fmt.Sprint(timeLimitStr))
				currentRow++
			}
		case "preempt":
			tableOutputHeader[i] = "Preempt"
			for _, info := range qosList {
				preemptStr := "-"
				if len(info.Preempt) > 0 {
					preemptStr = strings.Join(info.Preempt, ",")
				}
				formatTableData[currentRow] = append(formatTableData[currentRow], preemptStr)
				currentRow++
			}
		case "preemptmode":
			tableOutputHeader[i] = "PreemptMode"
			for _, info := range qosList {
				formatTableData[currentRow] = append(formatTableData[currentRow], formatPreemptMode(info.PreemptMode))
				currentRow++
			}
		default:
			return util.NewCraneErr(util.ErrorInvalidFormat, fmt.Sprintf("Invalid format. You entered: '%s'", formatReq[i]))
		}
	}
	tableCtx.header, tableCtx.tableData = util.FormatTable(tableOutputWidth, tableOutputHeader, formatTableData)
	return nil
}

func QosDefaultOutput(tableCtx *Tableoutput, qosList []*protos.QosInfo) {
	tableCtx.header = []string{"Name", "Description", "Priority", "MaxJobsPerUser", "MaxCpusPerUser",
		"MaxJobsPerAccount", "MaxSubmitJobsPerUser", "MaxSubmitJobsPerAccount", "MaxTresPerUser", "MaxTresPerAccount",
		"MaxTres", "MaxJobs", "MaxSubmitJobs", "MaxWall", "MaxTimeLimitPerJob", "Flags", "Preempt", "PreemptMode"}
	tableCtx.tableData = make([][]string, 0, len(qosList))
	for _, info := range qosList {
		var timeLimitStr string
		if info.MaxTimeLimitPerJob >= util.MaxJobTimeLimit {
			timeLimitStr = "unlimited"
		} else {
			timeLimitStr = util.SecondTimeFormat(int64(info.MaxTimeLimitPerJob))
		}
		var jobsPerUserStr string
		if info.MaxJobsPerUser == math.MaxUint32 {
			jobsPerUserStr = "unlimited"
		} else {
			jobsPerUserStr = strconv.FormatUint(uint64(info.MaxJobsPerUser), 10)
		}
		var cpusPerUserStr string
		if info.MaxCpusPerUser >= util.UnlimitedCpuThreshold {
			cpusPerUserStr = "unlimited"
		} else {
			cpusPerUserStr = strconv.FormatFloat(info.MaxCpusPerUser, 'f', -1, 64)
		}
		var jobsPerAccountStr string
		if info.MaxJobsPerAccount == math.MaxUint32 {
			jobsPerAccountStr = "unlimited"
		} else {
			jobsPerAccountStr = strconv.FormatUint(uint64(info.MaxJobsPerAccount), 10)
		}
		var submitJobsPerUserStr string
		if info.MaxSubmitJobsPerUser == math.MaxUint32 {
			submitJobsPerUserStr = "unlimited"
		} else {
			submitJobsPerUserStr = strconv.FormatUint(uint64(info.MaxSubmitJobsPerUser), 10)
		}
		var submitJobsPerAccountStr string
		if info.MaxSubmitJobsPerAccount == math.MaxUint32 {
			submitJobsPerAccountStr = "unlimited"
		} else {
			submitJobsPerAccountStr = strconv.FormatUint(uint64(info.MaxSubmitJobsPerAccount), 10)
		}
		var maxJobsStr string
		if info.MaxJobs == math.MaxUint32 {
			maxJobsStr = "unlimited"
		} else {
			maxJobsStr = strconv.FormatUint(uint64(info.MaxJobs), 10)
		}
		var maxSubmitJobsStr string
		if info.MaxSubmitJobs == math.MaxUint32 {
			maxSubmitJobsStr = "unlimited"
		} else {
			maxSubmitJobsStr = strconv.FormatUint(uint64(info.MaxSubmitJobs), 10)
		}
		var maxWallStr string
		if info.MaxWall == 0 {
			maxWallStr = "unlimited"
		} else {
			maxWallStr = util.SecondTimeFormat(int64(info.MaxWall))
		}

		var qosFlagStr string
		if info.Flags == 0 {
			qosFlagStr = "None"
		} else {
			qosFlagStr = "DenyOnLimit"
		}

		preemptStr := "-"
		if len(info.Preempt) > 0 {
			preemptStr = strings.Join(info.Preempt, ",")
		}
		preemptModeStr := formatPreemptMode(info.PreemptMode)

		tableCtx.tableData = append(tableCtx.tableData, []string{
			info.Name,
			info.Description,
			fmt.Sprint(info.Priority),
			fmt.Sprint(jobsPerUserStr),
			fmt.Sprint(cpusPerUserStr),
			fmt.Sprint(jobsPerAccountStr),
			fmt.Sprint(submitJobsPerUserStr),
			fmt.Sprint(submitJobsPerAccountStr),
			util.ResourceViewToTres(info.MaxTresPerUser),
			util.ResourceViewToTres(info.MaxTresPerAccount),
			util.ResourceViewToTres(info.MaxTres),
			maxJobsStr,
			maxSubmitJobsStr,
			maxWallStr,
			fmt.Sprint(timeLimitStr),
			qosFlagStr,
			preemptStr,
			preemptModeStr})
	}
}

func formatPreemptMode(m protos.PreemptMode) string {
	switch m {
	case protos.PreemptMode_PREEMPT_MODE_OFF:
		return "OFF"
	case protos.PreemptMode_PREEMPT_MODE_CANCEL:
		return "CANCEL"
	case protos.PreemptMode_PREEMPT_MODE_REQUEUE:
		return "REQUEUE"
	case protos.PreemptMode_PREEMPT_MODE_SUSPEND:
		return "SUSPEND"
	default:
		return "INVALID"
	}
}

func PrintTable(qosList []*protos.QosInfo) error {
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)

	var tableCtx Tableoutput
	if FlagFormat != "" {
		if err := QosFormatOutput(&tableCtx, qosList); err != nil {
			return err
		}
	} else {
		QosDefaultOutput(&tableCtx, qosList)
	}

	if !FlagFull && FlagFormat == "" {
		util.TrimTable(&tableCtx.tableData)
	}

	table.SetHeader(tableCtx.header)
	table.AppendBulk(tableCtx.tableData)
	table.Render()
	return nil
}

func ShowQos(value string) error {
	if FlagForce {
		log.Warning("--force flag is ignored for show operations")
	}
	var qosList []string
	if value != "" {
		var err error
		qosList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid QoS list specified: %s.\n", err)
		}
	}
	req := protos.QueryQosInfoRequest{Uid: userUid, QosList: qosList}
	reply, err := stub.QueryQosInfo(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to show QoS: %s", err)
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
			if richError.Description == "" {
				msg += fmt.Sprintln(util.ErrMsg(richError.Code))
				break
			}
			msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}

	return PrintQosList(reply.QosList)
}

func PrintWckeyList(wckeyList []*protos.QueryWckeyInfo) {
	sort.Slice(wckeyList, func(i, j int) bool {
		return wckeyList[i].UserName < wckeyList[j].UserName
	})

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader([]string{"Name", "Cluster", "User"})

	tableData := make([][]string, 0, len(wckeyList))
	for _, wckey := range wckeyList {
		name := wckey.Name
		if wckey.IsDefault {
			name = "*" + name
		}
		tableData = append(tableData, []string{
			name,
			wckey.Cluster,
			wckey.UserName,
		})
	}

	table.AppendBulk(tableData)
	table.Render()
}

func ShowWckey(wckeyStr string) error {
	var wckeyList []string
	if wckeyStr != "" {
		var err error
		wckeyList, err = util.ParseStringParamList(wckeyStr, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid wckey list specified: %v.\n", err)
		}
	}
	req := protos.QueryWckeyInfoRequest{Uid: userUid, WckeyList: wckeyList}
	reply, err := stub.QueryWckeyInfo(context.Background(), &req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to show the wckey")
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
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Show wckey err: %s \n", util.ErrMsg(reply.Code)))
	}

	PrintWckeyList(reply.WckeyList)

	return nil
}
