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

package creport

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"encoding/json"
	"fmt"
	"os"

	"io"
	"os/user"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	stub protos.CraneCtldClient
)

type CheckStatus int

const (
	CheckAccountUserStatus = iota
	CheckUserAccountStatus
	CheckUserWckeyStatus
	CheckWckeyUserStatus
	CheckAccountQosStatus
	CheckClusterStatus
	CheckAccountCpusStatus
	CheckWckeyCpusStatus
	CheckAccountWckeyCpusStatus
	StatusFailed
)

type UserTopSummaryJson struct {
	Cluster      string   `json:"cluster"`
	Username     string   `json:"login"`
	ProperName   string   `json:"proper_name"`
	Accounts     []string `json:"accounts"`
	TotalCpuTime float64  `json:"used"`
	Energy       string   `json:"energy"`
}

type AccountUserJson struct {
	Cluster    string  `json:"cluster"`
	Account    string  `json:"account"`
	Username   string  `json:"login"`
	ProperName string  `json:"proper_name"`
	Used       float64 `json:"used"`
	Energy     string  `json:"energy"`
}
type UserWckeyJson struct {
	Cluster    string  `json:"cluster"`
	Username   string  `json:"login"`
	ProperName string  `json:"proper_name"`
	Wckey      string  `json:"wckey"`
	Used       float64 `json:"used"`
}

type AccountQosJson struct {
	Cluster string  `json:"cluster"`
	Account string  `json:"account"`
	Qos     string  `json:"qos"`
	Used    float64 `json:"used"`
	Energy  string  `json:"energy"`
}

type ClusterUtilizationJson struct {
	Cluster  string  `json:"cluster"`
	Allocate float64 `json:"allocate"`
	Down     string  `json:"down"`
	Planned  string  `json:"planned"`
	Reported string  `json:"reported"`
}

func GetDefaultStartTime() string {
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	zero := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, yesterday.Location())
	return zero.UTC().Format("2006-01-02T15:04:05")
}

func GetDefaultEndTime() string {
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	endTime := time.Date(
		yesterday.Year(), yesterday.Month(), yesterday.Day(),
		23, 59, 59, 0, yesterday.Location())
	return endTime.UTC().Format("2006-01-02T15:04:05")
}

func QueryUsersTopSummaryItem() error {
	request := &protos.QueryJobSummaryRequest{ReportType: protos.QueryJobSummaryRequest_USER_TOP_USAGE}
	request.NumLimit = FlagTopCount
	if FlagFilterAccounts != "" {
		filterAccountList, err := util.ParseStringParamList(FlagFilterAccounts, ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid account list specified: %s.", err))
		}
		request.FilterAccounts = filterAccountList
	}

	if FlagFilterUsers != "" {
		filterUserList, err := util.ParseStringParamList(FlagFilterUsers, ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid user list specified: %s.", err))
		}
		request.FilterUsers = filterUserList
	}

	var start_time, end_time time.Time
	var err error
	if FlagFilterStartTime != "" {
		start_time, err = util.ParseTime(FlagFilterStartTime)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Failed to parse the StartTime filter: %s.", err))
		}
		request.FilterStartTime = timestamppb.New(start_time)
	}

	if FlagFilterEndTime != "" {
		end_time, err = util.ParseTime(FlagFilterEndTime)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Failed to parse the EndTime filter: %s.", err))
		}
		request.FilterEndTime = timestamppb.New(end_time)
		if request.FilterStartTime != nil {
			start := request.FilterStartTime.AsTime()
			end := request.FilterEndTime.AsTime()
			if !end.After(start) {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("End_time %s must be after start_time %s", FlagFilterEndTime, FlagFilterStartTime))
			}
		}
	}

	if FlagOutType != "" {
		if !CheckCreportOutType(FlagOutType) {
			return util.NewCraneErr(util.ErrorCmdArg, "Invalid --time/-t, please input seconds/minutes/hours")
		}
	}

	stream, err := stub.QueryJobSummary(context.Background(), request)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to query AccountUserSummary info")
	}

	var JobSummaryItemList []*protos.JobSummaryItem
	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to receive item")
		}
		JobSummaryItemList = append(JobSummaryItemList, batch.ItemList...)
	}
	PrintUsersTopSumList(JobSummaryItemList, start_time, end_time, FlagGroupSet, FlagJson)

	return nil
}

func QueryJobSummary(reportType protos.QueryJobSummaryRequest_JobSummaryReportType) error {
	request := &protos.QueryJobSummaryRequest{ReportType: reportType}
	if FlagFilterAccounts != "" {
		filterAccountList, err := util.ParseStringParamList(FlagFilterAccounts, ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid account list specified: %s.", err))
		}
		request.FilterAccounts = filterAccountList
	}

	if FlagFilterUsers != "" {
		filterUserList, err := util.ParseStringParamList(FlagFilterUsers, ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid user list specified: %s.", err))
		}
		request.FilterUsers = filterUserList
	}

	if FlagFilterQosList != "" {
		filterQosList, err := util.ParseStringParamList(FlagFilterQosList, ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid qos list specified: %s.", err))
		}
		request.FilterQoss = filterQosList
	}

	if FlagFilterGids != "" {
		if FlagFilterGids == "individual" {
			request.FilterGids = []uint32{}
		} else {
			gidStrs := strings.Split(FlagFilterGids, ",")
			gids := make([]uint32, 0)
			for _, gidStr := range gidStrs {
				val, err := strconv.ParseUint(gidStr, 10, 32)
				if err != nil {
					if err != nil {
						return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid gid list specified: %s.", err))
					}
				}
				gids = append(gids, uint32(val))
			}

			request.FilterGids = gids
		}
	}

	var start_time, end_time time.Time
	var err error
	if FlagFilterStartTime != "" {
		start_time, err = util.ParseTime(FlagFilterStartTime)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Failed to parse the StartTime filter: %s.", err))
		}
		request.FilterStartTime = timestamppb.New(start_time)
	}

	if FlagFilterEndTime != "" {
		end_time, err = util.ParseTime(FlagFilterEndTime)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Failed to parse the EndTime filter: %s.", err))
		}
		request.FilterEndTime = timestamppb.New(end_time)
		if request.FilterStartTime != nil {
			start := request.FilterStartTime.AsTime()
			end := request.FilterEndTime.AsTime()
			if !end.After(start) {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("End_time %s must be after start_time %s", FlagFilterEndTime, FlagFilterStartTime))
			}
		}
	}

	if FlagOutType != "" {
		if !CheckCreportOutType(FlagOutType) {
			return util.NewCraneErr(util.ErrorCmdArg, "Invalid --time/-t, please input seconds/minutes/hours")
		}
	}

	stream, err := stub.QueryJobSummary(context.Background(), request)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to query AccountUserSummary info")
	}

	var JobSummaryItemList []*protos.JobSummaryItem
	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to receive item")
		}
		JobSummaryItemList = append(JobSummaryItemList, batch.ItemList...)
	}

	switch reportType {
	case protos.QueryJobSummaryRequest_ACCOUNT_UTILIZATION_BY_USER:
		PrintAccountUserSummary(JobSummaryItemList, start_time, end_time, FlagJson, true)
	case protos.QueryJobSummaryRequest_USER_UTILIZATION_BY_ACCOUNT:
		PrintAccountUserSummary(JobSummaryItemList, start_time, end_time, FlagJson, false)
	// case protos.QueryJobSummaryRequest_CLUSTER_STATUS:
	// 	PrintClusterList(JobSummaryItemList, start_time, end_time, FlagJson)
	case protos.QueryJobSummaryRequest_ACCOUNT_UTILIZATION_BY_QOS:
		PrintAccountQosList(JobSummaryItemList, start_time, end_time, FlagJson)
	case protos.QueryJobSummaryRequest_USER_UTILIZATION_BY_WCKEY:
		PrintUserWckeySummary(JobSummaryItemList, start_time, end_time, FlagJson, true)
	case protos.QueryJobSummaryRequest_WCKEY_UTILIZATION_BY_USER:
		PrintUserWckeySummary(JobSummaryItemList, start_time, end_time, FlagJson, false)
	default:
		return util.NewCraneErr(util.ErrorCmdArg, "Unsupported cmd")
	}

	return nil
}

func PrintUsersTopSumList(JobSummaryItemList []*protos.JobSummaryItem, startTime, endTime time.Time, group bool, isJson bool) {
	sort.Slice(JobSummaryItemList, func(i, j int) bool {
		return JobSummaryItemList[i].TotalCpuTime > JobSummaryItemList[j].TotalCpuTime
	})

	countMax := FlagTopCount
	divisor := ReportUsageDivisor(FlagOutType)
	usernameToName := make(map[string]string)
	outputList := make([]UserTopSummaryJson, 0, countMax)

	if group {
		// Group by username, merge accounts and sum total CPU time
		userSummaryMap := make(map[string]*UserTopSummaryJson)
		for _, item := range JobSummaryItemList {
			summary, exists := userSummaryMap[item.Username]
			if !exists {
				summary = &UserTopSummaryJson{
					Cluster:      item.Cluster,
					Username:     item.Username,
					Accounts:     []string{},
					TotalCpuTime: 0,
					Energy:       "0",
				}
				userSummaryMap[item.Username] = summary
			}
			// Avoid duplicate accounts
			accountSet := make(map[string]struct{})
			for _, acc := range summary.Accounts {
				accountSet[acc] = struct{}{}
			}
			if _, found := accountSet[item.Account]; !found {
				summary.Accounts = append(summary.Accounts, item.Account)
			}
			summary.TotalCpuTime += item.TotalCpuTime
		}
		// Convert map to slice and sort by TotalCpuTime descending
		groupedList := make([]*UserTopSummaryJson, 0, len(userSummaryMap))
		for _, v := range userSummaryMap {
			groupedList = append(groupedList, v)
		}
		sort.Slice(groupedList, func(i, j int) bool {
			return groupedList[i].TotalCpuTime > groupedList[j].TotalCpuTime
		})

		for count, summary := range groupedList {
			if count >= int(countMax) {
				break
			}
			properName, ok := usernameToName[summary.Username]
			if !ok {
				properName = ""
				if usr, err := user.Lookup(summary.Username); err == nil {
					properName = usr.Name
				}
				usernameToName[summary.Username] = properName
			}
			summary.ProperName = properName
			sort.Strings(summary.Accounts)
			summary.TotalCpuTime = summary.TotalCpuTime / float64(divisor)
			outputList = append(outputList, *summary)
		}
	} else {
		// No grouping, original logic
		for count, item := range JobSummaryItemList {
			if count >= int(countMax) {
				break
			}
			properName, ok := usernameToName[item.Username]
			if !ok {
				properName = ""
				if usr, err := user.Lookup(item.Username); err == nil {
					properName = usr.Name
				}
				usernameToName[item.Username] = properName
			}
			outputList = append(outputList, UserTopSummaryJson{
				Cluster:      item.Cluster,
				Username:     item.Username,
				ProperName:   properName,
				Accounts:     []string{item.Account},
				TotalCpuTime: item.TotalCpuTime / float64(divisor),
				Energy:       "0",
			})
		}
	}

	// Output as JSON or table
	if isJson {
		PrintAsJsonToStdout(outputList)
		return
	}

	PrintUserTopTable(outputList, startTime, endTime, countMax)
}

// PrintUserTopTable prints the user summary as a formatted table
func PrintUserTopTable(outputList []UserTopSummaryJson, startTime, endTime time.Time, countMax uint32) {
	fmt.Println(strings.Repeat("-", 100))
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	if totalSecs > 0 {
		fmt.Printf("Top %v Users %s - %s (%d secs)\n",
			countMax, startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	PrintUsageTypeInfo(FlagOutType, false, false)
	header := []string{"Cluster", "Login", "Proper_name", "Account", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, 0, len(outputList))
	for _, summary := range outputList {
		accountStr := strings.Join(summary.Accounts, ",")
		tableData = append(tableData, []string{
			summary.Cluster,
			summary.Username,
			summary.ProperName,
			accountStr,
			strconv.FormatFloat(summary.TotalCpuTime, 'f', 2, 64),
			summary.Energy,
		})
	}
	table.AppendBulk(tableData)
	table.Render()
}

// PrintAccountUserTable prints a list of AccountUserJson in table format
func PrintAccountUserTable(outputList []AccountUserJson, startTime, endTime time.Time, header []string, title string) {
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("%s %s - %s (%d secs)\n",
			title, startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	PrintUsageTypeInfo(FlagOutType, false, false)
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, 0, len(outputList))
	for _, row := range outputList {
		var rowSlice []string
		for _, col := range header { // Use header as column order
			switch col {
			case "Cluster":
				rowSlice = append(rowSlice, row.Cluster)
			case "Account":
				rowSlice = append(rowSlice, row.Account)
			case "Login":
				rowSlice = append(rowSlice, row.Username)
			case "Proper_name":
				rowSlice = append(rowSlice, row.ProperName)
			case "Used":
				rowSlice = append(rowSlice, strconv.FormatFloat(row.Used, 'f', 2, 64))
			case "Energy":
				rowSlice = append(rowSlice, row.Energy)
			}
		}
		tableData = append(tableData, rowSlice)
	}
	table.AppendBulk(tableData)
	table.Render()
}

// PrintAccountUserSummary summarizes job usage by account and user, and prints as table or JSON
func PrintAccountUserSummary(JobSummaryItemList []*protos.JobSummaryItem, startTime, endTime time.Time, isJson bool, sortByAccountFirst bool) {
	divisor := ReportUsageDivisor(FlagOutType)
	usernameToName := make(map[string]string)
	outputList := []AccountUserJson{}

	// Sorting
	if sortByAccountFirst {
		sort.Slice(JobSummaryItemList, func(i, j int) bool {
			if JobSummaryItemList[i].Account < JobSummaryItemList[j].Account {
				return true
			}
			if JobSummaryItemList[i].Account > JobSummaryItemList[j].Account {
				return false
			}
			return JobSummaryItemList[i].Username < JobSummaryItemList[j].Username
		})
	} else {
		sort.Slice(JobSummaryItemList, func(i, j int) bool {
			if JobSummaryItemList[i].Username < JobSummaryItemList[j].Username {
				return true
			}
			if JobSummaryItemList[i].Username > JobSummaryItemList[j].Username {
				return false
			}
			return JobSummaryItemList[i].Account < JobSummaryItemList[j].Account
		})
	}

	// Build output list
	for _, item := range JobSummaryItemList {
		properName, ok := usernameToName[item.Username]
		if !ok {
			usr, err := user.Lookup(item.Username)
			if err != nil {
				continue // skip this user if lookup failed
			}
			properName = usr.Name
			usernameToName[item.Username] = properName
		}
		outputList = append(outputList, AccountUserJson{
			Cluster:    item.Cluster,
			Account:    item.Account,
			Username:   item.Username,
			ProperName: properName,
			Used:       float64(item.TotalCpuTime) / float64(divisor),
			Energy:     "0",
		})
	}

	// Output as JSON or table
	if isJson {
		PrintAsJsonToStdout(outputList)
		return
	}

	// Table output
	if sortByAccountFirst {
		header := []string{"Cluster", "Account", "Login", "Proper_name", "Used", "Energy"}
		title := "Cluster/Account/User Utilization"
		PrintAccountUserTable(outputList, startTime, endTime, header, title)
	} else {
		header := []string{"Cluster", "Login", "Proper_name", "Account", "Used", "Energy"}
		title := "Cluster/User/Account Utilization"
		PrintAccountUserTable(outputList, startTime, endTime, header, title)
	}
}

func PrintUserWckeyTable(outputList []UserWckeyJson, startTime, endTime time.Time, header []string, title string) {
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("%s %s - %s (%d secs)\n",
			title, startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	PrintUsageTypeInfo(FlagOutType, false, false)
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	for _, row := range outputList {
		var rowSlice []string
		for _, col := range header { // Use header as column order
			switch col {
			case "Cluster":
				rowSlice = append(rowSlice, row.Cluster)
			case "Login":
				rowSlice = append(rowSlice, row.Username)
			case "Proper_name":
				rowSlice = append(rowSlice, row.ProperName)
			case "Wckey":
				rowSlice = append(rowSlice, row.Wckey)
			case "Used":
				rowSlice = append(rowSlice, strconv.FormatFloat(row.Used, 'f', 2, 64))
			}
		}
		table.Append(rowSlice)
	}
	table.Render()
}

func PrintUserWckeySummary(itemList []*protos.JobSummaryItem, startTime, endTime time.Time, isJson bool, sortByUserFirst bool) {
	divisor := ReportUsageDivisor(FlagOutType)
	usernameToName := make(map[string]string)
	outputList := []UserWckeyJson{}

	// Sorting
	if sortByUserFirst {
		sort.Slice(itemList, func(i, j int) bool {
			if itemList[i].Username < itemList[j].Username {
				return true
			}
			if itemList[i].Username > itemList[j].Username {
				return false
			}
			return itemList[i].Wckey < itemList[j].Wckey
		})
	} else {
		sort.Slice(itemList, func(i, j int) bool {
			if itemList[i].Wckey < itemList[j].Wckey {
				return true
			}
			if itemList[i].Wckey > itemList[j].Wckey {
				return false
			}
			return itemList[i].Username < itemList[j].Username
		})
	}

	// Build output list
	for _, item := range itemList {
		properName, ok := usernameToName[item.Username]
		if !ok {
			usr, err := user.Lookup(item.Username)
			if err != nil {
				continue // skip if lookup failed
			}
			properName = usr.Name
			usernameToName[item.Username] = properName
		}
		outputList = append(outputList, UserWckeyJson{
			Cluster:    item.Cluster,
			Username:   item.Username,
			ProperName: properName,
			Wckey:      item.Wckey,
			Used:       float64(item.TotalCpuTime) / float64(divisor),
		})
	}

	if isJson {
		PrintAsJsonToStdout(outputList)
		return
	}

	// Table output
	if sortByUserFirst {
		header := []string{"Cluster", "Login", "Proper_name", "Wckey", "Used"}
		title := "Cluster/User/Wckey Utilization"
		PrintUserWckeyTable(outputList, startTime, endTime, header, title)
	} else {
		header := []string{"Cluster", "Wckey", "Login", "Proper_name", "Used"}
		title := "Cluster/Wckey/User Utilization"
		PrintUserWckeyTable(outputList, startTime, endTime, header, title)
	}
}

func PrintAccountQosList(JobSummaryItemList []*protos.JobSummaryItem, startTime, endTime time.Time, isJson bool) {
	sort.Slice(JobSummaryItemList, func(i, j int) bool {
		if JobSummaryItemList[i].Account < JobSummaryItemList[j].Account {
			return true
		}
		if JobSummaryItemList[i].Account > JobSummaryItemList[j].Account {
			return false
		}
		return JobSummaryItemList[i].Qos < JobSummaryItemList[j].Qos
	})

	divisor := ReportUsageDivisor(FlagOutType)
	outputList := []AccountQosJson{}

	for _, item := range JobSummaryItemList {
		outputList = append(outputList, AccountQosJson{
			Cluster: item.Cluster,
			Account: item.Account,
			Qos:     item.Qos,
			Used:    float64(item.TotalCpuTime) / float64(divisor),
			Energy:  "0",
		})
	}

	if isJson {
		PrintAsJsonToStdout(outputList)
		return
	}

	// Table output
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster/Account/Qos Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	PrintUsageTypeInfo(FlagOutType, false, false)
	header := []string{"Cluster", "Account", "Qos", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, 0, len(outputList))
	for _, row := range outputList {
		tableData = append(tableData, []string{
			row.Cluster,
			row.Account,
			row.Qos,
			strconv.FormatFloat(row.Used, 'f', 2, 64),
			row.Energy,
		})
	}
	table.AppendBulk(tableData)
	table.Render()
}

func PrintClusterList(JobSummaryItemList []*protos.JobSummaryItem, startTime, endTime time.Time, isJson bool) {
	clusterMap := make(map[string]float64)
	for _, item := range JobSummaryItemList {
		clusterMap[item.Cluster] += item.TotalCpuTime
	}

	divisor := ReportUsageDivisor(FlagOutType)
	outputList := []ClusterUtilizationJson{}

	for cluster, totalCpuTime := range clusterMap {
		outputList = append(outputList, ClusterUtilizationJson{
			Cluster:  cluster,
			Allocate: float64(totalCpuTime) / float64(divisor),
			Down:     "-",
			Planned:  "-",
			Reported: "-",
		})
	}

	if isJson {
		enc := json.NewEncoder(os.Stdout)
		if err := enc.Encode(outputList); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to encode json: %v\n", err)
		}
		return
	}

	// Table output
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	PrintUsageTypeInfo(FlagOutType, false, false)
	header := []string{"Cluster", "Allocate", "Down", "Planned", "Reported"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, 0, len(outputList))
	for _, row := range outputList {
		tableData = append(tableData, []string{
			row.Cluster,
			strconv.FormatFloat(row.Allocate, 'f', 2, 64),
			row.Down,
			row.Planned,
			row.Reported,
		})
	}
	table.AppendBulk(tableData)
	table.Render()
}
