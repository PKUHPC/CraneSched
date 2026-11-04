/**
 * Copyright (c) 2026 Peking University and Peking University
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
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CpuLevelSummary struct {
	CpuTime  []float64
	JobCount []int64
}

type cpusSummaryEntry struct {
	Cluster          string
	Key              string
	GroupHeaders     []string
	GroupValues      []float64
	Total            float64
	PercentOfCluster float64
}

type AccountCpusSummaryJson struct {
	Cluster          string    `json:"cluster"`
	Account          string    `json:"account"`
	GroupHeaders     []string  `json:"group_headers"`
	GroupValues      []float64 `json:"group_values"` // for cpu time or job count
	Total            float64   `json:"total"`
	PercentOfCluster float64   `json:"percent_of_cluster"`
}

type WckeyCpusSummaryJson struct {
	Cluster          string    `json:"cluster"`
	Wckey            string    `json:"wckey"`
	GroupHeaders     []string  `json:"group_headers"`
	GroupValues      []float64 `json:"group_values"` // for cpu time or job count
	Total            float64   `json:"total"`
	PercentOfCluster float64   `json:"percent_of_cluster"`
}

type AccountWckeyCpusSummaryJson struct {
	Cluster          string    `json:"cluster"`
	AccountWckey     string    `json:"account_wckey"`
	GroupHeaders     []string  `json:"group_headers"`
	GroupValues      []float64 `json:"group_values"` // for cpu time or job count
	Total            float64   `json:"total"`
	PercentOfCluster float64   `json:"percent_of_cluster"`
}

func QueryJobSizeSummary(CheckType CheckStatus) error {

	request := &protos.QueryJobSizeSummaryRequest{}
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

	if FlagFilterGrouping != "" {
		result, err := ParseAndSortJobSizeList(FlagFilterGrouping)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid grouping: %s", err))
		}
		request.FilterGroupingList = result
	}

	if FlagFilterJobIDs != "" {
		filterJobIdList, err := util.ParseJobIdList(FlagFilterJobIDs, ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid job list specified: %s.", err))
		}
		request.FilterJobIds = filterJobIdList
	}

	if FlagFilterPartitions != "" {
		filterPartitionList, err := util.ParseStringParamList(FlagFilterPartitions, ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid partition list specified: %s.", err))
		}
		request.FilterPartitions = filterPartitionList
	}

	if FlagFilterNodeNames != "" {
		filterNodenameList, ok := util.ParseHostList(FlagFilterNodeNames)
		if !ok {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid node pattern: %s.", FlagFilterNodeNames))
		}
		request.FilterNodenameList = filterNodenameList
	}

	stream, err := stub.QueryJobSizeSummary(context.Background(), request)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to query JobSizeSummary info")
	}

	var JobSummaryItemList []*protos.JobSizeSummaryItem
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
	switch CheckType {
	case CheckAccountCpusStatus:
		PrintAccountCpusList(JobSummaryItemList, start_time, end_time, request.FilterGroupingList, FlagPrintJobCount, FlagJson)
	case CheckWckeyCpusStatus:
		PrintWckeyCpusList(JobSummaryItemList, start_time, end_time, request.FilterGroupingList, FlagPrintJobCount, FlagJson)
	case CheckAccountWckeyCpusStatus:
		PrintAccountWckeyCpusList(JobSummaryItemList, start_time, end_time, request.FilterGroupingList, FlagPrintJobCount, FlagJson)
	default:
		return util.NewCraneErr(util.ErrorCmdArg, "Invalid check type")
	}

	return nil
}

func ParseAndSortJobSizeList(input string) ([]uint32, error) {
	segments := strings.Split(input, ",")
	uniqueNumbers := make(map[uint32]struct{})
	for index, segment := range segments {
		trimmed := strings.TrimSpace(segment)
		if trimmed == "" {
			return nil, fmt.Errorf("empty value detected at position %d", index+1)
		}
		number, err := strconv.Atoi(trimmed)
		if err != nil {
			return nil, fmt.Errorf("invalid number '%s' at position %d", trimmed, index+1)
		}
		if number < 0 {
			return nil, fmt.Errorf("negative number '%s' at position %d", trimmed, index+1)
		}
		uniqueNumbers[uint32(number)] = struct{}{}
	}

	resultList := make([]uint32, 0, len(uniqueNumbers))
	for number := range uniqueNumbers {
		resultList = append(resultList, number)
	}
	sort.Slice(resultList, func(i, j int) bool { return resultList[i] < resultList[j] })

	if len(resultList) == 0 {
		return nil, fmt.Errorf("no valid job size thresholds provided")
	}

	return resultList, nil
}

func GetCpusGroupHeaders(groupList []uint32) []string {

	headers := []string{}

	for i := 0; i < len(groupList); i++ {
		var baseCpu uint32
		if i == 0 {
			baseCpu = 0
		} else {
			baseCpu = groupList[i-1]
		}
		headers = append(headers, fmt.Sprintf("%d-%d CPUs", baseCpu, groupList[i]-1))
	}
	headers = append(headers, fmt.Sprintf(">= %d CPUs", groupList[len(groupList)-1]))
	return headers
}

func FindCpuGroupIndex(cpusAlloc uint32, groupList []uint32) int {
	if len(groupList) == 1 {
		return 0
	}
	for i := 0; i < len(groupList)-1; i++ {
		if cpusAlloc < groupList[i+1] {
			return i
		}
	}
	return len(groupList) - 1
}

func fillTableGroupValue(tableRow *[]string, nGroupHeaders int, GroupValues *[]float64, totalCpuTime float64, percentOfCluster float64, printCount bool) {
	for _, v := range *GroupValues {
		if printCount {
			*tableRow = append(*tableRow, fmt.Sprintf("%.0f", v))
		} else {
			*tableRow = append(*tableRow, fmt.Sprintf("%.2f", v))
		}
	}
	if len(*GroupValues) < nGroupHeaders {
		for i := 0; i < nGroupHeaders-len(*GroupValues); i++ {
			*tableRow = append(*tableRow, "0")
		}
	}
	if printCount {
		*tableRow = append(*tableRow,
			fmt.Sprintf("%.0f", totalCpuTime),
			fmt.Sprintf("%.2f%%", percentOfCluster))
	} else {
		*tableRow = append(*tableRow,
			fmt.Sprintf("%.2f", totalCpuTime),
			fmt.Sprintf("%.2f%%", percentOfCluster))
	}
}

func aggregateCpusSummary(
	items []*protos.JobSizeSummaryItem,
	groupList []uint32,
	isPrintCount bool,
	keyExtractor func(*protos.JobSizeSummaryItem) string,
) []cpusSummaryEntry {
	clusterKeyMap := make(map[string]map[string]*CpuLevelSummary)
	clusterTotal := make(map[string]float64)

	cpuAllocSet := make(map[uint32]struct{})
	for _, item := range items {
		cpuAllocSet[item.CpusAlloc] = struct{}{}
	}
	actualGroupList := make([]uint32, 0, len(cpuAllocSet))
	for cpus := range cpuAllocSet {
		actualGroupList = append(actualGroupList, cpus)
	}
	sort.Slice(actualGroupList, func(i, j int) bool { return actualGroupList[i] < actualGroupList[j] })
	cpuAllocIndexMap := make(map[uint32]int)
	for i, v := range actualGroupList {
		cpuAllocIndexMap[v] = i
	}

	for _, item := range items {
		if _, ok := clusterKeyMap[item.Cluster]; !ok {
			clusterKeyMap[item.Cluster] = make(map[string]*CpuLevelSummary)
		}
		key := keyExtractor(item)
		if _, ok := clusterKeyMap[item.Cluster][key]; !ok {
			clusterKeyMap[item.Cluster][key] = &CpuLevelSummary{
				CpuTime:  make([]float64, len(actualGroupList)),
				JobCount: make([]int64, len(actualGroupList)),
			}
		}
		summary := clusterKeyMap[item.Cluster][key]
		groupIdx := cpuAllocIndexMap[item.CpusAlloc]
		if isPrintCount {
			summary.JobCount[groupIdx] += item.TotalCount
			clusterTotal[item.Cluster] += float64(item.TotalCount)
		} else {
			summary.CpuTime[groupIdx] += item.TotalCpuTime
			clusterTotal[item.Cluster] += item.TotalCpuTime
		}
	}

	divisor := ReportUsageDivisor(FlagOutType)

	var groupHeaders []string
	if len(groupList) == 0 {
		for _, cpu := range actualGroupList {
			groupHeaders = append(groupHeaders, fmt.Sprintf("%d CPUs", cpu))
		}
	} else {
		groupHeaders = GetCpusGroupHeaders(groupList)
	}

	var entries []cpusSummaryEntry
	clusters := make([]string, 0, len(clusterKeyMap))
	for cluster := range clusterKeyMap {
		clusters = append(clusters, cluster)
	}
	sort.Strings(clusters)

	for _, cluster := range clusters {
		keyMap := clusterKeyMap[cluster]
		keys := make([]string, 0, len(keyMap))
		for k := range keyMap {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, key := range keys {
			summary := keyMap[key]
			var total float64
			var groupValues []float64
			if isPrintCount {
				for _, cnt := range summary.JobCount {
					groupValues = append(groupValues, float64(cnt))
					total += float64(cnt)
				}
			} else {
				for _, cpu := range summary.CpuTime {
					groupValues = append(groupValues, cpu/divisor)
					total += cpu
				}
				total /= divisor
			}
			clusterTotalValue := clusterTotal[cluster]
			if !isPrintCount {
				clusterTotalValue /= divisor
			}
			percent := 0.0
			if clusterTotalValue > 0 {
				percent = total / clusterTotalValue * 100
			}
			entries = append(entries, cpusSummaryEntry{
				Cluster:          cluster,
				Key:              key,
				GroupHeaders:     groupHeaders,
				GroupValues:      groupValues,
				Total:            total,
				PercentOfCluster: percent,
			})
		}
	}
	return entries
}

func printCpusSummaryTable(
	entries []cpusSummaryEntry,
	startTime, endTime time.Time,
	banner string,
	keyHeader string,
	isPrintCount bool,
) {
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("%s %s - %s (%d secs)\n",
			banner,
			startTime.Format("2006-01-02T15:04:05"),
			endTime.Format("2006-01-02T15:04:05"),
			totalSecs)
	}
	PrintUsageTypeInfo(FlagOutType, true, isPrintCount)

	header := []string{"Cluster", keyHeader}
	if len(entries) > 0 {
		header = append(header, entries[0].GroupHeaders...)
	}
	if isPrintCount {
		header = append(header, "Total Job Count", "% of cluster")
	} else {
		header = append(header, "Total Cpu Time", "% of cluster")
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(header)

	for _, row := range entries {
		tableRow := []string{row.Cluster, row.Key}
		fillTableGroupValue(&tableRow, len(row.GroupHeaders), &row.GroupValues, row.Total, row.PercentOfCluster, isPrintCount)
		table.Append(tableRow)
	}
	table.Render()
}

func PrintAccountCpusList(accountUserWckeyList []*protos.JobSizeSummaryItem, startTime, endTime time.Time, groupList []uint32, isPrintCount bool, isJson bool) {
	entries := aggregateCpusSummary(accountUserWckeyList, groupList, isPrintCount,
		func(item *protos.JobSizeSummaryItem) string { return item.Account })

	if isJson {
		outputList := make([]AccountCpusSummaryJson, 0, len(entries))
		for _, e := range entries {
			outputList = append(outputList, AccountCpusSummaryJson{
				Cluster:          e.Cluster,
				Account:          e.Key,
				GroupHeaders:     e.GroupHeaders,
				GroupValues:      e.GroupValues,
				Total:            e.Total,
				PercentOfCluster: e.PercentOfCluster,
			})
		}
		PrintAsJsonToStdout(outputList)
		return
	}

	printCpusSummaryTable(entries, startTime, endTime,
		"Job Sizes", "Account", isPrintCount)
}

func PrintWckeyCpusList(accountUserWckeyList []*protos.JobSizeSummaryItem, startTime, endTime time.Time, groupList []uint32, isPrintCount bool, isJson bool) {
	entries := aggregateCpusSummary(accountUserWckeyList, groupList, isPrintCount,
		func(item *protos.JobSizeSummaryItem) string { return item.Wckey })

	if isJson {
		outputList := make([]WckeyCpusSummaryJson, 0, len(entries))
		for _, e := range entries {
			outputList = append(outputList, WckeyCpusSummaryJson{
				Cluster:          e.Cluster,
				Wckey:            e.Key,
				GroupHeaders:     e.GroupHeaders,
				GroupValues:      e.GroupValues,
				Total:            e.Total,
				PercentOfCluster: e.PercentOfCluster,
			})
		}
		PrintAsJsonToStdout(outputList)
		return
	}

	printCpusSummaryTable(entries, startTime, endTime,
		"Job Sizes by Wckey", "Wckey", isPrintCount)
}

func PrintAccountWckeyCpusList(accountUserWckeyList []*protos.JobSizeSummaryItem, startTime, endTime time.Time, groupList []uint32, isPrintCount bool, isJson bool) {
	entries := aggregateCpusSummary(accountUserWckeyList, groupList, isPrintCount,
		func(item *protos.JobSizeSummaryItem) string {
			return fmt.Sprintf("%s:%s", item.Account, item.Wckey)
		})

	if isJson {
		outputList := make([]AccountWckeyCpusSummaryJson, 0, len(entries))
		for _, e := range entries {
			outputList = append(outputList, AccountWckeyCpusSummaryJson{
				Cluster:          e.Cluster,
				AccountWckey:     e.Key,
				GroupHeaders:     e.GroupHeaders,
				GroupValues:      e.GroupValues,
				Total:            e.Total,
				PercentOfCluster: e.PercentOfCluster,
			})
		}
		PrintAsJsonToStdout(outputList)
		return
	}

	printCpusSummaryTable(entries, startTime, endTime,
		"Job Sizes", "Account:Wckey", isPrintCount)
}
