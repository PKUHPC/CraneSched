/**
 * Copyright (c) 2025 Peking University and Peking University
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

package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

// StepData holds both stepInfo and parent job for processing
type StepData struct {
	stepInfo *protos.StepInfo
	job      *protos.JobInfo
}

// QueryStepsTableOutput displays step-level information in table format
// Columns: STEPID, NAME, PARTITION, USER, TIME, NODELIST
func QueryStepsTableOutput(reply *protos.QueryJobsInfoReply) error {
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)

	// Collect all steps from all jobs
	var stepDataList []StepData
	for _, job := range reply.JobInfoList {
		for _, stepInfo := range job.StepInfoList {
			stepDataList = append(stepDataList, StepData{
				stepInfo: stepInfo,
				job:      job,
			})
		}
	}

	less := func(i, j int) bool {
		jobId1, jobId2 := stepDataList[i].stepInfo.JobId, stepDataList[j].stepInfo.JobId
		stepId1, stepId2 := stepDataList[i].stepInfo.StepId, stepDataList[j].stepInfo.StepId
		if jobId1 != jobId2 {
			return jobId1 > jobId2
		}
		return stepId1 < stepId2
	}
	sort.Slice(stepDataList, less)

	var header []string
	var tableData [][]string
	var err error

	// Support custom format
	if FlagFormat != "" {
		header, tableData, err = FormatStepData(stepDataList)
		if err != nil {
			return err
		}
		table.SetTablePadding("")
		table.SetAutoFormatHeaders(false)
	} else {
		// Default format
		header = []string{"StepId", "Name", "Partition", "User", "Time", "NODELIST"}

		for _, stepData := range stepDataList {
			stepInfo := stepData.stepInfo
			job := stepData.job

			stepIdStr := ProcessStepId(stepData)

			name := stepInfo.Name

			partition := job.Partition

			user := job.Username

			var timeStr string
			if stepInfo.Status == protos.JobStatus_Running {
				if stepInfo.ElapsedTime != nil {
					timeStr = util.SecondTimeFormat(stepInfo.ElapsedTime.Seconds)
				} else {
					timeStr = "0:00"
				}
			} else {
				timeStr = "-"
			}

			nodeList := stepInfo.GetCranedList()

			tableData = append(tableData, []string{
				stepIdStr,
				name,
				partition,
				user,
				timeStr,
				nodeList,
			})
		}
	}

	if !FlagNoHeader {
		table.SetHeader(header)
	}

	if !FlagFull && FlagFormat == "" {
		util.TrimTable(&tableData)
	}

	table.AppendBulk(tableData)
	table.Render()
	return nil
}

// Step field processors
func ProcessStepId(stepData StepData) string {
	return util.FormatStepId(stepData.stepInfo.JobId, stepData.job.ArrayTask, stepData.stepInfo.StepId)
}

func ProcessStepName(stepData StepData) string {
	return stepData.stepInfo.Name
}

func ProcessStepPartition(stepData StepData) string {
	return stepData.job.Partition
}

func ProcessStepUser(stepData StepData) string {
	return stepData.job.Username
}

func ProcessStepUid(stepData StepData) string {
	return strconv.FormatUint(uint64(stepData.stepInfo.Uid), 10)
}

func ProcessStepElapsedTime(stepData StepData) string {
	if stepData.stepInfo.Status == protos.JobStatus_Running {
		if stepData.stepInfo.ElapsedTime != nil {
			return util.SecondTimeFormat(stepData.stepInfo.ElapsedTime.Seconds)
		}
		return "0:00"
	}
	return "-"
}

func ProcessStepNodeList(stepData StepData) string {
	return stepData.stepInfo.GetCranedList()
}

func ProcessStepState(stepData StepData) string {
	if util.IsSlurmOutputMode() {
		return FormatSlurmJobState(stepData.stepInfo.GetStatus())
	}
	return stepData.stepInfo.GetStatus().String()
}

func ProcessStepStateCompact(stepData StepData) string {
	return FormatSlurmJobStateCompact(stepData.stepInfo.GetStatus())
}

func ProcessStepTimeLimit(stepData StepData) string {
	if stepData.stepInfo.TimeLimit.Seconds >= util.InvalidDuration().Seconds {
		return "unlimited"
	}
	return util.SecondTimeFormat(stepData.stepInfo.TimeLimit.Seconds)
}

func ProcessStepNodeNum(stepData StepData) string {
	return strconv.FormatUint(uint64(stepData.stepInfo.NodeNum), 10)
}

func ProcessStepAccount(stepData StepData) string {
	return stepData.job.Account
}

func ProcessStepQoS(stepData StepData) string {
	return stepData.job.Qos
}

func ProcessStepCommand(stepData StepData) string {
	return util.PrivateJobField(stepData.stepInfo.CmdLine, stepData.stepInfo.QueryVisibility)
}

func ProcessStepJobId(stepData StepData) string {
	return FormatQueueJobID(stepData.job)
}

type StepFieldProcessor struct {
	header  string
	process func(stepData StepData) string
}

var stepFieldMap = map[string]StepFieldProcessor{
	// Step-specific fields
	"i":      {"StepId", ProcessStepId},
	"stepid": {"StepId", ProcessStepId},

	"j":     {"JobId", ProcessStepJobId},
	"jobid": {"JobId", ProcessStepJobId},

	"n":    {"Name", ProcessStepName},
	"name": {"Name", ProcessStepName},

	"P":         {"Partition", ProcessStepPartition},
	"partition": {"Partition", ProcessStepPartition},

	"u":    {"User", ProcessStepUser},
	"user": {"User", ProcessStepUser},

	"U":   {"Uid", ProcessStepUid},
	"uid": {"Uid", ProcessStepUid},

	"e":           {"ElapsedTime", ProcessStepElapsedTime},
	"elapsedtime": {"ElapsedTime", ProcessStepElapsedTime},

	"L":        {"NodeList", ProcessStepNodeList},
	"nodelist": {"NodeList", ProcessStepNodeList},

	"t":            {"State", ProcessStepState},
	"state":        {"State", ProcessStepState},
	"statecompact": {"ST", ProcessStepStateCompact},

	"l":         {"TimeLimit", ProcessStepTimeLimit},
	"timelimit": {"TimeLimit", ProcessStepTimeLimit},

	"N":       {"NodeNum", ProcessStepNodeNum},
	"nodenum": {"NodeNum", ProcessStepNodeNum},

	"a":       {"Account", ProcessStepAccount},
	"account": {"Account", ProcessStepAccount},

	"q":   {"QoS", ProcessStepQoS},
	"qos": {"QoS", ProcessStepQoS},

	"o":       {"Command", ProcessStepCommand},
	"command": {"Command", ProcessStepCommand},
}

// FormatStepData formats the step output data according to the format string
func FormatStepData(stepDataList []StepData) (header []string, tableData [][]string, err error) {
	re := regexp.MustCompile(`%(\.\d+)?([a-zA-Z]+)`)
	specifiers := re.FindAllStringSubmatchIndex(FlagFormat, -1)
	if specifiers == nil {
		return nil, nil, util.NewCraneErr(util.ErrorInvalidFormat, "Invalid format specifier.")
	}

	tableOutputWidth := make([]int, 0, len(specifiers))
	tableOutputHeader := make([]string, 0, len(specifiers))
	tableOutputCell := make([][]string, len(stepDataList))

	// Get the prefix of the format string
	if specifiers[0][0] != 0 {
		prefix := FlagFormat[0:specifiers[0][0]]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputHeader = append(tableOutputHeader, prefix)
		for j := 0; j < len(stepDataList); j++ {
			tableOutputCell[j] = append(tableOutputCell[j], prefix)
		}
	}

	for i, spec := range specifiers {
		// Get the padding string between specifiers
		if i > 0 && spec[0]-specifiers[i-1][1] > 0 {
			padding := FlagFormat[specifiers[i-1][1]:spec[0]]
			tableOutputWidth = append(tableOutputWidth, -1)
			tableOutputHeader = append(tableOutputHeader, padding)
			for j := 0; j < len(stepDataList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], padding)
			}
		}

		// Parse width specifier
		if spec[2] == -1 {
			// w/o width specifier
			tableOutputWidth = append(tableOutputWidth, -1)
		} else {
			// with width specifier
			width, err := strconv.ParseUint(FlagFormat[spec[2]+1:spec[3]], 10, 32)
			if err != nil {
				return nil, nil, util.NewCraneErr(util.ErrorInvalidFormat, "Invalid width specifier.")
			}
			tableOutputWidth = append(tableOutputWidth, int(width))
		}

		// Parse format specifier
		field := FlagFormat[spec[4]:spec[5]]
		if len(field) > 1 {
			field = strings.ToLower(field)
		}

		fieldProcessor, found := stepFieldMap[field]
		if !found {
			return nil, nil, util.NewCraneErr(util.ErrorInvalidFormat, fmt.Sprintf(
				"Invalid format specifier or string for step: %s, string unfold case insensitive, reference: i/StepId, j/JobId, n/Name, P/Partition, u/User, U/Uid, e/ElapsedTime, L/NodeList, t/State, l/TimeLimit, N/NodeNum, a/Account, q/QoS, o/Command", field))
		}

		tableOutputHeader = append(tableOutputHeader, strings.ToUpper(fieldProcessor.header))
		for j, stepData := range stepDataList {
			tableOutputCell[j] = append(tableOutputCell[j], fieldProcessor.process(stepData))
		}
	}

	// Get the suffix of the format string
	if len(FlagFormat)-specifiers[len(specifiers)-1][1] > 0 {
		suffix := FlagFormat[specifiers[len(specifiers)-1][1]:]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputHeader = append(tableOutputHeader, suffix)
		for j := 0; j < len(stepDataList); j++ {
			tableOutputCell[j] = append(tableOutputCell[j], suffix)
		}
	}

	header, tableData = util.FormatTable(tableOutputWidth, tableOutputHeader, tableOutputCell)
	return header, tableData, nil
}
