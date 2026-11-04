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

package ccon

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/nxadm/tail"
	"github.com/spf13/cobra"
)

// NOTE: See ContainerInstance in JobManager.h for log file structure details
const kLogDirPattern = "%d.out"
const kLogFilename = "%v.%v.log"

func logExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return util.NewCraneErr(util.ErrorCmdArg, "logs requires exactly one argument: CONTAINER (JOBID.STEPID)")
	}

	f := GetFlags()
	jobID, stepID, err := util.ParseJobIdStepIdStrict(args[0])
	if err != nil {
		return err
	}

	if stepID == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "step 0 is reserved for pods, please specify a valid container step ID")
	}

	request := protos.QueryJobsInfoRequest{
		FilterJobIds:               []*protos.JobIdSelector{{JobId: jobID, Steps: []uint32{stepID}}},
		FilterJobTypes:             []protos.JobType{protos.JobType_Container},
		OptionIncludeCompletedJobs: true,
	}

	reply, err := util.QueryJobsInfo(context.Background(), stub, &request)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to query container job")
	}

	if !reply.GetOk() {
		return util.NewCraneErr(util.ErrorBackend, "")
	}

	if len(reply.JobInfoList) == 0 {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("container %d.%d not found", jobID, stepID))
	}

	job := reply.JobInfoList[0]

	var targetStep *protos.StepInfo
	for _, step := range job.StepInfoList {
		if step.StepId == stepID {
			targetStep = step
			break
		}
	}
	if targetStep == nil {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("container %d.%d not found", jobID, stepID))
	}
	if err := util.RequireJobDetails(targetStep.QueryVisibility); err != nil {
		return err
	}
	if targetStep.GetContainerMeta() == nil {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("step %d.%d is not a container", jobID, stepID))
	}

	cwd := targetStep.Cwd
	if cwd == "" {
		cwd = job.Cwd
	}

	var nodeName string
	if targetStep.Status == protos.JobStatus_Running {
		nodeName, err = resolveTargetNode(targetStep, f.Log.TargetNode)
		if err != nil {
			return err
		}
	} else {
		if f.Log.TargetNode == "" {
			return util.NewCraneErr(util.ErrorCmdArg, "Target node must be specified for non-running containers")
		}
		nodeName, err = resolveNodeAlias(f.Log.TargetNode)
		if err != nil {
			return err
		}
	}

	logPath, err := buildLogPath(cwd, nodeName, jobID, stepID)
	if err != nil {
		return util.WrapCraneErr(util.ErrorBackend, "%v", err)
	}

	// Check if the file exists
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("log file does not exist: %s. Do you have shared storage?", logPath))
	}

	var sinceTime, untilTime *time.Time
	if f.Log.Since != "" {
		t, err := parseCliTimeString(f.Log.Since)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "invalid --since format: %v", err)
		}
		sinceTime = &t
	}

	if f.Log.Until != "" {
		t, err := parseCliTimeString(f.Log.Until)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "invalid --until format: %v", err)
		}
		untilTime = &t
	}

	if f.Log.Follow {
		return followLogFile(logPath, f.Log.Tail, sinceTime, untilTime, f.Log.Timestamps)
	} else {
		logLines, err := readLogFileWithTimeFilter(logPath, f.Log.Tail, sinceTime, untilTime)
		if err != nil {
			return err
		}

		if f.Global.Json {
			outputJson("logs", "", f.Log, logLines)
			return nil
		}

		for _, line := range logLines {
			if f.Log.Timestamps {
				fmt.Println(line)
			} else {
				cleanLine := removeTimestamp(line)
				fmt.Println(cleanLine)
			}
		}
	}

	return nil
}

func buildLogPath(cwd, nodeName string, jobId, stepId uint32) (string, error) {
	logDir := fmt.Sprintf(kLogDirPattern, jobId)
	logPath := filepath.Join(cwd, logDir, fmt.Sprintf(kLogFilename, stepId, nodeName))

	return logPath, nil
}

func followLogFile(logPath string, tailLines int, sinceTime, untilTime *time.Time, timestamps bool) error {
	printLogLine := func(line string, timestamps bool) {
		if timestamps {
			fmt.Println(line)
		} else {
			cleanLine := removeTimestamp(line)
			fmt.Println(cleanLine)
		}
	}

	// First, read existing content if needed
	if tailLines > 0 {
		existingLines, err := readLogFileWithTimeFilter(logPath, tailLines, sinceTime, untilTime)
		if err == nil {
			for _, line := range existingLines {
				printLogLine(line, timestamps)
			}
		}
	}

	// Setup tail configuration
	config := tail.Config{
		Follow:    true,  // equivalent to tail -f
		ReOpen:    true,  // equivalent to tail -F (reopen after rotation)
		Poll:      true,  // use polling for NFS/cross-host scenarios
		MustExist: false, // don't error if file doesn't exist yet
	}

	// Determine starting position
	if tailLines > 0 {
		// If we already showed tail lines, start from end
		config.Location = &tail.SeekInfo{Whence: io.SeekEnd}
	} else {
		// Start from beginning or since time
		config.Location = &tail.SeekInfo{Whence: io.SeekStart}
	}

	t, err := tail.TailFile(logPath, config)
	if err != nil {
		return util.WrapCraneErr(util.ErrorBackend, "failed to tail log file: %v", err)
	}
	defer t.Cleanup()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case line := <-t.Lines:
			if line == nil {
				return nil // tail finished
			}
			if line.Err != nil {
				return util.WrapCraneErr(util.ErrorBackend, "tail error: %v", line.Err)
			}

			// Apply time filtering
			if shouldFilterLogLine(line.Text, sinceTime, untilTime) {
				continue
			}

			printLogLine(line.Text, timestamps)

		case <-sigChan:
			return nil // graceful shutdown
		}
	}
}

func shouldFilterLogLine(line string, sinceTime, untilTime *time.Time) bool {
	if sinceTime == nil && untilTime == nil {
		return false // no filtering needed
	}

	logTime := extractTimeFromLogLine(line)
	if logTime == nil {
		return false // can't filter without timestamp
	}

	if sinceTime != nil && logTime.Before(*sinceTime) {
		return true // filter out
	}
	if untilTime != nil && logTime.After(*untilTime) {
		return true // filter out
	}

	return false // don't filter
}

func parseCliTimeString(timeStr string) (time.Time, error) {
	// Try relative time first (e.g., "2h", "30m", "45s")
	if matched, _ := regexp.MatchString(`^\d+[smhd]$`, timeStr); matched {
		duration, err := time.ParseDuration(timeStr)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid duration format: %s", timeStr)
		}
		return time.Now().Add(-duration), nil
	}

	// Try various absolute time formats
	timeFormats := []string{
		time.RFC3339,                  // 2025-01-15T10:30:00Z
		time.RFC3339Nano,              // 2025-01-15T10:30:00.123Z
		"2006-01-02T15:04:05",         // 2025-01-15T10:30:00
		"2006-01-02 15:04:05",         // 2025-01-15 10:30:00
		"2006-01-02T15:04:05.000Z",    // 2025-01-15T10:30:00.123Z
		"2006-01-02T15:04:05.000000Z", // 2025-01-15T10:30:00.123456Z
	}

	for _, format := range timeFormats {
		if t, err := time.Parse(format, timeStr); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unsupported time format: %s", timeStr)
}

func readLogFileWithTimeFilter(logPath string, tail int, sinceTime, untilTime *time.Time) ([]string, error) {
	file, err := os.Open(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("log file not found: %s", logPath)
		}
		return nil, fmt.Errorf("failed to open log file: %v", err)
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		// Apply time filtering if either since or until is specified
		if sinceTime != nil || untilTime != nil {
			logTime := extractTimeFromLogLine(line)
			if logTime != nil {
				if sinceTime != nil && logTime.Before(*sinceTime) {
					continue
				}
				if untilTime != nil && logTime.After(*untilTime) {
					continue
				}
			}
		}

		lines = append(lines, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read log file: %v", err)
	}

	if tail > 0 && tail < len(lines) {
		lines = lines[len(lines)-tail:]
	}

	return lines, nil
}

func extractTimeFromLogLine(line string) *time.Time {
	// Containerd log format: TIMESTAMP STREAM PARTIAL_FLAG LOG_CONTENT
	// Example: 2025-09-19T16:56:32.827697838+08:00 stdout F total 0

	// Split by space to get the first field (timestamp)
	parts := strings.SplitN(line, " ", 2)
	if len(parts) < 1 {
		return nil
	}

	timestampStr := parts[0]

	// Parse as RFC3339Nano (containerd format only)
	if t, err := time.Parse(time.RFC3339Nano, timestampStr); err == nil {
		return &t
	}

	return nil
}

func removeTimestamp(line string) string {
	// Containerd log format: TIMESTAMP STREAM PARTIAL_FLAG LOG_CONTENT
	// Example: 2025-09-19T16:56:32.827697838+08:00 stdout F total 0

	// Split by space and find the log content (after stream and partial flag)
	parts := strings.SplitN(line, " ", 4)
	if len(parts) >= 4 {
		// Return the log content (4th part)
		return parts[3]
	}

	// If format doesn't match, return original line
	return line
}
