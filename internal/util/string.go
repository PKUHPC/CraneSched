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
	"fmt"
	"math"
	"net"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

var SignalMap = map[string]int32{
	"HUP":  int32(syscall.SIGHUP),
	"INT":  int32(syscall.SIGINT),
	"QUIT": int32(syscall.SIGQUIT),
	"ILL":  int32(syscall.SIGILL),
	"ABRT": int32(syscall.SIGABRT),
	"FPE":  int32(syscall.SIGFPE),
	"KILL": int32(syscall.SIGKILL),
	"SEGV": int32(syscall.SIGSEGV),
	"PIPE": int32(syscall.SIGPIPE),
	"ALRM": int32(syscall.SIGALRM),
	"TERM": int32(syscall.SIGTERM),
	"USR1": int32(syscall.SIGUSR1),
	"USR2": int32(syscall.SIGUSR2),
}

// TODO: Refactor this to return ErrCodes instead of exiting.
func ParseConfig(configFilePath string) *Config {
	confFile, err := os.ReadFile(configFilePath)
	if err != nil {
		log.Errorf("Failed to read config file %s: %v", configFilePath, err)
		os.Exit(ErrorCmdArg)
	}

	config := &Config{
		Container: ContainerConfig{
			UserNsEnabledByDefault: DefaultUserNsEnabledByDefault,
		},
	}
	err = yaml.Unmarshal(confFile, config)
	if err != nil {
		log.Errorf("Config syntax error in %s: %v", configFilePath, err)
		os.Exit(ErrorCmdArg)
	}

	if config.CraneBaseDir == "" {
		config.CraneBaseDir = DefaultCraneBaseDir
	}

	if config.CranedCforedSockPath == "" {
		config.CranedCforedSockPath = filepath.Join(config.CraneBaseDir, DefaultCforedSocketPath)
	} else {
		config.CranedCforedSockPath = filepath.Join(config.CraneBaseDir, config.CranedCforedSockPath)
	}

	if config.CforedLogDir == "" {
		config.CforedLogDir = filepath.Join(config.CraneBaseDir, DefaultCforedLogDir)
	} else {
		config.CforedLogDir = filepath.Join(config.CraneBaseDir, config.CforedLogDir)
	}

	if config.CraneCtldForInternalListenPort == "" {
		log.Errorf("CraneCtldForInternalListenPort is not set in config file %s", configFilePath)
		os.Exit(ErrorGeneric)
	}

	if config.Cfored.PidFilePath == "" {
		config.Cfored.PidFilePath = filepath.Join(config.CraneBaseDir, DefaultCforedPidFilePath)
	} else {
		config.Cfored.PidFilePath = filepath.Join(config.CraneBaseDir, config.Cfored.PidFilePath)
	}

	if config.Cfored.DebugLevel == "" && config.CforedDebugLevel != nil {
		config.Cfored.DebugLevel = *config.CforedDebugLevel
	}

	return config
}

func ParsePluginConfig(configFilePath string) (*PluginConfig, error) {
	confFile, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, err
	}

	config := &PluginConfig{}
	if err = yaml.Unmarshal(confFile, config); err != nil {
		return nil, err
	}

	return config, nil
}

func ParseMemStringAsByte(mem string) (uint64, error) {
	re := regexp.MustCompile(`^([0-9]+(\.?[0-9]*))([MmGgKkB]?)$`)
	result := re.FindAllStringSubmatch(mem, -1)
	if result == nil || len(result) != 1 {
		return 0, fmt.Errorf("invalid memory format")
	}
	sz, err := ParseFloatWithPrecision(result[0][1], 10)
	if err != nil {
		return 0, err
	}
	switch result[0][len(result[0])-1] {
	case "M", "m":
		return uint64(1024 * 1024 * sz), nil
	case "G", "g":
		return uint64(1024 * 1024 * 1024 * sz), nil
	case "K", "k":
		return uint64(1024 * sz), nil
	case "B":
		return uint64(sz), nil
	}
	// default unit is MB
	return uint64(1024 * 1024 * sz), nil
}

func ParseInterval(interval string, intervalpb *protos.TimeInterval) (err error) {
	if !strings.Contains(interval, "~") {
		err = fmt.Errorf("'~' cannot be omitted")
		return
	}

	split := strings.Split(interval, "~")
	if len(split) > 2 {
		err = fmt.Errorf("too many '~' found")
		return
	}

	var tl, tr time.Time
	if split[0] != "" {
		tl, err = ParseTime(strings.TrimSpace(split[0]))
		if err != nil {
			return
		}
		intervalpb.LowerBound = timestamppb.New(tl)
	}
	if len(split) == 2 && split[1] != "" {
		tr, err = ParseTime(strings.TrimSpace(split[1]))
		if err != nil {
			return
		}
		if tr.Before(tl) {
			err = fmt.Errorf("%v is earlier than %v", tr, tl)
			return
		}
		intervalpb.UpperBound = timestamppb.New(tr)
	}
	return
}

func ParseDurationStrToSeconds(duration string) (int64, error) {
	re := regexp.MustCompile(`^((\d+)-)?(\d+):(\d+):(\d+)$`)
	result := re.FindStringSubmatch(duration)
	if result == nil {
		return 0, fmt.Errorf("invalid duration format: %s", duration)
	}

	var dd uint64 = 0
	if result[1] != "" {
		day, err := strconv.ParseUint(result[2], 10, 32)
		if err != nil {
			return 0, fmt.Errorf("invalid day format: %s", result[2])
		}
		dd = day
	}

	hh, err := strconv.ParseUint(result[3], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid hour format: %s", result[3])
	}

	mm, err := strconv.ParseUint(result[4], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid minute format: %s", result[4])
	}

	ss, err := strconv.ParseUint(result[5], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid second format: %s", result[5])
	}

	seconds := int64(24*60*60*dd + 60*60*hh + 60*mm + ss)
	return seconds, nil
}

func ParseArrayRangeSpec(spec string) (*protos.ArraySpec, error) {
	cleaned := strings.TrimSpace(spec)
	if cleaned == "" {
		return nil, fmt.Errorf("invalid --array value '%s': empty specification", spec)
	}

	arraySpec := &protos.ArraySpec{}
	if idx := strings.LastIndex(cleaned, "%"); idx >= 0 {
		concStr := cleaned[idx+1:]
		cleaned = cleaned[:idx]
		if concStr == "" {
			return nil, fmt.Errorf("invalid --array value '%s': missing number after '%%'", spec)
		}
		concVal, err := strconv.ParseUint(concStr, 10, 32)
		if err != nil || concVal == 0 {
			return nil, fmt.Errorf("invalid --array value '%s': concurrency limit must be a positive integer", spec)
		}
		maxConcurrent := uint32(concVal)
		arraySpec.MaxConcurrent = &maxConcurrent
	}

	result := regexp.MustCompile(`^(\d+)(?:-(\d+))?(?::(\d+))?$`).FindStringSubmatch(cleaned)
	if result == nil {
		return nil, fmt.Errorf("invalid --array value '%s'", spec)
	}

	start, err := strconv.ParseUint(result[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid --array index '%s'", result[1])
	}

	end := start
	if result[2] != "" {
		end, err = strconv.ParseUint(result[2], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid --array index '%s'", result[2])
		}
	}
	if start > end {
		return nil, fmt.Errorf("invalid --array value '%s': start > end", spec)
	}
	arraySpec.Start = uint32(start)
	arraySpec.End = uint32(end)

	stride := uint64(1)
	if result[3] != "" {
		if result[2] == "" {
			return nil, fmt.Errorf("invalid --array value '%s': stride without range", spec)
		}
		stride, err = strconv.ParseUint(result[3], 10, 32)
		if err != nil || stride == 0 {
			return nil, fmt.Errorf("invalid --array stride '%s'", result[3])
		}
		strideU32 := uint32(stride)
		arraySpec.Stride = &strideU32
	}

	taskCount := (end-start)/stride + 1
	if taskCount > MaxArrayTaskCount {
		return nil, fmt.Errorf("invalid --array value '%s': task count %d exceeds limit %d",
			spec, taskCount, MaxArrayTaskCount)
	}

	return arraySpec, nil
}

// FormatArraySpec renders an ArraySpec using the syntax accepted by --array.
func FormatArraySpec(arraySpec *protos.ArraySpec, includeMaxConcurrent bool) string {
	if arraySpec == nil {
		return ""
	}

	spec := strconv.FormatUint(uint64(arraySpec.GetStart()), 10)
	if arraySpec.GetStart() != arraySpec.GetEnd() {
		spec = fmt.Sprintf("%s-%d", spec, arraySpec.GetEnd())
	}
	if arraySpec.GetStride() > 1 {
		spec = fmt.Sprintf("%s:%d", spec, arraySpec.GetStride())
	}
	if includeMaxConcurrent && arraySpec.GetMaxConcurrent() > 0 {
		spec = fmt.Sprintf("%s%%%d", spec, arraySpec.GetMaxConcurrent())
	}
	return spec
}

func IsArrayParent(job *protos.JobInfo) bool {
	return job != nil && job.GetArraySpec() != nil && job.GetArrayTask() == nil
}

func IsPendingArrayParent(job *protos.JobInfo) bool {
	return IsArrayParent(job) && job.GetStatus() == protos.JobStatus_Pending
}

func FormatPendingArrayJobID(job *protos.JobInfo) string {
	if !IsPendingArrayParent(job) {
		return ""
	}
	return fmt.Sprintf("%d_[%s]", job.GetJobId(),
		FormatArraySpec(job.GetArraySpec(), true))
}

func FormatJobId(jobId uint32, arrayTask *protos.ArrayTaskIdentity) string {
	if arrayTask != nil {
		return fmt.Sprintf("%d_%d", arrayTask.ArrayJobId, arrayTask.TaskId)
	}
	return strconv.FormatUint(uint64(jobId), 10)
}

func FormatStepId(jobId uint32, arrayTask *protos.ArrayTaskIdentity, stepId uint32) string {
	return fmt.Sprintf("%s.%d", FormatJobId(jobId, arrayTask), stepId)
}

func ArrayTaskIdentityFromArrayTaskId(jobId uint32, arrayTaskId *uint32) *protos.ArrayTaskIdentity {
	if arrayTaskId == nil {
		return nil
	}
	return &protos.ArrayTaskIdentity{ArrayJobId: jobId, TaskId: *arrayTaskId}
}

func FormatJobIdFromArrayTaskId(jobId uint32, arrayTaskId *uint32) string {
	return FormatJobId(jobId, ArrayTaskIdentityFromArrayTaskId(jobId, arrayTaskId))
}

func FormatStepIdFromArrayTaskId(jobId uint32, arrayTaskId *uint32, stepId uint32) string {
	return FormatStepId(jobId, ArrayTaskIdentityFromArrayTaskId(jobId, arrayTaskId), stepId)
}

type JobIdentifier struct {
	JobId        uint32
	ArrayTaskId  uint32
	HasArrayTask bool
}

func JobIdentifierFromJobInfo(job *protos.JobInfo) JobIdentifier {
	if job.ArrayTask != nil {
		return JobIdentifier{
			JobId:        job.ArrayTask.ArrayJobId,
			ArrayTaskId:  job.ArrayTask.TaskId,
			HasArrayTask: true,
		}
	}
	return JobIdentifier{JobId: job.JobId}
}

func JobIdentifierFromSelector(selector *protos.JobIdSelector) JobIdentifier {
	if selector.ArrayTaskId != nil {
		return JobIdentifier{
			JobId:        selector.JobId,
			ArrayTaskId:  *selector.ArrayTaskId,
			HasArrayTask: true,
		}
	}
	return JobIdentifier{JobId: selector.JobId}
}

func (id JobIdentifier) String() string {
	if !id.HasArrayTask {
		return strconv.FormatUint(uint64(id.JobId), 10)
	}
	return FormatJobId(id.JobId, &protos.ArrayTaskIdentity{
		ArrayJobId: id.JobId,
		TaskId:     id.ArrayTaskId,
	})
}

type StepIdentifier struct {
	Job    JobIdentifier
	StepId uint32
}

func StepIdentifierFromStepInfo(job *protos.JobInfo, step *protos.StepInfo) StepIdentifier {
	return StepIdentifier{
		Job:    JobIdentifierFromJobInfo(job),
		StepId: step.StepId,
	}
}

func StepIdentifierFromSelector(selector *protos.JobIdSelector, stepId uint32) StepIdentifier {
	return StepIdentifier{
		Job:    JobIdentifierFromSelector(selector),
		StepId: stepId,
	}
}

func (id StepIdentifier) String() string {
	var arrayTask *protos.ArrayTaskIdentity
	if id.Job.HasArrayTask {
		arrayTask = &protos.ArrayTaskIdentity{
			ArrayJobId: id.Job.JobId,
			TaskId:     id.Job.ArrayTaskId,
		}
	}
	return FormatStepId(id.Job.JobId, arrayTask, id.StepId)
}

func ParseRelativeTime(ts string) (int64, error) {
	// handle compound duration like 1h2m30s, 1h30m, etc.
	if regexp.MustCompile(`^(\d+[a-zA-Z]+)+$`).MatchString(ts) {
		multiUnitRe := regexp.MustCompile(`(\d+)([a-zA-Z]+)`)
		matches := multiUnitRe.FindAllStringSubmatch(ts, -1)
		if matches != nil {
			var totalSeconds int64 = 0
			for _, match := range matches {
				value, err := strconv.ParseInt(match[1], 10, 64)
				if err != nil {
					return 0, fmt.Errorf("invalid duration: %v", ts)
				}
				unit := strings.ToLower(match[2])
				switch unit {
				case "", "s", "sec", "second", "seconds":
					totalSeconds += value
				case "m", "min", "minute", "minutes":
					totalSeconds += value * 60
				case "h", "hour", "hours":
					totalSeconds += value * 3600
				case "d", "day", "days":
					totalSeconds += value * 24 * 3600
				case "w", "week", "weeks":
					totalSeconds += value * 7 * 24 * 3600
				default:
					return 0, fmt.Errorf("invalid duration unit '%s' in: %v", unit, ts)
				}
			}
			return totalSeconds, nil
		}
	}
	// handle now+12:20:12
	return ParseDurationStrToSeconds(ts)
}

func ParseDayTime(day string) (time.Time, error) {
	now := time.Now()
	// handle time of day (HH:MM[:SS] am pm also available)
	// e.g. 09:00am 16:30
	re := regexp.MustCompile(`^(\d{2}):(\d{2})(?::(\d{2}))?(am|pm)?$`)
	if result := re.FindStringSubmatch(day); result != nil {
		hh, err := strconv.ParseUint(result[1], 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid hour format: %s", result[1])
		}
		if hh > 12 && result[4] == "am" {
			return time.Time{}, fmt.Errorf("invalid time format: %v", day)
		}
		if hh == 12 && result[4] == "am" {
			hh = 0
		}
		if hh != 12 && result[4] == "pm" {
			hh += 12
		}
		if hh >= 24 {
			return time.Time{}, fmt.Errorf("invalid hour format: %s", result[1])
		}
		mm, err := strconv.ParseUint(result[2], 10, 64)
		if err != nil || mm >= 60 {
			return time.Time{}, fmt.Errorf("invalid hour format: %s", result[1])
		}
		var ss uint64 = 0
		var err1 error
		if result[3] != "" {
			ss, err1 = strconv.ParseUint(result[3], 10, 64)
			if err1 != nil || ss >= 60 {
				return time.Time{}, fmt.Errorf("invalid second format: %s", result[3])
			}
		}
		t := time.Date(now.Year(), now.Month(), now.Day(), int(hh), int(mm), int(ss), 0, now.Location())
		if t.Before(now) {
			t = t.Add(24 * time.Hour)
		}
		return t, nil
	}
	return time.Time{}, fmt.Errorf("invalid time format: %v", day)
}

func ParseDate(year string, month string, day string) (time.Time, error) {
	// note: if the date is before now, the return date's year will plus one
	now := time.Now()
	var yy uint64
	if year == "" {
		yy = uint64(now.Year())
	} else {
		var err error
		yy, err = strconv.ParseUint(year, 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid year format: %s", year)
		}
	}
	if len(year) == 2 {
		yy += 2000
	}
	mm, err := strconv.ParseUint(month, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid month format: %s", month)

	}
	dd, err := strconv.ParseUint(day, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid day format: %s", day)
	}
	t := time.Date(int(yy), time.Month(mm), int(dd), 0, 0, 0, 0, now.Location())
	if t.Month() != time.Month(mm) || t.Day() != int(dd) {
		return time.Time{}, fmt.Errorf("invalid date format: %04d-%02d-%02d", yy, mm, dd)
	}
	if year == "" && t.Before(now) {
		t = t.AddDate(1, 0, 0)
	}
	return t, nil
}

func ParseDateTime(date string) (time.Time, error) {
	var re *regexp.Regexp
	// handle YYYY-MM-DD
	re = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})$`)
	if result := re.FindStringSubmatch(date); result != nil {
		t, err := ParseDate(result[1], result[2], result[3])
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid date format: %v", date)
		}
		return t, nil
	}
	// handle MM/DD[/YY]
	re = regexp.MustCompile(`^(\d{2})/(\d{2})(?:/(\d{2}))?$`)
	if result := re.FindStringSubmatch(date); result != nil {
		t, err := ParseDate(result[3], result[1], result[2])
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid date format: %v", date)
		}
		return t, nil
	}
	// handle MMDD[YY]
	re = regexp.MustCompile(`^(\d{2})(\d{2})(\d{2})?$`)
	if result := re.FindStringSubmatch(date); result != nil {
		t, err := ParseDate(result[3], result[1], result[2])
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid date format: %v", date)
		}
		return t, nil
	}
	// handle YYYY-MM-DD[THH:MM[:SS]]
	re = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})(?:T(.+))?$`)
	if result := re.FindStringSubmatch(date); result != nil {
		t, err := ParseDate(result[1], result[2], result[3])
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid date format: %v", date)
		}
		if result[4] != "" {
			dayTime, err := ParseDayTime(result[4])
			if err != nil {
				return time.Time{}, fmt.Errorf("invalid date format: %v", date)
			}
			hh, mm, ss := dayTime.Clock()
			t = t.Add(time.Duration(hh)*time.Hour +
				time.Duration(mm)*time.Minute +
				time.Duration(ss)*time.Second)
		}
		return t, nil
	}
	return time.Time{}, fmt.Errorf("invalid date format: %v", date)
}

func ParseKeywordTime(keyword string) (time.Time, error) {
	// note: donot support tomorrowTnoon
	// only handle single keyword
	now := time.Now()
	t := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	parseWeekday := func(offset int) (time.Time, error) {
		offset = (offset - int(now.Weekday()) + 7) % 7
		if offset == 0 {
			offset = 7
		}
		return t.AddDate(0, 0, offset), nil
	}
	switch keyword {
	case "midnight":
		return t, nil
	case "elevenses":
		return t.Add(11 * time.Hour), nil
	case "noon":
		return t.Add(12 * time.Hour), nil
	case "fika":
		return t.Add(15 * time.Hour), nil
	case "teatime":
		return t.Add(16 * time.Hour), nil
	case "today":
		return t, nil
	case "tomorrow":
		return t.AddDate(0, 0, 1), nil
	case "sunday":
		return parseWeekday(int(time.Sunday))
	case "monday":
		return parseWeekday(int(time.Monday))
	case "tuesday":
		return parseWeekday(int(time.Tuesday))
	case "wednesday":
		return parseWeekday(int(time.Wednesday))
	case "thursday":
		return parseWeekday(int(time.Thursday))
	case "friday":
		return parseWeekday(int(time.Friday))
	case "saturday":
		return parseWeekday(int(time.Saturday))
	}
	return time.Time{}, fmt.Errorf("invalid keyword time: %s", keyword)
}

func ParseTime(ts string) (time.Time, error) {
	if strings.HasPrefix(ts, "now") {
		t := time.Time{}
		if ts == "now" {
			t = time.Now()
		} else if ts[3] == '+' {
			// '+' adds offset to Now()
			seconds, err := ParseRelativeTime(ts[4:])
			if err != nil {
				return t, fmt.Errorf("invalid duration '%v'", ts[4:])
			}
			t = time.Now().Add(time.Duration(seconds) * time.Second)
		} else if ts[3] == '-' {
			// '-' subtracts offset from Now()
			seconds, err := ParseRelativeTime(ts[4:])
			if err != nil {
				return t, fmt.Errorf("invalid duration '%v'", ts[4:])
			}
			t = time.Now().Add(-1 * time.Duration(seconds) * time.Second)
		} else {
			return t, fmt.Errorf("invalid time format")
		}
		return t.Round(0), nil
	}

	dayTime, err := ParseDayTime(ts)
	if err == nil {
		return dayTime, nil
	}

	dateTime, err := ParseDateTime(ts)
	if err == nil {
		return dateTime, nil
	}

	keywordTime, err := ParseKeywordTime(ts)
	if err == nil {
		return keywordTime, nil
	}

	// Use regex to check if HH:MM:SS exists
	// This is required as Golang permits `2:03:14` but denies `2:3:14`,
	// which is undesired.
	re := regexp.MustCompile(`(\d{2}:\d{2}:\d{2})`)
	if !re.MatchString(ts) {
		return time.Time{}, fmt.Errorf("invalid time format")
	}

	// Try to parse the timezone at first
	layout := time.RFC3339
	parsed, err := time.Parse(layout, ts)
	if err == nil {
		return parsed, nil
	}

	// Fallback to the short layout, assuming local timezone
	layoutShort := time.RFC3339[:19]
	parsed, err = time.ParseInLocation(layoutShort, ts, time.Local)
	return parsed, err
}

func SecondTimeFormat(second int64) string {
	timeFormat := ""
	dd := second / 24 / 3600
	second %= 24 * 3600
	hh := second / 3600
	second %= 3600
	mm := second / 60
	ss := second % 60
	if dd > 0 {
		timeFormat = fmt.Sprintf("%d-%02d:%02d:%02d", dd, hh, mm, ss)
	} else {
		timeFormat = fmt.Sprintf("%02d:%02d:%02d", hh, mm, ss)
	}
	return timeFormat
}

// Parses a string containing a float number with a given precision.
func ParseFloatWithPrecision(val string, decimalPlaces int) (float64, error) {
	num, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, err
	}

	shift := math.Pow(10, float64(decimalPlaces))
	return math.Floor(num*shift) / shift, nil
}

// ParseJobIdStepIdStrict parses a step id string in the format "jobid.stepid".
// Returns jobid, stepid, error. If stepId is not present, return an error.
func ParseJobIdStepIdStrict(jobStepId string) (uint32, uint32, error) {
	parts := strings.Split(jobStepId, ".")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("CONTAINER must be in JOBID.STEPID format")
	}

	jobID64, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid job ID: %s", parts[0])
	}

	stepID64, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid step ID: %s", parts[1])
	}

	return uint32(jobID64), uint32(stepID64), nil
}

var allowedMailTypes = map[string]struct{}{
	"NONE":      {},
	"BEGIN":     {},
	"END":       {},
	"FAIL":      {},
	"TIMELIMIT": {},
	"OOM":       {},
	"ALL":       {},
}

func CheckMailType(mailtype string) bool {
	parts := strings.Split(mailtype, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if _, ok := allowedMailTypes[part]; !ok {
			return false
		}
	}
	return true
}

func ParseLicensesString(val string) ([]*protos.JobToCtld_License, bool, error) {
	tmpStr := strings.ReplaceAll(val, " ", "")
	var licenseVec []string

	hasComma := strings.Contains(tmpStr, ",")
	hasPipe := strings.Contains(tmpStr, "|")

	if hasComma && hasPipe {
		return nil, hasPipe, fmt.Errorf("cannot mix AND (,) and OR (|) in licenses string")
	}

	if hasPipe {
		licenseVec = strings.Split(tmpStr, "|")
	} else {
		licenseVec = strings.Split(tmpStr, ",")
	}

	var licCount []*protos.JobToCtld_License
	pattern := regexp.MustCompile(`^([a-zA-Z]+(?:@[a-zA-Z0-9_]+)?):(\d+)$`)

	for _, licenseCount := range licenseVec {
		if !pattern.MatchString(licenseCount) {
			return nil, hasPipe, fmt.Errorf("invalid licenses string format")
		}
		matches := pattern.FindStringSubmatch(licenseCount)
		if len(matches) != 3 {
			return nil, hasPipe, fmt.Errorf("invalid licenses string format")
		}
		name := matches[1]
		count, err := strconv.ParseUint(matches[2], 10, 32)
		if err != nil {
			return nil, hasPipe, err
		}
		if count <= 0 {
			return nil, hasPipe, fmt.Errorf("license count must > 0")
		}
		licCount = append(licCount, &protos.JobToCtld_License{
			Key:   name,
			Count: uint32(count),
		})
	}

	return licCount, hasPipe, nil
}

// CheckNodeList checks a comma-separated node list or hostlist expression.
func CheckNodeList(nodeStr string) bool {
	nameStr := strings.ReplaceAll(nodeStr, " ", "")
	if nameStr == "" {
		return true
	}
	ValidHostnameRegex := "(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9-]*[A-Za-z0-9])"
	ValidHostListRegex := "^(" + ValidHostnameRegex + ")(," + ValidHostnameRegex + ")*$"
	re := regexp.MustCompile(ValidHostListRegex)
	hostList, ok := ParseHostList(nameStr)
	if !ok || len(hostList) == 0 {
		return false
	}
	for _, host := range hostList {
		if host == "" || !re.MatchString(host) {
			return false
		}
	}
	return true
}

// CheckFileLength check if the file length is within the limit.
func CheckFileLength(filepath string) error {
	if len(path.Base(filepath)) > MaxJobFileNameLength {
		return fmt.Errorf("file name length exceeds %v characters", MaxJobFileNameLength)
	}

	if len(filepath) > MaxJobFilePathLengthForUnix {
		return fmt.Errorf("file path length exceeds %v characters", MaxJobFilePathLengthForUnix)
	}

	// Special case for Windows
	if runtime.GOOS == "windows" && len(filepath) > MaxJobFilePathLengthForWindows {
		return fmt.Errorf("file path length exceeds %v characters on Windows", MaxJobFilePathLengthForWindows)
	}

	return nil
}

func CheckJobNameLength(name string) error {
	if len(name) > MaxJobNameLength {
		return fmt.Errorf("name is too long (up to %v)", MaxJobNameLength)
	}
	return nil
}

func CheckJobArgs(job *protos.JobToCtld) error {
	if job.MemPerNode != nil && job.MemPerCpu != nil {
		return fmt.Errorf("--mem and --mem-per-cpu are mutually exclusive, " +
			"please check your command line options, submission script, and environment variables")
	}

	if job.NodeNumMin == 0 {
		if job.Ntasks == 0 {
			if job.NtasksPerNode == 0 {
				job.Ntasks = 1
				job.NtasksPerNode = 1
			} else {
				job.Ntasks = job.NtasksPerNode
			}
		}
		if job.NtasksPerNode == 0 {
			job.NtasksPerNode = job.Ntasks
			job.NodeNumMin = 1
			job.NodeNumMax = job.Ntasks
		} else {
			job.NodeNumMin = (job.Ntasks-1)/job.NtasksPerNode + 1
			job.NodeNumMax = job.NodeNumMin
		}
	}

	if job.Ntasks == 0 {
		if job.NtasksPerNode == 0 {
			job.Ntasks = job.NodeNumMin
		} else {
			job.Ntasks = job.NodeNumMin * job.NtasksPerNode
		}
	}

	if job.NodeNumMax == 0 {
		job.NodeNumMax = job.NodeNumMin
	}
	if job.NodeNumMin > job.Ntasks {
		log.Warnf("Warning: can't run %d tasks on %d nodes, setting NodeNum to ntasks.", job.Ntasks, job.NodeNumMin)
		job.NodeNumMin = job.Ntasks
		job.NodeNumMax = job.Ntasks
	} else if job.NodeNumMax > job.Ntasks {
		log.Warnf("Warning: can't run %d tasks on up to %d nodes, setting NodeNumMax to ntasks.", job.Ntasks, job.NodeNumMax)
		job.NodeNumMax = job.Ntasks
	}
	if job.NtasksPerNode > 0 && job.NtasksPerNode*job.NodeNumMax < job.Ntasks {
		return fmt.Errorf("invalid argument: NtasksPerNode * NodeNum < Ntasks, unable to allocate resources")
	}
	if job.CpusPerTask != nil && *job.CpusPerTask <= 0 {
		return fmt.Errorf("invalid --cpus-per-task")
	}
	if err := CheckJobNameLength(job.Name); err != nil {
		return err
	}
	if job.NodeNumMin <= 0 {
		return fmt.Errorf("--nodes must > 0")
	}
	if job.TimeLimit.AsDuration() <= 0 {
		return fmt.Errorf("--time must > 0")
	}
	if !CheckNodeList(job.Nodelist) {
		return fmt.Errorf("invalid format for --nodelist")
	}
	if job.Nodelist != "" {
		hostList, ok := ParseHostList(job.Nodelist)
		if !ok {
			return fmt.Errorf("invalid format for --nodelist")
		}
		job.Nodelist = strings.Join(hostList, ",")
	}
	if !CheckNodeList(job.Excludes) {
		return fmt.Errorf("invalid format for --exclude")
	}
	if job.Excludes != "" {
		hostList, ok := ParseHostList(job.Excludes)
		if !ok {
			return fmt.Errorf("invalid format for --exclude")
		}
		job.Excludes = strings.Join(hostList, ",")
	}
	// Calculate total CPUs if cpus_per_task is specified
	if job.CpusPerTask != nil && job.Ntasks > 0 {
		CpusTotal := *job.CpusPerTask * float64(job.Ntasks)
		if CpusTotal > 1e6 {
			return fmt.Errorf("requesting too many CPUs: %f", CpusTotal)
		}
	}
	if job.ExtraAttr != "" {
		// Check attrs in job.ExtraAttr, e.g., mail.type, mail.user
		mailtype := gjson.Get(job.ExtraAttr, "mail.type")
		mailuser := gjson.Get(job.ExtraAttr, "mail.user")
		if mailtype.Exists() != mailuser.Exists() {
			return fmt.Errorf("incomplete mail arguments")
		}
		if mailtype.Exists() && !CheckMailType(mailtype.String()) {
			return fmt.Errorf("invalid --mail-type")
		}
	}
	if job.Dependencies != nil {
		jobIds := make(map[uint32]bool)
		for _, dep := range job.Dependencies.Deps {
			if jobIds[dep.JobId] {
				return fmt.Errorf("invalid argument: duplicate job %d in dependencies", dep.JobId)
			}
			jobIds[dep.JobId] = true
		}
	}

	return nil
}

func CheckStepArgs(step *protos.StepToCtld) error {
	if step.MemPerNode != nil && step.MemPerCpu != nil {
		return fmt.Errorf("--mem and --mem-per-cpu are mutually exclusive, " +
			"please check your command line options, submission script, and environment variables")
	}
	if step.NodeNum == 0 {
		if step.Ntasks == 0 {
			if step.NtasksPerNode == 0 {
				step.Ntasks = 1
			} else {
				step.Ntasks = step.NtasksPerNode
			}
		}
		if step.NtasksPerNode == 0 {
			step.NodeNum = 1
		} else {
			step.NodeNum = (step.Ntasks-1)/step.NtasksPerNode + 1
		}
	}
	if step.Ntasks == 0 {
		if step.NtasksPerNode == 0 {
			step.Ntasks = step.NodeNum
		} else {
			step.Ntasks = step.NodeNum * step.NtasksPerNode
		}
	}
	if step.NodeNum > step.Ntasks {
		log.Warnf("Warning: can't run %d tasks on %d nodes, setting NodeNum to ntasks.", step.Ntasks, step.NodeNum)
		step.NodeNum = step.Ntasks
	}
	if step.NtasksPerNode > 0 && step.NtasksPerNode*step.NodeNum < step.Ntasks {
		log.Warnf("Warning: NtasksPerNode * NodeNum < Ntasks, ignoring NtasksPerNode.")
		step.NtasksPerNode = 0
	}
	if step.CpusPerTask != nil && *step.CpusPerTask <= 0 {
		return fmt.Errorf("invalid --cpus-per-task")
	}
	if err := CheckJobNameLength(step.Name); err != nil {
		return err
	}
	if step.NodeNum <= 0 {
		return fmt.Errorf("--nodes must > 0")
	}
	if step.TimeLimit.AsDuration() <= 0 {
		return fmt.Errorf("--time must > 0")
	}
	if !CheckNodeList(step.Nodelist) {
		return fmt.Errorf("invalid format for --nodelist")
	}
	if step.Nodelist != "" {
		hostList, ok := ParseHostList(step.Nodelist)
		if !ok {
			return fmt.Errorf("invalid format for --nodelist")
		}
		step.Nodelist = strings.Join(hostList, ",")
	}
	if !CheckNodeList(step.Excludes) {
		return fmt.Errorf("invalid format for --exclude")
	}
	if step.Excludes != "" {
		hostList, ok := ParseHostList(step.Excludes)
		if !ok {
			return fmt.Errorf("invalid format for --exclude")
		}
		step.Excludes = strings.Join(hostList, ",")
	}
	// Calculate total CPUs if cpus_per_task is specified
	if step.CpusPerTask != nil && step.Ntasks > 0 {
		CpusTotal := *step.CpusPerTask * float64(step.Ntasks)
		if CpusTotal > 1e6 {
			return fmt.Errorf("requesting too many CPUs: %f", CpusTotal)
		}
	}
	if step.ExtraAttr != "" {
		// Check attrs in job.ExtraAttr, e.g., mail.type, mail.user
		mailtype := gjson.Get(step.ExtraAttr, "mail.type")
		mailuser := gjson.Get(step.ExtraAttr, "mail.user")
		if mailtype.Exists() != mailuser.Exists() {
			return fmt.Errorf("incomplete mail arguments")
		}
		if mailtype.Exists() && !CheckMailType(mailtype.String()) {
			return fmt.Errorf("invalid --mail-type")
		}
	}

	return nil
}

func CheckEntityName(name string) error {
	if name == "=" {
		return fmt.Errorf("name empty")
	}

	if name == "ALL" {
		return fmt.Errorf("name cannot be \"ALL\"")
	}

	if len(name) > MaxEntityNameLength {
		return fmt.Errorf("name is too long (up to %v)", MaxEntityNameLength)
	}

	var validStringPattern = `^[a-zA-Z0-9][a-zA-Z0-9_]*$`
	re := regexp.MustCompile(validStringPattern)
	if !re.MatchString(name) {
		return fmt.Errorf("name can only contain letters, numbers or underscores")
	}

	return nil
}

func ParseHostList(hostStr string) ([]string, bool) {
	nameStr := strings.ReplaceAll(hostStr, " ", "")
	nameStr += ","

	var nameMeta string
	var strList []string
	var charQueue string

	for _, c := range nameStr {
		switch c {
		case '[':
			if charQueue == "" {
				charQueue = string(c)
			} else {
				log.Errorln("Illegal node name string format: duplicate brackets")
				return nil, false
			}
		case ']':
			if charQueue == "" {
				log.Errorln("Illegal node name string format: isolated bracket")
				return nil, false
			} else {
				nameMeta += charQueue
				nameMeta += string(c)
				charQueue = ""
			}
		case ',':
			if charQueue == "" {
				strList = append(strList, nameMeta)
				nameMeta = ""
			} else {
				charQueue += string(c)
			}
		default:
			if charQueue == "" {
				nameMeta += string(c)
			} else {
				charQueue += string(c)
			}
		}
	}
	if charQueue != "" {
		log.Errorln("Illegal node name string format: isolated bracket")
		return nil, false
	}

	regex := regexp.MustCompile(`.*\[(.*)\].*`)
	var hostList []string

	for _, str := range strList {
		strS := strings.TrimSpace(str)
		if !regex.MatchString(strS) {
			hostList = append(hostList, strS)
		} else {
			nodes, ok := parseNodeList(strS)
			if !ok {
				return nil, false
			}
			hostList = append(hostList, nodes...)
		}
	}
	return hostList, true
}

func parseNodeList(nodeStr string) ([]string, bool) {
	bracketsRegex := regexp.MustCompile(`.*\[(.*)\]`)
	numRegex := regexp.MustCompile(`^\d+$`)
	scopeRegex := regexp.MustCompile(`^(\d+)-(\d+)$`)

	if !bracketsRegex.MatchString(nodeStr) {
		return nil, false
	}

	unitStrList := strings.Split(nodeStr, "]")
	endStr := unitStrList[len(unitStrList)-1]
	unitStrList = unitStrList[:len(unitStrList)-1]
	resList := []string{""}

	for _, str := range unitStrList {
		nodeNum := strings.Split(strings.ReplaceAll(str, "[", ","), ",")
		if len(nodeNum) == 0 {
			return nil, false
		}
		unitList := []string{}
		headStr := nodeNum[0]

		for _, numStr := range nodeNum[1:] {
			if numRegex.MatchString(numStr) {
				unitList = append(unitList, fmt.Sprintf("%s%s", headStr, numStr))
			} else if scopeRegex.MatchString(numStr) {
				locIndex := scopeRegex.FindStringSubmatch(numStr)
				start, err1 := strconv.Atoi(locIndex[1])
				end, err2 := strconv.Atoi(locIndex[2])
				if err1 != nil || err2 != nil {
					return nil, false
				}
				width := len(locIndex[1])
				for j := start; j <= end; j++ {
					sNum := fmt.Sprintf("%0*d", width, j)
					unitList = append(unitList, fmt.Sprintf("%s%s", headStr, sNum))
				}
			} else {
				return nil, false // Format error
			}
		}

		tempList := []string{}
		for _, left := range resList {
			for _, right := range unitList {
				tempList = append(tempList, left+right)
			}
		}
		resList = tempList
	}

	if endStr != "" {
		for i := range resList {
			resList[i] += endStr
		}
	}

	return resList, true
}

func ParsePosNegList(posNegStr string) ([]string, []string, error) {
	posList := []string{}
	negList := []string{}

	if posNegStr == "" {
		return posList, negList, nil
	}

	posSet := make(map[string]bool)
	negSet := make(map[string]bool)

	hostList := strings.Split(posNegStr, ",")
	for _, host := range hostList {
		host = strings.TrimSpace(host)
		if host == "" {
			return nil, nil, fmt.Errorf("empty field")
		}
		switch host[0] {
		case '-':
			negSet[host[1:]] = true
		case '+':
			posSet[host[1:]] = true
		default:
			posSet[host] = true
		}
	}

	if len(posSet) < len(negSet) {
		for str := range posSet {
			if _, ok := negSet[str]; ok {
				return nil, nil, fmt.Errorf("item %s is in both positive and negative list", str)
			}
		}
	} else {
		for str := range negSet {
			if _, ok := posSet[str]; ok {
				return nil, nil, fmt.Errorf("item %s is in both positive and negative list", str)
			}
		}
	}

	for str := range posSet {
		posList = append(posList, str)
	}
	for str := range negSet {
		negList = append(negList, str)
	}
	return posList, negList, nil
}

func InvalidDuration() *durationpb.Duration {
	return &durationpb.Duration{
		Seconds: MaxJobTimeLimit,
		Nanos:   0,
	}
}

func HostNameListToStr(hostList []string) string {
	sourceList := hostList

	for {
		resList, res := HostNameListToStr_(sourceList)
		if res {
			sort.Strings(resList)
			hostNameStr := strings.Join(resList, ",")
			return RemoveBracketsWithoutDashOrComma(hostNameStr)
		}
		sourceList = resList
	}
}

func HostNameListToStr_(hostList []string) ([]string, bool) {
	hostMap := make(map[string][]string)
	resList := []string{}
	res := true

	sz := len(hostList)
	switch sz {
	case 0:
		return resList, true
	case 1:
		resList = append(resList, hostList[0])
		return resList, true
	}

	for _, host := range hostList {
		if host == "" {
			continue
		}

		start, end, found := FoundFirstNumberWithoutBrackets(host)
		if found {
			res = false
			numStr := host[start:end]
			headStr := host[:start]
			tailStr := host[end:]
			keyStr := fmt.Sprintf("%s<%s", headStr, tailStr)

			if _, ok := hostMap[keyStr]; !ok {
				hostMap[keyStr] = []string{}
			}
			hostMap[keyStr] = append(hostMap[keyStr], numStr)
		} else {
			resList = append(resList, host)
		}
	}

	if res {
		return resList, true
	}

	for key, nums := range hostMap {
		delimiterPos := strings.Index(key, "<")
		hostNameStr := key[:delimiterPos] + "["

		sort.Slice(nums, func(i, j int) bool {
			a, b := nums[i], nums[j]
			if len(a) != len(b) {
				return len(a) < len(b)
			}
			ai, _ := strconv.Atoi(a)
			bi, _ := strconv.Atoi(b)
			return ai < bi
		})

		nums = unique(nums)

		first, last := -1, -1
		firstStr, lastStr := "", ""

		for _, numStr := range nums {
			num, _ := strconv.Atoi(numStr)
			if first == -1 {
				first, last = num, num
				firstStr, lastStr = numStr, numStr
			} else if num == last+1 {
				last = num
				lastStr = numStr
			} else {
				if first == last {
					hostNameStr += firstStr
				} else {
					hostNameStr += fmt.Sprintf("%s-%s", firstStr, lastStr)
				}
				hostNameStr += ","
				first, last = num, num
				firstStr, lastStr = numStr, numStr
			}
		}

		if first == last {
			hostNameStr += firstStr
		} else {
			hostNameStr += fmt.Sprintf("%s-%s", firstStr, lastStr)
		}

		hostNameStr += "]" + key[delimiterPos+1:]
		resList = append(resList, hostNameStr)
	}

	return resList, res
}

func FoundFirstNumberWithoutBrackets(input string) (int, int, bool) {
	start, end := -1, -1
	opens := 0

	for i, c := range input {
		switch c {
		case '[':
			opens++
		case ']':
			opens--
		default:
			if opens == 0 {
				if start == -1 && c >= '0' && c <= '9' {
					start = i
				} else if start != -1 && (c < '0' || c > '9') {
					end = i
					return start, end, true
				}
			}
		}
	}

	if start != -1 {
		return start, len(input), true
	}
	return start, end, false
}

func unique(nums []string) []string {
	if len(nums) == 0 {
		return nums
	}
	j := 0
	for i := 1; i < len(nums); i++ {
		if nums[j] != nums[i] {
			j++
			nums[j] = nums[i]
		}
	}
	return nums[:j+1]
}

func RemoveBracketsWithoutDashOrComma(input string) string {
	output := input
	leftBracketPos := strings.Index(output, "[")

	for leftBracketPos != -1 {
		rightBracketPos := strings.Index(output[leftBracketPos:], "]")
		if rightBracketPos == -1 {
			break
		}
		rightBracketPos += leftBracketPos
		betweenBrackets := output[leftBracketPos+1 : rightBracketPos]
		if !strings.Contains(betweenBrackets, "-") && !strings.Contains(betweenBrackets, ",") {
			output = output[:rightBracketPos] + output[rightBracketPos+1:]
			output = output[:leftBracketPos] + output[leftBracketPos+1:]
		} else {
			leftBracketPos = rightBracketPos + 1
		}
		leftBracketPos = strings.Index(output[leftBracketPos:], "[")
		if leftBracketPos != -1 {
			leftBracketPos += leftBracketPos
		}
	}
	return output
}

func ParseGres(gres string) (*protos.GresMap, error) {
	result := &protos.GresMap{NameGresMap: make(map[string]*protos.GresCount)}
	if gres == "" {
		return result, nil
	}
	gresList := strings.Split(gres, ",")
	for _, g := range gresList {
		parts := strings.Split(g, ":")
		name := parts[0]
		if len(parts) == 2 {
			gresNameCount, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing gres count: %s", g)
			}
			if gresNameCount == 0 {
				continue
			}
			if _, exist := result.NameGresMap[name]; !exist {
				result.NameGresMap[name] = &protos.GresCount{Specified: make(map[string]uint64), Total: gresNameCount}
			} else {
				result.NameGresMap[name].Total += gresNameCount
			}
		} else if len(parts) == 3 {
			gresType := parts[1]
			count, err := strconv.ParseUint(parts[2], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("Error parsing count for %s: %v\n", name, err)
			}
			if count == 0 {
				continue
			}
			if _, exist := result.NameGresMap[name]; !exist {
				specified := make(map[string]uint64)
				specified[gresType] = count
				result.NameGresMap[name] = &protos.GresCount{Specified: specified, Total: count}
			} else {
				result.NameGresMap[name].Specified[gresType] = count
				result.NameGresMap[name].Total += count
			}
		} else {
			return nil, fmt.Errorf("Error parsing gres: %s\n", g)
		}
	}

	return result, nil
}

func ParseGpusPerNodeStr(gpuPerNodeStr string) (*protos.GresMap, error) {
	result := &protos.GresMap{NameGresMap: make(map[string]*protos.GresCount)}
	if strings.TrimSpace(gpuPerNodeStr) == "" {
		return result, nil
	}
	gpusPerNodeStrList := strings.Split(gpuPerNodeStr, ",")

	const (
		ModeNone  = 0
		ModeDigit = 1
		ModeType  = 2
	)
	mode := ModeNone
	gresCount := &protos.GresCount{Specified: make(map[string]uint64)}

	for index, gpuPerNodePart := range gpusPerNodeStrList {
		gpuPerNodePart = strings.TrimSpace(gpuPerNodePart)
		if gpuPerNodePart == "" {
			return nil, fmt.Errorf("invalid input: empty part in gpus-per-node string")
		}
		parts := strings.Split(gpuPerNodePart, ":")
		if len(parts) == 1 {
			if mode == ModeNone {
				mode = ModeDigit
			} else if mode != ModeDigit {
				return nil, fmt.Errorf("cannot mix simple number format with type:count format")
			}
			if index != 0 {
				return nil, fmt.Errorf("simple number format must be the only part")
			}
			val, err := strconv.ParseUint(parts[0], 10, 64)
			if err != nil || val == 0 {
				return nil, fmt.Errorf("invalid number: %q", parts[0])
			}
			gresCount.Total = val
		} else if len(parts) == 2 {
			if mode == ModeNone {
				mode = ModeType
			} else if mode != ModeType {
				return nil, fmt.Errorf("cannot mix simple number format with type:count format")
			}
			gpuType := strings.TrimSpace(parts[0])
			numStr := strings.TrimSpace(parts[1])
			if gpuType == "" {
				return nil, fmt.Errorf("gpu type cannot be empty")
			}
			val, err := strconv.ParseUint(numStr, 10, 64)
			if err != nil || val == 0 {
				return nil, fmt.Errorf("invalid number for type %q: %q", gpuType, numStr)
			}
			gresCount.Specified[gpuType] = val
			gresCount.Total += val
		} else {
			return nil, fmt.Errorf("invalid input: %q (too many colons)", gpuPerNodePart)
		}
	}

	result.NameGresMap[GresGpuName] = gresCount
	return result, nil
}

func ParseGresForQosLimit(gres string) (*protos.GresMap, error) {
	result := &protos.GresMap{NameGresMap: make(map[string]*protos.GresCount)}
	if gres == "" {
		return result, nil
	}

	gresList := strings.Split(gres, ",")
	for _, g := range gresList {
		parts := strings.Split(g, ":")
		name := parts[0]
		if len(parts) == 2 {
			if parts[1] == "unlimited" {
				if pair, exist := result.NameGresMap[name]; exist {
					if len(pair.Specified) > 0 {
						delete(result.NameGresMap, name)
					} else {
						result.NameGresMap[name].Total = math.MaxUint32
					}
				}
				continue
			}
			gresNameCount, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("Error parsing count for %s: %v\n", name, err)
			}
			if _, exist := result.NameGresMap[name]; !exist {
				result.NameGresMap[name] = &protos.GresCount{Specified: make(map[string]uint64), Total: gresNameCount}
			} else {
				result.NameGresMap[name].Total += gresNameCount
			}
		} else if len(parts) == 3 {
			gresType := parts[1]
			if parts[2] == "unlimited" {
				if pair, exist := result.NameGresMap[name]; exist {
					if pair.Specified != nil {
						delete(pair.Specified, gresType)
					}
					if len(pair.Specified) == 0 {
						delete(result.NameGresMap, name)
					}
				}
				continue
			}
			count, err := strconv.ParseUint(parts[2], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("Error parsing count for %s: %v\n", name, err)
			}
			if _, exist := result.NameGresMap[name]; !exist {
				specified := make(map[string]uint64)
				specified[gresType] = count
				result.NameGresMap[name] = &protos.GresCount{Specified: specified, Total: count}
			} else {
				old := result.NameGresMap[name].Specified[gresType]
				result.NameGresMap[name].Specified[gresType] = count
				result.NameGresMap[name].Total += count - old
			}
		} else {
			return nil, fmt.Errorf("Error parsing gres: %s\n", g)
		}
	}

	return result, nil
}

func ParseTres(tres string) (*protos.ResourceView, error) {
	result := &protos.ResourceView{
		CpuCount:      UnlimitedCpuValue,
		MemoryBytes:   MaxJobMemoryBytes,
		MemorySwBytes: MaxJobMemoryBytes,
		GresMap:       &protos.GresMap{NameGresMap: make(map[string]*protos.GresCount)},
	}
	if tres == "" {
		return result, nil
	}
	var gresStr string
	items := strings.Split(tres, ",")
	for _, item := range items {
		if strings.HasPrefix(item, "gres/") && len(item) > 5 {
			gresStr += item[5:] + ","
		} else {
			kv := strings.SplitN(item, ":", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])
				if key == "cpu" {
					if value == "unlimited" {
						result.CpuCount = UnlimitedCpuValue
					} else {
						count, err := strconv.ParseFloat(value, 64)
						if err != nil {
							return nil, fmt.Errorf("invalid cpu value: %q", value)
						}
						if count > UnlimitedCpuThreshold {
							return nil, fmt.Errorf("CPU setting %q exceeds the limit", value)
						}
						result.CpuCount = count
					}
				} else if key == "mem" {
					if value == "unlimited" {
						result.MemoryBytes = MaxJobMemoryBytes
						result.MemorySwBytes = MaxJobMemoryBytes
					} else {
						membytes, err := ParseMemStringAsByte(value)
						if err != nil {
							return nil, fmt.Errorf("invalid mem value: %q", value)
						}
						if membytes > MaxJobMemoryBytes {
							return nil, fmt.Errorf("invalid mem value: %q", value)
						}
						result.MemoryBytes = membytes
						result.MemorySwBytes = membytes
					}
				} else {
					return nil, fmt.Errorf("invalid tres name: %q", key)
				}
			} else {
				return nil, fmt.Errorf("invalid item: %q", item)
			}
		}
	}

	var err error
	result.GresMap, err = ParseGresForQosLimit(strings.TrimSuffix(gresStr, ","))
	if err != nil {
		return nil, err
	}

	return result, nil
}

func ParseJobStatusName(state string) (protos.JobStatus, error) {
	state = strings.ToLower(state)
	switch state {
	case "pending", "p":
		return protos.JobStatus_Pending, nil
	case "running", "r":
		return protos.JobStatus_Running, nil
	case "completed", "c":
		return protos.JobStatus_Completed, nil
	case "failed", "f":
		return protos.JobStatus_Failed, nil
	case "tle", "time-limit-exceeded", "timelimitexceeded", "t":
		return protos.JobStatus_ExceedTimeLimit, nil
	case "canceled", "cancelled", "x":
		return protos.JobStatus_Cancelled, nil
	case "oom", "out-of-memory", "outofmemory", "o":
		return protos.JobStatus_OutOfMemory, nil
	case "suspended", "suspend", "s":
		return protos.JobStatus_Suspended, nil
	case "all":
		return protos.JobStatus_Invalid, nil
	default:
		return protos.JobStatus_Invalid, fmt.Errorf("unknown state: %s", state)
	}
}

func ParseJobTypeName(jobType string) (protos.JobType, error) {
	jobType = strings.ToLower(jobType)
	switch jobType {
	case "interactive", "i":
		return protos.JobType_Interactive, nil
	case "batch", "b":
		return protos.JobType_Batch, nil
	case "container", "c":
		return protos.JobType_Container, nil
	default:
		return protos.JobType_Interactive, fmt.Errorf("unknown job type: %s", jobType)
	}
}

func ParseJobTypeList(jobTypesStr string) ([]protos.JobType, error) {
	var jobTypeSet = make(map[protos.JobType]bool)
	filterJobTypeList := strings.Split(jobTypesStr, ",")
	for i := 0; i < len(filterJobTypeList); i++ {
		jobType, err := ParseJobTypeName(filterJobTypeList[i])
		if err != nil {
			return nil, err
		}
		jobTypeSet[jobType] = true
	}

	var jobTypeList []protos.JobType
	for jobType := range jobTypeSet {
		jobTypeList = append(jobTypeList, jobType)
	}
	return jobTypeList, nil
}

func ParseJobStatusList(statesStr string) ([]protos.JobStatus, error) {
	var stateSet = make(map[protos.JobStatus]bool)
	filterStateList := strings.Split(statesStr, ",")
	for i := 0; i < len(filterStateList); i++ {
		state, err := ParseJobStatusName(filterStateList[i])
		if err != nil {
			return nil, err
		}
		stateSet[state] = true
	}
	if _, exists := stateSet[protos.JobStatus_Invalid]; !exists && len(stateSet) < len(protos.JobStatus_name)-1 {
		var stateList []protos.JobStatus
		for state := range stateSet {
			stateList = append(stateList, state)
		}
		return stateList, nil
	}
	return []protos.JobStatus{}, nil
}

func ParseInRamJobStatusList(statesStr string) ([]protos.JobStatus, error) {
	var stateSet = make(map[protos.JobStatus]bool)
	filterStateList := strings.Split(statesStr, ",")
	for i := 0; i < len(filterStateList); i++ {
		state, err := ParseJobStatusName(filterStateList[i])
		if err != nil {
			return nil, err
		}
		if state != protos.JobStatus_Invalid && state != protos.JobStatus_Pending &&
			state != protos.JobStatus_Running && state != protos.JobStatus_Suspended {
			return nil, fmt.Errorf("unsupported state: %s", filterStateList[i])
		}
		stateSet[state] = true
	}
	if len(stateSet) == 1 {
		for state := range stateSet {
			if state == protos.JobStatus_Invalid {
				return []protos.JobStatus{}, nil
			} else {
				return []protos.JobStatus{state}, nil
			}
		}
	}
	return []protos.JobStatus{}, nil
}

func ParseStringParamList(parameters string, splitStr string) ([]string, error) {
	parameterList := strings.Split(parameters, splitStr)
	for i := 0; i < len(parameterList); i++ {
		if parameterList[i] == "" {
			return nil, fmt.Errorf("empty value")
		}
	}

	return parameterList, nil
}

func ParseStringParamListAllowEmpty(parameters string, splitStr string) ([]string, error) {
	if strings.TrimSpace(parameters) == "" {
		return nil, nil
	}
	return ParseStringParamList(parameters, splitStr)
}

func ParseJobIdSelectorList(selectorList string, splitStr string) ([]*protos.JobIdSelector, error) {
	selectors := make([]*protos.JobIdSelector, 0)

	for token := range strings.SplitSeq(selectorList, splitStr) {
		parts := strings.Split(strings.TrimSpace(token), ".")
		if len(parts) == 0 || len(parts) > 2 || parts[0] == "" {
			return nil, fmt.Errorf("invalid selector \"%s\"", token)
		}

		jobParts := strings.Split(parts[0], "_")
		if len(jobParts) == 0 || len(jobParts) > 2 || jobParts[0] == "" {
			return nil, fmt.Errorf("invalid selector \"%s\"", token)
		}

		jobId, err := strconv.ParseUint(jobParts[0], 10, 32)
		if err != nil || jobId == 0 {
			return nil, fmt.Errorf("invalid job id \"%s\"", token)
		}

		selector := &protos.JobIdSelector{JobId: uint32(jobId)}
		if len(jobParts) == 2 {
			arrayTaskId, err := strconv.ParseUint(jobParts[1], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid array task id \"%s\"", token)
			}
			arrayTaskId32 := uint32(arrayTaskId)
			selector.ArrayTaskId = &arrayTaskId32
		}

		if len(parts) == 2 {
			stepId, err := strconv.ParseUint(parts[1], 10, 32)
			if err != nil || stepId == 0 {
				return nil, fmt.Errorf("invalid step id \"%s\"", token)
			}
			selector.Steps = append(selector.Steps, uint32(stepId))
		}

		selectors = append(selectors, selector)
	}

	return selectors, nil
}

func ParseJobIdList(jobIds string, splitStr string) ([]uint32, error) {
	var jobIdList []uint32
	for jobIdStr := range strings.SplitSeq(jobIds, splitStr) {
		jobId, err := strconv.ParseUint(jobIdStr, 10, 32)
		if err != nil || jobId == 0 {
			return nil, fmt.Errorf("invalid job id \"%s\"", jobIdStr)
		}
		jobIdList = append(jobIdList, uint32(jobId))
	}

	return jobIdList, nil
}

// StateToString converts a state value to a readable string
func StateToString(state int64) string {
	stateMap := map[int64]string{
		0: "Resume",
		1: "Drain",
	}
	if str, exists := stateMap[state]; exists {
		return str
	}
	return "Unknown"
}

func GetValidNodeList(CranedNodeList []ConfigNodesList) ([]string, error) {
	if len(CranedNodeList) == 0 {
		return nil, fmt.Errorf("nodes in config yaml file err")
	}

	nodeNameSet := make(map[string]struct{})
	var nodeNameList []string
	for _, cranedNode := range CranedNodeList {
		nodeNames, ok := ParseHostList(cranedNode.Name)
		if !ok || len(nodeNames) == 0 {
			continue
		}

		for _, nodeName := range nodeNames {
			if _, exists := nodeNameSet[nodeName]; !exists {
				nodeNameList = append(nodeNameList, nodeName)
				nodeNameSet[nodeName] = struct{}{}
			}
		}
	}

	if len(nodeNameList) == 0 {
		return nil, fmt.Errorf("no valid nodes found after parsing")
	}

	return nodeNameList, nil
}

// BuildNodeAliasMap expands the configured NodeName and NodeHostname lists
// and maps every supported user-facing alias to its canonical NodeName.
func BuildNodeAliasMap(nodes []ConfigNodesList) (map[string]string, error) {
	aliases := make(map[string]string)
	register := func(alias, nodeName string) error {
		if alias == "" {
			return nil
		}
		if existing, ok := aliases[alias]; ok && existing != nodeName {
			return fmt.Errorf("node alias %q refers to both %q and %q", alias,
				existing, nodeName)
		}
		aliases[alias] = nodeName
		return nil
	}

	for _, nodeConfig := range nodes {
		nodeNames, ok := ParseHostList(nodeConfig.Name)
		if !ok || len(nodeNames) == 0 {
			return nil, fmt.Errorf("invalid node name list %q", nodeConfig.Name)
		}

		hostnames := nodeNames
		if nodeConfig.NodeHostname != "" {
			parsedHostnames, parsed := ParseHostList(nodeConfig.NodeHostname)
			if !parsed || len(parsedHostnames) != len(nodeNames) {
				return nil, fmt.Errorf(
					"NodeHostname list %q must contain %d entries to match %q",
					nodeConfig.NodeHostname, len(nodeNames), nodeConfig.Name)
			}
			hostnames = parsedHostnames
		}

		for i, nodeName := range nodeNames {
			for _, alias := range []string{
				nodeName,
				hostnames[i],
			} {
				if err := register(alias, nodeName); err != nil {
					return nil, err
				}
			}
		}
	}

	return aliases, nil
}

// Merge two JSON strings.
// If there are overlapping keys, values from the second JSON take precedence.
func AmendJobExtraAttrs(origin, new string) string {
	if origin == "" {
		return new
	}

	result := gjson.Parse(new)
	result.ForEach(func(key, value gjson.Result) bool {
		var err error
		// Use sjson to set/override the value in the first JSON
		origin, err = sjson.Set(origin, key.String(), value.Value())
		return err == nil
	})

	return origin
}

type JobExtraAttrs struct {
	ExtraAttr string
	MailType  string
	MailUser  string
	Comment   string
}

func (j *JobExtraAttrs) Marshal(r *string) error {
	var err error

	extra := j.ExtraAttr
	if extra != "" && !gjson.Valid(extra) {
		return fmt.Errorf("invalid --extra-attr: invalid JSON string")
	}

	if j.MailType != "" {
		if !CheckMailType(j.MailType) {
			return fmt.Errorf("invalid --mail-type")
		}
		extra, err = sjson.Set(extra, "mail.type", j.MailType)
		if err != nil {
			return fmt.Errorf("invalid --mail-type: %v", err)
		}
	}

	if j.MailUser != "" {
		extra, err = sjson.Set(extra, "mail.user", j.MailUser)
		if err != nil {
			return fmt.Errorf("invalid --mail-user: %v", err)
		}
	}

	if j.Comment != "" {
		extra, err = sjson.Set(extra, "comment", j.Comment)
		if err != nil {
			return fmt.Errorf("invalid --comment: %v", err)
		}
	}

	*r = extra
	return nil
}

func ConvertSliceToString[T any](slice []T, sep string) string {
	str := make([]string, len(slice))
	for i, v := range slice {
		str[i] = fmt.Sprint(v)
	}
	return strings.Join(str, sep)
}

func ExtractExecNameFromArgs(args []string) string {
	for _, arg := range args {
		name := path.Base(arg)
		if name != "" {
			return name
		}
	}
	return "Interactive"
}

func FormatMemToMB(data uint64) string {
	var B2MBRatio uint64 = 1024 * 1024
	if data == 0 {
		return "0"
	} else {
		return fmt.Sprintf("%vM", data/B2MBRatio)
	}
}

var TxnActionMap = map[string]protos.TxnAction{
	"addaccount":    protos.TxnAction_AddAccount,
	"modifyaccount": protos.TxnAction_ModifyAccount,
	"deleteaccount": protos.TxnAction_DeleteAccount,
	"adduser":       protos.TxnAction_AddUser,
	"modifyuser":    protos.TxnAction_ModifyUser,
	"deleteuser":    protos.TxnAction_DeleteUser,
	"addqos":        protos.TxnAction_AddQos,
	"modifyqos":     protos.TxnAction_ModifyQos,
	"deleteqos":     protos.TxnAction_DeleteQos,
}

func StringToTxnAction(str string) (protos.TxnAction, bool) {
	if action, exists := TxnActionMap[str]; exists {
		return action, true
	}

	return 0, false
}
func SetJobDependencies(job *protos.JobToCtld, depStr string) error {
	depStr = strings.TrimSpace(depStr)

	hasComma := strings.Contains(depStr, ",")
	hasQuestion := strings.Contains(depStr, "?")
	if hasComma && hasQuestion {
		return fmt.Errorf("cannot use both ',' and '?' in the dependency string")
	}

	sep := ","
	if hasQuestion {
		sep = "?"
	}

	job.Dependencies = &protos.Dependencies{
		IsOr: hasQuestion,
	}

	// Regex: (type):(job_id[+delay][:job_id[+delay]]...)
	depPattern := regexp.MustCompile(`^(after|afterok|afternotok|afterany):(.+)$`)
	jobPattern := regexp.MustCompile(`(\d+)(?:\+([^:]+))?(?::|$)`)

	depStrList := strings.Split(depStr, sep)
	for _, subDepStr := range depStrList {
		matches := depPattern.FindStringSubmatch(subDepStr)
		if matches == nil {
			return fmt.Errorf("unrecognized dependency string: %s", subDepStr)
		}

		depTypeStr, jobsStr := matches[1], matches[2]

		if !regexp.MustCompile(`^[\d+:a-zA-Z]+$`).MatchString(jobsStr) {
			return fmt.Errorf("invalid job list format in '%s': should only contain job_id[+delay] separated by ':'", jobsStr)
		}

		var depType protos.DependencyType
		switch depTypeStr {
		case "after":
			depType = protos.DependencyType_AFTER
		case "afterok":
			depType = protos.DependencyType_AFTER_OK
		case "afternotok":
			depType = protos.DependencyType_AFTER_NOT_OK
		case "afterany":
			depType = protos.DependencyType_AFTER_ANY
		}

		jobMatches := jobPattern.FindAllStringSubmatch(jobsStr, -1)
		if jobMatches == nil {
			return fmt.Errorf("no valid job IDs found in: %s", subDepStr)
		}

		for _, jobMatch := range jobMatches {
			jobId, err := strconv.ParseUint(jobMatch[1], 10, 32)
			if err != nil {
				return fmt.Errorf("invalid job ID '%s' in dependency: %v", jobMatch[1], err)
			}
			var delaySeconds uint64 = 0
			if jobMatch[2] != "" {
				delayStr := jobMatch[2]
				// If delayStr contains only digits, treat it as minutes by default
				if regexp.MustCompile(`^\d+$`).MatchString(delayStr) {
					delayStr = delayStr + "m"
				}
				seconds, err := ParseRelativeTime(delayStr)
				if err != nil {
					return fmt.Errorf("invalid delay format '%s' in dependency: %v", jobMatch[2], err)
				}
				delaySeconds = uint64(seconds)
			}

			job.Dependencies.Deps = append(job.Dependencies.Deps, &protos.DependencyCondition{
				JobId:        uint32(jobId),
				Type:         depType,
				DelaySeconds: delaySeconds,
			})
		}
	}
	return nil
}

func ParseSignalParamString(input string) ([]*protos.Signal, error) {
	var signals []*protos.Signal

	var signalPattern = regexp.MustCompile(`^(?:(R|B):)?([A-Za-z0-9]+)(?:@(\d+))?$`)

	items := strings.Split(input, ",")
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		matches := signalPattern.FindStringSubmatch(item)
		if matches == nil {
			return nil, fmt.Errorf("invalid signal format: %v", input)
		}

		signal := &protos.Signal{}

		// matches[1]: Source (R or B)
		if matches[1] != "" {
			if matches[1] == "R" {
				signal.SignalFlag = protos.Signal_RESERVATION_OVERLAP
			} else if matches[1] == "B" {
				signal.SignalFlag = protos.Signal_BATCH_ONLY
			}
		}

		// matches[2]: Number or string
		sigName := strings.ToUpper(matches[2])
		if strings.HasPrefix(sigName, "SIG") && len(sigName) > 3 {
			sigName = sigName[3:]
		}
		if sig_num, ok := SignalMap[sigName]; ok {
			signal.SignalNumber = sig_num
		} else {
			num, err := strconv.Atoi(matches[2])
			if err != nil {
				return nil, fmt.Errorf("invalid signal number: %v", matches[2])
			}
			signal.SignalNumber = int32(num)
		}

		// matches[3]: Time (optional)
		if matches[3] != "" {
			t, err := strconv.Atoi(matches[3])
			if err != nil {
				return nil, fmt.Errorf("invalid signal time: %v", matches[3])
			}
			signal.SignalTime = uint32(t)
		} else {
			signal.SignalTime = 60
		}

		signals = append(signals, signal)
	}

	return signals, nil
}

func CheckIpv4Format(ip string) error {
	parsed := net.ParseIP(ip)
	if parsed == nil || parsed.To4() == nil {
		return fmt.Errorf("Invalid ipv4 format: %s", ip)
	}
	return nil
}

func ReadableMemory(memoryBytes uint64) string {
	if memoryBytes < 1024 {
		return fmt.Sprintf("%dB", memoryBytes)
	} else if memoryBytes < 1024*1024 {
		return fmt.Sprintf("%dK", memoryBytes/1024)
	} else if memoryBytes < 1024*1024*1024 {
		return fmt.Sprintf("%dM", memoryBytes/(1024*1024))
	} else {
		return fmt.Sprintf("%dG", memoryBytes/(1024*1024*1024))
	}
}

func ResourceViewToTres(rv *protos.ResourceView) string {
	var parts []string

	if rv == nil {
		return ""
	}
	if rv.CpuCount < UnlimitedCpuThreshold {
		cpu := strconv.FormatFloat(rv.CpuCount, 'f', -1, 64)
		parts = append(parts, "cpu="+cpu)
	}
	if rv.MemoryBytes != MaxJobMemoryBytes {
		mem := ReadableMemory(rv.MemoryBytes)
		parts = append(parts, "mem="+mem)
	}
	if rv.GresMap != nil && len(rv.GresMap.NameGresMap) > 0 {
		for name, gresCount := range rv.GresMap.NameGresMap {
			if len(gresCount.Specified) > 0 {
				for typ, count := range gresCount.Specified {
					if count > 0 {
						parts = append(parts, fmt.Sprintf("gres/%s:%s:%d", name, typ, count))
					}
				}
			} else if gresCount.Total > 0 {
				parts = append(parts, fmt.Sprintf("gres/%s:%d", name, gresCount.Total))
			}
		}
	}
	if len(parts) == 0 {
		return "unlimited"
	}
	return strings.Join(parts, ",")
}

func ParseFlags(s string) (uint32, error) {
	var flags uint32 = 0

	if strings.TrimSpace(s) == "" {
		return 0, fmt.Errorf("empty flags")
	}
	items := strings.Split(s, ",")
	for _, item := range items {
		item = strings.ToLower(strings.TrimSpace(item))
		val, ok := QoSFlagNameMap[item]
		if !ok {
			return 0, fmt.Errorf("invalid QoS flag: %s", item)
		}
		flags |= val
	}

	return flags, nil
}
