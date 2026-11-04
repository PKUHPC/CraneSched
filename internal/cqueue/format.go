package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// 'a' group
func ProcessAccount(job *protos.JobInfo) string {
	return job.Account
}

// 'c' group
func ProcessAllocCpus(job *protos.JobInfo) string {
	return strconv.FormatFloat(job.GetAllocatedResView().GetCpuCount(), 'f', 2, 64)
}

// 'C' group
func ProcessReqCpuPerTask(job *protos.JobInfo) string {
	if job.Ntasks == nil {
		return "unknown"
	}
	totalCpu := job.GetReqTotalResView().GetCpuCount()
	if job.GetNtasks() > 0 {
		return strconv.FormatFloat(totalCpu/float64(job.GetNtasks()), 'f', 2, 64)
	}
	return strconv.FormatFloat(totalCpu, 'f', 2, 64)
}

// 'd' group
func ProcessDeadline(task *protos.JobInfo) string {
	if task.DeadlineTime == nil {
		return "unknown"
	}
	deadlineTime := task.DeadlineTime.AsTime()
	if !deadlineTime.Equal(util.InfiniteFuture) {
		return deadlineTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	return "unknown"
}

// 'e' group
func ProcessElapsedTime(job *protos.JobInfo) string {
	if job.Status == protos.JobStatus_Running {
		return util.SecondTimeFormat(job.ElapsedTime.Seconds)
	}
	return "-"
}

// 'h' group
func ProcessHeld(job *protos.JobInfo) string {
	return strconv.FormatBool(job.Held)
}

// 'j' group
func ProcessJobId(job *protos.JobInfo) string {
	return FormatQueueJobID(job)
}

// 'k'wckey
func ProcessWckey(job *protos.JobInfo) string {
	return job.Wckey
}

// 'k' group
func ProcessComment(job *protos.JobInfo) string {
	if job.QueryVisibility != protos.QueryVisibility_QUERY_VISIBILITY_DETAILS {
		return util.RestrictedJobField
	}
	if !gjson.Valid(job.ExtraAttr) {
		return ""
	}
	return gjson.Get(job.ExtraAttr, "comment").String()
}

// 'l' group
func ProcessTimeLimit(job *protos.JobInfo) string {
	if job.TimeLimit.Seconds >= util.InvalidDuration().Seconds {
		return "unlimited"
	}
	return util.SecondTimeFormat(job.TimeLimit.Seconds)
}

func ProcessNodeList(job *protos.JobInfo) string {
	return job.GetCranedList()
}

// 'm' group
func ProcessAllocMemPerNode(job *protos.JobInfo) string {
	if job.NodeNum == 0 {
		return "0"
	}
	return util.FormatMemToMB(job.GetAllocatedResView().GetMemoryBytes() /
		uint64(job.NodeNum))
}

// 'M' group
func ProcessReqMemPerNode(job *protos.JobInfo) string {
	totalMem := job.GetReqTotalResView().GetMemoryBytes()
	if job.NodeNum > 0 {
		return util.FormatMemToMB(totalMem / uint64(job.NodeNum))
	}
	return util.FormatMemToMB(totalMem)
}

// 'n' group
func ProcessName(job *protos.JobInfo) string {
	return job.Name
}

func ProcessNodeNum(job *protos.JobInfo) string {
	return strconv.FormatUint(uint64(job.NodeNum), 10)
}

// 'o' group
func ProcessCommand(job *protos.JobInfo) string {
	return util.PrivateJobField(job.CmdLine, job.QueryVisibility)
}

// 'p' group
func ProcessPriority(job *protos.JobInfo) string {
	return strconv.FormatUint(uint64(job.Priority), 10)
}

func ProcessPartition(job *protos.JobInfo) string {
	return job.Partition
}

// 'q' group
func ProcessQoS(job *protos.JobInfo) string {
	return job.Qos
}

// 'Q' group
func ProcessReqCPUs(job *protos.JobInfo) string {
	totalCpu := job.GetReqTotalResView().GetCpuCount()
	return strconv.FormatFloat(totalCpu, 'f', 2, 64)
}

// 'r' group
func ProcessReqNodes(job *protos.JobInfo) string {
	return strings.Join(job.ReqNodes, ",")
}

func ProcessReason(job *protos.JobInfo) string {
	if job.Status == protos.JobStatus_Pending {
		return job.GetPendingReason()
	}
	return " "
}

// 's' group
func ProcessSubmitTime(job *protos.JobInfo) string {
	submitTime := job.SubmitTime.AsTime()
	if !submitTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
		return submitTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	return "unknown"
}

func ProcessStartTime(job *protos.JobInfo) string {
	startTime := job.StartTime.AsTime()
	if !startTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) && startTime.Before(time.Now()) {
		return startTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	return "unknown"
}

// 't' group
func ProcessState(job *protos.JobInfo) string {
	return FormatQueueState(job)
}

func ProcessStateCompact(job *protos.JobInfo) string {
	return FormatSlurmJobStateCompact(job.GetStatus())
}

func ProcessJobType(job *protos.JobInfo) string {
	return job.Type.String()
}

// 'u' group
func ProcessUser(job *protos.JobInfo) string {
	return job.Username
}

func ProcessUid(job *protos.JobInfo) string {
	return strconv.FormatUint(uint64(job.Uid), 10)
}

// 'x' group
func ProcessExcludeNodes(job *protos.JobInfo) string {
	return strings.Join(job.ExcludeNodes, ",")
}

// 'X' group
func ProcessExclusive(job *protos.JobInfo) string {
	return strconv.FormatBool(job.Exclusive)
}

type FieldProcessor struct {
	header  string
	process func(job *protos.JobInfo) string
}

var fieldMap = map[string]FieldProcessor{
	// 'a' group
	"a":       {"Account", ProcessAccount},
	"account": {"Account", ProcessAccount},

	// 'c' group
	"c":         {"AllocCpus", ProcessAllocCpus},
	"alloccpus": {"AllocCpus", ProcessAllocCpus},
	"C":         {"ReqCpus", ProcessReqCPUs},
	"reqcpus":   {"ReqCpus", ProcessReqCPUs},

	// 'd' group
	"deadline": {"Deadline", ProcessDeadline},

	// 'e' group
	"e":           {"ElapsedTime", ProcessElapsedTime},
	"elapsedtime": {"ElapsedTime", ProcessElapsedTime},

	// 'h' group
	"h":    {"Held", ProcessHeld},
	"held": {"Held", ProcessHeld},

	// 'j' group
	"j":     {"JobId", ProcessJobId},
	"jobid": {"JobId", ProcessJobId},

	// Group K
	"K":     {"Wckey", ProcessWckey},
	"wckey": {"Wckey", ProcessWckey},

	// 'k' group
	"k":       {"Comment", ProcessComment},
	"comment": {"Comment", ProcessComment},

	// 'l' group
	"l":         {"TimeLimit", ProcessTimeLimit},
	"timelimit": {"TimeLimit", ProcessTimeLimit},
	"L":         {"NodeList(Reason)", ProcessNodeList},
	"nodelist":  {"NodeList(Reason)", ProcessNodeList},

	// 'm' group
	"m":               {"AllocMemPerNode", ProcessAllocMemPerNode},
	"allocmempernode": {"AllocMemPerNode", ProcessAllocMemPerNode},
	"M":               {"ReqMemPerNode", ProcessReqMemPerNode},
	"reqmempernode":   {"ReqMemPerNode", ProcessReqMemPerNode},

	// 'n' group
	"n":       {"Name", ProcessName},
	"name":    {"Name", ProcessName},
	"N":       {"NodeNum", ProcessNodeNum},
	"nodenum": {"NodeNum", ProcessNodeNum},

	"o":       {"Command", ProcessCommand},
	"command": {"Command", ProcessCommand},
	// 'p' group
	"p":         {"Priority", ProcessPriority},
	"priority":  {"Priority", ProcessPriority},
	"P":         {"Partition", ProcessPartition},
	"partition": {"Partition", ProcessPartition},

	// 'q' group
	"q":             {"QoS", ProcessQoS},
	"qos":           {"QoS", ProcessQoS},
	"Q":             {"ReqCpuPerTask", ProcessReqCpuPerTask},
	"reqcpupertask": {"ReqCpuPerTask", ProcessReqCpuPerTask},

	// 'r' group
	"r":        {"ReqNodes", ProcessReqNodes},
	"reqnodes": {"ReqNodes", ProcessReqNodes},
	"R":        {"Reason", ProcessReason},
	"reason":   {"Reason", ProcessReason},

	// 's' group
	"s":          {"SubmitTime", ProcessSubmitTime},
	"submittime": {"SubmitTime", ProcessSubmitTime},
	"S":          {"StartTime", ProcessStartTime},
	"starttime":  {"StartTime", ProcessStartTime},

	// 't' group
	"t":            {"State", ProcessState},
	"state":        {"State", ProcessState},
	"statecompact": {"ST", ProcessStateCompact},
	"T":            {"JobType", ProcessJobType},
	"jobtype":      {"JobType", ProcessJobType},

	// 'u' group
	"u":    {"User", ProcessUser},
	"user": {"User", ProcessUser},
	"U":    {"Uid", ProcessUid},
	"uid":  {"Uid", ProcessUid},

	// 'x' group
	"x":            {"ExcludeNodes", ProcessExcludeNodes},
	"excludenodes": {"ExcludeNodes", ProcessExcludeNodes},
	"X":            {"Exclusive", ProcessExclusive},
	"exclusive":    {"Exclusive", ProcessExclusive},
}

type segmentType int

const (
	textSegment      segmentType = iota // 0
	specifierSegment                    // 1
)

type formatSegment struct {
	segmentType segmentType
	content     string
	width       int
	rightAlign  bool
}

type tableBuilder struct {
	widths     []int
	rightAlign []bool
	headers    []string
	cells      [][]string
}

func NewTableBuilder(rowCount int) *tableBuilder {
	return &tableBuilder{
		cells: make([][]string, rowCount),
	}
}

func BuildColumns(builder *tableBuilder, reply *protos.QueryJobsInfoReply, segments []formatSegment) error {
	for _, seg := range segments {
		switch seg.segmentType {
		case textSegment:
			AddTextColumn(builder, seg.content)
		case specifierSegment:
			if err := AddSpecifierColumn(builder, reply, seg); err != nil {
				return err
			}
		}
	}
	return nil
}

func AddTextColumn(builder *tableBuilder, text string) {
	builder.widths = append(builder.widths, -1)
	builder.rightAlign = append(builder.rightAlign, false)
	builder.headers = append(builder.headers, text)
	for j := range builder.cells {
		builder.cells[j] = append(builder.cells[j], text)
	}
}

func AddSpecifierColumn(builder *tableBuilder, reply *protos.QueryJobsInfoReply, seg formatSegment) error {
	key := seg.content
	if len(key) > 1 {
		key = strings.ToLower(key)
	}
	processor, found := fieldMap[key]
	if !found {
		return fmt.Errorf("invalid format specifier: %s", key)
	}

	builder.widths = append(builder.widths, seg.width)
	builder.rightAlign = append(builder.rightAlign, seg.rightAlign)
	builder.headers = append(builder.headers, strings.ToUpper(processor.header))

	for j, job := range reply.JobInfoList {
		builder.cells[j] = append(builder.cells[j], processor.process(job))
	}
	return nil
}

// `%(\.)?(\d+)?([a-zA-Z]+)`
// This regular expression starts with a percent sign (%), which may be followed
// by an optional dot (.) for right alignment, an optional number for width,
// and finally followed by a string of one or more letters.
// Examples: "%5j" (left align, width 5), "%.5j" (right align, width 5), "%j" (no width)
// For example "-o=ID:%.5j | Status:%5t"
// return specifier is a int[][] arr
//
//	ID:%.5j | Status:%5t
//	012345678901234567
//
// The part corresponding to the ID, specifier[0]=[3,8,4,5,5,6,7,8]
// %.5j --> [3,8)  . --> [4,5)  5 --> [5,6)  j --> [7,8)
// Parse into    [start,end)   [dot_start,dot_end)   [num_start,num_end)   [spec_start,spec_end)
//
//	0      1       2          3              4          5              6          7
func ParseFormatSegments(format string, re *regexp.Regexp) ([]formatSegment, error) {
	specifiers := re.FindAllStringSubmatchIndex(format, -1)
	if specifiers == nil {
		return nil, fmt.Errorf("invalid format specifier")
	}

	var segments []formatSegment
	// ID:%.5j because specifiers[0][0] == 3 , so content ；format[0:start]
	if start := specifiers[0][0]; start != 0 {
		segments = append(segments, formatSegment{
			segmentType: textSegment,
			content:     format[0:start],
		})
	}

	for i, spec := range specifiers {
		// i > 0 , i = 0 the situation has been handled separately above
		if i > 0 {
			// get the ending position of the previous one
			prevEnd := specifiers[i-1][1]
			// If gap < 0, this is incorrect. The starting position index of
			// the second item must be greater than the ending position of the last item
			// content : format[prevEnd:spec[0]]
			if gap := spec[0] - prevEnd; gap > 0 {
				segments = append(segments, formatSegment{
					segmentType: textSegment,
					content:     format[prevEnd:spec[0]],
				})
			}
		}
		width := -1
		rightAlign := false

		// spec[2] != -1 means dot (.) is present -> right alignment
		if spec[2] != -1 {
			rightAlign = true
		}

		// spec[4] != -1 means width is specified
		if spec[4] != -1 {
			widthVal, err := strconv.ParseUint(format[spec[4]:spec[5]], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid width specifier")
			}
			width = int(widthVal)
		}

		//get the field specifier
		field := format[spec[6]:spec[7]]
		segments = append(segments, formatSegment{
			segmentType: specifierSegment,
			content:     field,
			width:       width,
			rightAlign:  rightAlign,
		})
	}

	// The last spec needs to be processed separately,
	// as it may still contain some characters after the spec ends
	if lastSpec := specifiers[len(specifiers)-1]; len(format) > lastSpec[1] {
		segments = append(segments, formatSegment{
			segmentType: textSegment,
			content:     format[lastSpec[1]:],
		})
	}

	return segments, nil
}

// FormatData formats the output data according to the format string
// format : "%j" or "%5j" or "%.5j" or  "ID:%j"  or "ID:%5j"
// cqueue -o="ID:%.4j | Status:%t | user:%u
// ID:JOBID | Status:STATE   | user:USER
// ID:44    | Status:Running | user:internthree
// ID:45    | Status:Running | user:internthree
func FormatData(reply *protos.QueryJobsInfoReply) (header []string, tableData [][]string, err error) {
	var formatSpecRegex = regexp.MustCompile(`%(\.)?(\d+)?([a-zA-Z]+)`)

	segments, err := ParseFormatSegments(FlagFormat, formatSpecRegex)
	if err != nil {
		return nil, nil, util.NewCraneErr(util.ErrorInvalidFormat, err.Error())
	}

	builder := NewTableBuilder(len(reply.JobInfoList))
	if err := BuildColumns(builder, reply, segments); err != nil {
		return nil, nil, util.NewCraneErr(util.ErrorInvalidFormat, err.Error())
	}

	header, tableData = util.FormatTable(builder.widths, builder.headers, builder.cells, builder.rightAlign)
	return header, tableData, nil
}
