package cinfo

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"bytes"
	"cmp"
	"fmt"
	"maps"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
)

// Define the flattened data structure
type FlattenedData struct {
	PartitionName   string
	Avail           string
	CranedListRegex string
	ResourceState   protos.CranedResourceState
	ControlState    protos.CranedControlState
	PowerState      protos.CranedPowerState
	CranedListCount uint64
	HasNodes        bool
	Reason          string
}

// groupClusterNodes owns cinfo's fixed native state/reason grouping. Partition
// totals and membership in the protobuf reply remain complete after filtering.
func groupClusterNodes(reply *protos.QueryClusterInfoReply) map[string][]FlattenedData {
	type stateKey struct {
		control  protos.CranedControlState
		resource protos.CranedResourceState
		power    protos.CranedPowerState
		reason   string
	}
	groups := make(map[string]map[stateKey][]string, len(reply.Partitions))
	for _, part := range reply.Partitions {
		groups[part.Name] = make(map[stateKey][]string)
	}
	for _, node := range reply.Nodes {
		key := stateKey{node.ControlState, node.ResourceState, node.PowerState, node.StateReason}
		for _, part := range node.PartitionNames {
			if group, ok := groups[part]; ok {
				group[key] = append(group[key], node.Hostname)
			}
		}
	}
	rows := make(map[string][]FlattenedData, len(reply.Partitions))
	for _, part := range reply.Partitions {
		name := part.Name
		if part.IsDefault {
			name += "*"
		}
		base := FlattenedData{PartitionName: name, Avail: strings.ToLower(strings.TrimPrefix(part.State.String(), "PARTITION_"))}
		keys := slices.SortedFunc(maps.Keys(groups[part.Name]), func(a, b stateKey) int {
			return cmp.Or(cmp.Compare(a.control, b.control), cmp.Compare(a.resource, b.resource),
				cmp.Compare(a.power, b.power), cmp.Compare(a.reason, b.reason))
		})
		for _, key := range keys {
			row := base
			hosts := groups[part.Name][key]
			row.ResourceState, row.ControlState, row.PowerState = key.resource, key.control, key.power
			row.Reason, row.HasNodes = key.reason, true
			row.CranedListCount, row.CranedListRegex = uint64(len(hosts)), util.HostNameListToStr(hosts)
			rows[part.Name] = append(rows[part.Name], row)
		}
		if len(keys) == 0 {
			rows[part.Name] = []FlattenedData{base}
		}
	}
	return rows
}

func FlattenReplyData(reply *protos.QueryClusterInfoReply) []FlattenedData {
	var populated, empty []FlattenedData
	groups := groupClusterNodes(reply)
	for _, part := range reply.Partitions {
		for _, row := range groups[part.Name] {
			if row.HasNodes {
				populated = append(populated, row)
			} else {
				empty = append(empty, row)
			}
		}
	}
	return append(populated, empty...)
}

type FieldProcessor struct {
	header  string
	process func(flattened []FlattenedData, tableOutputCell [][]string)
}

var fieldMap = map[string]FieldProcessor{
	"p":         {"Partition", ProcessPartition},
	"partition": {"Partition", ProcessPartition},
	"a":         {"Avail", ProcessAvail},
	"avail":     {"Avail", ProcessAvail},
	"n":         {"Nodes", ProcessNodes},
	"nodes":     {"Nodes", ProcessNodes},
	"s":         {"State", ProcessState},
	"state":     {"State", ProcessState},
	"l":         {"NodeList", ProcessNodeList},
	"nodelist":  {"NodeList", ProcessNodeList},
}

// / Partition
func ProcessPartition(flattened []FlattenedData, tableOutputCell [][]string) {
	for idx, data := range flattened {
		tableOutputCell[idx] = append(tableOutputCell[idx], data.PartitionName)
	}
}

// Avail
func ProcessAvail(flattened []FlattenedData, tableOutputCell [][]string) {
	for idx, data := range flattened {
		tableOutputCell[idx] = append(tableOutputCell[idx], data.Avail)
	}
}

// Nodes
func ProcessNodes(flattened []FlattenedData, tableOutputCell [][]string) {
	for idx, data := range flattened {
		tableOutputCell[idx] = append(tableOutputCell[idx], strconv.FormatUint(data.CranedListCount, 10))
	}
}

// State
func ProcessState(flattened []FlattenedData, tableOutputCell [][]string) {
	for idx, data := range flattened {
		tableOutputCell[idx] = append(tableOutputCell[idx], BuildStateString(data))
	}
}

// NodeList
func ProcessNodeList(flattened []FlattenedData, tableOutputCell [][]string) {
	for idx, data := range flattened {
		tableOutputCell[idx] = append(tableOutputCell[idx], data.CranedListRegex)
	}
}

func ParseBySpec(specifiers [][]int, reply *protos.QueryClusterInfoReply) ([]int, []string, [][]string, error) {
	tableOutputWidth := make([]int, 0, len(specifiers))
	tableOutputHeader := make([]string, 0, len(specifiers))
	flattened := FlattenReplyData(reply)
	tableLen := len(flattened)
	tableOutputCell := make([][]string, tableLen)

	// Get the prefix of the format string
	if specifiers[0][0] != 0 {
		prefix := FlagFormat[0:specifiers[0][0]]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputHeader = append(tableOutputHeader, prefix)
		for j := 0; j < tableLen; j++ {
			tableOutputCell[j] = append(tableOutputCell[j], prefix)
		}
	}

	for i, spec := range specifiers {
		// Get the padding string between specifiers
		if i > 0 && spec[0]-specifiers[i-1][1] > 0 {
			padding := FlagFormat[specifiers[i-1][1]:spec[0]]
			tableOutputWidth = append(tableOutputWidth, -1)
			tableOutputHeader = append(tableOutputHeader, padding)
			for j := 0; j < tableLen; j++ {
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
				return nil, nil, nil, util.NewCraneErr(
					util.ErrorInvalidFormat,
					"Invalid width specifier.",
				)
			}
			tableOutputWidth = append(tableOutputWidth, int(width))
		}

		// Parse format specifier
		field := FlagFormat[spec[4]:spec[5]]
		if len(field) > 1 {
			field = strings.ToLower(field)
		}

		if processor, exists := fieldMap[field]; exists {
			tableOutputHeader = append(tableOutputHeader, strings.ToUpper(processor.header))
			processor.process(flattened, tableOutputCell)
		} else {
			return nil, nil, nil, util.NewCraneErr(
				util.ErrorInvalidFormat,
				fmt.Sprintf("Invalid format specifier or string: %s, string unfold case insensitive, reference:\n"+
					"p/Partition, a/Avail, n/Nodes, s/State, l/NodeList.", field),
			)
		}
	}
	// Get the suffix of the format string
	if len(FlagFormat)-specifiers[len(specifiers)-1][1] > 0 {
		suffix := FlagFormat[specifiers[len(specifiers)-1][1]:]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputHeader = append(tableOutputHeader, suffix)
		for j := 0; j < tableLen; j++ {
			tableOutputCell[j] = append(tableOutputCell[j], suffix)
		}
	}

	return tableOutputWidth, tableOutputHeader, tableOutputCell, nil
}

func FormatData(reply *protos.QueryClusterInfoReply) (header []string, tableData [][]string, err error) {
	re := regexp.MustCompile(`%(\.\d+)?([a-zA-Z]+)`)
	specifiers := re.FindAllStringSubmatchIndex(FlagFormat, -1)
	if specifiers == nil {
		return nil, nil, util.NewCraneErr(util.ErrorInvalidFormat, "Invalid format specifier.")
	}

	tableOutputWidth, tableOutputHeader, tableOutputCell, err := ParseBySpec(specifiers, reply)
	if err != nil {
		return nil, nil, err
	}

	formattedHeader, formattedData := util.FormatTable(tableOutputWidth, tableOutputHeader, tableOutputCell)
	return formattedHeader, formattedData, nil
}

func BuildStateString(cranedList FlattenedData) string {
	if !cranedList.HasNodes {
		return "n/a"
	}
	if util.IsSlurmOutputMode() {
		return util.FormatSlurmNodeState(
			cranedList.ResourceState, cranedList.ControlState, cranedList.PowerState)
	}
	stateStr := strings.ToLower(strings.TrimPrefix(cranedList.ResourceState.String(), "CRANE_"))

	if cranedList.ControlState != protos.CranedControlState_CRANE_NONE {
		controlState := strings.ToLower(strings.TrimPrefix(cranedList.ControlState.String(), "CRANE_"))
		stateStr += "(" + controlState + ")"
	}

	powerStateSuffix := "[" + strings.ToLower(strings.TrimPrefix(cranedList.PowerState.String(), "CRANE_")) + "]"
	if cranedList.ResourceState == protos.CranedResourceState_CRANE_DOWN &&
		(cranedList.PowerState == protos.CranedPowerState_CRANE_POWER_IDLE ||
			cranedList.PowerState == protos.CranedPowerState_CRANE_POWER_ACTIVE) {
		powerStateSuffix = "[failed]"
	}
	stateStr += powerStateSuffix

	return stateStr
}

func FindTableDataByReply(reply *protos.QueryClusterInfoReply) [][]string {
	var tableData [][]string
	for _, data := range FlattenReplyData(reply) {
		row := []string{data.PartitionName, data.Avail, strconv.FormatUint(data.CranedListCount, 10),
			BuildStateString(data), data.CranedListRegex}
		if FlagListReason {
			row = append(row, data.Reason)
		}
		tableData = append(tableData, row)
	}
	return tableData
}

func FillTable(reply *protos.QueryClusterInfoReply, table *tablewriter.Table) error {
	var err error
	header := []string{"PARTITION", "AVAIL", "NODES", "STATE", "NODELIST"}
	tableData := FindTableDataByReply(reply)
	if FlagListReason {
		header = append(header, "REASON")
		tableData = FilterBadState(tableData)
	}

	if FlagFormat != "" {
		header, tableData, err = FormatData(reply)
		if err != nil {
			return err
		}
		table.SetTablePadding("")
		table.SetAutoFormatHeaders(false)
	}
	table.AppendBulk(tableData)
	if !FlagNoHeader {
		table.SetHeader(header)
	}
	if len(tableData) == 0 {
		log.Info("No matching partitions were found for the given filter.")
	} else {
		table.Render()
	}

	return nil
}

func GetFoundedNodes(reply *protos.QueryClusterInfoReply) map[string]bool {
	found := make(map[string]bool, len(reply.Nodes))
	for _, node := range reply.Nodes {
		found[node.Hostname] = true
	}
	return found
}

func GetNodeList() []string {
	var nodeList []string
	for _, node := range FlagFilterNodes {
		if node == "" {
			log.Warn("Empty node name is ignored.")
			continue
		}
		nodeList = append(nodeList, node)
	}
	return nodeList
}

func ExtraDealNodeList(reply *protos.QueryClusterInfoReply, aliases map[string]string) {
	var redList []string
	requestedNodes_, _ := util.ParseHostList(strings.Join(GetNodeList(), ","))
	foundedNodes := GetFoundedNodes(reply)
	for _, node := range requestedNodes_ {
		canonicalNode, ok := aliases[node]
		if !ok {
			canonicalNode = node
		}
		_, exist := foundedNodes[canonicalNode]
		if !exist {
			redList = append(redList, node)
		}
	}
	if len(redList) > 0 {
		log.Infof("Requested nodes do not exist or do not meet the given filter condition: %s.",
			util.HostNameListToStr(redList))
	}
}

func trimTableLinePadding(output string) string {
	lines := strings.SplitAfter(output, "\n")
	for i, line := range lines {
		if strings.HasSuffix(line, "\n") {
			content := strings.TrimSuffix(line, "\n")
			lines[i] = strings.TrimRight(content, " \t") + "\n"
		} else {
			lines[i] = strings.TrimRight(line, " \t")
		}
	}
	return strings.Join(lines, "")
}

func QueryTableOutput(reply *protos.QueryClusterInfoReply, aliases map[string]string) error {
	var output bytes.Buffer
	table := tablewriter.NewWriter(&output)
	util.SetBorderlessTable(table)

	if err := FillTable(reply, table); err != nil {
		return err
	}
	fmt.Fprint(os.Stdout, trimTableLinePadding(output.String()))

	if len(FlagFilterNodes) != 0 {
		ExtraDealNodeList(reply, aliases)
	}
	return nil
}

func JsonOutput(reply *protos.QueryClusterInfoReply) error {
	if util.IsSlurmOutputMode() {
		output, err := formatSlurmClusterJSON(reply)
		if err != nil {
			return util.WrapCraneErr(util.ErrorInvalidFormat, "%v", err)
		}
		fmt.Println(output)
	} else {
		fmt.Println(util.FmtJson.FormatReply(reply))
	}
	if reply.GetOk() {
		return nil
	} else {
		return util.NewCraneErr(util.ErrorBackend, "Backend returned non-ok status")
	}
}

func FilterBadState(tableData [][]string) [][]string {

	var new_table [][]string
	//TODO: need to expand
	pattern := `(drain|down)`

	re := regexp.MustCompile(pattern)
	for _, data := range tableData {
		//FIXME: HARD CODE
		if re.MatchString(data[3]) {
			new_table = append(new_table, data)
		}
	}

	return new_table
}
