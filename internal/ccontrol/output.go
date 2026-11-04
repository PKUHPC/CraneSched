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

package ccontrol

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/user"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

func expandHostlist(hostlist string) ([]string, error) {
	normalized := strings.ReplaceAll(hostlist, ";", ",")
	normalized = strings.Join(strings.Fields(normalized), " ")
	normalized = strings.ReplaceAll(normalized, ", ", ",")
	normalized = strings.ReplaceAll(normalized, " ,", ",")
	normalized = strings.ReplaceAll(normalized, " ", ",")
	if normalized == "" {
		return nil, fmt.Errorf("host list is empty")
	}

	hostnames, ok := util.ParseHostList(normalized)
	if !ok || len(hostnames) == 0 {
		return nil, fmt.Errorf("invalid hostlist: %s", hostlist)
	}
	for _, hostname := range hostnames {
		if hostname == "" {
			return nil, fmt.Errorf("invalid hostlist: %s", hostlist)
		}
	}
	return hostnames, nil
}

func printHostnames(writer io.Writer, hostnames []string) error {
	for _, hostname := range hostnames {
		if _, err := fmt.Fprintln(writer, hostname); err != nil {
			return err
		}
	}
	return nil
}

func nodeHostnames(nodes []*protos.CranedInfo) []string {
	hostnames := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if hostname := node.GetHostname(); hostname != "" {
			hostnames = append(hostnames, hostname)
		}
	}
	return hostnames
}

// show Config
func PrintFlattenYAML(prefix string, m interface{}) {
	switch v := m.(type) {
	case map[string]interface{}:
		for key, value := range v {
			newPrefix := key
			if prefix != "" {
				newPrefix = prefix + "." + key
			}
			PrintFlattenYAML(newPrefix, value)
		}
	case []interface{}:
		for i, value := range v {
			PrintFlattenYAML(fmt.Sprintf("%s.%d", prefix, i), value)
		}
	default:
		fmt.Printf("%s = %v\n", prefix, v)
	}
}
func ShowConfig(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Failed to read configuration file: %s", err)
	}

	var config map[string]interface{}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Failed to unmarshal yaml configuration file: %s", err)
	}
	if FlagJson {
		output, _ := json.Marshal(config)
		fmt.Println(string(output))
	} else {
		PrintFlattenYAML("", config)
	}

	return nil
}

func printTraceConfig(config *protos.RuntimeTraceConfig) {
	if config == nil {
		fmt.Println("trace config is unavailable")
		return
	}
	fmt.Printf("compiled_with_tracing = %t\n", config.GetCompiledWithTracing())
	fmt.Printf("compiled_max_level = %s\n", config.GetCompiledMaxLevel())
	fmt.Printf("runtime_enabled = %t\n", config.GetRuntimeEnabled())
	fmt.Printf("runtime_level = %s\n", config.GetRuntimeLevel())
	fmt.Printf("effective_level = %s\n", config.GetEffectiveLevel())
	fmt.Printf("clamped = %t\n", config.GetClamped())
}

func ShowTraceConfig() error {
	req := &protos.QueryTraceConfigRequest{Uid: userUid}
	reply, err := stub.QueryTraceConfig(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(
			util.ErrorNetwork, err, "Failed to show trace config")
	}
	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		return nil
	}
	if !reply.GetOk() {
		return util.NewCraneErr(
			util.ErrorBackend,
			fmt.Sprintf("Failed to show trace config: %s", reply.GetReason()))
	}
	printTraceConfig(reply.GetConfig())
	return nil
}

// show Nodes
func formatGresMap(data *protos.GresMap) string {
	if data == nil {
		return "None"
	}
	var kvStrings []string
	for gresName, gresCount := range data.NameGresMap {
		if len(gresCount.Specified) > 0 {
			for typeName, count := range gresCount.Specified {
				if count != 0 {
					kvStrings = append(kvStrings, fmt.Sprintf("%s:%s:%d", gresName, typeName, count))
				}
			}
		} else if gresCount.Total != 0 {
			kvStrings = append(kvStrings, fmt.Sprintf("%s:%d", gresName, gresCount.Total))
		}
	}
	if len(kvStrings) == 0 {
		return "None"
	}
	return strings.Join(kvStrings, ", ")
}
func formatDedicatedResource(data *protos.DedicatedResourceInNode) string {
	if data == nil {
		return "None"
	}

	var kvStrings []string
	for deviceName, typeCountMap := range data.NameTypeMap {
		var typeCountPairs []string
		for deviceType, slots := range typeCountMap.TypeSlotsMap {
			slotsSize := len(slots.Slots)
			if slotsSize != 0 {
				typeCountPairs = append(typeCountPairs, fmt.Sprintf("%s:%d", deviceType, slotsSize))
			}
		}
		for _, typeCountPair := range typeCountPairs {
			kvStrings = append(kvStrings, fmt.Sprintf("%s:%s", deviceName, typeCountPair))
		}
	}
	if len(kvStrings) == 0 {
		return "None"
	}
	kvString := strings.Join(kvStrings, ", ")

	return kvString
}
func formatAllowedAccounts(allowedAccounts []string) string {
	if len(allowedAccounts) == 0 {
		return "ALL"
	}
	return strings.Join(allowedAccounts, ",")
}
func formatDeniedAccounts(deniedAccounts []string) string {
	if len(deniedAccounts) == 0 {
		return "None"
	}

	return strings.Join(deniedAccounts, ",")
}

func getCranedNodesReply(nodeName string) (*protos.QueryCranedInfoReply, error) {
	req := &protos.QueryCranedInfoRequest{CranedName: nodeName}
	reply, err := stub.QueryCranedInfo(context.Background(), req)
	if err != nil {
		return nil, util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to show nodes")
	}
	return reply, nil
}
func handleNodesEmptyResult(nodeName string, queryAll bool) error {
	if queryAll {
		fmt.Println("No node is available.")
		return nil
	}
	return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Node %s not found.", nodeName))
}

func outputNodes(nodes []*protos.CranedInfo) {
	for _, node := range nodes {
		printNodeDetails(node)
	}
}

func printNodeDetails(node *protos.CranedInfo) {
	stateStr := formatNodeState(node)
	startTimeField := "CranedStartTime"
	if util.IsSlurmOutputMode() {
		startTimeField = "SlurmdStartTime"
	}
	cranedVersion := "unknown"
	if len(node.CranedVersion) > 0 {
		cranedVersion = node.CranedVersion
	}
	cranedOs := "unknown"
	if len(node.SystemDesc) > 2 {
		cranedOs = node.SystemDesc
	}
	// time/cpu/mem/gres
	timeInfo := formatNodeTimes(node)
	cpuInfo := formatCpuInfo(node)
	memInfo := formatMemInfo(node)
	gresInfo := formatGresInfo(node)

	fmt.Printf(
		"NodeName=%v State=%v %s\n"+
			"\tNodeAddr=%s NodeHostName=%s\n"+
			"\t%s\n"+
			"\t%s\n"+
			"\tSockets=%d\n"+
			"\tPartition=%s RunningJob=%d Version=%s\n"+
			"\tOs=%s\n"+
			"\tBootTime=%s %s=%s\n"+
			"\tLastBusyTime=%s\n",
		node.Hostname, stateStr, cpuInfo,
		node.NodeAddr, node.NodeHostname,
		memInfo,
		gresInfo,
		node.GetNodeTopoInfo().GetSockets(),
		strings.Join(node.PartitionNames, ","), node.RunningJobNum, cranedVersion,
		cranedOs,
		timeInfo.bootTime, startTimeField, timeInfo.startTime, timeInfo.lastBusyTime,
	)
}

type nodeTimes struct {
	bootTime     string
	startTime    string
	lastBusyTime string
}

func formatNodeTimes(node *protos.CranedInfo) nodeTimes {
	formatTime := func(t *timestamppb.Timestamp) string {
		if t == nil || t.AsTime().Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
			return "unknown"
		}
		return t.AsTime().In(time.Local).Format("2006-01-02 15:04:05")
	}
	return nodeTimes{
		bootTime:     formatTime(node.SystemBootTime),
		startTime:    formatTime(node.CranedStartTime),
		lastBusyTime: formatTime(node.LastBusyTime),
	}
}

func formatNodeState(node *protos.CranedInfo) string {
	if util.IsSlurmOutputMode() {
		return util.FormatSlurmNodeState(node.ResourceState, node.ControlState, node.PowerState)
	}
	stateStr := strings.ToLower(node.ResourceState.String()[6:])
	if node.ControlState != protos.CranedControlState_CRANE_NONE {
		stateStr += "(" + strings.ToLower(node.ControlState.String()[6:]) + ")"
	}
	stateStr += "[" + strings.ToLower(node.PowerState.String()[6:]) + "]"
	return stateStr
}

// Cpu
func formatCpuInfo(node *protos.CranedInfo) string {
	return fmt.Sprintf("CPU=%.2f AllocCPU=%.2f FreeCPU=%.2f",
		math.Round(node.ResTotal.CpuCount*100)/100,
		math.Round(math.Abs(node.ResAlloc.CpuCount)*100)/100,
		math.Round(math.Abs(node.ResAvail.CpuCount)*100)/100,
	)
}

// Mem
func formatMemInfo(node *protos.CranedInfo) string {
	return fmt.Sprintf("RealMemory=%s AllocMem=%s FreeMem=%s",
		util.FormatMemToMB(node.ResTotal.MemoryBytes),
		util.FormatMemToMB(node.ResAlloc.MemoryBytes),
		util.FormatMemToMB(node.ResAvail.MemoryBytes),
	)
}

// Gres
func formatGresInfo(node *protos.CranedInfo) string {
	return fmt.Sprintf("Gres=%s AllocGres=%s FreeGres=%s",
		formatDedicatedResource(node.ResTotal.GetGres()),
		formatDedicatedResource(node.ResAlloc.GetGres()),
		formatDedicatedResource(node.ResAvail.GetGres()),
	)
}

func ShowNodes(nodeName string, queryAll bool) error {
	reply, err := getCranedNodesReply(nodeName)
	if err != nil {
		return err
	}
	if FlagJson {
		if util.IsSlurmOutputMode() {
			output, err := formatSlurmNodesJSON(reply)
			if err != nil {
				return util.WrapCraneErr(util.ErrorInvalidFormat, "%v", err)
			}
			fmt.Println(output)
		} else {
			fmt.Println(util.FmtJson.FormatReply(reply))
		}
		return nil
	}
	if len(reply.CranedInfoList) == 0 {
		return handleNodesEmptyResult(nodeName, queryAll)
	}
	outputNodes(reply.CranedInfoList)
	return nil
}

// show Partitions
func getPartitionInfoReply(partitionName string) (*protos.QueryPartitionInfoReply, error) {
	req := &protos.QueryPartitionInfoRequest{PartitionName: partitionName}
	reply, err := stub.QueryPartitionInfo(context.Background(), req)
	if err != nil {
		return nil, util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to show partitions")
	}
	return reply, nil
}
func handleEmptyPartitionResult(partitionName string, queryAll bool) error {
	if queryAll {
		fmt.Println("No partition is available.")
		return nil
	}
	return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Partition %s not found.", partitionName))
}

func outputPartitions(partitions []*protos.PartitionInfo) {
	for _, partition := range partitions {
		printPartitionDetails(partition)
	}
}

func printPartitionDetails(partition *protos.PartitionInfo) {
	accountsInfo := formatAccountsInfo(partition)
	nodesInfo := fmt.Sprintf("TotalNodes=%d AliveNodes=%d", partition.TotalNodes, partition.AliveNodes)
	cpuInfo := formatCpuResources(partition)
	memInfo := formatMemoryResources(partition)
	gresInfo := formatGresResources(partition)

	memLimits := fmt.Sprintf("DefaultMemPerCPU=%s MaxMemPerCPU=%s DefaultMemPerNode=%s MaxMemPerNode=%s",
		util.FormatMemToMB(partition.DefaultMemPerCpu),
		util.FormatMemToMB(partition.MaxMemPerCpu),
		util.FormatMemToMB(partition.DefaultMemPerNode),
		util.FormatMemToMB(partition.MaxMemPerNode))
	fmt.Printf("PartitionName=%v State=%v\n"+
		"\t%s\n"+
		"\t%s\n"+
		"\t%s\n"+
		"\t%s\n"+
		"\t%s\n"+
		"\t%s\n"+
		"\tHostList=%v\n\n",
		partition.Name, partition.State.String()[10:],
		accountsInfo,
		nodesInfo,
		cpuInfo,
		memInfo,
		gresInfo,
		memLimits,
		partition.Hostlist)
}

func formatAccountsInfo(partition *protos.PartitionInfo) string {
	allowed := formatAllowedAccounts(partition.AllowedAccounts)
	denied := formatDeniedAccounts(partition.DeniedAccounts)
	return fmt.Sprintf("AllowedAccounts=%s DeniedAccounts=%s", allowed, denied)
}

func formatCpuResources(partition *protos.PartitionInfo) string {
	total := math.Round(math.Abs(partition.ResTotal.CpuCount)*100) / 100
	avail := math.Round(math.Abs(partition.ResAvail.CpuCount)*100) / 100
	alloc := math.Round(math.Abs(partition.ResAlloc.CpuCount)*100) / 100
	return fmt.Sprintf("TotalCPU=%.2f AvailCPU=%.2f AllocCPU=%.2f", total, avail, alloc)
}
func formatMemoryResources(partition *protos.PartitionInfo) string {
	total := util.FormatMemToMB(partition.ResTotal.MemoryBytes)
	avail := util.FormatMemToMB(partition.ResAvail.MemoryBytes)
	alloc := util.FormatMemToMB(partition.ResAlloc.MemoryBytes)
	return fmt.Sprintf("TotalMem=%s AvailMem=%s AllocMem=%s", total, avail, alloc)
}
func formatGresResources(partition *protos.PartitionInfo) string {
	total := formatGresMap(partition.ResTotal.GetGresMap())
	avail := formatGresMap(partition.ResAvail.GetGresMap())
	alloc := formatGresMap(partition.ResAlloc.GetGresMap())
	return fmt.Sprintf("TotalGres=%s AvailGres=%s AllocGres=%s", total, avail, alloc)
}

func ShowPartitions(partitionName string, queryAll bool) error {
	reply, err := getPartitionInfoReply(partitionName)
	if err != nil {
		return err
	}
	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		return nil
	}
	if len(reply.PartitionInfoList) == 0 {
		return handleEmptyPartitionResult(partitionName, queryAll)
	}
	outputPartitions(reply.PartitionInfoList)
	return nil
}

// show Reservation
func getReservationInfoReply(reservationName string) (*protos.QueryReservationInfoReply, error) {
	req := &protos.QueryReservationInfoRequest{
		Uid:             uint32(os.Getuid()),
		ReservationName: reservationName,
	}
	reply, err := stub.QueryReservationInfo(context.Background(), req)
	if err != nil {
		return nil, util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to show reservations")
	}
	if !reply.GetOk() {
		return nil, util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to retrieve reservation info: %s", reply.GetReason()))
	}
	return reply, nil
}
func handleEmptyReservationResult(reservationName string, queryAll bool) error {
	if queryAll {
		fmt.Println("No reservation is available.")
		return nil
	}
	return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Reservation %s not found.", reservationName))
}
func outputReservations(reservations []*protos.ReservationInfo) {
	for _, res := range reservations {
		fmt.Println(formatReservationDetails(res))
	}
}

// strings.Builder
func formatReservationDetails(res *protos.ReservationInfo) string {
	var buf strings.Builder

	buf.WriteString(fmt.Sprintf("ReservationName=%v\n", res.ReservationName))
	buf.WriteString(fmt.Sprintf("\tStartTime=%v Duration=%v\n",
		res.StartTime.AsTime().In(time.Local).Format("2006-01-02 15:04:05"),
		res.Duration.AsDuration().String()))
	buf.WriteString(formatReservationTarget(res))
	buf.WriteString(formatReservationAccounts(res))
	buf.WriteString(formatReservationResources(res))

	return buf.String()
}

func formatReservationTarget(res *protos.ReservationInfo) string {
	nodeField := "CranedRegex"
	if util.IsSlurmOutputMode() {
		nodeField = "Nodes"
	}
	if res.Partition != "" && res.CranedRegex != "" {
		return fmt.Sprintf("\tPartition=%v %s=%v\n", res.Partition, nodeField, res.CranedRegex)
	} else if res.Partition != "" {
		return fmt.Sprintf("\tPartition=%v\n", res.Partition)
	} else if res.CranedRegex != "" {
		return fmt.Sprintf("\t%s=%v\n", nodeField, res.CranedRegex)
	}
	return ""
}
func formatReservationAccounts(res *protos.ReservationInfo) string {
	var buf strings.Builder
	if len(res.AllowedAccounts) > 0 {
		buf.WriteString(fmt.Sprintf("\tAllowedAccounts=%s\n", strings.Join(res.AllowedAccounts, ",")))
	}
	if len(res.DeniedAccounts) > 0 {
		buf.WriteString(fmt.Sprintf("\tDeniedAccounts=%s\n", strings.Join(res.DeniedAccounts, ",")))
	}
	if len(res.AllowedUsers) > 0 {
		buf.WriteString(fmt.Sprintf("\tAllowedUsers=%s\n", strings.Join(res.AllowedUsers, ",")))
	}
	if len(res.DeniedUsers) > 0 {
		buf.WriteString(fmt.Sprintf("\tDeniedUsers=%s\n", strings.Join(res.DeniedUsers, ",")))
	}
	return buf.String()
}
func formatReservationResources(res *protos.ReservationInfo) string {
	var buf strings.Builder
	// CPU
	buf.WriteString(fmt.Sprintf("\tTotalCPU=%.2f AvailCPU=%.2f AllocCPU=%.2f\n",
		math.Abs(res.ResTotal.CpuCount),
		math.Abs(res.ResAvail.CpuCount),
		math.Abs(res.ResAlloc.CpuCount)))

	// mem
	buf.WriteString(fmt.Sprintf("\tTotalMem=%s AvailMem=%s AllocMem=%s\n",
		util.FormatMemToMB(res.ResTotal.MemoryBytes),
		util.FormatMemToMB(res.ResAvail.MemoryBytes),
		util.FormatMemToMB(res.ResAlloc.MemoryBytes)))

	// Gres
	buf.WriteString(fmt.Sprintf("\tTotalGres=%s AvailGres=%s AllocGres=%s\n",
		formatGresMap(res.ResTotal.GetGresMap()),
		formatGresMap(res.ResAvail.GetGresMap()),
		formatGresMap(res.ResAlloc.GetGresMap())))

	return buf.String()
}

func ShowReservations(reservationName string, queryAll bool) error {
	reply, err := getReservationInfoReply(reservationName)
	if err != nil {
		return err
	}
	if FlagJson {
		if util.IsSlurmOutputMode() {
			output, err := formatSlurmReservationsJSON(reply)
			if err != nil {
				return util.WrapCraneErr(util.ErrorInvalidFormat, "%v", err)
			}
			fmt.Println(output)
		} else {
			fmt.Println(util.FmtJson.FormatReply(reply))
		}
		return nil
	}
	if len(reply.ReservationInfoList) == 0 {
		return handleEmptyReservationResult(reservationName, queryAll)
	}
	outputReservations(reply.ReservationInfoList)
	return nil
}

func parseJobIdSelectors(jobIds string, queryAll bool) ([]*protos.JobIdSelector, error) {
	if queryAll {
		return nil, nil
	}
	selectors, err := util.ParseJobIdSelectorList(jobIds, ",")
	if err != nil {
		return nil, util.WrapCraneErr(util.ErrorCmdArg, "Invalid job list specified: %s.", err)
	}
	for _, selector := range selectors {
		if len(selector.Steps) > 0 {
			return nil, util.NewCraneErr(util.ErrorCmdArg, "Step selector is unsupported in show job, use show step for step queries.")
		}
	}
	return selectors, nil
}

func parseStepIdSelectors(jobIds string, queryAll bool) ([]*protos.JobIdSelector, error) {
	if queryAll {
		return nil, nil
	}
	selectors, err := util.ParseJobIdSelectorList(jobIds, ",")
	if err != nil {
		return nil, util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid job list specified: %s.", err))
	}
	return selectors, nil
}

func getJobInfoReply(selectors []*protos.JobIdSelector) (*protos.QueryJobsInfoReply, error) {
	req := &protos.QueryJobsInfoRequest{
		FilterJobIds: selectors,
	}
	reply, err := util.QueryJobsInfo(context.Background(), stub, req)
	if err != nil {
		return nil, util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to show jobs")
	}

	if !reply.GetOk() {
		return nil, util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to retrieve information for job %v", selectors))
	}
	return reply, nil
}

func handleEmptyJobResult(selectors []*protos.JobIdSelector, queryAll bool) error {
	if queryAll {
		fmt.Println("No job is running.")
		return nil
	}

	ids := make([]string, 0, len(selectors))
	for _, sel := range selectors {
		ids = append(ids, formatSelector(sel))
	}
	fmt.Printf("Job %s is not running.\n", strings.Join(ids, ", "))
	return nil
}

func outputJobs(jobs []*protos.JobInfo, selectors []*protos.JobIdSelector) error {
	returnedJobs := make(map[util.JobIdentifier]bool)
	for _, job := range jobs {
		if err := printJobDetails(job); err != nil {
			return err
		}
		returnedJobs[util.JobIdentifierFromJobInfo(job)] = true
	}
	return checkMissingJobs(selectors, returnedJobs)
}

func formatSelector(sel *protos.JobIdSelector) string {
	return util.JobIdentifierFromSelector(sel).String()
}

func checkMissingJobs(selectors []*protos.JobIdSelector, returnedJobs map[util.JobIdentifier]bool) error {
	missing := make([]string, 0)
	for _, sel := range selectors {
		id := util.JobIdentifierFromSelector(sel)
		if !returnedJobs[id] {
			missing = append(missing, id.String())
		}
	}
	if len(missing) > 0 {
		fmt.Printf("Job %s is not running.\n", strings.Join(missing, ", "))
	}
	return nil
}

func printJobDetails(job *protos.JobInfo) error {
	// id/name
	fmt.Printf("JobId=%v JobName=%v\n", util.FormatJobId(job.JobId, job.ArrayTask), job.Name)
	// user / group
	userInfo, err := getUserGroupInfo(job)
	if err != nil {
		return err
	}
	fmt.Printf("\tUser=%s(%d) GroupId=%s(%d) Account=%v\n", userInfo.username, job.Uid, userInfo.groupname, job.Gid, job.Account)

	// time
	timeInfo := formatJobTimes(job)
	fmt.Printf("\tJobState=%v RunTime=%v TimeLimit=%s SubmitTime=%v\n"+
		"\tStartTime=%v EndTime=%v Deadline=%v Partition=%v NodeList=%v ExecutionHost=%v\n",
		job.Status.String(), timeInfo.runTime, timeInfo.timeLimit, timeInfo.submitTime,
		timeInfo.startTime, timeInfo.endTime, timeInfo.deadline, job.Partition,
		formatHostNameStr(job.GetCranedList()),
		formatHostNameStr(util.HostNameListToStr(job.GetExecutionNode())))

	// cmd / work
	fmt.Printf("\tCmdLine=\"%v\" Workdir=%v\n",
		util.PrivateJobField(job.CmdLine, job.QueryVisibility),
		util.PrivateJobField(job.Cwd, job.QueryVisibility))
	// resource
	printResourceRequests(job)
	fmt.Printf("\tReqNodeList=%v ExcludeNodeList=%v\n"+
		"\tExclusive=%v Comment=%v Wckey=%v\n",
		formatHostNameStr(util.HostNameListToStr(job.GetReqNodes())),
		formatHostNameStr(util.HostNameListToStr(job.GetExcludeNodes())),
		strconv.FormatBool(job.Exclusive), util.PrivateJobField(getJobExtraAttr(job.ExtraAttr, "comment"), job.QueryVisibility),
		job.Wckey)

	//  mail
	if job.QueryVisibility == protos.QueryVisibility_QUERY_VISIBILITY_DETAILS {
		printMailNotification(job.ExtraAttr)
	}

	// dependency
	if job.DependencyStatus != nil {
		depStatusStr := formatDependencyStatus(job.DependencyStatus)
		if depStatusStr != "" {
			fmt.Printf("\tDependency=%s\n", depStatusStr)
		}
	}
	fmt.Printf("\tSubmitNode=%v\n", job.SubmitHostname)

	return nil
}

type userGroupInfo struct {
	username  string
	groupname string
}

func getUserGroupInfo(job *protos.JobInfo) (userGroupInfo, error) {
	craneUser, err := user.LookupId(strconv.Itoa(int(job.Uid)))
	if err != nil {
		return userGroupInfo{}, util.NewCraneErr(util.ErrorGeneric,
			fmt.Sprintf("Failed to get username for UID %d: %s", job.Uid, err))
	}

	group, err := user.LookupGroupId(strconv.Itoa(int(job.Gid)))
	if err != nil {
		return userGroupInfo{}, util.NewCraneErr(util.ErrorGeneric,
			fmt.Sprintf("Failed to get groupname for GID %d: %s", job.Gid, err))
	}

	return userGroupInfo{
		username:  craneUser.Username,
		groupname: group.Name,
	}, nil
}

type jobTimeInfo struct {
	submitTime string
	startTime  string
	endTime    string
	deadline   string
	runTime    string
	timeLimit  string
}

func GetElapsedTime(job *protos.JobInfo) string {
	if job.Status == protos.JobStatus_Running && job.ElapsedTime != nil {
		return util.SecondTimeFormat(job.ElapsedTime.Seconds)
	}
	return "unknown"
}

func formatJobTimes(job *protos.JobInfo) jobTimeInfo {
	// formatTime for submit and start
	formatTime := func(t time.Time) string {
		if t.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
			return "unknown"
		}
		return t.In(time.Local).Format("2006-01-02 15:04:05")
	}
	// submit time
	submitTime := formatTime(job.SubmitTime.AsTime())
	// start time
	startTime := formatTime(job.StartTime.AsTime())
	// end time
	endTime := "unknown"
	timeEnd := job.EndTime.AsTime()
	if !timeEnd.Before(job.StartTime.AsTime()) && timeEnd.Unix() < util.MaxJobTimeStamp {
		endTime = timeEnd.In(time.Local).Format("2006-01-02 15:04:05")
	}
	// deadline
	deadline := "unknown"
	if job.DeadlineTime != nil {
		deadlineTime := job.DeadlineTime.AsTime()
		if !deadlineTime.Equal(util.InfiniteFuture) {
			deadline = deadlineTime.In(time.Local).Format("2006-01-02 15:04:05")
		}
	}
	// time limit
	timeLimitStr := "unlimited"
	if job.TimeLimit.Seconds < util.MaxJobTimeLimit {
		timeLimitStr = util.SecondTimeFormat(job.TimeLimit.Seconds)
	}
	// runTime
	// runTimeStr := "unknown"
	runTimeStr := GetElapsedTime(job)

	return jobTimeInfo{
		submitTime: submitTime,
		startTime:  startTime,
		endTime:    endTime,
		deadline:   deadline,
		runTime:    runTimeStr,
		timeLimit:  timeLimitStr,
	}
}

func printResourceRequests(job *protos.JobInfo) {
	totalCpu := job.GetReqTotalResView().GetCpuCount()
	totalMem := job.GetReqTotalResView().GetMemoryBytes()

	cpusPerTask := "Unknown"
	var memPerNode uint64

	if job.GetNtasks() > 0 {
		cpusPerTask = fmt.Sprint(totalCpu / float64(job.GetNtasks()))
	}
	if job.NodeNum > 0 {
		memPerNode = totalMem / uint64(job.NodeNum)
	}

	// Priority / QoS
	fmt.Printf("\tPriority=%v Qos=%v CpusPerTask=%v MemPerNode=%v\n",
		job.Priority, job.Qos,
		cpusPerTask,
		util.FormatMemToMB(memPerNode))
	requestedGres := "Unknown"
	if job.GetReqTotalResView().GetGresMap() != nil {
		requestedGres = formatGresMap(job.GetReqTotalResView().GetGresMap())
	}
	// ReqRes
	fmt.Printf("\tReqRes:node=%d cpu=%.2f mem=%v gres=%s\n",
		job.NodeNum,
		totalCpu,
		util.FormatMemToMB(totalMem),
		requestedGres)
	// AllocRes
	if job.Status == protos.JobStatus_Running {
		fmt.Printf("\tAllocRes:node=%d cpu=%.2f mem=%v gres=%s\n",
			job.NodeNum,
			job.GetAllocatedResView().GetCpuCount(),
			util.FormatMemToMB(job.GetAllocatedResView().GetMemoryBytes()),
			formatGresMap(job.GetAllocatedResView().GetGresMap()))
	}
}

func getJobExtraAttr(extraAttr, key string) string {
	if !gjson.Valid(extraAttr) {
		return ""
	}
	return gjson.Get(extraAttr, key).String()
}

func printMailNotification(extraAttr string) {
	mailUser := getJobExtraAttr(extraAttr, "mail.user")
	mailType := getJobExtraAttr(extraAttr, "mail.type")

	if mailUser != "" || mailType != "" {
		fmt.Printf("\tMailUser=%v MailType=%v\n", mailUser, mailType)
	}
}

func formatDependencyType(depType protos.DependencyType) string {
	switch depType {
	case protos.DependencyType_AFTER:
		return "AFTER"
	case protos.DependencyType_AFTER_ANY:
		return "AFTERANY"
	case protos.DependencyType_AFTER_OK:
		return "AFTEROK"
	case protos.DependencyType_AFTER_NOT_OK:
		return "AFTERNOTOK"
	default:
		return "UNKNOWN"
	}
}

func formatDependencyCondition(cond *protos.DependencyCondition) string {
	if cond == nil {
		return ""
	}
	depTypeStr := formatDependencyType(cond.Type)
	if cond.DelaySeconds > 0 {
		delayStr := util.SecondTimeFormat(int64(cond.DelaySeconds))
		return fmt.Sprintf("%s:%d+%s", depTypeStr, cond.JobId, delayStr)
	}
	return fmt.Sprintf("%s:%d", depTypeStr, cond.JobId)
}

func formatDependencyStatus(depStatus *protos.DependencyStatus) string {
	var buf strings.Builder

	sep := ","
	if depStatus.IsOr {
		sep = "?"
	}

	if len(depStatus.Pending) > 0 {
		var pendingStrs []string
		for _, cond := range depStatus.Pending {
			pendingStrs = append(pendingStrs, formatDependencyCondition(cond))
		}
		buf.WriteString(fmt.Sprintf("PendingDependencies=%s ", strings.Join(pendingStrs, sep)))
	}

	switch depStatus.GetReadyTimeType().(type) {
	case *protos.DependencyStatus_ReadyTime:
		readyTime := depStatus.GetReadyTime().AsTime()
		if depStatus.IsOr || len(depStatus.Pending) == 0 {
			buf.WriteString(fmt.Sprintf("Status=ReadyAfter %s", readyTime.In(time.Local).Format("2006-01-02 15:04:05")))
		} else {
			buf.WriteString("Status=WaitForAll")
		}
	case *protos.DependencyStatus_InfiniteFuture:
		if depStatus.GetInfiniteFuture() {
			if depStatus.IsOr {
				if len(depStatus.Pending) == 0 {
					buf.WriteString("Status=AllFailed")
				} else {
					buf.WriteString("Status=WaitForAny")
				}
			} else {
				buf.WriteString("Status=SomeFailed")
			}
		}
	case *protos.DependencyStatus_InfinitePast:
		// must be AND
		if depStatus.GetInfinitePast() {
			buf.WriteString("Status=WaitForAll")
		}
	}
	return buf.String()
}

func formatHostNameStr(hosts string) string {
	if hosts == "" {
		return "None"
	}
	return hosts
}

func ShowJobs(jobIds string, queryAll bool) error {
	selectors, err := parseJobIdSelectors(jobIds, queryAll)
	if err != nil {
		return err
	}
	reply, err := getJobInfoReply(selectors)
	if err != nil {
		return err
	}
	if FlagJson {
		if util.IsSlurmOutputMode() {
			output, err := util.FormatSlurmJobsJSON(reply)
			if err != nil {
				return util.WrapCraneErr(util.ErrorInvalidFormat, "%v", err)
			}
			fmt.Println(output)
		} else {
			fmt.Println(util.FmtJson.FormatReply(reply))
		}
		return nil
	}
	if len(reply.JobInfoList) == 0 {
		return handleEmptyJobResult(selectors, queryAll)
	}
	return outputJobs(reply.JobInfoList, selectors)
}

func ShowSteps(stepIds string, queryAll bool) error {
	var req *protos.QueryJobsInfoRequest
	var err error

	selectors, err := parseStepIdSelectors(stepIds, queryAll)
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid step list specified: %s.", err))
	}

	req = &protos.QueryJobsInfoRequest{
		FilterJobIds: selectors,
	}
	reply, err := util.QueryJobsInfo(context.Background(), stub, req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to show steps")
	}

	if !reply.GetOk() {
		return util.NewCraneErr(util.ErrorBackend,
			fmt.Sprintf("Failed to retrive the information of step %s", stepIds),
		)
	}

	if FlagJson {
		if util.IsSlurmOutputMode() {
			output, err := util.FormatSlurmJobsJSON(reply)
			if err != nil {
				return util.WrapCraneErr(util.ErrorInvalidFormat, "%v", err)
			}
			fmt.Println(output)
		} else {
			fmt.Println(util.FmtJson.FormatReply(reply))
		}
		return nil
	}

	requestedSteps := requestedStepIdentifiers(selectors)
	if len(reply.JobInfoList) == 0 {
		if queryAll {
			fmt.Println("No step is running.")
		} else if len(requestedSteps) > 0 {
			printMissingSteps(requestedSteps, nil)
		} else {
			fmt.Printf("Step %s is not running.\n", stepIds)
		}
		return nil
	}

	// Helper function to format null/empty strings
	formatNullStr := func(s string) string {
		if len(s) == 0 {
			return "(null)"
		}
		return s
	}

	printedSteps := 0
	printed := make(map[util.StepIdentifier]bool)
	for _, jobInfo := range reply.JobInfoList {
		// Iterate through all steps in this job
		for _, stepInfo := range jobInfo.StepInfoList {
			printedSteps++
			printed[util.StepIdentifierFromStepInfo(jobInfo, stepInfo)] = true
			stepId := stepInfo.StepId

			var timeStartStr string
			timeStart := stepInfo.StartTime.AsTime()
			if !timeStart.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
				timeStartStr = timeStart.In(time.Local).Format("2006-01-02T15:04:05")
			} else {
				timeStartStr = "Unknown"
			}

			var timeLimitStr string
			if stepInfo.TimeLimit.Seconds >= util.MaxJobTimeLimit {
				timeLimitStr = "UNLIMITED"
			} else {
				timeLimitStr = util.SecondTimeFormat(stepInfo.TimeLimit.Seconds)
			}

			stateStr := strings.ToUpper(stepInfo.Status.String())

			partitionStr := formatNullStr(jobInfo.Partition)

			nodeListStr := formatNullStr(stepInfo.GetCranedList())

			var cpusStr string
			if stepInfo.AllocatedResView != nil {
				cpus := stepInfo.GetAllocatedResView().GetCpuCount()
				cpusStr = strconv.FormatInt(int64(cpus), 10)
			} else {
				cpusStr = "0"
			}

			taskStr := fmt.Sprintf("1")

			nameStr := formatNullStr(stepInfo.Name)

			var gresStr string
			if stepInfo.AllocatedResView != nil {
				resView := stepInfo.GetAllocatedResView()
				var gresParts []string
				if resView.GetCpuCount() > 0 {
					gresParts = append(gresParts, fmt.Sprintf("cpu=%d", int64(resView.GetCpuCount())))
				}
				if resView.GetMemoryBytes() > 0 {
					gresParts = append(gresParts, fmt.Sprintf("mem=%s", util.FormatMemToMB(resView.GetMemoryBytes())))
				}
				if stepInfo.NodeNum > 0 {
					gresParts = append(gresParts, fmt.Sprintf("node=%d", stepInfo.NodeNum))
				}
				if len(gresParts) > 0 {
					gresStr = strings.Join(gresParts, ",")
				} else {
					gresStr = "(null)"
				}
			} else {
				gresStr = "(null)"
			}

			fullStepId := util.FormatStepId(jobInfo.JobId, jobInfo.ArrayTask, stepId)

			fmt.Printf("StepId=%v UserId=%v StartTime=%v TimeLimit=%v\n",
				fullStepId, stepInfo.Uid, timeStartStr, timeLimitStr)

			fmt.Printf("   State=%v Partition=%v NodeList=%v\n",
				stateStr, partitionStr, nodeListStr)

			fmt.Printf("   Nodes=%v CPUs=%v Tasks=%v Name=%v\n",
				stepInfo.NodeNum, cpusStr, taskStr, nameStr)

			if gresStr != "(null)" {
				fmt.Printf("   GRES=%v\n", gresStr)
			}

			fmt.Println()
		}
	}
	if !queryAll {
		if len(requestedSteps) > 0 {
			printMissingSteps(requestedSteps, printed)
		} else if printedSteps == 0 {
			fmt.Printf("Step %s is not running.\n", stepIds)
		}
	}

	return nil
}

func requestedStepIdentifiers(selectors []*protos.JobIdSelector) []util.StepIdentifier {
	requestedSteps := make([]util.StepIdentifier, 0)
	for _, selector := range selectors {
		for _, stepId := range selector.Steps {
			requestedSteps = append(requestedSteps, util.StepIdentifierFromSelector(selector, stepId))
		}
	}
	return requestedSteps
}

func printMissingSteps(requestedSteps []util.StepIdentifier, printed map[util.StepIdentifier]bool) {
	missing := make([]string, 0)
	for _, step := range requestedSteps {
		if printed == nil || !printed[step] {
			missing = append(missing, step.String())
		}
	}
	if len(missing) > 0 {
		fmt.Printf("Step %s is not running.\n", strings.Join(missing, ", "))
	}
}

func ShowLicenses(licenseName string, queryAll bool) error {
	var licenseNameList []string
	if licenseName != "" {
		licenseNameList = strings.Split(licenseName, ",")
	}

	req := &protos.QueryLicensesInfoRequest{LicenseNameList: licenseNameList}
	reply, err := stub.QueryLicensesInfo(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to show license")
	}

	if !reply.Ok {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to retrieve information for lic %v", licenseName))
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		return nil
	}

	if len(reply.LicenseInfoList) == 0 {
		if queryAll {
			fmt.Println("No license is available.")
		} else {
			fmt.Printf("license %s not found.\n", licenseName)
		}
		return nil
	}

	for _, licenseInfo := range reply.LicenseInfoList {
		if !licenseInfo.Remote {
			fmt.Printf("LicenseName=%v \n"+
				"\tTotal=%v Used=%d Free=%d Reserved=%d Remote=no\n",
				licenseInfo.Name, licenseInfo.Total, licenseInfo.Used, licenseInfo.Free, licenseInfo.Reserved)
		} else {
			timeStr := "unknown"
			if licenseInfo.LastUpdate > 0 {
				timeStr = time.Unix(licenseInfo.LastUpdate, 0).Format("2006-01-02 15:04:05")
			}

			fmt.Printf("LicenseName=%v \n"+
				"\tTotal=%v Used=%d Free=%d Reserved=%d Remote=yes\n"+
				"\tLastConsumed=%d LastDeficit=%d LastUpdated=%s\n",
				licenseInfo.Name, licenseInfo.Total, licenseInfo.Used, licenseInfo.Free, licenseInfo.Reserved,
				licenseInfo.LastConsumed, licenseInfo.LastDeficit, timeStr)
		}
	}

	return nil
}
