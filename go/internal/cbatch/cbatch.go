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

package cbatch

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CbatchArg struct {
	name string
	val  string
}

var (
	setGresGpusFlag    = false
	setGpusPerNodeFlag = false
)

// BuildCbatchJob reads flags and script file to build a job
func BuildCbatchJob(cmd *cobra.Command, args []string, config *util.Config) (*protos.JobToCtld, error) {
	job := new(protos.JobToCtld)
	job.Uid = uint32(os.Getuid())
	gids, err := util.CollectEffectiveGroups()
	if err != nil {
		return nil, err
	}
	job.Gids = gids
	warnUnsupportedCLIFlags(cmd)

	// Parse the script file or use wrapped script
	cbatchArgs := make([]CbatchArg, 0)
	shScript := ""

	if FlagWrappedScript == "" {
		shLines := make([]string, 0)
		if err := ParseCbatchScript(args[0], &cbatchArgs, &shLines); err != nil {
			return nil, fmt.Errorf("invalid argument: failed to parse script: %w", err)
		}
		job.Name = filepath.Base(args[0])
		shScript = strings.Join(shLines, "\n")
	} else {
		job.Name = util.DefaultWrappedJobName
		shScript = FlagWrappedScript
	}

	if isPod, err := isPodJob(cmd, cbatchArgs); err != nil {
		return nil, err
	} else if isPod {
		job.Type = protos.JobType_Container
	} else {
		job.Type = protos.JobType_Batch
	}

	// Set the payload
	job.Payload = &protos.JobToCtld_BatchMeta{
		BatchMeta: &protos.BatchJobAdditionalMeta{},
	}
	job.IoMeta = &protos.IoMeta{}
	job.ShScript = shScript

	// Set default values
	job.NtasksPerNode = 0
	job.NodeNumMin = 0
	job.NodeNumMax = 0
	job.Ntasks = 0
	job.GetUserEnv = false
	job.Env = make(map[string]string)
	job.TimeLimit = util.InvalidDuration()

	structExtraFromScript := util.JobExtraAttrs{}
	structExtraFromCli := util.JobExtraAttrs{}
	podOpts := podOptions{userns: config.Container.UserNsEnabledByDefault}

	// Set args from the script
	if err := applyScriptArgs(cmd, cbatchArgs, job, &structExtraFromScript, &podOpts); err != nil {
		return nil, err
	}

	// CLI overrides are now known, so finalize and validate the container
	// identity before processing the remaining submission options.
	if job.Type == protos.JobType_Container {
		overridePodFromFlags(cmd, &podOpts)
		podMeta, err := buildPodMeta(job, &podOpts)
		if err != nil {
			return nil, fmt.Errorf("invalid container options: %v", err)
		}
		job.PodMeta = podMeta
	}

	var extraFromScript string
	if err := structExtraFromScript.Marshal(&extraFromScript); err != nil {
		return nil, fmt.Errorf("invalid argument: failed to marshal extra attributes from script: %w", err)
	}

	// Set args from the command line flags
	// Command line has a higher priority
	if cmd.Flags().Changed("nodes") {
		if FlagNodes == 0 {
			return nil, fmt.Errorf("invalid argument: --nodes must be > 0")
		}
		job.NodeNumMin = FlagNodes
		job.NodeNumMax = FlagNodes
	}
	if cmd.Flags().Changed("cpus-per-task") {
		cpuPerTask := float64(FlagCpuPerTask)
		job.CpusPerTask = &cpuPerTask
	}

	if cmd.Flags().Changed("ntasks-per-node") {
		if FlagNtasksPerNode == 0 {
			return nil, fmt.Errorf("invalid argument: --ntasks-per-node must be > 0")
		}
		job.NtasksPerNode = FlagNtasksPerNode
	}
	if cmd.Flags().Changed("ntasks") {
		if FlagNtasks == 0 {
			return nil, fmt.Errorf("invalid argument: --ntasks must be > 0")
		}
		job.Ntasks = FlagNtasks
	}
	if cmd.Flags().Changed("array") {
		arraySpec, err := util.ParseArrayRangeSpec(FlagArray)
		if err != nil {
			return nil, err
		}
		job.ArraySpec = arraySpec
	}
	if cmd.Flags().Changed("gres") {
		gresMap, err := util.ParseGres(FlagGres)
		if err != nil {
			return nil, err
		}
		if _, exist := gresMap.NameGresMap[util.GresGpuName]; exist {
			if setGpusPerNodeFlag {
				return nil, fmt.Errorf("invalid argument: cannot specify both --gres gpus and --gpus-per-node flags simultaneously")
			}
			setGresGpusFlag = true
		}
		job.GresPerNode = gresMap
	}
	if cmd.Flags().Changed("gpus-per-node") {
		if setGresGpusFlag {
			return nil, fmt.Errorf("invalid argument: cannot specify both --gres gpus and --gpus-per-node flags simultaneously")
		}
		setGpusPerNodeFlag = true
		gpuDeviceMap, err := util.ParseGpusPerNodeStr(FlagGpusPerNode)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: --gpus-per-node value '%s'", FlagGpusPerNode)
		}
		job.GresPerNode = gpuDeviceMap
	}
	if cmd.Flags().Changed("wckey") {
		job.Wckey = &FlagWckey
	}

	if cmd.Flags().Changed("time") {
		seconds, err := util.ParseDurationStrToSeconds(FlagTime)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: --time value '%s'", FlagTime)
		}
		job.TimeLimit.Seconds = seconds
	}
	if cmd.Flags().Changed("mem") {
		memInByte, err := util.ParseMemStringAsByte(FlagMem)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: --mem value '%s'", FlagMem)
		}
		job.MemPerNode = &memInByte
	}
	if cmd.Flags().Changed("mem-per-cpu") {
		memInBytePerCpu, err := util.ParseMemStringAsByte(FlagMemPerCpu)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: --mem-per-cpu value '%s'", FlagMemPerCpu)
		}
		job.MemPerCpu = &memInBytePerCpu
	}
	if cmd.Flags().Changed("partition") {
		job.PartitionName = FlagPartition
	}
	if cmd.Flags().Changed("job-name") {
		job.Name = FlagJob
	}
	if job.PodMeta != nil && podOpts.name == "" {
		job.PodMeta.Name = job.Name
	}
	if cmd.Flags().Changed("qos") {
		job.Qos = FlagQos
	}
	if cmd.Flags().Changed("licenses") {
		licCount, isLicenseOr, err := util.ParseLicensesString(FlagLicenses)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: --licenses value '%s'", FlagLicenses)
		}
		job.LicensesCount = licCount
		job.IsLicensesOr = isLicenseOr
	}
	if cmd.Flags().Changed("chdir") {
		job.Cwd = FlagCwd
	}
	if cmd.Flags().Changed("account") {
		job.Account = FlagAccount
	}
	if cmd.Flags().Changed("nodelist") {
		job.Nodelist = FlagNodelist
	}
	if cmd.Flags().Changed("exclude") {
		job.Excludes = FlagExcludes
	}
	if cmd.Flags().Changed("get-user-env") {
		job.GetUserEnv = FlagGetUserEnv
	}
	if cmd.Flags().Changed("export") {
		job.Env["CRANE_EXPORT_ENV"] = FlagExport
	}
	if cmd.Flags().Changed("input") {
		job.GetIoMeta().InputFilePattern = FlagStdinPath
	}
	if cmd.Flags().Changed("output") {
		job.GetIoMeta().OutputFilePattern = FlagStdoutPath
	}
	if cmd.Flags().Changed("error") {
		job.GetIoMeta().ErrorFilePattern = FlagStderrPath
	}
	if cmd.Flags().Changed("interpreter") {
		job.GetBatchMeta().Interpreter = FlagInterpreter
	}

	if FlagExtraAttr != "" {
		structExtraFromCli.ExtraAttr = FlagExtraAttr
	}
	if FlagMailType != "" {
		structExtraFromCli.MailType = FlagMailType
	}
	if FlagMailUser != "" {
		structExtraFromCli.MailUser = FlagMailUser
	}
	if FlagComment != "" {
		structExtraFromCli.Comment = FlagComment
	}
	if cmd.Flags().Changed("open-mode") {
		switch FlagOpenMode {
		case util.OpenModeAppend:
			job.GetIoMeta().OpenModeAppend = proto.Bool(true)
		case util.OpenModeTruncate:
			job.GetIoMeta().OpenModeAppend = proto.Bool(false)
		default:
			return nil, fmt.Errorf("invalid argument: --open-mode must be either '%s' or '%s'", util.OpenModeAppend, util.OpenModeTruncate)
		}
	}
	if cmd.Flags().Changed("begin") {
		beginTime, err := util.ParseTime(FlagBeginTime)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: --begin value '%s'", FlagBeginTime)
		}
		job.BeginTime = timestamppb.New(beginTime)
	}
	if cmd.Flags().Changed("deadline") {
		deadlineTime, err := util.ParseTime(FlagDeadlineTime)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: --deadline value '%s'", FlagDeadlineTime)
		}
		job.DeadlineTime = timestamppb.New(deadlineTime)
	}
	if cmd.Flags().Changed("exclusive") {
		job.Exclusive = FlagExclusive
	}
	if cmd.Flags().Changed("hold") {
		job.Hold = FlagHold
	}
	if cmd.Flags().Changed("no-requeue") || cmd.Flags().Changed("requeue") {
		job.NoRequeue = FlagNoRequeue
		job.RequeueIfFailed = FlagRequeue
	}
	if cmd.Flags().Changed("reservation") {
		job.Reservation = FlagReservation
	}

	if cmd.Flags().Changed("signal") {
		signals, err := util.ParseSignalParamString(FlagSignal)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: --signal value '%s'", FlagSignal)
		}
		for _, signal := range signals {
			job.Signals = append(job.Signals, signal)
		}
	}

	// Set and check the extra attributes
	var extraFromCli string
	if err := structExtraFromCli.Marshal(&extraFromCli); err != nil {
		return nil, fmt.Errorf("invalid argument: failed to marshal extra attributes from CLI: %w", err)
	}
	job.ExtraAttr = util.AmendJobExtraAttrs(extraFromScript, extraFromCli)

	// Set the submit hostname
	submitHostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("Failed to get hostname of the submitting host: %v", err)
	}
	job.SubmitHostname = submitHostname
	if cmd.Flags().Changed("dependency") {
		err := util.SetJobDependencies(job, FlagDependency)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: failed to set dependencies: %w", err)
		}
	}

	// Check the validity of the parameters
	if err := util.CheckFileLength(job.GetIoMeta().InputFilePattern); err != nil {
		return nil, fmt.Errorf("invalid argument: input file path is invalid: %w", err)
	}
	if err := util.CheckFileLength(job.GetIoMeta().OutputFilePattern); err != nil {
		return nil, fmt.Errorf("invalid argument: output file path is invalid: %w", err)
	}
	if err := util.CheckFileLength(job.GetIoMeta().ErrorFilePattern); err != nil {
		return nil, fmt.Errorf("invalid argument: error file path is invalid: %w", err)
	}
	if err := util.CheckJobArgs(job); err != nil {
		return nil, fmt.Errorf("invalid argument: %w", err)
	}

	return job, nil
}

func applyScriptArgs(cmd *cobra.Command, cbatchArgs []CbatchArg, job *protos.JobToCtld, extraFromScript *util.JobExtraAttrs, podOpts *podOptions) error {
	for _, arg := range cbatchArgs {
		switch arg.name {
		case "--parsable":
			FlagParsable = true
		case "--nodes", "-N":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
			}
			if num == 0 {
				return fmt.Errorf("invalid argument: %s must be > 0 in script", arg.name)
			}
			job.NodeNumMin = uint32(num)
			job.NodeNumMax = uint32(num)
		case "--cpus-per-task", "-c":
			num, err := util.ParseFloatWithPrecision(arg.val, 10)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
			}
			job.CpusPerTask = &num
		case "--gres":
			gresMap, err := util.ParseGres(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
			}
			if _, exist := gresMap.NameGresMap[util.GresGpuName]; exist {
				if setGpusPerNodeFlag {
					return fmt.Errorf("invalid argument: cannot specify both --gres gpus and --gpus-per-node flags simultaneously")
				}
				setGresGpusFlag = true
			}
			job.GresPerNode = gresMap
		case "--gpus-per-node":
			if setGresGpusFlag {
				return fmt.Errorf("invalid argument: cannot specify both --gres gpus and --gpus-per-node flags simultaneously")
			}
			setGpusPerNodeFlag = true
			gpuDeviceMap, err := util.ParseGpusPerNodeStr(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
			}
			job.GresPerNode = gpuDeviceMap
		case "--ntasks-per-node":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
			}
			if num == 0 {
				return fmt.Errorf("invalid argument: %s must be > 0 in script", arg.name)
			}
			job.NtasksPerNode = uint32(num)
		case "--ntasks", "-n":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
			}
			if num == 0 {
				return fmt.Errorf("invalid argument: %s must be > 0 in script", arg.name)
			}
			job.Ntasks = uint32(num)
		case "--array", "-a":
			arraySpec, err := util.ParseArrayRangeSpec(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
			}
			job.ArraySpec = arraySpec
		case "--time", "-t":
			seconds, err := util.ParseDurationStrToSeconds(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
			}
			job.TimeLimit.Seconds = seconds
		case "--begin", "-b":
			beginTime, err := util.ParseTime(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
			}
			job.BeginTime = timestamppb.New(beginTime)
		case "--mem":
			memInByte, err := util.ParseMemStringAsByte(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
			}
			job.MemPerNode = &memInByte
		case "--mem-per-cpu":
			memInBytePerCpu, err := util.ParseMemStringAsByte(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
			}
			job.MemPerCpu = &memInBytePerCpu
		case "-p", "--partition":
			job.PartitionName = arg.val
		case "-J", "--job-name":
			job.Name = arg.val
		case "-A", "--account":
			job.Account = arg.val
		case "--qos", "-q":
			job.Qos = arg.val
		case "--licenses", "-L":
			licCount, isLicenseOr, err := util.ParseLicensesString(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
			}
			job.LicensesCount = licCount
			job.IsLicensesOr = isLicenseOr
		case "-D", "--chdir":
			job.Cwd = arg.val
		case "--exclude", "-x":
			job.Excludes = arg.val
		case "--nodelist", "-w":
			job.Nodelist = arg.val
		case "--get-user-env":
			val, err := parseScriptBool(arg)
			if err != nil {
				return err
			}
			job.GetUserEnv = val
		case "--export":
			job.Env["CRANE_EXPORT_ENV"] = arg.val
		case "-i", "--input":
			job.GetIoMeta().InputFilePattern = arg.val
		case "-o", "--output":
			job.GetIoMeta().OutputFilePattern = arg.val
		case "-e", "--error":
			job.GetIoMeta().ErrorFilePattern = arg.val
		case "--interpreter":
			job.GetBatchMeta().Interpreter = arg.val
		case "--extra-attr":
			extraFromScript.ExtraAttr = arg.val
		case "--mail-type":
			extraFromScript.MailType = arg.val
		case "--mail-user":
			extraFromScript.MailUser = arg.val
		case "--comment":
			extraFromScript.Comment = arg.val
		case "--open-mode":
			switch arg.val {
			case util.OpenModeAppend:
				job.GetIoMeta().OpenModeAppend = proto.Bool(true)
			case util.OpenModeTruncate:
				job.GetIoMeta().OpenModeAppend = proto.Bool(false)
			default:
				return fmt.Errorf("invalid argument: --open-mode must be either '%s' or '%s'", util.OpenModeAppend, util.OpenModeTruncate)
			}
		case "--wrap":
			job.ShScript = arg.val
		case "-r", "--reservation":
			job.Reservation = arg.val
		case "--exclusive":
			val, err := parseScriptBool(arg)
			if err != nil {
				return err
			}
			job.Exclusive = val
		case "-H", "--hold":
			val, err := parseScriptBool(arg)
			if err != nil {
				return err
			}
			job.Hold = val
		case "--wckey":
			wckey := arg.val
			job.Wckey = &wckey
		case "--pod":
			// No need to process here, already handled in isPodJob
		case "--pod-name":
			podOpts.name = arg.val
		case "--pod-port":
			podOpts.ports = append(podOpts.ports, arg.val)
		case "--pod-user":
			podOpts.user = arg.val
		case "--pod-userns":
			val, err := strconv.ParseBool(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
			}
			podOpts.userns = val
		case "--pod-host-network":
			val, err := strconv.ParseBool(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
			}
			podOpts.hostNet = val
		case "--pod-dns":
			servers := strings.Split(arg.val, ",")
			for i, s := range servers {
				servers[i] = strings.TrimSpace(s)
				if err := util.CheckIpv4Format(servers[i]); err != nil {
					return fmt.Errorf("invalid argument: %s dns server '%s' in script: %w", arg.name, servers[i], err)
				}
			}
			podOpts.dns = append(podOpts.dns, servers...)
		case "--dependency", "-d":
			err := util.SetJobDependencies(job, arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: failed to set dependencies: %w", err)
			}
		case "-s", "--signal":
			signals, err := util.ParseSignalParamString(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
			}
			for _, signal := range signals {
				job.Signals = append(job.Signals, signal)
			}
		case "--deadline":
			deadlineTime, err := util.ParseTime(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
			}
			job.DeadlineTime = timestamppb.New(deadlineTime)
		case "--requeue":
			val, err := parseScriptBool(arg)
			if err != nil {
				return err
			}
			job.NoRequeue = false
			job.RequeueIfFailed = val
		case "--no-requeue":
			val, err := parseScriptBool(arg)
			if err != nil {
				return err
			}
			job.NoRequeue = val
			job.RequeueIfFailed = false
		default:
			return fmt.Errorf("invalid argument: unrecognized '%s' in script", arg.name)
		}
	}

	return nil
}

func parseScriptBool(arg CbatchArg) (bool, error) {
	if arg.val == "" {
		return true, nil
	}
	val, err := strconv.ParseBool(arg.val)
	if err != nil {
		return false, fmt.Errorf("invalid argument: %s value '%s' in script", arg.name, arg.val)
	}
	return val, nil
}

func SendRequest(config *util.Config, job *protos.JobToCtld) error {
	stub := util.GetStubToCtldByConfig(config)
	req := &protos.SubmitBatchJobRequest{Job: job}

	reply, err := stub.SubmitBatchJob(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to submit the job")
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
		fmt.Print(formatJobSubmissionOutput([]uint32{reply.GetJobId()}))
		return nil
	} else {
		if len(reply.GetReason()) > 0 {
			return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Job allocation failed: %s.", reply.GetReason()))
		}

		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Job allocation failed: %s.", util.ErrMsg(reply.GetCode())))
	}
}

func SendMultipleRequests(config *util.Config, job *protos.JobToCtld, count uint32) error {
	stub := util.GetStubToCtldByConfig(config)
	req := &protos.SubmitBatchJobsRequest{Job: job, Count: count}

	reply, err := stub.SubmitBatchJobs(context.Background(), req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to submit jobs")
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.GetCodeList()) > 0 {
			return &util.CraneError{Code: util.ErrorBackend}
		} else {
			return nil
		}
	}

	if len(reply.JobIdList) > 0 {
		fmt.Print(formatJobSubmissionOutput(reply.JobIdList))
	}

	if len(reply.GetCodeList()) > 0 {
		reasons := make([]string, 0, len(reply.GetCodeList()))
		for _, reason := range reply.GetCodeList() {
			reasons = append(reasons, util.ErrMsg(reason))
		}

		return util.NewCraneErr(util.ErrorBackend,
			fmt.Sprintf("Job allocation failed: %s.", strings.Join(reasons, "; ")))
	}
	return nil
}

// ParseCbatchScript Split the job script into two parts: the arguments and the shell script.
func ParseCbatchScript(path string, args *[]CbatchArg, sh *[]string) error {
	file, err := os.Open(path)
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, err.Error())
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Errorf("Failed to close %s.\n", file.Name())
		}
	}(file)

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	num := 0

	for scanner.Scan() {
		num++

		// Shebang
		if num == 1 && strings.HasPrefix(scanner.Text(), "#!") {
			*args = append(*args, CbatchArg{
				name: "--interpreter",
				val:  strings.TrimPrefix(scanner.Text(), "#!"),
			})
			*sh = append(*sh, scanner.Text())
			continue
		}

		// Arguments
		reC := regexp.MustCompile(`^#CBATCH`)
		reS := regexp.MustCompile(`^#SBATCH`)
		reL := regexp.MustCompile(`^#BSUB`)
		var processor LineProcessor
		if reC.MatchString(scanner.Text()) {
			processor = &cLineProcessor{}
		} else if reS.MatchString(scanner.Text()) {
			processor = &sLineProcessor{}
		} else if reL.MatchString(scanner.Text()) {
			processor = &lLineProcessor{}
		} else {
			processor = &defaultProcessor{}
		}
		err := processor.Process(scanner.Text(), sh, args)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Parsing error at line %d: %s", num, err))
		}

	}

	if err := scanner.Err(); err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Failed to read the script file", err)
	}
	*args = FilterDummyArgs(*args)
	return nil
}

var unsupportedFlags = map[string]string{
	"ntasks-per-socket": "The feature --ntasks-per-socket is not yet supported by Crane, the use is ignored.",
	"cpu-freq":          "The feature --cpu-freq is not yet supported by Crane, the use is ignored.",
	"priority":          "The feature --priority is not yet supported by Crane, the use is ignored.",
	"threads-per-core":  "The feature --threads-per-core is not yet supported by Crane, the use is ignored.",
	"distribution":      "The feature --distribution/-m is not yet supported by Crane, the use is ignored.",
	"m":                 "The feature --distribution/-m is not yet supported by Crane, the use is ignored.",
	"sockets-per-node":  "The feature --sockets-per-node is not yet supported by Crane, the use is ignored.",
	"cores-per-socket":  "The feature --cores-per-socket is not yet supported by Crane, the use is ignored.",
	"wait":              "The feature --wait/-W is not yet supported by Crane, the use is ignored.",
	"W":                 "The feature --wait/-W is not yet supported by Crane, the use is ignored.",
}

func warnUnsupportedCLIFlags(cmd *cobra.Command) {
	for name, message := range unsupportedFlags {
		if cmd.Flags().Changed(name) {
			fmt.Fprintln(os.Stderr, message)
		}
	}
}

func unsupportedFlagMessage(name string) (string, bool) {
	var key string
	switch {
	case len(name) == 2 && name[0] == '-':
		key = name[1:]
	case len(name) > 3 && strings.HasPrefix(name, "--"):
		key = name[2:]
	default:
		return "", false
	}

	message, found := unsupportedFlags[key]
	return message, found
}

func FilterDummyArgs(args []CbatchArg) []CbatchArg {
	filteredArgs := make([]CbatchArg, 0, len(args))

	for _, arg := range args {
		if message, found := unsupportedFlagMessage(arg.name); found {
			fmt.Fprintln(os.Stderr, message)
		} else {
			filteredArgs = append(filteredArgs, arg)
		}
	}

	return filteredArgs
}
