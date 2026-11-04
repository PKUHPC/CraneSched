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
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// parseUserSpec parses user specification in format "uid" or "uid:gid"
func parseUserSpec(userSpec string, podMeta *protos.PodJobAdditionalMeta) error {
	parts := strings.SplitN(userSpec, ":", 2)

	// Parse user (UID only)
	user := parts[0]
	if uid, err := strconv.ParseUint(user, 10, 32); err == nil {
		podMeta.RunAsUser = uint32(uid)
	} else {
		// We currently do not intend to support user name resolution
		return fmt.Errorf("user name resolution not supported, please provide a numeric UID")
	}

	// Parse group if provided
	if len(parts) == 2 {
		group := parts[1]
		if gid, err := strconv.ParseUint(group, 10, 32); err == nil {
			podMeta.RunAsGroup = uint32(gid)
		} else {
			return fmt.Errorf("group name resolution not supported, please provide a numeric GID")
		}
	}

	return nil
}

// parseEnvVar parses environment variable in format "KEY=VALUE" or "KEY"
func parseEnvVar(envVar string, envMap map[string]string) error {
	parts := strings.SplitN(envVar, "=", 2)
	if len(parts) == 1 {
		// KEY format - set empty value
		envMap[parts[0]] = ""
	} else if len(parts) == 2 {
		// KEY=VALUE format
		envMap[parts[0]] = parts[1]
	} else {
		return fmt.Errorf("environment variable must be in KEY=VALUE or KEY format")
	}
	return nil
}

// parsePortMapping parses port mapping in format "host:container" or "port"
func parsePortMapping(portSpec string, portList *[]*protos.PodJobAdditionalMeta_PortMapping) error {
	parts := strings.SplitN(portSpec, ":", 2)

	mapping := &protos.PodJobAdditionalMeta_PortMapping{
		Protocol: protos.PodJobAdditionalMeta_PortMapping_TCP,
	}

	if len(parts) == 1 {
		// Single port, map to same port
		port, err := strconv.ParseInt(parts[0], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid port number: %v", err)
		}
		mapping.HostPort = int32(port)
		mapping.ContainerPort = int32(port)
	} else {
		// Host:container format
		hostPort, err := strconv.ParseInt(parts[0], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid host port: %v", err)
		}
		containerPort, err := strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid container port: %v", err)
		}
		mapping.HostPort = int32(hostPort)
		mapping.ContainerPort = int32(containerPort)
	}

	*portList = append(*portList, mapping)
	return nil
}

// parseVolumeMount parses volume mount in format "host:container" or "volume"
func parseVolumeMount(volumeSpec string, mountMap map[string]string) error {
	parts := strings.SplitN(volumeSpec, ":", 2)

	if len(parts) != 2 {
		return fmt.Errorf("volume mount must be in HOST:CONTAINER format")
	}

	hostPath := parts[0]
	containerPath := parts[1]

	// Basic validation
	if hostPath == "" || containerPath == "" {
		return fmt.Errorf("host path and container path cannot be empty")
	}

	mountMap[hostPath] = containerPath
	return nil
}

// flagChanged checks if a flag is explicitly provided by user on current command or its root.
func flagChanged(cmd *cobra.Command, name string) bool {
	if cmd == nil {
		return false
	}
	if flag := cmd.Flags().Lookup(name); flag != nil && flag.Changed {
		return true
	}
	if flag := cmd.InheritedFlags().Lookup(name); flag != nil && flag.Changed {
		return true
	}
	if flag := cmd.Root().PersistentFlags().Lookup(name); flag != nil && flag.Changed {
		return flag.Changed
	}
	if flag := cmd.Root().Flags().Lookup(name); flag != nil && flag.Changed {
		return flag.Changed
	}
	return false
}

// runExecute handles the run command execution
func runExecute(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		return util.NewCraneErr(util.ErrorCmdArg, "run requires at least one argument: IMAGE [COMMAND] [ARG...]")
	}

	jobId, stepMode, err := util.ParseJobNestedEnv()
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "failed to build container step: %v", err)
	}

	f := GetFlags()
	image := args[0]

	var command []string
	if len(args) > 1 {
		command = args[1:]
	}

	var errSubmit error
	var reply protoreflect.ProtoMessage
	if stepMode {
		// Build the container step
		step, err := buildContainerStep(cmd, f, jobId, image, command)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "failed to build container step: %v", err)
		}

		// Check generic step arguments
		if err := util.CheckStepArgs(step); err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "invalid step arguments: %v", err)
		}

		if err := validateContainerStep(step); err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "validation failed: %v", err)
		}

		reply, errSubmit = submitContainerStep(step)
	} else {
		// Build the container job
		job, err := buildContainerJob(cmd, f, image, command)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "failed to build container job: %v", err)
		}

		// Check generic job arguments
		if err := util.CheckJobArgs(job); err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "invalid job arguments: %v", err)
		}

		if err := validateContainerJob(job); err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "validation failed: %v", err)
		}

		reply, errSubmit = submitContainerJob(job)
	}

	if f.Global.Json {
		outputJson("run", "", f.Run, reply)
		return errSubmit
	}
	if errSubmit != nil {
		return errSubmit
	}

	if err := attachAfterRun(f, reply); err != nil {
		return util.WrapCraneErr(util.ErrorBackend, "Stream connection lost. Please check container status with 'ccon ps' or try reattaching: %v", err)
	}

	return nil
}

// applyResourceOptions applies resource options to the job
func applyResourceOptions(f *Flags, job *protos.JobToCtld) error {
	// Check mutally exclusive flags
	if f.Run.Cpus > 0 && f.Crane.CpusPerTask > 0 {
		return fmt.Errorf("--cpus and --cpus-per-task are mutually exclusive")
	}

	if f.Run.Memory != "" && f.Crane.Mem != "" {
		return fmt.Errorf("--memory and --mem are mutually exclusive")
	}

	if f.Run.Gpus != "" && f.Crane.Gres != "" {
		return fmt.Errorf("--gpus and --gres are mutually exclusive")
	}

	// CPU allocation
	if f.Crane.CpusPerTask > 0 {
		cpuPerTask := float64(f.Crane.CpusPerTask)
		job.CpusPerTask = &cpuPerTask
	} else if f.Run.Cpus > 0 {
		log.Warn("--cpus is deprecated, please use --cpus-per-task instead")
		cpuPerTask := float64(f.Run.Cpus)
		job.CpusPerTask = &cpuPerTask
	} else {
		// Default to 1 CPU if not specified
		cpuPerTask := float64(1)
		job.CpusPerTask = &cpuPerTask
	}

	// Memory allocation - prefer --mem over --memory
	memorySpec := ""
	if f.Crane.Mem != "" {
		memorySpec = f.Crane.Mem
	} else if f.Run.Memory != "" {
		log.Warn("--memory is deprecated, please use --mem instead")
		memorySpec = f.Run.Memory
	}

	if memorySpec != "" {
		memoryBytes, err := util.ParseMemStringAsByte(memorySpec)
		if err != nil {
			return fmt.Errorf("invalid memory specification '%s': %v", memorySpec, err)
		}
		job.MemPerNode = &memoryBytes
	}

	// GPU allocation - prefer --gres over --gpus
	gresSpec := ""
	if f.Crane.Gres != "" {
		gresSpec = f.Crane.Gres
	} else if f.Run.Gpus != "" {
		return fmt.Errorf("--gpus is not supported. Please use --gres instead with format like 'gpu:1' or 'gpu:a100:2'")
	}

	if gresSpec != "" {
		gresMap, err := util.ParseGres(gresSpec)
		if err != nil {
			return fmt.Errorf("invalid gres specification '%s': %v", gresSpec, err)
		}
		job.GresPerNode = gresMap
	}

	return nil
}

// applyStepResourceOptions applies optional resource overrides to a container step.
func applyStepResourceOptions(cmd *cobra.Command, f *Flags, step *protos.StepToCtld) error {
	// CPU allocation
	if flagChanged(cmd, "cpus-per-task") || flagChanged(cmd, "cpus") {
		if f.Run.Cpus > 0 && f.Crane.CpusPerTask > 0 {
			return fmt.Errorf("--cpus and --cpus-per-task are mutually exclusive")
		}
		if f.Crane.CpusPerTask > 0 {
			cpuPerTask := float64(f.Crane.CpusPerTask)
			step.CpusPerTask = &cpuPerTask
		} else if f.Run.Cpus > 0 {
			log.Warn("--cpus is deprecated, please use --cpus-per-task instead")
			cpuPerTask := float64(f.Run.Cpus)
			step.CpusPerTask = &cpuPerTask
		}
	}

	// Memory allocation
	if flagChanged(cmd, "mem") || flagChanged(cmd, "memory") {
		if f.Run.Memory != "" && f.Crane.Mem != "" {
			return fmt.Errorf("--memory and --mem are mutually exclusive")
		}
		memorySpec := f.Crane.Mem
		if memorySpec == "" {
			memorySpec = f.Run.Memory
		}
		if memorySpec != "" {
			memoryBytes, err := util.ParseMemStringAsByte(memorySpec)
			if err != nil {
				return fmt.Errorf("invalid memory specification '%s': %v", memorySpec, err)
			}
			step.MemPerNode = &memoryBytes
		}
	}

	// GPU allocation
	if flagChanged(cmd, "gres") || flagChanged(cmd, "gpus") {
		if f.Run.Gpus != "" && f.Crane.Gres != "" {
			return fmt.Errorf("--gpus and --gres are mutually exclusive")
		}
		gresSpec := f.Crane.Gres
		if gresSpec == "" {
			gresSpec = f.Run.Gpus
		}
		if gresSpec != "" {
			if f.Run.Gpus != "" && f.Crane.Gres == "" {
				return fmt.Errorf("--gpus is not supported. Please use --gres instead with format like 'gpu:1' or 'gpu:a100:2'")
			}
			gresPerNode, err := util.ParseGres(gresSpec)
			if err != nil {
				return fmt.Errorf("invalid gres specification '%s': %v", gresSpec, err)
			}
			step.GresPerNode = gresPerNode

		}
	}

	// Node-related overrides
	if flagChanged(cmd, "nodes") {
		val := f.Crane.Nodes
		step.NodeNum = val
	}

	if flagChanged(cmd, "ntasks-per-node") {
		val := f.Crane.NtasksPerNode
		step.NtasksPerNode = val
	}

	if flagChanged(cmd, "ntasks") {
		val := f.Crane.Ntasks
		step.Ntasks = val
	}

	if flagChanged(cmd, "nodelist") {
		step.Nodelist = f.Crane.Nodelist
	}

	if flagChanged(cmd, "exclude") {
		step.Excludes = f.Crane.Excludes
	}

	// Time limit override
	if flagChanged(cmd, "time") && f.Crane.Time != "" {
		seconds, err := util.ParseDurationStrToSeconds(f.Crane.Time)
		if err != nil {
			return fmt.Errorf("invalid time specification '%s': %v", f.Crane.Time, err)
		}
		step.TimeLimit.Seconds = seconds
	}

	return nil
}

// applySchedulingOptions applies cluster scheduling options to the job
func applySchedulingOptions(cmd *cobra.Command, f *Flags, job *protos.JobToCtld) error {
	// Partition/queue assignment
	if f.Crane.Partition != "" {
		job.PartitionName = f.Crane.Partition
	}

	// Time limit
	if f.Crane.Time != "" {
		seconds, err := util.ParseDurationStrToSeconds(f.Crane.Time)
		if err != nil {
			return fmt.Errorf("invalid time specification '%s': %v", f.Crane.Time, err)
		}
		job.TimeLimit.Seconds = seconds
	}

	// Account
	if f.Crane.Account != "" {
		job.Account = f.Crane.Account
	}

	// QoS
	if f.Crane.Qos != "" {
		job.Qos = f.Crane.Qos
	}

	if flagChanged(cmd, "nodes") {
		job.NodeNumMin = f.Crane.Nodes
		job.NodeNumMax = f.Crane.Nodes
	}
	if flagChanged(cmd, "ntasks-per-node") {
		job.NtasksPerNode = f.Crane.NtasksPerNode
	}
	if flagChanged(cmd, "ntasks") {
		job.Ntasks = f.Crane.Ntasks
	}

	// Node list (specific nodes)
	if f.Crane.Nodelist != "" {
		job.Nodelist = f.Crane.Nodelist
	}

	// Exclude nodes
	if f.Crane.Excludes != "" {
		job.Excludes = f.Crane.Excludes
	}

	// Reservation
	if f.Crane.Reservation != "" {
		job.Reservation = f.Crane.Reservation
	}

	// Extra attributes - handle both individual flags and JSON like cbatch
	structExtraFromCli := util.JobExtraAttrs{}

	if f.Crane.ExtraAttr != "" {
		structExtraFromCli.ExtraAttr = f.Crane.ExtraAttr
	}
	if f.Crane.MailType != "" {
		structExtraFromCli.MailType = f.Crane.MailType
	}
	if f.Crane.MailUser != "" {
		structExtraFromCli.MailUser = f.Crane.MailUser
	}
	if f.Crane.Comment != "" {
		structExtraFromCli.Comment = f.Crane.Comment
	}

	// Marshal extra attributes
	var extraFromCli string
	if err := structExtraFromCli.Marshal(&extraFromCli); err != nil {
		return fmt.Errorf("invalid extra attributes: %v", err)
	}
	job.ExtraAttr = extraFromCli

	// Exclusive node allocation
	job.Exclusive = f.Crane.Exclusive

	// Hold job submission
	job.Hold = f.Crane.Hold

	return nil
}

// applyEnvironmentOptions applies environment-related options to the job
func applyEnvironmentOptions(_ *Flags, job *protos.JobToCtld) error {
	// Set CWD if not already set
	if job.Cwd == "" {
		job.Cwd, _ = os.Getwd()
	}

	// Set UID/GID for job execution
	job.Uid = uint32(os.Getuid())
	gids, err := util.CollectEffectiveGroups()
	if err != nil {
		return fmt.Errorf("failed to collect effective groups: %w", err)
	}
	job.Gids = gids

	// Set command line for auditing
	job.CmdLine = strings.Join(os.Args, " ")

	return nil
}

// applyStepEnvironmentOptions sets defaults for step submission
func applyStepEnvironmentOptions(step *protos.StepToCtld) error {
	if step.Cwd == "" {
		step.Cwd, _ = os.Getwd()
	}

	if step.CmdLine == "" {
		step.CmdLine = strings.Join(os.Args, " ")
	}

	if step.Uid == 0 {
		step.Uid = uint32(os.Getuid())
	}

	if len(step.Gids) == 0 {
		gids, err := util.CollectEffectiveGroups()
		if err != nil {
			return fmt.Errorf("failed to collect effective groups: %w", err)
		}
		step.Gids = gids
	}

	return nil
}

// applyIOOptions configures TTY and input stream options for the container
// Based on docker/podman behavior matrix:
//
// | Flags     | fg/bg | tty   | stdin | stdin_once | description                                 |
// | ------------------ | ---: | -----: | -----: | ---------: | ------------------------------------- |
// | (none -i/-d/-t)    | foreground | false | false |      false | Run in foreground; no interactive input channel |
// | -i                 | foreground | false |  true |       true | Has STDIN, one-time: input is closed when session ends |
// | -t                 | foreground |  true | false |      false | TTY mode; merged output (stderr combined); no input channel |
// | -it                | foreground |  true |  true |       true | Common interactive: TTY + one-time STDIN |
// | -d                 | background | false | false |      false | Background only; no interactive input |
// | -di                | background | false |  true |      false | Background but retains persistent input capability; can attach -i later |
// | -dt                | background |  true | false |      false | Background + TTY; no input |
// | -dit               | background |  true |  true |      false | Background + TTY + persistent input capability; can attach -it later |
func applyIOOptions(f *Flags, containerMeta *protos.ContainerJobAdditionalMeta) {
	// TTY allocation: directly use -t flag
	containerMeta.Tty = f.Run.Tty

	// Stdin attachment: directly use -i flag
	containerMeta.Stdin = f.Run.Interactive

	// Stdin_once logic:
	// - true for foreground containers with stdin
	// - false for background containers
	// - false when stdin is disabled
	containerMeta.StdinOnce = containerMeta.Stdin && !f.Run.Detach
}

// buildContainerMeta builds container-level settings shared by job and step submissions.
func buildContainerMeta(f *Flags, image string, command []string) (*protos.ContainerJobAdditionalMeta, error) {
	if f.Run.ImagePullingTimeout < 0 || f.Run.ImagePullingTimeout > 3600 {
		return nil, fmt.Errorf("--pull-timeout must be between 0 and 3600 seconds (0 uses the server configuration)")
	}
	imageRef, err := NormalizeImageRef(image)
	if err != nil {
		return nil, fmt.Errorf("invalid image reference '%s': %v", image, err)
	}

	username, password, err := getAuthForRegistry(imageRef.ServerAddress)
	if err != nil {
		log.Warnf("Failed to get auth for registry %s: %v", imageRef.ServerAddress, err)
		username, password = "", ""
	}

	containerMeta := &protos.ContainerJobAdditionalMeta{
		Image: &protos.ContainerJobAdditionalMeta_ImageInfo{
			Image:         imageRef.Image,
			Username:      username,
			Password:      password,
			ServerAddress: imageRef.ServerAddress,
			PullPolicy:    f.Run.PullPolicy,
		},
		Detached: f.Run.Detach,
		Env:      make(map[string]string),
		Mounts:   make(map[string]string),
	}
	if f.Run.ImagePullingTimeout > 0 {
		containerMeta.Image.PullTimeoutSec = &f.Run.ImagePullingTimeout
	}

	if f.Run.Name != "" {
		containerMeta.Name = f.Run.Name
	}

	if f.Run.Entrypoint != "" {
		containerMeta.Command = f.Run.Entrypoint
		containerMeta.Args = command
	} else if len(command) > 0 {
		containerMeta.Command = command[0]
		if len(command) > 1 {
			containerMeta.Args = command[1:]
		}
	}

	if f.Run.Workdir != "" {
		containerMeta.Workdir = f.Run.Workdir
	}

	for _, env := range f.Run.Env {
		if err := parseEnvVar(env, containerMeta.Env); err != nil {
			return nil, fmt.Errorf("invalid environment variable '%s': %v", env, err)
		}
	}

	for _, volume := range f.Run.Volume {
		if err := parseVolumeMount(volume, containerMeta.Mounts); err != nil {
			return nil, fmt.Errorf("invalid volume mount '%s': %v", volume, err)
		}
	}

	applyIOOptions(f, containerMeta)
	return containerMeta, nil
}

// buildPodMeta constructs pod-level metadata for container jobs.
func buildPodMeta(_ *cobra.Command, f *Flags, job *protos.JobToCtld) (*protos.PodJobAdditionalMeta, error) {
	var networkMode protos.PodJobAdditionalMeta_NamespaceMode
	switch f.Run.Network {
	case "host": // NODE
		networkMode = protos.PodJobAdditionalMeta_NODE
	case "default": // POD
		networkMode = protos.PodJobAdditionalMeta_POD
	default: // TARGET / CONTAINER is not support.
		return nil, fmt.Errorf("invalid network specification '%s': only 'host' and 'default' are supported", f.Run.Network)
	}

	podMeta := &protos.PodJobAdditionalMeta{
		Name: f.Run.Name,
		Namespace: &protos.PodJobAdditionalMeta_NamespaceOption{
			Network: networkMode,
		},
		Userns: f.Run.UserNS,
	}

	if len(job.Gids) == 0 {
		return nil, fmt.Errorf("effective group list is empty")
	}
	if !podMeta.Userns {
		podMeta.RunAsUser = job.Uid
		podMeta.RunAsGroup = job.Gids[0]
	}
	if f.Run.User != "" {
		// Keep the mode's default GID when only a UID is specified.
		if err := parseUserSpec(f.Run.User, podMeta); err != nil {
			return nil, fmt.Errorf("invalid user specification '%s': %v", f.Run.User, err)
		}
	}
	if podMeta.Userns && len(job.Gids) > 1 {
		// User namespaces currently execute with the effective group only.
		job.Gids = job.Gids[:1]
	}
	if err := util.ValidateContainerIdentity(job.Uid, job.Gids, podMeta); err != nil {
		return nil, err
	}

	if networkMode == protos.PodJobAdditionalMeta_NODE && len(f.Run.Ports) != 0 {
		// Port mapping not feasible in NODE mode.
		return nil, fmt.Errorf("port mapping is not supported in 'host' network mode")
	}

	for _, port := range f.Run.Ports {
		if err := parsePortMapping(port, &podMeta.Ports); err != nil {
			return nil, fmt.Errorf("invalid port mapping '%s': %v", port, err)
		}
	}

	for i, server := range f.Run.Dns {
		f.Run.Dns[i] = strings.TrimSpace(server)
		if err := util.CheckIpv4Format(f.Run.Dns[i]); err != nil {
			return nil, fmt.Errorf("invalid dns server '%s': %w", f.Run.Dns[i], err)
		}
	}
	podMeta.DnsServers = f.Run.Dns

	return podMeta, nil
}

// buildContainerJob creates a JobToCtld with container metadata from command line arguments
func buildContainerJob(cmd *cobra.Command, f *Flags, image string, command []string) (*protos.JobToCtld, error) {
	job := &protos.JobToCtld{
		Type:          protos.JobType_Container,
		TimeLimit:     util.InvalidDuration(),
		NodeNumMin:    0,
		NodeNumMax:    0,
		NtasksPerNode: 0,
		Ntasks:        0,
		GetUserEnv:    false,
		Env:           make(map[string]string),
	}

	if err := applyResourceOptions(f, job); err != nil {
		return nil, fmt.Errorf("failed to apply resource options: %v", err)
	}

	if err := applySchedulingOptions(cmd, f, job); err != nil {
		return nil, fmt.Errorf("failed to apply scheduling options: %v", err)
	}

	if err := applyEnvironmentOptions(f, job); err != nil {
		return nil, fmt.Errorf("failed to apply environment options: %v", err)
	}

	podMeta, err := buildPodMeta(cmd, f, job)
	if err != nil {
		return nil, err
	}

	containerMeta, err := buildContainerMeta(f, image, command)
	if err != nil {
		return nil, err
	}

	if f.Run.Name != "" {
		job.Name = f.Run.Name
	}

	job.ContainerMeta = containerMeta
	job.PodMeta = podMeta
	return job, nil
}

// buildContainerStep creates a StepToCtld for a container step submission.
func buildContainerStep(cmd *cobra.Command, f *Flags, jobId uint32, image string, command []string) (*protos.StepToCtld, error) {
	if f.Run.User != "" || len(f.Run.Ports) > 0 || flagChanged(cmd, "userns") || flagChanged(cmd, "network") {
		return nil, fmt.Errorf("user, userns, port, and network options are not supported when submitting container steps; pod configuration is inherited from the job")
	}

	step := &protos.StepToCtld{
		TimeLimit:     util.InvalidDuration(),
		JobId:         jobId,
		Type:          protos.JobType_Container,
		Env:           make(map[string]string),
		Name:          f.Run.Name,
		NodeNum:       0,
		NtasksPerNode: 0,
		Ntasks:        0,
	}

	// Inherit from job environment variables
	if ntasksStr, exists := syscall.Getenv("CRANE_NTASKS"); exists {
		if ntasks, err := strconv.ParseUint(ntasksStr, 10, 32); err == nil {
			step.Ntasks = uint32(ntasks)
		}
	}

	if step.Ntasks > 1 {
		log.Warnf("CRANE_NTASKS environment variable is set to %d, but container steps only support a single task, overriding to 1.", step.Ntasks)
		step.Ntasks = 1
	}

	if numNodesStr, exists := syscall.Getenv("CRANE_JOB_NUM_NODES"); exists {
		if numNodes, err := strconv.ParseUint(numNodesStr, 10, 32); err == nil {
			step.NodeNum = uint32(numNodes)
		}
	}
	if ntasksPerNodeStr, exists := syscall.Getenv("CRANE_NTASKS_PER_NODE"); exists {
		if ntasksPerNode, err := strconv.ParseUint(ntasksPerNodeStr, 10, 32); err == nil {
			step.NtasksPerNode = uint32(ntasksPerNode)
		}
	}

	if err := applyStepResourceOptions(cmd, f, step); err != nil {
		return nil, fmt.Errorf("failed to apply step resource options: %v", err)
	}

	if err := applyStepEnvironmentOptions(step); err != nil {
		return nil, fmt.Errorf("failed to apply step environment options: %v", err)
	}

	containerMeta, err := buildContainerMeta(f, image, command)
	if err != nil {
		return nil, err
	}

	step.ContainerMeta = containerMeta
	return step, nil
}

// validateContainerJob validates container-specific parameters
func validateContainerJob(job *protos.JobToCtld) error {
	containerMeta := job.ContainerMeta
	if containerMeta == nil {
		return fmt.Errorf("container metadata is missing")
	}
	if job.PodMeta == nil {
		return fmt.Errorf("pod metadata is required for container jobs")
	}
	if job.Type != protos.JobType_Container {
		return fmt.Errorf("job type must be Container")
	}

	if err := util.ValidateContainerIdentity(job.Uid, job.Gids, job.PodMeta); err != nil {
		return err
	}

	// Validate image specification
	if containerMeta.Image == nil || containerMeta.Image.Image == "" {
		return fmt.Errorf("container image is required")
	}

	for _, port := range job.PodMeta.Ports {
		if port.HostPort < 1 || port.HostPort > 65535 {
			return fmt.Errorf("invalid host port %d: must be between 1 and 65535", port.HostPort)
		}
		if port.ContainerPort < 1 || port.ContainerPort > 65535 {
			return fmt.Errorf("invalid container port %d: must be between 1 and 65535", port.ContainerPort)
		}
	}

	// Validate volume mounts
	for hostPath, containerPath := range containerMeta.Mounts {
		if hostPath == "" || containerPath == "" {
			return fmt.Errorf("host path and container path cannot be empty")
		}
		// Note: Skip file existence check for now as it may not be accessible from frontend
	}

	// Validate environment variables - check for reserved names
	for envName := range containerMeta.Env {
		if strings.HasPrefix(envName, "CRANE_") {
			log.Warnf("Environment variable %s uses reserved CRANE_ prefix", envName)
		}
	}

	// Validate pull policy if specified
	if policy := containerMeta.Image.PullPolicy; policy != "" {
		if policy != "Always" && policy != "IfNotPresent" && policy != "Never" {
			return fmt.Errorf("invalid pull policy '%s': must be Always, IfNotPresent, or Never", policy)
		}
	}

	return nil
}

// validateContainerStep validates container step submission payload.
func validateContainerStep(step *protos.StepToCtld) error {
	if step.JobId == 0 {
		return fmt.Errorf("job_id is required for container steps")
	}

	if step.Type != protos.JobType_Container {
		return fmt.Errorf("step type must be Container")
	}

	containerMeta := step.ContainerMeta
	if containerMeta == nil {
		return fmt.Errorf("container metadata is required for steps")
	}

	if containerMeta.Image == nil || containerMeta.Image.Image == "" {
		return fmt.Errorf("container image is required")
	}

	for hostPath, containerPath := range containerMeta.Mounts {
		if hostPath == "" || containerPath == "" {
			return fmt.Errorf("host path and container path cannot be empty")
		}
	}

	for envName := range containerMeta.Env {
		if strings.HasPrefix(envName, "CRANE_") {
			log.Warnf("Environment variable %s uses reserved CRANE_ prefix", envName)
		}
	}

	if policy := containerMeta.Image.PullPolicy; policy != "" {
		if policy != "Always" && policy != "IfNotPresent" && policy != "Never" {
			return fmt.Errorf("invalid pull policy '%s': must be Always, IfNotPresent, or Never", policy)
		}
	}

	return nil
}

// submitContainerJob submits a container job via gRPC
func submitContainerJob(job *protos.JobToCtld) (*protos.SubmitBatchJobReply, error) {
	req := &protos.SubmitBatchJobRequest{Job: job}

	reply, err := stub.SubmitBatchJob(context.Background(), req)
	if err != nil {
		return reply, util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to submit the container job")
	}

	if reply.GetOk() {
		return reply, nil
	}

	if reply.GetReason() != "" {
		return reply, util.NewCraneErr(util.ErrorBackend,
			fmt.Sprintf("Container job submission failed: %s", reply.GetReason()))
	}

	return reply, util.NewCraneErr(util.ErrorBackend,
		fmt.Sprintf("Container job submission failed: %s", util.ErrMsg(reply.GetCode())))
}

// submitContainerStep submits a container step via gRPC
func submitContainerStep(step *protos.StepToCtld) (*protos.SubmitContainerStepReply, error) {
	req := &protos.SubmitContainerStepRequest{Step: step}

	reply, err := stub.SubmitContainerStep(context.Background(), req)
	if err != nil {
		return reply, util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to submit the container step")
	}

	if reply.GetOk() {
		return reply, nil
	}

	return reply, util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Container step submission failed: %s", util.ErrMsg(reply.GetCode())))
}

// Handle auto-attaching to container after run command
// See applyIOOptions for explanation of the behavior matrix
func attachAfterRun(f *Flags, reply protoreflect.ProtoMessage) error {
	var jobId, stepId uint32
	switch r := reply.(type) {
	case *protos.SubmitBatchJobReply:
		// Primary container step
		jobId = r.GetJobId()
		stepId = 1
	case *protos.SubmitContainerStepReply:
		// Specific container step
		jobId = r.GetJobId()
		stepId = r.GetStepId()
	default:
		return fmt.Errorf("invalid reply type for attachAfterRun")
	}

	if f.Run.Detach {
		fmt.Printf("Container submitted successfully. Job ID: %d, Step ID: %d\n", jobId, stepId)
		return nil
	}

	streamOpt := StreamOptions{
		Stdin:  f.Run.Interactive,
		Stdout: true,
		Stderr: !f.Run.Tty,
		Tty:    f.Run.Tty,
		// TODO: consider add transport selection.
		Transport: "spdy",
	}

	// NOTE: We left NodeName empty here, as we can't predict where the container will be scheduled.
	// So the backend will handle the routing.
	req := &protos.AttachContainerStepRequest{
		JobId:  jobId,
		StepId: stepId,
		Uid:    uint32(os.Getuid()),
		Stdin:  streamOpt.Stdin,
		Stdout: streamOpt.Stdout,
		Stderr: streamOpt.Stderr,
		Tty:    streamOpt.Tty,
	}

	attach := func(ctx context.Context, req *protos.AttachContainerStepRequest) (*protos.AttachContainerStepReply, error) {
		grpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		reply, err := stub.AttachContainerStep(grpcCtx, req)
		if err != nil {
			return nil, util.NewCraneErrFromGrpc(util.ErrorNetwork, err,
				"Failed to get attach URL for container (Job ID: %d, Step ID: %d)", jobId, stepId)
		}

		return reply, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGHUP, syscall.SIGTERM)
	go func() {
		sig := <-sigchan
		log.Infof("Received signal %s when attaching to container (Job ID: %d, Step ID: %d), exiting...\n(Note: Step is not cancelled.)", sig, jobId, stepId)
		cancel()
	}()

	for {
		if ctx.Err() != nil {
			return nil
		}

		reply, err := attach(ctx, req)
		if err != nil {
			// Network or other gRPC errors: exit immediately
			return err
		}

		if reply.GetOk() {
			// Case 1: Success - execute expected operation
			return streamWithURL(ctx, reply.GetUrl(), streamOpt, jobId, stepId)
		}

		switch reply.GetStatus().GetCode() {
		case protos.ErrCode_ERR_CRI_CONTAINER_NOT_READY:
			// Case 2: NOT_READY - retry until status changes
			log.Debugf("Attach not ready yet: %s. Retrying in 10 seconds...", reply.GetStatus().GetDescription())
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(10 * time.Second):
				continue
			}
		case protos.ErrCode_ERR_CRI_MULTIPLE_NODES:
			// Case 3: MULTIPLE_NODES (step is spawned on multiple nodes) - exit with msg
			fmt.Printf("Container submitted successfully. Job ID: %d, Step ID: %d\nMultiple nodes requested, auto-attach disabled.\n", jobId, stepId)
			return nil
		}

		// Case 3: Any other backend error - exit immediately with error
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to get attach URL for container (Job ID: %d, Step ID: %d): %s", jobId, stepId, reply.GetStatus().GetDescription()))
	}
}
