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
	"CraneFrontEnd/internal/util"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const detachKeysHelp = "Detach key: in an interactive TTY session, press Ctrl-P + Ctrl-Q to detach without stopping the container."

func ParseCmdArgs() {
	cobra.EnableTraverseRunHooks = true
	util.RunEWrapperForLeafCommand(RootCmd)
	util.RunAndHandleExit(RootCmd)
}

func initConfigAndStub(cmd *cobra.Command, args []string) {
	f := GetFlags()
	config = util.ParseConfig(f.Global.ConfigPath)
	if cmd != nil && cmd.Name() == "run" && !flagChanged(cmd, "userns") {
		f.Run.UserNS = config.Container.UserNsEnabledByDefault
	}
	stub = util.GetStubToCtldByConfig(config)
}

var (
	RootCmd = &cobra.Command{
		Use:           "ccon",
		Short:         "Container job management tool",
		Version:       util.Version(),
		SilenceErrors: true,
		SilenceUsage:  true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			util.DetectNetworkProxy()
			util.InitDiagLogger(GetFlags().Global.DebugLevel)

			var err error
			if cmd != RunCmd {
				cmd.Root().Flags().Visit(func(flag *pflag.Flag) {
					if err == nil && flag.Changed && IsCraneFlag(flag) {
						err = util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("flag '%s' is only valid for the 'run' command", flag.Name))
					}
				})
			}

			for i, arg := range args {
				if i < len(args)-1 && (arg == "true" || arg == "false") {
					err = util.NewCraneErr(util.ErrorCmdArg,
						fmt.Sprintf("unexpected boolean argument '%s'; if you intended to set a boolean flag, use '--flag=%s'.", arg, arg))
				}
			}

			return err
		},
	}

	RunCmd = &cobra.Command{
		Use:              "run [flags] IMAGE [COMMAND] [ARG...]",
		Short:            "Create and run a new container",
		Long:             "Create and run a new container.\n\n" + detachKeysHelp,
		PersistentPreRun: initConfigAndStub,
		RunE:             runExecute,
	}

	LogCmd = &cobra.Command{
		Use:              "logs [flags] CONTAINER",
		Short:            "Fetch the logs of a container step (JOBID.STEPID)",
		PersistentPreRun: initConfigAndStub,
		RunE:             logExecute,
	}

	StopCmd = &cobra.Command{
		Use:              "stop [flags] CONTAINER",
		Short:            "Stop one or more running containers",
		PersistentPreRun: initConfigAndStub,
		RunE:             stopExecute,
	}

	WaitCmd = &cobra.Command{
		Use:              "wait [flags]",
		Short:            "Wait for all container steps in the current job to finish",
		PersistentPreRun: initConfigAndStub,
		RunE:             waitExecute,
	}

	PsCmd = &cobra.Command{
		Use:              "ps [flags]",
		Short:            "List containers (steps)",
		PersistentPreRun: initConfigAndStub,
		RunE:             psExecute,
	}

	PodCmd = &cobra.Command{
		Use:              "pods [flags]",
		Short:            "List container pods (jobs)",
		PersistentPreRun: initConfigAndStub,
		RunE:             podExecute,
	}

	InspectPodCmd = &cobra.Command{
		Use:              "inspectp POD",
		Short:            "Display detailed information for a container pod (JOBID)",
		PersistentPreRun: initConfigAndStub,
		RunE:             inspectPodExecute,
	}

	InspectCmd = &cobra.Command{
		Use:              "inspect CONTAINER",
		Short:            "Display detailed information for a container step (JOBID.STEPID)",
		PersistentPreRun: initConfigAndStub,
		RunE:             inspectStepExecute,
	}

	AttachCmd = &cobra.Command{
		Use:              "attach [flags] CONTAINER",
		Short:            "Attach local standard input, output, and error streams to a running container",
		Long:             "Attach local standard input, output, and error streams to a running container.\n\n" + detachKeysHelp,
		PersistentPreRun: initConfigAndStub,
		RunE:             attachExecute,
	}

	ExecCmd = &cobra.Command{
		Use:              "exec [flags] CONTAINER COMMAND [ARG...]",
		Short:            "Execute a command in a running container",
		Long:             "Execute a command in a running container.\n\n" + detachKeysHelp,
		PersistentPreRun: initConfigAndStub,
		RunE:             execExecute,
	}

	// These two commands do not need config and stub
	LoginCmd = &cobra.Command{
		Use:   "login [flags] SERVER",
		Short: "Log in to a container registry",
		RunE:  loginExecute,
	}

	LogoutCmd = &cobra.Command{
		Use:   "logout SERVER",
		Short: "Log out from a container registry",
		RunE:  logoutExecute,
	}

	// Not supported commands
	CreateCmd = &cobra.Command{
		Use:    "create is not supported",
		Short:  "Not supported - use 'ccon run' instead",
		RunE:   createExecute,
		Hidden: true,
	}

	StartCmd = &cobra.Command{
		Use:    "start is not supported",
		Short:  "Not supported - use 'ccon run' instead",
		RunE:   startExecute,
		Hidden: true,
	}

	RestartCmd = &cobra.Command{
		Use:    "restart is not supported",
		Short:  "Not supported - use 'ccon run' instead",
		RunE:   restartExecute,
		Hidden: true,
	}

	RmCmd = &cobra.Command{
		Use:    "rm [flags] CONTAINER",
		Short:  "Not supported - containers are auto-cleaned",
		RunE:   rmExecute,
		Hidden: true,
	}
)

func InitializeCommandFlags() {
	f := GetFlags()

	f.InitializeCraneFlags()
	RootCmd.Flags().AddFlagSet(f.flagSetCrane)
	RootCmd.PersistentFlags().StringVarP(&f.Global.ConfigPath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	RootCmd.PersistentFlags().StringVarP(&f.Global.DebugLevel, "debug-level", "",
		"info", "Available debug level: trace, debug, info")
	RootCmd.PersistentFlags().BoolVar(&f.Global.Json, "json", false, "Output in JSON format")

	RunCmd.Flags().StringVar(&f.Run.Name, "name", "", "Assign a name to the container")
	RunCmd.Flags().StringSliceVarP(&f.Run.Ports, "ports", "p", []string{}, "Publish a container's port(s) to the host")
	RunCmd.Flags().StringSliceVarP(&f.Run.Env, "env", "e", []string{}, "Set environment variables")
	RunCmd.Flags().StringSliceVarP(&f.Run.Volume, "volume", "v", []string{}, "Bind mount a volume")
	RunCmd.Flags().BoolVarP(&f.Run.Detach, "detach", "d", false, "Run container in background and print container ID")
	RunCmd.Flags().BoolVarP(&f.Run.Interactive, "interactive", "i", false, "Make STDIN available to the contained process")
	RunCmd.Flags().BoolVarP(&f.Run.Tty, "tty", "t", false, "Allocate a pseudo-TTY for container")
	RunCmd.Flags().StringVar(&f.Run.Entrypoint, "entrypoint", "", "Override the default entrypoint of the image")
	RunCmd.Flags().StringVarP(&f.Run.User, "user", "u", "", "UID[:GID]; mapped container IDs with userns (default 0:0), otherwise submitter UID/effective GID")
	RunCmd.Flags().BoolVar(&f.Run.UserNS, "userns", util.DefaultUserNsEnabledByDefault, "Enable user namespace; default follows Container.UserNsEnabledByDefault in config")
	RunCmd.Flags().Lookup("userns").DefValue = "from config"
	RunCmd.Flags().StringVarP(&f.Run.Workdir, "workdir", "w", "", "Working directory inside the container")
	RunCmd.Flags().StringVar(&f.Run.PullPolicy, "pull-policy", "", "Image pull policy: Always, IfNotPresent, or Never")
	RunCmd.Flags().Int64Var(&f.Run.ImagePullingTimeout, "pull-timeout", 0, "Timeout for pulling the image, in seconds (1-3600; 0 uses the server configuration)")
	RunCmd.Flags().StringVar(&f.Run.Network, "network", "default", "Network mode for the container (currently only 'host' and 'default' is supported)")

	RunCmd.Flags().Float64Var(&f.Run.Cpus, "cpus", 0, "Number of CPUs (maps to cpus-per-task)")
	RunCmd.Flags().StringVar(&f.Run.Memory, "memory", "", "Memory limit (e.g., 2g, 512m)")
	RunCmd.Flags().StringVar(&f.Run.Gpus, "gpus", "", "GPU devices")
	RunCmd.Flags().MarkDeprecated("cpus", "use --cpus-per-task instead. See \"Crane Flags\" for details.")
	RunCmd.Flags().MarkDeprecated("memory", "use --mem instead. See \"Crane Flags\" for details.")
	RunCmd.Flags().MarkDeprecated("gpus", "use --gres instead. See \"Crane Flags\" for details.")

	// Disable interspersed flags to avoid confusion with IMAGE and COMMAND
	// e.g., ccon run ubuntu /bin/bash -c 'echo hello'
	// Here -c is part of COMMAND, not a flag
	RunCmd.Flags().SetInterspersed(false)
	RunCmd.Flags().StringSliceVar(&f.Run.Dns, "dns", []string{}, "Configure DNS server(s) for pod (comma-separated or repeated)")

	StopCmd.Flags().IntVarP(&f.Stop.Timeout, "timeout", "t", 10, "Seconds to wait for stop before killing it")

	PsCmd.Flags().BoolVarP(&f.Ps.All, "all", "a", false, "Show all containers (default shows just running)")
	PsCmd.Flags().BoolVarP(&f.Ps.Quiet, "quiet", "q", false, "Only display container IDs")

	PodCmd.Flags().BoolVarP(&f.Pod.All, "all", "a", false, "Show all pods (default shows just running)")
	PodCmd.Flags().BoolVarP(&f.Pod.Quiet, "quiet", "q", false, "Only display pod IDs")

	LogCmd.Flags().StringVarP(&f.Log.TargetNode, "target-node", "n", "", "Target node to fetch logs from")
	LogCmd.Flags().BoolVarP(&f.Log.Follow, "follow", "f", false, "Follow log output")
	LogCmd.Flags().IntVar(&f.Log.Tail, "tail", -1, "Number of lines to show from the end of the logs")
	LogCmd.Flags().BoolVarP(&f.Log.Timestamps, "timestamps", "t", false, "Show timestamps")
	LogCmd.Flags().StringVar(&f.Log.Since, "since", "", "Show logs since timestamp (e.g. 2025-01-15T10:30:00Z) or relative (e.g. 42m for 42 minutes)")
	LogCmd.Flags().StringVar(&f.Log.Until, "until", "", "Show logs before a timestamp (e.g. 2025-01-15T10:30:00Z) or relative (e.g. 42m for 42 minutes)")

	LoginCmd.Flags().StringVarP(&f.Login.Username, "username", "u", "", "Username for registry authentication")
	LoginCmd.Flags().StringVarP(&f.Login.Password, "password", "p", "", "Password for registry authentication")
	LoginCmd.Flags().BoolVar(&f.Login.PasswordStdin, "password-stdin", false, "Read password from stdin")

	AttachCmd.Flags().BoolVar(&f.Attach.Stdin, "stdin", true, "Attach STDIN")
	AttachCmd.Flags().BoolVar(&f.Attach.Stdout, "stdout", true, "Attach STDOUT")
	AttachCmd.Flags().BoolVar(&f.Attach.Stderr, "stderr", false, "Attach STDERR")
	AttachCmd.Flags().BoolVar(&f.Attach.Tty, "tty", true, "Allocate a pseudo-TTY")
	AttachCmd.Flags().StringVar(&f.Attach.Transport, "transport", "spdy", "Transport protocol (spdy, ws)")
	AttachCmd.Flags().StringVarP(&f.Attach.TargetNode, "target-node", "n", "", "Target node to attach to")

	ExecCmd.Flags().BoolVarP(&f.Exec.Interactive, "interactive", "i", false, "Keep STDIN open")
	ExecCmd.Flags().BoolVarP(&f.Exec.Tty, "tty", "t", false, "Allocate a pseudo-TTY")
	ExecCmd.Flags().StringVar(&f.Exec.Transport, "transport", "spdy", "Transport protocol (spdy, ws)")
	ExecCmd.Flags().StringVarP(&f.Exec.TargetNode, "target-node", "n", "", "Target node to execute command on")

	WaitCmd.Flags().IntVarP(&f.Wait.Interval, "interval", "t", defaultWaitIntervalSeconds, "Polling interval in seconds (min 10s)")
}

func init() {
	f := GetFlags()

	InitializeCommandFlags()
	craneFlagsUsage := f.GetCraneFlagSet().FlagUsages()

	// Parse root-level flags before running any command0
	RootCmd.TraverseChildren = true
	RootCmd.SetVersionTemplate(util.VersionTemplate())

	// Link all commands to the root command
	RootCmd.AddCommand(RunCmd, StopCmd, WaitCmd, RmCmd, PsCmd, PodCmd, InspectPodCmd, InspectCmd, LogCmd, LoginCmd, LogoutCmd, AttachCmd, ExecCmd)
	RootCmd.AddCommand(CreateCmd, StartCmd, RestartCmd)

	// Hide crane flags by default. Only display them when running 'ccon run'.
	var hiddenFlags []*pflag.Flag
	RootCmd.Flags().VisitAll(func(flag *pflag.Flag) {
		if IsCraneFlag(flag) && !flag.Hidden {
			flag.Hidden = true
			hiddenFlags = append(hiddenFlags, flag)
		}
	})

	// Display crane flags when running 'ccon run'
	runCmdOriginUsageStr := RunCmd.UsageString()
	RunCmd.SetUsageFunc(func(cmd *cobra.Command) error {
		cmd.Println(runCmdOriginUsageStr)
		cmd.Println("Crane Flags:")
		cmd.Println(craneFlagsUsage)
		cmd.Println("To use crane flags, place them between 'ccon' and 'run', e.g.,\n  ccon -p CPU run ...")
		return nil
	})
}
