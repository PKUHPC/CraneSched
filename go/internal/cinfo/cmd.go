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

package cinfo

import (
	"CraneFrontEnd/internal/util"

	"github.com/spf13/cobra"
)

var (
	FlagFilterDownOnly       bool
	FlagFilterRespondingOnly bool
	FlagFilterPartitions     []string
	FlagFilterNodes          []string
	FlagFilterCranedStates   []string
	FlagIterate              uint64
	FlagConfigFilePath       string
	FlagNoHeader             bool
	FlagJson                 bool
	FlagFormat               string
	// FlagSummarize            bool
	FlagListReason bool

	RootCmd = &cobra.Command{
		Use:           "cinfo [flags]",
		Short:         "Display the state of partitions and nodes",
		Long:          "",
		Version:       util.Version(),
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.ExactArgs(0),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if FlagIterate != 0 {
				return loopedQuery(FlagIterate)
			} else {
				return Query()
			}
		},
	}
)

func ParseCmdArgs() {
	util.RunEWrapperForLeafCommand(RootCmd)
	util.RunAndHandleExit(RootCmd)
}

func init() {
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	RootCmd.Flags().BoolVarP(&FlagFilterDownOnly, "dead", "d", false,
		"Display non-responding nodes only")
	RootCmd.Flags().StringSliceVarP(&FlagFilterPartitions, "partition", "p",
		nil, "Display nodes in the specified partition only")
	RootCmd.Flags().StringSliceVarP(&FlagFilterNodes, "nodes", "n", nil,
		"Display the specified nodes only")
	RootCmd.Flags().StringSliceVarP(&FlagFilterCranedStates, "states", "t", nil,
		"Display nodes with the specified states only. \n"+
			"The state can take IDLE, MIX, ALLOC, DOWN (case-insensitive). \n"+
			"Example: \n"+
			"\t -t idle,mix \n"+
			"\t -t=alloc \n")
	RootCmd.Flags().BoolVarP(&FlagFilterRespondingOnly, "responding", "r", false,
		"Display responding nodes only")
	RootCmd.Flags().Uint64VarP(&FlagIterate, "iterate", "i", 0,
		"Display at specified intervals (seconds)")
	RootCmd.Flags().BoolVarP(&FlagNoHeader, "noheader", "N", false,
		"Do not print header line in the output")
	RootCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
	//RootCmd.Flags().BoolVarP(&FlagSummarize, "summarize", "s", false,
	//	"Display state summary only")
	RootCmd.Flags().BoolVarP(&FlagListReason, "list-reasons", "R", false,
		"Display reasons if nodes are down or drained")

	RootCmd.MarkFlagsMutuallyExclusive("states", "responding", "dead")
	RootCmd.Flags().StringVarP(&FlagFormat, "format", "o", "",
		`Specify the output format.
	Fields are identified by a percent sign (%) followed by a character or string. 
	Use a dot (.) and a number between % and the format character or string to specify a minimum width for the field.

Supported format identifiers or string, string case insensitive:
	%p/%Partition     - Display all partitions in the current environment.
	%a/%Avail         - Displays the state of the node.
	%n/%Nodes         - Display the number of partition nodes. 
	%s/%State         - Display the status of partition nodes
	%l/%NodeList      - Display all node list in the partition.

Each format specifier or string can be modified with a width specifier (e.g., "%.5j").
If the width is specified, the field will be formatted to at least that width. 
If the format is invalid or unrecognized, the program will terminate with an error message.

Example: --format "%.5partition %.6a %s" would output the partition's name in the current environment 
         with a minimum width of 5, state of the node with a minimum width of 6, and the State.
`)
	util.InitCraneLogger()
}
