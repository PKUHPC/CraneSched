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

package cwrapper

import (
	"CraneFrontEnd/internal/util"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

type Wrapper interface {
	Group() *cobra.Group
	SubCommands() []*cobra.Command
	HasCommand(string) bool
	Preprocess() error
}

var (
	rootCmd = &cobra.Command{
		Use:   "cwrapper",
		Short: "Wrapper of CraneSched commands",
		Long: `Wrapper of CraneSched commands.
This is a highly EXPERIMENTAL feature. 
If any error occurs, please refer to original commands.`,
		Version:       util.Version(),
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	wrappers = []Wrapper{
		LSFWrapper{},
		SGEWrapper{},
		SlurmWrapper{},
	}
)

func ParseCmdArgs() {
	util.InitCraneLogger()
	rootCmd.SetVersionTemplate(util.VersionTemplate())

	for _, wrapper := range wrappers {
		rootCmd.AddGroup(wrapper.Group())
		rootCmd.AddCommand(wrapper.SubCommands()...)
		if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") && wrapper.HasCommand(os.Args[1]) {
			wrapper.Preprocess()
		}
	}

	util.RunAndHandleExit(rootCmd)
}

func addConfigPathFlag(cmd *cobra.Command, target *string) {
	cmd.Flags().StringVar(target, "config", util.DefaultConfigPath, "Path to configuration file")
}

func exitWithCommandError(command string, err error) {
	var craneErr *util.CraneError
	if errors.As(err, &craneErr) && craneErr != nil {
		message := strings.TrimSpace(craneErr.Message)
		if message != "" {
			fmt.Fprintf(os.Stderr, "%s: error: %s\n", command, message)
		}
		os.Exit(craneErr.Code)
	}

	message := strings.TrimSpace(err.Error())
	if message != "" {
		fmt.Fprintf(os.Stderr, "%s: error: %s\n", command, message)
	}
	os.Exit(util.ErrorGeneric)
}
