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

package creport

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"

	"github.com/spf13/cobra"
)

var (
	FlagConfigFilePath   string
	FlagFilterStartTime  string
	FlagFilterEndTime    string
	FlagFilterAccounts   string
	FlagFilterUsers      string
	FlagFilterQosList    string
	FlagOutType          string
	FlagGroupSet         bool
	FlagFilterWckeys     string
	FlagFilterGids       string
	FlagFilterGrouping   string
	FlagFilterJobIDs     string
	FlagFilterPartitions string
	FlagFilterNodeNames  string
	FlagTopCount         uint32
	FlagPrintJobCount    bool
	FlagJson             bool

	RootCmd = &cobra.Command{
		Use:           "creport",
		Short:         "Display system jobs info report",
		Long:          "",
		Version:       util.Version(),
		SilenceErrors: true,
		SilenceUsage:  true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			util.DetectNetworkProxy()
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
			return nil
		},
	}

	userCmd = &cobra.Command{
		Use:   "user",
		Short: "Display system user jobs info report",
		Long:  "",
	}

	clusterCmd = &cobra.Command{
		Use:   "cluster",
		Short: "Display system cluster jobs info report",
		Long:  "",
	}

	jobCmd = &cobra.Command{
		Use:   "job",
		Short: "Display system job size info report",
		Long:  "",
	}

	userTopUsageCmd = &cobra.Command{
		Use:   "topusage",
		Short: "Display statistical information of the top job accounts under the specified users",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("group") {
				FlagGroupSet = true
			} else {
				FlagGroupSet = false
			}
			if cmd.Flags().Changed("topcount") {
				if FlagTopCount == 0 {
					return util.NewCraneErr(util.ErrorCmdArg, "Output line number limit must be greater than 0")
				}
			}
			return QueryUsersTopSummaryItem()
		},
	}

	accountUtilizationByUserCmd = &cobra.Command{
		Use:   "accountutilizationbyuser",
		Short: "Display statistical information of all job accounts under the specified users",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryJobSummary(protos.QueryJobSummaryRequest_ACCOUNT_UTILIZATION_BY_USER)
		},
	}
	userUtilizationByAccountCmd = &cobra.Command{
		Use:   "userutilizationbyaccount",
		Short: "Display statistical information for all job users under the specified accounts",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryJobSummary(protos.QueryJobSummaryRequest_USER_UTILIZATION_BY_ACCOUNT)
		},
	}
	userUtilizationByWckeyCmd = &cobra.Command{
		Use:   "userutilizationbywckey",
		Short: "Display the statistical information for all job users under the specified wckeys",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryJobSummary(protos.QueryJobSummaryRequest_USER_UTILIZATION_BY_WCKEY)
		},
	}
	wckeyUtilizationByUserCmd = &cobra.Command{
		Use:   "wckeyutilizationbyuser",
		Short: "Display the statistical information for all job wckeys under the specified users",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryJobSummary(protos.QueryJobSummaryRequest_WCKEY_UTILIZATION_BY_USER)
		},
	}
	accountUtilizationByQosCmd = &cobra.Command{
		Use:   "accountutilizationbyqos",
		Short: "Display the statistical information for all job accounts under the specified qos",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryJobSummary(protos.QueryJobSummaryRequest_ACCOUNT_UTILIZATION_BY_QOS)
		},
	}
	// utilizationCmd = &cobra.Command{
	// 	Use:   "utilization",
	// 	Short: "Display relevant cluster parameters",
	// 	Long:  "",
	// 	Args:  cobra.ExactArgs(0),
	// 	RunE: func(cmd *cobra.Command, args []string) error {
	// 		return fmt.Errorf("Not implemented yet")
	// 	},
	// }
	sizesByAccountCmd = &cobra.Command{
		Use:   "sizesbyaccount",
		Short: "Display job size statistics information under the specified accounts",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryJobSizeSummary(CheckAccountCpusStatus)
		},
	}
	sizesByWckeyCmd = &cobra.Command{
		Use:   "sizesbywckey",
		Short: "Display job size statistics information under the specified wckeys",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryJobSizeSummary(CheckWckeyCpusStatus)
		},
	}
	sizesByAccountAndWcKey = &cobra.Command{
		Use:   "sizesbyaccountandwckey",
		Short: "Display job size statistics information under the specified accounts and wckeys",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return QueryJobSizeSummary(CheckAccountWckeyCpusStatus)
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
	RootCmd.PersistentFlags().StringVarP(&FlagFilterStartTime, "start-time", "S",
		GetDefaultStartTime(), "Filter job collections by start time(format: 2006-01-02T15:04:05)")
	RootCmd.PersistentFlags().StringVarP(&FlagFilterEndTime, "end-time", "E",
		GetDefaultEndTime(), "Filter job collections by end time(format: 2006-01-02T15:04:05)")
	RootCmd.PersistentFlags().StringVarP(&FlagOutType, "time", "t", "minutes",
		"Set the job output time unit(seconds/minutes/hours)")
	RootCmd.PersistentFlags().BoolVar(&FlagJson, "json", false, "Output in JSON format")

	RootCmd.AddCommand(userCmd)
	{
		userCmd.AddCommand(userTopUsageCmd)
		{
			userTopUsageCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
				"Select accounts to view (comma separated list)")
			userTopUsageCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
				"Select users to view (comma separated list)")
			userTopUsageCmd.Flags().Uint32Var(&FlagTopCount, "topcount", 10, "Change the number of users displayed")
			userTopUsageCmd.Flags().BoolVar(&FlagGroupSet, "group", false,
				"Group all accounts together for each user, Default is a separate entry for each user and account reference")
		}

	}
	RootCmd.AddCommand(clusterCmd)
	{
		clusterCmd.AddCommand(accountUtilizationByUserCmd)
		{
			accountUtilizationByUserCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
				"Select accounts to view (comma separated list)")
			accountUtilizationByUserCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
				"Select users to view (comma separated list)")
		}
		clusterCmd.AddCommand(userUtilizationByAccountCmd)
		{
			userUtilizationByAccountCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
				"Select accounts to view (comma separated list)")
			userUtilizationByAccountCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
				"Select users to view (comma separated list)")
		}
		clusterCmd.AddCommand(userUtilizationByWckeyCmd)
		{
			userUtilizationByWckeyCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
				"Select users to view (comma separated list)")
			userUtilizationByWckeyCmd.Flags().StringVarP(&FlagFilterWckeys, "wckeys", "w", "",
				"Select wckeys to view (comma separated list)")
		}
		clusterCmd.AddCommand(wckeyUtilizationByUserCmd)
		{
			wckeyUtilizationByUserCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
				"Select users to view (comma separated list)")
			wckeyUtilizationByUserCmd.Flags().StringVarP(&FlagFilterWckeys, "wckeys", "w", "",
				"Select wckeys to view (comma separated list)")
		}
		clusterCmd.AddCommand(accountUtilizationByQosCmd)
		{

			accountUtilizationByQosCmd.Flags().StringVarP(&FlagFilterAccounts, "account", "A", "",
				"Select accounts to view (comma separated list)")
			accountUtilizationByQosCmd.Flags().StringVarP(&FlagFilterUsers, "user", "u", "",
				"Select users to view (comma separated list)")
			accountUtilizationByQosCmd.Flags().StringVarP(&FlagFilterQosList, "qos", "q",
				"", "Select QoSs to view (comma separated list)")
		}
		// clusterCmd.AddCommand(utilizationCmd)
		// {
		// 	utilizationCmd.Flags().StringVarP(&FlagFilterEndTime, "end-time", "E",
		// 		GetDefaultEndTime(), "Filter job collections by end time(format: 2006-01-02T15:04:05)")
		// 	utilizationCmd.Flags().StringVarP(&FlagFilterStartTime, "start-time", "S",
		// 		GetDefaultStartTime(), "Filter job collections by start time(format: 2006-01-02T15:04:05)")
		// 	utilizationCmd.Flags().StringVarP(&FlagOutType, "time", "t", "minutes",
		// 		"Set the job output time unit(seconds/minutes/hours, default: minutes")
		// 	utilizationCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
		// }

	}
	RootCmd.AddCommand(jobCmd)
	{
		jobCmd.PersistentFlags().StringVar(&FlagFilterGids, "gid", "",
			"Select group id to view (comma separated list)")
		jobCmd.PersistentFlags().StringVar(&FlagFilterGrouping, "grouping", "50,250,500,1000",
			"Comma separated list of size groupings "+
				"(e.g. 50,100,150 would group job cpu count 1-49, 50-99, 100-149, > 150). "+
				"grouping=individual will result in a single column for each job size found.")
		jobCmd.PersistentFlags().StringVarP(&FlagFilterJobIDs, "jobs", "j", "",
			"Select job ids to view (comma separated list), default is all")
		jobCmd.PersistentFlags().StringVarP(&FlagFilterPartitions, "partition", "p", "",
			"Specify partitions to view (comma separated list), default is all")
		jobCmd.PersistentFlags().BoolVar(&FlagPrintJobCount, "printjobcount", false,
			"The report will print number of jobs range instead of time used")
		jobCmd.PersistentFlags().StringVarP(&FlagFilterNodeNames, "nodes", "n", "",
			"Specify nodes name to view (comma separated list), default is all")
		jobCmd.AddCommand(sizesByAccountCmd)
		jobCmd.AddCommand(sizesByWckeyCmd)
		{
			sizesByWckeyCmd.Flags().StringVarP(&FlagFilterWckeys, "wckeys", "w", "",
				"Select wckeys to view (comma separated list)")
		}
		jobCmd.AddCommand(sizesByAccountAndWcKey)
		{
			sizesByAccountAndWcKey.Flags().StringVarP(&FlagFilterWckeys, "wckeys", "w", "",
				"Select wckeys to view (comma separated list)")
		}
	}
	util.InitCraneLogger()
}
