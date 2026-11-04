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

package cacctmgr

import (
	"CraneFrontEnd/internal/util"
	"fmt"
)

func showHelp() {
	commandName := "cacctmgr"
	if util.IsSlurmOutputMode() {
		commandName = "sacctmgr"
	}
	help := fmt.Sprintf(`Crane Account Manager (%[1]s) - version %[2]s
  
  USAGE: %[1]s <ACTION> <ENTITY> [OPTIONS]
  
  ACTIONS:
	add       - Create a new account, user, or QoS
	delete    - Remove an account, user, or QoS
	block     - Block an account or user
	unblock   - Unblock an account or user
	modify    - Change attributes of an account, user, or QoS
	show      - Display basic information
	reset     - Reset user certificate
  
  ENTITIES:
	account     - User account in the system
	user        - Individual user
	qos         - Quality of Service settings
	transaction - Log of account-related operations
	event       - Node on/off events
	wckey       - Workload characterization key
	resource    - License resource
  
  COMMANDS:
	add account <name> [Description=<desc>] [Parent=<parent>] [DefaultQos=<qos>] 
			[Partition=<part1,part2,...>] [QosList=<qos1,qos2,...>] [Name=<name1,name2,...>]
    Create a new account with the specified attributes.
    Parameter details:
      Description=<desc>         Description of the account
      Parent=<parent>            Name of the parent account
      DefaultQos=<qos>           Default QoS for the account
      Partition=<part1,part2,...>  Allowed partitions (comma-separated)
      QosList=<qos1,qos2,...>    Allowed QoS list (comma-separated)
      Name=<name1,name2,...>    List of account name for batch creation
  
	delete account <name> | Name=<name1,name2,...>
    Remove the specified account(s) from the system.
    Parameter details:
      Name=<name1,name2,...>    Name of accounts to delete (comma-separated)
  
	show account [Name=<name1,name2,...>] [format=<Name,Description,...>]
    Display information about accounts.
    Parameter details:
      Name=<name1,name2,...>    Show only these accounts (comma-separated)
      (Equivalent forms: show account <name1,...> / show account where Name=<name1,...>)
      (If not specified, all accounts will be displayed)
      format=<Name,Description,AllowedPartition,Users,DefaultQos,AllowedQosList,
              Coordinators,Blocked> (Spelling must be correct, case is not important)
              For Example: %[1]s show account format=name,users,coordinators
      --partition-limit, -P      Also display partition resource limits for each account
  
	add user <name> Account=<account> [Coordinator=true|false] [Level=<level>] 
		[Partition=<part1,part2,...>] [Name=<name1,name2,...>]
    Create a new user and associate with an account.
    Parameter details:
      Account=<account>          Account the user belongs to (required)
      Coordinator=true|false     Whether the user is a coordinator for the account
      Level=<level>              User admin level
      Partition=<part1,part2,...>  Allowed partitions (comma-separated)
      Name=<name1,name2,...>    List of user name for batch creation
  
	delete user <name> [Account=<account>] [Name=<name1,name2,...>]
    Remove a user from the system or from a specific account.
    Parameter details:
      Account=<account>          Specify the account (optional)
      Name=<name1,name2,...>    Name of users to delete (comma-separated)
    (if name is 'ALL' and set --force, all users from a specific account will be delete.)
  
	show user [Account=<account>] [Name=<name1,name2,...>] [format=<Account,UserName,...>]
    Display information about users.
    Parameter details:
      Account=<account>          Show users of this account only
      Name=<name1,name2,...>    Show only these users (comma-separated)
      (Equivalent forms: show user <name1,...> / show user where Name=<name1,...> [Account=<account>])
      (If not specified, all users will be displayed)
      format=<Account,UserName,Uid,AllowedPartition,AllowedQosList,DefaultQos,Coordinated,
              AdminLevel,Blocked> (Spelling must be correct, case is not important)
              For Example: %[1]s show user format=account,defaultqos,adminlevel
      --partition-limit, -P      Also display partition resource limits for each user

	add wckey <name> user=<user>
    Create a new wckey and bind it to the user
    Parameter details:
      user=<user>               Name of user to add wckey

	delete wckey <name> user=<user>
    Delete an existing wckey
    Parameter details:
      user=<user>               Name of user to delete wckey

	show wckey [<name1,name2,...>]
    Display information about wckeys.
    Parameter details:
      <name1,name2,...>         Show only these wckeys (comma-separated) (optional)
      If no name is specified, information for all wckeys will be displayed.

  	add resource <name> server=<server>
    Create a new license resource
    Parameter details:
      name=<name>               License resource name
      server=<server>           Server name
      servertype=<server_type>  Server type
      type=<notset|license>     License type (default is 'notset')
      count=<count>             Total number of license resource
      lastconsumed=<lastconsumed>  Usage count obtained from external source
      flags=<none|absolute>     License resource flags (default is 'none')
      allowed=<allowed>         Amount available to the cluster license resource
      cluster=<cluster>         Cluster name
      description=<description> License resource description

	delete resource <name> server=<server>
    Delete an existing resource <name> server=<server>
    Parameter details:
      name=<name>               License resource name
      server=<serer>            License resource server name
      cluster=<cluster>         Cluster name

	show resource [withclusters] where [options]
    Display information about resource.
    Parameter details:
      name=<name>               Show only the specified resource
      server=<server>	        Show resources for a specific server
      cluster=<cluster>	        Show resources for a specific cluster

	block account <name> [Account=<account>]
    Block the specified account from submitting jobs.
    Parameter details:
      name                      Name of the account to block
      Account=<account>         Specify the account context (optional)
  
	block user <name> [Account=<account>]
    Block the specified user from submitting jobs.
    Parameter details:
      name                      Name of the user to block
      Account=<account>         Specify the account context (optional)
  
	unblock account <name> [Account=<account>]
    Unblock the specified account to allow job submission.
    Parameter details:
      name                      Name of the account to unblock
      Account=<account>         Specify the account context (optional)
  
	unblock user <name> [Account=<account>]
    Unblock the specified user to allow job submission.
    Parameter details:
      name                      Name of the user to unblock
      Account=<account>         Specify the account context (optional)

	reset <name>
	Reset the specified user's certificate.
	Parameter details:
      name 					    Name of the user to reset
      (if name is 'all', all users will be reset.)
  
	add qos <name> [Description=<desc>] [Priority=<priority>]
		[MaxJobsPerUser=<num>] [MaxCpusPerUser=<num>] [MaxTimeLimitPerJob=<duration|seconds>]
		[Preempt=<qos1,qos2,...>] [PreemptMode=<OFF|CANCEL>]
    Create a new QoS with the specified attributes.
    Parameter details:
      Description=<desc>         Description of the QoS
      Priority=<priority>        Priority (higher value means higher priority)
      MaxJobsPerUser=<num>       Maximum number of jobs per user
      MaxSubmitJobsPerUser=<num>  Maximum number of submit jobs per user
      MaxTresPerUser=<tres>       Maximum tres per user (format: cpu:<num>,mem:<num>,gres/type[:name]:num)
      MaxJobsPerAccount=<num>     Maximum number of jobs per account
      MaxSubmitJobsPerAccount=<num>  Maximum number of submit jobs per account
      MaxTresPerAccount=<tres>    Maximum tres per account (format: cpu:<num>,mem:<num>,gres/type[:name]:num)
      MaxCpusPerUser=<num>        Maximum number of CPUs per user
      MaxSubmitJobs=<num>         Maximum number of submit jobs per qos
      MaxJobs=<num>               Maximum number of jobs per qos
      MaxTres=<tres>              Maximum tres per qos (format: cpu:<num>,mem:<num>,gres/type[:name]:num)
      MaxWall=<seconds>           Maximum all run time per qos
      Flags=<DenyOnLimit|None>    Qos Flags
      MaxTimeLimitPerJob=<duration|seconds>  Maximum run time per job in seconds
      Preempt=<qos1,qos2,...>   Comma-separated QoS names this QoS may preempt. Self-reference is rejected.
      PreemptMode=<mode>        Preemption handling (OFF, CANCEL). Default OFF. REQUEUE/SUSPEND are reserved for future versions.
      Name=<name1,name2,...>    List of QoS name for batch creation
  
	delete qos <name> [Name=<name1,name2,...>]
    Remove the specified QoS from the system.
    Parameter details:
      Name=<name1,name2,...>    Name of QoS to delete (comma-separated)
  
	show qos [Name=<name1,name2,...>] [format=<Name,Description,...>]
    Display information about QoS.
    Parameter details:
      Name=<name1,name2,...>    Show only these QoS (comma-separated)
      (Equivalent forms: show qos <name1,...> / show qos where Name=<name1,...>)
      (If not specified, all QoS will be displayed)
      format=<Name,Description,Priority,MaxJobsPerUser,MaxCpusPerUser,MaxJobsPerAccount,
              MaxSubmitJobsPerUser,MaxSubmitJobsPerAccount,MaxTresPerUser,MaxTresPerAccount,
              MaxTres,MaxJobs,MaxSubmitJobs,MaxWall,Flags,MaxTimeLimitPerJob,Preempt,PreemptMode>
              (Spelling must be correct, case is not important)
      For Example: %[1]s show qos format=name,Priority,Preempt,PreemptMode

    show transaction [where] [Actor=<actor>] [Target=<target>] [Action=<action>] [Info=<info>] [StartTime=<start_time>]
    Display transaction log records.
    Parameter details:
      Actor=<actor>           Filter by actor (who performed the action)
      Target=<target>         Filter by target (object of the action)
      Action=<action>         Filter by action type (e.g. adduser, deleteaccount)
      Info=<info>             Filter by additional info (fuzzy query)
      StartTime=<start_time>  Filter by start time (format: ~YYYY-MM-DDTHH:MM:SS or YYYY-MM-DDTHH:MM:SS~)
      Example: cacctmgr show transaction where StartTime ~2026-08-04T00:00:00
      (If not specified, all transactions will be displayed, limit 1000)
    show event where [Maxlines=<maxlines>] [Nodes=<Nodes>] 
    Display node event records.
      Maxlines=<maxlines>        Filter by maxlines (Show maximum number of rows)
      Nodes=<node1,node2...>     Filter by nodes (Output specified node)
  modify <entity> where [OPTIONS] set [OPTIONS]
    Modify attributes of an existing account, user, or QoS.
    Account options:
      where Name=<account>         Specify the account to modify
      set Description=<desc>       Set account description
      set DefaultQos=<qos>         Set default QoS
      set AllowedPartition+=<part1,part2,...>   Add allowed partitions
      set AllowedQos+=<qos1,qos2,...>          Add allowed QoS
      set AllowedPartition-=<part1,part2,...> Delete allowed partitions
      set AllowedQos-=<qos1,qos2,...>        Delete allowed QoS
      set AllowedPartition=<part1,part2,...>      Set allowed partitions directly
      set AllowedQos=<qos1,qos2,...>              Set allowed QoS directly
    Account partition resource limit options (partition must be specified in where clause):
      where Name=<account> Partition=<partition>
      set MaxJobs=<num>              Set max running jobs for the account in the partition
      set MaxSubmitJobs=<num>        Set max submit jobs for the account in the partition
      set MaxTres=<tres>             Set max Tres for the account in the partition (format: cpu:<num>,mem:<num>)
      set MaxTresPerJob=<tres>       Set max Tres per job for the account in the partition
      set MaxWall=<sec>              Set max wall time for the account in the partition (seconds)
      set MaxWallPerJob=<sec>        Set max wall time per job for the account in the partition (seconds)
    User options:
      where Name=<user> [Account=<account>] [Partition=<part1,part2,...>]
      set AdminLevel=<level>         Set user admin level
      set DefaultAccount=<account>   Set default account
      set DefaultQos=<qos>           Set default QoS
      set AllowedPartition+=...    Add allowed partitions
      set AllowedQos+=...          Add allowed QoS
      set AllowedPartition-=... Delete allowed partitions
      set AllowedQos-=...       Delete allowed QoS
      set AllowedPartition=...       Set allowed partitions directly
      set AllowedQos=...             Set allowed QoS directly
    User partition resource limit options (partition must be specified in where clause):
      where Name=<user> [Account=<account>] Partition=<partition>
      set MaxJobs=<num>              Set max running jobs for the user in the partition
      set MaxSubmitJobs=<num>        Set max submit jobs for the user in the partition
      set MaxTres=<tres>             Set max Tres for the user in the partition (format: cpu:<num>,mem:<num>)
      set MaxTresPerJob=<tres>       Set max Tres per job for the user in the partition
      set MaxWall=<sec>              Set max wall time for the user in the partition (seconds)
      set MaxWallPerJob=<sec>        Set max wall time per job for the user in the partition (seconds)
    QoS options:
      where Name=<qos>
      set Description=<desc>             Set description
      set MaxCpusPerUser=<num>           Set max CPUs per user
      set MaxJobsPerUser=<num>           Set max jobs per user
      set MaxSubmitJobsPerUser=<num>     Set max submit jobs per user
      set MaxTresPerUser=<tres>          Set max Tres per user (format: cpu:<num>,mem:<num>,gres/type[:name]:num)
      set MaxJobsPerAccount=<num>        Set max jobs per account
      set MaxSubmitJobsPerAccount=<num>  Set max submit jobs per account
      set MaxTresPerAccount=<tres>       Set max Tres per account (format: cpu:<num>,mem:<num>,gres/type[:name]:num)
      set MaxSubmitJobs=<num>            Set max submit jobs
      set MaxJobs=<num>                  Set max jobs
      set MaxTres=<tres>                 Set max Tres (format: cpu:<num>,mem:<num>,gres/type[:name]:num)
      set MaxWall=<sec>                  Set max wall
      set Flags=<DenyOnLimit|None>       Set qos flags (default is None)
      set MaxTimeLimitPerJob=<duration|sec>   Set the maximum time limit per job (format: \"day-hours:minutes:seconds\" 5-0:0:1 for 5 days, 1 second or \"hours:minutes:seconds\" 10:1:2 for 10 hours, 1 minute, 2 seconds) (default 315576000000 seconds)"
      set Priority=<priority>            Set priority
      set Preempt=<qos1,qos2,...>       Replace the preemptible QoS list (use empty value to clear)
      set PreemptMode=<mode>            Set preemption mode (OFF, CANCEL; default OFF)
    Wckey options:
      where user=<name> 
      set defaultwckey=<wckeyname>    Set default wckey for specific users
    Resource options:
      where Name=<name>, Server=<server> [Cluster=<cluster1,cluster2,..>]
      set Description=<desc>         Set description
      set ServerType=<desc>          Set server type for specific users
      set Type=<notset|license>      Set license resource type
      set Count=<num>                Set quantity of resources
      set LastConsumed=<num>         Set external usage count
      set Flags=<none|absolute>      Set license resource flag
      set Allowed=<num>              Set amount allowed for the cluster (cluster must be specified)

  GLOBAL OPTIONS:
	--help, -h              Display this help message
	--config, -C            Specify config file path (default: /etc/crane/config.yaml)
	--plugin-config, -p     Specify plugin config file path (default: /etc/crane/plugin.yaml)
	--json, -J              Format output as JSON
	--version, -v           Display program version
	--force, -f             Force operation without confirmation
	--partition-limit, -P   Display partition resource limits (for show account/user)

  NOTE: Parameters in [] are optional. Parameters in <> should be replaced with actual values.
`, commandName, util.Version())
	fmt.Println(help)
}
