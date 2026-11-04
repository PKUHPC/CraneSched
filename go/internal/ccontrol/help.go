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

package ccontrol

import (
	"CraneFrontEnd/internal/util"
	"fmt"
)

func showHelp() {
	commandName := "ccontrol"
	traceTargets := "online craned nodes"
	if util.IsSlurmOutputMode() {
		commandName = "scontrol"
		traceTargets = "online nodes"
	}
	help := fmt.Sprintf(`CraneSched Control Tool (%s) - version %s

USAGE: %s <ACTION> <ENTITIES> [OPTIONS]

ACTIONS:
  show      - Display information about entities
  update    - Modify attributes of entities
  hold      - Hold entities
  release   - Release previously held entities
  create    - Create entities
  delete    - Delete entities

ENTITIES:
  node        - Compute nodes
  partition   - Node partitions
  job         - Jobs
  reservation - Reservations
  lic         - Licenses
  hostnames   - Expand a hostlist expression
  trace       - Runtime tracing configuration
  config      - Configuration file contents

COMMANDS:
  show node [<nodename>]
    Show information about compute nodes.
    If no node name is specified, information for all nodes will be displayed.

  show partition [<partition>]
    Show information about partitions.
    If no partition name is specified, information for all partitions will be displayed.

  show job [jobid]
    Show information about jobs.
    If no job ID is specified, information for all jobs will be displayed.

  show reservation [<reservationName>]
    Show information about reservations.
    If no reservation name is specified, information for all reservations will be displayed.
  
  show lic [<licenseName>]
   Show information about licenses.
   if no license name is specified, information for all licenses will be displayed.

  show hostnames [<hostlist>]
    Expand a hostlist expression and print one hostname per line.
    If omitted, use SLURM_JOB_NODELIST, SLURM_NODELIST, CRANE_JOB_NODELIST, or CRANE_NODELIST when set; otherwise print all configured node hostnames.

  show trace
    Show runtime tracing configuration and the compiled tracing limit.

  show config
    Show the contents of the configuration file.

  update nodeName=<nodename> state=<state> [reason=<reason>]
    Update attributes of a node.
    state: Valid states are 'drain' or 'resume'
    reason: Required when setting state to 'drain'

  update jobid=<jobid> [priority=<priority>] [timelimit=<timelimit>] [comment=<comment>] [mailuser=<mailuser>] [mailtype=<mailtype>] [deadline=<deadline>]
    Update attributes of a job.
    job/jobid: ID of the job to update
    priority: New priority value
    timelimit: New time limit for the job
    comment: New comment for the job
    mailuser: New mailuser for the job
    mailtype: New mailtype for the job
    deadline: New deadline for the job

  update partitionName=<partition> [accounts=<accounts>] [deniedaccounts=<accounts>]
    Update partition attributes.
    accounts: List of accounts allowed to use the partition
    deniedaccounts: List of accounts denied from using the partition

  update trace [--enabled true|false] [--level basic|detailed|debug] [--no-propagate]
    Update runtime tracing config on ctld and, by default, %s.

  hold <jobid> [timelimit=<duration>]
    Hold specified job(s).
    timelimit: Duration to hold the job (e.g., 1:00:00 for 1 hour)

  release <jobid>
    Release a previously held job.

  suspend <jobid>
    Suspend specified running job(s) by freezing their processes while keeping allocated resources.

  resume <jobid>
    Resume previously suspended job(s).

  create reservation <name> [startTime=<time>] [duration=<duration>] [partition=<partition>]
                    [nodes=<nodelist>] [account=<account>] [user=<username>]
    Create a new reservation.
    name: Name of the reservation
    startTime: Time when reservation starts
    duration: Length of reservation
    partition: Partition to reserve
    nodes: List of nodes to reserve
    account: Account to associate with the reservation
    user: User to associate with the reservation
    nodeCnt: Number of nodes to reserve (valid when nodes is not specified)

  delete reservation <name>
    Delete an existing reservation.

GLOBAL OPTIONS:
  --help, -h     Display this help message
  --version, -v  Show version information
  --json, -J     Format output as JSON
  --config, -C   Specify an alternative configuration file
  
`, commandName, util.Version(), commandName, traceTargets)
	fmt.Println(help)
}
