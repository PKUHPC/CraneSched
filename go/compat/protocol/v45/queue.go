// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package v45

// Slurm 26.05.4 queue report types in wire order, including schema-only fields.

type QueueJobDerivedExitCodeSignal struct {
	ID   Number[Int32] `json:"id" description:"Signal sent to process (numeric) (16 bit integer number with flags)"`
	Name string        `json:"name" description:"Signal sent to process (name)"`
}

type QueueJobDerivedExitCode struct {
	Status     []string                      `json:"status" description:"Status given by return code" enum:"INVALID|PENDING|SUCCESS|ERROR|SIGNALED|CORE_DUMPED"`
	ReturnCode Number[Int32]                 `json:"return_code" description:"Process return code (numeric) (32 bit integer number with flags)"`
	Signal     QueueJobDerivedExitCodeSignal `json:"signal"`
}

func (QueueJobDerivedExitCode) SchemaRequired() []string { return []string{} }

type QueueJobJobResourcesNodesAllocationCPUs struct {
	Count Int32 `json:"count" description:"Total number of CPUs assigned to job"`
	Used  Int32 `json:"used" description:"Total number of CPUs used by job"`
}

type QueueJobJobResourcesNodesAllocationMemory struct {
	Used      uint64 `json:"used" description:"Total memory (MiB) used by job"`
	Allocated uint64 `json:"allocated" description:"Total memory (MiB) allocated to job"`
}

type QueueJobJobResourcesNodesAllocationSocketsCores struct {
	Index  Int32    `json:"index" description:"Core index"`
	Status []string `json:"status" description:"Core status" enum:"INVALID|UNALLOCATED|ALLOCATED|IN_USE"`
}

func (QueueJobJobResourcesNodesAllocationSocketsCores) SchemaRequired() []string {
	return []string{"index", "status"}
}

type QueueJobJobResourcesNodesAllocationSockets struct {
	Index Int32                                             `json:"index" description:"Core index"`
	Cores []QueueJobJobResourcesNodesAllocationSocketsCores `json:"cores" description:"Core in socket"`
}

func (QueueJobJobResourcesNodesAllocationSockets) SchemaRequired() []string {
	return []string{"index", "cores"}
}

type QueueJobJobResourcesNodesAllocation struct {
	Index   Int32                                        `json:"index" description:"Node index"`
	Name    string                                       `json:"name" description:"Node name"`
	CPUs    QueueJobJobResourcesNodesAllocationCPUs      `json:"cpus"`
	Memory  QueueJobJobResourcesNodesAllocationMemory    `json:"memory"`
	Sockets []QueueJobJobResourcesNodesAllocationSockets `json:"sockets" description:"Socket allocations in node"`
}

func (QueueJobJobResourcesNodesAllocation) SchemaRequired() []string {
	return []string{"index", "name", "cpus/count", "cpus/used", "memory/used", "memory/allocated", "sockets"}
}

type QueueJobJobResourcesNodes struct {
	Count      Int32                                 `json:"count" description:"Number of allocated nodes"`
	SelectType []string                              `json:"select_type" description:"Node scheduling selection method" enum:"AVAILABLE|ONE_ROW|RESERVED"`
	List       string                                `json:"list" description:"Node(s) allocated to the job"`
	Whole      bool                                  `json:"whole" description:"Whether whole nodes were allocated"`
	Allocation []QueueJobJobResourcesNodesAllocation `json:"allocation" description:"Allocated node resources (Job resources for a node)"`
}

type QueueJobJobResources struct {
	SelectType     []string                  `json:"select_type" description:"Scheduler consumable resource selection type" enum:"NONE|CR_CPU_MEMORY|CR_CPU|CR_CORE_MEMORY|CR_CORE|CR_SOCKET_MEMORY|CR_SOCKET|CR_MEMORY|CR_ONE_TASK_PER_CORE|CR_CORE_DEFAULT_DIST_BLOCK|CR_NO_DIST_TOPO_BLOCK|CR_LLN|CR_PACK_NODES|LL_SHARED_GRES|MULTIPLE_SHARING_GRES_PJ|ENFORCE_BINDING_GRES|ONE_TASK_PER_SHARING_GRES"`
	Nodes          QueueJobJobResourcesNodes `json:"nodes"`
	CPUs           Int32                     `json:"cpus" description:"Number of allocated CPUs"`
	ThreadsPerCore Number[Int32]             `json:"threads_per_core" description:"Number of processor threads per CPU core (16 bit integer number with flags)"`
}

func (QueueJobJobResources) SchemaRequired() []string {
	return []string{"select_type", "nodes/count", "nodes/select_type", "nodes/list", "cpus", "threads_per_core", "nodes/whole", "nodes/allocation"}
}

type QueueJobPriorityByPartition struct {
	Partition string `json:"partition" description:"Partition name"`
	Priority  Int32  `json:"priority" description:"Prospective job priority if it runs in this partition"`
}

func (QueueJobPriorityByPartition) SchemaRequired() []string { return []string{} }

type QueueJobStepID struct {
	SLUID            string        `json:"sluid" description:"SLUID (Slurm Lexicographically-sortable Unique ID)"`
	JobID            Number[Int32] `json:"job_id" description:"Job ID (32 bit integer number with flags)"`
	StepHetComponent Number[Int32] `json:"step_het_component" description:"HetJob component (32 bit integer number with flags)"`
	StepID           string        `json:"step_id" description:"Job step ID"`
}

func (QueueJobStepID) SchemaRequired() []string { return []string{} }

type QueueJob struct {
	Account                  string                        `json:"account" description:"Account associated with the job"`
	AccrueTime               Number[uint64]                `json:"accrue_time" description:"When the job started accruing age priority (UNIX timestamp) (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)" report:"-"`
	AdminComment             string                        `json:"admin_comment" description:"Arbitrary comment made by administrator" report:"-"`
	AllocatingNode           string                        `json:"allocating_node" description:"Local node making the resource allocation"`
	ArrayJobID               Number[Int32]                 `json:"array_job_id" description:"Job ID of job array, or 0 if N/A (32 bit integer number with flags)"`
	ArrayTaskID              Number[Int32]                 `json:"array_task_id" description:"Task ID of this task in job array (32 bit integer number with flags)"`
	ArrayMaxTasks            Number[Int32]                 `json:"array_max_tasks" description:"Maximum number of simultaneously running array tasks, 0 if no limit (32 bit integer number with flags)"`
	ArrayTaskString          string                        `json:"array_task_string" description:"String expression of task IDs in this record"`
	AssociationID            Int32                         `json:"association_id" description:"Unique identifier for the association" report:"-"`
	BatchFeatures            string                        `json:"batch_features" description:"Features required for batch script's node" report:"-"`
	BatchFlag                bool                          `json:"batch_flag" description:"True if batch job"`
	BatchHost                string                        `json:"batch_host" description:"Name of host running batch script"`
	Flags                    []string                      `json:"flags" description:"Job flags" enum:"KILL_INVALID_DEPENDENCY|NO_KILL_INVALID_DEPENDENCY|HAS_STATE_DIRECTORY|TESTING_BACKFILL|GRES_BINDING_ENFORCED|TEST_NOW_ONLY|SEND_JOB_ENVIRONMENT|SPREAD_JOB|PREFER_MINIMUM_NODE_COUNT|JOB_KILL_HURRY|SKIP_TRES_STRING_ACCOUNTING|SIBLING_CLUSTER_UPDATE_ONLY|HETEROGENEOUS_JOB|EXACT_TASK_COUNT_REQUESTED|EXACT_CPU_COUNT_REQUESTED|TESTING_WHOLE_NODE_BACKFILL|TOP_PRIORITY_JOB|ACCRUE_COUNT_CLEARED|GRES_BINDING_DISABLED|JOB_WAS_RUNNING|JOB_ACCRUE_TIME_RESET|CRON_JOB|EXACT_MEMORY_REQUESTED|EXTERNAL_JOB|USING_DEFAULT_ACCOUNT|USING_DEFAULT_PARTITION|USING_DEFAULT_QOS|USING_DEFAULT_WCKEY|DEPENDENT|MAGNETIC|PARTITION_ASSIGNED|BACKFILL_ATTEMPTED|SCHEDULING_ATTEMPTED|STEPMGR_ENABLED|SPREAD_SEGMENTS|CONSOLIDATE_SEGMENTS|EXPEDITED_REQUEUE|JOB_IMPLICIT_MAX_NODES" report:"-"`
	BurstBuffer              string                        `json:"burst_buffer" description:"Burst buffer specifications" report:"-"`
	BurstBufferState         string                        `json:"burst_buffer_state" description:"Burst buffer state details" report:"-"`
	Cluster                  string                        `json:"cluster" description:"Cluster name"`
	ClusterFeatures          string                        `json:"cluster_features" description:"List of required cluster features" report:"-"`
	Command                  *string                       `json:"command" description:"Executed command"`
	Comment                  string                        `json:"comment" description:"Arbitrary comment"`
	Container                string                        `json:"container" description:"Absolute path to OCI container bundle" report:"-"`
	ContainerID              string                        `json:"container_id" description:"OCI container ID" report:"-"`
	ContainerType            string                        `json:"container_type" description:"Job container type" report:"-"`
	Contiguous               bool                          `json:"contiguous" description:"True if job requires contiguous nodes" report:"-"`
	CoreSpec                 Int32                         `json:"core_spec" description:"Specialized core count" report:"-"`
	ThreadSpec               Int32                         `json:"thread_spec" description:"Specialized thread count" report:"-"`
	CoresPerSocket           Number[Int32]                 `json:"cores_per_socket" description:"Cores per socket required (16 bit integer number with flags)" report:"-"`
	BillableTRES             *Number[float64]              `json:"billable_tres" description:"Billable TRES (64 bit floating point number with flags)"`
	CPUsPerTask              *Number[Int32]                `json:"cpus_per_task" description:"Number of CPUs required by each task (16 bit integer number with flags)"`
	CPUFrequencyMinimum      Number[Int32]                 `json:"cpu_frequency_minimum" description:"Minimum CPU frequency (32 bit integer number with flags)" report:"-"`
	CPUFrequencyMaximum      Number[Int32]                 `json:"cpu_frequency_maximum" description:"Maximum CPU frequency (32 bit integer number with flags)" report:"-"`
	CPUFrequencyGovernor     Number[Int32]                 `json:"cpu_frequency_governor" description:"CPU frequency governor (32 bit integer number with flags)" report:"-"`
	CPUsPerTRES              string                        `json:"cpus_per_tres" description:"Semicolon delimited list of TRES=# values indicating how many CPUs should be allocated for each specified TRES (currently only used for gres/gpu)" report:"-"`
	Cron                     string                        `json:"cron" description:"Time specification for scrontab job" report:"-"`
	Deadline                 Number[uint64]                `json:"deadline" description:"Latest time that the job may start (UNIX timestamp) (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)"`
	DelayBoot                Number[Int32]                 `json:"delay_boot" description:"Number of seconds after job eligible start that nodes will be rebooted to satisfy feature specification (32 bit integer number with flags)" report:"-"`
	Dependency               string                        `json:"dependency" description:"Other jobs that must meet certain criteria before this job can start"`
	DerivedExitCode          *QueueJobDerivedExitCode      `json:"derived_exit_code" description:"Highest exit code of all job steps (return code returned by process)"`
	EligibleTime             Number[uint64]                `json:"eligible_time" description:"Time when the job became eligible to run (UNIX timestamp) (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)" report:"-"`
	EndTime                  *Number[uint64]               `json:"end_time" description:"End time, real or expected (UNIX timestamp) (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)"`
	ExcludedNodes            string                        `json:"excluded_nodes" description:"Comma-separated list of nodes that may not be used"`
	ExitCode                 *QueueJobDerivedExitCode      `json:"exit_code" description:"Exit code of the job (return code returned by process)"`
	Extra                    string                        `json:"extra" description:"Arbitrary string used for node filtering if extra constraints are enabled" report:"-"`
	FailedNode               string                        `json:"failed_node" description:"Name of node that caused job failure" report:"-"`
	Features                 string                        `json:"features" description:"Comma-separated list of features that are required" report:"-"`
	FederationOrigin         string                        `json:"federation_origin" description:"Origin cluster's name (when using federation)" report:"-"`
	FederationSiblingsActive string                        `json:"federation_siblings_active" description:"Active sibling job names" report:"-"`
	FederationSiblingsViable string                        `json:"federation_siblings_viable" description:"Viable sibling job names" report:"-"`
	GresDetail               []string                      `json:"gres_detail" description:"List of GRES index and counts allocated per node" report:"-"`
	GroupID                  Int32                         `json:"group_id" description:"Group ID of the user that owns the job"`
	GroupName                string                        `json:"group_name" description:"Group name of the user that owns the job"`
	HetJobID                 Number[Int32]                 `json:"het_job_id" description:"Heterogeneous job ID, if applicable (32 bit integer number with flags)" report:"-"`
	HetJobIDSet              string                        `json:"het_job_id_set" description:"Job ID range for all heterogeneous job components" report:"-"`
	HetJobOffset             Number[Int32]                 `json:"het_job_offset" description:"Unique sequence number applied to this component of the heterogeneous job (32 bit integer number with flags)" report:"-"`
	JobID                    Int32                         `json:"job_id" description:"Job ID"`
	JobResources             QueueJobJobResources          `json:"job_resources" description:"Resources used by the job" report:"-"`
	JobSizeStr               []string                      `json:"job_size_str" description:"Number of nodes (in a range) required for this job" report:"-"`
	JobState                 []string                      `json:"job_state" description:"Current state" enum:"PENDING|RUNNING|SUSPENDED|COMPLETED|CANCELLED|FAILED|TIMEOUT|NODE_FAIL|PREEMPTED|BOOT_FAIL|DEADLINE|OUT_OF_MEMORY|LAUNCH_FAILED|REQUEUED|REQUEUE_HOLD|SPECIAL_EXIT|RESIZING|CONFIGURING|COMPLETING|STOPPED|RECONFIG_FAIL|POWER_UP_NODE|REVOKED|REQUEUE_FED|RESV_DEL_HOLD|SIGNALING|STAGE_OUT|EXPEDITING"`
	LastSchedEvaluation      Number[uint64]                `json:"last_sched_evaluation" description:"Last time job was evaluated for scheduling (UNIX timestamp) (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)" report:"-"`
	Licenses                 string                        `json:"licenses" description:"License(s) required by the job"`
	LicensesAllocated        string                        `json:"licenses_allocated" description:"License(s) allocated to the job"`
	MailType                 []string                      `json:"mail_type" description:"Mail event type(s)" enum:"BEGIN|END|FAIL|REQUEUE|TIME=100%|TIME=90%|TIME=80%|TIME=50%|STAGE_OUT|ARRAY_TASKS|INVALID_DEPENDENCY" report:"-"`
	MailUser                 string                        `json:"mail_user" description:"User to receive email notifications" report:"-"`
	MaxCPUs                  Number[Int32]                 `json:"max_cpus" description:"Maximum number of CPUs usable by the job (32 bit integer number with flags)"`
	MaxNodes                 *Number[Int32]                `json:"max_nodes" description:"Maximum number of nodes usable by the job (32 bit integer number with flags)"`
	McsLabel                 string                        `json:"mcs_label" description:"Multi-Category Security label on the job" report:"-"`
	MemoryPerTRES            string                        `json:"memory_per_tres" description:"Semicolon delimited list of TRES=# values indicating how much memory in megabytes should be allocated for each specified TRES (currently only used for gres/gpu)" report:"-"`
	MemoryUpdateDelay        Int32                         `json:"memory_update_delay" description:"Auto-reduce memory delay in minutes" report:"-"`
	MemoryUpdateMargin       Int32                         `json:"memory_update_margin" description:"Auto-reduce memory margin percent" report:"-"`
	Name                     string                        `json:"name" description:"Job name"`
	Network                  string                        `json:"network" description:"Network specs for the job" report:"-"`
	Nodes                    string                        `json:"nodes" description:"Node(s) allocated to the job"`
	Nice                     Int32                         `json:"nice" description:"Requested job priority change" report:"-"`
	TasksPerCore             Number[Int32]                 `json:"tasks_per_core" description:"Number of tasks invoked on each core (16 bit integer number with flags)" report:"-"`
	TasksPerTRES             Number[Int32]                 `json:"tasks_per_tres" description:"Number of tasks that can assess each GPU (16 bit integer number with flags)" report:"-"`
	TasksPerNode             *Number[Int32]                `json:"tasks_per_node" description:"Number of tasks invoked on each node (16 bit integer number with flags)"`
	TasksPerSocket           Number[Int32]                 `json:"tasks_per_socket" description:"Number of tasks invoked on each socket (16 bit integer number with flags)" report:"-"`
	TasksPerBoard            Number[Int32]                 `json:"tasks_per_board" description:"Number of tasks invoked on each board (16 bit integer number with flags)" report:"-"`
	CPUs                     *Number[Int32]                `json:"cpus" description:"Minimum number of CPUs required (32 bit integer number with flags)"`
	NodeCount                Number[Int32]                 `json:"node_count" description:"Minimum number of nodes required (32 bit integer number with flags)"`
	Tasks                    *Number[Int32]                `json:"tasks" description:"Number of tasks (32 bit integer number with flags)"`
	Partition                string                        `json:"partition" description:"Partition assigned to the job"`
	Prefer                   string                        `json:"prefer" description:"Feature(s) the job requested but that are not required" report:"-"`
	MemoryPerCPU             *Number[uint64]               `json:"memory_per_cpu" description:"Minimum memory in megabytes per allocated CPU (64 bit integer number with flags)"`
	MemoryPerNode            *Number[uint64]               `json:"memory_per_node" description:"Minimum memory in megabytes per allocated node (64 bit integer number with flags)"`
	MinimumCPUsPerNode       *Number[Int32]                `json:"minimum_cpus_per_node" description:"Minimum number of CPUs per node (16 bit integer number with flags)"`
	MinimumTmpDiskPerNode    Number[Int32]                 `json:"minimum_tmp_disk_per_node" description:"Minimum tmp disk space required per node (32 bit integer number with flags)" report:"-"`
	PreemptTime              Number[uint64]                `json:"preempt_time" description:"Time job received preemption signal (UNIX timestamp) (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)" report:"-"`
	PreemptableTime          Number[uint64]                `json:"preemptable_time" description:"Time job becomes eligible for preemption (UNIX timestamp) (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)" report:"-"`
	PreSusTime               Number[uint64]                `json:"pre_sus_time" description:"Total run time prior to last suspend in seconds (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)" report:"-"`
	Hold                     bool                          `json:"hold" description:"Hold (true) or release (false) job (Job held)"`
	Priority                 Number[Int32]                 `json:"priority" description:"Request specific job priority (32 bit integer number with flags)"`
	PriorityByPartition      []QueueJobPriorityByPartition `json:"priority_by_partition" description:"Prospective job priority in each partition that may be used by this job" report:"-"`
	Profile                  []string                      `json:"profile" description:"Profile used by the acct_gather_profile plugin" enum:"NOT_SET|NONE|ENERGY|LUSTRE|NETWORK|TASK" report:"-"`
	QoS                      string                        `json:"qos" description:"Quality of Service assigned to the job, if pending the QOS requested"`
	Reboot                   bool                          `json:"reboot" description:"Node reboot requested before start" report:"-"`
	RequiredNodes            string                        `json:"required_nodes" description:"Comma-separated list of required nodes"`
	RequiredSwitches         Int32                         `json:"required_switches" description:"Maximum number of switches" report:"-"`
	Requeue                  *bool                         `json:"requeue" description:"Determines whether the job may be requeued"`
	ResizeTime               Number[uint64]                `json:"resize_time" description:"Time of last size change (UNIX timestamp) (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)" report:"-"`
	RestartCnt               Int32                         `json:"restart_cnt" description:"Number of job restarts"`
	ResvName                 string                        `json:"resv_name" description:"Name of reservation to use"`
	ScheduledNodes           string                        `json:"scheduled_nodes" description:"List of nodes scheduled to be used for the job"`
	SegmentSize              Int32                         `json:"segment_size" description:"Requested segment size" report:"-"`
	SelinuxContext           string                        `json:"selinux_context" description:"SELinux context" report:"-"`
	StepID                   QueueJobStepID                `json:"step_id" description:"Job step ID" report:"-"`
	SocketsPerBoard          Int32                         `json:"sockets_per_board" description:"Number of sockets per board required" report:"-"`
	SocketsPerNode           Number[Int32]                 `json:"sockets_per_node" description:"Number of sockets per node required (16 bit integer number with flags)" report:"-"`
	StartTime                *Number[uint64]               `json:"start_time" description:"Time execution began, or is expected to begin (UNIX timestamp) (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)"`
	StateDescription         string                        `json:"state_description" description:"Optional details for state_reason" report:"-"`
	StateReason              string                        `json:"state_reason" description:"Reason for current Pending or Failed state"`
	StandardInput            string                        `json:"standard_input" description:"Path to stdin file"`
	StandardOutput           string                        `json:"standard_output" description:"Path to stdout file"`
	StandardError            string                        `json:"standard_error" description:"Path to stderr file"`
	StdinExpanded            string                        `json:"stdin_expanded" description:"Job stdin with expanded fields"`
	StdoutExpanded           string                        `json:"stdout_expanded" description:"Job stdout with expanded fields"`
	StderrExpanded           string                        `json:"stderr_expanded" description:"Job stderr with expanded fields"`
	SubmitTime               *Number[uint64]               `json:"submit_time" description:"Time when the job was submitted (UNIX timestamp) (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)"`
	SubmitLine               string                        `json:"submit_line" description:"Job submit line (e.g. 'sbatch -N3 job.sh job_arg'"`
	SuspendTime              Number[uint64]                `json:"suspend_time" description:"Time the job was last suspended or resumed (UNIX timestamp) (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)" report:"-"`
	SystemComment            string                        `json:"system_comment" description:"Arbitrary comment from slurmctld" report:"-"`
	TimeLimit                Number[Int32]                 `json:"time_limit" description:"Maximum run time in minutes (32 bit integer number with flags)"`
	TimeMinimum              Number[Int32]                 `json:"time_minimum" description:"Minimum run time in minutes (32 bit integer number with flags)" report:"-"`
	ThreadsPerCore           Number[Int32]                 `json:"threads_per_core" description:"Number of processor threads per CPU core required (16 bit integer number with flags)" report:"-"`
	TRESBind                 string                        `json:"tres_bind" description:"Task to TRES binding directives" report:"-"`
	TRESFreq                 string                        `json:"tres_freq" description:"TRES frequency directives" report:"-"`
	TRESPerJob               string                        `json:"tres_per_job" description:"Comma-separated list of TRES=# values to be allocated per job" report:"-"`
	TRESPerNode              string                        `json:"tres_per_node" description:"Comma-separated list of TRES=# values to be allocated per node"`
	TRESPerSocket            string                        `json:"tres_per_socket" description:"Comma-separated list of TRES=# values to be allocated per socket" report:"-"`
	TRESPerTask              *string                       `json:"tres_per_task" description:"Comma-separated list of TRES=# values to be allocated per task"`
	TRESReqStr               *string                       `json:"tres_req_str" description:"TRES requested by the job"`
	TRESAllocStr             *string                       `json:"tres_alloc_str" description:"TRES used by the job"`
	UserID                   Int32                         `json:"user_id" description:"User ID that owns the job"`
	UserName                 string                        `json:"user_name" description:"User name that owns the job"`
	MaximumSwitchWaitTime    Int32                         `json:"maximum_switch_wait_time" description:"Maximum time to wait for switches in seconds" report:"-"`
	Wckey                    string                        `json:"wckey" description:"Workload characterization key"`
	CurrentWorkingDirectory  string                        `json:"current_working_directory" description:"Working directory to use for the job"`
}

func (QueueJob) SchemaRequired() []string { return []string{} }

type QueueResponse struct {
	Jobs         []QueueJob     `json:"jobs" description:"List of jobs"`
	LastBackfill Number[uint64] `json:"last_backfill" description:"Time of last backfill scheduler run (UNIX timestamp) (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)" report:"-"`
	LastUpdate   Number[uint64] `json:"last_update" description:"Time of last job change (UNIX timestamp) (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)" report:"-"`
	Meta         Meta           `json:"meta" description:"Slurm meta values"`
	Errors       []Error        `json:"errors" description:"Query errors"`
	Warnings     []Warning      `json:"warnings" description:"Query warnings"`
}

func (QueueResponse) SchemaRequired() []string {
	return []string{"jobs", "last_backfill", "last_update"}
}

type QueueStepArray struct {
	JobID  Int32  `json:"job_id" description:"Job ID of job array, or 0 if N/A"`
	TaskID *Int32 `json:"task_id" description:"Task ID of this task in job array"`
}

type QueueStepCPUFrequency struct {
	Min      Number[Int32] `json:"min" description:"Minimum CPU frequency (32 bit integer number with flags)"`
	Max      Number[Int32] `json:"max" description:"Maximum CPU frequency (32 bit integer number with flags)"`
	Governor Number[Int32] `json:"governor" description:"CPU frequency governor (32 bit integer number with flags)"`
}

type QueueStepCPU struct {
	Frequency QueueStepCPUFrequency `json:"frequency"`
}

type QueueStepTRESPer struct {
	CPU    string  `json:"cpu" description:"Semicolon delimited list of TRES=# values indicating how many CPUs should be allocated for each specified TRES (currently only used for gres/gpu)" report:"-"`
	Memory string  `json:"memory" description:"Semicolon delimited list of TRES=# values indicating how much memory should be allocated for each specified TRES (currently only used for gres/gpu)" report:"-"`
	Step   string  `json:"step" description:"Comma-separated list of TRES=# values to be allocated per job step" report:"-"`
	Node   string  `json:"node" description:"Comma-separated list of TRES=# values to be allocated per node"`
	Socket string  `json:"socket" description:"Comma-separated list of TRES=# values to be allocated per socket" report:"-"`
	Task   *string `json:"task" description:"Comma-separated list of TRES=# values to be allocated per task"`
}

type QueueStepTRES struct {
	Per        QueueStepTRESPer `json:"per"`
	Allocation *string          `json:"allocation" description:"Trackable resources allocated to the step"`
	Binding    string           `json:"binding" description:"Task to TRES binding directives" report:"-"`
	Frequency  string           `json:"frequency" description:"TRES frequency directive" report:"-"`
}

type QueueStepTime struct {
	Running Number[uint64]  `json:"running" description:"Total run time in seconds (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)"`
	Start   *Number[uint64] `json:"start" description:"Start time (UNIX timestamp) (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)"`
	Limit   Number[Int32]   `json:"limit" description:"Maximum run time in minutes (32 bit integer number with flags)"`
}

type QueueStepSrun struct {
	Host string `json:"host" description:"Host of srun command"`
	Pid  Int32  `json:"pid" description:"PID of srun command"`
}

type QueueStepTask struct {
	Distribution string `json:"distribution" description:"Layout"`
}

type QueueStep struct {
	Array          QueueStepArray `json:"array"`
	Cluster        string         `json:"cluster" description:"Cluster name"`
	Container      string         `json:"container" description:"Absolute path to OCI container bundle" report:"-"`
	ContainerID    string         `json:"container_id" description:"OCI container ID" report:"-"`
	ContainerType  string         `json:"container_type" description:"Job container type" report:"-"`
	CPU            QueueStepCPU   `json:"cpu" report:"-"`
	TRES           QueueStepTRES  `json:"tres"`
	Name           string         `json:"name" description:"Job step name"`
	Network        string         `json:"network" description:"Network specs for the job step" report:"-"`
	Nodes          string         `json:"nodes" description:"Node(s) allocated to the job step"`
	NumberCPUs     *Int32         `json:"number_cpus" description:"Number of CPUs used by the step"`
	NumberTasks    *Int32         `json:"number_tasks" description:"Number of tasks"`
	Partition      string         `json:"partition" description:"Partition assigned to the job step"`
	ReservedPorts  string         `json:"reserved_ports" description:"Ports allocated for MPI" report:"-"`
	Time           QueueStepTime  `json:"time"`
	Srun           QueueStepSrun  `json:"srun" report:"-"`
	State          []string       `json:"state" description:"Current state" enum:"PENDING|RUNNING|SUSPENDED|COMPLETED|CANCELLED|FAILED|TIMEOUT|NODE_FAIL|PREEMPTED|BOOT_FAIL|DEADLINE|OUT_OF_MEMORY|LAUNCH_FAILED|REQUEUED|REQUEUE_HOLD|SPECIAL_EXIT|RESIZING|CONFIGURING|COMPLETING|STOPPED|RECONFIG_FAIL|POWER_UP_NODE|REVOKED|REQUEUE_FED|RESV_DEL_HOLD|SIGNALING|STAGE_OUT|EXPEDITING"`
	SLUID          string         `json:"sluid" description:"SLUID (Slurm Lexicographically-sortable Unique ID)" report:"-"`
	ID             string         `json:"id" description:"Step ID (Slurm job step ID)"`
	Stderr         string         `json:"stderr" description:"Step stderr file path"`
	Stdin          string         `json:"stdin" description:"Step stdin file path"`
	Stdout         string         `json:"stdout" description:"Step stdout file path"`
	StdinExpanded  string         `json:"stdin_expanded" description:"Step stdin with expanded fields"`
	StdoutExpanded string         `json:"stdout_expanded" description:"Step stdout with expanded fields"`
	StderrExpanded string         `json:"stderr_expanded" description:"Step stderr with expanded fields"`
	SubmitLine     *string        `json:"submit_line" description:"Full command used to submit the step"`
	Task           QueueStepTask  `json:"task" report:"-"`
	User           string         `json:"user" description:"User ID that owns the step"`
}

func (QueueStep) SchemaRequired() []string { return []string{} }

type QueueStepsResponse struct {
	Steps      []QueueStep    `json:"steps" description:"List of steps"`
	LastUpdate Number[uint64] `json:"last_update" description:"Time of last licenses change (UNIX timestamp) (UNIX timestamp or time string recognized by Slurm (e.g., '[MM/DD[/YY]-]HH:MM[:SS]'); may also be infinite or unset) (64 bit integer number with flags)" report:"-"`
	Meta       Meta           `json:"meta" description:"Slurm meta values"`
	Errors     []Error        `json:"errors" description:"Query errors"`
	Warnings   []Warning      `json:"warnings" description:"Query warnings"`
}

func (QueueStepsResponse) SchemaRequired() []string { return []string{"steps", "last_update"} }

type QueueJobState struct {
	JobID string   `json:"job_id" description:"Job ID"`
	State []string `json:"state" description:"Job state" enum:"PENDING|RUNNING|SUSPENDED|COMPLETED|CANCELLED|FAILED|TIMEOUT|NODE_FAIL|PREEMPTED|BOOT_FAIL|DEADLINE|OUT_OF_MEMORY|LAUNCH_FAILED|REQUEUED|REQUEUE_HOLD|SPECIAL_EXIT|RESIZING|CONFIGURING|COMPLETING|STOPPED|RECONFIG_FAIL|POWER_UP_NODE|REVOKED|REQUEUE_FED|RESV_DEL_HOLD|SIGNALING|STAGE_OUT|EXPEDITING"`
}

func (QueueJobState) SchemaRequired() []string { return []string{"job_id", "state"} }

type QueueStatesResponse struct {
	Jobs     []QueueJobState `json:"jobs" description:"List of job states (List of jobs)"`
	Meta     Meta            `json:"meta" description:"Slurm meta values"`
	Errors   []Error         `json:"errors" description:"Query errors"`
	Warnings []Warning       `json:"warnings" description:"Query warnings"`
}

func (QueueStatesResponse) SchemaRequired() []string { return []string{} }
