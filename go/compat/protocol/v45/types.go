// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package v45

// These v0.0.45 types define both the data report and its runtime schema.
// Declaration order is output order. Descriptions and enums are public interface
// metadata. Removed fields are deliberately omitted from both representations.
// An int32 schema annotation must not truncate a Crane resource count.
type Int32 uint64

// SchemaFormat annotates the wire schema without narrowing resource counts.
func (Int32) SchemaFormat() string { return "int32" }

type Number[T ~uint64] struct {
	Set      bool `json:"set" description:"True if number has been set; False if number is unset"`
	Infinite bool `json:"infinite" description:"True if number has been set to infinite; \"set\" and \"number\" will be ignored"`
	Number   T    `json:"number" description:"If \"set\" is True the number will be set with value; otherwise ignore number contents"`
}

func (Number[T]) SchemaRequired() []string { return []string{} }

func (n Number[T]) ComplexValue() any {
	if n.Infinite {
		return "Infinity"
	}
	if !n.Set {
		return nil
	}
	return uint64(n.Number)
}

func Numeric[T ~uint64](value uint64, unset, infinite bool) Number[T] {
	if unset || infinite {
		value = 0
	}
	return Number[T]{Set: !unset && !infinite, Infinite: infinite, Number: T(value)}
}

type SinfoNode struct {
	State []string `json:"state" description:"Node state(s)" enum:"INVALID|UNKNOWN|DOWN|IDLE|ALLOCATED|ERROR|MIXED|FUTURE|EXTERNAL|RESERVED|UNDRAIN|CLOUD|RESUME|DRAIN|COMPLETING|NOT_RESPONDING|POWERED_DOWN|FAIL|POWERING_UP|MAINTENANCE|REBOOT_REQUESTED|REBOOT_CANCELED|POWERING_DOWN|DYNAMIC_FUTURE|REBOOT_ISSUED|PLANNED|INVALID_REG|POWER_DOWN|POWER_UP|POWER_DRAIN|POWER_DOWN_ASAP|POWER_DOWN_FORCE|DYNAMIC_NORM|BLOCKED"`
}

type SinfoNodes struct {
	Allocated Int32    `json:"allocated" description:"Number of nodes that are allocated"`
	Idle      Int32    `json:"idle" description:"Number of nodes that are idle"`
	Other     Int32    `json:"other" description:"Number of nodes that are not allocated or idle"`
	Total     Int32    `json:"total" description:"Total number of nodes"`
	Hostnames []string `json:"hostnames" description:"NodeHost - List of node hostnames"`
	Addresses []string `json:"addresses" description:"NodeAddr - List of node communication addresses"`
	Nodes     []string `json:"nodes" description:"NodeList - List of node names"`
}

type SinfoCPUsLoad struct {
	Minimum Int32 `json:"minimum" description:"Minimum CPUsLoad on any single node"`
	Maximum Int32 `json:"maximum" description:"Maximum CPUsLoad on any single node"`
}

type SinfoCPUsPerNode struct {
	Max Number[Int32] `json:"max" description:"MaxCPUsPerNode - Maximum number of CPUs on any node available to all jobs from this partition (32 bit integer number with flags)"`
}

type SinfoCPUs struct {
	Allocated Int32            `json:"allocated" description:"Number of allocated CPUs"`
	Idle      Int32            `json:"idle" description:"Number of idle CPUs"`
	Other     Int32            `json:"other" description:"Number of CPUs that are not allocated or idle"`
	Total     Int32            `json:"total" description:"Total number of CPUs"`
	Minimum   Int32            `json:"minimum" description:"Minimum number of CPUs on any single node"`
	Maximum   Int32            `json:"maximum" description:"Maximum number of CPUs on any single node"`
	Load      SinfoCPUsLoad    `json:"load"`
	PerNode   SinfoCPUsPerNode `json:"per_node"`
}

type SinfoSockets struct {
	Minimum Int32 `json:"minimum" description:"Minimum number of sockets on any single node"`
	Maximum Int32 `json:"maximum" description:"Maximum number of sockets on any single node"`
}

type SinfoCores struct {
	Minimum Int32 `json:"minimum" description:"Minimum number of cores on any single node"`
	Maximum Int32 `json:"maximum" description:"Maximum number of cores on any single node"`
}

type SinfoThreads struct {
	Minimum Int32 `json:"minimum" description:"Minimum number of threads per core on any single node"`
	Maximum Int32 `json:"maximum" description:"Maximum number of threads per core on any single node"`
}

type SinfoDisk struct {
	Minimum Int32 `json:"minimum" description:"Minimum TMP_DISK on any single node"`
	Maximum Int32 `json:"maximum" description:"Maximum TMP_DISK on any single node"`
}

type SinfoMemoryFree struct {
	Minimum Number[uint64] `json:"minimum" description:"Minimum FreeMem in megabytes on any single node (64 bit integer number with flags)"`
	Maximum Number[uint64] `json:"maximum" description:"Maximum FreeMem in megabytes on any single node (64 bit integer number with flags)"`
}

type SinfoMemory struct {
	Minimum   uint64          `json:"minimum" description:"Minimum memory in megabytes on any single node"`
	Maximum   uint64          `json:"maximum" description:"Maximum memory in megabytes on any single node"`
	Free      SinfoMemoryFree `json:"free"`
	Allocated uint64          `json:"allocated" description:"AllocMem - Total memory in megabytes allocated to jobs"`
}

type SinfoWeight struct {
	Minimum Int32 `json:"minimum" description:"Minimum Weight on any single node"`
	Maximum Int32 `json:"maximum" description:"Maximum Weight on any single node"`
}

type SinfoFeatures struct {
	Total  string `json:"total" description:"Features - Features available on the nodes"`
	Active string `json:"active" description:"features_act - Features currently active on the nodes"`
}

type SinfoGRES struct {
	Total string `json:"total" description:"Gres - Generic resources configured on these nodes"`
	Used  string `json:"used" description:"GresUsed - Generic resources allocated to jobs"`
}

type SinfoReason struct {
	Description string `json:"description" description:"Why a node is unavailable"`
	Time        int64  `json:"time" description:"When the reason was set (UNIX timestamp)"`
}

type SinfoResourceSpec struct {
	CPUs   string `json:"cpus" description:"CpuSpecList - Comma-separated list of Slurm abstract CPU IDs reserved for system use"`
	Memory uint64 `json:"memory" description:"MemSpecLimit - Amount of RealMemory in megabytes reserved for system use"`
}

type PartitionNodes struct {
	AllowedAllocation string `json:"allowed_allocation" description:"AllocNodes - Comma-separated list of nodes from which users can submit jobs in the partition"`
	Configured        string `json:"configured" description:"Nodes - Comma-separated list of nodes which are associated with this partition"`
	Total             Int32  `json:"total" description:"TotalNodes - Number of nodes available in this partition (read-only)"`
}

type PartitionAccounts struct {
	Allowed string `json:"allowed" description:"AllowAccounts - Comma-separated list of accounts which may execute jobs in the partition"`
	Deny    string `json:"deny" description:"DenyAccounts - Comma-separated list of accounts which may not execute jobs in the partition"`
}

type PartitionGroups struct {
	Allowed string `json:"allowed" description:"AllowGroups - Comma-separated list of group names which may execute jobs in this partition"`
}

type PartitionQoS struct {
	Allowed  string `json:"allowed" description:"AllowQOS - Comma-separated list of Qos which may execute jobs in the partition"`
	Deny     string `json:"deny" description:"DenyQOS - Comma-separated list of Qos which may not execute jobs in the partition"`
	Assigned string `json:"assigned" description:"QOS - QOS name containing limits that will apply to all jobs in this partition"`
}

type PartitionTRES struct {
	BillingWeights string `json:"billing_weights" description:"TRESBillingWeights - Billing weights of each tracked TRES type that will be used in calculating the usage of a job"`
	Configured     string `json:"configured" description:"TRES - Number of each applicable TRES type available in this partition (read-only)"`
}

type PartitionCPUs struct {
	TaskBinding []string `json:"task_binding" description:"CpuBind - Default method controlling how tasks are bound to allocated resources" enum:"CPU_BIND_TO_THREADS|CPU_BIND_TO_CORES|CPU_BIND_TO_SOCKETS|CPU_BIND_TO_LDOMS|CPU_BIND_NONE|CPU_BIND_OFF|VERBOSE"`
	Total       Int32    `json:"total" description:"TotalCPUs - Number of CPUs available in this partition (read-only)"`
}

type PartitionDefaults struct {
	PartitionMemoryPerCPU  Number[uint64] `json:"partition_memory_per_cpu" description:"DefMemPerCPU - Default real memory size available per allocated CPU in megabytes (64 bit integer number with flags)"`
	PartitionMemoryPerNode Number[uint64] `json:"partition_memory_per_node" description:"DefMemPerNode - Default real memory size available per allocated node in megabytes (64 bit integer number with flags)"`
	Time                   Number[Int32]  `json:"time" description:"DefaultTime - Run time limit in minutes used for jobs that don't specify a value (32 bit integer number with flags)"`
	Job                    string         `json:"job" description:"JobDefaults - Comma-separated list of job default values (this field is only used to set new defaults)"`
}

type PartitionState struct {
	Exclusive     string   `json:"exclusive" description:"Exclusive= string (same as scontrol show partition)"`
	Oversubscribe string   `json:"oversubscribe" description:"OverSubscribe display: FORCE:n, NO, or YES:n (same as show partition and sinfo) (Partition oversubscribe value)"`
	State         []string `json:"state" description:"Current state(s)" enum:"INACTIVE|UNKNOWN|UP|DOWN|DRAIN"`
}

type PartitionMaximums struct {
	CPUsPerNode            Number[Int32]  `json:"cpus_per_node" description:"MaxCPUsPerNode - Maximum number of CPUs on any node available to all jobs from this partition (32 bit integer number with flags)"`
	CPUsPerSocket          Number[Int32]  `json:"cpus_per_socket" description:"MaxCPUsPerSocket - Maximum number of CPUs on any node available on the all jobs from this partition (32 bit integer number with flags)"`
	PartitionMemoryPerCPU  Number[uint64] `json:"partition_memory_per_cpu" description:"MaxMemPerCPU - Maximum real memory size available per allocated CPU in megabytes (64 bit integer number with flags)"`
	PartitionMemoryPerNode Number[uint64] `json:"partition_memory_per_node" description:"MaxMemPerNode - Maximum real memory size available per allocated node in a job allocation in megabytes (64 bit integer number with flags)"`
	Nodes                  Number[Int32]  `json:"nodes" description:"MaxNodes - Maximum count of nodes which may be allocated to any single job (32 bit integer number with flags)"`
	Time                   Number[Int32]  `json:"time" description:"MaxTime - Maximum run time limit for jobs in minutes (32 bit integer number with flags)"`
	OverTimeLimit          Number[Int32]  `json:"over_time_limit" description:"OverTimeLimit - Number of minutes by which a job can exceed its time limit before being canceled (16 bit integer number with flags)"`
}

type PartitionMinimums struct {
	Nodes Int32 `json:"nodes" description:"MinNodes - Minimum count of nodes which may be allocated to any single job"`
}

type PartitionPriority struct {
	JobFactor Int32 `json:"job_factor" description:"PriorityJobFactor - Partition factor used by priority/multifactor plugin in calculating job priority"`
	Tier      Int32 `json:"tier" description:"PriorityTier - Controls the order in which the scheduler evaluates jobs from different partitions"`
}

type PartitionTimeouts struct {
	Resume  Number[Int32] `json:"resume" description:"ResumeTimeout - Resumed nodes which fail to respond in this time frame will be marked DOWN (read-only; GLOBAL if both set and infinite are false). (16 bit integer number with flags)"`
	Suspend Number[Int32] `json:"suspend" description:"SuspendTimeout - Maximum time permitted (in seconds) between when a node suspend request is issued and when the node is shutdown (read-only; GLOBAL if both set and infinite are false). (16 bit integer number with flags)"`
}

type PartitionInfo struct {
	Nodes       PartitionNodes    `json:"nodes"`
	Accounts    PartitionAccounts `json:"accounts"`
	Groups      PartitionGroups   `json:"groups"`
	QoS         PartitionQoS      `json:"qos"`
	Alternate   string            `json:"alternate" description:"Alternate - Partition name of alternate partition to be used if the state of this partition is DRAIN or INACTIVE"`
	TRES        PartitionTRES     `json:"tres"`
	Cluster     string            `json:"cluster" description:"Cluster name (read-only)"`
	SelectType  []string          `json:"select_type" description:"Scheduler consumable resource selection type (read-only)" enum:"NONE|CR_CPU_MEMORY|CR_CPU|CR_CORE_MEMORY|CR_CORE|CR_SOCKET_MEMORY|CR_SOCKET|CR_MEMORY|CR_ONE_TASK_PER_CORE|CR_CORE_DEFAULT_DIST_BLOCK|CR_NO_DIST_TOPO_BLOCK|CR_LLN|CR_PACK_NODES|LL_SHARED_GRES|MULTIPLE_SHARING_GRES_PJ|ENFORCE_BINDING_GRES|ONE_TASK_PER_SHARING_GRES"`
	CPUs        PartitionCPUs     `json:"cpus"`
	Defaults    PartitionDefaults `json:"defaults"`
	Flags       []string          `json:"flags" description:"Partition flag options" enum:"DEFAULT|HIDDEN|NO_ROOT|ROOT_ONLY|REQ_RESV|LLN|PDOI|DEFAULT_CLEAR|HIDDEN_CLEAR|NO_ROOT_CLEAR|ROOT_ONLY_CLEAR|REQ_RESV_CLEAR|LLN_CLEAR|PDOI_CLEAR"`
	Partition   PartitionState    `json:"partition"`
	GraceTime   Int32             `json:"grace_time" description:"GraceTime - Grace time in seconds to be extended to a job which has been selected for preemption"`
	Maximums    PartitionMaximums `json:"maximums"`
	Minimums    PartitionMinimums `json:"minimums"`
	Name        string            `json:"name" description:"PartitionName - Name by which the partition may be referenced"`
	NodeSets    string            `json:"node_sets" description:"NodeSets - Comma-separated list of nodesets which are associated with this partition"`
	PreemptMode []string          `json:"preempt_mode" description:"PreemptMode - Mechanism used to preempt jobs for this partition when PreemptType=preempt/partition_prio is configured (the \"CLUSTER_GLOBAL\" flag will be ignored when updating a partition)." enum:"CLUSTER_GLOBAL|DISABLED|SUSPEND|REQUEUE|CANCEL|PRIORITY"`
	Priority    PartitionPriority `json:"priority"`
	Timeouts    PartitionTimeouts `json:"timeouts"`
	Topology    string            `json:"topology" description:"Topology - Name of the topology, defined in topology.yaml, used by jobs in this partition"`
	SuspendTime Number[Int32]     `json:"suspend_time" description:"SuspendTime - Nodes which remain idle or down for this number of seconds will be placed into power save mode (read-only; GLOBAL if both set and infinite are false). (32 bit integer number with flags)"`
}

type SinfoRecord struct {
	Port            Int32             `json:"port" description:"Node TCP port"`
	Node            SinfoNode         `json:"node"`
	Nodes           SinfoNodes        `json:"nodes"`
	CPUs            SinfoCPUs         `json:"cpus"`
	Sockets         SinfoSockets      `json:"sockets"`
	Cores           SinfoCores        `json:"cores"`
	Threads         SinfoThreads      `json:"threads"`
	Disk            SinfoDisk         `json:"disk"`
	Memory          SinfoMemory       `json:"memory"`
	Weight          SinfoWeight       `json:"weight"`
	Features        SinfoFeatures     `json:"features"`
	GRES            SinfoGRES         `json:"gres"`
	Cluster         string            `json:"cluster" description:"Cluster name if running in a federation"`
	Comment         string            `json:"comment" description:"Arbitrary descriptive string"`
	Extra           string            `json:"extra" description:"Arbitrary string used for node filtering if SchedulerParameters=extra_constraints is enabled"`
	Reason          SinfoReason       `json:"reason"`
	Reservation     string            `json:"reservation" description:"Name of advanced reservation"`
	ReasonSetByUser string            `json:"reason_set_by_user" description:"User who set the reason" complex:"keep_empty"`
	ResourceSpec    SinfoResourceSpec `json:"resource_spec"`
	Partition       PartitionInfo     `json:"partition" description:"Partition name; default partition is followed by \"*\""`
}

type MetaPlugin struct {
	Type              string `json:"type" description:"Slurm plugin type (if applicable)"`
	Name              string `json:"name" description:"Slurm plugin name (if applicable)"`
	DataParser        string `json:"data_parser" description:"Slurm data_parser plugin"`
	AccountingStorage string `json:"accounting_storage" description:"Slurm accounting plugin"`
}

type MetaClient struct {
	Source string `json:"source" description:"Client source description"`
	User   string `json:"user" description:"Client user (if known)"`
	Group  string `json:"group" description:"Client group (if known)"`
}

type SlurmVersion struct {
	Major string `json:"major" description:"Slurm release major version"`
	Micro string `json:"micro" description:"Slurm release micro version"`
	Minor string `json:"minor" description:"Slurm release minor version"`
}

type SlurmMeta struct {
	Version SlurmVersion `json:"version"`
	Release string       `json:"release" description:"Slurm release string"`
	Cluster string       `json:"cluster" description:"Slurm cluster name"`
}

type Meta struct {
	Plugin  MetaPlugin `json:"plugin"`
	Client  MetaClient `json:"client"`
	Command []string   `json:"command" description:"CLI command (if applicable)"`
	Slurm   SlurmMeta  `json:"slurm"`
}

type Error struct {
	Description string `json:"description" description:"Long form error description"`
	ErrorNumber Int32  `json:"error_number" description:"Slurm numeric error identifier"`
	Error       string `json:"error" description:"Short form error description"`
	Source      string `json:"source" description:"Source of error or where error was first detected"`
}

type Warning struct {
	Description string `json:"description" description:"Long form warning description"`
	Source      string `json:"source" description:"Source of warning or where warning was first detected"`
}

type SinfoResponse struct {
	Sinfo    []SinfoRecord `json:"sinfo" description:"node and partition info"`
	Meta     Meta          `json:"meta" description:"Slurm meta values"`
	Errors   []Error       `json:"errors" description:"Query errors"`
	Warnings []Warning     `json:"warnings" description:"Query warnings"`
}

func (PartitionInfo) SchemaRequired() []string { return []string{} }
func (SinfoRecord) SchemaRequired() []string   { return []string{} }
func (Meta) SchemaRequired() []string          { return []string{} }
func (Error) SchemaRequired() []string         { return []string{} }
func (Warning) SchemaRequired() []string       { return []string{} }
func (SinfoResponse) SchemaRequired() []string { return []string{"sinfo"} }
