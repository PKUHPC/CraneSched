// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package squeue

import (
	_ "embed"
	"fmt"
	"io"
	"math"
	"os"
	"os/user"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PKUHPC/CraneSched/go/compat/utils"
	pb "github.com/PKUHPC/CraneSched/go/generated/protos"
	"github.com/PKUHPC/CraneSched/go/internal/util"
)

//go:embed help.txt
var help string

//go:embed usage.txt
var usage string

type operandError struct{ operand string }

func (e *operandError) Error() string { return "squeue: error: Unrecognized option: " + e.operand }

const (
	none     = utils.NoArgument
	required = utils.RequiredArgument
	optional = utils.OptionalArgument
)

var vocabulary = []utils.OptionSpec{
	{Name: "autocomplete", Argument: required}, {Name: "accounts", Short: 'A', Argument: required}, {Name: "all", Short: 'a'}, {Name: "array", Short: 'r'},
	{Name: "expand-patterns"}, {Name: "Format", Short: 'O', Argument: required}, {Name: "format", Short: 'o', Argument: required}, {Name: "federation"},
	{Name: "help"}, {Name: "helpstate"}, {Name: "helpformat"}, {Name: "helpFormat"}, {Name: "hide"}, {Name: "iterate", Short: 'i', Argument: required},
	{Name: "jobs", Short: 'j', Argument: optional}, {Name: "local"}, {Name: "long", Short: 'l'}, {Name: "licenses", Short: 'L', Argument: required},
	{Name: "cluster", Short: 'M', Argument: required}, {Name: "clusters", Short: 'M', Argument: required}, {Name: "me"}, {Name: "name", Short: 'n', Argument: required},
	{Name: "noconvert"}, {Name: "node", Short: 'w', Argument: required}, {Name: "nodes", Short: 'w', Argument: required}, {Name: "nodelist", Short: 'w', Argument: required},
	{Name: "noheader", Short: 'h'}, {Name: "notme"}, {Name: "only-job-state"}, {Name: "partitions", Short: 'p', Argument: required}, {Name: "priority", Short: 'P'},
	{Name: "qos", Short: 'q', Argument: required}, {Name: "reservation", Short: 'R', Argument: required}, {Name: "running-over", Argument: required}, {Name: "running-under", Argument: required},
	{Name: "sib"}, {Name: "sibling"}, {Name: "sort", Short: 'S', Argument: required}, {Name: "start"}, {Name: "steps", Short: 's', Argument: optional}, {Name: "states", Short: 't', Argument: required},
	{Name: "usage"}, {Name: "user", Short: 'u', Argument: required}, {Name: "users", Short: 'u', Argument: required}, {Name: "verbose", Short: 'v'}, {Name: "version", Short: 'V'},
	{Name: "json", Argument: optional}, {Name: "yaml", Argument: optional},
}

type stepSelector struct {
	job   *pb.JobIdSelector
	index uint32
}

type options struct {
	fullBitmapID                                         bool
	stepSelectors                                        []stepSelector
	nodeNames                                            map[string]bool
	formatTime                                           func(time.Time) string
	encoding                                             utils.SerializerOptions
	req                                                  *pb.QueryJobsInfoRequest
	jobs, steps, array, all                              bool
	local, federation, sibling                           bool
	long, noHeader, start, onlyState                     bool
	noConvert, expand, priority                          bool
	clusterName, jobIDs, stepIDs, clusters               string
	format, longFormat, sort                             string
	serializer, parser, action, actionValue, notUser     string
	parserAttached, customFormat, customLong, customSort bool
	iterate, verbose                                     int
	under, over                                          int64
	licenses, users, names, accounts, qos                []string
	nodes                                                *utils.HostList
	states                                               []pb.JobStatus
	raw                                                  map[string]string
	nodeText                                             string
}

func unsupported(feature string) error {
	return fmt.Errorf("squeue: error: current Crane version does not support %s", feature)
}
func assigned(m map[string]string, k string) bool { _, ok := m[k]; return ok }
func list(s string) []string                      { return strings.FieldsFunc(s, func(r rune) bool { return r == ',' }) }

func parse(args []string, env map[string]string, diag io.Writer) (*options, error) {
	o := &options{fullBitmapID: assigned(env, "SLURM_BITSTR_LEN"), formatTime: utils.TimeFormatter("squeue", env["SLURM_TIME_FORMAT"], time.Now().In(utils.CommandTimezone(env)), diag), sort: env["SQUEUE_SORT"], customSort: assigned(env, "SQUEUE_SORT"), all: assigned(env, "SQUEUE_ALL"), array: assigned(env, "SQUEUE_ARRAY"), priority: assigned(env, "SQUEUE_PRIORITY"),
		clusters: env["SLURM_CLUSTERS"], local: assigned(env, "SQUEUE_LOCAL"), federation: assigned(env, "SQUEUE_FEDERATION"), sibling: assigned(env, "SQUEUE_SIB") || assigned(env, "SQUEUE_SIBLING"),
		req: &pb.QueryJobsInfoRequest{IncludePublicQueueDetails: true, Mode: pb.QueryJobsInfoMode_QUERY_JOBS_INFO_QUEUE, NumLimit: math.MaxUint32}}
	s := utils.NewOptionScanner(args, vocabulary, assigned(env, "POSIXLY_CORRECT"))
	values := map[string]string{}
	o.raw = values
	for {
		op, err := s.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("squeue: %s\nTry \"squeue --help\" for more information", err)
		}
		v := op.Value
		switch op.Name {
		case "help", "usage", "version", "helpformat", "helpFormat", "helpstate", "autocomplete":
			o.action, o.actionValue = op.Name, v
			return o, nil
		case "states":
			if e := o.selectStates(v); e != nil {
				return nil, e
			}
			o.states = nil
			values["states"] = v
		case "jobs":
			if v != "" {
				if _, e := util.ParseJobIdSelectorList(strings.Split(v, ".")[0], ","); e != nil {
					return nil, fmt.Errorf("squeue: error: Invalid job id: %s", v)
				}
			}
			o.jobs, o.jobIDs = true, v
		case "steps":
			if v != "" && !strings.Contains(v, ".") {
				return nil, fmt.Errorf("squeue: error: Invalid job_step id: %s.??", v)
			}
			o.steps, o.stepIDs = true, v
		case "all":
			o.all = true
		case "hide":
			o.all = false
		case "array":
			o.array = true
		case "expand-patterns":
			o.expand = true
		case "federation":
			o.federation = true
		case "local":
			o.local = true
		case "sib", "sibling":
			o.sibling = true
		case "cluster", "clusters":
			o.clusters = v
		case "only-job-state":
			o.onlyState = true
		case "priority":
			o.priority = true
		case "start":
			o.start = true
		case "long":
			o.long = true
		case "noheader":
			o.noHeader = true
		case "noconvert":
			o.noConvert = true
		case "verbose":
			o.verbose++
		case "format":
			if o.customLong {
				return nil, fmt.Errorf("squeue: error: -o (--format) is incompatible with -O (--Format)")
			}
			o.format, o.customFormat = v, true
		case "Format":
			if o.customFormat {
				return nil, fmt.Errorf("squeue: error: -O (--Format) is incompatible with -o (--format)")
			}
			o.longFormat, o.customLong = v, true
		case "sort":
			o.sort, o.customSort = v, true
		case "node", "nodes", "nodelist":
			o.nodeText = v
			o.nodes, err = utils.ParseHostList(v)
			if err != nil {
				return nil, fmt.Errorf("squeue: error: invalid node list %s", v)
			}
		case "partitions":
			values[op.Name] = v
			o.all = true
		case "iterate":
			o.iterate = 0
			_, _ = fmt.Sscanf(strings.TrimSpace(v), "%d", &o.iterate)
			if o.iterate <= 0 {
				return nil, fmt.Errorf("squeue: error: --iterate=%s\n", v)
			}
		case "me":
			values["users"] = strconv.Itoa(os.Getuid())
		case "notme":
			o.notUser = strconv.Itoa(os.Getuid())
		case "user", "users":
			values["users"] = v
		case "running-over", "running-under":
			n, err := slurmSeconds(v)
			if err != nil || n <= 0 {
				return nil, fmt.Errorf("squeue: fatal: Invalid time limit specification")
			}
			if op.Name == "running-over" {
				o.over = n
			} else {
				o.under = n
			}
		case "json", "yaml":
			o.encoding, err = utils.ParseSerializerOptions(op.Name, env)
			if err != nil {
				return nil, fmt.Errorf("squeue: fatal: %w", err)
			}
			o.serializer, o.parser = op.Name, v
			o.parserAttached = op.Attached
			if v == "list" {
				o.action = "parsers"
				return o, nil
			}
		default:
			values[op.Name] = v
		}
	}
	if o.customFormat && o.long {
		return nil, fmt.Errorf("squeue: fatal: Options -o(--format) and -l(--long) are mutually exclusive. Please remove one and retry.")
	}
	// Slurm checks explicit query options before applying SQUEUE_* filter defaults.
	if o.onlyState && (o.all || o.long || o.steps || o.start || o.priority || o.notUser != "" ||
		o.nodes != nil || o.over > 0 || o.under > 0 || assigned(values, "accounts") ||
		assigned(values, "partitions") || assigned(values, "qos") || assigned(values, "name") ||
		assigned(values, "licenses") || assigned(values, "users") || assigned(values, "reservation")) {
		return nil, fmt.Errorf("squeue: fatal: Option --only-job-state only works alone, with -j (--jobs) and/or -t (--states) parameters, parameters for cluster/federation selection, and the printing options.")
	}
	rest := s.Operands()
	if len(rest) > 0 {
		if o.jobs {
			o.jobIDs = rest[0]
			rest = rest[1:]
		} else if o.steps {
			o.stepIDs = rest[0]
			rest = rest[1:]
		}
		if len(rest) > 0 {
			return nil, &operandError{rest[0]}
		}
	}
	if o.jobs && o.steps && o.jobIDs == "" {
		return nil, fmt.Errorf("squeue: error: Incompatible options --jobs and --steps")
	}
	if o.clusters != "" {
		return nil, unsupported("multi-cluster queries")
	}
	if !o.local && (o.federation || o.sibling) {
		return nil, unsupported("federated queries")
	}
	if !o.customFormat && !o.customLong && !o.long && !o.steps {
		if v, ok := env["SQUEUE_FORMAT"]; ok {
			o.format, o.customFormat = v, true
		} else if v, ok := env["SQUEUE_FORMAT2"]; ok {
			o.longFormat, o.customLong = v, true
		}
	}
	for k, e := range map[string]string{"accounts": "ACCOUNT", "name": "NAMES", "licenses": "LICENSES", "partitions": "PARTITION", "qos": "QOS", "states": "STATES", "users": "USERS"} {
		if _, ok := values[k]; !ok {
			values[k] = env["SQUEUE_"+e]
		}
	}
	o.accounts = list(values["accounts"])
	o.req.FilterPartitions = list(values["partitions"])
	if len(o.req.FilterPartitions) > 0 {
		o.all = true
	}
	o.qos = list(values["qos"])
	o.licenses = list(values["licenses"])
	if raw, ok := values["reservation"]; ok && !o.steps {
		o.req.FilterReservations = reservationNames(raw, diag)
	}
	o.names = list(values["name"])
	if values["users"] != "" {
		o.users = []string{}
	}
	for _, v := range list(values["users"]) {
		u, err := user.Lookup(v)
		if err != nil {
			uid, e := strconv.ParseUint(v, 10, 32)
			if e != nil {
				fmt.Fprintf(diag, "squeue: error: Invalid user: %s\n\n", v)
				continue
			}
			o.users = append(o.users, strconv.FormatUint(uid, 10))
		} else {
			o.users = append(o.users, u.Uid)
		}
	}
	if err := o.selectIDs(); err != nil {
		return nil, err
	}
	if err := o.selectStates(values["states"]); err != nil {
		return nil, err
	}

	if o.under > 0 && o.over > o.under {
		return nil, fmt.Errorf("squeue: fatal: --running-over=%02d:%02d:%02d and --running-under=%02d:%02d:%02d will return 0 jobs.", o.over/3600, o.over/60%60, o.over%60, o.under/3600, o.under/60%60, o.under%60)
	}
	if o.steps {
		o.licenses = nil
	}
	o.req.OptionQueryStepsOnly = o.steps
	return o, nil
}

// A reservation filter is a Slurm name list. Commas inside a bracketed
// range belong to the range, and a leading quote delimits the whole list.
func reservationNames(raw string, diag io.Writer) []string {
	if raw != "" && (raw[0] == '\'' || raw[0] == '"') {
		raw, _, _ = strings.Cut(raw[1:], raw[:1])
	}
	raw = strings.ReplaceAll(strings.ReplaceAll(raw, "'", "`"), "\"", "`")
	var names []string
	start := 0
	inRange, sawRange := false, false
	for i, ch := range raw {
		switch ch {
		case '[':
			inRange = true
		case ']':
			hosts, err := utils.ParseHostList(strings.TrimSpace(raw[start : i+1]))
			if err != nil {
				fmt.Fprintf(diag, "squeue: error: %v\n", err)
			} else {
				names = slices.AppendSeq(names, hosts.Names())
				start = i + 1
			}
			inRange, sawRange = false, true
		case ',':
			if !inRange {
				if name := strings.TrimSpace(raw[start:i]); !sawRange && name != "" {
					names = append(names, name)
				}
				start, sawRange = i+1, false
			}
		}
	}
	if name := strings.TrimSpace(raw[start:]); name != "" {
		names = append(names, name)
	}
	return names
}

func (o *options) selectIDs() error {
	for _, v := range list(o.jobIDs) {
		job, _, _ := strings.Cut(v, ".")
		ids, err := util.ParseJobIdSelectorList(job, ",")
		if err != nil {
			return fmt.Errorf("squeue: error: Invalid job id: %s", v)
		}
		o.req.FilterJobIds = append(o.req.FilterJobIds, ids...)
	}
	var selectedSteps []string
	for _, v := range list(o.stepIDs) {
		job, step, ok := strings.Cut(v, ".")
		if !ok || step == "" {
			return fmt.Errorf("squeue: error: Invalid job_step id: %s.??", job)
		}
		// In 26.05.4 -s reads the signed decimal prefix, including zero
		// for text such as "batch". Displayed step names are independent.
		var index int64
		fmt.Sscanf(step, "%d", &index)
		if int32(index) < 0 {
			return fmt.Errorf("squeue: error: Invalid job_step id: %s", v)
		}
		selectedSteps = append(selectedSteps, job+"."+num(uint32(index)))
		// Crane and Slurm use different special step IDs. Match the numeric selector
		// after reading the Crane step type.
		ids, err := util.ParseJobIdSelectorList(job, ",")
		if err != nil {
			return fmt.Errorf("squeue: error: Invalid job_step id: %s", v)
		}
		o.stepSelectors = append(o.stepSelectors, stepSelector{ids[0], uint32(index)})
	}
	o.stepIDs = strings.Join(selectedSteps, ",")
	return nil
}

func (o *options) selectStates(val string) error {
	if strings.EqualFold(val, "all") {
		return nil
	}
	if val == "" {
		if o.onlyState && o.serializer != "" {
			return nil
		}
		if o.start {
			val = "pending"
		} else {
			val = "pending,running,suspended,completing,configuring"
		}
	}
	for _, input := range list(val) {
		key := strings.ToUpper(input)
		i := slices.IndexFunc(slurmStates, func(state jobState) bool {
			return state.long == key || state.short == key
		})
		if i < 0 {
			return fmt.Errorf("squeue: error: Invalid job state specified: %s\nsqueue: error: Valid job states include: %s\n", input, stateNames(","))
		}
		state := slurmStates[i]
		if len(state.statuses) == 0 {
			return unsupported("job state " + key)
		}
		o.states = append(o.states, state.statuses...)
	}
	return nil
}

func (o *options) printOptions(out io.Writer) error {
	var b strings.Builder
	b.WriteString("-----------------------------\n")
	nullable := func(s string) string {
		if s == "" {
			return "(null)"
		}
		return s
	}
	bit := func(b bool) int {
		if b {
			return 1
		}
		return 0
	}
	states := o.raw["states"]
	if o.start && !o.steps && states == "" {
		states = "PD"
	}
	format := o.format
	sortOrder := o.sort
	if o.start && !o.steps {
		if !o.customFormat {
			format = "%.18i %.9P %.8j %.8u %.2t %.19S %.6D %20Y %R"
		}
		if !o.customSort {
			sortOrder = "S"
		}
	}
	for _, v := range []struct {
		k string
		v any
	}{
		{"all", o.all}, {"array", o.array}, {"federation", o.federation}, {"format", nullable(format)}, {"iterate", o.iterate}, {"job_flag", bit(o.jobs && !o.steps)}, {"jobs", nullable(o.jobIDs)}, {"licenses", nullable(o.raw["licenses"])}, {"local", o.local}, {"names", nullable(o.raw["name"])}, {"nodes", o.nodeText}, {"only_job_state", o.onlyState}, {"partitions", nullable(o.raw["partitions"])}, {"priority", o.priority}, {"reservation", nullable(o.raw["reservation"])}, {"sibling ", o.sibling}, {"sort", nullable(sortOrder)}, {"start_flag", bit(o.start)}, {"states", nullable(states)}, {"step_flag", bit(o.steps)}, {"steps", nullable(o.stepIDs)}, {"users", nullable(o.raw["users"])}, {"verbose", o.verbose},
	} {
		if v.k == "sibling " {
			fmt.Fprintf(&b, "sibling      = %v\n", v.v)
		} else {
			fmt.Fprintf(&b, "%-11s = %v\n", v.k, v.v)
		}
	}
	if o.verbose > 1 {
		printList := func(k string, vs []string) {
			for i, v := range vs {
				fmt.Fprintf(&b, "%s_list[%d] = %s\n", k, i, v)
			}
		}
		printList("job", list(o.jobIDs))
		for i, n := range o.names {
			fmt.Fprintf(&b, "name_list[%d] = %d\n", i, n[0])
		}
		printList("licenses", list(o.raw["licenses"]))
		printList("part", o.req.FilterPartitions)
		if strings.EqualFold(states, "all") {
			b.WriteString("state_list = all\n")
		} else if states != "" {
			var names []string
			for _, st := range o.states {
				n := jobStates[st].long
				if !slices.Contains(names, n) {
					names = append(names, n)
				}
			}
			printList("state", names)
		}
		printList("step", list(o.stepIDs))
		printList("user", o.users)
	}
	b.WriteString("-----------------------------\n\n\n")
	_, err := io.WriteString(out, b.String())
	return err
}
