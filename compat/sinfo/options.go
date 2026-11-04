// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package sinfo implements the Slurm sinfo command contract against Crane data.
// Parsing, planning and rendering do not depend on a running controller.
package sinfo

import (
	"CraneFrontEnd/compat/utils"
	_ "embed"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const compatibilityVersion = "26.05.4"

type options struct {
	Program                                                      string
	All, Dead, Exact, Future, NoHeader, Local, Long              bool
	Node, Responding, Reasons, Summary, Reservations, Federation bool
	NoConvert, HasSort                                           bool
	Iterate, Verbose                                             int
	Clusters, Nodes, Partition, Sort, States, Format             string
	HasClusters, HasNodes, HasPartition, HasStates, HasFormat    bool
	LongFormat                                                   bool
	StateFilter                                                  stateExpression
	NodeFilter                                                   *utils.HostList
	Serialization, DataParser, Action, ActionValue               string
	HasDataParser                                                bool
	Serializer                                                   utils.SerializerOptions
}

// This is the public option vocabulary, not a general-purpose CLI framework.
var optionSpecs = []utils.OptionSpec{
	{Name: "autocomplete", Short: 0, Argument: utils.RequiredArgument},
	{Name: "all", Short: 'a', Argument: utils.NoArgument},
	{Name: "dead", Short: 'd', Argument: utils.NoArgument},
	{Name: "exact", Short: 'e', Argument: utils.NoArgument},
	{Name: "federation", Short: 0, Argument: utils.NoArgument},
	{Name: "future", Short: 'F', Argument: utils.NoArgument},
	{Name: "help", Short: 0, Argument: utils.NoArgument},
	{Name: "helpformat", Short: 0, Argument: utils.NoArgument},
	{Name: "helpFormat", Short: 0, Argument: utils.NoArgument},
	{Name: "helpstate", Short: 0, Argument: utils.NoArgument},
	{Name: "hide", Short: 0, Argument: utils.NoArgument},
	{Name: "iterate", Short: 'i', Argument: utils.RequiredArgument},
	{Name: "local", Short: 0, Argument: utils.NoArgument},
	{Name: "long", Short: 'l', Argument: utils.NoArgument},
	{Name: "cluster", Short: 'M', Argument: utils.RequiredArgument},
	{Name: "clusters", Short: 'M', Argument: utils.RequiredArgument},
	{Name: "nodes", Short: 'n', Argument: utils.RequiredArgument},
	{Name: "noconvert", Short: 0, Argument: utils.NoArgument},
	{Name: "noheader", Short: 'h', Argument: utils.NoArgument},
	{Name: "Node", Short: 'N', Argument: utils.NoArgument},
	{Name: "format", Short: 'o', Argument: utils.RequiredArgument},
	{Name: "Format", Short: 'O', Argument: utils.RequiredArgument},
	{Name: "partition", Short: 'p', Argument: utils.RequiredArgument},
	{Name: "responding", Short: 'r', Argument: utils.NoArgument},
	{Name: "list-reasons", Short: 'R', Argument: utils.NoArgument},
	{Name: "summarize", Short: 's', Argument: utils.NoArgument},
	{Name: "sort", Short: 'S', Argument: utils.RequiredArgument},
	{Name: "states", Short: 't', Argument: utils.RequiredArgument},
	{Name: "reservation", Short: 'T', Argument: utils.NoArgument},
	{Name: "usage", Short: 0, Argument: utils.NoArgument},
	{Name: "verbose", Short: 'v', Argument: utils.NoArgument},
	{Name: "version", Short: 'V', Argument: utils.NoArgument},
	{Name: "json", Short: 0, Argument: utils.OptionalArgument},
	{Name: "yaml", Short: 0, Argument: utils.OptionalArgument},
}

type commandError struct{ message string }

func (e *commandError) Error() string { return e.message }
func optionError(text string) error   { return &commandError{"sinfo: error: " + text + "\n"} }
func getoptError(program, text string) error {
	return &commandError{program + ": " + text + "\nTry \"sinfo --help\" for more information\n"}
}

func parseOptions(argv []string, env map[string]string) (*options, error) {
	o := &options{Program: "sinfo"}
	if len(argv) != 0 {
		o.Program = argv[0]
		argv = argv[1:]
	}
	_, o.All = env["SINFO_ALL"]
	_, o.Federation = env["SINFO_FEDERATION"]
	_, o.Future = env["SINFO_FUTURE"]
	_, o.Local = env["SINFO_LOCAL"]
	o.Partition, o.HasPartition = env["SINFO_PARTITION"]
	if o.All && o.HasPartition {
		return nil, optionError("Conflicting options, SINFO_ALL and SINFO_PARTITION, specified. Please choose one or the other.")
	}
	o.All = o.All || o.HasPartition
	o.Sort, o.HasSort = env["SINFO_SORT"]
	o.Clusters, o.HasClusters = env["SLURM_CLUSTERS"]
	o.Local = o.Local || o.HasClusters
	_, posix := env["POSIXLY_CORRECT"]
	var explicitAll, explicitPartition bool
	apply := func(name, value string) error {
		switch name {
		case "all":
			o.All = true
			explicitAll = true
			o.HasPartition = false
			o.Partition = ""
		case "hide":
			o.All = false
		case "dead":
			o.Dead = true
		case "exact":
			o.Exact = true
		case "future":
			o.Future = true
		case "noheader":
			o.NoHeader = true
		case "iterate":
			o.Iterate = decimalPrefix(value)
			if o.Iterate <= 0 {
				return optionError("Error: invalid entry for --iterate=" + value)
			}
		case "long":
			o.Long = true
		case "cluster", "clusters":
			o.Clusters = value
			o.HasClusters = true
			o.Local = true
		case "nodes":
			list, err := utils.ParseHostList(value)
			if err != nil {
				diagnostic := fmt.Sprintf("'%s' invalid entry for --nodes", value)
				if detail, ok := err.(*utils.HostRangeError); ok {
					diagnostic = detail.Error() + "\nsinfo: error: " + diagnostic
				}
				return optionError(diagnostic)
			}
			o.Nodes, o.HasNodes, o.NodeFilter = value, true, list
		case "Node":
			o.Node = true
		case "Format":
			o.LongFormat = true
			o.Format, o.HasFormat = value, true
		case "format":
			o.Format, o.HasFormat = value, true
		case "partition":
			o.Partition, o.HasPartition = value, true
			o.All, explicitPartition = true, true
		case "responding":
			o.Responding = true
		case "list-reasons":
			o.Reasons = true
		case "summarize":
			o.Summary = true
		case "sort":
			o.Sort = value
			o.HasSort = true
		case "states":
			filter, err := parseStates(value, o.StateFilter.AllOf)
			if err != nil {
				return err
			}
			o.States, o.HasStates, o.StateFilter = value, true, filter
		case "reservation":
			o.Reservations = true
		case "verbose":
			o.Verbose++
		case "federation":
			o.Federation = true
		case "local":
			o.Local = true
		case "noconvert":
			o.NoConvert = true
		case "json", "yaml":
			settings, err := parseSerializerEnvironment(env)
			if err != nil {
				return err
			}
			o.Serializer = settings[name]
			o.Serialization, o.DataParser = name, value
		default:
			o.Action, o.ActionValue = name, value
		}
		return nil
	}
	scanner := utils.NewOptionScanner(argv, optionSpecs, posix)
	for {
		option, err := scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, getoptError(o.Program, err.Error())
		}
		if err := apply(option.Name, option.Value); err != nil {
			return nil, err
		}
		if option.Name == "json" || option.Name == "yaml" {
			o.HasDataParser = option.Attached
		}
		if o.Action != "" {
			return o, nil
		}
	}
	if o.Long && o.HasFormat {
		return nil, &commandError{"sinfo: fatal: Options -o(--format) and -l(--long) are mutually exclusive. Please remove one and retry.\n"}
	}
	if explicitAll && explicitPartition {
		return nil, optionError("Conflicting options, -a and -p, specified. Please choose one or the other.")
	}
	return o, nil
}

func decimalPrefix(value string) int {
	value = strings.TrimLeft(value, " \t\r\n\v\f")
	end := 0
	if len(value) != 0 && (value[0] == '-' || value[0] == '+') {
		end++
	}
	start := end
	for end < len(value) && value[end] >= '0' && value[end] <= '9' {
		end++
	}
	if start == end {
		return 0
	}
	// The reference's atoi converts through a signed long before narrowing to
	// int. ParseInt returns the saturated long even when it reports overflow.
	n, _ := strconv.ParseInt(value[:end], 10, 64)
	return int(int32(n))
}

// These strings reproduce the observable help/usage interface of the target
// command. They contain no scheduler code and do not drive option parsing.
//
//go:embed help.txt
var helpText string

//go:embed usage.txt
var usageText string

func writeAction(w io.Writer, o *plan) error {
	var output strings.Builder
	switch o.Action {
	case "help":
		output.WriteString(helpText)
	case "usage":
		output.WriteString(usageText)
	case "version":
		fmt.Fprintf(&output, "slurm %s\n", compatibilityVersion)
	case "helpstate":
		output.WriteString(strings.ReplaceAll(stateNames, ",", "\n"))
		output.WriteByte('\n')
	case "helpformat", "helpFormat":
		count, columns := 0, 4
		if o.Action == "helpformat" {
			columns = 8
		}
		for _, field := range fieldSpecs {
			if field.Hidden || o.Action == "helpformat" && field.Short == 0 {
				continue
			}
			if count > 0 && count%columns == 0 {
				output.WriteByte('\n')
			}
			if o.Action == "helpformat" {
				fmt.Fprintf(&output, "%%%-5c", field.Short)
			} else {
				fmt.Fprintf(&output, "%-20s", field.Name)
			}
			count++
		}
		output.WriteByte('\n')
	case "autocomplete":
		query := o.ActionValue
		if query == "" {
			return nil
		}
		alpha := query[0] >= 'a' && query[0] <= 'z' || query[0] >= 'A' && query[0] <= 'Z'
		for _, spec := range optionSpecs {
			if spec.Short != 0 && (query[0] == '-' || alpha) {
				flag := "-" + string(spec.Short)
				if strings.Contains(flag, query) {
					output.WriteString(flag + "\n")
				}
			}
			flag := "--" + spec.Name
			if (len(query) > 1 || alpha) && strings.Contains(flag, query) {
				if spec.Argument != utils.NoArgument {
					output.WriteString(flag + "=\n")
				}
				if spec.Argument == utils.OptionalArgument {
					output.WriteString(flag + " \n")
				}
				if spec.Argument == utils.NoArgument {
					output.WriteString(flag + "\n")
				}
			}
		}
		if output.Len() != 0 {
			output.WriteByte('\n')
		}
	}
	_, err := io.WriteString(w, output.String())
	return err
}

func unsupported(entry, feature string) error {
	return optionError(fmt.Sprintf("%s: current Crane version does not support %s", entry, feature))
}

// Check only effective requests. In particular -T has its own report and does
// not consume fields, states, node filters, sorting or a structured serializer.
func validateCapabilities(o *plan) error {
	if o.HasClusters {
		return unsupported("--clusters", "multi-cluster queries")
	}
	if o.Federation && !o.Local {
		return unsupported("--federation", "federated queries")
	}
	if o.Reservations {
		return nil
	}
	if o.Serialization != "" && !strings.EqualFold(o.DataParser, "list") {
		schema, err := selectSchema(o, io.Discard)
		if err != nil {
			return err
		}
		// Schema output still queries the controller, but does not project
		// node fields, states or sort keys into a data report.
		if schema.SpecOnly {
			return nil
		}
	}
	if o.Future {
		return unsupported("--future", "future node registration")
	}
	for _, item := range o.Fields {
		if item.Field != nil && item.Field.Capability != "" {
			return unsupported("field "+item.Field.Name, item.Field.Capability)
		}
	}
	// Sorting by an unexposed feature is still a request for that feature.
	for _, key := range o.Sort {
		name := map[rune]string{'b': "features_act", 'd': "Disk", 'f': "Features", 'g': "Groups", 'h': "OverSubscribe", 'O': "CPUsLoad", 'e': "FreeMem", 'p': "PriorityTier", 'r': "Root", 's': "Size", 'w': "Weight"}[key]
		if f := fieldByName(name); f != nil && f.Capability != "" {
			return unsupported(fmt.Sprintf("sort key %c", key), f.Capability)
		}
	}
	if o.HasStates {
		for _, term := range o.StateFilter.Terms {
			switch term.Name {
			case "future", "planned", "blocked", "cloud", "inval", "fail", "maint", "reboot", "reboot^", "external", "dynamic_future", "dynamic_norm":
				return unsupported("state "+term.Name, "the requested node state")
			}
		}
	}
	return nil
}

// plan owns derived display/filter settings, separate from the parsed input.
// The embedded options are a copy; selecting defaults never mutates raw input.
type plan struct {
	options
	Query                             query
	Fields                            []formatItem
	Diagnostics                       []string
	AutoPartitionWidth, AutoNodeWidth bool
}

func parseCommand(argv []string, env map[string]string) (*plan, error) {
	input, err := parseOptions(argv, env)
	if err != nil {
		return nil, err
	}
	o := &plan{options: *input, Query: query{ReservationsOnly: input.Reservations}}
	if o.Action != "" {
		return o, nil
	}
	if !o.HasFormat {
		o.LongFormat = true
		switch {
		case o.Summary:
			o.AutoPartitionWidth = true
			o.Format = "partition:9 ,available:.5 ,time:.10 ,nodeaiot:.16 ,nodelist:0"
		case o.Node:
			o.AutoPartitionWidth, o.AutoNodeWidth = true, true
			o.Format = "nodelist:0 ,nodes:.6 ,partition:.9 ,statecompact:6"
			if o.Long {
				o.Format = "nodelist:0 ,nodes:.6 ,partition:.9 ,statelong:.11 ,cpus:4 ,socketcorethread:.8 ,memory:.6 ,disk:.8 ,weight:.6 ,features:.8 ,reason:20"
			}
		case o.Reasons:
			o.Format = "reason:20 ,user:9 ,timestamp:19 ,nodelist:0"
			if o.Long {
				o.Format = "reason:20 ,userlong:12 ,timestamp:19 ,statecompact:6 ,nodelist:0"
			}
		default:
			if value, exists := env["SINFO_FORMAT"]; exists {
				o.Format, o.LongFormat = value, false
			} else {
				o.AutoPartitionWidth = true
				o.Format = "partition:9 ,available:.5 ,time:.10 ,nodes:.6 ,statecompact:.6 ,nodelist:0"
				if o.Long {
					o.Format = "partition:9 ,available:.5 ,time:.10 ,size:.10 ,root:.4 ,oversubscribe:.8 ,groups:.10 ,nodes:.6 ,statelong:.11 ,reservation:.11 ,nodelist:0"
				}
			}
		}
	}
	o.Fields, o.Diagnostics = parseFormat(o.Format, o.LongFormat)
	if o.Reasons && !o.HasStates {
		o.States = "down,fail,drain"
		o.StateFilter, _ = parseStates(o.States, false)
	}
	return o, nil
}
