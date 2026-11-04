// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"CraneFrontEnd/compat/utils"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

const defaultConfigPath = "/etc/crane/config.yaml"

// Run executes the standalone command. argv includes the invocation name;
// environ contains KEY=value strings. Only main owns process exit.
func Run(ctx context.Context, argv, environ []string, stdout, stderr io.Writer) int {
	env := make(map[string]string, len(environ))
	for _, entry := range environ {
		if key, value, ok := strings.Cut(entry, "="); ok {
			env[key] = value
		}
	}
	r := runner{open: func(ctx context.Context) (snapshotSource, error) {
		return openCraneSource(ctx, defaultConfigPath)
	}, now: time.Now, wait: waitForIteration, renderer: newTextRenderer()}
	return r.run(ctx, argv, env, stdout, stderr)
}

type runner struct {
	open     func(context.Context) (snapshotSource, error)
	now      func() time.Time
	wait     func(context.Context, time.Duration) error
	renderer textRenderer
}

func waitForIteration(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func reportError(w io.Writer, err error) {
	var diagnostic *commandError
	if errors.As(err, &diagnostic) {
		_, _ = io.WriteString(w, diagnostic.message)
	} else {
		_, _ = fmt.Fprintf(w, "sinfo: error: %s\n", err)
	}
}

func (r runner) run(ctx context.Context, argv []string, env map[string]string, stdout, stderr io.Writer) int {
	clock := r.now
	location := utils.CommandTimezone(env)
	r.now = func() time.Time { return clock().In(location) }
	o, err := parseCommand(argv, env)
	if err != nil {
		reportError(stderr, err)
		return 1
	}
	if o.Action != "" {
		if err := writeAction(stdout, o); err != nil {
			reportError(stderr, err)
			return 1
		}
		return 0
	}
	for _, diagnostic := range o.Diagnostics {
		if _, err := io.WriteString(stderr, diagnostic); err != nil {
			return 1
		}
	}
	if err := validateCapabilities(o); err != nil {
		reportError(stderr, err)
		return 1
	}
	// Structured output also splits groups by allocated generic resources.
	if o.Serialization != "" && !o.Reservations {
		o.Fields = append(o.Fields, formatItem{Field: fieldByName("GresUsed")})
	}
	if o.Verbose != 0 {
		if err := writeVerbose(stdout, o); err != nil {
			reportError(stderr, err)
			return 1
		}
	}
	source, err := r.open(ctx)
	if err != nil {
		reportError(stderr, err)
		return 1
	}
	defer source.Close()
	r.renderer.TimeFormat = timeFormatter(env["SLURM_TIME_FORMAT"], r.now(), stderr)
	rc := 0
	for {
		if ctx.Err() != nil {
			return rc
		}
		if !o.NoHeader && o.Serialization == "" && (o.Long || o.Iterate != 0 || o.Verbose != 0) {
			if _, err := fmt.Fprintln(stdout, r.now().Format("Mon Jan 02 15:04:05 2006")); err != nil {
				reportError(stderr, err)
				return 1
			}
		}
		queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		s, loadErr := source.Load(queryCtx, o.Query)
		cancel()
		if ctx.Err() != nil {
			return rc
		}
		if loadErr != nil {
			reportError(stderr, loadErr)
			rc = 1
		} else {
			// Validate and render a complete frame before publishing it. Missing
			// controller data must not produce a half-valid table or JSON document.
			var frame bytes.Buffer
			err = r.render(&frame, stderr, argv, s, o)
			if err != nil {
				reportError(stderr, err)
				rc = 1
			} else if _, err = io.Copy(stdout, &frame); err != nil {
				reportError(stderr, err)
				return 1
			}
		}
		if o.Iterate == 0 {
			return rc
		}
		if _, err := io.WriteString(stdout, "\n"); err != nil {
			reportError(stderr, err)
			return 1
		}
		if err := r.wait(ctx, time.Duration(o.Iterate)*time.Second); err != nil {
			return rc
		}
	}
}

func (r runner) render(w, stderr io.Writer, argv []string, s *snapshot, o *plan) error {
	if o.Reservations {
		return r.renderer.reservations(w, s.Reservations, o.NoHeader, r.now())
	}
	if name, single := o.NodeFilter.Single(); single {
		found := false
		for _, n := range s.Nodes {
			found = found || n.Name == name
		}
		if !found {
			return &commandError{"slurm_load_node: Invalid node name specified\n"}
		}
	}
	rows := buildRows(s, o)
	sortRows(rows, o)
	if o.Serialization != "" {
		return renderStructured(w, stderr, argv, rows, s, o, r.renderer.UserName)
	}
	return r.renderer.table(w, rows, o)
}

func writeVerbose(w io.Writer, o *plan) error {
	groups := make(map[string]bool)
	for _, item := range o.Fields {
		if item.Field != nil {
			groups[item.Field.Group] = true
		}
	}
	optional := func(value string, set bool, absent string) string {
		if set {
			return value
		}
		return absent
	}
	var out strings.Builder
	states := optional(o.States, o.HasStates || o.Reasons, "(null)")
	if o.Reasons && !o.HasStates {
		states = "down" // The reference tokenizes its implicit state string in place.
	}
	out.WriteString("-----------------------------\n")
	for _, item := range []struct {
		name  string
		value any
	}{
		{"dead", o.Dead}, {"exact", map[bool]int{true: 1}[o.Exact]},
		{"filtering", o.Dead || o.Responding || o.HasNodes || o.HasPartition || o.HasStates || o.Reasons},
		{"format", o.Format}, {"iterate", o.Iterate}, {"long", o.Long}, {"no_header", o.NoHeader},
		{"node_field", o.AutoNodeWidth}, {"node_format", o.Node},
		{"nodes", optional(o.Nodes, o.HasNodes, "n/a")}, {"part_field", o.AutoPartitionWidth},
		{"partition", optional(o.Partition, o.HasPartition, "n/a")}, {"responding", o.Responding},
		{"states", states}, {"sort", optional(o.Sort, o.HasSort, "(null)")},
		{"summarize", o.Summary}, {"verbose", o.Verbose},
	} {
		fmt.Fprintf(&out, "%-12s= %v\n", item.name, item.value)
	}
	out.WriteString("-----------------------------\n")
	groups["all"], groups["reservation_mode"] = o.All, o.Reservations
	for _, item := range []struct{ label, group string }{
		{"all_flag        = ", "all"}, {"alloc_mem_flag  = ", "allocmem"}, {"avail_flag      = ", "available"},
		{"comment_flag    = ", "comment"}, {"cpus_flag       = ", "cpus"}, {"default_time_flag =", "defaulttime"},
		{"disk_flag       = ", "disk"}, {"extra_flag      = ", "extra"}, {"features_flag   = ", "features"},
		{"features_flag_act = ", "activefeatures"}, {"groups_flag     = ", "groups"}, {"gres_flag       = ", "gres"},
		{"gres_used_flag  = ", "gresused"}, {"job_size_flag   = ", "size"}, {"max_time_flag   = ", "time"},
		{"memory_flag     = ", "memory"}, {"partition_flag  = ", "partition"}, {"port_flag       = ", "port"},
		{"priority_job_factor_flag   = ", "priorityfactor"}, {"priority_tier_flag   = ", "prioritytier"},
		{"reason_flag     = ", "reason"}, {"reason_timestamp_flag = ", "reasontime"}, {"reason_user_flag = ", "reasonuser"},
		{"reservation_flag = ", "reservation_mode"}, {"resv_name_flag   = ", "reservation"}, {"root_flag       = ", "root"},
		{"oversubscribe_flag      = ", "oversubscribe"}, {"state_flag      = ", "state"},
		{"statecomplete_flag = ", "statecomplete"}, {"weight_flag     = ", "weight"},
	} {
		fmt.Fprintf(&out, "%s%t\n", item.label, groups[item.group])
	}
	out.WriteString("-----------------------------\n\n")
	_, err := io.WriteString(w, out.String())
	return err
}
