// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package squeue

import (
	"fmt"
	"io"
	"os"
	"os/user"
	"strconv"
	"strings"

	"github.com/PKUHPC/CraneSched/go/compat/protocol/v45"
	"github.com/PKUHPC/CraneSched/go/compat/utils"
)

type reportOptions struct {
	schema, complex bool
	plugin          string
	encoding        utils.SerializerOptions
}

func (o *options) reportOptions(diag io.Writer) (reportOptions, error) {
	parser := strings.TrimPrefix(o.parser, "data_parser/")
	if !o.parserAttached {
		parser = "v0.0.45"
	}
	version, flags, _ := strings.Cut(parser, "+")
	s := reportOptions{plugin: "data_parser/" + parser}
	if version != "v0.0.45" {
		return s, unsupported(fmt.Sprintf("data parser %q (supported: v0.0.45)", version))
	}
	for flag := range strings.SplitSeq(strings.ToLower(flags), "+") {
		switch flag {
		case "", "fast", "minimize_refs":
		case "spec_only":
			s.schema = true
		case "complex":
			s.complex = true
		case "prefer_refs":
			fmt.Fprintln(diag, "squeue: data_parser/v0.0.45 ignoring default flag FLAG_PREFER_REFS")
		default:
			fmt.Fprintf(diag, "squeue: warning: data_parser/v0.0.45 ignoring unknown flag %s\n", flag)
		}
	}
	for _, kind := range []string{"json", "yaml"} {
		raw := o.env["SLURM_"+strings.ToUpper(kind)]
		flags := utils.SerializerOptions{Compact: raw != ""}
		for flag := range strings.SplitSeq(strings.ToLower(raw), ",") {
			switch flag {
			case "":
			case "compact":
				flags.Compact = true
			case "pretty":
				flags.Compact = false
			case "complex":
				flags.Complex = true
			case "no_tag":
				flags.NoTag = true
			default:
				return s, fmt.Errorf("squeue: fatal: Unable to parse serializer %q flags: Invalid argument", raw)
			}
		}
		if kind == o.serializer {
			s.encoding = flags
		}
	}
	if !s.schema && !o.onlyState {
		if o.steps {
			return s, unsupported("full Slurm step report (reserved ports and TRES binding)")
		}
		return s, unsupported("full Slurm job report (heterogeneous jobs and federation)")
	}
	return s, nil
}
func (o *options) writeReport(out io.Writer, s reportOptions, argv []string, rows []row) error {
	if s.schema {
		switch {
		case o.steps:
			return utils.WriteSchema[v45.QueueStepsResponse](out, o.serializer, s.encoding)
		case o.onlyState:
			return utils.WriteSchema[v45.QueueStatesResponse](out, o.serializer, s.encoding)
		default:
			return utils.WriteSchema[v45.QueueResponse](out, o.serializer, s.encoding)
		}
	}
	uid := strconv.Itoa(os.Getuid())
	gid := strconv.Itoa(os.Getgid())
	if u, e := user.LookupId(uid); e == nil {
		uid = u.Username
	}
	if g, e := user.LookupGroupId(gid); e == nil {
		gid = g.Name
	}
	response := v45.QueueStatesResponse{Meta: v45.Meta{Plugin: v45.MetaPlugin{DataParser: s.plugin}, Client: v45.MetaClient{User: uid, Group: gid}, Command: argv, Slurm: v45.SlurmMeta{Version: v45.SlurmVersion{Major: "26", Minor: "05", Micro: "4"}, Release: "26.05.4", Cluster: o.clusterName}}}
	for _, r := range rows {
		state, err := r.value("State", o)
		if err != nil {
			return err
		}
		response.Jobs = append(response.Jobs, v45.QueueJobState{JobID: r.id(), State: []string{state}})
	}
	return utils.WriteReport(out, response, o.serializer, s.encoding, s.complex)
}
