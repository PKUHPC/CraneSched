// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package squeue

import (
	_ "embed"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// These names, headings and short codes are the observable 26.05.4 interface.
//
//go:embed job-fields.txt
var jobFields string

//go:embed step-fields.txt
var stepFields string

type field struct {
	name, header string
	code         byte
}
type catalog struct {
	ordered []field
	names   map[string]field
	codes   map[byte]field
}

func registry(data string, step bool) catalog {
	r := catalog{names: map[string]field{}, codes: map[byte]field{}}
	for line := range strings.SplitSeq(strings.TrimSpace(data), "\n") {
		v := strings.Fields(line)
		f := field{name: v[0], header: v[1]}
		if len(v) == 3 {
			f.code = v[2][0]
			r.codes[f.code] = f
		}
		r.names[strings.ToLower(f.name)] = f
		r.ordered = append(r.ordered, f)
	}
	// Short-only fields remain accepted even though --helpformat omits them.
	if step {
		r.codes['b'] = field{"tres-per-step", "TRES_PER_JOB", 'b'}
	} else {
		r.codes['b'] = field{"tres-per-node", "TRES_PER_NODE", 'b'}
	}
	return r
}

var jobs = registry(jobFields, false)
var steps = registry(stepFields, true)

func (o *options) catalog() catalog {
	if o.steps {
		return steps
	}
	return jobs
}
func (o *options) helpFields(short bool) string {
	var b strings.Builder
	n, cols := 0, 4
	if short {
		cols = 8
	}
	for _, f := range o.catalog().ordered {
		if short && f.code == 0 {
			continue
		}
		if n > 0 && n%cols == 0 {
			b.WriteByte('\n')
		}
		if short {
			fmt.Fprintf(&b, "%%%-5c", f.code)
		} else {
			fmt.Fprintf(&b, "%-20s", f.name)
		}
		n++
	}
	b.WriteByte('\n')
	return b.String()
}

type column struct {
	field  field
	width  int
	right  bool
	suffix string
}

func width(s string, defaultWidth int) (int, bool, string) {
	right := strings.HasPrefix(s, ".")
	if right {
		s = s[1:]
	}
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 {
		return defaultWidth, right, s
	}
	v, _ := strconv.ParseUint(s[:i], 10, 32)
	return int(v), right, s[i:]
}
func (o *options) columns(diag io.Writer) ([]column, error) {
	cat := o.catalog()
	var cols []column
	if o.customLong {
		for part := range strings.SplitSeq(o.longFormat, ",") {
			name, tail, has := strings.Cut(part, ":")
			w, right, suffix := 20, false, ""
			if has {
				w, right, suffix = width(tail, 0)
			}
			f, ok := cat.names[strings.ToLower(name)]
			if !ok {
				fmt.Fprintf(diag, "squeue: error: Invalid job format specification: %s\n", name)
				w = 0
			}
			cols = append(cols, column{f, w, right, suffix})
		}
	} else {
		spec := o.format
		if !o.customFormat {
			switch {
			case o.steps:
				spec = "%.15i %.8j %.9P %.8u %.9M %N"
			case o.onlyState:
				spec = "%.18i %.2t"
			case o.start:
				spec = "%.18i %.9P %.8j %.8u %.2t %.19S %.6D %20Y %R"
			case o.long:
				spec = "%.18i %.9P %.8j %.8u %.8T %.10M %.9l %.6D %R"
			default:
				spec = "%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R"
			}
		}
		prefix, rest, has := strings.Cut(spec, "%")
		if prefix != "" {
			cols = append(cols, column{suffix: prefix})
		}
		if has && strings.EqualFold(rest, "all") {
			for _, code := range "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" {
				if f, ok := cat.codes[byte(code)]; ok {
					cols = append(cols, column{field: f, suffix: "|"})
				}
			}
		} else if has {
			for chunk := range strings.SplitSeq(rest, "%") {
				if chunk == "" {
					continue
				}
				w, right, tail := width(chunk, 0)
				if tail == "" {
					continue
				}
				f, ok := cat.codes[tail[0]]
				if !ok {
					kind := "job"
					if o.steps {
						kind = "job step"
					}
					fmt.Fprintf(diag, "squeue: error: Invalid %s format specification: %c\n", kind, tail[0])
					cols = append(cols, column{suffix: "%" + chunk})
					continue
				}
				cols = append(cols, column{f, w, right, tail[1:]})
			}
		}
	}
	for _, c := range cols {
		if c.width > 1<<20 {
			return nil, fmt.Errorf("squeue: error: field width exceeds output limit")
		}
		if c.field.name != "" && !supported(c.field.name) {
			return nil, unsupported("field " + c.field.name)
		}
	}
	return cols, nil
}

func printLine(w io.Writer, cols []column, r *row, o *options) error {
	var b strings.Builder
	for _, c := range cols {
		v := c.field.header
		if r != nil && c.field.name != "" {
			var err error
			v, err = r.value(c.field.name, o)
			if err != nil {
				return err
			}
		}
		if c.width > 0 {
			if len(v) > c.width {
				v = v[:c.width]
			}
			pad := strings.Repeat(" ", c.width-len(v))
			if c.right {
				b.WriteString(pad)
				b.WriteString(v)
			} else {
				b.WriteString(v)
				b.WriteString(pad)
			}
		} else {
			b.WriteString(v)
		}
		b.WriteString(c.suffix)
	}
	b.WriteByte('\n')
	_, err := io.WriteString(w, b.String())
	return err
}
