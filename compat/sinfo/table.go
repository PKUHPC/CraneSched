// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"CraneFrontEnd/compat/utils"
	"fmt"
	"io"
	"os/user"
	"strconv"
	"strings"
	"time"
)

// Text presentation is separate from selection and aggregation. A serializer
// uses the same rows without parsing a formatted table back into values.
type textRenderer struct {
	TimeFormat func(time.Time) string
	UserName   func(uint32) (string, bool)
}

func newTextRenderer() textRenderer {
	return textRenderer{
		TimeFormat: func(t time.Time) string { return t.Format("2006-01-02T15:04:05") },
		UserName: func(uid uint32) (string, bool) {
			u, err := user.LookupId(strconv.FormatUint(uint64(uid), 10))
			if err != nil {
				return "", false
			}
			return u.Username, true
		},
	}
}

func (r textRenderer) table(w io.Writer, rows []*row, o *plan) error {
	if err := validateProjection(rows, o); err != nil {
		return err
	}
	partWidth, nodeWidth := 9, 9
	for _, entry := range rows {
		if entry.Partition != nil {
			width := len(entry.Partition.Name)
			if entry.Partition.Default {
				width++
			}
			partWidth = max(partWidth, width)
		}
		nodeWidth = max(nodeWidth, len(utils.CompressHosts(entry.names(""))))
	}
	writeRow := func(entry *row) error {
		var line strings.Builder
		for _, item := range o.Fields {
			if item.Field == nil {
				line.WriteString(item.Literal)
				line.WriteString(item.Suffix)
				continue
			}
			width := item.Width
			if o.AutoPartitionWidth && item.Field.Group == "partition" {
				width = partWidth
			}
			if o.AutoNodeWidth && (item.Field.Name == "NodeList" || item.Field.Name == "NodeHost") {
				width = nodeWidth
			}
			text := item.Field.Header
			if entry != nil {
				text = r.value(entry, item.Field, o, width)
			}
			cut := entry != nil || item.Field.Name != "NodeList" && item.Field.Name != "NodeHost" && item.Field.Name != "NodeAddr"
			line.WriteString(column(text, width, item.Right, cut))
			line.WriteString(item.Suffix)
		}
		line.WriteByte('\n')
		_, err := io.WriteString(w, line.String())
		return err
	}
	if !o.NoHeader {
		if err := writeRow(nil); err != nil {
			return err
		}
	}
	for _, entry := range rows {
		if err := writeRow(entry); err != nil {
			return err
		}
	}
	return nil
}

func (r textRenderer) value(entry *row, field *fieldSpec, o *plan, width int) string {
	p := entry.Partition
	// Empty partition rows retain the aggregate's zero UID. A real node
	// without a reason instead has an unset UID and renders as Unknown.
	n := &node{ReasonUID: new(uint32)}
	if len(entry.Nodes) != 0 {
		n = entry.Nodes[0]
	}
	nodes, cpus := entry.counts()
	nullString := func(s string) string {
		if s == "" {
			return "(null)"
		}
		return s
	}
	if _, isPartition := p.groupValue(field.Group); isPartition && p == nil {
		return "n/a"
	}
	switch field.Name {
	case "Partition", "PartitionName":
		if p == nil {
			return "n/a"
		}
		name := p.Name
		if field.Name == "Partition" && p.Default {
			if width > 0 && len(name) >= width {
				name = name[:width-1] + "*"
			} else {
				name += "*"
			}
		}
		return name
	case "Available":
		return p.Available
	case "Time":
		return limitString(p.TimeLimit)
	case "DefaultTime":
		return limitString(p.DefaultTime)
	case "Size":
		lo := p.MinNodes
		if lo == 0 && p.MaxNodes > 0 {
			lo = 1
		}
		low := unitNumber(lo, o.NoConvert)
		if lo == p.MaxNodes {
			return low
		}
		if p.MaxNodes == 0xffffffff {
			return low + "-infinite"
		}
		return low + "-" + unitNumber(p.MaxNodes, o.NoConvert)
	case "Groups":
		if p.Groups == "" {
			return "all"
		}
		return p.Groups
	case "AllocNodes":
		if p.AllocNodes == "" {
			return "all"
		}
		return p.AllocNodes
	case "OverSubscribe", "Share":
		return p.OverSubscribe
	case "PreemptMode":
		return p.PreemptMode
	case "PriorityJobFactor":
		return strconv.FormatUint(p.PriorityFactor, 10)
	case "PriorityTier":
		return strconv.FormatUint(p.PriorityTier, 10)
	case "Root":
		if p.RootOnly {
			return "yes"
		}
		return "no"
	case "MaxCPUsPerNode":
		if p.MaxCPUs == 0xffffffff {
			return "UNLIMITED"
		}
		return strconv.FormatUint(p.MaxCPUs, 10)
	case "Nodes":
		return strconv.FormatUint(nodes.Total, 10)
	case "NodeAI":
		return fmt.Sprintf("%d/%d", nodes.Alloc, nodes.Idle)
	case "NodeAIOT":
		return nodes.String()
	case "CPUsState":
		return cpus.String()
	case "NodeList":
		return utils.CompressHosts(entry.names(""))
	case "NodeAddr":
		return utils.CompressHosts(entry.names("address"))
	case "NodeHost":
		return utils.CompressHosts(entry.names("hostname"))
	case "StateCompact", "StateComplete", "StateLong":
		if nodes.Total == 0 {
			return "n/a"
		}
		switch field.Name {
		case "StateCompact":
			return n.State.compact()
		case "StateComplete":
			return strings.ToLower(n.State.complete())
		default:
			return n.State.long()
		}
	case "Reason":
		if n.Reason == "" || strings.HasPrefix(n.Reason, "(null)") {
			return "none"
		}
		return n.Reason
	case "Reservation":
		return n.Reservation
	case "TimeStamp":
		if n.ReasonTime.IsZero() || n.ReasonTime.Unix() == 0 {
			return "Unknown"
		}
		return r.TimeFormat(n.ReasonTime)
	case "User", "UserLong":
		if n.ReasonUID == nil {
			return "Unknown"
		}
		name, ok := r.UserName(*n.ReasonUID)
		if !ok {
			name = "Unk"
		}
		if !ok || field.Name == "UserLong" {
			name += fmt.Sprintf("(%d)", *n.ReasonUID)
		}
		return name
	case "Features":
		return nullString(n.Features)
	case "features_act":
		return nullString(n.ActiveFeatures)
	case "Gres":
		return nullString(n.Gres)
	case "GresUsed":
		return nullString(n.GresUsed)
	case "Comment":
		return nullString(n.Comment)
	case "Extra":
		return nullString(n.Extra)
	case "Version":
		if n.Version == "" {
			return "N/A"
		}
		return n.Version
	case "Cluster":
		if entry.Cluster == "" {
			return "N/A"
		}
		return entry.Cluster
	case "SocketCoreThread":
		return numericRange(entry, "sockets", o) + ":" + numericRange(entry, "cores", o) + ":" + numericRange(entry, "threads", o)
	case "Port":
		return unitNumber(n.Port, o.NoConvert)
	case "AllocMem":
		if len(entry.Nodes) == 0 {
			return "0"
		}
		return strconv.FormatUint(entry.Nodes[len(entry.Nodes)-1].AllocMemory, 10)
	default:
		return numericRange(entry, field.Group, o)
	}
}

func numericRange(entry *row, group string, o *plan) string {
	lo, hi := entry.minmax(group)
	format := func(value uint64) string {
		switch group {
		case "cpus", "sockets", "cores", "threads":
			return unitNumber(value, o.NoConvert)
		case "load":
			if value == 0xfffffffe {
				return "N/A"
			}
			return fmt.Sprintf("%.2f", float64(value)/100)
		case "freemem", "memory":
			if value == 0xfffffffffffffffe {
				return "N/A"
			}
		}
		return strconv.FormatUint(value, 10)
	}
	text := format(lo)
	if lo != hi {
		if group == "load" || group == "freemem" {
			text += "-" + format(hi)
		} else {
			text += "+"
		}
	}
	return text
}

func unitNumber(n uint64, noConvert bool) string {
	units := " KMGTPE"
	unit := 0
	value := float64(float32(n))
	if !noConvert {
		for value >= 1024 && uint64(value)%512 == 0 && unit < len(units)-1 {
			value /= 1024
			unit++
		}
	}
	text := strconv.FormatUint(uint64(value), 10)
	if value != float64(uint64(value)) {
		text = fmt.Sprintf("%.2f", value)
	}
	if unit != 0 {
		text += string(units[unit])
	}
	return text
}

func limitString(limit durationLimit) string {
	if limit.Unset {
		return "n/a"
	}
	if limit.Infinite {
		return "infinite"
	}
	return secondsString(limit.Seconds)
}

func secondsString(seconds uint64) string {
	days, hours, minutes, secs := seconds/86400, (seconds/3600)%24, (seconds/60)%60, seconds%60
	if days != 0 {
		return fmt.Sprintf("%d-%02d:%02d:%02d", days, hours, minutes, secs)
	}
	if hours != 0 {
		return fmt.Sprintf("%d:%02d:%02d", hours, minutes, secs)
	}
	return fmt.Sprintf("%d:%02d", minutes, secs)
}

func reservationDuration(seconds uint64) string {
	if seconds >= 86400 {
		return secondsString(seconds)
	}
	return fmt.Sprintf("%02d:%02d:%02d", seconds/3600, seconds/60%60, seconds%60)
}

func (r textRenderer) reservations(w io.Writer, reservations []reservation, noHeader bool, now time.Time) error {
	if len(reservations) == 0 {
		_, err := io.WriteString(w, "No reservations in the system\n")
		return err
	}
	width := 9
	if !noHeader {
		for _, res := range reservations {
			width = max(width, len(res.Name))
		}
	}
	line := func(name, state, start, end, duration, nodes string) error {
		_, err := fmt.Fprintf(w, "%-*s  %8s  %19s  %19s  %11s  %s\n", width, name, state, start, end, duration, nodes)
		return err
	}
	if !noHeader {
		if err := line("RESV_NAME", "STATE", "START_TIME", "END_TIME", "DURATION", "NODELIST"); err != nil {
			return err
		}
	}
	for _, res := range reservations {
		state := "INACTIVE"
		if res.Start.Unix() <= now.Unix() && now.Unix() <= res.End.Unix() {
			state = "ACTIVE"
		}
		if err := line(res.Name, state, r.TimeFormat(res.Start), r.TimeFormat(res.End), reservationDuration(uint64(res.End.Unix()-res.Start.Unix())), res.Nodes); err != nil {
			return err
		}
	}
	return nil
}

func timeFormatter(format string, now time.Time, diagnostics io.Writer) func(time.Time) string {
	return func(t time.Time) string {
		switch t.Unix() {
		case 0, 0xffffffff:
			return "Unknown"
		case 0xfffffffe:
			return "None"
		}
		if t.IsZero() {
			return "Unknown"
		}
		t = t.In(now.Location())
		pattern := "%FT%T"
		switch format {
		case "", "standard":
		case "relative":
			// The reference uses a year/day-of-year key, including its deliberate
			// year-boundary discontinuity. now is fixed at command startup.
			distance := (t.Year()-now.Year())*1000 + t.YearDay() - now.YearDay()
			switch {
			case distance == -1:
				pattern = "Ystday %H:%M"
			case distance == 0:
				pattern = "%H:%M:%S"
			case distance == 1:
				pattern = "Tomorr %H:%M"
			case distance < -365 || distance > 365:
				pattern = "%-d %b %Y"
			case distance < -1 || distance > 6:
				pattern = "%-d %b %H:%M"
			default:
				pattern = "%a %H:%M"
			}
		default:
			if !strings.Contains(format, "%") || len(format) >= 32 {
				fmt.Fprintf(diagnostics, "sinfo: error: invalid SLURM_TIME_FORMAT = '%s'\n", format)
			} else {
				pattern = format
			}
		}
		value := utils.Strftime(pattern, t)
		if len(value) >= 256 || value == "" {
			return strings.Repeat("#", 255)
		}
		return value
	}
}
