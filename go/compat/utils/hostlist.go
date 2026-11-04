// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package utils

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

// The grammar preserves numeric spelling: leading zeroes are part of a name.
// Expansion is bounded separately from parsing and is never needed for Match.
type HostList struct{ hosts []*hostPattern }

type hostSyntax struct {
	Hosts []*hostPattern `parser:"Separator* (@@ Separator*)*"`
}
type hostPattern struct {
	Segments []*hostSegment `parser:"@@+"`
}
type hostSegment struct {
	Literal string       `parser:"@Text"`
	Ranges  []*hostRange `parser:"| '[' @@ (',' @@)* ']'"`
}
type hostRange struct {
	Raw       string `parser:"@Range?"`
	low, high uint64
	width     int
}

var hostParser = participle.MustBuild[hostSyntax](participle.Lexer(lexer.MustStateful(lexer.Rules{
	"Root": {
		{Name: "Open", Pattern: `\[`, Action: lexer.Push("Range")},
		{Name: "Separator", Pattern: `[,\s]+`},
		{Name: "Text", Pattern: `[^\[\],\s]+`},
	},
	"Range": {
		{Name: "Close", Pattern: `\]`, Action: lexer.Pop()},
		{Name: "Comma", Pattern: `,`},
		{Name: "Range", Pattern: `[^\],]+`},
	},
})))

func ParseHostList(input string) (*HostList, error) {
	result, err := hostParser.ParseString("", input)
	if err != nil {
		return nil, err
	}
	for _, pattern := range result.Hosts {
		for _, segment := range pattern.Segments {
			for _, r := range segment.Ranges {
				if err := r.decode(); err != nil {
					return nil, err
				}
			}
		}
	}
	return &HostList{hosts: result.Hosts}, nil
}

// Keep each range's spelling until semantic decoding. The reference accepts
// whitespace and a leading plus; the raw lower endpoint controls name padding.
type HostRangeError struct{ message string }

func (e *HostRangeError) Error() string { return e.message }

func (r *hostRange) decode() error {
	invalid := func() error {
		return &HostRangeError{fmt.Sprintf("_parse_single_range: Invalid range: `%s'", r.Raw)}
	}
	left, right, hasDash := strings.Cut(r.Raw, "-")
	r.width = len(left)
	low, remainder, ok := hostEndpoint(left)
	if !ok || strings.Contains(r.Raw, "x") || strings.HasPrefix(right, "-") {
		return invalid()
	}
	high := low
	if hasDash && right != "" {
		high, remainder, ok = hostEndpoint(right)
	}
	if !ok || remainder != "" || high < low {
		return invalid()
	}
	// Subtraction avoids overflow at the unsigned-long boundary. Matching never
	// expands a hostlist, even for multiple large Cartesian dimensions.
	if high-low >= 65536 {
		return &HostRangeError{fmt.Sprintf("_parse_single_range: Too many hosts in range `%s'", r.Raw)}
	}
	r.low, r.high = low, high
	return nil
}

func hostEndpoint(raw string) (uint64, string, bool) {
	text := strings.TrimLeft(raw, " \t\r\n\v\f")
	text = strings.TrimPrefix(text, "+")
	end := 0
	for end < len(text) && text[end] >= '0' && text[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, text, false
	}
	value, err := strconv.ParseUint(text[:end], 10, 64)
	return value, text[end:], err == nil
}

func (h *HostList) Match(name string) bool {
	for _, pattern := range h.hosts {
		if matchSegments(pattern.Segments, name) {
			return true
		}
	}
	return false
}

func matchSegments(segments []*hostSegment, name string) bool {
	// Adjacent numeric segments can reach the same suffix through different
	// splits. Evaluate each failed (segment, byte offset) only once per name.
	type position struct{ segment, offset int }
	failed := make(map[position]bool)
	var match func(int, int) bool
	match = func(index, offset int) bool {
		if index == len(segments) {
			return offset == len(name)
		}
		pos := position{index, offset}
		if failed[pos] {
			return false
		}
		segment, rest := segments[index], name[offset:]
		if segment.Literal != "" {
			if strings.HasPrefix(rest, segment.Literal) && match(index+1, offset+len(segment.Literal)) {
				return true
			}
		} else {
			for end := 1; end <= len(rest) && rest[end-1] >= '0' && rest[end-1] <= '9'; end++ {
				value, err := strconv.ParseUint(rest[:end], 10, 64)
				if err != nil {
					break
				}
				for _, r := range segment.Ranges {
					if value >= r.low && value <= r.high && fmt.Sprintf("%0*d", r.width, value) == rest[:end] && match(index+1, offset+end) {
						return true
					}
				}
			}
		}
		failed[pos] = true
		return false
	}
	return match(0, 0)
}

func (h *HostList) Single() (string, bool) {
	if h == nil || len(h.hosts) != 1 {
		return "", false
	}
	var name strings.Builder
	for _, segment := range h.hosts[0].Segments {
		if segment.Literal != "" {
			name.WriteString(segment.Literal)
			continue
		}
		if len(segment.Ranges) != 1 {
			return "", false
		}
		r := segment.Ranges[0]
		if r.low != r.high {
			return "", false
		}
		fmt.Fprintf(&name, "%0*d", r.width, r.low)
	}
	return name.String(), true
}

// NaturalLess compares digit runs without integer conversion. If either run
// starts with zero, Slurm orders the digits lexically rather than by magnitude.
func NaturalLess(a, b string) bool {
	for i, j := 0, 0; ; {
		if i == len(a) || j == len(b) {
			return i == len(a) && j != len(b)
		}
		if a[i] >= '0' && a[i] <= '9' && b[j] >= '0' && b[j] <= '9' {
			ai, bj := i, j
			for i < len(a) && a[i] >= '0' && a[i] <= '9' {
				i++
			}
			for j < len(b) && b[j] >= '0' && b[j] <= '9' {
				j++
			}
			x, y := a[ai:i], b[bj:j]
			if x[0] != '0' && y[0] != '0' && len(x) != len(y) {
				return len(x) < len(y)
			}
			if x != y {
				return x < y
			}
			continue
		}
		if a[i] != b[j] {
			return a[i] < b[j]
		}
		i++
		j++
	}
}

type nameSuffix struct {
	prefix, digits string
	value          uint64
	width          int
}

func splitName(name string) nameSuffix {
	end := len(name)
	start := end
	for start > 0 && name[start-1] >= '0' && name[start-1] <= '9' {
		start--
	}
	if start == end {
		return nameSuffix{prefix: name}
	}
	digits := name[start:end]
	value, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return nameSuffix{prefix: name}
	}
	width := 0
	if len(digits) > 1 && digits[0] == '0' {
		width = len(digits)
	}
	return nameSuffix{name[:start], digits, value, width}
}

func CompressHosts(names []string) string {
	unique := make(map[string]bool, len(names))
	ordered := make([]string, 0, len(names))
	for _, name := range names {
		if name != "" && !unique[name] {
			unique[name] = true
			ordered = append(ordered, name)
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return HostLess(ordered[i], ordered[j]) })
	var output []string
	for i := 0; i < len(ordered); {
		first := splitName(ordered[i])
		j := i + 1
		if first.digits != "" {
			for j < len(ordered) {
				next := splitName(ordered[j])
				if next.digits == "" || next.prefix != first.prefix {
					break
				}
				j++
			}
		}
		if j == i+1 {
			output = append(output, ordered[i])
			i = j
			continue
		}
		var ranges []string
		for k := i; k < j; {
			start := splitName(ordered[k])
			end := k + 1
			for end < j {
				next := splitName(ordered[end])
				if next.value != splitName(ordered[end-1]).value+1 || fmt.Sprintf("%0*d", start.width, next.value) != next.digits {
					break
				}
				end++
			}
			value := start.digits
			if end > k+1 {
				value += "-" + splitName(ordered[end-1]).digits
			}
			ranges = append(ranges, value)
			k = end
		}
		output = append(output, first.prefix+"["+strings.Join(ranges, ",")+"]")
		i = j
	}
	return strings.Join(output, ",")
}

func HostLess(a, b string) bool {
	x, y := splitName(a), splitName(b)
	if x.prefix != y.prefix {
		return NaturalLess(x.prefix, y.prefix)
	}
	if len(x.digits) != len(y.digits) {
		return len(x.digits) < len(y.digits)
	}
	return x.value < y.value
}
