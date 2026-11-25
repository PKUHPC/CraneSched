// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pkg/term/termios"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

type memorySource struct {
	load   func(context.Context, query) (*snapshot, error)
	closed bool
}

func (s *memorySource) Load(ctx context.Context, q query) (*snapshot, error) {
	return s.load(ctx, q)
}
func (s *memorySource) Close() error { s.closed = true; return nil }

func testRunner(source snapshotSource) runner {
	return runner{
		open: func(context.Context) (snapshotSource, error) { return source, nil },
		now:  func() time.Time { return time.Date(2026, 6, 4, 12, 34, 56, 0, time.UTC) },
		wait: waitForIteration, renderer: newTextRenderer(),
	}
}

func TestRunRepeatedStateFilters(t *testing.T) {
	for _, tc := range []struct {
		states []string
		want   string
	}{
		{[]string{"all"}, "n[01-03]\n"},
		{[]string{"down", "all"}, "n[01-03]\n"},
		{[]string{"idle,down", "all"}, "n[01-03]\n"},
		{[]string{"idle+down", "all"}, ""},
		{[]string{"idle&down", "ALL"}, ""},
		{[]string{"idle+down", "all", "idle,down"}, ""},
		{[]string{"idle+down", "all", "idle"}, "n[01-03]\n"},
		{[]string{"all", "idle+down"}, ""},
	} {
		t.Run(strings.Join(tc.states, "/"), func(t *testing.T) {
			source := &memorySource{load: func(context.Context, query) (*snapshot, error) { return testSnapshot(), nil }}
			r := testRunner(source)
			args := []string{"sinfo", "-h", "-p", "compute", "-o", "%N"}
			for _, state := range tc.states {
				args = append(args, "-t", state)
			}
			var out, diagnostic bytes.Buffer
			code := r.run(context.Background(), args, nil, &out, &diagnostic)
			if code != 0 || out.String() != tc.want || diagnostic.Len() != 0 || !source.closed {
				t.Fatalf("code=%d stdout=%q stderr=%q closed=%t; want stdout=%q", code, out.String(), diagnostic.String(), source.closed, tc.want)
			}
		})
	}
}

func TestRunSortWithMissingTelemetry(t *testing.T) {
	s := testSnapshot()
	for i := range s.Nodes {
		s.Nodes[i].TelemetryExpected = true
	}
	s.Nodes[0].FreeMemory = new(uint64(48))
	s.Nodes[1].CPULoad = new(uint64(50))
	s.Nodes[1].FreeMemory = new(uint64(0))
	s.Nodes[2].CPULoad = new(uint64(0))

	for _, tc := range []struct {
		key, column           string
		wantNodes, wantValues []string
	}{
		{"O", "%O", []string{"n03", "n02", "n01"}, []string{"0.00", "0.50", "N/A"}},
		{"-O", "%O", []string{"n01", "n02", "n03"}, []string{"N/A", "0.50", "0.00"}},
		{"e", "%e", []string{"n02", "n01", "n03"}, []string{"0", "48", "N/A"}},
		{"-e", "%e", []string{"n03", "n01", "n02"}, []string{"N/A", "48", "0"}},
	} {
		for _, format := range []string{"text", "json", "yaml"} {
			t.Run(tc.key+"/"+format, func(t *testing.T) {
				args := []string{"sinfo", "-N", "-h", "-p", "compute", "-S", tc.key}
				if format == "text" {
					args = append(args, "-o", "%N|"+tc.column)
				} else {
					args = append(args, "--"+format)
				}
				source := &memorySource{load: func(context.Context, query) (*snapshot, error) { return s, nil }}
				var out, diag bytes.Buffer
				if code := testRunner(source).run(t.Context(), args, nil, &out, &diag); code != 0 || diag.Len() != 0 {
					t.Fatalf("exit %d, stderr %q", code, &diag)
				}
				if format == "text" {
					var want []string
					for i, name := range tc.wantNodes {
						want = append(want, name+"|"+tc.wantValues[i])
					}
					if got := strings.Fields(out.String()); !slices.Equal(got, want) {
						t.Fatalf("rows = %v, want %v", got, want)
					}
					return
				}
				var doc struct {
					Sinfo []struct {
						Nodes  struct{ Nodes []string }
						CPUs   map[string]any
						Memory map[string]any
					}
				}
				decode := json.Unmarshal
				if format == "yaml" {
					decode = yaml.Unmarshal
				}
				if err := decode(out.Bytes(), &doc); err != nil {
					t.Fatal(err)
				}
				var nodes []string
				for _, row := range doc.Sinfo {
					if len(row.Nodes.Nodes) != 1 {
						t.Fatalf("expected one node per row, got %v", row.Nodes.Nodes)
					}
					name := row.Nodes.Nodes[0]
					nodes = append(nodes, name)
					_, hasLoad := row.CPUs["load"]
					_, hasFree := row.Memory["free"]
					if hasLoad != (name != "n01") || hasFree != (name != "n03") {
						t.Errorf("node %s: load present %t, free memory present %t", name, hasLoad, hasFree)
					}
				}
				if !slices.Equal(nodes, tc.wantNodes) {
					t.Fatalf("nodes = %v, want %v", nodes, tc.wantNodes)
				}
			})
		}
	}
}

func TestRunEmptyPartitionFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"filtered_state", []string{"-t", "down"}, "compute|N/A|root|root(0)\n"},
		{"filtered_nodes", []string{"-n", "n[98-99]"}, "compute|N/A|root|root(0)\n"},
		{"missing_single_node", []string{"-n", "missing"}, "compute|N/A|root|root(0)\n"},
		{"node_mode", []string{"-N", "-t", "down"}, ""},
		{"live_node_without_reason", []string{"-n", "n01"}, "compute|Crane-test|Unknown|Unknown\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testSnapshot()
			s.Nodes[0].Version = "Crane-test"
			source := &memorySource{load: func(context.Context, query) (*snapshot, error) { return s, nil }}
			r := testRunner(source)
			r.renderer.UserName = func(uid uint32) (string, bool) {
				if uid != 0 {
					t.Fatalf("unexpected reason UID: %d", uid)
				}
				return "root", true
			}
			for _, format := range [][]string{
				{"-o", "%P|%v|%u|%U"},
				{"-O", "Partition:0|,Version:0|,User:0|,UserLong:0"},
			} {
				args := append([]string{"sinfo", "-h", "-p", "compute"}, tc.args...)
				args = append(args, format...)
				var out, diagnostic bytes.Buffer
				code := r.run(context.Background(), args, nil, &out, &diagnostic)
				if code != 0 || out.String() != tc.want || diagnostic.Len() != 0 {
					t.Errorf("%v: code=%d stdout=%q stderr=%q; want stdout=%q", format, code, out.String(), diagnostic.String(), tc.want)
				}
			}
		})
	}
}

func TestRunNodeSelection(t *testing.T) {
	for _, tc := range []struct {
		name, nodes string
		flags       []string
		want        string
	}{
		{"missing", "missing", nil, ""},
		{"missing_node_view", "missing", []string{"-N"}, ""},
		{"missing_with_state", "missing", []string{"-t", "idle"}, ""},
		{"existing_filtered", "n01", []string{"-t", "down"}, ""},
		{"mixed_names", "missing,n01", nil, "1\n"},
		{"existing", "n01", nil, "1\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &memorySource{load: func(context.Context, query) (*snapshot, error) { return testSnapshot(), nil }}
			args := append([]string{"sinfo", "-h", "-p", "compute", "-o", "%D", "-n", tc.nodes}, tc.flags...)
			var out, diagnostic bytes.Buffer
			if code := testRunner(source).run(t.Context(), args, nil, &out, &diagnostic); code != 0 || out.String() != tc.want || diagnostic.Len() != 0 || !source.closed {
				t.Fatalf("code=%d stdout=%q stderr=%q closed=%t; want stdout=%q", code, &out, &diagnostic, source.closed, tc.want)
			}
		})
	}
}

func TestRunUsesTZForHeadingAndReason(t *testing.T) {
	s := testSnapshot()
	s.Nodes[0].ReasonTime = time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	source := &memorySource{load: func(context.Context, query) (*snapshot, error) { return s, nil }}
	r := testRunner(source)
	var out, diagnostic bytes.Buffer
	code := r.run(t.Context(), []string{"sinfo", "-v", "-n", "n01", "-o", "%H"}, map[string]string{"TZ": "UTC-8"}, &out, &diagnostic)
	if code != 0 || !strings.Contains(out.String(), "Thu Jun 04 20:34:56 2026\n") || !strings.Contains(out.String(), "2026-06-04T20:00:00") {
		t.Fatalf("code=%d out=%q err=%q", code, &out, &diagnostic)
	}
}

func TestStructuredParserScope(t *testing.T) {
	for _, kind := range []string{"json", "yaml"} {
		for _, parser := range []string{"list", "v0.0.41", "v0.0.42+spec_only", "data_parser/v0.0.43", "v0.0.44", "v0.0.46", "", "notreal"} {
			t.Run(kind+"/"+parser, func(t *testing.T) {
				source := &memorySource{load: func(context.Context, query) (*snapshot, error) { return testSnapshot(), nil }}
				var out, diagnostic bytes.Buffer
				code := testRunner(source).run(t.Context(), []string{"sinfo", "--" + kind + "=" + parser}, nil, &out, &diagnostic)
				if parser == "list" {
					if code != 0 || out.String() != "data_parser/v0.0.45\n" || diagnostic.String() != "Possible data_parser plugins:\n" {
						t.Fatalf("%d %q %q", code, &out, &diagnostic)
					}
				} else if code != 1 || out.Len() != 0 || !strings.Contains(diagnostic.String(), "supported: v0.0.45") {
					t.Fatalf("unsupported parser fell back: %d %q %q", code, &out, &diagnostic)
				}
			})
		}
	}
}

func TestRunActionsAndCapabilityPrecedence(t *testing.T) {
	cases := []struct {
		args     []string
		env      map[string]string
		code     int
		out, err string
	}{
		{[]string{"--version", "--not-an-option"}, nil, 0, "slurm 26.05.4\n", ""},
		{[]string{"--version"}, map[string]string{"SINFO_ALL": "", "SINFO_PARTITION": "batch"}, 1, "", "Conflicting options"},
		{[]string{"--json", "--version"}, map[string]string{"SLURM_JSON": "unknown"}, 1, "", "Unable to parse serializer"},
		{[]string{"--version", "--json"}, map[string]string{"SLURM_JSON": "unknown"}, 0, "slurm 26.05.4\n", ""},
		{[]string{"--future"}, nil, 1, "", "current Crane version does not support"},
		{[]string{"--forma"}, nil, 1, "", "sinfo: option '--format' requires an argument\nTry \"sinfo --help\" for more information\n"},
		{[]string{"-O", "Features"}, nil, 1, "", "field Features"},
		{[]string{"-S", "f"}, nil, 1, "", "sort key f"},
		{[]string{"-t", "!cloud"}, nil, 1, "", "state cloud"},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			r := testRunner(nil)
			r.open = func(context.Context) (snapshotSource, error) {
				t.Fatal("configuration opened for a local action or unsupported request")
				return nil, nil
			}
			var out, err bytes.Buffer
			code := r.run(context.Background(), append([]string{"sinfo"}, tc.args...), tc.env, &out, &err)
			if code != tc.code || out.String() != tc.out || !strings.Contains(err.String(), tc.err) || tc.err == "" && err.Len() != 0 {
				t.Fatalf("code=%d out=%q err=%q", code, &out, &err)
			}
		})
	}
}

func TestStructuredExplicitUnsupportedGrouping(t *testing.T) {
	for _, args := range [][]string{{"--json", "-O", "Features,NodeList"}, {"--yaml", "-o", "%f"}} {
		source := &memorySource{load: func(context.Context, query) (*snapshot, error) {
			t.Fatal("unsupported grouping reached controller")
			return nil, nil
		}}
		var out, diag bytes.Buffer
		rc := testRunner(source).run(t.Context(), append([]string{"sinfo"}, args...), nil, &out, &diag)
		if rc != 1 || out.Len() != 0 || !strings.Contains(diag.String(), "field Features") {
			t.Fatalf("%v: %d %q %q", args, rc, &out, &diag)
		}
	}
}

func TestRunSchemaOnlyIgnoresUnusedProjections(t *testing.T) {
	for _, kind := range []string{"json", "yaml"} {
		for _, flags := range [][]string{{"-o", "%f"}, {"-S", "f"}, {"-t", "cloud"}, {"--future"}, {"--long"}} {
			t.Run(kind+strings.Join(flags, " "), func(t *testing.T) {
				loads := 0
				source := &memorySource{load: func(context.Context, query) (*snapshot, error) {
					loads++
					return testSnapshot(), nil
				}}
				r := testRunner(source)
				r.renderer.UserName = func(uint32) (string, bool) {
					t.Fatal("schema-only output must not resolve report identities")
					return "", false
				}
				var want, out, diagnostic bytes.Buffer
				args := []string{"sinfo", "--" + kind + "=v0.0.45+spec_only"}
				if code := r.run(t.Context(), args, nil, &want, &diagnostic); code != 0 || diagnostic.Len() != 0 {
					t.Fatalf("schema baseline: code=%d stderr=%q", code, &diagnostic)
				}
				if code := r.run(t.Context(), append(args, flags...), nil, &out, &diagnostic); code != 0 || diagnostic.Len() != 0 || out.String() != want.String() || loads != 2 {
					t.Fatalf("schema with flags: code=%d loads=%d stderr=%q", code, loads, &diagnostic)
				}
				source.load = func(context.Context, query) (*snapshot, error) { return nil, errors.New("controller unavailable") }
				out.Reset()
				if code := r.run(t.Context(), append(args, flags...), nil, &out, &diagnostic); code != 1 || out.Len() != 0 || !strings.Contains(diagnostic.String(), "controller unavailable") {
					t.Fatalf("schema query failure: code=%d stdout=%q stderr=%q", code, &out, &diagnostic)
				}
			})
		}
	}
}

func TestRunReservationsOverrideUnusedProjections(t *testing.T) {
	source := &memorySource{load: func(_ context.Context, q query) (*snapshot, error) {
		if !q.ReservationsOnly {
			t.Fatal("wrong query mode")
		}
		return &snapshot{}, nil
	}}
	r := testRunner(source)
	var out, err bytes.Buffer
	code := r.run(context.Background(), []string{"sinfo", "-T", "--future", "-O", "Features", "--json=not-real"}, nil, &out, &err)
	if code != 0 || out.String() != "No reservations in the system\n" || err.Len() != 0 || !source.closed {
		t.Fatalf("code=%d out=%q err=%q closed=%t", code, &out, &err, source.closed)
	}
}

func TestRunIterationKeepsFailureAndClosesSource(t *testing.T) {
	loads, waits := 0, 0
	source := &memorySource{load: func(context.Context, query) (*snapshot, error) {
		loads++
		if loads == 1 {
			return nil, errors.New("temporary query failure")
		}
		return testSnapshot(), nil
	}}
	r := testRunner(source)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.wait = func(ctx context.Context, d time.Duration) error {
		if d != 2*time.Second {
			t.Fatalf("interval %s", d)
		}
		waits++
		if waits == 2 {
			cancel()
			return ctx.Err()
		}
		return nil
	}
	var out, err bytes.Buffer
	code := r.run(ctx, []string{"sinfo", "-h", "-i", "2junk", "-p", "compute", "-o", "%D|%N"}, nil, &out, &err)
	if code != 1 || loads != 2 || !source.closed || out.String() != "\n3|n[01-03]\n\n" || !strings.Contains(err.String(), "temporary query failure") {
		t.Fatalf("code=%d loads=%d closed=%t out=%q err=%q", code, loads, source.closed, &out, &err)
	}
}

func TestRunOutputBuffering(t *testing.T) {
	for _, tc := range []struct {
		name, format, frame string
		terminal            bool
		visible             []int
	}{
		{"pipe", "%a|%D|%t", "up|3|idle\n\n", false, []int{0, 0, 0}},
		{"terminal", "%a|%D|%t", "up|3|idle\n\n", true, []int{11, 11, 11}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reader, writer *os.File
			var err error
			if tc.terminal {
				reader, writer, err = termios.Pty()
			} else {
				reader, writer, err = os.Pipe()
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { reader.Close(); writer.Close() })
			if tc.terminal {
				attrs, err := unix.IoctlGetTermios(int(writer.Fd()), unix.TCGETS)
				if err != nil {
					t.Fatal(err)
				}
				attrs.Oflag &^= unix.OPOST
				if err := unix.IoctlSetTermios(int(writer.Fd()), unix.TCSETS, attrs); err != nil {
					t.Fatal(err)
				}
			}
			var out, diagnostic bytes.Buffer
			readVisible := func(want int) {
				t.Helper()
				poll := []unix.PollFd{{Fd: int32(reader.Fd()), Events: unix.POLLIN}}
				for remaining := want; ; {
					pending, err := unix.IoctlGetInt(int(reader.Fd()), unix.TIOCINQ)
					if err != nil || pending > remaining {
						t.Fatalf("readable=%d remaining=%d err=%v", pending, remaining, err)
					}
					if _, err := io.CopyN(&out, reader, int64(pending)); err != nil {
						t.Fatal(err)
					}
					remaining -= pending
					if remaining == 0 {
						break
					}
					if n, err := unix.Poll(poll, 1000); err != nil || n != 1 {
						t.Fatalf("output not readable: n=%d err=%v", n, err)
					}
				}
			}
			iterations := 0
			source := &memorySource{load: func(context.Context, query) (*snapshot, error) { return testSnapshot(), nil }}
			r := testRunner(source)
			r.wait = func(context.Context, time.Duration) error {
				readVisible(tc.visible[iterations])
				iterations++
				if iterations == len(tc.visible) {
					return io.EOF
				}
				return nil
			}
			code := r.run(t.Context(), []string{"sinfo", "-h", "-p", "compute", "-o", tc.format, "-i", "1"}, nil, writer, &diagnostic)
			readVisible(len(tc.frame)*iterations - out.Len())
			if want := strings.Repeat(tc.frame, iterations); code != 0 || out.String() != want || diagnostic.Len() != 0 || !source.closed {
				t.Fatalf("code=%d output bytes=%d want=%d stderr=%q closed=%t", code, out.Len(), len(want), &diagnostic, source.closed)
			}
		})
	}
}

func TestRunOutputFlushFailure(t *testing.T) {
	for _, tc := range []struct{ name, format string }{
		{"final_flush", "%D"},
		{"during_write", "%D" + strings.Repeat("x", 5000)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, writer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			reader.Close()
			t.Cleanup(func() { writer.Close() })
			source := &memorySource{load: func(context.Context, query) (*snapshot, error) { return testSnapshot(), nil }}
			var diagnostic bytes.Buffer
			code := testRunner(source).run(t.Context(), []string{"sinfo", "-h", "-p", "compute", "-o", tc.format}, nil, writer, &diagnostic)
			if code != 1 || strings.Count(diagnostic.String(), "broken pipe") != 1 || !source.closed {
				t.Fatalf("code=%d stderr=%q closed=%t", code, &diagnostic, source.closed)
			}
		})
	}
}

func TestRunProjectionFailureDoesNotPublishHeader(t *testing.T) {
	s := testSnapshot()
	s.Nodes[0].FractionalAllocation = true
	source := &memorySource{load: func(context.Context, query) (*snapshot, error) { return s, nil }}
	r := testRunner(source)
	var out, err bytes.Buffer
	if code := r.run(context.Background(), []string{"sinfo", "-o", "%C"}, nil, &out, &err); code != 1 || out.Len() != 0 || !strings.Contains(err.String(), "fractional") {
		t.Fatalf("code=%d out=%q err=%q", code, &out, &err)
	}
}

func TestCancellationInterruptsQueryAndTimer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	source := &memorySource{load: func(ctx context.Context, _ query) (*snapshot, error) {
		cancel()
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	r := testRunner(source)
	if code := r.run(ctx, []string{"sinfo", "-i", "100"}, nil, io.Discard, io.Discard); code != 0 || !source.closed {
		t.Fatalf("code=%d closed=%t", code, source.closed)
	}
	if err := waitForIteration(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
