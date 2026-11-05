// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
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

func TestRunEmptyPartitionFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"filtered_state", []string{"-t", "down"}, "compute|N/A|root|root(0)\n"},
		{"filtered_nodes", []string{"-n", "n[98-99]"}, "compute|N/A|root|root(0)\n"},
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
