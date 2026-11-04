/**
 * Copyright (c) 2025 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ccon

import (
	"bytes"
	"io"
	"testing"
	"time"
)

// TestDetachOnPQ_SameRead verifies the canonical P-then-Q sequence delivered
// in a single Read triggers detach and the leading bytes are forwarded.
func TestDetachOnPQ_SameRead(t *testing.T) {
	inner := bytes.NewReader([]byte{'a', DetachFirstKey, DetachSecondKey, 'b'})
	d := NewDetachDetector(inner)

	buf := make([]byte, 16)
	n, err := d.Read(buf)
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}
	if got := string(buf[:n]); got != "a" {
		t.Fatalf("forwarded bytes = %q, want %q", got, "a")
	}
	select {
	case <-d.Detached():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Detached() did not close after P-Q")
	}
}

// TestDetachAcrossReads verifies the state machine survives the cross-Read
// case: a raw TTY typically returns Ctrl-P in one Read and Ctrl-Q in the
// next, separated by a short interval within DetachKeyTimeout.
func TestDetachAcrossReads(t *testing.T) {
	// First call yields the prefix including Ctrl-P.
	inner := &stepReader{
		chunks: [][]byte{
			{'a', DetachFirstKey},
			{DetachSecondKey, 'b'},
		},
	}
	d := NewDetachDetector(inner)

	buf := make([]byte, 16)
	n, err := d.Read(buf)
	if err != nil {
		t.Fatalf("first Read error: %v", err)
	}
	if got := string(buf[:n]); got != "a" {
		t.Fatalf("first forwarded = %q, want %q", got, "a")
	}

	// Second Read should observe the held P timed out... but we are still
	// within DetachKeyTimeout, so it stays armed and triggers on Q.
	n, err = d.Read(buf)
	if err != io.EOF {
		t.Fatalf("second Read err = %v, want io.EOF", err)
	}
	if n != 0 {
		t.Fatalf("second Read n = %d, want 0", n)
	}
	select {
	case <-d.Detached():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Detached() did not close across reads")
	}
}

// TestTimeoutFlushesCtrlP verifies an armed P that does not see Q within
// DetachKeyTimeout is re-emitted on the next Read. This protects ordinary
// Ctrl-P usage (e.g. readline history) when no Q follows.
func TestTimeoutFlushesCtrlP(t *testing.T) {
	inner := &stepReader{
		chunks: [][]byte{
			{DetachFirstKey},
			{'x'},
		},
	}
	d := NewDetachDetector(inner)

	buf := make([]byte, 16)
	if _, err := d.Read(buf); err != nil {
		t.Fatalf("first Read error: %v", err)
	}

	// Wait long enough for the armed P to expire.
	time.Sleep(DetachKeyTimeout + 100*time.Millisecond)

	n, err := d.Read(buf)
	if err != nil {
		t.Fatalf("second Read error: %v", err)
	}
	if got := string(buf[:n]); got != string([]byte{DetachFirstKey, 'x'}) {
		t.Fatalf("timeout flush output = %q, want %q", got, string([]byte{DetachFirstKey, 'x'}))
	}
	select {
	case <-d.Detached():
		t.Fatal("Detached() should not have closed on a single P")
	default:
	}
}

// TestPendingCtrlPFlushesOnEOF verifies a held Ctrl-P is not lost when the
// underlying input closes before a matching Ctrl-Q or timeout-triggering byte.
func TestPendingCtrlPFlushesOnEOF(t *testing.T) {
	inner := &stepReader{
		chunks: [][]byte{
			{DetachFirstKey},
		},
	}
	d := NewDetachDetector(inner)

	buf := make([]byte, 16)
	if _, err := d.Read(buf); err != nil {
		t.Fatalf("first Read error: %v", err)
	}

	n, err := d.Read(buf)
	if err != nil {
		t.Fatalf("second Read error: %v", err)
	}
	if got := string(buf[:n]); got != string([]byte{DetachFirstKey}) {
		t.Fatalf("EOF flush output = %q, want %q", got, string([]byte{DetachFirstKey}))
	}
}

// TestRepeatedCtrlPForwardsPreviousKey verifies a second Ctrl-P releases the
// first held Ctrl-P and keeps the second Ctrl-P armed for the next byte.
func TestRepeatedCtrlPForwardsPreviousKey(t *testing.T) {
	inner := bytes.NewReader([]byte{DetachFirstKey, DetachFirstKey, 'x'})
	d := NewDetachDetector(inner)

	buf := make([]byte, 16)
	n, err := d.Read(buf)
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}
	if got := string(buf[:n]); got != string([]byte{DetachFirstKey, DetachFirstKey, 'x'}) {
		t.Fatalf("forwarded bytes = %q, want %q", got, string([]byte{DetachFirstKey, DetachFirstKey, 'x'}))
	}
	select {
	case <-d.Detached():
		t.Fatal("Detached() should not close on Ctrl-P Ctrl-P x")
	default:
	}
}

// TestRepeatedCtrlPThenQDetaches verifies Ctrl-P Ctrl-P Ctrl-Q forwards the
// first Ctrl-P, then treats the second Ctrl-P plus Ctrl-Q as the detach key.
func TestRepeatedCtrlPThenQDetaches(t *testing.T) {
	inner := bytes.NewReader([]byte{'a', DetachFirstKey, DetachFirstKey, DetachSecondKey, 'b'})
	d := NewDetachDetector(inner)

	buf := make([]byte, 16)
	n, err := d.Read(buf)
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}
	if got := string(buf[:n]); got != string([]byte{'a', DetachFirstKey}) {
		t.Fatalf("forwarded bytes = %q, want %q", got, string([]byte{'a', DetachFirstKey}))
	}
	select {
	case <-d.Detached():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Detached() did not close after second Ctrl-P plus Ctrl-Q")
	}
}

// TestPostDetachEOF verifies subsequent Reads after detach return EOF
// without consulting the inner reader.
func TestPostDetachEOF(t *testing.T) {
	inner := bytes.NewReader([]byte{DetachFirstKey, DetachSecondKey})
	d := NewDetachDetector(inner)

	buf := make([]byte, 16)
	if _, err := d.Read(buf); err != io.EOF {
		t.Fatalf("first Read err = %v, want io.EOF", err)
	}

	// A second call must also return EOF cleanly.
	n, err := d.Read(buf)
	if n != 0 || err != io.EOF {
		t.Fatalf("post-detach Read = (%d, %v), want (0, io.EOF)", n, err)
	}
}

func TestShouldEnableDetachRequiresStdinTTYAndTerminal(t *testing.T) {
	tests := []struct {
		name             string
		opts             StreamOptions
		stdinIsTerminal  bool
		wantEnableDetach bool
	}{
		{
			name: "interactive TTY terminal",
			opts: StreamOptions{
				Stdin: true,
				Tty:   true,
			},
			stdinIsTerminal:  true,
			wantEnableDetach: true,
		},
		{
			name: "stdin disabled",
			opts: StreamOptions{
				Stdin: false,
				Tty:   true,
			},
			stdinIsTerminal:  true,
			wantEnableDetach: false,
		},
		{
			name: "tty disabled",
			opts: StreamOptions{
				Stdin: true,
				Tty:   false,
			},
			stdinIsTerminal:  true,
			wantEnableDetach: false,
		},
		{
			name: "piped stdin",
			opts: StreamOptions{
				Stdin: true,
				Tty:   true,
			},
			stdinIsTerminal:  false,
			wantEnableDetach: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldEnableDetach(tt.opts, tt.stdinIsTerminal); got != tt.wantEnableDetach {
				t.Fatalf("shouldEnableDetach() = %t, want %t", got, tt.wantEnableDetach)
			}
		})
	}
}

// stepReader hands out its chunks one at a time, simulating a TTY that
// returns a small number of bytes per Read call.
type stepReader struct {
	chunks [][]byte
	idx    int
}

func (s *stepReader) Read(p []byte) (int, error) {
	if s.idx >= len(s.chunks) {
		return 0, io.EOF
	}
	c := s.chunks[s.idx]
	s.idx++
	if len(c) > len(p) {
		c = c[:len(p)]
	}
	copy(p, c)
	return len(c), nil
}
