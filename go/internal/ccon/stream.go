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
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/term"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

const (
	DetachFirstKey   byte = 0x10 // Ctrl-P
	DetachSecondKey  byte = 0x11 // Ctrl-Q
	DetachKeyTimeout      = 1 * time.Second
)

// DetachDetector wraps an io.Reader, scanning for a Ctrl-P then Ctrl-Q sequence
// within DetachKeyTimeout. On match, Detached() closes and subsequent Read
// calls return io.EOF.
//
// It is intended to be placed between os.Stdin and remotecommand's
// StreamOptions.Stdin so that the streaming goroutine observes an EOF on
// stdin and shuts down the SPDY/WS stream, leaving the remote container
// running.
//
// Read is called serially by remotecommand's stdin goroutine, but Detached()
// may be selected from a different goroutine; the mutex protects the
// detached/detachCh fields from a data race.
type DetachDetector struct {
	inner  io.Reader
	first  byte
	second byte

	mu       sync.Mutex
	armed    bool
	armedAt  time.Time
	detached bool
	detachCh chan struct{}
}

func NewDetachDetector(inner io.Reader) *DetachDetector {
	return &DetachDetector{
		inner:    inner,
		first:    DetachFirstKey,
		second:   DetachSecondKey,
		detachCh: make(chan struct{}),
	}
}

// Detached returns a channel that is closed when the user has pressed
// Ctrl-P then Ctrl-Q within DetachKeyTimeout.
func (d *DetachDetector) Detached() <-chan struct{} { return d.detachCh }

func (d *DetachDetector) IsDetached() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.detached
}

func (d *DetachDetector) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	d.mu.Lock()
	if d.detached {
		d.mu.Unlock()
		return 0, io.EOF
	}
	d.mu.Unlock()

	n, err := d.inner.Read(p)
	if n <= 0 {
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.detached {
			return 0, io.EOF
		}
		if err == io.EOF && d.armed {
			d.armed = false
			p[0] = d.first
			return 1, nil
		}
		return n, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.detached {
		return 0, io.EOF
	}

	out := make([]byte, 0, n)
	for i := 0; i < n; i++ {
		b := p[i]

		// If armed has timed out, flush the held Ctrl-P into the output
		// before processing this byte. This implements the "timeout
		// auto-flush" semantics required for the cross-Read case.
		if d.armed && time.Since(d.armedAt) > DetachKeyTimeout {
			out = append(out, d.first)
			d.armed = false
		}

		if !d.armed {
			if b == d.first {
				d.armed = true
				d.armedAt = time.Now()
				continue
			}
			out = append(out, b)
			continue
		}

		// armed == true
		switch b {
		case d.second:
			d.detached = true
			close(d.detachCh)
			// Flush any pending output bytes that were accumulated
			// earlier in this same Read (e.g. the "a" in "aPQ"), then
			// signal EOF. If there is nothing to flush, return EOF
			// directly.
			nOut := copy(p, out)
			if nOut == 0 {
				return 0, io.EOF
			}
			return nOut, nil
		case d.first:
			// "P P" means the first held P did not start a detach sequence.
			// Forward it and keep the second P armed for the next byte.
			out = append(out, d.first)
			d.armed = true
			d.armedAt = time.Now()
		default:
			// Armed P was not followed by Q within the sequence; emit
			// the held P together with this byte and disarm.
			out = append(out, d.first)
			d.armed = false
			out = append(out, b)
		}
	}
	nOut := copy(p, out)
	return nOut, nil
}

func shouldEnableDetach(opts StreamOptions, stdinIsTerminal bool) bool {
	return opts.Stdin && opts.Tty && stdinIsTerminal
}

type StreamOptions struct {
	Stdin     bool
	Stdout    bool
	Stderr    bool
	Tty       bool
	Transport string // "spdy" or "ws"
}

type TerminalSizeQueue struct {
	resizeCh chan remotecommand.TerminalSize
	done     chan struct{}
}

func NewTerminalSizeQueue() *TerminalSizeQueue {
	return &TerminalSizeQueue{
		resizeCh: make(chan remotecommand.TerminalSize, 1),
		done:     make(chan struct{}),
	}
}

func (t *TerminalSizeQueue) Next() *remotecommand.TerminalSize {
	select {
	case size := <-t.resizeCh:
		return &size
	case <-t.done:
		return nil
	}
}

func (t *TerminalSizeQueue) Stop() {
	close(t.done)
}

func (t *TerminalSizeQueue) monitorResize() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGWINCH)

	go func() {
		defer signal.Stop(sigCh)
		for {
			select {
			case <-sigCh:
				size := t.getTerminalSize()
				if size != nil {
					select {
					case t.resizeCh <- *size:
					default:
					}
				}
			case <-t.done:
				return
			}
		}
	}()

	initialSize := t.getTerminalSize()
	if initialSize != nil {
		select {
		case t.resizeCh <- *initialSize:
		default:
		}
	}
}

func (t *TerminalSizeQueue) getTerminalSize() *remotecommand.TerminalSize {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		return nil
	}

	width, height, err := term.GetSize(fd)
	if err != nil {
		log.Debugf("Failed to get terminal size: %v", err)
		return nil
	}

	return &remotecommand.TerminalSize{
		Width:  uint16(width),
		Height: uint16(height),
	}
}

// streamWithURL is the single ccon streaming entry point. It preserves retry
// for ordinary stream failures and exits without retry after Ctrl-P-Q detach.
func streamWithURL(ctx context.Context, streamURL string,
	opts StreamOptions, jobID, stepID uint32) error {
	var lastErr error
	const maxRetries = 3

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Debugf("Stream attempt %d/%d for URL: %s", attempt, maxRetries, streamURL)

		streamCtx := ctx
		cancel := func() {}
		var stdin io.Reader
		var detector *DetachDetector
		if opts.Stdin {
			stdin = os.Stdin
			if shouldEnableDetach(opts, term.IsTerminal(int(os.Stdin.Fd()))) {
				streamCtx, cancel = context.WithCancel(ctx)
				detector = NewDetachDetector(os.Stdin)
				stdin = detector
				go func() {
					select {
					case <-detector.Detached():
						cancel()
					case <-streamCtx.Done():
					}
				}()
			}
		}

		err := streamOnce(streamCtx, streamURL, opts, stdin)
		cancel()
		if detector != nil && detector.IsDetached() {
			fmt.Fprintf(os.Stderr,
				"\nDetached from container %d.%d. The container is still running; "+
					"re-attach with: ccon attach %d.%d\n",
				jobID, stepID, jobID, stepID)
			return nil
		}
		if err == nil || errors.Is(err, context.Canceled) {
			return nil
		}

		lastErr = err
		log.Warnf("Stream attempt %d failed: %v", attempt, err)

		if attempt < maxRetries {
			// Wait a bit before retrying, especially for server readiness issues
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * time.Second):
			}
		}
	}

	return fmt.Errorf("stream failed after %d attempts, last error: %w", maxRetries, lastErr)
}

func streamOnce(ctx context.Context, streamURL string,
	opts StreamOptions, stdin io.Reader) error {

	parsedURL, err := url.Parse(streamURL)
	if err != nil {
		return fmt.Errorf("invalid stream URL: %w", err)
	}

	config := &rest.Config{
		Host: fmt.Sprintf("%s://%s", parsedURL.Scheme, parsedURL.Host),
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: true,
		},
	}

	var stdout, stderr io.Writer
	var tty bool

	if opts.Stdout {
		stdout = os.Stdout
	}
	if opts.Stderr && !opts.Tty {
		stderr = os.Stderr
	}
	tty = opts.Tty

	if tty && term.IsTerminal(int(os.Stdin.Fd())) {
		oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err != nil {
			return fmt.Errorf("failed to set terminal to raw mode: %w", err)
		}
		defer func() {
			if err := term.Restore(int(os.Stdin.Fd()), oldState); err != nil {
				log.Errorf("Failed to restore terminal: %v", err)
			}
		}()
	}

	streamOpts := remotecommand.StreamOptions{
		Stdin:             stdin,
		Stdout:            stdout,
		Stderr:            stderr,
		Tty:               tty,
		TerminalSizeQueue: nil,
	}

	if tty {
		sizeQueue := NewTerminalSizeQueue()
		defer sizeQueue.Stop()
		sizeQueue.monitorResize()
		streamOpts.TerminalSizeQueue = sizeQueue
	}

	executor, err := createExecutor(config, parsedURL, opts.Transport)
	if err != nil {
		return fmt.Errorf("failed to create executor: %w", err)
	}

	log.Debugf("Starting stream with URL: %s (stdin=%t, stdout=%t, stderr=%t, tty=%t)",
		streamURL, opts.Stdin, opts.Stdout, opts.Stderr, opts.Tty)

	return executor.StreamWithContext(ctx, streamOpts)
}

func createExecutor(config *rest.Config, parsedURL *url.URL, transport string) (remotecommand.Executor, error) {
	tr, err := rest.TransportFor(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	if httpTransport, ok := tr.(*http.Transport); ok {
		if httpTransport.TLSClientConfig == nil {
			httpTransport.TLSClientConfig = &tls.Config{}
		}
		httpTransport.TLSClientConfig.InsecureSkipVerify = true
	}

	switch transport {
	case "spdy":
		return createSPDYExecutor(config, parsedURL)
	case "ws":
		return createWebSocketExecutor(config, parsedURL)
	default:
		return nil, fmt.Errorf("unsupported transport: %s (supported: spdy, ws)", transport)
	}
}

func createSPDYExecutor(config *rest.Config, parsedURL *url.URL) (remotecommand.Executor, error) {
	spdyExecutor, err := remotecommand.NewSPDYExecutor(config, "POST", parsedURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create SPDY executor: %w", err)
	}
	log.Debug("Using SPDY executor")
	return spdyExecutor, nil
}

func createWebSocketExecutor(config *rest.Config, parsedURL *url.URL) (remotecommand.Executor, error) {
	wsExecutor, err := remotecommand.NewWebSocketExecutor(config, "GET", parsedURL.String())
	if err != nil {
		return nil, fmt.Errorf("failed to create WebSocket executor: %w", err)
	}
	log.Debug("Using WebSocket executor")
	return wsExecutor, nil
}
