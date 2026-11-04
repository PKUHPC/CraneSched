// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package testutil

import (
	"io"
	"os"
	"testing"
)

// CaptureStdout captures sequential command tests. It must not be used by
// parallel tests because os.Stdout is process-global.
func CaptureStdout(t *testing.T, run func() error) (string, error) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stdout
	os.Stdout = writer
	writerClosed := false
	defer func() {
		os.Stdout = original
		if !writerClosed {
			if err := writer.Close(); err != nil {
				t.Errorf("close stdout writer: %v", err)
			}
		}
		if err := reader.Close(); err != nil {
			t.Errorf("close stdout reader: %v", err)
		}
	}()
	type capture struct {
		data []byte
		err  error
	}
	result := make(chan capture, 1)
	go func() {
		data, err := io.ReadAll(reader)
		result <- capture{data: data, err: err}
	}()
	runErr := run()
	err = writer.Close()
	writerClosed = true
	if err != nil {
		t.Fatalf("close stdout writer: %v", err)
	}
	output := <-result
	if output.err != nil {
		t.Fatalf("read captured stdout: %v", output.err)
	}
	return string(output.data), runErr
}
