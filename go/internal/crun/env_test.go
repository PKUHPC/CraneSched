/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

package crun

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
)

type memoryRequestRecorder struct {
	protos.UnimplementedCraneForeDServer
	requests chan *protos.StreamCrunRequest
}

func (server *memoryRequestRecorder) CrunStream(stream protos.CraneForeD_CrunStreamServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	select {
	case server.requests <- request:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	return stream.Send(&protos.StreamCrunReply{
		Type: protos.StreamCrunReply_STEP_ID_REPLY,
		Payload: &protos.StreamCrunReply_PayloadStepIdReply{
			PayloadStepIdReply: &protos.StreamCrunReply_StepIdReply{
				Ok: false, FailureReason: "test captured submission",
			},
		},
	})
}

func TestCrunMemoryCommandProcess(t *testing.T) {
	if os.Getenv("CRUN_MEMORY_TEST_PROCESS") != "1" {
		return
	}
	for index, argument := range os.Args {
		if argument == "--" {
			RootCmd.SetArgs(os.Args[index+1:])
			ParseCmdArgs()
			return
		}
	}
	t.Fatal("missing command argument separator")
}

func TestCrunMemorySubmission(t *testing.T) {
	for _, test := range []struct {
		name      string
		env       []string
		flags     []string
		wantNode  uint64
		wantCPU   uint64
		wantError string
		wantCode  int
	}{
		{name: "inherit_512M", env: []string{"CRANE_MEM_PER_NODE=512M"}, flags: []string{"--nodes=1", "--ntasks=4"}, wantNode: 536870912},
		{name: "explicit_mem_overrides_env", env: []string{"CRANE_MEM_PER_NODE=512M"}, flags: []string{"--mem=100M"}, wantNode: 104857600},
		{name: "explicit_mem_per_cpu_overrides_env", env: []string{"CRANE_MEM_PER_NODE=512M"}, flags: []string{"--mem-per-cpu=64M"}, wantCPU: 67108864},
		{name: "unset_memory_stays_unspecified"},
		{name: "invalid_memory_rejected", env: []string{"CRANE_MEM_PER_NODE=invalid"}, wantError: "invalid CRANE_MEM_PER_NODE from env", wantCode: util.ErrorInvalidFormat},
		{name: "invalid_memory_rejected_with_mem", env: []string{"CRANE_MEM_PER_NODE=invalid"}, flags: []string{"--mem=100M"}, wantError: "invalid CRANE_MEM_PER_NODE from env", wantCode: util.ErrorInvalidFormat},
		{name: "invalid_memory_rejected_with_mem_per_cpu", env: []string{"CRANE_MEM_PER_NODE=invalid"}, flags: []string{"--mem-per-cpu=64M"}, wantError: "invalid CRANE_MEM_PER_NODE from env", wantCode: util.ErrorInvalidFormat},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			listener, err := net.Listen("unix", filepath.Join(directory, "s"))
			if err != nil {
				t.Fatal(err)
			}
			recorder := &memoryRequestRecorder{requests: make(chan *protos.StreamCrunRequest, 1)}
			server := grpc.NewServer()
			protos.RegisterCraneForeDServer(server, recorder)
			serveDone := make(chan error, 1)
			go func() { serveDone <- server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				if err := <-serveDone; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
					t.Errorf("serve test cfored: %v", err)
				}
			})
			configPath := filepath.Join(directory, "config.yaml")
			config := fmt.Sprintf("CraneBaseDir: %q\nCranedCforedSockPath: s\nCraneCtldForInternalListenPort: 10013\n", directory)
			if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
				t.Fatal(err)
			}
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			args := []string{"-test.run=^TestCrunMemoryCommandProcess$", "--", "--config", configPath}
			args = append(args, test.flags...)
			command := exec.CommandContext(ctx, executable, append(args, "hostname")...)
			for _, variable := range os.Environ() {
				if !strings.HasPrefix(variable, "CRANE_") && !strings.HasPrefix(variable, "CRUN_MEMORY_TEST_PROCESS=") {
					command.Env = append(command.Env, variable)
				}
			}
			command.Env = append(command.Env, "CRUN_MEMORY_TEST_PROCESS=1", "CRANE_NTASKS=4", "CRANE_JOB_NUM_NODES=1", "CRANE_NTASKS_PER_NODE=4")
			command.Env = append(command.Env, "CRANE_JOB_ID=42")
			command.Env = append(command.Env, test.env...)
			output, err := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("crun timed out: %s", output)
			}
			wantCode := util.ErrorBackend
			wantError := "test captured submission"
			if test.wantError != "" {
				wantCode, wantError = test.wantCode, test.wantError
			}
			var exitError *exec.ExitError
			if !errors.As(err, &exitError) || exitError.ExitCode() != wantCode || !strings.Contains(string(output), wantError) {
				t.Fatalf("crun error=%v output=%s; want exit=%d and %q", err, output, wantCode, wantError)
			}
			if test.wantError != "" {
				select {
				case request := <-recorder.requests:
					t.Fatalf("invalid arguments reached cfored: %v", request)
				default:
				}
				return
			}
			var request *protos.StreamCrunRequest
			select {
			case request = <-recorder.requests:
			default:
				t.Fatal("crun did not send a submission")
			}
			step := request.GetPayloadStepReq().GetStep()
			if request.Type != protos.StreamCrunRequest_STEP_REQUEST || step == nil {
				t.Fatalf("expected nested step submission, got %v", request)
			}
			if step.JobId != 42 || step.NodeNum != 1 || step.Ntasks != 4 || step.NtasksPerNode != 4 {
				t.Fatalf("incorrect inherited job identity or topology: %v", step)
			}
			if step.GetMemPerNode() != test.wantNode || (step.MemPerNode != nil) != (test.wantNode != 0) {
				t.Errorf("MemPerNode=%v (%d), want %d with presence=%t", step.MemPerNode, step.GetMemPerNode(), test.wantNode, test.wantNode != 0)
			}
			if step.GetMemPerCpu() != test.wantCPU || (step.MemPerCpu != nil) != (test.wantCPU != 0) {
				t.Errorf("MemPerCpu=%v (%d), want %d with presence=%t", step.MemPerCpu, step.GetMemPerCpu(), test.wantCPU, test.wantCPU != 0)
			}
		})
	}
}
