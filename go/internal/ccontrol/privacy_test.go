package ccontrol

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/testutil"
	"CraneFrontEnd/internal/util"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestJobTextRespectsVisibility(t *testing.T) {
	job := &protos.JobInfo{JobId: 42, Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid()),
		TimeLimit: durationpb.New(time.Hour), CmdLine: "owner-command", Cwd: "owner-directory",
		ExtraAttr:      `{"comment":"owner-comment","mail":{"user":"private@example.invalid","type":"ALL"}}`,
		SubmitHostname: "public-login", QueryVisibility: protos.QueryVisibility_QUERY_VISIBILITY_PUBLIC}
	output, err := testutil.CaptureStdout(t, func() error { return printJobDetails(job) })
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(output, util.RestrictedJobField) != 3 || !strings.Contains(output, "public-login") {
		t.Fatalf("incorrect public output: %s", output)
	}
	for _, private := range []string{"owner-command", "owner-directory", "owner-comment", "private@example.invalid"} {
		if strings.Contains(output, private) {
			t.Fatalf("private text leaked: %q", private)
		}
	}
	job.QueryVisibility = protos.QueryVisibility_QUERY_VISIBILITY_DETAILS
	output, err = testutil.CaptureStdout(t, func() error { return printJobDetails(job) })
	if err != nil || !strings.Contains(output, "owner-command") || strings.Contains(output, util.RestrictedJobField) {
		t.Fatalf("owner output=%q err=%v", output, err)
	}
}

type privacyQueryClient struct {
	protos.CraneCtldClient
	reply *protos.QueryJobsInfoReply
	err   error
}

func (c privacyQueryClient) QueryJobsInfo(_ context.Context, request *protos.QueryJobsInfoRequest, _ ...grpc.CallOption) (*protos.QueryJobsInfoReply, error) {
	if request.Uid == nil {
		panic("missing caller UID")
	}
	return c.reply, c.err
}

func TestExtraAttrsUpdateDoesNotOverwriteRestrictedValues(t *testing.T) {
	old := stub
	t.Cleanup(func() { stub = old })
	stub = privacyQueryClient{reply: &protos.QueryJobsInfoReply{Ok: true, JobInfoList: []*protos.JobInfo{{JobId: 42, QueryVisibility: protos.QueryVisibility_QUERY_VISIBILITY_PUBLIC}}}}
	err := ChangeJobExtraAttrs("42", map[UpdateJobParamFlags]string{CommentTypeFlag: "new comment"})
	var craneErr *util.CraneError
	if !errors.As(err, &craneErr) || craneErr.Code == 0 || !strings.Contains(err.Error(), "restricted") {
		t.Fatalf("expected restricted update error, got %v", err)
	}
}

func TestQueryFailureDoesNotPrintJSON(t *testing.T) {
	oldStub, oldJSON := stub, FlagJson
	t.Cleanup(func() { stub, FlagJson = oldStub, oldJSON })
	FlagJson = true
	for _, reply := range []privacyQueryClient{
		{err: status.Error(codes.Unauthenticated, "bad identity")},
		{reply: &protos.QueryJobsInfoReply{JobInfoList: []*protos.JobInfo{{CmdLine: "private-marker"}}}},
	} {
		stub = reply
		output, err := testutil.CaptureStdout(t, func() error { return ShowSteps("42.1", false) })
		var craneErr *util.CraneError
		if !errors.As(err, &craneErr) || craneErr.Code == 0 || output != "" {
			t.Fatalf("output=%q err=%v", output, err)
		}
	}
}

func TestShowConfigRemainsLocalAndPublic(t *testing.T) {
	oldStub, oldJSON := stub, FlagJson
	t.Cleanup(func() { stub, FlagJson = oldStub, oldJSON })
	stub = nil // Configuration display must not make an authenticated query.
	path := filepath.Join(t.TempDir(), "config.yaml")
	const config = "ControlMachine: control.example.invalid\nTlsConfig:\n  CaFilePath: /etc/crane/tls/ca.pem\n"
	if err := os.WriteFile(path, []byte(config), 0644); err != nil {
		t.Fatal(err)
	}
	for _, asJSON := range []bool{false, true} {
		FlagJson = asJSON
		output, err := testutil.CaptureStdout(t, func() error { return ShowConfig(path) })
		if err != nil || !strings.Contains(output, "control.example.invalid") || !strings.Contains(output, "/etc/crane/tls/ca.pem") {
			t.Fatalf("config output=%q err=%v", output, err)
		}
	}
}

func TestHistoricalResourceRequestsKeepUnknownValues(t *testing.T) {
	job := &protos.JobInfo{ReqTotalResView: &protos.ResourceView{CpuCount: 12}}
	output, err := testutil.CaptureStdout(t, func() error { printResourceRequests(job); return nil })
	if err != nil || !strings.Contains(output, "CpusPerTask=Unknown") || !strings.Contains(output, "gres=Unknown") {
		t.Fatalf("unknown history output=%q err=%v", output, err)
	}
	job.ReqTotalResView.GresMap = &protos.GresMap{}
	output, err = testutil.CaptureStdout(t, func() error { printResourceRequests(job); return nil })
	if err != nil || !strings.Contains(output, "gres=None") {
		t.Fatalf("empty request output=%q err=%v", output, err)
	}
}
