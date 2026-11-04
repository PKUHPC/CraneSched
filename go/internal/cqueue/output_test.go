package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"testing"

	"google.golang.org/protobuf/types/known/durationpb"
)

func uint32Pointer(value uint32) *uint32 {
	return &value
}

func TestFormatPendingArrayJobID(t *testing.T) {
	tests := []struct {
		name string
		spec *protos.ArraySpec
		want string
	}{
		{
			name: "range with concurrency",
			spec: &protos.ArraySpec{Start: 0, End: 9, MaxConcurrent: uint32Pointer(2)},
			want: "42_[0-9%2]",
		},
		{
			name: "stride with concurrency",
			spec: &protos.ArraySpec{Start: 1, End: 9, Stride: uint32Pointer(2), MaxConcurrent: uint32Pointer(3)},
			want: "42_[1-9:2%3]",
		},
		{
			name: "range without concurrency",
			spec: &protos.ArraySpec{Start: 4, End: 8},
			want: "42_[4-8]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			job := &protos.JobInfo{
				JobId:     42,
				Status:    protos.JobStatus_Pending,
				ArraySpec: test.spec,
			}
			if got := FormatQueueJobID(job); got != test.want {
				t.Fatalf("FormatQueueJobID() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSlurmQueueStatesAreConsistent(t *testing.T) {
	util.SetOutputMode(util.OutputModeSlurm)
	t.Cleanup(func() { util.SetOutputMode(util.OutputModeNative) })

	pending := &protos.JobInfo{
		JobId:     10,
		Status:    protos.JobStatus_Pending,
		ArraySpec: &protos.ArraySpec{Start: 1, End: 3},
		TimeLimit: durationpb.New(0),
	}
	running := &protos.JobInfo{
		JobId:     11,
		Status:    protos.JobStatus_Running,
		TimeLimit: durationpb.New(0),
	}

	config := GenerateTableConfig()
	if got := config.Header[5]; got != "ST" {
		t.Fatalf("default state header = %q, want ST", got)
	}
	if got := config.RowMapper(pending)[5]; got != "PD" {
		t.Fatalf("pending default state = %q, want PD", got)
	}
	if got := config.RowMapper(running)[5]; got != "R" {
		t.Fatalf("running default state = %q, want R", got)
	}
	if got := FormatQueueState(pending); got != "PENDING" {
		t.Fatalf("pending long state = %q, want PENDING", got)
	}
	if got := FormatQueueState(running); got != "RUNNING" {
		t.Fatalf("running long state = %q, want RUNNING", got)
	}
}

func TestFormatSlurmJobStateCompact(t *testing.T) {
	tests := map[protos.JobStatus]string{
		protos.JobStatus_Pending:         "PD",
		protos.JobStatus_Running:         "R",
		protos.JobStatus_Completed:       "CD",
		protos.JobStatus_Failed:          "F",
		protos.JobStatus_ExceedTimeLimit: "TO",
		protos.JobStatus_Cancelled:       "CA",
		protos.JobStatus_OutOfMemory:     "OOM",
		protos.JobStatus_Configuring:     "CF",
		protos.JobStatus_Starting:        "CF",
		protos.JobStatus_Completing:      "CG",
		protos.JobStatus_Suspended:       "S",
		protos.JobStatus_Deadline:        "DL",
		protos.JobStatus_Invalid:         "?",
	}

	for status, want := range tests {
		if got := FormatSlurmJobStateCompact(status); got != want {
			t.Errorf("FormatSlurmJobStateCompact(%s) = %q, want %q", status, got, want)
		}
	}
}
