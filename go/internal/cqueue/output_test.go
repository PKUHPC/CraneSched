package cqueue

import (
	"github.com/PKUHPC/CraneSched/go/generated/protos"

	"testing"
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
