package cacct

import (
	"testing"
	"time"

	"CraneFrontEnd/generated/protos"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testTimestamp(seconds int64) *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(seconds, 0))
}

func testAccountingItem(step bool, status protos.JobStatus, start, end int64,
	elapsed *durationpb.Duration) *JobOrStep {
	if step {
		return &JobOrStep{isStep: true, stepInfo: &protos.StepInfo{
			Status: status, StartTime: testTimestamp(start),
			EndTime: testTimestamp(end), ElapsedTime: elapsed,
		}}
	}
	return &JobOrStep{job: &protos.JobInfo{
		Status: status, StartTime: testTimestamp(start),
		EndTime: testTimestamp(end), ElapsedTime: elapsed,
	}}
}

func TestTerminalAccountingTimes(t *testing.T) {
	const start = int64(1_700_000_000)
	statuses := []protos.JobStatus{
		protos.JobStatus_Completed,
		protos.JobStatus_Failed,
		protos.JobStatus_Cancelled,
		protos.JobStatus_ExceedTimeLimit,
		protos.JobStatus_OutOfMemory,
		protos.JobStatus_Deadline,
	}
	for _, status := range statuses {
		for _, step := range []bool{false, true} {
			item := testAccountingItem(step, status, start, start+5, nil)
			if got := ProcessElapsedTime(item); got != "00:00:05" {
				t.Errorf("status=%v step=%v elapsed=%q", status, step, got)
			}
			if got := ProcessEndTime(item); got == "unknown" {
				t.Errorf("status=%v step=%v end time is unknown", status, step)
			}
		}
	}
}

func TestTerminalAccountingAllowsZeroAndPrefersBackendElapsed(t *testing.T) {
	const start = int64(1_700_000_000)
	for _, step := range []bool{false, true} {
		item := testAccountingItem(step, protos.JobStatus_Cancelled, start, start,
			durationpb.New(0))
		if got := ProcessElapsedTime(item); got != "00:00:00" {
			t.Errorf("step=%v zero elapsed=%q", step, got)
		}
		if got := ProcessEndTime(item); got == "unknown" {
			t.Errorf("step=%v zero-duration end time is unknown", step)
		}
	}

	item := testAccountingItem(false, protos.JobStatus_Failed, start, start+5,
		durationpb.New(7*time.Second))
	if got := ProcessElapsedTime(item); got != "00:00:07" {
		t.Fatalf("backend elapsed=%q", got)
	}
}

func TestTerminalAccountingRejectsInvalidTimes(t *testing.T) {
	const start = int64(1_700_000_000)
	tests := []struct {
		name string
		item *JobOrStep
	}{
		{
			name: "reverse",
			item: testAccountingItem(false, protos.JobStatus_Failed,
				start+5, start, durationpb.New(5*time.Second)),
		},
		{
			name: "missing both",
			item: &JobOrStep{job: &protos.JobInfo{
				Status: protos.JobStatus_Completed,
			}},
		},
		{
			name: "missing end",
			item: &JobOrStep{job: &protos.JobInfo{
				Status: protos.JobStatus_Completed, StartTime: testTimestamp(start),
			}},
		},
		{
			name: "missing start",
			item: &JobOrStep{job: &protos.JobInfo{
				Status: protos.JobStatus_Completed, EndTime: testTimestamp(start + 5),
			}},
		},
		{
			name: "invalid timestamp",
			item: &JobOrStep{job: &protos.JobInfo{
				Status:    protos.JobStatus_Completed,
				StartTime: &timestamppb.Timestamp{Seconds: start, Nanos: int32(time.Second)},
				EndTime:   testTimestamp(start + 5),
			}},
		},
		{
			name: "invalid elapsed",
			item: testAccountingItem(false, protos.JobStatus_Failed,
				start, start+5, &durationpb.Duration{Seconds: 1, Nanos: -1}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ProcessElapsedTime(test.item); got != "unknown" {
				t.Errorf("elapsed=%q", got)
			}
			if test.name != "invalid elapsed" {
				if got := ProcessEndTime(test.item); got != "unknown" {
					t.Errorf("end time=%q", got)
				}
			}
		})
	}
}

func TestAccountingNonTerminalBehavior(t *testing.T) {
	const start = int64(1_700_000_000)
	running := testAccountingItem(false, protos.JobStatus_Running, start,
		start+10, durationpb.New(3*time.Second))
	if got := ProcessElapsedTime(running); got != "00:00:03" {
		t.Fatalf("running elapsed=%q", got)
	}
	if got := ProcessEndTime(running); got != "unknown" {
		t.Fatalf("running end time=%q", got)
	}

	pending := testAccountingItem(false, protos.JobStatus_Pending, start,
		start+10, nil)
	if got := ProcessElapsedTime(pending); got != "" {
		t.Fatalf("pending elapsed=%q", got)
	}
	if got := ProcessEndTime(pending); got != "unknown" {
		t.Fatalf("pending end time=%q", got)
	}
}
