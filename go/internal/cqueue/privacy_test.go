package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"testing"
)

func TestPrivateJobAndStepColumns(t *testing.T) {
	for _, visibility := range []protos.QueryVisibility{protos.QueryVisibility_QUERY_VISIBILITY_PUBLIC, protos.QueryVisibility_QUERY_VISIBILITY_DETAILS} {
		job := &protos.JobInfo{QueryVisibility: visibility, CmdLine: "owner-command", ExtraAttr: `{"comment":"owner-comment"}`}
		step := StepData{job: job, stepInfo: &protos.StepInfo{QueryVisibility: visibility, CmdLine: "owner-command"}}
		command, comment := "owner-command", "owner-comment"
		if visibility == protos.QueryVisibility_QUERY_VISIBILITY_PUBLIC {
			command, comment = util.RestrictedJobField, util.RestrictedJobField
		}
		if ProcessCommand(job) != command || ProcessStepCommand(step) != command || ProcessComment(job) != comment {
			t.Fatalf("private columns ignored visibility %v", visibility)
		}
	}
}
