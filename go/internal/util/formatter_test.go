package util

import (
	"CraneFrontEnd/generated/protos"
	"encoding/json"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestRequestPresenceInNativeAndSlurmJSON(t *testing.T) {
	for _, count := range []*uint32{nil, proto.Uint32(0), proto.Uint32(3)} {
		job := &protos.JobInfo{Ntasks: count, ReqTotalResView: &protos.ResourceView{}, StepInfoList: []*protos.StepInfo{{Ntasks: count}}}
		if count != nil {
			job.ReqTotalResView.GresMap = &protos.GresMap{}
		}
		reply := &protos.QueryJobsInfoReply{Ok: true, JobInfoList: []*protos.JobInfo{job}}
		native, err := MarshalProtoJSON(reply)
		if err != nil {
			t.Fatal(err)
		}
		slurm, err := FormatSlurmJobsJSON(reply)
		if err != nil {
			t.Fatal(err)
		}
		for _, data := range []string{string(native), slurm} {
			var result struct {
				Jobs []struct {
					Ntasks  *uint32 `json:"ntasks"`
					Request struct {
						Gres json.RawMessage `json:"gres_map"`
					} `json:"req_total_res_view"`
					Steps []struct {
						Ntasks *uint32 `json:"ntasks"`
					} `json:"step_info_list"`
				} `json:"job_info_list"`
			}
			if err := json.Unmarshal([]byte(data), &result); err != nil {
				t.Fatal(err)
			}
			got := result.Jobs[0]
			for _, value := range []*uint32{got.Ntasks, got.Steps[0].Ntasks} {
				if (value == nil) != (count == nil) || value != nil && *value != *count {
					t.Fatalf("task count presence/value changed: %s", data)
				}
			}
			if (got.Request.Gres == nil) != (count == nil) {
				t.Fatalf("unknown and empty GRES conflated: %s", data)
			}
		}
	}
}
