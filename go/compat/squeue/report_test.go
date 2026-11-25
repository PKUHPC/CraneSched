// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package squeue

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/PKUHPC/CraneSched/go/compat/utils"
	pb "github.com/PKUHPC/CraneSched/go/generated/protos"
)

func TestJobReportKeepsKnownFalseAndFractionalBilling(t *testing.T) {
	job := &pb.JobInfo{
		Type: pb.JobType_Batch, Status: pb.JobStatus_Pending,
		RequeueEligible: new(false), ReqTotalResView: &pb.ResourceView{CpuCount: 0.5},
	}
	o, err := parse([]string{"--json", "--sort=?"}, nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := o.rows(&pb.QueryJobsInfoReply{JobInfoList: []*pb.JobInfo{job}})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := o.writeReport(&buf, utils.DataParserOptions{}, []string{"squeue", "--json"}, rows); err != nil {
		t.Fatal(err)
	}
	var data struct{ Jobs []map[string]json.RawMessage }
	if err := json.Unmarshal(buf.Bytes(), &data); err != nil || len(data.Jobs) != 1 {
		t.Fatalf("jobs: %v, err: %v", data.Jobs, err)
	}
	record := data.Jobs[0]
	for _, missing := range []string{"cpus", "command", "het_job_id", "submit_time"} {
		if _, ok := record[missing]; ok {
			t.Errorf("unexpected %s: %s", missing, record[missing])
		}
	}
	if string(record["requeue"]) != "false" {
		t.Errorf("requeue = %s", record["requeue"])
	}
	var billing struct {
		Set    bool
		Number float64
	}
	if err := json.Unmarshal(record["billable_tres"], &billing); err != nil || !billing.Set || billing.Number != 0.5 {
		t.Errorf("billable_tres = %s, err %v", record["billable_tres"], err)
	}
}
