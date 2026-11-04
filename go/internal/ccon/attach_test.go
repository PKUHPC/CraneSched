package ccon

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"testing"
)

func TestResolveTargetNodeSupportsConfiguredAliases(t *testing.T) {
	originalConfig := config
	config = &util.Config{CranedNodeList: []util.ConfigNodesList{{
		Name:         "crnd1",
		NodeHostname: "host01.example.com",
	}}}
	t.Cleanup(func() { config = originalConfig })

	step := &protos.StepInfo{
		ExecutionNode: []string{"crnd1"},
		CranedList:    "crnd1",
	}
	for _, target := range []string{"crnd1", "host01.example.com"} {
		got, err := resolveTargetNode(step, target)
		if err != nil {
			t.Errorf("resolveTargetNode(%q) returned error: %v", target, err)
			continue
		}
		if got != "crnd1" {
			t.Errorf("resolveTargetNode(%q) = %q, want %q", target, got, "crnd1")
		}
	}
	if got, err := resolveNodeAlias("host01"); err != nil || got != "host01" {
		t.Errorf("resolveNodeAlias(%q) = (%q, %v), want (%q, nil)",
			"host01", got, err, "host01")
	}
	if _, err := resolveTargetNode(step, "host01"); err == nil {
		t.Error("resolveTargetNode accepted an unconfigured short hostname")
	}
}
