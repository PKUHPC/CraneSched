package util

import (
	"reflect"
	"testing"
	"time"

	"CraneFrontEnd/generated/protos"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestParseHostListExpandsNodeNameExpression(t *testing.T) {
	got, ok := ParseHostList("b1u01n1,b2u[05,02]n3,b3u03n4")
	if !ok {
		t.Fatal("ParseHostList rejected a valid hostlist expression")
	}

	want := []string{"b1u01n1", "b2u05n3", "b2u02n3", "b3u03n4"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseHostList returned %v, want %v", got, want)
	}
}

func TestBuildNodeAliasMapSupportsNodeNameAndHostname(t *testing.T) {
	nodes := []ConfigNodesList{{
		Name:         "crnd[1-2]",
		NodeHostname: "host[01-02].example.com",
	}}

	aliases, err := BuildNodeAliasMap(nodes)
	if err != nil {
		t.Fatalf("BuildNodeAliasMap returned error: %v", err)
	}

	want := map[string]string{
		"crnd1":              "crnd1",
		"crnd2":              "crnd2",
		"host01.example.com": "crnd1",
		"host02.example.com": "crnd2",
	}
	for alias, nodeName := range want {
		if got := aliases[alias]; got != nodeName {
			t.Errorf("alias %q = %q, want %q", alias, got, nodeName)
		}
	}
	for _, unsupported := range []string{"host01", "host02"} {
		if _, ok := aliases[unsupported]; ok {
			t.Errorf("unexpected short-hostname alias %q", unsupported)
		}
	}
}

func TestBuildNodeAliasMapRejectsMismatchedHostnames(t *testing.T) {
	_, err := BuildNodeAliasMap([]ConfigNodesList{{
		Name:         "crnd[1-2]",
		NodeHostname: "host01.example.com",
	}})
	if err == nil {
		t.Fatal("BuildNodeAliasMap accepted mismatched NodeHostname list")
	}
}

func TestBuildNodeAliasMapDefaultsHostnameToNodeName(t *testing.T) {
	aliases, err := BuildNodeAliasMap([]ConfigNodesList{{Name: "crnd1"}})
	if err != nil {
		t.Fatalf("BuildNodeAliasMap returned error: %v", err)
	}
	if got := aliases["crnd1"]; got != "crnd1" {
		t.Fatalf("default hostname alias = %q, want %q", got, "crnd1")
	}
}

func TestCheckJobArgsExpandsAggregatedNodeLists(t *testing.T) {
	job := &protos.JobToCtld{
		NodeNumMin: 1,
		NodeNumMax: 1,
		Ntasks:     1,
		TimeLimit:  durationpb.New(time.Second),
		Nodelist:   "b2u[05,02]n3",
		Excludes:   "b3u[03-04]n4",
	}

	if err := CheckJobArgs(job); err != nil {
		t.Fatalf("CheckJobArgs returned error: %v", err)
	}
	if job.Nodelist != "b2u05n3,b2u02n3" {
		t.Fatalf("Nodelist = %q, want %q", job.Nodelist, "b2u05n3,b2u02n3")
	}
	if job.Excludes != "b3u03n4,b3u04n4" {
		t.Fatalf("Excludes = %q, want %q", job.Excludes, "b3u03n4,b3u04n4")
	}
}

func TestCheckStepArgsExpandsAggregatedNodeLists(t *testing.T) {
	step := &protos.StepToCtld{
		NodeNum:   1,
		Ntasks:    1,
		TimeLimit: durationpb.New(time.Second),
		Nodelist:  "b2u[05,02]n3",
		Excludes:  "b3u[03-04]n4",
	}

	if err := CheckStepArgs(step); err != nil {
		t.Fatalf("CheckStepArgs returned error: %v", err)
	}
	if step.Nodelist != "b2u05n3,b2u02n3" {
		t.Fatalf("Nodelist = %q, want %q", step.Nodelist, "b2u05n3,b2u02n3")
	}
	if step.Excludes != "b3u03n4,b3u04n4" {
		t.Fatalf("Excludes = %q, want %q", step.Excludes, "b3u03n4,b3u04n4")
	}
}

func TestCheckNodeListRejectsMalformedExpression(t *testing.T) {
	if CheckNodeList("node[01,foo]") {
		t.Fatal("CheckNodeList accepted malformed expression")
	}
	if CheckNodeList("[]") {
		t.Fatal("CheckNodeList accepted empty expression")
	}
	if CheckNodeList("node[01,]") {
		t.Fatal("CheckNodeList accepted a trailing empty member")
	}
	if CheckNodeList("node[,01]") {
		t.Fatal("CheckNodeList accepted a leading empty member")
	}
	if CheckNodeList("node[01,,02]") {
		t.Fatal("CheckNodeList accepted a repeated delimiter")
	}
}
