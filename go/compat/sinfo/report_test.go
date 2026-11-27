// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/PKUHPC/CraneSched/go/compat/protocol/v45"
	"github.com/PKUHPC/CraneSched/go/compat/utils"
	"gopkg.in/yaml.v3"
)

func TestStructuredV45Report(t *testing.T) {
	for _, parser := range []string{"", "=v0.0.45", "=data_parser/v0.0.45", "=v0.0.45+complex"} {
		t.Run(parser, func(t *testing.T) {
			argv := []string{"/opt/crane/bin/sinfo", "--json" + parser, "-p", "compute"}
			o, err := parseCommand(argv, nil)
			if err != nil {
				t.Fatal(err)
			}
			snap := testSnapshot()
			// Values above 32 bits must not be truncated by schema format annotations.
			snap.Nodes[0].CPUs = 1 << 33
			snap.Partitions[0].DefaultMemCPU = 1024
			var out, diagnostics bytes.Buffer
			if err := renderStructured(&out, &diagnostics, argv, buildRows(snap, o), snap, o, func(uint32) (string, bool) { return "user", true }); err != nil {
				t.Fatal(err)
			}
			var data map[string]any
			dec := json.NewDecoder(&out)
			dec.UseNumber()
			if err := dec.Decode(&data); err != nil {
				t.Fatal(err)
			}
			meta := data["meta"].(map[string]any)
			command := meta["command"].([]any)
			if command[0] != argv[0] || !strings.HasPrefix(meta["plugin"].(map[string]any)["data_parser"].(string), "data_parser/v0.0.45") {
				t.Fatal(meta)
			}
			row := data["sinfo"].([]any)[0].(map[string]any)
			if row["cpus"].(map[string]any)["maximum"] != json.Number("8589934592") {
				t.Fatal(row["cpus"])
			}
			if _, ok := row["reason"].(map[string]any)["user"]; ok {
				t.Fatal("legacy reason.user emitted")
			}
			if _, present := row["reason_set_by_user"]; present {
				t.Fatal("missing reason UID must be omitted")
			}
			partition := row["partition"].(map[string]any)
			defaults, maximums := partition["defaults"].(map[string]any), partition["maximums"].(map[string]any)
			if _, ok := defaults["memory_per_cpu"]; ok {
				t.Fatal("removed default memory emitted")
			}
			for _, key := range []string{"memory_per_cpu", "shares", "oversubscribe"} {
				if _, ok := maximums[key]; ok {
					t.Fatalf("removed maximum %s emitted", key)
				}
			}
			if _, ok := defaults["partition_memory_per_node"]; !ok {
				t.Fatal("active memory field missing")
			}
			if !strings.Contains(parser, "complex") {
				var schema map[string]any
				var buf bytes.Buffer
				if err := utils.WriteSchema[v45.SinfoResponse](&buf, "json", utils.SerializerOptions{}); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(buf.Bytes(), &schema); err != nil {
					t.Fatal(err)
				}
				checkReportSchema(t, schema, data, "root")
			} else if defaults["partition_memory_per_cpu"] != json.Number("1024") || defaults["partition_memory_per_node"] != nil {
				t.Fatal(defaults)
			}
		})
	}
}

// Check every emitted field against the public schema, including fields nested
// in arrays. This catches drift between the report adapter and schema traversal.
func checkReportSchema(t *testing.T, schema map[string]any, value any, path string) {
	t.Helper()
	switch schema["type"] {
	case "object":
		obj, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("%s: expected object, got %T", path, value)
		}
		props := schema["properties"].(map[string]any)
		for k, child := range obj {
			v, ok := props[k]
			if !ok {
				t.Fatalf("%s.%s absent from v45 schema", path, k)
			}
			checkReportSchema(t, v.(map[string]any), child, path+"."+k)
		}
	case "array":
		a, ok := value.([]any)
		if !ok {
			t.Fatalf("%s: expected array, got %T", path, value)
		}
		for _, v := range a {
			checkReportSchema(t, schema["items"].(map[string]any), v, path+"[]")
		}
	case "string":
		if _, ok := value.(string); !ok {
			t.Fatalf("%s: expected string, got %T", path, value)
		}
	case "integer":
		if _, ok := value.(json.Number); !ok {
			t.Fatalf("%s: expected number, got %T", path, value)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			t.Fatalf("%s: expected bool, got %T", path, value)
		}
	default:
		t.Fatalf("%s: unknown schema type %v", path, schema["type"])
	}
}

func TestEmptyPartitionReport(t *testing.T) {
	r := &row{Partition: &partition{Name: "empty", PreemptMode: "OFF"}}
	record := structuredRow(r, &plan{}, nil)
	if record.Nodes.Total != 0 || record.CPUs.Idle == nil || *record.CPUs.Idle != 0 {
		t.Fatalf("empty partition counts: %+v %+v", record.Nodes, record.CPUs)
	}
	if record.Port != nil || record.Node != nil || record.CPUs.Minimum != nil || record.Memory != nil || record.Reason != nil {
		t.Fatal("node observations present in an empty partition")
	}
	if !reflect.DeepEqual(record.Partition.PreemptMode, []string{"DISABLED"}) {
		t.Fatalf("preempt mode = %v", record.Partition.PreemptMode)
	}
}

func TestStructuredImplicitCapabilitiesDoNotBecomeZeroValues(t *testing.T) {
	for _, kind := range []string{"json", "yaml"} {
		o, err := parseCommand([]string{"sinfo", "--" + kind, "-o", "%N"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		s, err := adaptSnapshot(adapterFixture(), o.Query)
		if err != nil {
			t.Fatal(err)
		}
		var out, diagnostics bytes.Buffer
		err = renderStructured(&out, &diagnostics, []string{"sinfo"}, buildRows(s, o), s, o, newTextRenderer().UserName)
		if err != nil {
			t.Fatal(err)
		}
		var doc struct{ Sinfo []map[string]any }
		if kind == "json" {
			err = json.Unmarshal(out.Bytes(), &doc)
		} else {
			err = yaml.Unmarshal(out.Bytes(), &doc)
		}
		if err != nil || len(doc.Sinfo) != 1 {
			t.Fatalf("%s: invalid report: %v", kind, err)
		}
		item := doc.Sinfo[0]
		for _, key := range []string{"disk", "weight", "features", "comment", "extra", "resource_spec"} {
			if _, ok := item[key]; ok {
				t.Errorf("%s: emitted unsupported field %s", kind, key)
			}
		}
		cpus := item["cpus"].(map[string]any)
		if _, hasLoad := cpus["load"]; fmt.Sprint(cpus["total"]) != "4" || fmt.Sprint(cpus["other"]) != "0" || hasLoad {
			t.Fatalf("missing load or known counts misreported: %v", cpus)
		}
	}
}
