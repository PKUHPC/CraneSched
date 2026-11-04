package template

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildVars(t *testing.T) {
	t.Parallel()

	vars := BuildVars("0000:86:00.2", "3", map[string]string{
		"MY_KEY": "my_value",
	})

	if vars.Gres.Device != "0000:86:00.2" {
		t.Fatalf("Gres.Device = %q, want %q", vars.Gres.Device, "0000:86:00.2")
	}
	if vars.Gres.Index != "3" {
		t.Fatalf("Gres.Index = %q, want %q", vars.Gres.Index, "3")
	}
	if vars.Args["MY_KEY"] != "my_value" {
		t.Fatalf("Args.MY_KEY = %q, want %q", vars.Args["MY_KEY"], "my_value")
	}
}

func TestRenderSimple(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"type":"sriov","deviceID":"{{.Gres.Device}}"}`)
	out, err := Render(raw, BuildVars("0000:86:00.2", "0", nil))
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if got["deviceID"] != "0000:86:00.2" {
		t.Fatalf("deviceID = %v, want %q", got["deviceID"], "0000:86:00.2")
	}
}

func TestRenderNestedValues(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"type":"plugin",
		"labels":["{{.Gres.Index}}","fixed"],
		"nested":{"alias":"{{.Args.ALIAS}}"}
	}`)

	out, err := Render(raw, BuildVars("dev0", "2", map[string]string{
		"ALIAS": "rdma2",
	}))
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var got struct {
		Labels []string          `json:"labels"`
		Nested map[string]string `json:"nested"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if got.Labels[0] != "2" {
		t.Fatalf("labels[0] = %q, want %q", got.Labels[0], "2")
	}
	if got.Nested["alias"] != "rdma2" {
		t.Fatalf("nested.alias = %q, want %q", got.Nested["alias"], "rdma2")
	}
}

func TestRenderAllowsBuiltInTemplateSyntax(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"alias":"{{if .Args.ALIAS}}{{.Args.ALIAS}}{{else}}rdma{{.Gres.Index}}{{end}}",
		"dashed":"{{index .Args \"KEY-WITH-DASH\"}}"
	}`)

	out, err := Render(raw, BuildVars("dev0", "7", map[string]string{
		"KEY-WITH-DASH": "dash-value",
	}))
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var got map[string]string
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if got["alias"] != "rdma7" {
		t.Fatalf("alias = %q, want %q", got["alias"], "rdma7")
	}
	if got["dashed"] != "dash-value" {
		t.Fatalf("dashed = %q, want %q", got["dashed"], "dash-value")
	}
}

func TestRenderMissingDirectArgKeyUsesZeroValue(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"missingArg":"{{.Args.MISSING}}"}`)

	out, err := Render(raw, BuildVars("dev0", "7", map[string]string{}))
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var got map[string]string
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if got["missingArg"] != "" {
		t.Fatalf("missingArg = %q, want empty string", got["missingArg"])
	}
}

func TestRenderMissingIndexedKeyUsesZeroValue(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"missingDashed":"{{index .Args \"MISSING-DASH\"}}"}`)
	out, err := Render(raw, BuildVars("", "", map[string]string{}))
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var got map[string]string
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if got["missingDashed"] != "" {
		t.Fatalf("missingDashed = %q, want empty string", got["missingDashed"])
	}
}

func TestRenderUnknownGresFieldErrors(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"missingGres":"{{.Gres.Missing}}"}`)
	_, err := Render(raw, BuildVars("dev0", "7", map[string]string{}))
	if err == nil {
		t.Fatal("expected error for unknown .Gres field")
	}
}

func TestRenderKeepsOutputValidJSON(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"note":"{{.Args.TEXT}}"}`)
	out, err := Render(raw, BuildVars("", "", map[string]string{
		"TEXT": `a"b\c`,
	}))
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	var got map[string]string
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("output is not valid JSON: %v (output=%s)", err, string(out))
	}
	if got["note"] != `a"b\c` {
		t.Fatalf("note = %q, want %q", got["note"], `a"b\c`)
	}
}

func TestRenderPreservesLargeJSONNumbers(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"type":"plugin",
		"big":9007199254740993,
		"nested":{"huge":18446744073709551615}
	}`)

	out, err := Render(raw, BuildVars("", "", nil))
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	dec := json.NewDecoder(bytes.NewReader(out))
	dec.UseNumber()

	var got map[string]any
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("decode output: %v", err)
	}

	big, ok := got["big"].(json.Number)
	if !ok {
		t.Fatalf("big has type %T, want json.Number", got["big"])
	}
	if big.String() != "9007199254740993" {
		t.Fatalf("big = %q, want %q", big.String(), "9007199254740993")
	}

	nested, ok := got["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested has type %T, want map[string]any", got["nested"])
	}
	huge, ok := nested["huge"].(json.Number)
	if !ok {
		t.Fatalf("nested.huge has type %T, want json.Number", nested["huge"])
	}
	if huge.String() != "18446744073709551615" {
		t.Fatalf("nested.huge = %q, want %q", huge.String(), "18446744073709551615")
	}
}

func TestRenderBadTemplateErrors(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"note":"{{if .Gres.Device}}"}`)
	_, err := Render(raw, BuildVars("dev0", "0", nil))
	if err == nil {
		t.Fatal("expected parse error")
	}
}

func TestRenderParseErrorDoesNotLeakTemplateValue(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"note":"token=super-secret {{if .Gres.Device}}"}`)
	_, err := Render(raw, BuildVars("dev0", "0", nil))
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), `$["note"]`) {
		t.Fatalf("error %q does not include JSON path", err)
	}
	if strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("error %q leaked raw template value", err)
	}
}

func TestRenderExecuteErrorDoesNotLeakTemplateValue(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"note":"token=super-secret {{.Gres.Missing}}"}`)
	_, err := Render(raw, BuildVars("dev0", "0", nil))
	if err == nil {
		t.Fatal("expected execute error")
	}
	if !strings.Contains(err.Error(), `$["note"]`) {
		t.Fatalf("error %q does not include JSON path", err)
	}
	if strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("error %q leaked raw template value", err)
	}
}
