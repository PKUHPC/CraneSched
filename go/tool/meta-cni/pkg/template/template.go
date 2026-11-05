package template

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	gotemplate "text/template"
)

type Vars struct {
	Gres GresVars
	Args map[string]string
}

type GresVars struct {
	Device string
	Index  string
}

func BuildVars(gresDevice, gresIndex string, cniArgs map[string]string) Vars {
	argsCopy := make(map[string]string, len(cniArgs))
	for k, v := range cniArgs {
		argsCopy[k] = v
	}

	return Vars{
		Gres: GresVars{
			Device: gresDevice,
			Index:  gresIndex,
		},
		Args: argsCopy,
	}
}

func Render(raw []byte, vars Vars) ([]byte, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	var payload any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode conf: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode conf: trailing data after JSON value")
		}
		return nil, fmt.Errorf("decode conf: %w", err)
	}

	rendered, err := renderValue(payload, vars)
	if err != nil {
		return nil, err
	}

	out, err := json.Marshal(rendered)
	if err != nil {
		return nil, fmt.Errorf("encode rendered conf: %w", err)
	}
	return out, nil
}

func renderValue(v any, vars Vars) (any, error) {
	return renderValueAt(v, vars, "$")
}

func renderValueAt(v any, vars Vars, path string) (any, error) {
	switch typed := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for k, child := range typed {
			rendered, err := renderValueAt(child, vars, jsonObjectPath(path, k))
			if err != nil {
				return nil, err
			}
			out[k] = rendered
		}
		return out, nil
	case []any:
		out := make([]any, len(typed))
		for i, child := range typed {
			rendered, err := renderValueAt(child, vars, jsonArrayPath(path, i))
			if err != nil {
				return nil, err
			}
			out[i] = rendered
		}
		return out, nil
	case string:
		return renderString(typed, vars, path)
	default:
		return v, nil
	}
}

func renderString(raw string, vars Vars, path string) (string, error) {
	tmpl, err := gotemplate.New("conf-string").Option("missingkey=zero").Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse template string at %s: %w", path, err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, vars); err != nil {
		return "", fmt.Errorf("execute template string at %s: %w", path, err)
	}
	return buf.String(), nil
}

func jsonObjectPath(path, key string) string {
	return path + "[" + strconv.Quote(key) + "]"
}

func jsonArrayPath(path string, index int) string {
	return path + "[" + strconv.Itoa(index) + "]"
}
