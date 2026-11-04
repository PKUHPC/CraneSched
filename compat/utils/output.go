// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

// SerializerOptions controls encoding independently of data-parser complex values.
type SerializerOptions struct{ Compact, NoTag, Complex bool }

// WriteSchema emits a report type's ordered schema without a second definition.
func WriteSchema[T any](w io.Writer, format string, options SerializerOptions) error {
	return writeOutput(w, reportSchema(reflect.TypeFor[T](), "", ""), format, options)
}

// WriteReport encodes a typed report. complexValues controls numeric wrappers and
// absent strings; options.Complex controls serializer-level special numbers.
func WriteReport(w io.Writer, report any, format string, options SerializerOptions, complexValues bool) error {
	return writeOutput(w, reportValue(reflect.ValueOf(report), complexValues, false), format, options)
}

func writeOutput(w io.Writer, root any, format string, options SerializerOptions) error {
	switch format {
	case "json":
		return writeJSON(w, root, options)
	case "yaml":
		mapping, ok := root.(object)
		if !ok {
			return fmt.Errorf("YAML report root must be an object")
		}
		return writeYAML(w, mapping, options)
	default:
		return fmt.Errorf("unknown serialization format %q", format)
	}
}

// An ordered object is shared by the JSON and YAML encoders. Ordering is part
// of the CLI contract; Go maps would silently reorder its output vocabulary.
type member struct {
	name  string
	value any
}
type object []member
type array []any

func stringsArray(values []string) array {
	a := make(array, len(values))
	for i, value := range values {
		a[i] = value
	}
	return a
}

// Walk only our report types. Unlike a general OpenAPI generator, this preserves
// declaration order, inline types and explicit empty required arrays used by
// the CLI's spec_only response. The same types also carry the actual data.
func reportSchema(t reflect.Type, description, enum string) object {
	var kind, format string
	switch t.Kind() {
	case reflect.Struct:
		kind = "object"
	case reflect.Slice:
		kind = "array"
	case reflect.String:
		kind = "string"
	case reflect.Bool:
		kind = "boolean"
	case reflect.Uint64, reflect.Int64:
		kind, format = "integer", "int64"
		if annotated, ok := reflect.Zero(t).Interface().(interface{ SchemaFormat() string }); ok {
			format = annotated.SchemaFormat()
		}
	default:
		panic(fmt.Sprintf("unsupported compatibility report type %s", t))
	}
	result := object{{"type", kind}}
	if format != "" {
		result = append(result, member{"format", format})
	}
	if description != "" {
		result = append(result, member{"description", description})
	}
	switch t.Kind() {
	case reflect.Struct:
		properties := object{}
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			properties = append(properties, member{field.Tag.Get("json"), reportSchema(field.Type, field.Tag.Get("description"), field.Tag.Get("enum"))})
		}
		result = append(result, member{"properties", properties})
		if required, ok := reflect.Zero(t).Interface().(interface{ SchemaRequired() []string }); ok {
			result = append(result, member{"required", stringsArray(required.SchemaRequired())})
		}
	case reflect.Slice:
		items := reportSchema(t.Elem(), "", "")
		if enum != "" {
			items = append(object{{"enum", stringsArray(strings.Split(enum, "|"))}}, items...)
		}
		result = append(result, member{"items", items})
	}
	return result
}

// Preserve the report's field order for both encoders. Complex mode changes
// optional scalar values, but leaves string array elements (including argv)
// intact. Numeric wrappers own their unset/infinite representation.
func reportValue(v reflect.Value, complex, keepEmpty bool) any {
	if complex {
		if number, ok := v.Interface().(interface{ ComplexValue() any }); ok {
			return number.ComplexValue()
		}
	}
	switch v.Kind() {
	case reflect.Struct:
		result := object{}
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			result = append(result, member{field.Tag.Get("json"), reportValue(v.Field(i), complex, field.Tag.Get("complex") == "keep_empty")})
		}
		return result
	case reflect.Slice:
		result := make(array, v.Len())
		for i := range result {
			result[i] = reportValue(v.Index(i), complex, true)
		}
		return result
	case reflect.String:
		if complex && !keepEmpty && v.String() == "" {
			return nil
		}
		return v.String()
	case reflect.Bool:
		return v.Bool()
	case reflect.Uint64:
		return v.Uint()
	case reflect.Int64:
		return v.Int()
	default:
		panic(fmt.Sprintf("unsupported compatibility report type %s", v.Type()))
	}
}

func jsonScalar(v any) string {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	_ = e.Encode(v)
	// json-c escapes forward slashes, including argv[0] and plugin names.
	return strings.ReplaceAll(strings.TrimSuffix(b.String(), "\n"), "/", `\/`)
}

func writeJSON(w io.Writer, root any, options SerializerOptions) error {
	var out strings.Builder
	var emit func(any, int)
	indent := func(depth int) { out.WriteString(strings.Repeat("  ", depth)) }
	newline := func(depth int) {
		if !options.Compact {
			out.WriteByte('\n')
			indent(depth)
		}
	}
	emit = func(value any, depth int) {
		switch v := value.(type) {
		case object:
			out.WriteByte('{')
			for i, m := range v {
				if i > 0 {
					out.WriteByte(',')
				}
				newline(depth + 1)
				out.WriteString(jsonScalar(m.name))
				out.WriteByte(':')
				if !options.Compact {
					out.WriteByte(' ')
				}
				emit(m.value, depth+1)
			}
			if len(v) > 0 {
				newline(depth)
			}
			out.WriteByte('}')
		case array:
			out.WriteByte('[')
			for i, child := range v {
				if i > 0 {
					out.WriteByte(',')
				}
				newline(depth + 1)
				emit(child, depth+1)
			}
			newline(depth)
			out.WriteByte(']')
		case float64:
			switch {
			case math.IsInf(v, 0):
				if options.Complex {
					out.WriteString("Infinity")
				} else {
					out.WriteString("1.8446744073709552e+19")
				}
			case math.IsNaN(v):
				if options.Complex {
					out.WriteString("NaN")
				} else {
					out.WriteString("1.8446744073709552e+19")
				}
			default:
				out.WriteString(jsonScalar(v))
			}
		default:
			out.WriteString(jsonScalar(v))
		}
	}
	emit(root, 0)
	out.WriteByte('\n')
	_, err := io.WriteString(w, out.String())
	return err
}

func yamlScalar(value any, noTag bool) (string, error) {
	tag, text := "!!str", ""
	switch v := value.(type) {
	case nil:
		tag, text = "!!null", "null"
	case string:
		text = v
	case bool:
		tag, text = "!!bool", fmt.Sprint(v)
	case float64:
		tag, text = "!!float", fmt.Sprintf("%f", v)
	case json.Number:
		text, tag = v.String(), "!!int"
		if strings.ContainsAny(text, ".eE") {
			f, err := v.Float64()
			if err != nil {
				return "", err
			}
			tag, text = "!!float", fmt.Sprintf("%f", f)
		}
	default:
		tag, text = "!!int", fmt.Sprint(v)
	}
	if text == "" {
		if noTag {
			return "", nil
		}
		return tag, nil
	}
	node := yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: text, Style: yaml.TaggedStyle}
	// libyaml chooses a quoted scalar for embedded line breaks. yaml.v3's
	// automatic literal-block style would change the command's byte contract.
	if tag == "!!str" && strings.Contains(text, "\n") {
		node.Style |= yaml.SingleQuotedStyle
	}
	data, err := yaml.Marshal(&node)
	if err != nil {
		return "", err
	}
	s := strings.TrimSuffix(string(data), "\n")
	s = strings.TrimSuffix(s, "\n...")
	if noTag {
		s = strings.TrimPrefix(s, tag+" ")
	}
	return s, nil
}

func writeYAML(w io.Writer, root object, options SerializerOptions) error {
	var out strings.Builder
	out.WriteString("%YAML 1.1\n---")
	if !options.NoTag {
		out.WriteString(" !!map")
	}
	out.WriteByte('\n')
	var mapping func(object, int, string) error
	var sequence func(array, int) error
	var value func(any, int) error
	space := func(depth int) { out.WriteString(strings.Repeat(" ", depth)) }
	value = func(v any, depth int) error {
		switch v := v.(type) {
		case object:
			if !options.NoTag {
				out.WriteString(" !!map")
			}
			if len(v) == 0 {
				out.WriteString(" {}\n")
				return nil
			}
			out.WriteByte('\n')
			return mapping(v, depth+2, "")
		case array:
			if !options.NoTag {
				out.WriteString(" !!seq")
			}
			if len(v) == 0 {
				out.WriteString(" []\n")
				return nil
			}
			out.WriteByte('\n')
			return sequence(v, depth)
		default:
			text, err := yamlScalar(v, options.NoTag)
			if err != nil {
				return err
			}
			if text != "" {
				out.WriteByte(' ')
				out.WriteString(strings.ReplaceAll(text, "\n", "\n"+strings.Repeat(" ", depth+2)))
			}
			out.WriteByte('\n')
			return nil
		}
	}
	mapping = func(v object, depth int, firstPrefix string) error {
		for i, m := range v {
			if i == 0 && firstPrefix != "" {
				space(depth - 2)
				out.WriteString(firstPrefix)
			} else {
				space(depth)
			}
			key, err := yamlScalar(m.name, options.NoTag)
			if err != nil {
				return err
			}
			out.WriteString(key)
			out.WriteByte(':')
			if err := value(m.value, depth); err != nil {
				return err
			}
		}
		return nil
	}
	sequence = func(v array, depth int) error {
		for _, child := range v {
			if obj, ok := child.(object); ok && options.NoTag && len(obj) > 0 {
				if err := mapping(obj, depth+2, "- "); err != nil {
					return err
				}
				continue
			}
			space(depth)
			out.WriteByte('-')
			if err := value(child, depth); err != nil {
				return err
			}
		}
		return nil
	}
	if err := mapping(root, 0, ""); err != nil {
		return err
	}
	out.WriteString("...\n\n")
	_, err := io.WriteString(w, out.String())
	return err
}
