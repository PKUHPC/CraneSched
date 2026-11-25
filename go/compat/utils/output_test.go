// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package utils

import (
	"bytes"
	"testing"
)

func TestStructuredEncoderBytes(t *testing.T) {
	v := struct {
		List   []string `json:"list"`
		Value  string   `json:"value"`
		Nested struct {
			Empty string `json:"empty"`
			Count uint64 `json:"count"`
		} `json:"nested"`
	}{}
	v.Value = "/a<b>"
	v.Nested.Count = 2
	var out bytes.Buffer
	if err := WriteReport(&out, v, "json", SerializerOptions{}, false); err != nil {
		t.Fatal(err)
	}
	want := "{\n  \"list\": [\n  ],\n  \"value\": \"\\/a<b>\",\n  \"nested\": {\n    \"empty\": \"\",\n    \"count\": 2\n  }\n}\n"
	if out.String() != want {
		t.Fatalf("%q != %q", out.String(), want)
	}
	out.Reset()
	if err := WriteReport(&out, v, "yaml", SerializerOptions{}, false); err != nil {
		t.Fatal(err)
	}
	want = "%YAML 1.1\n--- !!map\n!!str list: !!seq []\n!!str value: !!str /a<b>\n!!str nested: !!map\n  !!str empty: !!str\n  !!str count: !!int 2\n...\n\n"
	if out.String() != want {
		t.Fatalf("%q != %q", out.String(), want)
	}
	out.Reset()
	if err := WriteReport(&out, v, "json", SerializerOptions{Compact: true}, false); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "{\"list\":[],\"value\":\"\\/a<b>\",\"nested\":{\"empty\":\"\",\"count\":2}}\n" {
		t.Fatal(got)
	}
}
