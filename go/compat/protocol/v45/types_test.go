// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package v45_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/PKUHPC/CraneSched/go/compat/protocol/v45"
	"github.com/PKUHPC/CraneSched/go/compat/utils"
)

func TestSchemaV45Contract(t *testing.T) {
	var out bytes.Buffer
	if err := utils.WriteSchema[v45.SinfoResponse](&out, "json", utils.SerializerOptions{Compact: true}); err != nil {
		t.Fatal(err)
	}
	// Slurm 26.05.4 v45 spec_only, compacted in property order, excluding the
	// five removed partition fields and their now-empty oversubscribe container.
	// This fingerprint covers descriptions, enums, formats and empty required
	// arrays without embedding a second schema definition in the executable.
	const want = "07c046c68c5c48a4670a6785e1250149ba94867b832b243fb099b09c5d89e3e8"
	if got := fmt.Sprintf("%x", sha256.Sum256(out.Bytes())); got != want {
		t.Fatalf("v45 schema contract changed: %s; compare against the reference before updating the fingerprint", got)
	}
}

func TestReportNumericModes(t *testing.T) {
	for _, tc := range []struct {
		name            string
		value           uint64
		unset, infinite bool
		plain, complex  string
	}{
		{"zero", 0, false, false, `{"set":true,"infinite":false,"number":0}`, `0`},
		{"wide", 1<<63 + 1, false, false, `{"set":true,"infinite":false,"number":9223372036854775809}`, `9223372036854775809`},
		{"unset", 123, true, false, `{"set":false,"infinite":false,"number":0}`, `null`},
		{"infinite", 123, false, true, `{"set":false,"infinite":true,"number":0}`, `"Infinity"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, complex := range []bool{false, true} {
				var out bytes.Buffer
				number := v45.Numeric[uint64](tc.value, tc.unset, tc.infinite)
				if err := utils.WriteReport(&out, number, "json", utils.SerializerOptions{Compact: true}, complex); err != nil {
					t.Fatal(err)
				}
				want := tc.plain
				if complex {
					want = tc.complex
				}
				if out.String() != want+"\n" {
					t.Fatalf("%q != %q", &out, want)
				}
			}
		})
	}
}
