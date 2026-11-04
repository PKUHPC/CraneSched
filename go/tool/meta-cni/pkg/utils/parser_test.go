package utils

import (
	"reflect"
	"testing"
)

func TestParseManipulator(t *testing.T) {
	testCases := []struct {
		name      string
		input     string
		want      *Expr
		expectErr bool
	}{
		{
			name:  "upsert bare value",
			input: "FOO=bar",
			want: &Expr{
				Key:   "FOO",
				Value: "bar",
			},
		},
		{
			name:  "delete key",
			input: "-FOO",
			want: &Expr{
				Delete: true,
				Key:    "FOO",
			},
		},
		{
			name:  "single quoted value",
			input: "FOO='value with space'",
			want: &Expr{
				Key:   "FOO",
				Value: "value with space",
			},
		},
		{
			name:  "double quoted value",
			input: `FOO="value with space"`,
			want: &Expr{
				Key:   "FOO",
				Value: "value with space",
			},
		},
		{
			name:      "missing value",
			input:     "FOO",
			expectErr: true,
		},
		{
			name:      "delete with value",
			input:     "-FOO=bar",
			expectErr: true,
		},
		{
			name:      "empty entry",
			input:     "   ",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			expr, err := ParseManipulator(tc.input)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if expr.Delete != tc.want.Delete || expr.Key != tc.want.Key || expr.Value != tc.want.Value {
				t.Fatalf("unexpected expr: %+v want %+v", expr, tc.want)
			}
		})
	}
}

func TestParseManipulators(t *testing.T) {
	input := []string{
		"FOO=bar",
		"-BAR",
		"BAZ='some value'",
	}

	exprs, err := ParseManipulators(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []*Expr{
		{Key: "FOO", Value: "bar"},
		{Delete: true, Key: "BAR"},
		{Key: "BAZ", Value: "some value"},
	}

	if !reflect.DeepEqual(exprs, want) {
		t.Fatalf("unexpected parsed expressions: %+v want %+v", exprs, want)
	}
}
