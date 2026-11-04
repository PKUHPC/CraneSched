package utils

import (
	"os"
	"reflect"
	"testing"
)

func TestParseArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  map[string]string
	}{
		{
			name:  "empty string returns empty map",
			input: "",
			want:  map[string]string{},
		},
		{
			name:  "ignores malformed segments",
			input: "foo=bar;broken;bar=baz;",
			want: map[string]string{
				"foo": "bar",
				"bar": "baz",
			},
		},
		{
			name:  "last occurrence wins",
			input: "foo=bar;foo=baz",
			want: map[string]string{
				"foo": "baz",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ParseArgs(tt.input)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ParseArgs(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestMergeArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		base      string
		overrides []string
		want      string
		wantErr   bool
	}{
		{
			name:      "applies overrides and deletions",
			base:      "foo=1;bar=2",
			overrides: []string{"foo=3", "baz=4", "-bar"},
			want:      "baz=4;foo=3",
		},
		{
			name:      "returns base when overrides empty",
			base:      "foo=1",
			overrides: nil,
			want:      "foo=1",
		},
		{
			name:      "returns empty string when all entries removed",
			base:      "foo=1",
			overrides: []string{"-foo"},
			want:      "",
		},
		{
			name:      "propagates invalid override error",
			base:      "",
			overrides: []string{"=value"},
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := MergeArgs(tt.base, tt.overrides)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("MergeArgs(%q, %v) expected error", tt.base, tt.overrides)
				}
				return
			}

			if err != nil {
				t.Fatalf("MergeArgs(%q, %v) unexpected error: %v", tt.base, tt.overrides, err)
			}
			if got != tt.want {
				t.Fatalf("MergeArgs(%q, %v) = %q, want %q", tt.base, tt.overrides, got, tt.want)
			}
		})
	}
}

func TestMergeEnvs(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"foo": "1",
		"bar": "2",
	}
	err := MergeEnvs(env, []string{"foo=updated", "baz=3", "-bar"})
	if err != nil {
		t.Fatalf("MergeEnvs returned error: %v", err)
	}

	want := map[string]string{
		"foo": "updated",
		"baz": "3",
	}
	if !reflect.DeepEqual(env, want) {
		t.Fatalf("MergeEnvs mutated map to %v, want %v", env, want)
	}
}

func TestMergeEnvsError(t *testing.T) {
	t.Parallel()

	err := MergeEnvs(make(map[string]string), []string{"=value"})
	if err == nil {
		t.Fatalf("MergeEnvs with invalid override expected error")
	}
}

func TestApplyEnvAndRestore(t *testing.T) {
	const (
		existingKey = "CRANE_APPLY_EXISTING"
		removedKey  = "CRANE_APPLY_REMOVE"
		newKey      = "CRANE_APPLY_NEW"
	)

	t.Setenv(existingKey, "original")
	t.Setenv(removedKey, "to-remove")
	os.Unsetenv(newKey)

	restore, err := ApplyEnv(map[string]string{
		existingKey: "updated",
		removedKey:  "",
		newKey:      "fresh",
	})
	if err != nil {
		t.Fatalf("ApplyEnv returned error: %v", err)
	}
	if restore == nil {
		t.Fatalf("ApplyEnv did not return restore function")
	}

	if got := os.Getenv(existingKey); got != "updated" {
		t.Fatalf("expected %s=%q, got %q", existingKey, "updated", got)
	}
	if _, ok := os.LookupEnv(removedKey); ok {
		t.Fatalf("expected %s to be unset", removedKey)
	}
	if got := os.Getenv(newKey); got != "fresh" {
		t.Fatalf("expected %s=%q, got %q", newKey, "fresh", got)
	}

	restore()

	if got := os.Getenv(existingKey); got != "original" {
		t.Fatalf("after restore expected %s=%q, got %q", existingKey, "original", got)
	}
	if got := os.Getenv(removedKey); got != "to-remove" {
		t.Fatalf("after restore expected %s=%q, got %q", removedKey, "to-remove", got)
	}
	if _, ok := os.LookupEnv(newKey); ok {
		t.Fatalf("after restore expected %s to be unset", newKey)
	}
}

func TestApplyEnvError(t *testing.T) {
	t.Parallel()

	restore, err := ApplyEnv(map[string]string{
		"INVALID=KEY": "value",
	})
	if err == nil {
		t.Fatalf("ApplyEnv expected error for invalid key")
	}
	if restore != nil {
		t.Fatalf("ApplyEnv should not return restore function on error")
	}
}

func TestRestoreEnv(t *testing.T) {
	const (
		valueKey  = "CRANE_RESTORE_VALUE"
		deleteKey = "CRANE_RESTORE_DELETE"
	)

	t.Setenv(valueKey, "mutated")
	t.Setenv(deleteKey, "stale")

	original := "original"
	snapshot := map[string]*string{
		valueKey:  &original,
		deleteKey: nil,
	}

	RestoreEnv(snapshot)

	if got := os.Getenv(valueKey); got != "original" {
		t.Fatalf("RestoreEnv expected %s=%q, got %q", valueKey, "original", got)
	}
	if _, ok := os.LookupEnv(deleteKey); ok {
		t.Fatalf("RestoreEnv expected %s to be unset", deleteKey)
	}
}
