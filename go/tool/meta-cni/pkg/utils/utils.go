package utils

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

func ParseArgs(input string) map[string]string {
	result := make(map[string]string)
	for entry := range strings.SplitSeq(input, ";") {
		if entry == "" {
			continue
		}
		keyVal := strings.SplitN(entry, "=", 2)
		if len(keyVal) != 2 {
			continue
		}
		result[keyVal[0]] = keyVal[1]
	}
	return result
}

func MergeArgs(base string, overrides []string) (string, error) {
	if len(overrides) == 0 {
		return base, nil
	}

	current := ParseArgs(base)

	expressions, err := ParseManipulators(overrides)
	if err != nil {
		return "", err
	}

	for _, expr := range expressions {
		if expr.Delete {
			delete(current, expr.Key)
			continue
		}
		current[expr.Key] = expr.Value
	}

	if len(current) == 0 {
		return "", nil
	}

	keys := make([]string, 0, len(current))
	for key := range current {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, fmt.Sprintf("%s=%s", key, current[key]))
	}
	return strings.Join(pairs, ";"), nil
}

// NOTE: env is modified in place
func MergeEnvs(env map[string]string, entries []string) error {
	if len(entries) == 0 {
		return nil
	}

	expressions, err := ParseManipulators(entries)
	if err != nil {
		return err
	}

	for _, expr := range expressions {
		if expr.Delete {
			delete(env, expr.Key)
			continue
		}
		env[expr.Key] = expr.Value
	}
	return nil
}

func ApplyEnv(env map[string]string) (func(), error) {
	snapshot := make(map[string]*string, len(env))

	for key, value := range env {
		if _, recorded := snapshot[key]; !recorded {
			if prev, ok := os.LookupEnv(key); ok {
				val := prev
				snapshot[key] = &val
			} else {
				snapshot[key] = nil
			}
		}

		var err error
		if value == "" {
			err = os.Unsetenv(key)
		} else {
			err = os.Setenv(key, value)
		}

		if err != nil {
			RestoreEnv(snapshot)
			return nil, fmt.Errorf("set env %s: %w", key, err)
		}
	}

	return func() {
		RestoreEnv(snapshot)
	}, nil
}

func RestoreEnv(snapshot map[string]*string) {
	for key, value := range snapshot {
		if value == nil {
			_ = os.Unsetenv(key)
			continue
		}
		_ = os.Setenv(key, *value)
	}
}
