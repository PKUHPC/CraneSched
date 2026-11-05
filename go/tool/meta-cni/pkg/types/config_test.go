package config

import (
	"encoding/json"
	"testing"
)

func TestLoadMetaPluginConf(t *testing.T) {
	t.Parallel()

	t.Run("valid minimal config", func(t *testing.T) {
		data := []byte(`{
			"cniVersion": "1.0.0",
			"name": "test",
			"type": "crane-meta",
			"pipelines": [
				{
					"name": "eth",
					"ifName": "eth0",
					"delegates": [{"name": "d1", "type": "bridge"}]
				}
			]
		}`)
		conf, err := LoadMetaPluginConf(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if conf.CNIVersion != "1.0.0" {
			t.Errorf("cniVersion = %q, want %q", conf.CNIVersion, "1.0.0")
		}
		if conf.Name != "test" {
			t.Errorf("name = %q, want %q", conf.Name, "test")
		}
		if len(conf.Pipelines) != 1 {
			t.Fatalf("len(pipelines) = %d, want 1", len(conf.Pipelines))
		}
	})

	t.Run("sets default cniVersion", func(t *testing.T) {
		data := []byte(`{"name": "test", "type": "crane-meta", "pipelines": [{"name": "e", "ifName": "eth0", "delegates": [{"name": "d", "type": "bridge"}]}]}`)
		conf, err := LoadMetaPluginConf(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if conf.CNIVersion == "" {
			t.Error("expected default cniVersion to be set")
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		_, err := LoadMetaPluginConf([]byte(`{invalid`))
		if err == nil {
			t.Fatal("expected error for invalid JSON")
		}
	})
}

func TestValidate(t *testing.T) {
	t.Parallel()

	validDelegate := DelegateEntry{Name: "d1", Type: "bridge"}

	tests := []struct {
		name      string
		conf      *MetaPluginConf
		expectErr string // substring expected in error, empty = no error
	}{
		{
			name:      "nil config",
			conf:      nil,
			expectErr: "config is nil",
		},
		{
			name:      "missing name",
			conf:      &MetaPluginConf{},
			expectErr: "name is required",
		},
		{
			name:      "no pipelines",
			conf:      &MetaPluginConf{},
			expectErr: "at least one pipeline",
		},
		{
			name: "pipeline missing name",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{{IfName: "eth0", Delegates: []DelegateEntry{validDelegate}}},
			},
			expectErr: "name is required",
		},
		{
			name: "duplicate pipeline name",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "a", IfName: "eth0", Delegates: []DelegateEntry{validDelegate}},
					{Name: "a", IfName: "eth1", Delegates: []DelegateEntry{validDelegate}},
				},
			},
			expectErr: "duplicate name",
		},
		{
			name: "neither ifName nor ifNamePrefix",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "a", Delegates: []DelegateEntry{validDelegate}},
				},
			},
			expectErr: "exactly one of ifName or ifNamePrefix",
		},
		{
			name: "both ifName and ifNamePrefix",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "a", IfName: "eth0", IfNamePrefix: "eth", Delegates: []DelegateEntry{validDelegate}},
				},
			},
			expectErr: "exactly one of ifName or ifNamePrefix",
		},
		{
			name: "duplicate static ifName",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "a", IfName: "eth0", Delegates: []DelegateEntry{validDelegate}},
					{Name: "b", IfName: "eth0", Delegates: []DelegateEntry{validDelegate}},
				},
			},
			expectErr: "duplicate ifName",
		},
		{
			name: "duplicate template prefix",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "a", IfNamePrefix: "roce", Delegates: []DelegateEntry{validDelegate}},
					{Name: "b", IfNamePrefix: "roce", Delegates: []DelegateEntry{validDelegate}},
				},
			},
			expectErr: "duplicate ifNamePrefix",
		},
		{
			name: "overlapping template prefixes conflict",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "a", IfNamePrefix: "eth", Delegates: []DelegateEntry{validDelegate}},
					{Name: "b", IfNamePrefix: "eth1", Delegates: []DelegateEntry{validDelegate}},
				},
			},
			expectErr: "conflicts with template prefix",
		},
		{
			name: "static ifName conflicts with template prefix",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "a", IfName: "roce0", Delegates: []DelegateEntry{validDelegate}},
					{Name: "b", IfNamePrefix: "roce", Delegates: []DelegateEntry{validDelegate}},
				},
			},
			expectErr: "conflicts with template prefix",
		},
		{
			name: "static lexical overlap without numeric suffix is allowed",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "a", IfName: "roce-mgmt", Delegates: []DelegateEntry{validDelegate}},
					{Name: "b", IfNamePrefix: "roce", Delegates: []DelegateEntry{validDelegate}},
				},
			},
			expectErr: "",
		},
		{
			name: "template prefixes lexical overlap without canonical collision is allowed",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "a", IfNamePrefix: "roce", Delegates: []DelegateEntry{validDelegate}},
					{Name: "b", IfNamePrefix: "roce0", Delegates: []DelegateEntry{validDelegate}},
				},
			},
			expectErr: "",
		},
		{
			name: "static non-canonical numeric suffix does not conflict",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "a", IfName: "roce01", Delegates: []DelegateEntry{validDelegate}},
					{Name: "b", IfNamePrefix: "roce", Delegates: []DelegateEntry{validDelegate}},
				},
			},
			expectErr: "",
		},
		{
			name: "no delegates",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "a", IfName: "eth0", Delegates: nil},
				},
			},
			expectErr: "at least one delegate",
		},
		{
			name: "delegate missing type and conf",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "a", IfName: "eth0", Delegates: []DelegateEntry{{Name: "d"}}},
				},
			},
			expectErr: "must specify either type or conf",
		},
		{
			name: "delegate missing name without conf name",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "a", IfName: "eth0", Delegates: []DelegateEntry{{Type: "bridge"}}},
				},
			},
			expectErr: "delegate name is required",
		},
		{
			name: "pipeline runtimeOverride ifName is rejected",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{
						Name:      "a",
						IfName:    "eth0",
						Delegates: []DelegateEntry{validDelegate},
						RuntimeOverride: &RuntimeOverride{
							IfName: "eth1",
						},
					},
				},
			},
			expectErr: "runtimeOverride.ifName is not allowed",
		},
		{
			name: "delegate runtimeOverride ifName is rejected",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{
						Name:   "a",
						IfName: "eth0",
						Delegates: []DelegateEntry{{
							Name: "d1",
							Type: "bridge",
							RuntimeOverride: &RuntimeOverride{
								IfName: "eth1",
							},
						}},
					},
				},
			},
			expectErr: "runtimeOverride.ifName is not allowed",
		},
		{
			name: "valid static pipeline",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "eth", IfName: "eth0", Delegates: []DelegateEntry{validDelegate}},
				},
			},
			expectErr: "",
		},
		{
			name: "valid template pipeline",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "roce", IfNamePrefix: "roce", Delegates: []DelegateEntry{validDelegate}},
				},
			},
			expectErr: "",
		},
		{
			name: "valid mixed static and template",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "eth", IfName: "eth0", Delegates: []DelegateEntry{validDelegate}},
					{Name: "roce", IfNamePrefix: "roce", Delegates: []DelegateEntry{validDelegate}},
					{Name: "ib", IfNamePrefix: "ib", Delegates: []DelegateEntry{validDelegate}},
				},
			},
			expectErr: "",
		},
		{
			name: "delegate with name in conf",
			conf: &MetaPluginConf{
				Pipelines: []Pipeline{
					{Name: "a", IfName: "eth0", Delegates: []DelegateEntry{
						{Conf: json.RawMessage(`{"name": "from-conf", "type": "bridge"}`)},
					}},
				},
			},
			expectErr: "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Ensure Name is set for cases that should pass name validation
			if tt.conf != nil && tt.conf.Name == "" && tt.expectErr != "name is required" {
				tt.conf.Name = "test-net"
			}

			err := tt.conf.Validate()
			if tt.expectErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.expectErr)
			}
			if !contains(err.Error(), tt.expectErr) {
				t.Fatalf("error %q does not contain %q", err.Error(), tt.expectErr)
			}
		})
	}
}

func TestPipelineIsTemplate(t *testing.T) {
	t.Parallel()

	static := Pipeline{Name: "eth", IfName: "eth0"}
	if static.IsTemplate() {
		t.Error("static pipeline should not be template")
	}

	tmpl := Pipeline{Name: "roce", IfNamePrefix: "roce"}
	if !tmpl.IsTemplate() {
		t.Error("template pipeline should be template")
	}
}

func TestAnnotations(t *testing.T) {
	t.Parallel()

	t.Run("nil runtimeConfig", func(t *testing.T) {
		conf := &MetaPluginConf{}
		if a := conf.Annotations(); a != nil {
			t.Errorf("expected nil, got %v", a)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		conf := &MetaPluginConf{RuntimeConfig: map[string]any{"other": "value"}}
		if a := conf.Annotations(); a != nil {
			t.Errorf("expected nil, got %v", a)
		}
	})

	t.Run("map[string]string type", func(t *testing.T) {
		conf := &MetaPluginConf{
			RuntimeConfig: map[string]any{
				"io.kubernetes.cri.pod-annotations": map[string]string{
					"k1": "v1",
				},
			},
		}
		a := conf.Annotations()
		if a == nil || a["k1"] != "v1" {
			t.Errorf("expected {k1:v1}, got %v", a)
		}
	})

	t.Run("map[string]any type (JSON unmarshalled)", func(t *testing.T) {
		conf := &MetaPluginConf{
			RuntimeConfig: map[string]any{
				"io.kubernetes.cri.pod-annotations": map[string]any{
					"k1": "v1",
					"k2": 42, // non-string, should be skipped
				},
			},
		}
		a := conf.Annotations()
		if a == nil || a["k1"] != "v1" {
			t.Errorf("expected k1=v1, got %v", a)
		}
		if _, ok := a["k2"]; ok {
			t.Error("non-string annotation should be skipped")
		}
	})

	t.Run("unexpected type", func(t *testing.T) {
		conf := &MetaPluginConf{
			RuntimeConfig: map[string]any{
				"io.kubernetes.cri.pod-annotations": 42,
			},
		}
		if a := conf.Annotations(); a != nil {
			t.Errorf("expected nil for unexpected type, got %v", a)
		}
	})
}

func TestDelegateIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		d    *DelegateEntry
		want string
	}{
		{"nil", nil, "<nil>"},
		{"with name", &DelegateEntry{Name: "my-plugin"}, "my-plugin"},
		{"with type only", &DelegateEntry{Type: "bridge"}, "bridge"},
		{"empty", &DelegateEntry{}, "<unknown>"},
		{"name takes precedence", &DelegateEntry{Name: "n", Type: "t"}, "n"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.d.Identifier(); got != tt.want {
				t.Errorf("Identifier() = %q, want %q", got, tt.want)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
