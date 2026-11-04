package engine

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	metatypes "CraneFrontEnd/tool/meta-cni/pkg/types"

	"github.com/containernetworking/cni/pkg/skel"
	cnitypes "github.com/containernetworking/cni/pkg/types"
	types100 "github.com/containernetworking/cni/pkg/types/100"
)

func TestFindGRESAnnotations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		annotations  map[string]string
		pipelineName string
		want         []gresInstance
	}{
		{
			name:         "nil annotations",
			annotations:  nil,
			pipelineName: "roce",
			want:         nil,
		},
		{
			name:         "no matching annotations",
			annotations:  map[string]string{"other/key": "val"},
			pipelineName: "roce",
			want:         nil,
		},
		{
			name: "single match",
			annotations: map[string]string{
				"cranesched.internal/meta-cni/gres/roce/0": "0000:86:00.2",
			},
			pipelineName: "roce",
			want: []gresInstance{
				{Index: 0, Device: "0000:86:00.2"},
			},
		},
		{
			name: "multiple matches sorted",
			annotations: map[string]string{
				"cranesched.internal/meta-cni/gres/roce/2": "dev2",
				"cranesched.internal/meta-cni/gres/roce/0": "dev0",
				"cranesched.internal/meta-cni/gres/roce/1": "dev1",
			},
			pipelineName: "roce",
			want: []gresInstance{
				{Index: 0, Device: "dev0"},
				{Index: 1, Device: "dev1"},
				{Index: 2, Device: "dev2"},
			},
		},
		{
			name: "different pipelines don't mix",
			annotations: map[string]string{
				"cranesched.internal/meta-cni/gres/roce/0": "roce-dev",
				"cranesched.internal/meta-cni/gres/ib/0":   "ib-dev",
			},
			pipelineName: "roce",
			want: []gresInstance{
				{Index: 0, Device: "roce-dev"},
			},
		},
		{
			name: "invalid index is skipped",
			annotations: map[string]string{
				"cranesched.internal/meta-cni/gres/roce/0":   "dev0",
				"cranesched.internal/meta-cni/gres/roce/abc": "devX",
			},
			pipelineName: "roce",
			want: []gresInstance{
				{Index: 0, Device: "dev0"},
			},
		},
		{
			name: "negative index is skipped",
			annotations: map[string]string{
				"cranesched.internal/meta-cni/gres/roce/-1": "devNeg",
				"cranesched.internal/meta-cni/gres/roce/0":  "dev0",
			},
			pipelineName: "roce",
			want: []gresInstance{
				{Index: 0, Device: "dev0"},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := findGRESAnnotations(tt.annotations, tt.pipelineName)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("findGRESAnnotations() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpandTemplate(t *testing.T) {
	t.Parallel()

	pipeline := &metatypes.Pipeline{
		Name:         "roce",
		IfNamePrefix: "roce",
		Delegates: []metatypes.DelegateEntry{
			{
				Name: "sriov",
				Type: "sriov",
				Conf: json.RawMessage(`{"type":"sriov","deviceID":"{{.Gres.Device}}"}`),
			},
		},
	}

	rp, err := expandTemplate(pipeline, 0, "0000:86:00.2", map[string]string{"FOO": "bar"})
	if err != nil {
		t.Fatalf("expandTemplate() error = %v", err)
	}

	if rp.Name != "roce-0" {
		t.Errorf("name = %q, want %q", rp.Name, "roce-0")
	}
	if rp.IfName != "roce0" {
		t.Errorf("ifName = %q, want %q", rp.IfName, "roce0")
	}
	if rp.InstanceIndex != 0 {
		t.Errorf("instanceIndex = %d, want 0", rp.InstanceIndex)
	}
	if rp.GRESDevice != "0000:86:00.2" {
		t.Errorf("gresDevice = %q, want %q", rp.GRESDevice, "0000:86:00.2")
	}

	var conf map[string]any
	if err := json.Unmarshal(rp.Delegates[0].Conf, &conf); err != nil {
		t.Fatalf("unmarshal conf: %v", err)
	}
	if conf["deviceID"] != "0000:86:00.2" {
		t.Fatalf("deviceID = %v, want %q", conf["deviceID"], "0000:86:00.2")
	}
	if conf["type"] != "sriov" {
		t.Errorf("type = %v, want %q", conf["type"], "sriov")
	}
}

func TestExpandTemplateWithArgsVar(t *testing.T) {
	t.Parallel()

	pipeline := &metatypes.Pipeline{
		Name:         "net",
		IfNamePrefix: "net",
		Delegates: []metatypes.DelegateEntry{
			{
				Name: "plug",
				Type: "plug",
				Conf: json.RawMessage(`{"type":"plug","customField":"{{.Args.MY_KEY}}"}`),
			},
		},
	}

	rp, err := expandTemplate(pipeline, 1, "dev1", map[string]string{"MY_KEY": "my_value"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var conf map[string]any
	if err := json.Unmarshal(rp.Delegates[0].Conf, &conf); err != nil {
		t.Fatalf("unmarshal conf: %v", err)
	}
	if conf["customField"] != "my_value" {
		t.Errorf("customField = %v, want %q", conf["customField"], "my_value")
	}
}

func TestExpandTemplateDeepCopy(t *testing.T) {
	t.Parallel()

	original := &metatypes.Pipeline{
		Name:         "roce",
		IfNamePrefix: "roce",
		Delegates: []metatypes.DelegateEntry{
			{
				Name: "sriov",
				Type: "sriov",
				Conf: json.RawMessage(`{"type":"sriov","deviceID":"{{.Gres.Device}}"}`),
			},
		},
	}

	rp0, err := expandTemplate(original, 0, "dev0", nil)
	if err != nil {
		t.Fatalf("expand 0: %v", err)
	}
	rp1, err := expandTemplate(original, 1, "dev1", nil)
	if err != nil {
		t.Fatalf("expand 1: %v", err)
	}

	var conf0, conf1 map[string]any
	json.Unmarshal(rp0.Delegates[0].Conf, &conf0)
	json.Unmarshal(rp1.Delegates[0].Conf, &conf1)

	if conf0["deviceID"] != "dev0" {
		t.Errorf("instance 0 deviceID = %v, want dev0", conf0["deviceID"])
	}
	if conf1["deviceID"] != "dev1" {
		t.Errorf("instance 1 deviceID = %v, want dev1", conf1["deviceID"])
	}

	// Original Conf should still contain the template placeholder.
	var origConf map[string]any
	json.Unmarshal(original.Delegates[0].Conf, &origConf)
	if origConf["deviceID"] != "{{.Gres.Device}}" {
		t.Errorf("original conf was mutated by expandTemplate: %v", origConf["deviceID"])
	}
}

func TestValidateUniqueIfNames(t *testing.T) {
	t.Parallel()

	t.Run("unique names pass", func(t *testing.T) {
		t.Parallel()
		pipelines := []ResolvedPipeline{
			{Pipeline: metatypes.Pipeline{Name: "a", IfName: "eth0"}},
			{Pipeline: metatypes.Pipeline{Name: "b", IfName: "roce0"}},
		}
		if err := validateUniqueIfNames(pipelines); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("duplicate names fail", func(t *testing.T) {
		t.Parallel()
		pipelines := []ResolvedPipeline{
			{Pipeline: metatypes.Pipeline{Name: "a", IfName: "eth0"}},
			{Pipeline: metatypes.Pipeline{Name: "b", IfName: "eth0"}},
		}
		if err := validateUniqueIfNames(pipelines); err == nil {
			t.Error("expected error for duplicate ifNames")
		}
	})

	t.Run("empty list passes", func(t *testing.T) {
		t.Parallel()
		if err := validateUniqueIfNames(nil); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestBuildRuntimeEnv(t *testing.T) {
	t.Parallel()

	args := &skel.CmdArgs{
		ContainerID: "ctr1",
		Netns:       "/proc/1/ns/net",
		IfName:      "eth0",
		Args:        "K1=V1",
		Path:        "/opt/cni/bin",
	}

	t.Run("no overrides uses args values", func(t *testing.T) {
		t.Parallel()
		pipeline := &metatypes.Pipeline{Name: "eth", IfName: "eth0"}
		delegate := &metatypes.DelegateEntry{Name: "d"}

		env, err := buildRuntimeEnv(args, nil, pipeline, delegate)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if env["CNI_CONTAINERID"] != "ctr1" {
			t.Errorf("CNI_CONTAINERID = %q, want %q", env["CNI_CONTAINERID"], "ctr1")
		}
		if env["CNI_IFNAME"] != "eth0" {
			t.Errorf("CNI_IFNAME = %q, want %q", env["CNI_IFNAME"], "eth0")
		}
	})

	t.Run("pipeline ifName overrides args.IfName", func(t *testing.T) {
		t.Parallel()
		pipeline := &metatypes.Pipeline{Name: "rdma", IfName: "rdma0"}
		delegate := &metatypes.DelegateEntry{Name: "d"}

		env, err := buildRuntimeEnv(args, nil, pipeline, delegate)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if env["CNI_IFNAME"] != "rdma0" {
			t.Errorf("CNI_IFNAME = %q, want %q", env["CNI_IFNAME"], "rdma0")
		}
	})

	t.Run("four layer priority", func(t *testing.T) {
		t.Parallel()
		global := &metatypes.RuntimeOverride{IfName: "global"}
		pipeline := &metatypes.Pipeline{
			Name:   "p",
			IfName: "pipeline-ifname",
			RuntimeOverride: &metatypes.RuntimeOverride{
				IfName: "pipeline-override",
			},
		}
		delegate := &metatypes.DelegateEntry{
			Name: "d",
			RuntimeOverride: &metatypes.RuntimeOverride{
				IfName: "delegate-override",
			},
		}

		env, err := buildRuntimeEnv(args, global, pipeline, delegate)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if env["CNI_IFNAME"] != "delegate-override" {
			t.Errorf("CNI_IFNAME = %q, want %q", env["CNI_IFNAME"], "delegate-override")
		}
	})

	t.Run("global override without pipeline override", func(t *testing.T) {
		t.Parallel()
		global := &metatypes.RuntimeOverride{ContainerID: "global-ctr"}
		pipeline := &metatypes.Pipeline{Name: "p", IfName: "eth0"}
		delegate := &metatypes.DelegateEntry{Name: "d"}

		env, err := buildRuntimeEnv(args, global, pipeline, delegate)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if env["CNI_CONTAINERID"] != "global-ctr" {
			t.Errorf("CNI_CONTAINERID = %q, want %q", env["CNI_CONTAINERID"], "global-ctr")
		}
	})
}

func TestEffectiveConf(t *testing.T) {
	t.Parallel()

	t.Run("nil delegate", func(t *testing.T) {
		t.Parallel()
		_, _, err := effectiveConf(nil, "1.0.0", nil, nil)
		if err == nil {
			t.Fatal("expected error for nil delegate")
		}
	})

	t.Run("type only, no conf", func(t *testing.T) {
		t.Parallel()
		d := &metatypes.DelegateEntry{Name: "d", Type: "bridge"}
		confBytes, pluginType, err := effectiveConf(d, "1.0.0", nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pluginType != "bridge" {
			t.Errorf("pluginType = %q, want bridge", pluginType)
		}
		var payload map[string]any
		json.Unmarshal(confBytes, &payload)
		if payload["type"] != "bridge" {
			t.Errorf("conf type = %v, want bridge", payload["type"])
		}
		if payload["cniVersion"] != "1.0.0" {
			t.Errorf("conf cniVersion = %v, want 1.0.0", payload["cniVersion"])
		}
		if payload["name"] != "d" {
			t.Errorf("conf name = %v, want d", payload["name"])
		}
	})

	t.Run("with conf", func(t *testing.T) {
		t.Parallel()
		d := &metatypes.DelegateEntry{
			Name: "d",
			Conf: json.RawMessage(`{"type": "macvlan", "master": "eth0"}`),
		}
		confBytes, pluginType, err := effectiveConf(d, "1.0.0", nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pluginType != "macvlan" {
			t.Errorf("pluginType = %q, want macvlan", pluginType)
		}
		var payload map[string]any
		json.Unmarshal(confBytes, &payload)
		if payload["master"] != "eth0" {
			t.Errorf("master = %v, want eth0", payload["master"])
		}
		if payload["cniVersion"] != "1.0.0" {
			t.Errorf("cniVersion should be injected")
		}
	})

	t.Run("runtimeConfig passthrough", func(t *testing.T) {
		t.Parallel()
		d := &metatypes.DelegateEntry{Name: "d", Type: "bridge"}
		rc := map[string]any{"portMappings": []any{}}
		confBytes, _, err := effectiveConf(d, "1.0.0", nil, rc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var payload map[string]any
		json.Unmarshal(confBytes, &payload)
		if _, ok := payload["runtimeConfig"]; !ok {
			t.Error("runtimeConfig should be passed through")
		}
	})

	t.Run("missing type errors", func(t *testing.T) {
		t.Parallel()
		d := &metatypes.DelegateEntry{Name: "d"}
		_, _, err := effectiveConf(d, "1.0.0", nil, nil)
		if err == nil {
			t.Fatal("expected error for missing type")
		}
	})

	t.Run("conf without type errors", func(t *testing.T) {
		t.Parallel()
		d := &metatypes.DelegateEntry{
			Name: "d",
			Conf: json.RawMessage(`{"name": "test"}`),
		}
		_, _, err := effectiveConf(d, "1.0.0", nil, nil)
		if err == nil {
			t.Fatal("expected error for conf without type")
		}
	})
}

func TestResolvePipelines(t *testing.T) {
	t.Parallel()

	t.Run("static only", func(t *testing.T) {
		t.Parallel()
		conf := &metatypes.MetaPluginConf{
			Pipelines: []metatypes.Pipeline{
				{Name: "eth", IfName: "eth0", Delegates: []metatypes.DelegateEntry{{Name: "d", Type: "bridge"}}},
			},
		}
		args := &skel.CmdArgs{}
		resolved, err := resolvePipelines(conf, args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(resolved) != 1 {
			t.Fatalf("len(resolved) = %d, want 1", len(resolved))
		}
		if resolved[0].Name != "eth" || resolved[0].IfName != "eth0" {
			t.Errorf("resolved pipeline: name=%q ifName=%q", resolved[0].Name, resolved[0].IfName)
		}
		if resolved[0].InstanceIndex != -1 {
			t.Errorf("static pipeline should have InstanceIndex=-1")
		}
	})

	t.Run("template with annotations", func(t *testing.T) {
		t.Parallel()
		conf := &metatypes.MetaPluginConf{
			Pipelines: []metatypes.Pipeline{
				{Name: "roce", IfNamePrefix: "roce", Delegates: []metatypes.DelegateEntry{
					{Name: "sriov", Type: "sriov",
						Conf: json.RawMessage(`{"type":"sriov","deviceID":"{{.Gres.Device}}"}`)},
				}},
			},
			RuntimeConfig: map[string]any{
				"io.kubernetes.cri.pod-annotations": map[string]any{
					"cranesched.internal/meta-cni/gres/roce/0": "0000:86:00.2",
					"cranesched.internal/meta-cni/gres/roce/1": "0000:86:00.3",
				},
			},
		}
		args := &skel.CmdArgs{}
		resolved, err := resolvePipelines(conf, args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(resolved) != 2 {
			t.Fatalf("len(resolved) = %d, want 2", len(resolved))
		}
		if resolved[0].Name != "roce-0" || resolved[0].IfName != "roce0" {
			t.Errorf("instance 0: name=%q ifName=%q", resolved[0].Name, resolved[0].IfName)
		}
		if resolved[1].Name != "roce-1" || resolved[1].IfName != "roce1" {
			t.Errorf("instance 1: name=%q ifName=%q", resolved[1].Name, resolved[1].IfName)
		}

		var conf0 map[string]any
		json.Unmarshal(resolved[0].Delegates[0].Conf, &conf0)
		if conf0["deviceID"] != "0000:86:00.2" {
			t.Errorf("instance 0 deviceID = %v", conf0["deviceID"])
		}
	})

	t.Run("template without annotations is skipped", func(t *testing.T) {
		t.Parallel()
		conf := &metatypes.MetaPluginConf{
			Pipelines: []metatypes.Pipeline{
				{Name: "eth", IfName: "eth0", Delegates: []metatypes.DelegateEntry{{Name: "d", Type: "bridge"}}},
				{Name: "roce", IfNamePrefix: "roce", Delegates: []metatypes.DelegateEntry{{Name: "s", Type: "sriov"}}},
			},
		}
		args := &skel.CmdArgs{}
		resolved, err := resolvePipelines(conf, args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(resolved) != 1 {
			t.Fatalf("len(resolved) = %d, want 1 (template should be skipped)", len(resolved))
		}
		if resolved[0].Name != "eth" {
			t.Errorf("only eth pipeline should remain, got %q", resolved[0].Name)
		}
	})

	t.Run("template without annotations is not expanded", func(t *testing.T) {
		t.Parallel()
		conf := &metatypes.MetaPluginConf{
			Pipelines: []metatypes.Pipeline{
				{
					Name:         "roce",
					IfNamePrefix: "roce",
					Delegates: []metatypes.DelegateEntry{
						{
							Name: "sriov",
							Type: "sriov",
							// If template expansion were attempted, this would fail.
							Conf: json.RawMessage(`{"type":"sriov","deviceID":"{{if .Gres.Device}}"}`),
						},
					},
				},
			},
		}
		args := &skel.CmdArgs{}
		resolved, err := resolvePipelines(conf, args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(resolved) != 0 {
			t.Fatalf("len(resolved) = %d, want 0", len(resolved))
		}
	})

	t.Run("mixed static and template", func(t *testing.T) {
		t.Parallel()
		conf := &metatypes.MetaPluginConf{
			Pipelines: []metatypes.Pipeline{
				{Name: "eth", IfName: "eth0", Delegates: []metatypes.DelegateEntry{{Name: "d", Type: "bridge"}}},
				{Name: "roce", IfNamePrefix: "roce", Delegates: []metatypes.DelegateEntry{
					{Name: "s", Type: "sriov", Conf: json.RawMessage(`{"type":"sriov"}`)},
				}},
				{Name: "ib", IfNamePrefix: "ib", Delegates: []metatypes.DelegateEntry{
					{Name: "s", Type: "ib-sriov", Conf: json.RawMessage(`{"type":"ib-sriov"}`)},
				}},
			},
			RuntimeConfig: map[string]any{
				"io.kubernetes.cri.pod-annotations": map[string]any{
					"cranesched.internal/meta-cni/gres/roce/0": "roce-dev0",
					"cranesched.internal/meta-cni/gres/ib/0":   "ib-dev0",
					"cranesched.internal/meta-cni/gres/ib/1":   "ib-dev1",
				},
			},
		}
		args := &skel.CmdArgs{}
		resolved, err := resolvePipelines(conf, args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(resolved) != 4 {
			t.Fatalf("len(resolved) = %d, want 4", len(resolved))
		}
		names := make([]string, len(resolved))
		for i, rp := range resolved {
			names[i] = rp.Name
		}
		want := []string{"eth", "roce-0", "ib-0", "ib-1"}
		if !reflect.DeepEqual(names, want) {
			t.Errorf("pipeline names = %v, want %v", names, want)
		}
	})
}

func TestResolvePipelinesForDel(t *testing.T) {
	t.Parallel()

	t.Run("template pipeline without annotations is skipped even if prevResult exists", func(t *testing.T) {
		t.Parallel()

		conf := &metatypes.MetaPluginConf{
			PluginConf: cnitypes.PluginConf{PrevResult: &types100.Result{
				CNIVersion: "1.0.0",
				Interfaces: []*types100.Interface{
					{Name: "roce0", Sandbox: "/proc/123/ns/net"},
				},
			}},
			Pipelines: []metatypes.Pipeline{
				{
					Name:         "roce",
					IfNamePrefix: "roce",
					Delegates: []metatypes.DelegateEntry{
						{
							Name: "sriov",
							Type: "sriov",
							Conf: json.RawMessage(`{"type":"sriov","deviceID":"{{.Gres.Device}}"}`),
						},
					},
				},
			},
		}

		resolved, err := resolvePipelinesForDel(conf, &skel.CmdArgs{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(resolved) != 0 {
			t.Fatalf("len(resolved) = %d, want 0", len(resolved))
		}
	})

	t.Run("template pipeline without annotations is not expanded on DEL/CHECK", func(t *testing.T) {
		t.Parallel()

		conf := &metatypes.MetaPluginConf{
			PluginConf: cnitypes.PluginConf{PrevResult: &types100.Result{
				CNIVersion: "1.0.0",
				Interfaces: []*types100.Interface{
					{Name: "roce0", Sandbox: "/proc/123/ns/net"},
				},
			}},
			Pipelines: []metatypes.Pipeline{
				{
					Name:         "roce",
					IfNamePrefix: "roce",
					Delegates: []metatypes.DelegateEntry{
						{
							Name: "sriov",
							Type: "sriov",
							// If template expansion were attempted, this would fail.
							Conf: json.RawMessage(`{"type":"sriov","deviceID":"{{if .Gres.Device}}"}`),
						},
					},
				},
			},
		}

		resolved, err := resolvePipelinesForDel(conf, &skel.CmdArgs{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(resolved) != 0 {
			t.Fatalf("len(resolved) = %d, want 0", len(resolved))
		}
	})

	t.Run("static pipeline still resolves when template pipeline is skipped", func(t *testing.T) {
		t.Parallel()

		conf := &metatypes.MetaPluginConf{
			PluginConf: cnitypes.PluginConf{PrevResult: &types100.Result{
				CNIVersion: "1.0.0",
				Interfaces: []*types100.Interface{
					{Name: "eth0", Sandbox: "/proc/123/ns/net"},
				},
			}},
			Pipelines: []metatypes.Pipeline{
				{Name: "eth", IfName: "eth0", Delegates: []metatypes.DelegateEntry{{Name: "bridge", Type: "bridge"}}},
				{
					Name:         "roce",
					IfNamePrefix: "roce",
					Delegates: []metatypes.DelegateEntry{
						{Name: "sriov", Type: "sriov", Conf: json.RawMessage(`{"type":"sriov","deviceID":"{{.Gres.Device}}"}`)},
					},
				},
			},
		}

		resolved, err := resolvePipelinesForDel(conf, &skel.CmdArgs{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(resolved) != 1 {
			t.Fatalf("len(resolved) = %d, want 1", len(resolved))
		}
		if resolved[0].Name != "eth" || resolved[0].IfName != "eth0" {
			t.Fatalf("resolved[0] = name=%q ifName=%q, want eth/eth0", resolved[0].Name, resolved[0].IfName)
		}
	})

	t.Run("annotations allow DEL resolution", func(t *testing.T) {
		t.Parallel()
		conf := &metatypes.MetaPluginConf{
			PluginConf: cnitypes.PluginConf{PrevResult: &types100.Result{
				CNIVersion: "1.0.0",
				Interfaces: []*types100.Interface{{Name: "roce0", Sandbox: "/proc/123/ns/net"}},
			}},
			Pipelines: []metatypes.Pipeline{
				{Name: "roce", IfNamePrefix: "roce", Delegates: []metatypes.DelegateEntry{
					{Name: "sriov", Type: "sriov",
						Conf: json.RawMessage(`{"type":"sriov","deviceID":"{{.Gres.Device}}"}`)},
				}},
			},
			RuntimeConfig: map[string]any{
				"io.kubernetes.cri.pod-annotations": map[string]any{
					"cranesched.internal/meta-cni/gres/roce/0": "0000:86:00.2",
				},
			},
		}

		resolved, err := resolvePipelinesForDel(conf, &skel.CmdArgs{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(resolved) != 1 {
			t.Fatalf("len(resolved) = %d, want 1", len(resolved))
		}

		var delegateConf map[string]any
		if err := json.Unmarshal(resolved[0].Delegates[0].Conf, &delegateConf); err != nil {
			t.Fatalf("unmarshal delegate conf: %v", err)
		}
		if delegateConf["deviceID"] != "0000:86:00.2" {
			t.Errorf("deviceID = %v, want %q", delegateConf["deviceID"], "0000:86:00.2")
		}
	})
}

func TestContextWithTimeout(t *testing.T) {
	t.Parallel()

	t.Run("zero timeout returns background", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := contextWithTimeout(0)
		defer cancel()
		if _, ok := ctx.Deadline(); ok {
			t.Error("expected no deadline for timeout=0")
		}
	})

	t.Run("negative timeout returns background", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := contextWithTimeout(-1)
		defer cancel()
		if _, ok := ctx.Deadline(); ok {
			t.Error("expected no deadline for timeout=-1")
		}
	})

	t.Run("positive timeout has deadline", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := contextWithTimeout(5)
		defer cancel()
		if _, ok := ctx.Deadline(); !ok {
			t.Error("expected deadline for timeout=5")
		}
	})
}

func TestExecutePrevResultRequirement(t *testing.T) {
	t.Parallel()

	t.Run("check requires prevResult", func(t *testing.T) {
		t.Parallel()
		conf := &metatypes.MetaPluginConf{}
		_, err := Execute(conf, ActionCheck, &skel.CmdArgs{})
		if err == nil {
			t.Fatal("expected error for CHECK without prevResult")
		}
	})

	t.Run("del allows nil prevResult", func(t *testing.T) {
		t.Parallel()
		conf := &metatypes.MetaPluginConf{}
		_, err := Execute(conf, ActionDel, &skel.CmdArgs{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestRollbackPipelines(t *testing.T) {
	t.Parallel()

	t.Run("empty pipelines returns nil", func(t *testing.T) {
		t.Parallel()
		err := rollbackPipelines(context.Background(), &metatypes.MetaPluginConf{}, &skel.CmdArgs{}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("returns first rollback error in reverse order", func(t *testing.T) {
		t.Parallel()
		pipelines := []ResolvedPipeline{
			{Pipeline: metatypes.Pipeline{
				Name: "p1",
				Delegates: []metatypes.DelegateEntry{{
					Name: "d1",
					Type: "sriov",
					Conf: json.RawMessage(`{invalid-json`),
				}},
			}},
			{Pipeline: metatypes.Pipeline{
				Name: "p2",
				Delegates: []metatypes.DelegateEntry{{
					Name: "d2",
					Type: "sriov",
					Conf: json.RawMessage(`{invalid-json`),
				}},
			}},
		}

		err := rollbackPipelines(context.Background(), &metatypes.MetaPluginConf{}, &skel.CmdArgs{}, pipelines)
		if err == nil {
			t.Fatal("expected rollback error")
		}
		if !strings.Contains(err.Error(), "d2") {
			t.Fatalf("expected first rollback error from reverse order pipeline delegate d2, got: %v", err)
		}
	})
}
