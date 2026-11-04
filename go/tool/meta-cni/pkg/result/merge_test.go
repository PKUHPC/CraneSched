package result

import (
	"net"
	"testing"

	cnitypes "github.com/containernetworking/cni/pkg/types"
	types100 "github.com/containernetworking/cni/pkg/types/100"
)

func TestMergeNilInputs(t *testing.T) {
	t.Parallel()

	t.Run("both nil", func(t *testing.T) {
		t.Parallel()
		got, err := Merge(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})

	t.Run("prev nil", func(t *testing.T) {
		t.Parallel()
		next := &types100.Result{CNIVersion: "1.0.0"}
		got, err := Merge(nil, next)
		if err != nil {
			t.Fatal(err)
		}
		if got != next {
			t.Error("expected next result to be returned directly")
		}
	})

	t.Run("next nil", func(t *testing.T) {
		t.Parallel()
		prev := &types100.Result{CNIVersion: "1.0.0"}
		got, err := Merge(prev, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got != prev {
			t.Error("expected prev result to be returned directly")
		}
	})
}

func TestMergeTwoResults(t *testing.T) {
	t.Parallel()

	ifaceIdx := func(i int) *int { return &i }

	prev := &types100.Result{
		CNIVersion: "1.0.0",
		Interfaces: []*types100.Interface{
			{Name: "eth0", Mac: "aa:bb:cc:dd:ee:00", Sandbox: "/proc/1/ns/net"},
		},
		IPs: []*types100.IPConfig{
			{Address: mustIPNet("10.0.0.2/24"), Interface: ifaceIdx(0)},
		},
		Routes: []*cnitypes.Route{
			{Dst: mustIPNet("0.0.0.0/0")},
		},
		DNS: cnitypes.DNS{
			Nameservers: []string{"8.8.8.8"},
		},
	}

	next := &types100.Result{
		CNIVersion: "1.0.0",
		Interfaces: []*types100.Interface{
			{Name: "roce0", Mac: "11:22:33:44:55:66", Sandbox: "/proc/1/ns/net"},
		},
		IPs: []*types100.IPConfig{
			{Address: mustIPNet("10.56.0.2/16"), Interface: ifaceIdx(0)},
		},
		Routes: []*cnitypes.Route{
			{Dst: mustIPNet("10.56.0.0/16")},
		},
		DNS: cnitypes.DNS{
			Nameservers: []string{"8.8.4.4"},
		},
	}

	merged, err := Merge(prev, next)
	if err != nil {
		t.Fatalf("Merge error: %v", err)
	}

	result100, err := types100.NewResultFromResult(merged)
	if err != nil {
		t.Fatalf("convert merged: %v", err)
	}

	// Check interfaces
	if len(result100.Interfaces) != 2 {
		t.Fatalf("expected 2 interfaces, got %d", len(result100.Interfaces))
	}
	if result100.Interfaces[0].Name != "eth0" {
		t.Errorf("iface 0 name = %q, want eth0", result100.Interfaces[0].Name)
	}
	if result100.Interfaces[1].Name != "roce0" {
		t.Errorf("iface 1 name = %q, want roce0", result100.Interfaces[1].Name)
	}

	// Check IPs
	if len(result100.IPs) != 2 {
		t.Fatalf("expected 2 IPs, got %d", len(result100.IPs))
	}
	if result100.IPs[0].Interface == nil || *result100.IPs[0].Interface != 0 {
		t.Error("IP 0 should reference interface 0")
	}
	if result100.IPs[1].Interface == nil || *result100.IPs[1].Interface != 1 {
		t.Errorf("IP 1 should reference interface 1, got %v", result100.IPs[1].Interface)
	}

	// Check routes
	if len(result100.Routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(result100.Routes))
	}

	// Check DNS
	if len(result100.DNS.Nameservers) != 2 {
		t.Fatalf("expected 2 nameservers, got %d", len(result100.DNS.Nameservers))
	}
}

func TestMergeDeduplicatesInterfaces(t *testing.T) {
	t.Parallel()

	ifaceIdx := func(i int) *int { return &i }

	prev := &types100.Result{
		CNIVersion: "1.0.0",
		Interfaces: []*types100.Interface{
			{Name: "eth0", Mac: "aa:bb:cc:dd:ee:00", Sandbox: "/proc/1/ns/net"},
		},
		IPs: []*types100.IPConfig{
			{Address: mustIPNet("10.0.0.2/24"), Interface: ifaceIdx(0)},
		},
	}

	// Same interface name + sandbox → should be deduplicated
	next := &types100.Result{
		CNIVersion: "1.0.0",
		Interfaces: []*types100.Interface{
			{Name: "eth0", Mac: "aa:bb:cc:dd:ee:00", Sandbox: "/proc/1/ns/net"},
		},
		IPs: []*types100.IPConfig{
			{Address: mustIPNet("10.0.0.3/24"), Interface: ifaceIdx(0)},
		},
	}

	merged, err := Merge(prev, next)
	if err != nil {
		t.Fatalf("Merge error: %v", err)
	}

	result100, _ := types100.NewResultFromResult(merged)

	// Interface should be deduplicated
	if len(result100.Interfaces) != 1 {
		t.Fatalf("expected 1 interface (deduplicated), got %d", len(result100.Interfaces))
	}

	// But IPs should both be present
	if len(result100.IPs) != 2 {
		t.Fatalf("expected 2 IPs, got %d", len(result100.IPs))
	}
	// Both should point to interface 0
	for i, ip := range result100.IPs {
		if ip.Interface == nil || *ip.Interface != 0 {
			t.Errorf("IP %d should reference interface 0", i)
		}
	}
}

func TestMergeDNS(t *testing.T) {
	t.Parallel()

	t.Run("next domain overrides", func(t *testing.T) {
		t.Parallel()
		base := cnitypes.DNS{Domain: "base.local"}
		next := cnitypes.DNS{Domain: "next.local"}
		got := mergeDNS(base, next)
		if got.Domain != "next.local" {
			t.Errorf("domain = %q, want next.local", got.Domain)
		}
	})

	t.Run("empty next preserves base", func(t *testing.T) {
		t.Parallel()
		base := cnitypes.DNS{Domain: "base.local", Nameservers: []string{"1.1.1.1"}}
		next := cnitypes.DNS{}
		got := mergeDNS(base, next)
		if got.Domain != "base.local" {
			t.Errorf("domain = %q, want base.local", got.Domain)
		}
		if len(got.Nameservers) != 1 || got.Nameservers[0] != "1.1.1.1" {
			t.Errorf("nameservers = %v, want [1.1.1.1]", got.Nameservers)
		}
	})

	t.Run("deduplicates nameservers", func(t *testing.T) {
		t.Parallel()
		base := cnitypes.DNS{Nameservers: []string{"8.8.8.8", "1.1.1.1"}}
		next := cnitypes.DNS{Nameservers: []string{"8.8.8.8", "8.8.4.4"}}
		got := mergeDNS(base, next)
		if len(got.Nameservers) != 3 {
			t.Errorf("expected 3 unique nameservers, got %v", got.Nameservers)
		}
	})
}

func TestMergeStrings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		base  []string
		extra []string
		want  int
	}{
		{"both empty", nil, nil, 0},
		{"base only", []string{"a", "b"}, nil, 2},
		{"extra only", nil, []string{"a", "b"}, 2},
		{"deduplicated", []string{"a", "b"}, []string{"b", "c"}, 3},
		{"all same", []string{"a"}, []string{"a"}, 1},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := mergeStrings(tt.base, tt.extra)
			if len(got) != tt.want {
				t.Errorf("len = %d, want %d (got: %v)", len(got), tt.want, got)
			}
		})
	}
}

func TestInterfaceKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		intf *types100.Interface
		want string
	}{
		{"nil", nil, ""},
		{"empty", &types100.Interface{}, ""},
		{"name only", &types100.Interface{Name: "eth0"}, "eth0|"},
		{"sandbox only", &types100.Interface{Sandbox: "/ns"}, "|/ns"},
		{"both", &types100.Interface{Name: "eth0", Sandbox: "/ns"}, "eth0|/ns"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := interfaceKey(tt.intf); got != tt.want {
				t.Errorf("interfaceKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- helpers ---

func mustIPNet(s string) net.IPNet {
	ip, ipnet, err := net.ParseCIDR(s)
	if err != nil {
		panic("bad CIDR: " + s)
	}
	ipnet.IP = ip
	return *ipnet
}
