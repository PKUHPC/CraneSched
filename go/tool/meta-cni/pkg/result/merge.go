package result

import (
	"fmt"

	cnitypes "github.com/containernetworking/cni/pkg/types"
	types100 "github.com/containernetworking/cni/pkg/types/100"
)

// Merge combines two CNI results into a single result.
func Merge(prev, next cnitypes.Result) (cnitypes.Result, error) {
	if prev == nil {
		return next, nil
	}
	if next == nil {
		return prev, nil
	}

	prevResult, err := types100.NewResultFromResult(prev)
	if err != nil {
		return nil, fmt.Errorf("convert previous result: %w", err)
	}
	nextResult, err := types100.NewResultFromResult(next)
	if err != nil {
		return nil, fmt.Errorf("convert next result: %w", err)
	}

	merged := &types100.Result{
		CNIVersion: prevResult.CNIVersion,
		DNS:        mergeDNS(prevResult.DNS, nextResult.DNS),
	}
	if merged.CNIVersion == "" {
		merged.CNIVersion = nextResult.CNIVersion
	}

	var ifaceIndex map[string]int
	merged.Interfaces, ifaceIndex = copyInterfaces(prevResult.Interfaces)
	indexMap := mergeInterfaces(&merged.Interfaces, ifaceIndex, nextResult.Interfaces)

	merged.IPs = append(merged.IPs, copyIPs(prevResult.IPs)...)
	merged.IPs = append(merged.IPs, copyIPsWithIndex(nextResult.IPs, indexMap)...)

	merged.Routes = append(merged.Routes, copyRoutes(prevResult.Routes)...)
	merged.Routes = append(merged.Routes, copyRoutes(nextResult.Routes)...)

	return merged, nil
}

func copyInterfaces(src []*types100.Interface) ([]*types100.Interface, map[string]int) {
	out := make([]*types100.Interface, 0, len(src))
	indexByKey := make(map[string]int, len(src))
	for _, intf := range src {
		if intf == nil {
			out = append(out, nil)
			continue
		}
		copy := intf.Copy()
		out = append(out, copy)
		if key := interfaceKey(copy); key != "" {
			if _, exists := indexByKey[key]; !exists {
				indexByKey[key] = len(out) - 1
			}
		}
	}
	return out, indexByKey
}

// mergeInterfaces appends missing interfaces and returns a mapping from the next result's
// interface indices to indices in the merged result.
func mergeInterfaces(dest *[]*types100.Interface, indexByKey map[string]int, next []*types100.Interface) map[int]int {
	indexMap := make(map[int]int, len(next))
	for i, intf := range next {
		if intf == nil {
			*dest = append(*dest, nil)
			indexMap[i] = len(*dest) - 1
			continue
		}

		key := interfaceKey(intf)
		if key != "" {
			if existing, ok := indexByKey[key]; ok {
				indexMap[i] = existing
				continue
			}
		}

		*dest = append(*dest, intf.Copy())
		newIndex := len(*dest) - 1
		if key != "" {
			indexByKey[key] = newIndex
		}
		indexMap[i] = newIndex
	}
	return indexMap
}

func interfaceKey(intf *types100.Interface) string {
	if intf == nil {
		return ""
	}
	if intf.Name == "" && intf.Sandbox == "" {
		return ""
	}
	return intf.Name + "|" + intf.Sandbox
}

func copyIPs(src []*types100.IPConfig) []*types100.IPConfig {
	out := make([]*types100.IPConfig, 0, len(src))
	for _, ip := range src {
		if ip == nil {
			out = append(out, nil)
			continue
		}
		out = append(out, ip.Copy())
	}
	return out
}

func copyIPsWithIndex(src []*types100.IPConfig, indexMap map[int]int) []*types100.IPConfig {
	out := make([]*types100.IPConfig, 0, len(src))
	for _, ip := range src {
		if ip == nil {
			out = append(out, nil)
			continue
		}
		copy := ip.Copy()
		if copy.Interface != nil {
			if newIndex, ok := indexMap[*copy.Interface]; ok {
				*copy.Interface = newIndex
			}
		}
		out = append(out, copy)
	}
	return out
}

func copyRoutes(src []*cnitypes.Route) []*cnitypes.Route {
	out := make([]*cnitypes.Route, 0, len(src))
	for _, route := range src {
		if route == nil {
			out = append(out, nil)
			continue
		}
		out = append(out, route.Copy())
	}
	return out
}

func mergeDNS(base, next cnitypes.DNS) cnitypes.DNS {
	merged := cnitypes.DNS{
		Domain: base.Domain,
	}
	if next.Domain != "" {
		merged.Domain = next.Domain
	}
	merged.Nameservers = mergeStrings(base.Nameservers, next.Nameservers)
	merged.Search = mergeStrings(base.Search, next.Search)
	merged.Options = mergeStrings(base.Options, next.Options)
	return merged
}

func mergeStrings(base, extra []string) []string {
	if len(base) == 0 {
		return append([]string{}, extra...)
	}

	seen := make(map[string]struct{}, len(base)+len(extra))
	out := make([]string, 0, len(base)+len(extra))
	for _, item := range base {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	for _, item := range extra {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}
