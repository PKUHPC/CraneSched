// dns-register.go
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/containernetworking/cni/pkg/skel"
	"github.com/containernetworking/cni/pkg/types"
	current "github.com/containernetworking/cni/pkg/types/100"
	"github.com/containernetworking/cni/pkg/version"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	aboutPlugin = `DNS Register CNI Plugin for CraneSched.
This plugin registers the pod's IP address in etcd for CoreDNS consumption.`

	// kAnnotationFQDN is the pod annotation key set by the CraneSched backend.
	// It carries the authoritative FQDN for the pod's DNS record.
	kAnnotationFQDN = "cranesched.internal/fqdn"

	// kEtcdOpTimeout is the timeout for individual etcd operations.
	kEtcdOpTimeout = 3 * time.Second

	kMinTTL      = 1
	kMaxTTL      = 86400
	kMinLeaseTTL = 5
	kMaxLeaseTTL = 86400
)

type DNSRegisterConfig struct {
	types.NetConf

	// EtcdEndpoints is the list of etcd endpoints.
	// Accepts both a single string ("http://...") and an array (["http://..."]).
	EtcdEndpoints stringOrSlice `json:"etcd_endpoints"`

	// Etcd TLS fields (optional, flat layout matching Calico convention).
	EtcdCACertFile string `json:"etcd_ca_cert_file,omitempty"`
	EtcdCertFile   string `json:"etcd_cert_file,omitempty"`
	EtcdKeyFile    string `json:"etcd_key_file,omitempty"`

	// TTL is the DNS record TTL in seconds.
	TTL int `json:"ttl"`

	// LeaseTTL is the etcd lease TTL in seconds.
	// 0 (default) means no lease; record never expires automatically.
	LeaseTTL int `json:"lease_ttl,omitempty"`

	// Required controls failure mode.
	// - true (default): any registration error fails CNI ADD.
	// - false: log the error and continue (prints PrevResult).
	Required *bool `json:"required,omitempty"`

	// RuntimeConfig is populated by the container runtime (e.g., containerd)
	// when the corresponding capabilities are declared in the CNI config.
	RuntimeConfig struct {
		PodAnnotations map[string]string `json:"io.kubernetes.cri.pod-annotations,omitempty"`
	} `json:"runtimeConfig"`
}

// stringOrSlice is a []string that also accepts a single JSON string.
// This matches Calico's etcd_endpoints convention.
type stringOrSlice []string

func (s *stringOrSlice) UnmarshalJSON(data []byte) error {
	// Try []string first.
	var list []string
	if err := json.Unmarshal(data, &list); err == nil {
		*s = list
		return nil
	}

	// Fall back to a single string (optionally comma-separated).
	var single string
	if err := json.Unmarshal(data, &single); err != nil {
		return fmt.Errorf("etcd_endpoints: must be a string or string array")
	}
	var result []string
	for ep := range strings.SplitSeq(single, ",") {
		ep = strings.TrimSpace(ep)
		if ep != "" {
			result = append(result, ep)
		}
	}
	*s = result
	return nil
}

// isRequired returns whether DNS registration failure should be fatal.
// Defaults to true when not explicitly set.
func (c *DNSRegisterConfig) isRequired() bool {
	if c.Required == nil {
		return true
	}
	return *c.Required
}

func (c *DNSRegisterConfig) validate() error {
	if len(c.EtcdEndpoints) == 0 {
		return fmt.Errorf("etcd_endpoints must not be empty")
	}
	if c.TTL < kMinTTL || c.TTL > kMaxTTL {
		return fmt.Errorf("ttl must be between %d and %d, got %d", kMinTTL, kMaxTTL, c.TTL)
	}

	// LeaseTTL <= 0 means no lease, so skip further checks.
	if c.LeaseTTL > 0 && (c.LeaseTTL < kMinLeaseTTL || c.LeaseTTL > kMaxLeaseTTL) {
		return fmt.Errorf("lease_ttl must be between %d and %d when set, got %d", kMinLeaseTTL, kMaxLeaseTTL, c.LeaseTTL)
	}
	if c.LeaseTTL > 0 && c.LeaseTTL < c.TTL {
		return fmt.Errorf("lease_ttl (%d) should not be less than ttl (%d)", c.LeaseTTL, c.TTL)
	}
	return nil
}

// used in etcd
type DNSRecord struct {
	// ipv4
	Host string `json:"host"`
	// seconds 30–120
	TTL int `json:"ttl"`
	// ID taskID/podUID
	Owner string `json:"owner,omitempty"`
	// lastUpdate
	TS int64 `json:"ts,omitempty"`
}

type leaseGranter interface {
	Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
}

func cmdAdd(args *skel.CmdArgs) error {
	config := DNSRegisterConfig{}
	if err := json.Unmarshal(args.StdinData, &config); err != nil {
		return fmt.Errorf("failed to parse config: %v", err)
	}

	if err := config.validate(); err != nil {
		return fmt.Errorf("invalid config: %v", err)
	}

	if err := version.ParsePrevResult(&config.NetConf); err != nil {
		return err
	}

	currentResult, err := current.NewResultFromResult(config.PrevResult)
	if err != nil {
		return fmt.Errorf("failed to convert prevResult to current.Result: %v", err)
	}

	var podIP string
	for _, ip := range currentResult.IPs {
		if ip.Address.IP.To4() != nil {
			podIP = ip.Address.IP.String()
			break
		}
	}
	if podIP == "" {
		return fmt.Errorf("no IP address found")
	}

	fqdn, err := getFQDNFromAnnotation(config.RuntimeConfig.PodAnnotations)
	if err != nil {
		return err
	}

	etcdKey, err := fqdnToEtcdKey(fqdn)
	if err != nil {
		return err
	}
	uid := args.ContainerID

	cli, err := newEtcdClient(&config)
	if err != nil {
		if !config.isRequired() {
			log.Printf("dns-register: connect etcd failed (ignored): %v", err)
			return types.PrintResult(config.PrevResult, config.CNIVersion)
		}
		return fmt.Errorf("failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), kEtcdOpTimeout)
	defer cancel()

	record := DNSRecord{
		Host:  podIP,
		TTL:   config.TTL,
		Owner: uid,
		TS:    time.Now().Unix(),
	}

	recordJSON, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %v", err)
	}

	putOpts, err := buildPutOptions(ctx, cli, config.LeaseTTL)
	if err != nil {
		return fmt.Errorf("failed to prepare etcd put options: %v", err)
	}

	_, err = cli.Put(ctx, etcdKey, string(recordJSON), putOpts...)
	if err != nil {
		if !config.isRequired() {
			log.Printf("dns-register: put etcd key %q failed (ignored): %v", etcdKey, err)
			return types.PrintResult(config.PrevResult, config.CNIVersion)
		}
		return fmt.Errorf("failed to write to etcd: %v", err)
	}

	return types.PrintResult(config.PrevResult, config.CNIVersion)
}

func cmdDel(args *skel.CmdArgs) error {
	config := DNSRegisterConfig{}
	if err := json.Unmarshal(args.StdinData, &config); err != nil {
		return err
	}

	if err := config.validate(); err != nil {
		return fmt.Errorf("invalid config: %v", err)
	}

	if args.ContainerID == "" {
		return fmt.Errorf("container ID is required for DEL")
	}

	fqdn, err := getFQDNFromAnnotation(config.RuntimeConfig.PodAnnotations)
	if err != nil {
		// Idempotent: if we can't get a valid FQDN, there's nothing to delete.
		// Log the error and return success.
		log.Println(err)
		return nil
	}

	etcdKey, err := fqdnToEtcdKey(fqdn)
	if err != nil {
		log.Println(err)
		return nil
	}

	cli, err := newEtcdClient(&config)
	if err != nil {
		if !config.isRequired() {
			log.Printf("dns-register: connect etcd failed during DEL (ignored): %v", err)
			return nil
		}
		return err
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), kEtcdOpTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, etcdKey)
	if err != nil {
		if !config.isRequired() {
			log.Printf("dns-register: get etcd key %q failed during DEL (ignored): %v", etcdKey, err)
			return nil
		}
		return err
	}

	// Idempotent: if the key doesn't exist, nothing to delete.
	if len(resp.Kvs) == 0 {
		return nil
	}

	kv := resp.Kvs[0]

	shouldDelete, err := shouldDeleteRecordForContainer(kv.Value, args.ContainerID)
	if err != nil {
		return fmt.Errorf("failed to decode dns record for key %q: %v", etcdKey, err)
	}
	if !shouldDelete {
		return nil
	}

	// Revoke the lease first so the key is auto-deleted even if
	// the explicit Delete below fails for some reason.
	if kv.Lease > 0 {
		_, _ = cli.Revoke(ctx, clientv3.LeaseID(kv.Lease))
	}

	_, err = cli.Delete(ctx, etcdKey)
	if err != nil && !config.isRequired() {
		log.Printf("dns-register: delete etcd key %q failed during DEL (ignored): %v", etcdKey, err)
		return nil
	}
	return err
}

// TODO: Implement CHECK — verify that the DNS record still matches PrevResult.
func cmdCheck(args *skel.CmdArgs) error {
	log.Println("dns-register: CHECK called (not yet implemented)")
	return nil
}

// TODO: Implement GC — clean up orphaned DNS records based on local state.
func cmdGC(args *skel.CmdArgs) error {
	log.Println("dns-register: GC called (not yet implemented)")
	return nil
}

// TODO: Implement STATUS — report plugin health and etcd connectivity.
func cmdStatus(args *skel.CmdArgs) error {
	log.Println("dns-register: STATUS called (not yet implemented)")
	return nil
}

func main() {
	skel.PluginMainFuncs(skel.CNIFuncs{
		Add:    cmdAdd,
		Del:    cmdDel,
		Check:  cmdCheck,
		GC:     cmdGC,
		Status: cmdStatus,
	}, version.All, aboutPlugin)
}

// getFQDNFromAnnotation extracts and validates the FQDN from pod annotations.
func getFQDNFromAnnotation(annotations map[string]string) (string, error) {
	fqdn, ok := annotations[kAnnotationFQDN]
	if !ok || fqdn == "" {
		return "", fmt.Errorf("pod annotation %q is required but missing", kAnnotationFQDN)
	}

	normalized, err := normalizeAndValidateFQDN(fqdn)
	if err != nil {
		return "", fmt.Errorf("invalid FQDN %q: %w", fqdn, err)
	}

	return normalized, nil
}

// validateFQDN checks that fqdn is a syntactically valid domain name.
func normalizeAndValidateFQDN(fqdn string) (string, error) {
	name := strings.TrimSpace(fqdn)
	name = strings.TrimSuffix(name, ".")
	name = strings.ToLower(name)
	if name == "" {
		return "", fmt.Errorf("empty name")
	}
	if len(name) > 253 {
		return "", fmt.Errorf("exceeds 253 characters")
	}

	labels := strings.Split(name, ".")
	if len(labels) < 2 {
		return "", fmt.Errorf("must contain at least 2 labels")
	}

	for _, label := range labels {
		if label == "" {
			return "", fmt.Errorf("contains empty label")
		}
		if len(label) > 63 {
			return "", fmt.Errorf("label %q exceeds 63 characters", label)
		}

		// RFC1123-ish: [a-z0-9]([-a-z0-9]*[a-z0-9])?
		first := label[0]
		last := label[len(label)-1]
		if !isLowerAlphaNum(first) || !isLowerAlphaNum(last) {
			return "", fmt.Errorf("label %q must start/end with alphanumeric", label)
		}
		for i := 0; i < len(label); i++ {
			ch := label[i]
			if ch > 0x7f {
				return "", fmt.Errorf("non-ascii character in label %q", label)
			}
			if isLowerAlphaNum(ch) || ch == '-' {
				continue
			}
			return "", fmt.Errorf("invalid character %q in label %q", ch, label)
		}
	}
	return name, nil
}

func isLowerAlphaNum(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9')
}

// fqdnToEtcdKey converts a FQDN to a CoreDNS etcd plugin key.
// CoreDNS etcd plugin stores records under a reversed domain path,
// e.g. "host.ns.cluster.local" → "/coredns/local/cluster/ns/host".
func fqdnToEtcdKey(fqdn string) (string, error) {
	name := strings.TrimSuffix(fqdn, ".")
	if name == "" {
		return "", fmt.Errorf("empty fqdn")
	}
	parts := strings.Split(name, ".")
	for _, p := range parts {
		if p == "" {
			return "", fmt.Errorf("invalid fqdn: empty label")
		}
		if strings.Contains(p, "/") {
			return "", fmt.Errorf("invalid fqdn label %q: contains '/'", p)
		}
	}

	reversed := make([]string, len(parts))
	for i, p := range parts {
		reversed[len(parts)-1-i] = p
	}

	return "/coredns/" + strings.Join(reversed, "/"), nil
}

func newEtcdClient(cfg *DNSRegisterConfig) (*clientv3.Client, error) {
	clientTLS, err := buildEtcdTLSConfig(cfg)
	if err != nil {
		return nil, err
	}

	return clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
		TLS:         clientTLS,
	})
}

func buildEtcdTLSConfig(cfg *DNSRegisterConfig) (*tls.Config, error) {
	hasTLSFiles := cfg.EtcdCACertFile != "" || cfg.EtcdCertFile != "" || cfg.EtcdKeyFile != ""

	needTLS := hasTLSFiles
	if !needTLS {
		for _, ep := range cfg.EtcdEndpoints {
			if strings.HasPrefix(strings.ToLower(strings.TrimSpace(ep)), "https://") {
				needTLS = true
				break
			}
		}
	}
	if !needTLS {
		return nil, nil
	}

	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		rootCAs = x509.NewCertPool()
	}

	if cfg.EtcdCACertFile != "" {
		pem, readErr := os.ReadFile(cfg.EtcdCACertFile)
		if readErr != nil {
			return nil, fmt.Errorf("read etcd_ca_cert_file: %w", readErr)
		}
		if ok := rootCAs.AppendCertsFromPEM(pem); !ok {
			return nil, fmt.Errorf("failed to parse etcd_ca_cert_file PEM")
		}
	}

	var certs []tls.Certificate
	if cfg.EtcdCertFile != "" || cfg.EtcdKeyFile != "" {
		if cfg.EtcdCertFile == "" || cfg.EtcdKeyFile == "" {
			return nil, fmt.Errorf("etcd_cert_file and etcd_key_file must both be set")
		}
		pair, loadErr := tls.LoadX509KeyPair(cfg.EtcdCertFile, cfg.EtcdKeyFile)
		if loadErr != nil {
			return nil, fmt.Errorf("load etcd client cert/key: %w", loadErr)
		}
		certs = append(certs, pair)
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      rootCAs,
		Certificates: certs,
	}, nil
}

func buildPutOptions(ctx context.Context, granter leaseGranter, leaseTTL int) ([]clientv3.OpOption, error) {
	// leaseTTL == 0 means permanent record (no lease attached).
	// The record will persist until explicitly deleted via cmdDel.
	if leaseTTL <= 0 {
		return nil, nil
	}

	leaseResp, err := granter.Grant(ctx, int64(leaseTTL))
	if err != nil {
		return nil, err
	}
	return []clientv3.OpOption{clientv3.WithLease(leaseResp.ID)}, nil
}

func shouldDeleteRecordForContainer(recordValue []byte, containerID string) (bool, error) {
	if containerID == "" {
		return false, fmt.Errorf("container ID is empty")
	}

	var record DNSRecord
	if err := json.Unmarshal(recordValue, &record); err != nil {
		return false, err
	}

	// Strict check: only the owner who created this record can remove it.
	if record.Owner == "" {
		return false, nil
	}
	return record.Owner == containerID, nil
}
