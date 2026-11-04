# dns-register CNI plugin

A chained CNI plugin that registers pod DNS records in etcd for CoreDNS.

## What it does

- On **ADD**: reads the pod FQDN from annotation `cranesched.internal/fqdn`,
  extracts the IPv4 from the previous result, and writes a DNS record to etcd
  under the CoreDNS etcd plugin key format (`/coredns/<reversed-domain>`).
- On **DEL**: removes the DNS record if the owner matches the container ID.
- Always returns the unmodified `PrevResult` to the next plugin in the chain.

## Build

```sh
make tool
sudo install -m 0755 build/tool/dns-register /opt/cni/bin/dns-register
```

## Configuration

All field names use underscore style, matching Calico convention.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `etcd_endpoints` | string or []string | yes | — | etcd endpoint(s). Accepts `"http://..."` or `["http://..."]`. |
| `etcd_ca_cert_file` | string | no | — | Path to etcd CA certificate. |
| `etcd_cert_file` | string | no | — | Path to etcd client certificate. |
| `etcd_key_file` | string | no | — | Path to etcd client key. |
| `ttl` | int | yes | — | DNS record TTL in seconds (1–86400). |
| `lease_ttl` | int | no | 0 | etcd lease TTL in seconds. 0 disables lease. |
| `required` | bool | no | true | If false, etcd errors are logged but do not fail the CNI operation. |

The plugin also requires the `io.kubernetes.cri.pod-annotations` capability to
receive the FQDN annotation from the container runtime.

## Example (as a meta-cni delegate)

```json
{
  "name": "dns-register",
  "conf": {
    "type": "dns-register",
    "etcd_endpoints": ["http://192.168.24.2:2379"],
    "ttl": 60,
    "lease_ttl": 0,
    "capabilities": {
      "io.kubernetes.cri.pod-annotations": true
    }
  }
}
```

For a complete chained configuration, see
`tool/meta-cni/config/00-crane-calico.conf`.
