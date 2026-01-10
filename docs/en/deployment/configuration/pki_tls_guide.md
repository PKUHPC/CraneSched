# Crane PKI+TLS Secure Communication Deployment Guide

This document guides administrators in the Crane cluster to implement automated PKI certificate management based on HashiCorp Vault, 
enabling secure communication both inside and outside the cluster, 
as well as seamless certificate distribution to users without requiring user intervention.

## Environment Preparation

All the following components should be installed on the `CraneCtld` node. 
If you are deploying on a separate node, 
make sure to modify the `api_addr` configuration accordingly.

### Install Dependencies

```bash
dnf install libcurl-devel jq
```

### Install Vault
```bash
dnf install vault
```

If Vault is available and suitable in your system's package repository, you can install it using the above command.
If Vault is not available in your repository or you require a specific version, please refer to [Vault Installation](https://www.vaultproject.io/docs/install)
to download and install the official Vault binary package.

## Vault Configuration and Deployment

### Configuration File `/etc/vault.d/vault.hcl`
```hcl
ui            = true
cluster_addr  = "https://127.0.0.1:8201"
api_addr      = "https://127.0.0.1:8200" # If deploying on a separate node, change api_addr to node_ip:8200
disable_mlock = true

storage "raft" {
  path    = "/etc/vault/data" # Custom path
  node_id = "node1"
}

Warning: TLS must be enabled in production environments. Disabling TLS is only allowed for local testing when the network is secure. For more details, please refer to the official Vault documentation.
listener "tcp" {
  address     = "127.0.0.1:8200"
  tls_disable = "true"
  # Need to configure in real environment
  # tls_cert_file = "/path/to/full-chain.pem"
  # tls_key_file  = "/path/to/private-key.pem"
}
```
### Create Data Directory
```bash
mkdir -p /etc/vault/data
```

### Configure systemd Service (Recommended)
Create `/etc/systemd/system/vault.service`:
```service
[Unit]
Description=HashiCorp Vault
Documentation=https://www.vaultproject.io/docs/
Requires=network-online.target
After=network-online.target

[Service]
User=root
ExecStart=/usr/bin/vault server -config=/etc/vault.d/vault.hcl
ExecReload=/bin/kill -HUP $MAINPID
KillSignal=SIGINT
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

### Load and Start the Service
```bash
sudo systemctl daemon-reload
sudo systemctl start vault
sudo systemctl enable vault
```

## Vault Initialization and Certificate System Initialization
The `script/vault.sh` script is provided to complete the initialization 
and certificate system initialization.

**_vault.sh provides two initialization methods:_**

1. One-click initialization (**recommended**)
```bash
bash vault.sh init [domainSuffix]
```
2. Step-by-step initialization
```bash
mkdir -p /etc/crane/tls 

bash vault.sh init_vault

bash vault.sh init_cert [domainSuffix]
```

The functions of the vault.sh script are described as follows:

- **Initialize All (Recommended for first-time deployment or after reset)**

    `bash vault.sh init [domainSuffix]`

    Function: Initialize `Vault`, create administrator user, initialize `PKI` certificate system, and issue internal/external certificates.

    The default domain suffix is `crane.local`, which can be customized, for example: `bash vault.sh init crane.com`

- **Initialize Vault (First-time or after reset)**

    `bash vault.sh init_vault`

    Function: Initialize `Vault`, generate unseal key and `root token`, create administrator account.

- **Unseal Vault (Required after service restart)**

    `bash vault.sh unseal_vault`

    Function: Automatically unseal `Vault` using the saved unseal key

- **Initialize PKI Certificate System**

    `bash vault.sh init_cert [domainSuffix]`

    Function: Initialize the `PKI` certificate system and issue internal/external certificates.

    The default domain suffix is `crane.local`, which can be customized.

- **Issue Internal Certificates**

    `bash vault.sh issue_internal [domainSuffix]`

    Function: Issue TLS certificates for internal communication (`internal.pem`, `internal.key`).

- **Issue External Certificates**

    `bash vault.sh issue_external [domainSuffix]`

    Function: Issue TLS certificates for external communication (`external.pem`, `external.key`).

- **Administrator Login to Vault**

    `bash vault.sh login`

    Function: Login to `Vault` as `admin` user, facilitating subsequent CLI operations.

- **Clean Vault Data and Reset**

    `bash vault.sh clean_vault`

    Function: Clear the `Vault` data directory and restart `Vault`. **_Use with caution, only for completely resetting Vault_**.

    **_Execute `cacctmgr reset all` to reset all account certificates before running this command_**


### Notes
1. `Vault` only needs to be initialized once. Do not repeatedly execute `init`, `init_vault`, otherwise you must first reset with `cacctmgr reset all` and `clean_vault` before reinitialization.
2. `/etc/vault.d/vault_keys.txt` and `/etc/vault.d/vault_token.txt` are sensitive files, do not disclose, delete, or move them.
3. Certificate files are generated in `/etc/crane/tls/` by default, please reference them correctly according to the Crane configuration file.
4. After each `Vault` service restart, run `unseal_vault` to unseal.
5. If changing the domain suffix, adjust the script parameters and `Crane` configuration file accordingly.

## Crane Configuration

### Certificate Deployment
After initialization, 5 certificate files will be generated in the `/etc/crane/tls/` directory: `ca.pem`, `external.key`, `external.pem`, `internal.key`, and `internal.pem`. The certificate deployment is as follows:

1. `CraneCtld` node: `ca.pem`, `external.key`, `external.pem`, `internal.key`, `internal.pem`
2. `Craned` node: `ca.pem`, `internal.key`, `internal.pem`
3. User login node: `ca.pem`, `external.pem`
4. `Cfored` node: `ca.pem`, `internal.key`, `internal.pem`

After deployment, please ensure the file permissions are correct: 
external.key and internal.key should have permission 600, 
while ca.pem, external.pem, and internal.pem should have permission 644.

### Configuration File `/etc/crane/crane.yaml`
```yaml
TLS:
  Enabled: true
  InternalCertFilePath: /etc/crane/tls/internal.pem
  InternalKeyFilePath: /etc/crane/tls/internal.key
  InternalCaFilePath: /etc/crane/tls/ca.pem
  ExternalCertFilePath: /etc/crane/tls/external.pem
  ExternalKeyFilePath: /etc/crane/tls/external.key
  ExternalCaFilePath: /etc/crane/tls/ca.pem
  # AllowedNodes: "cn[15-18]" # Frontend nodes allowed for signing
  DomainSuffix: crane.local # Domain suffix
```

### Database Configuration File `/etc/crane/database.yaml`
```yaml
Vault:
  Enabled: true
  Addr: 127.0.0.1 # api_addr
  Port: 8200
  Username: admin
  Password: "123456"
  Tls: false # Set to true when Vault is using TLS; it is recommended to enable this in production environments.
```
