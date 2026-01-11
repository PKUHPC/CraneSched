# CraneSched PKI + TLS Security Configuration

Enabling PKI/TLS provides identity verification and encryption for communication between CraneSched components, reducing the risk of man-in-the-middle attacks and rogue nodes.

CraneSched uses HashiCorp Vault to issue and manage certificates, simplifying certificate management while securing access inside and outside the cluster.

## Environment Preparation

All steps below are performed on the control node (CraneCtld). If you deploy Vault on a separate node, update `api_addr` accordingly.

### Install Vault

Refer to the [Vault official installation guide](https://www.vaultproject.io/docs/install) and choose the method for your distribution.

A container-based deployment is recommended.

## Vault Configuration

### Vault Configuration File

Create the configuration file `/etc/vault.d/vault.hcl`:

```hcl
ui            = true
cluster_addr  = "https://127.0.0.1:8201"
api_addr      = "https://127.0.0.1:8200" # If deploying on a separate node, change api_addr to node IP:8200
disable_mlock = true

storage "raft" {
  path    = "/etc/vault/data" # Custom path
  node_id = "node1"
}

# Warning: TLS must be enabled in production. Disabling TLS is only allowed for local testing on a trusted network. See the Vault documentation for details.
listener "tcp" {
  address     = "127.0.0.1:8200"
  tls_disable = true
  # Configure in real environments
  # tls_cert_file = "/path/to/full-chain.pem"
  # tls_key_file  = "/path/to/private-key.pem"
}
```

### Create Data Directory

```bash
mkdir -p /etc/vault/data
```

### Configure Systemd Service (Recommended)

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

After creating the Systemd service, load and start it:

```bash
# Start the service
sudo systemctl daemon-reload
sudo systemctl start vault

# Enable on boot
sudo systemctl enable vault
```

## CraneSched PKI Integration

CraneSched provides the `scripts/vault.sh` script to help administrators initialize Vault and integrate it with the CraneSched certificate system.

**vault.sh provides two initialization methods:**

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

**vault.sh command reference:**

- **Initialize all (recommended for first-time deployment or after a reset)**

  ```bash
  bash vault.sh init [domainSuffix]
  ```

  Function: initialize Vault, create an administrator user, initialize the PKI certificate system, and issue internal/external certificates.

  The default domain suffix is `crane.local`. You can customize it, for example: `bash vault.sh init crane.com`.

- **Initialize Vault (first-time or after reset)**

  ```bash
  bash vault.sh init_vault
  ```

  Function: initialize Vault, generate unseal keys and root token, and create an administrator account.

- **Unseal Vault (required after a service restart)**

  ```bash
  bash vault.sh unseal_vault
  ```

  Function: unseal Vault automatically using the saved unseal keys.

- **Initialize the PKI certificate system**

  ```bash
  bash vault.sh init_cert [domainSuffix]
  ```

  Function: initialize the PKI certificate system and issue internal/external certificates.

  The default domain suffix is `crane.local`. You can customize it.

- **Issue internal certificates**

  ```bash
  bash vault.sh issue_internal [domainSuffix]
  ```

  Function: issue TLS certificates for internal communication (`internal.pem`, `internal.key`).

- **Issue external certificates**

  ```bash
  bash vault.sh issue_external [domainSuffix]
  ```

  Function: issue TLS certificates for external communication (`external.pem`, `external.key`).

- **Log in to Vault as administrator**

  ```bash
  bash vault.sh login
  ```

  Function: log in to Vault as the `admin` user for subsequent CLI operations.

- **Clean Vault data and reset**

  ```bash
  bash vault.sh clean_vault
  ```

  Function: clear the Vault data directory and restart Vault. **Use with caution, only for a complete reset of Vault.**

  **Before running this command, execute `cacctmgr reset all` to reset all account certificates.**

**Notes:**

1. Vault only needs to be initialized once. Do not repeatedly execute `init` or `init_vault`. If you must reinitialize, run `cacctmgr reset all` and `clean_vault` first.
2. `/etc/vault.d/vault_keys.txt` and `/etc/vault.d/vault_token.txt` are sensitive files. Do not disclose, delete, or move them.
3. Certificate files are generated in `/etc/crane/tls/` by default. Reference them correctly in the CraneSched configuration files.
4. After each Vault service restart, run `unseal_vault` to unseal.
5. If you change the domain suffix, update the script parameters and CraneSched configuration files accordingly.

## CraneSched Configuration

### Deploy Certificates

After initialization, five certificate files are generated in `/etc/crane/tls/`: `ca.pem`, `external.key`, `external.pem`, `internal.key`, and `internal.pem`. Deploy them as follows:

1. Control node (`CraneCtld`): `ca.pem`, `external.key`, `external.pem`, `internal.key`, `internal.pem`
2. Compute node (`Craned`): `ca.pem`, `internal.key`, `internal.pem`
3. User login node: `ca.pem`, `external.pem`
4. Frontend node (`Cfored`): `ca.pem`, `internal.key`, `internal.pem`

After deployment, ensure permissions are correct: `external.key` and `internal.key` should be 600, while `ca.pem`, `external.pem`, and `internal.pem` should be 644.

### System Configuration File

Update the CraneSched system configuration file `/etc/crane/crane.yaml` as follows:

```yaml
TLS:
  Enabled: true
  InternalCertFilePath: /etc/crane/tls/internal.pem
  InternalKeyFilePath: /etc/crane/tls/internal.key
  InternalCaFilePath: /etc/crane/tls/ca.pem
  ExternalCertFilePath: /etc/crane/tls/external.pem
  ExternalKeyFilePath: /etc/crane/tls/external.key
  ExternalCaFilePath: /etc/crane/tls/ca.pem
  # AllowedNodes: "cn[15-18]" # Allowed frontend nodes for signing
  DomainSuffix: crane.local # Domain suffix
```

### Database Configuration File

Update the CraneSched database configuration file `/etc/crane/database.yaml` as follows:

```yaml
Vault:
  Enabled: true
  Addr: 127.0.0.1 # api_addr
  Port: 8200
  Username: admin
  Password: "123456"
  Tls: false # Set to true when Vault uses TLS; recommended for production environments
```
