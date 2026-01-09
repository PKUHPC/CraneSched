# Crane PKI+TLS 安全通信部署手册

本文档指导管理员在 `Crane` 集群中，基于 `HashiCorp Vault` 实现 `PKI` 证书自动化管理，实现集群内外安全通信与用户无感知证书分发。

## 环境准备
下述环境皆在 `CraneCtld` 节点上安装，如若单独节点部署，注意修改 `api_addr` 配置项。

### 安装依赖

```bash
dnf install libcurl-devel jq
```

### 安装 Vault
```bash
dnf install vault
```
如果仓库中没有该软件包，请参考 [Vault 安装](https://www.vaultproject.io/docs/install)下载并安装vault二进制包

## Vault 配置与部署

### 配置文件 `/etc/vault.d/vault.hcl`
```hcl
ui            = true
cluster_addr  = "https://127.0.0.1:8201"
api_addr      = "https://127.0.0.1:8200" # 若单独节点部署，更改api_addr为节点ip:8200
disable_mlock = true

storage "raft" {
  path    = "/etc/vault/data" # 自定义路径
  node_id = "node1"
}

# 警告：必须在生产环境中启用 TLS，仅在本地测试且确保网络安全时才可临时关闭；详情请查阅Vault官方文档
listener "tcp" {  
  address     = "127.0.0.1:8200"
  tls_disable = "true"
  # 真实环境中需配置
  # tls_cert_file = "/path/to/full-chain.pem"
  # tls_key_file  = "/path/to/private-key.pem"
}
```
### 创建数据目录
```bash
mkdir -p /etc/vault/data
```

### 配置systemd服务（推荐）
创建 `/etc/systemd/system/vault.service`:
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

### 加载并启动服务
```bash
sudo systemctl daemon-reload
sudo systemctl start vault
sudo systemctl enable vault
```

## Vault 初始化与证书系统初始化
提供`vault.sh`脚本，用于完成初始化与证书系统初始化

**_vault.sh提供两种初始化方式：_**

1. 一键初始化（**建议使用**）
```bash
bash vault.sh init [domainSuffix]
```
2. 分步初始化
```bash
mkdir -p /etc/crane/tls 

bash vault.sh init_vault

bash vault.sh init_cert [domainSuffix]
```

**vault.sh 脚本所有功能介绍如下：**

- **初始化全部（推荐首次部署或重置后使用）**
    - `bash vault.sh init [domainSuffix]`
        - 功能：初始化 Vault、创建管理员用户、初始化 PKI 证书系统、签发内部/外部证书。
        - 默认域名后缀为 `crane.local`，可自定义，例如：
            - `bash vault.sh init crane.com`

- **初始化 Vault（首次或重置后）**
    - `bash vault.sh init_vault`
        - 功能：初始化 Vault，生成解封密钥和 root token，创建管理员账号。

- **解封 Vault（服务重启后需执行）**
    - `bash vault.sh unseal_vault`
        - 功能：自动使用保存的解封密钥解锁 Vault。

- **初始化 PKI 证书系统**
    - `bash vault.sh init_cert [domainSuffix]`
        - 功能：初始化 PKI 证书系统，并签发内部/外部证书。
        - 默认域名后缀为 `crane.local`，可自定义。

- **签发内部证书**
    - `bash vault.sh issue_internal [domainSuffix]`
        - 功能：签发内部通信用 TLS 证书（`internal.pem`、`internal.key`）。

- **签发外部证书**
    - `bash vault.sh issue_external [domainSuffix]`
        - 功能：签发外部通信用 TLS 证书（`external.pem`、`external.key`）。

- **管理员登陆 Vault**
    - `bash vault.sh login`
        - 功能：以 admin 用户登录 Vault，便于后续 CLI 操作。

- **清理 Vault 数据并重置**
    - **_执行命令前请执行 `cacctmgr reset all` 重置所有账户的证书_**
    - `bash vault.sh clean_vault`
        - 功能：清空 Vault 数据目录并重启 Vault。**_慎用，仅用于彻底重置 Vault_**。

### 注意事项
1. `Vault` 只需初始化一次，勿重复执行 `init`、`init_vault`，否则需先 `cacctmgr reset all`、`clean_vault` 重置后才能重新初始化。
2. `/etc/vault.d/vault_keys.txt` 和 `/etc/vault.d/vault_token.txt` 为敏感文件，请勿泄露、删除或移动。
3. 证书文件默认生成在 `/etc/crane/tls/`，请根据 Crane 配置文件正确引用。
4. 每次 `Vault` 服务重启后需运行 `unseal_vault` 解封。
5. 若需更换域名后缀，请相应调整脚本参数和 `Crane` 配置文件。

## Crane 配置

### 证书部署
初始化后，`/etc/crane/tls/` 目录下生成`ca.pem`、`external.key`、`external.pem`、`internal.key`、`internal.pem` 5项证书文件，证书部署情况如下：

1. `CraneCtld` 所在节点：`ca.pem`、`external.key`、`external.pem`、`internal.key`、`internal.pem`
2. `Craned`所在节点：`ca.pem`、`internal.key`、`internal.pem`
3. 用户登陆节点：`ca.pem`、`external.pem`
4. `Cfored` 所在节点：`ca.pem`、`internal.key`、`internal.pem`

部署后请确认文件权限正确：`external.key`与`internal.key`权限为600，`ca.pem`、`external.pem`、`internal.pem`权限为644

### 配置文件 `/etc/crane/crane.yaml`
```yaml
TLS:
  Enabled: true
  InternalCertFilePath: /etc/crane/tls/internal.pem
  InternalKeyFilePath: /etc/crane/tls/internal.key
  InternalCaFilePath: /etc/crane/tls/ca.pem
  ExternalCertFilePath: /etc/crane/tls/external.pem
  ExternalKeyFilePath: /etc/crane/tls/external.key
  ExternalCaFilePath: /etc/crane/tls/ca.pem
  # AllowedNodes: "cn[15-18]" # 允许签发的前端节点
  DomainSuffix: crane.local # 域名后缀
```

### 数据库配置文件 `/etc/crane/database.yaml`
```yaml
Vault:
  Enabled: true
  Addr: 127.0.0.1 # api_addr
  Port: 8200
  Username: admin
  Password: "123456"
  Tls: false # 当Vault启用TLS时设置为true，建议生产环境中启用
```