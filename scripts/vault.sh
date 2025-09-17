#!/bin/bash

export VAULT_ADDR='http://127.0.0.1:8200'

unseal_vault(){
    VAULT_KEY_FILE="/etc/vault.d/vault_keys.txt"

    if [ ! -f "$VAULT_KEY_FILE" ]; then
        echo "Error: File $VAULT_KEY_FILE not found."
        exit 1
    fi

    keys=$(cat "$VAULT_KEY_FILE" | jq -r '.[0:3][]')
    if [ -z "$keys" ]; then
        echo "Error: Failed to parse JSON array from $VAULT_KEY_FILE."
        exit 1
    fi

    for key in $keys; do
        vault operator unseal $key
    done
}

login_vault() {
    vault login -method=userpass username=admin
}

init_vault() {
    output=$(vault operator init --format=json) # 该命令会得到5个Unseal Key以及1个root token，需要秘密保存后续使用
    if [ $? -ne 0 ]; then
        exit 1
    fi
    # 5个Unseal Key必须保存！！丢失可能导致后续无法使用

    #vault使用前需要unseal解密，解析需要用到上述unseal key中的三个
    key=$(echo "$output" | jq -r '.unseal_keys_b64[0]')
    vault operator unseal $key
    key=$(echo "$output" | jq -r '.unseal_keys_b64[1]')
    vault operator unseal $key
    key=$(echo "$output" | jq -r '.unseal_keys_b64[2]')
    vault operator unseal $key

    token=$(echo "$output" | jq -r '.root_token')
    vault login $token #采用root token登录即可使用CLI命令进行操作
    if [ $? -ne 0 ]; then
        echo "登录失败"
        exit 1
    fi

    mkdir /etc/vault.d
    keys=$(echo "$output" | jq '.unseal_keys_b64')
    echo $keys > /etc/vault.d/vault_keys.txt
    chmod 600 /etc/vault.d/vault_keys.txt

    echo -n $token > /etc/vault.d/vault_token.txt
    chmod 600 /etc/vault.d/vault_token.txt

    # 创建 admin-policy 策略文件
    echo "Creating admin-policy file..."
    cat <<EOF > /etc/vault.d/admin-policy.hcl
path "*" {
  capabilities = ["create", "read", "update", "delete", "list", "sudo"]
}
EOF

    # 写入 admin-policy 策略
    echo "Writing admin-policy to Vault..."
    vault policy write admin-policy /etc/vault.d/admin-policy.hcl || {
      echo "Failed to write admin-policy."
      exit 1
    }

    vault auth enable userpass

    # 创建 admin 用户并分配策略
    echo "Creating admin user with admin-policy..."
    vault write auth/userpass/users/admin password="123456" policies="admin-policy" || {
      echo "Failed to create admin user."
      exit 1
    }

    # 使用 admin 用户登录
    echo "Logging in as admin user..."
    vault login -method=userpass username=admin password=123456 || {
      echo "Failed to log in as admin user."
      exit 1
    }

   vault status
}

init_cert() {
    if [ "$#" -eq 0 ]; then
      domainSuffix="crane.local"
    else
      domainSuffix="$1"
    fi

    vault secrets enable pki

    mkdir -p /etc/crane/tls

    # 生成CA文件
    vault write -field=certificate pki/root/generate/internal \
    common_name="*.$domainSuffix"  issuer_name="CraneSched_root_CA" not_after="9999-12-31T23:59:59Z" > /etc/crane/CA_cert.crt
    if [ $? -ne 0 ]; then
        exit 1
    fi

    vault write pki/roles/CraneSched \
        issuer_ref="$(vault read -field=default pki/config/issuers)" \
        allowed_domains="$domainSuffix" \
        allow_subdomains=true \
        allow_glob_domains=true \
        not_after="9999-12-31T23:59:59Z"
    if [ $? -ne 0 ]; then
        exit 1
    fi

    vault write pki/config/urls \
          issuing_certificates="$VAULT_ADDR/v1/pki/ca" \
          crl_distribution_points="$VAULT_ADDR/v1/pki/crl" \
          ocsp_servers="$VAULT_ADDR/v1/pki/ocsp"
    if [ $? -ne 0 ]; then
        exit 1
    fi

    vault write -format=json pki/intermediate/generate/internal common_name="*.$domainSuffix" \
          issuer_name="CraneSched_CA" not_after="9999-12-31T23:59:59Z" | jq -r '.data.csr' > /etc/crane/tls/ca.csr

    vault write -format=json pki/root/sign-intermediate csr=@/etc/crane/tls/ca.csr \
          format=pem_bundle not_after="9999-12-31T23:59:59Z" | jq -r '.data.certificate' > /etc/crane/tls/ca.pem

    # Import the signed intermediate certificate into Vault.
    vault write pki/intermediate/set-signed certificate=@/etc/crane/tls/ca.pem

    issue_internal "$domainSuffix"
    issue_external "$domainSuffix"
}

issue_internal (){
    # 检查是否传递了 domainSuffix 参数
    if [ -z "$1" ]; then
        echo "Error: Please provide a domain suffix!"
        echo "Usage: issure_internal <domainSuffix>"
        return 1
    fi

    local domainSuffix="$1"  # 接收传递的 domainSuffix 参数

    output=$(vault write -format=json pki/issue/CraneSched common_name="*.$domainSuffix" alt_names="localhost,*.$domainSuffix" not_after="9999-12-31T23:59:59Z" exclude_cn_from_sans=true)
    certificate=$(echo "$output" | jq -r '.data.certificate')
    echo "$certificate" > /etc/crane/tls/internal.pem

    private_key=$(echo "$output" | jq -r '.data.private_key')
    echo "$private_key" > /etc/crane/tls/internal.key
    chmod 600 /etc/crane/tls/internal.key

    ca=$(echo "$output" | jq -r '.data.issuing_ca')
    echo "$ca" > /etc/crane/tls/ca.pem
}

issue_external() {
    # 检查是否传递了 domainSuffix 参数
    if [ -z "$1" ]; then
        echo "Error: Please provide a domain suffix!"
        echo "Usage: issure_internal <domainSuffix>"
        return 1
    fi

    mkdir -p /etc/crane/tls

    local domainSuffix="$1"  # 接收传递的 domainSuffix 参数

    output=$(vault write -format=json pki/issue/CraneSched common_name="external.$domainSuffix" alt_names="localhost, *.$domainSuffix" not_after="9999-12-31T23:59:59Z"  exclude_cn_from_sans=true)
    certificate=$(echo "$output" | jq -r '.data.certificate')
    echo "$certificate" > /etc/crane/tls/external.pem

    private_key=$(echo "$output" | jq -r '.data.private_key')
    echo "$private_key" > /etc/crane/tls/external.key
    chmod 600 /etc/crane/tls/external.key

    ca=$(echo "$output" | jq -r '.data.issuing_ca')
    echo "$ca" > /etc/crane/tls/ca.pem
}


init() {
    if [ "$#" -eq 0 ]; then
      domainSuffix="crane.local"
    else
      domainSuffix="$1"
    fi

    mkdir -p /etc/crane/tls

    init_vault
    init_cert "$domainSuffix"
    echo "init 完成"
}

clean_vault(){
  systemctl stop vault
  rm -rf /etc/vault/data/
  mkdir -p /etc/vault/data/
  systemctl start vault
  echo "clean vault 完成"
}

# 主逻辑：根据输入参数调用对应的函数
if [ "$#" -lt 1 ]; then
    echo "Usage: \$0 <function_name> [init, init_vault, init_cert, issue_internal, issue_external, login_vault, unseal_vault, clean_vault]"
    exit 1
fi

function_name="$1"
shift # 移除第一个参数，保留剩余参数

case "$function_name" in
    init)
        init "$@"
        ;;
    init_vault)
        init_vault
        ;;
     issue_internal)
        issue_internal "$@"
        ;;
     issue_external)
         issue_external "$@"
         ;;
     login_vault)
         login_vault "$@"
         ;;
     unseal_vault)
         unseal_vault "$@"
         ;;
     init_cert)
         init_cert "$@"
         ;;
     clean_vault)
         clean_vault "$@"
         ;;
    *)
        echo "Error: Unknown function name '$function_name'."
        echo "Available functions: init, init_vault, init_cert, issue_internal, issue_external, login_vault, unseal_vault, clean_vault"
        exit 1
        ;;
esac