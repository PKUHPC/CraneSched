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
    output=$(vault operator init --format=json) # This command will generate 5 Unseal Keys and 1 root token. Keep them safe for future use.
    if [ $? -ne 0 ]; then
        exit 1
    fi
    # The 5 Unseal Keys must be saved! Losing them may result in Vault being unusable.

    # Vault needs to be unsealed before use. Use any 3 of the above unseal keys.
    key=$(echo "$output" | jq -r '.unseal_keys_b64[0]')
    vault operator unseal $key
    key=$(echo "$output" | jq -r '.unseal_keys_b64[1]')
    vault operator unseal $key
    key=$(echo "$output" | jq -r '.unseal_keys_b64[2]')
    vault operator unseal $key

    token=$(echo "$output" | jq -r '.root_token')
    vault login $token # Use root token to log in and perform CLI operations
    if [ $? -ne 0 ]; then
        echo "Login failed"
        exit 1
    fi

    mkdir -p /etc/vault.d
    keys=$(echo "$output" | jq '.unseal_keys_b64')
    echo $keys > /etc/vault.d/vault_keys.txt
    chmod 600 /etc/vault.d/vault_keys.txt

    echo -n $token > /etc/vault.d/vault_token.txt
    chmod 600 /etc/vault.d/vault_token.txt

    # Create admin-policy file
    echo "Creating admin-policy file..."
    cat <<EOF > /etc/vault.d/admin-policy.hcl
path "*" {
  capabilities = ["create", "read", "update", "delete", "list", "sudo"]
}
EOF

    # Write admin-policy to Vault
    echo "Writing admin-policy to Vault..."
    vault policy write admin-policy /etc/vault.d/admin-policy.hcl || {
      echo "Failed to write admin-policy."
      exit 1
    }

    vault auth enable userpass

    # Create admin user and assign policy
    echo "Creating admin user with admin-policy..."
    vault write auth/userpass/users/admin password="123456" policies="admin-policy" || {
      echo "Failed to create admin user."
      exit 1
    }

    # Log in as admin user
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

    # Generate CA certificate
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
    # Check if domainSuffix is provided
    if [ -z "$1" ]; then
        echo "Error: Please provide a domain suffix!"
        echo "Usage: issue_internal <domainSuffix>"
        return 1
    fi

    local domainSuffix="$1"  # Receive domainSuffix parameter

    output=$(vault write -format=json pki/issue/CraneSched common_name="*.$domainSuffix" alt_names="localhost,*.$domainSuffix" not_after="9999-12-31T23:59:59Z" exclude_cn_from_sans=true)
    certificate=$(echo "$output" | jq -r '.data.certificate')
    echo "$certificate" > /etc/crane/tls/internal.pem

    private_key=$(echo "$output" | jq -r '.data.private_key')
    echo "$private_key" > /etc/crane/tls/internal.key
    if chgrp crane /etc/crane/tls/internal.key; then
      echo "chgrp success"
      chmod 640 /etc/crane/tls/internal.key
    else
      echo "chgrp failed，group not exist"
      chmod 600 /etc/crane/tls/internal.key
    fi

    ca=$(echo "$output" | jq -r '.data.issuing_ca')
    echo "$ca" > /etc/crane/tls/ca.pem
}

issue_external() {
    # Check if domainSuffix is provided
    if [ -z "$1" ]; then
        echo "Error: Please provide a domain suffix!"
        echo "Usage: issue_external <domainSuffix>"
        return 1
    fi

    mkdir -p /etc/crane/tls

    local domainSuffix="$1"  # Receive domainSuffix parameter

    output=$(vault write -format=json pki/issue/CraneSched common_name="external.$domainSuffix" alt_names="localhost, *.$domainSuffix" not_after="9999-12-31T23:59:59Z"  exclude_cn_from_sans=true)
    certificate=$(echo "$output" | jq -r '.data.certificate')
    echo "$certificate" > /etc/crane/tls/external.pem

    private_key=$(echo "$output" | jq -r '.data.private_key')
    echo "$private_key" > /etc/crane/tls/external.key
    if chgrp crane /etc/crane/tls/external.key; then
      echo "chgrp success"
      chmod 640 /etc/crane/tls/internal.key
    else
      echo "chgrp failed，group not exist"
      chmod 600 /etc/crane/tls/internal.key
    fi

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
    echo "Initialization completed"
}

clean_vault(){
  systemctl stop vault
  rm -rf /etc/vault/data/
  mkdir -p /etc/vault/data/
  systemctl start vault
  echo "Vault cleanup completed"
}

# Main logic: call the corresponding function according to the input parameter
if [ "$#" -lt 1 ]; then
    echo "Usage: \$0 <function_name> [init, init_vault, init_cert, issue_internal, issue_external, login_vault, unseal_vault, clean_vault]"
    exit 1
fi

function_name="$1"
shift # Remove the first argument, keep the rest

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