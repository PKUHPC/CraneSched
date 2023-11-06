#!/bin/bash
# post_install.sh

if ! grep -q "pam_Crane.so" /etc/pam.d/sshd; then
    sed -i '/-auth/a\account    required     pam_Crane.so' /etc/pam.d/sshd
    sed -i '/session    include      password-auth/a\session    optional     pam_Crane.so' /etc/pam.d/sshd
else
    echo "pam_Crane.so is already added."
fi