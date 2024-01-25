#!/bin/bash

if ! grep -q "pam_crane.so" /etc/pam.d/sshd; then
    sed -i '/-auth      optional     pam_reauthorize.so prepare/a\account    required     pam_crane.so' /etc/pam.d/sshd
    sed -i '/session    include      password-auth/a\session    optional     pam_crane.so' /etc/pam.d/sshd
fi