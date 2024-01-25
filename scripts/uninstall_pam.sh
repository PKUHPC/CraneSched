#!/bin/bash
if grep -q "pam_crane.so" /etc/pam.d/sshd; then
   sed -i '/^#account    required     pam_crane.so/d' /etc/pam.d/sshd
   sed -i '/^#session    optional     pam_crane.so/d' /etc/pam.d/sshd
fi