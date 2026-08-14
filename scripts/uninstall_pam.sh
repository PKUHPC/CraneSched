#!/bin/bash
if grep -q "pam_crane.so" /etc/pam.d/sshd; then
   sed -i '/pam_crane\.so/d' /etc/pam.d/sshd
fi
