#!/bin/bash

rm -f /usr/lib/libcraned.so
rm -f /usr/local/bin/craned
rm -rf /usr/include/craned
systemctl disable craned
rm -f /etc/systemd/system/craned.service

rm -f /usr/lib64/security/pam_crane.so
if grep -q "pam_crane.so" /etc/pam.d/sshd; then
   sed -i '/account    required     pam_crane.so/d' /etc/pam.d/sshd
   sed -i '/session    optional     pam_crane.so/d' /etc/pam.d/sshd
fi
systemctl deamon-reload
