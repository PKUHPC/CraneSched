#!/bin/bash

rm -f /usr/lib/libcranectld.so
rm -f /usr/local/bin/cranectld
rm -f /usr/lib64/security/pam_crane.so

rm -rf /usr/include/cranectld

systemctl disable craned
rm -f /etc/systemd/system/cranectld.service
rm -rf /etc/crane

if grep -q "pam_crane.so" /etc/pam.d/sshd; then
   sed -i '/account    required     pam_crane.so/d' /etc/pam.d/sshd
   sed -i '/session    optional     pam_crane.so/d' /etc/pam.d/sshd
fi
systemctl deamon-reload
