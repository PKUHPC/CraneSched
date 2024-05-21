#!/bin/bash

unameOut="$(uname -s)"
system="unknown"

if [[ "${unameOut}" == "Linux" && -f "/etc/lsb-release" ]]; then
    distro=$(grep "DISTRIB_ID" /etc/lsb-release | cut -d'=' -f2)
    if [[ "${distro}" == "Ubuntu" ]]; then
        echo "Ubuntu system detected"
        system="ubuntu"
    fi
fi

if [[ "${unameOut}" == "Linux" && -f "/etc/redhat-release" ]]; then
    distro=$(cat /etc/redhat-release)
    if [[ "${distro}" == *"CentOS"* ]]; then
        echo "CentOS system detected"
        system="centos"
    fi
fi

if [[ "$system" == "unknown" ]]; then
  echo "unknown system, exiting.."
  exit 1
fi

if [[ "$system" == "centos" ]]; then
    if ! grep -q "pam_crane.so" /etc/pam.d/sshd; then
        sed -i '/-auth      optional     pam_reauthorize.so prepare/a\#account    required     pam_crane.so' /etc/pam.d/sshd
        sed -i '/session    include      password-auth/a\#session    optional     pam_crane.so' /etc/pam.d/sshd
    fi
  else
    if ! grep -q "pam_crane.so" /etc/pam.d/sshd; then
        sed -i '/@include common-account/a\#account    required     pam_crane.so' /etc/pam.d/sshd
        sed -i '/@include common-session/a\#session    optional     pam_crane.so' /etc/pam.d/sshd
    fi
fi
