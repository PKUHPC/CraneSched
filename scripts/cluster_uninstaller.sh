#!/bin/bash

function error_exit {
  echo "uninstall fail: $1"
  exit 1
}

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

while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
    --ctld)
      ctld_pdsh_params="$2"
      shift
      shift
      ;;
    --craned)
      craned_pdsh_params="$2"
      shift
      shift
      ;;
    --db)
      db_pdsh_params="$2"
      shift
      shift
      ;;
    *)
      echo "unknown option $1"
      exit 1
      ;;
  esac
done

if [[ "$system" == "centos" ]]; then
    pdsh "$ctld_pdsh_params" -R ssh "sudo yum remove cranesched-cranectld -y" || error_exit "uninstall rpm fail"
    pdsh "$craned_pdsh_params" -R ssh "sudo yum remove cranesched-craned -y" || error_exit "uninstall rpm fail"
  else
    pdsh "$ctld_pdsh_params" -R ssh "sudo dpkg -r cranesched-cranectld" || error_exit "uninstall deb fail"
    pdsh "$craned_pdsh_params" -R ssh "sudo dpkg -r cranesched-craned" || error_exit "uninstall deb fail"
  fi

pdcp "$db_pdsh_params" wipe_data.sh /tmp/
pdsh "$db_pdsh_params" -R ssh "sudo bash /tmp/wipe_data.sh 5" || error_exit "wipe data fail"
