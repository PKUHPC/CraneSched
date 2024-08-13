#!/bin/bash

function error_exit {
  echo "install fail: $1"
  exit 1
}

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

    -f)
      force_install="1"
      shift
      shift
      ;;
    *)
      echo "unknown option $1"
      exit 1
      ;;
  esac
done

build_path="../build/"

files=$(ls $build_path | grep -E "craneSched-.*-Linux-cranectld.rpm")
count=$(echo "$files" | wc -l)
if [ $count -ge 1 ]; then
    ctld_rpm_name=$(echo "$files" | head -n 1)
else
    echo "cranectld rpm not found"
    exit 1
fi

files=$(ls $build_path | grep -E "craneSched-.*-Linux-craned.rpm")
count=$(echo "$files" | wc -l)
if [ $count -ge 1 ]; then
    craned_rpm_name=$(echo "$files" | head -n 1)
else
    echo "craned rpm not found"
    exit 1
fi

ctld_rpm_path="$build_path$ctld_rpm_name"
craned_rpm_path="$build_path$craned_rpm_name"

if [ -z "$ctld_pdsh_params" ] || [ -z "$craned_pdsh_params" ]; then
  echo "missing params: ./cluster_installer_rocky.sh --ctld [pdsh params...] --craned [pdsh params...]"
  exit 1
fi

echo "checking & installing pdsh.."
if command -v pdsh &> /dev/null; then
    echo "pdsh check done"
else
    echo "installing pdsh.."
    yum install pdsh
fi
output=$(pdsh -R ssh -w "$ctld_pdsh_params" "rpm -q pdsh")
if [[ ! $output == *"pdsh-"* ]]; then
  echo "installing pdsh for nodes.."
  pdsh -R ssh -w "$ctld_pdsh_params" yum install pdsh
fi
output=$(pdsh -R ssh -w "$craned_pdsh_params" "rpm -q pdsh")
if [[ ! $output == *"pdsh-"* ]]; then
  echo "installing pdsh for nodes.."
  pdsh -R ssh -w "$craned_pdsh_params" yum install pdsh
fi

echo "copying rpm.."


pdcp -R ssh -w "$ctld_pdsh_params" "$ctld_rpm_path" /tmp/ || error_exit "copy rpm fail"
pdcp -R ssh -w "$craned_pdsh_params" "$craned_rpm_path" /tmp/ || error_exit "copy rpm fail"

if [ -n "$force_install" ]; then
  echo "uninstalling former rpm.."
  pdsh -R ssh -w "$ctld_pdsh_params"  rpm -e cranesched-cranectld || error_exit "uninstall former rpm fail"
  pdsh -R ssh -w "$craned_pdsh_params"  rpm -e cranesched-craned || error_exit "uninstall former rpm fail"
fi

echo "installing rpm.."

pdsh -R ssh -w "$ctld_pdsh_params"  yum install -y /tmp/$ctld_rpm_name || error_exit "install rpm fail"
pdsh -R ssh -w "$craned_pdsh_params"  yum install -y /tmp/$craned_rpm_name || error_exit "install rpm fail"

echo "done!"
