#!/bin/bash

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
  echo "missing params: ./cluster_installer.sh --ctld [pdsh params...] --craned [pdsh params...]"
  exit 1
fi

pdsh $ctld_pdsh_params -R ssh "sudo yum install -y pdsh"
pdsh $craned_pdsh_params -R ssh "sudo yum install -y pdsh"

pdcp "$ctld_pdsh_params" "$ctld_rpm_path" /tmp/
pdcp "$craned_pdsh_params" "$craned_rpm_path" /tmp/

pdsh $ctld_pdsh_params -R ssh "sudo rpm -ivh /tmp/$ctld_rpm_name"
pdsh $craned_pdsh_params -R ssh "sudo rpm -ivh /tmp/$craned_rpm_name"