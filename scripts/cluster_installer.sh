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
    --dbpath)
      mongo_path="$2"
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
  echo "missing params: ./cluster_installer.sh --ctld [pdsh params...] --craned [pdsh params...]"
  exit 1
fi

echo "checking & installing pdsh.."
if command -v pdsh &> /dev/null; then
    echo "pdsh check done"
else
    echo "installing pdsh.."
    sudo yum install -y pdsh
fi
output=$(pdsh "$ctld_pdsh_params" -R ssh "rpm -q pdsh 2>/dev/null")
if [[ ! "$output" =~ "pdsh" ]]; then
  echo "installing pdsh for nodes.."
  pdsh "$ctld_pdsh_params" -R ssh "sudo yum install -y pdsh"
fi
output=$(pdsh "$craned_pdsh_params" -R ssh "rpm -q pdsh 2>/dev/null")
if [[ ! "$output" =~ "pdsh" ]]; then
  echo "installing pdsh for nodes.."
  pdsh "$craned_pdsh_params" -R ssh "sudo yum install -y pdsh"
fi

echo "copying rpm.."

pdcp "$ctld_pdsh_params" "$ctld_rpm_path" /tmp/ || error_exit "copy rpm fail"
pdcp "$craned_pdsh_params" "$craned_rpm_path" /tmp/ || error_exit "copy rpm fail"

if [ -n "$force_install" ]; then
  echo "uninstalling former rpm.."
  pdsh "$ctld_pdsh_params" -R ssh "sudo yum remove cranesched-cranectld -y" || error_exit "uninstall former rpm fail"
  pdsh "$craned_pdsh_params" -R ssh "sudo yum remove cranesched-craned -y" || error_exit "uninstall former rpm fail"
fi

echo "installing rpm.."

pdsh "$ctld_pdsh_params" -R ssh "sudo rpm -ivh /tmp/$ctld_rpm_name" || error_exit "install rpm fail"
pdsh "$craned_pdsh_params" -R ssh "sudo rpm -ivh /tmp/$craned_rpm_name" || error_exit "install rpm fail"

if [ -z "$db_pdsh_params" ]; then
  echo "skipping mongodb instal.."
  echo "installation done!"
  exit 0
fi
echo "installing mongodb.."
if [ -z "$mongo_path" ]; then
  mongo_path="/var/lib/mongo"
fi

pdcp "$db_pdsh_params" install_db.sh /tmp/
pdsh "$db_pdsh_params" -R ssh "sudo bash /tmp/install_db.sh $mongo_path" || error_exit "install mongodb fail"
echo "installation done."
