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

files=$(ls $build_path | grep -E "craneSched-.*-Linux-cranectld.deb")
count=$(echo "$files" | wc -l)
if [ $count -ge 1 ]; then
    ctld_deb_name=$(echo "$files" | head -n 1)
else
    echo "cranectld deb not found"
    exit 1
fi

files=$(ls $build_path | grep -E "craneSched-.*-Linux-craned.deb")
count=$(echo "$files" | wc -l)
if [ $count -ge 1 ]; then
    craned_deb_name=$(echo "$files" | head -n 1)
else
    echo "craned deb not found"
    exit 1
fi

ctld_deb_path="$build_path$ctld_deb_name"
craned_deb_path="$build_path$craned_deb_name"

if [ -z "$ctld_pdsh_params" ] || [ -z "$craned_pdsh_params" ]; then
  echo "missing params: ./cluster_installer_ubuntu.sh --ctld [pdsh params...] --craned [pdsh params...]"
  exit 1
fi

echo "checking & installing pdsh.."
if command -v pdsh &> /dev/null; then
    echo "pdsh check done"
else
    echo "installing pdsh.."
    sudo apt -y install pdsh
fi
output=$(pdsh "$ctld_pdsh_params" -R ssh "dpkg -s pdsh")
if [[ ! $output == *"install ok installed"* ]]; then
  echo "installing pdsh for nodes.."
  pdsh "$ctld_pdsh_params" -R ssh "sudo apt -y install pdsh"
fi
output=$(pdsh "$craned_pdsh_params" -R ssh "dpkg -s pdsh")
if [[ ! $output == *"install ok installed"* ]]; then
  echo "installing pdsh for nodes.."
  pdsh "$craned_pdsh_params" -R ssh "sudo apt -y install pdsh"
fi

echo "copying deb.."

pdcp "$ctld_pdsh_params" "$ctld_deb_path" /tmp/ || error_exit "copy deb fail"
pdcp "$craned_pdsh_params" "$craned_deb_path" /tmp/ || error_exit "copy deb fail"

if [ -n "$force_install" ]; then
  echo "uninstalling former deb.."
  pdsh "$ctld_pdsh_params" -R ssh "sudo dpkg -r cranesched-cranectld" || error_exit "uninstall former deb fail"
  pdsh "$craned_pdsh_params" -R ssh "sudo dpkg -r cranesched-craned" || error_exit "uninstall former deb fail"
fi

echo "installing deb.."

pdsh "$ctld_pdsh_params" -R ssh "sudo dpkg --force-all -i  /tmp/$ctld_rpm_name" || error_exit "install deb fail"
pdsh "$craned_pdsh_params" -R ssh "sudo dpkg --force-all -i  /tmp/$craned_rpm_name" || error_exit "install deb fail"

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
