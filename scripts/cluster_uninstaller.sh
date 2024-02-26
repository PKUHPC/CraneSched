#!/bin/bash

function error_exit {
  echo "uninstall fail: $1"
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
    *)
      echo "unknown option $1"
      exit 1
      ;;
  esac
done

pdsh "$ctld_pdsh_params" -R ssh "sudo yum remove cranesched-cranectld -y" || error_exit "uninstall rpm fail"
pdsh "$craned_pdsh_params" -R ssh "sudo yum remove cranesched-craned -y" || error_exit "uninstall rpm fail"

pdcp "$db_pdsh_params" wipe_data.sh /tmp/
pdsh "$db_pdsh_params" -R ssh "sudo bash /tmp/wipe_data.sh 5" || error_exit "wipe data fail"
