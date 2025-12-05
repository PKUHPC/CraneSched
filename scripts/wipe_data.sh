#!/bin/bash

echo "Usage: $0 mode(1:acct_table | 2:qos_table | 3:task_table | 4:user_table | 5:all | 6:acct_table+qos_table+user_table| 7:hour_table+day_table+month_table |8: summary_time_table)"

if [ "$#" -ne 1 ]; then
  echo "Parameter error: please input mode num!"
  exit 1
fi

mode=$1

# Read info from configuration file
global_conf_file=/etc/crane/config.yaml
conf_file=$(grep 'DbConfigPath:' "$global_conf_file" | awk '{print $2}')
base_dir=$(grep 'CraneBaseDir:' "$global_conf_file" | awk '{print $2}')

username=$(grep 'DbUser:' "$conf_file" | awk '{print $2}')
password=$(grep 'DbPassword:' "$conf_file" | awk -F\" '{print $2}')
host=$(grep 'DbHost:' "$conf_file" | awk '{print $2}')
port=$(grep 'DbPort:' "$conf_file" | awk '{print $2}')
dbname=$(grep 'DbName:' "$conf_file" | awk '{print $2}')
embedded_db_path="$base_dir$(grep 'CraneCtldDbPath:' "$conf_file" | awk '{print $2}')"

# Use mongosh to connect to MongoDB and wipe data
function wipe_collection() {
  mongosh --username "$username" --password "$password" --host "$host" --port "$port" --authenticationDatabase admin <<EOF
    use $dbname
    db.$1.deleteMany({})
    exit
EOF
}

function batch_insert_collection() {
  collection=$1
  json_file=$2
  mongoimport --username "$username" --password "$password" --host "$host" --port "$port" \
    --authenticationDatabase admin --db "$dbname" --collection "$collection" \
    --file "$json_file" --jsonArray
}

function batch_insert_collection_process() {
  collection=$1
  json_file=$2
  mongoimport --username "$username" --password "$password" --host "$host" --port "$port" \
    --authenticationDatabase admin --db "$dbname" --collection "$collection" \
    --file "$json_file"
}

# Wipe data according to mode
if [ "$mode" -eq 1 ] || [ "$mode" -eq 5 ] || [ "$mode" -eq 6 ]; then
  wipe_collection acct_table
fi
if [ "$mode" -eq 2 ] || [ "$mode" -eq 5 ] || [ "$mode" -eq 6 ]; then
  wipe_collection qos_table
fi
if [ "$mode" -eq 3 ] || [ "$mode" -eq 5 ]; then
  wipe_collection task_table

  # Get the directory and filename of the embedded database
  db_dir=$(dirname "$embedded_db_path")
  db_filename=$(basename "$embedded_db_path")

  # Remove the embedded database files
  if [ -d "$db_dir" ]; then
    echo "Removing files like $db_filename* in $db_dir ..."
    rm -f "$db_dir"/"$db_filename"*
  fi
fi
if [ "$mode" -eq 4 ] || [ "$mode" -eq 5 ] || [ "$mode" -eq 6 ]; then
  wipe_collection user_table
fi
if [ "$mode" -eq 5 ] || [ "$mode" -eq 7 ]; then
  wipe_collection hour_job_summary_table
  wipe_collection day_job_summary_table
  wipe_collection month_job_summary_table
fi

if [ "$mode" -eq 5 ] || [ "$mode" -eq 8 ]; then
  wipe_collection "summary_time_table"
fi
