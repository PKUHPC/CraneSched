#!/bin/bash

echo "Usage: $0 mode(1:acct_table | 2:qos_table | 3:task_table | 4:user_table | 5:all | 6:acct_table+qos_table+user_table | 7:all+task_table_index)"

if [ "$#" -ne 1 ]; then
  echo "Parameter error: please input mode num!"
  exit 1
fi

mode=$1

# Read info from configuration file
conf_file=/etc/crane/database.yaml
base_dir=/var/crane/

username=$(grep 'DbUser:' "$conf_file" | awk '{print $2}')
password=$(grep 'DbPassword:' "$conf_file" | awk -F\" '{print $2}')
host=$(grep 'DbHost:' "$conf_file" | awk '{print $2}')
port=$(grep 'DbPort:' "$conf_file" | awk '{print $2}')
dbname=$(grep 'DbName:' "$conf_file" | awk '{print $2}')
embedded_db_path="$base_dir$(grep 'CraneCtldDbPath:' "$conf_file" | awk '{print $2}')"

# Use mongosh to connect to MongoDB and wipe data
function wipe_collection() {
  local table_name="$1"
  local isDelIndex="${2:-false}"
  mongosh --username "$username" --password "$password" --host "$host" --port "$port" --authenticationDatabase admin <<EOF
    use $dbname
    if ("$isDelIndex" === "true") {
      db.$table_name.drop(); // 删除集合及其索引
      db.createCollection("$table_name"); // 重新创建集合
      print("Collection '$table_name' and its indexes were dropped and recreated.")
    } else {
      db.$table_name.deleteMany({}); // 删除集合中的所有数据
      print("Documents in collection '$table_name' were deleted.")
    }
EOF
}

# Wipe data according to mode
if [ "$mode" -eq 1 ] || [ "$mode" -eq 5 ] || [ "$mode" -eq 6 ] || [ "$mode" -eq 7 ]; then
  wipe_collection acct_table
fi
if [ "$mode" -eq 2 ] || [ "$mode" -eq 5 ] || [ "$mode" -eq 6 ] || [ "$mode" -eq 7 ]; then
  wipe_collection qos_table
fi
if [ "$mode" -eq 3 ] || [ "$mode" -eq 5 ] || [ "$mode" -eq 7 ]; then
  if [ "$mode" -eq 7 ]; then
    wipe_collection task_table true
  else
    wipe_collection task_table false
  fi
  # Get the directory and filename of the embedded database
  db_dir=$(dirname "$embedded_db_path")
  db_filename=$(basename "$embedded_db_path")

  # Remove the embedded database files
  if [ -d "$db_dir" ]; then
    echo "Removing files like $db_filename* in $db_dir ..."
    rm -f "$db_dir"/"$db_filename"*
  fi
fi
if [ "$mode" -eq 4 ] || [ "$mode" -eq 5 ] || [ "$mode" -eq 6 ] || [ "$mode" -eq 7 ]; then
  wipe_collection user_table
fi