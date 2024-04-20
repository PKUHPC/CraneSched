#!/bin/bash

echo "Usage: $0 mode(1:acct_table | 2:qos_table | 3:task_table | 4:user_table | 5:all | 6:acct_table+qos_table+user_table)"

if [ "$#" -ne 1 ]; then
  echo "Parameter error: please input mode num!"
  exit 1
fi

mode=$1

# 读取配置文件中的账号密码以及unqlite文件路径
conf_file=/etc/crane/database.yaml
base_dir=/var/crane/
username=$(grep 'DbUser:' "$conf_file" | awk '{print $2}')
password=$(grep 'DbPassword:' "$conf_file" | awk -F\" '{print $2}')
host=$(grep 'DbHost:' "$conf_file" | awk '{print $2}')
port=$(grep 'DbPort:' "$conf_file" | awk '{print $2}')
dbname=$(grep 'DbName:' "$conf_file" | awk '{print $2}')
embedded_db_path="$base_dir$(grep 'CraneCtldDbPath:' "$conf_file" | awk '{print $2}')"

# 使用mongo shell连接到MongoDB服务器并清空指定的集合
function wipe_collection() {
  mongosh --username "$username" --password "$password" --host "$host" --port "$port" --authenticationDatabase admin <<EOF
    use $dbname
    db.$1.deleteMany({})
    exit
EOF
}

# 集合清除操作，根据mode执行不同操作
if [ "$mode" -eq 1 ] || [ "$mode" -eq 5 ] || [ "$mode" -eq 6 ]; then
  wipe_collection acct_table
fi
if [ "$mode" -eq 2 ] || [ "$mode" -eq 5 ] || [ "$mode" -eq 6 ]; then
  wipe_collection qos_table
fi
if [ "$mode" -eq 3 ] || [ "$mode" -eq 5 ]; then
  wipe_collection task_table

  # 获取Unqlite数据库文件所在目录和文件名前缀
  db_dir=$(dirname "$embedded_db_path")
  db_filename=$(basename "$embedded_db_path")

  # 删除该目录下所有以文件名前缀开头的文件
  if [ -d "$db_dir" ]; then
    echo "Removing files like $db_filename* in $db_dir ..."
    rm -f "$db_dir"/"$db_filename"*
  fi
fi
if [ "$mode" -eq 4 ] || [ "$mode" -eq 5 ] || [ "$mode" -eq 6 ]; then
  wipe_collection user_table
fi
