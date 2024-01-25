mongo_path="$1"
if [ $# -lt 1 ]; then
    echo "usage: ./install_db.sh [mongodb_data_path]"
    exit 1
fi
echo "using mongo_path $mongo_path"

function error_exit {
  echo "install mongodb fail: $1"
  exit 1
}

echo "install mongodb.."
rm -f /etc/yum.repos.d/mongodb-6.0.2.repo
rm -f /etc/mongod.conf
rm -f "$mongo_path"/mongo.key
cat >> /etc/yum.repos.d/mongodb-6.0.2.repo << 'EOF'
[mongodb-org-6.0.2]
name=MongoDB 6.0.2 Repository
baseurl=http://mirrors.aliyun.com/mongodb/yum/redhat/7Server/mongodb-org/6.0/x86_64/
gpgcheck=0
enabled=1
EOF
yum install mongodb-org -y

config="
systemLog:
  destination: file
  logAppend: true
  path: /var/log/mongodb/mongod.log
storage:
  dbPath: $mongo_path
  journal:
    enabled: true
processManagement:
  fork: true
  pidFilePath: /var/run/mongodb/mongod.pid
  timeZoneInfo: /usr/share/zoneinfo
net:
  port: 27017
  bindIp: 127.0.0.1
"
echo "$config" > /etc/mongod.conf

echo "gen ssl.."
mkdir -p "$mongo_path"
openssl rand -base64 756 | sudo -u mongod tee "$mongo_path"/mongo.key
sudo -u mongod chmod 400 "$mongo_path"/mongo.key
chown mongod:mongod "$mongo_path" -R
chown mongod:mongod /etc/mongod.conf
rm -f /tmp/mongodb-27017.sock

echo "enable mongod.."
systemctl enable mongod
systemctl start mongod || error_exit "failed to start mongod"
sleep 1

echo "create user.."
commands='
use admin;
db.createUser({ user: "admin", pwd: "123456", roles: [{ role: "root", db: "admin" }] });
db.shutdownServer();
quit()
'
echo "$commands" | mongosh || error_exit "failed to execute mongosh"

echo "configure mongod.."

config2="
#开启权限验证
security:
  authorization: enabled
  keyFile: $mongo_path/mongo.key
replication:
  #副本集名称,crane的配置文件要与此一致
  replSetName: crane_rs
"
echo "$config2" >> /etc/mongod.conf
systemctl restart mongod
sleep 1
commands2='
use admin;
db.auth("admin","123456");
rs.initiate();
quit();
'
echo "$commands2" | mongosh || error_exit "failed to execute mongosh"

echo "install mongodb done."