mongo_path="$1"
if [ $# -lt 1 ]; then
    echo "usage: ./install_db.sh [mongodb_data_path]"
    exit 1
fi
if [ "${mongo_path: -1}" = "/" ]; then
  mongo_path="${mongo_path%?}"
fi
echo "using mongo_path $mongo_path"

function error_exit {
  echo "install mongodb fail: $1"
  exit 1
}

echo "install mongodb.."
wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | sudo apt-key add -
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.4.list
echo "deb http://security.ubuntu.com/ubuntu focal-security main" | sudo tee /etc/apt/sources.list.d/focal-security.list
sudo apt-get update
sudo apt-get install libcurl4 openssl
sudo apt-get install libssl1.1
sudo apt-get install -y mongodb-org=4.4.2 mongodb-org-server=4.4.2 mongodb-org-shell=4.4.2 mongodb-org-mongos=4.4.2 mongodb-org-tools=4.4.2
sudo apt-get install -y mongodb-mongosh-shared-openssl11

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

sudo rm -rf /tmp/mongodb-27017.sock
sudo rm -f /var/lib/mongo/mongod.lock
sudo rm -f /var/run/mongodb/mongod.pid
sudo mkdir -p  /var/run/mongodb/
touch /var/run/mongodb/mongod.pid
sudo chown -R  mongodb:mongodb /var/run/mongodb/
sudo chown mongodb:mongodb /var/run/mongodb/mongod.pid

sudo systemctl start mongod

echo "gen ssl.."
mkdir -p "$mongo_path"
openssl rand -base64 756 | sudo tee "$mongo_path"/mongo.key

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
security:
  authorization: enabled
  keyFile: $mongo_path/mongo.key
replication:
  replSetName: crane_rs
"
echo "$config2" >> /etc/mongod.conf

chown mongodb:mongodb /var/lib/mongodb/mongo.key
chmod 600 /var/lib/mongodb/mongo.key

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