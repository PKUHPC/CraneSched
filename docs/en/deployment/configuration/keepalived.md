# Keepalived Configuration

The system adopts a high-availability management solution based on the
VRRP (Virtual Router Redundancy Protocol).
It enables second-level master–backup failover and transparent service migration,
significantly improving system disaster recovery capability and availability.

By automatically detecting failures of the master node and switching the
Virtual IP (VIP) to the backup node, the system can quickly restore services
and ensure high availability.
In addition, the health check module supports custom monitoring logic,
providing application-level high-availability scheduling and further enhancing
system stability and reliability.

## Installation
```shell
dnf install keepalived
```
## Configuration Files

### Crane Config (/etc/crane/config.yaml)
```bash
Keepalived:
  # the base directory on a shared storage
  CraneSharedBaseDir: /var/crane/
  # file path of cranectld alive file (relative to CraneBaseDir)
  CraneCtldAliveFile: cranectld/cranectld.alive

```

### Master Node (/etc/keepalived/keepalived.conf)
```bash
global_defs {
   script_user root # user used to execute scripts
   enable_script_security
}

vrrp_script chk_cranectld {
    script "/etc/keepalived/check_and_failover.sh"
    interval 1   # script execution interval
    fall 2       # mark as failed only after 2 consecutive failures (~4 seconds)
    weight -20   # reduce master priority by 20 when script fails
}

vrrp_instance VI_1 {
    state MASTER               # use BACKUP on standby nodes
    interface ens33            # replace with your actual NIC name
    virtual_router_id 51
    priority 100               # use 90 or lower on backup nodes
    advert_int 1
    authentication {
        auth_type PASS
        auth_pass 1111
    }
    virtual_ipaddress {
        192.168.1.211          # replace with your VIP
    }
    track_script {
        chk_cranectld
    }
    notify_master "/etc/keepalived/on_master.sh"   # optional, executed when becoming MASTER
    # notify_backup "/etc/keepalived/on_backup.sh" # optional, executed when becoming BACKUP
    # notify_fault  "/etc/keepalived/on_fault.sh"  # optional, executed on fault
}
```

### Backup Node (/etc/keepalived/keepalived.conf)
```bash
global_defs {
   script_user root # user used to execute scripts
   enable_script_security
}

vrrp_instance VI_1 {
    state BACKUP
    interface ens33            # replace with your actual NIC name
    virtual_router_id 51
    priority 90
    advert_int 1
    authentication {
        auth_type PASS
        auth_pass 1111
    }
    virtual_ipaddress {
        192.168.1.211          # replace with your VIP
    }
    track_script {
        chk_cranectld
    }
    notify_master "/etc/keepalived/on_master.sh"   # optional, executed when switching to MASTER
    notify_backup "/etc/keepalived/on_backup.sh"   # optional, executed when switching to BACKUP
    # notify_fault  "/etc/keepalived/on_fault.sh"  # optional, executed on fault
}
```

## Scripts

**_All scripts, every directory in their paths, and the root (/) directory
must be writable only by root and owned by root.
Otherwise, keepalived will refuse to execute the scripts._**

### Health Check Script (/etc/keepalived/check_and_failover.sh)
```bash
#!/bin/bash

set -e

SCRIPT_NAME=$(basename "$0")
PROC_NAME="cranectld"
ALIVE_FILE="/var/crane/cranectld/cranectld.alive"

if ! pgrep -f "$PROC_NAME" > /dev/null 2>&1 && [ -f "$ALIVE_FILE" ]; then
    echo "[$(date)] [$SCRIPT_NAME] $PROC_NAME not running, $ALIVE_FILE exists, triggering failover..."
    exit 1
else
    echo "[$(date)] [$SCRIPT_NAME] health check passed."
    exit 0
fi
```

### OnMaster Script (/etc/keepalived/on_master.sh)
```bash
#!/bin/bash

LOCK_FILE="/nfs/home/shouxin/crane/cranectld/cranectld.lock"

echo "$date on_master execute" >> /tmp/on_master.log

# Open file descriptor 9
exec 9>"$LOCK_FILE"

# Try to acquire an exclusive lock, wait up to 2 seconds
if flock -x -w 2 9; then
  echo "Lock is NOT held by another CraneCtld instance (acquired)." >> /tmp/on_master.log
  # Release the lock without deleting the file
  flock -u 9

  systemctl restart cranectld >> /tmp/on_master.log 2>&1
else
  echo "Could not acquire lock within 5 seconds. Assuming lock is held by another instance." >> /tmp/on_master.log
  # Close fd 9
  exec 9>&-
fi

# Email notifications can be configured here if needed
```

### OnBackup Script (/etc/keepalived/on_backup.sh)
```bash
#!/bin/bash

# Email notifications can be configured here if needed

echo "on_backup execute" >> /tmp/on_backup.log

systemctl stop cranectld >> /tmp/on_backup.log 2>&1
```

## Startup

1. Deploy **ctld**: only start `ctld` on the **master** node.  
   Starting `ctld` manually on the **backup** node is strictly prohibited.
2. Start **keepalived**:
```bash
systemctl enable keepalived
systemctl start keepalived
```

**_Notes_**

1. If `journalctl -u keepalived` does **not** show 
   `Unsafe permission found for script 'xxx.sh' -disabling`, 
   the script permissions are configured correctly.
2. If the **onMaster** or **onBackUp** scripts fail to execute, you can disable SELinux:
   `sudo setenforce 0`
3. If email notifications are required, configure them in the onmaster, onbackup, and onfault scripts.
4. Do not manually start ctld on the backup node, as this may prevent ctld from starting on the master node.
5. If ctld fails to start on the master node with an error indicating that another instance already exists, you can delete the file:
`/var/crane/cranectld/cranectld.alive`, After deletion, the master node will be re-elected automatically, ctld will start running on the master node, 
and ctld on the backup node will be stopped automatically. If the master node becomes unavailable, 
the administrator should restore the master node as soon as possible.


```bash
# /etc/keepalived/on_master.sh: line 13: /usr/bin/systemctl: Permission denied

# When SELinux is in Enforcing mode, it may prevent keepalived
# from executing systemctl as root.
# Solution:
# Check SELinux status:
getenforce
# If the output is Enforcing, try disabling it temporarily:
setenforce 0
```
