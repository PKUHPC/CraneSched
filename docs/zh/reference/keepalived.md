# keepalived 配置

系统采用基于 VRRP（Virtual Router Redundancy Protocol） 协议的高可用管理方案，
实现秒级主备切换和业务无感知迁移，大幅提升了系统的容灾能力和可用性。通过自动检测主节点故障并将虚拟 IP（VIP）切换到备节点，
系统可快速恢复业务，确保高可用性。此外，健康检查模块支持自定义监控逻辑，
提供应用层级的高可用调度，进一步增强系统的稳定性与可靠性。

## 安装
```shell
dnf install keepalived
```
## 配置文件
### Crane Config 配置（/etc/crane/config.yaml）
```yaml
Keepalived:
  # the base directory of NFS storage
  CraneNFSBaseDir: /var/crane/
  # file path of cranectld alive file (relative to CraneBaseDir)
  CraneCtldAliveFile: cranectld/cranectld.alive
```
### Master 节点（ /etc/keepalived/keepalived.conf）
```bash
global_defs {
   script_user root # 执行脚本的user
   enable_script_security
}

vrrp_script chk_cranectld {
    script "/etc/keepalived/check_and_failover.sh"
    interval 1 # 脚本运行间隔时间  
    fall 2 # 连续2次失败（约4秒）才判定真正挂了
    weight -20 # 当偏移时，master priority-20
}

vrrp_instance VI_1 {
    state MASTER               # 备节点写 BACKUP
    interface ens33            # 改为你的实际网卡名
    virtual_router_id 51
    priority 100               # 备节点写90或更低
    advert_int 1
    authentication {
        auth_type PASS
        auth_pass 1111
    }
    virtual_ipaddress {
        192.168.1.211          # 改为你的VIP
    }
    track_script {
        chk_cranectld
    }
    notify_master "/etc/keepalived/on_master.sh"   # 可选，切换成主状态时执行
    # notify_backup "/etc/keepalived/on_backup.sh"   # 可选，切换成备状态时执行
    # notify_fault  "/etc/keepalived/on_fault.sh"    # 可选，故障时执行
}

```

### BackUp节点（/etc/keepalived/keepalived.conf）
```bash
global_defs {
   script_user root # 执行脚本的user
   enable_script_security
}

vrrp_instance VI_1 {
    state BACKUP               # 备节点写 BACKUP
    interface ens33            # 改为你的实际网卡名
    virtual_router_id 51
    priority 90               
    advert_int 1
    authentication {
        auth_type PASS
        auth_pass 1111
    }
    virtual_ipaddress {
        192.168.1.211          # 改为你的VIP
    }
    track_script {
        chk_cranectld
    }
    notify_master "/etc/keepalived/on_master.sh"   # 可选，主状态切换时执行
    notify_backup "/etc/keepalived/on_backup.sh"   # 可选，备状态切换时执行
    # notify_fault  "/etc/keepalived/on_fault.sh"    # 可选，故障时执行
}
```

## 脚本
**_脚本、脚本路径每一层、以及`/`目录，需设置只有root可写，且属主为root，
否则keepalived不会启用脚本。_**

### 检查脚本（/etc/keepalived/check_and_failover.sh）
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

### OnMaster脚本  /etc/keepalived/on_master.sh
```bash
#!/bin/bash

LOCK_FILE="/nfs/home/shouxin/crane/cranectld/cranectld.lock"

echo "$date on_master execute">>/tmp/on_master.log

# 打开文件描述符 9
exec 9>"$LOCK_FILE"

# 尝试获取排它锁，等待最多 2 秒
if flock -x -w 2 9; then
  echo "Lock is NOT held by another CraneCtld instance (acquired).">>/tmp/on_master.log
  # 释放锁，不删除文件
  flock -u 9

  systemctl restart cranectld>>/tmp/on_master.log 2>&1
else
  echo "Could not acquire lock within 5 seconds. Assuming lock is held by another instance.">>/tmp/on_master.log
  # 关闭 fd 9
  exec 9>&-
fi

# 如有需要，可在此配置邮件通知
```

### OnBackup脚本 /etc/keepalived/on_backup.sh
```bash
#!/bin/bash

# 如有需要，可在此配置邮件通知

echo "on_backup execute">>/tmp/on_backup.log

systemctl stop cranectld>>/tmp/on_backup.log 2>&1
```

## 启动
1. 部署ctld  只在master节点启动ctld，禁止在backup节点手动启动ctld
2. 启动keepalived
```bash
   systemctl enable keepalived
   systemctl start keepalived 
```

**_注意事项_**

1. `journalctl -u keepalived`未出现`Unsafe permission found for script 'xxx.sh' -disabling`，则权限设置正确
2. 如果onMaster脚本与onBackUp脚本执行失败，可关闭 SELinux`sudo setenforce 0`
3. 如需配置邮件通知，可在onmaster、onbackup和onfault脚本内设置。
4. 禁止在backup节点手动启动ctld，有可能导致master节点ctld无法启动
5. 当出现在master节点启动ctld报错有其他实例存在的情况，可删除/var/crane/cranectld/cranectld.alive文件，此时master节点会重新当选，ctld会自动启动运行，backup节点ctld会自动关闭。
   当master节点不可用时，管理员应尽快重新启用master节点

```bash
# /etc/keepalived/on_master.sh: line 13: /usr/bin/systemctl: Permission denied

# SELinux 在 Enforcing 模式下，可能阻止 keepalived 以 root 执行 systemctl
# 解决办法：
# 查看 SELinux 状态：
getenforce
# 如果输出 Enforcing，临时关闭试试：
setenforce 0
```
