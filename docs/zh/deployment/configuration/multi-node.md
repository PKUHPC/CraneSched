# 多节点部署

本指南介绍如何在集群中的多个节点上部署鹤思二进制文件和配置。

## 先决条件

- 鹤思已在一个节点（通常是控制节点）上构建和测试
- 所有节点都有网络连接并可以解析主机名
- 您对所有节点具有 root 或 sudo 访问权限

## 部署方法

### 方法 1：使用 SCP

适用于小型集群或单独部署到特定节点。

#### 部署到控制节点

```bash
# 复制控制节点二进制文件
ssh cranectl "mkdir -p /etc/crane"
scp CraneSched-1.1.2-Linux-x86_64-cranectld.rpm cranectl:/tmp
ssh cranectl "rpm -ivh /tmp/CraneSched-1.1.2-Linux-x86_64-cranectld.rpm"
scp /etc/crane/config.yaml cranectl:/etc/crane/
scp /etc/crane/database.yaml cranectl:/etc/crane/
```

#### 部署到计算节点

```bash
# 复制计算节点二进制文件
ssh crane02 "mkdir -p /etc/crane"
scp CraneSched-1.1.2-Linux-x86_64-craned.rpm crane02:/tmp
ssh crane02 "rpm -ivh /tmp/CraneSched-1.1.2-Linux-x86_64-craned.rpm"
scp /etc/crane/config.yaml crane02:/etc/crane/
```

对每个计算节点（crane03、crane04 等）重复此操作。

### 方法 2：使用 PDSH

推荐用于较大的集群。PDSH 允许跨多个节点并行执行。

#### 安装 PDSH

**Rocky 9：**
```bash
dnf install -y pdsh
```

**CentOS 7：**
```bash
yum install -y pdsh
```

#### 部署到控制节点

```bash
# 复制并安装 cranectld
pdcp -w cranectl CraneSched-1.1.2-Linux-x86_64-cranectld.rpm /tmp
pdsh -w cranectl "rpm -ivh /tmp/CraneSched-1.1.2-Linux-x86_64-cranectld.rpm"

# 复制配置文件
pdcp -w cranectl /etc/crane/config.yaml /etc/crane/
pdcp -w cranectl /etc/crane/database.yaml /etc/crane/

# 启动服务
pdsh -w cranectl "systemctl daemon-reload"
pdsh -w cranectl "systemctl enable cranectld"
pdsh -w cranectl "systemctl start cranectld"
```

#### 部署到计算节点

```bash
# 复制并安装 craned
pdcp -w crane[01-04] CraneSched-1.1.2-Linux-x86_64-craned.rpm /tmp
pdsh -w crane[01-04] "rpm -ivh /tmp/CraneSched-1.1.2-Linux-x86_64-craned.rpm"

# 复制配置文件
pdcp -w crane[01-04] /etc/crane/config.yaml /etc/crane/

# 启动服务
pdsh -w crane[01-04] "systemctl daemon-reload"
pdsh -w crane[01-04] "systemctl enable craned"
pdsh -w crane[01-04] "systemctl start craned"
```

## 替代方案：手动复制二进制文件

如果您从源码构建而不是 RPM 软件包：

```bash
# 控制节点
scp /usr/local/bin/cranectld cranectl:/usr/local/bin/
scp /etc/systemd/system/cranectld.service cranectl:/etc/systemd/system/

# 计算节点
pdcp -w crane[01-04] /usr/local/bin/craned /usr/local/bin/
pdcp -w crane[01-04] /usr/libexec/csupervisor /usr/libexec/
pdcp -w crane[01-04] /etc/systemd/system/craned.service /etc/systemd/system/
```

## 验证部署

部署后，验证所有节点都在线：

```bash
cinfo
```

您应该看到所有计算节点都列出，状态为 `IDLE` 或 `ALLOC`。

## 更新已部署的节点

更新现有部署：

```bash
# 停止服务
pdsh -w cranectl "systemctl stop cranectld"
pdsh -w crane[01-04] "systemctl stop craned"

# 部署新版本
pdcp -w cranectl CraneSched-new-version-cranectld.rpm /tmp
pdsh -w cranectl "rpm -Uvh /tmp/CraneSched-new-version-cranectld.rpm"

pdcp -w crane[01-04] CraneSched-new-version-craned.rpm /tmp
pdsh -w crane[01-04] "rpm -Uvh /tmp/CraneSched-new-version-craned.rpm"

# 启动服务
pdsh -w cranectl "systemctl start cranectld"
pdsh -w crane[01-04] "systemctl start craned"
```

## Gres配置

> 设备资源相关配置

- **name**：一般是资源类型如：`GPU`，`NPU`等
- **type**：一般是资源型号如：`A100`，`3090`等
- **DeviceFileRegex**: 资源对应的 /dev 目录下的设备文件，适用于一个物理设备对应一个设备文件的资源，**每个文件对应系统内的一个 Gres 资源**，支持 Regex 格式。常见设备对应设备文件。如 Nvidia、AMD、海光 DCU、昇腾等。
- **DeviceFileList**：适用于**一个物理设备对应多个 /dev 目录下设备文件的 Gres 资源**，每一组文件对应系统内的一个 Gres 资源，支持 Regex 格式。

DeviceFileRegex与DeviceFileList二选一，以上设备文件必须存在，**否则 Craned 启动时将报错退出**

- **EnvInjector**: 设备需要注入的环境变量

  -  可选值：对应环境变量

  - `nvidia`：`CUDA_VISIABLE_DEVICES`
  - `hip`：`HIP_VISIABLE_DEVICES`
  - `ascend`：`ASCEND_RT_VISIBLE_DEVICES`

- 常见厂商设备文件路径及相关配置

  - | 厂商        | 设备文件路径            | EnvInjector |
    | :---------- | :---------------------- | :---------- |
    | Nvidia      | /dev/nvidia0 ...        | nvidia      |
    | AMD/海光DCU | /dev/dri/renderer128... | hip         |
    | 昇腾        | /dev/davinci0 ...       | ascend      |
    | 天数智芯    | /dev/iluvatar0 ...      | nvidia      |

  - 


## 故障排除

**找不到 PDSH**：从 EPEL 仓库安装 `pdsh` 软件包。

**权限被拒绝**：确保为 root 或您的部署用户设置了 SSH 密钥身份验证。

**节点未出现在 `cinfo` 中**：检查防火墙设置，并确保允许在所需端口（10010-10013）上进行节点间通信。

**服务无法启动**：使用 `journalctl -u cranectld` 或 `journalctl -u craned` 检查日志。
