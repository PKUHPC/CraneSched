# 容器功能部署

本指南说明如何在鹤思集群中启用和配置容器功能，允许用户通过 CRI（容器运行时接口）在集群中运行容器化作业。

## 环境准备

### 容器运行时

鹤思通过 CRI 接口与容器运行时交互，支持 **containerd** 和 **CRI-O** 等兼容 CRI 的运行时。

#### 安装容器运行时

=== "containerd（推荐）"

    我们推荐您参阅 [containerd 官方安装指南](https://github.com/containerd/containerd/blob/main/docs/getting-started.md) 以获取最新的安装步骤。

    部分 Linux 发行版可通过包管理器直接安装 containerd，但可能版本较旧，以下命令仅供参考：

    ```bash
    # Rocky Linux 9 / RHEL 9
    dnf install -y containerd
    systemctl enable --now containerd

    # Debian / Ubuntu
    apt install -y containerd
    systemctl enable --now containerd
    ```

=== "CRI-O"

    我们推荐您参阅 [CRI-O 官方安装指南](https://github.com/cri-o/cri-o/blob/main/install.md) 以获取最新的安装步骤。

    部分 Linux 发行版可通过包管理器直接安装 CRI-O，但可能版本较旧，以下命令仅供参考：

    ```bash
    # Rocky Linux 9 / RHEL 9
    dnf install -y cri-o
    systemctl enable --now crio

    # Debian / Ubuntu
    apt install -y cri-o
    systemctl enable --now crio
    ```

#### 启用 CRI 支持

容器运行时通过 UNIX 套接字暴露接口，大部分容器运行时在默认配置下已经启用 CRI 服务：

| 运行时 | 默认套接字路径 |
|:-------|:---------------|
| containerd | `/run/containerd/containerd.sock` |
| CRI-O | `/run/crio/crio.sock` |

使用 crictl 工具检查 CRI 接口状态：

=== "containerd"

    ```bash
    crictl --runtime-endpoint unix:///run/containerd/containerd.sock version
    ```

=== "CRI-O"

    ```bash
    crictl --runtime-endpoint unix:///run/crio/crio.sock version
    ```

预期输出（以 containerd 为例）：

```
Version:  0.1.0
RuntimeName:  containerd
RuntimeVersion:  2.0.7
RuntimeApiVersion:  v1
```

如果出现错误，请按容器运行时的配置文档启用 CRI 支持。

#### 启用远程连接

鹤思系统允许用户在集群任意节点上连接到本用户正在运行的容器作业。因此，需配置容器运行时允许来自本集群其他节点的远程连接。

!!! warning
    启用远程连接可能带来安全风险。建议通过防火墙和 TLS 证书限制访问。

=== "containerd"
    生成 Containerd 默认配置（如当前无配置）：
   
    ```bash
    containerd config default
    ```

    找到 `/etc/containerd/config.toml` 中的以下部分：
    
    ```toml
    [plugins.'io.containerd.grpc.v1.cri']
      disable_tcp_service = false       # 启用 TCP 服务
      stream_server_address = '0.0.0.0' # 修改为集群内可访问的地址
      enable_tls_streaming = false      # 根据需要启用 TLS

    # 根据情况配置 TLS 证书路径
    [plugins.'io.containerd.grpc.v1.cri'.x509_key_pair_streaming]
      tls_cert_file = ''
      tls_key_file = ''
    ```

    配置完毕后，请重启 containerd 服务。
    
    请参考 [Containerd 配置指南](https://github.com/containerd/containerd/blob/main/docs/cri/config.md) 获取更多细节。

=== "CRI-O"
    此部分内容待补充。


#### 启用 GPU 支持

!!! note
    仅在集群节点配置 GPU/NPU 资源时，才需启用此功能。该功能依赖容器运行时和来自硬件厂商的插件支持。

=== "NVIDIA GPU"

    请参阅 [NVIDIA Container Toolkit 安装指南](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html) 安装 NVIDIA Container Toolkit。
    
    安装完成后，请参考上述文档中 “Configuring containerd” 或 “Configuring CRI-O” 部分启用 GPU 支持。

=== "华为昇腾 NPU"
    
    请参阅 [MindCluster - 容器化支持特性指南](https://www.hiascend.com/document/detail/zh/mindcluster/72rc1/clustersched/dlug/dlruntime_ug_002.html) 安装昇腾 NPU 容器运行时插件。

    安装完成后，请参考 [在 Containerd 客户端中使用](https://www.hiascend.com/document/detail/zh/mindcluster/72rc1/clustersched/dlug/dlruntime_ug_006.html) 教程启用 NPU 支持。


=== "AMD GPU"

    请参阅 [AMD Container Toolkit 安装指南](https://instinct.docs.amd.com/projects/container-toolkit/en/latest/index.html) 安装 AMD Container Toolkit。

    **注意：** 鹤思对 AMD GPU 的容器支持情况尚在测试和评估中。

### 容器网络插件

鹤思系统通过调用容器网络接口（CNI），实现容器网络隔离与通信。请根据所选容器运行时安装恰当的 CNI 插件。

鹤思提供 Crane Meta CNI 插件，用于灵活地适配现有的 CNI 插件（如 Calico）。目前通过兼容性测试的 CNI 插件有：Calico。

#### 鹤思 Meta CNI

在 CraneSched-FrontEnd 根目录下，执行 make tool 可构建 Meta CNI 插件，编译后的产物在：build/tool/meta-cni。
Meta CNI 需要经过配置才能正常使用。

!!! warning
    鹤思 Meta CNI 本身不进行网络配置，仅作为桥梁连接鹤思系统与实际的 CNI 插件。不同的集群、不同的 CNI 插件，需要不同的 Meta CNI 配置。

示例配置文件位于 tool/meta-cni/config/00-meta.example.conf，请根据实际情况进行编写。

填写配置文件后，将其放置到 /etc/cni/net.d/ 中（具体位置由容器运行时决定，请保持路径一致）。此目录下可能同时存在多个配置文件，字典序靠前的配置文件优先生效。

#### 案例：Calico

!!! warning
    此部分文档尚在完善中，后续将补充更多细节。

**[Calico](https://github.com/projectcalico/calico)** 是一个流行的容器网络插件，支持网络策略和高性能网络通信。鹤思已完成对 Calico 的兼容性测试。

以下是 Meta CNI + Calico + Port Mapping + Bandwidth 配置，请将其写入 `/etc/cni/net.d/00-crane-calico.conf`。如没有更优先的配置文件，该配置将在下一次启动容器时生效。

??? "示例配置"
      ```json
      {
      "cniVersion": "1.0.0",
      "name": "crane-meta",
      "type": "meta-cni",
      "logLevel": "debug",
      "timeoutSeconds": 10,
      "resultMode": "chained",
      "runtimeOverride": {
         "args": [
            "-K8S_POD_NAMESPACE",
            "-K8S_POD_NAME",
            "-K8S_POD_INFRA_CONTAINER_ID",
            "-K8S_POD_UID"
         ],
         "envs": []
      },
      "delegates": [
         {
            "name": "calico",
            "conf": {
            "type": "calico",
            "log_level": "info",
            "datastore_type": "etcdv3",
            "etcd_endpoints": "http://192.168.24.2:2379",
            "etcd_key_file": "",
            "etcd_cert_file": "",
            "etcd_ca_cert_file": "",
            "ipam": {
               "type": "calico-ipam"
            },
            "policy": {
               "type": "none"
            },
            "container_settings": {
               "allow_ip_forwarding": true
            },
            "capabilities": {
               "portMappings": true
            }
            }
         },
         {
            "name": "portmap",
            "conf": {
            "type": "portmap",
            "snat": true,
            "capabilities": {
               "portMappings": true
            }
            }
         },
         {
            "name": "bandwidth",
            "conf": {
            "type": "bandwidth",
            "capabilities": {
               "bandwidth": true
            }
            }
         }
      ]
      }
      ```
    该配置文件中部分内容和实际集群部署相关，请根据实际情况调整。

### 可选依赖

#### BindFs

!!! note
    仅在需要使用用户命名空间（`--userns`, 即 “Fake Root”）运行容器，且欲挂载目录的底层文件系统不支持 ID Mapped Mounts 时，才需要安装 BindFs。

!!! warning
    由于 BindFs 基于 FUSE，可引入性能开销，建议仅在必要时启用。

[BindFs](https://bindfs.org/) 是一个基于 FUSE 的文件系统工具，用于在不支持 ID Mapped Mounts 的文件系统上实现真实用户 ID 和容器内用户 ID 的映射。可按照以下命令安装：

=== "RHEL/Fedora/CentOS"

    ```bash
    dnf install -y bindfs fuse3
    ```

=== "Debian / Ubuntu"
   
    ```bash
    apt install -y bindfs fuse3
    ```

完成安装后，请参照下文的[容器配置说明](#容器配置说明)部分启用 BindFs 功能。

## 部署步骤

### 修改配置文件

编辑 `/etc/crane/config.yaml`，添加以下容器配置：

!!! note
    完整的容器配置选项请参见[容器配置说明](#容器配置说明)。

```yaml
Container:
  Enabled: true
  TempDir: supervisor/containers/
  RuntimeEndpoint: /run/containerd/containerd.sock
  ImageEndpoint: /run/containerd/containerd.sock
```

修改配置文件后，保存并将其分发至各节点。

### 重启鹤思服务

重启在所有节点上的鹤思服务以应用新配置：

```bash
pdsh -w crane01 "systemctl restart cranectld"
pdsh -w crane[02-04] "systemctl restart craned"
```

### 验证容器功能

在任意可提交作业的节点上运行测试容器：

```bash
# 提交简单容器作业
ccon run -p CPU alpine:latest -- echo "Hello from container"

# 查看作业状态
ccon ps -a
```

## 容器配置说明

在 `/etc/crane/config.yaml` 中配置容器相关选项。以下是完整配置示例及字段说明：

```yaml
Container:
  # 启用容器功能
  Enabled: true
  
  # 容器临时数据目录（相对于 CraneBaseDir）
  TempDir: supervisor/containers/
  
  # CRI 运行时服务套接字
  RuntimeEndpoint: /run/containerd/containerd.sock
  
  # CRI 镜像服务套接字（通常与 RuntimeEndpoint 相同）
  ImageEndpoint: /run/containerd/containerd.sock
  
  # BindFs 配置（可选，用于用户命名空间映射）
  BindFs:
    Enabled: false
    BindfsBinary: /usr/bin/bindfs
    FusermountBinary: /usr/bin/fusermount3
    MountBaseDir: /mnt/crane
```

### 核心配置

| 字段 | 类型 | 默认值 | 说明 |
|:-----|:-----|:-------|:-----|
| `Enabled` | bool | `false` | 是否启用容器功能。设为 `true` 以启用 |
| `TempDir` | string | `supervisor/containers/` | 容器运行期间的临时数据目录，相对于 `CraneBaseDir`。存储容器元数据、日志等 |
| `RuntimeEndpoint` | string | — | **必填**。CRI 运行时服务的 Unix 套接字路径，用于容器生命周期管理（创建、启动、停止等） |
| `ImageEndpoint` | string | 同 `RuntimeEndpoint` | CRI 镜像服务的 Unix 套接字路径，用于镜像拉取与管理。大多数情况下与 `RuntimeEndpoint` 相同 |

### BindFs 配置

BindFs 用于实现宿主机目录到容器内的用户 ID 映射挂载，解决用户命名空间下的权限问题。

| 字段 | 类型 | 默认值 | 说明 |
|:-----|:-----|:-------|:-----|
| `BindFs.Enabled` | bool | `false` | 是否启用 BindFs 功能 |
| `BindFs.BindfsBinary` | string | `bindfs` | bindfs 可执行文件路径 |
| `BindFs.FusermountBinary` | string | `fusermount3` | fusermount 可执行文件路径（用于卸载 FUSE 文件系统） |
| `BindFs.MountBaseDir` | string | `/mnt/crane` | BindFs 挂载点的基础目录。可以是绝对路径，或相对于 `CraneBaseDir` 的相对路径 |

## 镜像管理

鹤思系统并不直接管理镜像存储，容器运行时负责镜像的拉取与存储。

容器镜像可从以下位置获取：

- **公共仓库**：Docker Hub、GHCR、Quay.io 等
- **私有仓库**：企业内部 Registry
- **本地镜像**：通过 `ctr` 或 `crictl` 预先导入

建议您在集群中部署恰当的镜像加速器或私有 Registry 以提升拉取速度。

## 故障排除

请参阅[容器故障排除指南](../reference/container/troubleshooting.md)获取常见问题的解决方案。

## 相关文档

- [容器功能概览](../reference/container/index.md)：了解容器功能的整体定位与优势
- [核心概念](../reference/container/concepts.md)：理解容器作业、Pod、资源模型等概念
- [快速上手](../reference/container/quickstart.md)：快速体验容器作业提交
- [集群配置](./configuration/config.md)：完整的配置文件说明
