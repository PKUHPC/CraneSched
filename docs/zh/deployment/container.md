# 容器功能部署

本指南说明如何在鹤思集群中启用和配置容器功能，允许用户通过 CRI（容器运行时接口）在集群中运行容器化作业。

## 环境准备

### 容器运行时

鹤思通过 CRI 接口与容器运行时交互，支持 **containerd** 和 **CRI-O** 等兼容 CRI 的运行时。

!!! warning "版本要求"
    容器运行时必须支持 **CRI v1 API**。对于 containerd，要求版本 **≥ 1.7**（推荐 2.0+）。

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
    
    除此之外，您也可以使用鹤思的 “容器设备接口” 功能将 GPU/NPU/RDMA 网卡 等各种类型的设备注入容器。

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

为了灵活地适配现有的 CNI 插件，鹤思提供了一组 **鹤思 CNI 套件**，目前包括：

- **Meta CNI**：连接鹤思系统与其他 CNI 插件的桥梁，负责传递作业信息给 CNI 插件。
- **DNS CNI (dns-register)**：在容器创建时向 CoreDNS 注册容器的 FQDN 和 IP，实现集群内容器通过 hostname 互相访问。

在实际部署中，您可以根据需求选择兼容的第三方 CNI 插件，搭配鹤思 CNI 套件实现容器网络功能。

当前通过兼容性测试的第三方 CNI 插件有：**Calico**。因此，我们推荐您按照以下教程部署 Meta CNI + DNS CNI + Calico 实现鹤思容器网络功能。

#### 鹤思 CNI 套件

在 CraneSched-FrontEnd 根目录下，执行 `make tool` 可构建鹤思 CNI 套件中的插件，编译后的产物在 `build/tool/` 下。

以下是鹤思 CNI 套件中各组件的功能说明：

=== "Meta CNI"
    鹤思 Meta CNI 本身不进行网络配置，仅作为桥梁连接鹤思系统与实际的 CNI 插件。Meta CNI 是鹤思系统中所有 CNI 的统一入口，其他 CNI 插件由 Meta CNI 负责编排、调用。不同的集群、不同的插件，需要不同的 Meta CNI 配置。

    请参阅 [Meta CNI README](https://github.com/PKUHPC/CraneSched-FrontEnd/blob/master/tool/meta-cni/README.md) 了解完整的配置说明与示例。

=== "DNS CNI"
    鹤思 DNS CNI 插件（dns-register）负责在容器创建时，将容器的 FQDN 和 IP 注册到 etcd，供 CoreDNS 查询。

    使用 dns-register 前，您需要部署 [CoreDNS + etcd 插件](https://coredns.io/plugins/etcd/)，并确保 CoreDNS Corefile 中的 etcd `path` 设置为 `/coredns`（dns-register 写入记录的前缀）。

    请参阅 [dns-register README](https://github.com/PKUHPC/CraneSched-FrontEnd/blob/master/tool/dns-register/README.md) 了解完整的配置说明。

#### 示例：Calico

!!! warning
    此部分文档尚在完善中，后续将补充更多细节。

**[Calico](https://github.com/projectcalico/calico)** 是一个流行的容器网络插件，支持网络策略和高性能网络通信。鹤思已完成对 Calico 的兼容性测试。

以下是 Meta CNI + Calico + dns-register + Port Mapping + Bandwidth 配置，请将其写入 `/etc/cni/net.d/00-crane-calico.conf`。如没有更优先的配置文件，该配置将在下一次启动容器时生效。

??? "示例配置"
    参见 [实例配置文件](https://github.com/PKUHPC/CraneSched-FrontEnd/blob/master/tool/meta-cni/config/00-crane-calico.conf)。

    该配置文件中部分内容和实际集群部署相关，请根据实际情况调整。

#### 示例：RDMA VF 网络

对于 RDMA VF 这类既需要设备注入，又需要网络接口配置的资源，需要将
**CDI** 与 **Meta CNI** 配合使用：

- CDI 负责把 `uverbs`、`rdma_cm` 等设备节点注入容器
- Meta CNI 负责把分配到的 VF 网卡移动到容器网络命名空间，并创建 `rdma0`、`rdma1` 等接口

请参考下文 CDI 相关内容，配置好
`DeviceCDIRegex` 或 `DeviceCDIList`。然后在 `/etc/crane/config.yaml` 中补充
`DeviceCniPipeline` 字段，并提供对应的 Meta CNI 配置。

??? "GRES 配置示例（/etc/crane/config.yaml）"
    下面的示例是在“配置 GRES 使用 CDI”基础上增加了 `DeviceCniPipeline: rdma`
    字段后的写法：

    ```yaml
    Nodes:
      - name: "node01"
        gres:
          - name: rdma
            type: cx5vf
            DeviceCniPipeline: rdma
            DeviceFileList:
              - /dev/infiniband/uverbs3,/dev/infiniband/umad3,/dev/infiniband/rdma_cm
              - /dev/infiniband/uverbs4,/dev/infiniband/umad4,/dev/infiniband/rdma_cm
            DeviceCDIList:
              - rdma/pci-0000-17-00-2=0000:17:00.2
              - rdma/pci-0000-17-00-3=0000:17:00.3
    ```

    这里各字段的作用分别是：

    - `DeviceFileList`：声明需要注入容器的 RDMA 设备文件
    - `DeviceCDIList`：为这些设备文件提供 CDI 完全限定名称
    - `DeviceCniPipeline`：声明这些 GRES 设备还需要交给 Meta CNI 进行网络配置

    `DeviceCniPipeline` 的值必须与 Meta CNI 配置中对应网络管线的 `name` 保持一致。

??? "Meta CNI 配置示例"
    RDMA VF 网络示例配置可参考：
    [tool/meta-cni/config/00-crane-calico-sriov.conf](https://github.com/PKUHPC/CraneSched-FrontEnd/blob/master/tool/meta-cni/config/00-crane-calico-sriov.conf)。

    该示例中包含两类网络：

    - `ethernet`：容器的基础网络接口，一般用于普通 IP 通信
    - `rdma`：RDMA VF 专用网络接口，按作业实际分配的 GRES 数量动态创建

    其中与 RDMA VF 相关的关键点如下：

    - `name: rdma`：必须与 `DeviceCniPipeline: rdma` 一致
    - `ifNamePrefix: rdma`：生成的容器内接口名形如 `rdma0`、`rdma1`
    - `confFromArgs: {"deviceID": "$gres.device"}`：把 GRES 对应的 CDI 设备标识传给 `sriov` 插件

如果您只需要将 RDMA 设备文件注入容器，而不需要在容器内创建独立 RDMA 网络接口，
则无需配置 `DeviceCniPipeline`。

### 容器设备接口

除了上述厂商提供的运行时 Hook 方式，鹤思还支持通过 **容器设备接口（Container Device Interface, CDI）** 标准将设备注入容器。CDI 是一种与厂商无关的设备注入规范，使用 JSON 规范文件（CDI Spec）描述设备及其注入所需的设备节点、挂载和环境变量。特别适用于以下场景：

- RDMA/InfiniBand 等非 GPU 设备
- 一个物理设备对应多个设备文件节点的场景
- 需要向容器注入额外挂载或环境变量的设备

容器运行时在创建容器时读取 CDI Spec，按照规范将设备自动注入容器。鹤思在提交容器作业时，会将分配到的设备对应的 CDI 完全限定名称（Fully Qualified Device Name）通过 CRI 接口传递给容器运行时，运行时据此完成设备注入。

#### 启用 CDI 支持

=== "containerd"

    编辑 `/etc/containerd/config.toml`，启用 CDI 并配置 CDI Spec 目录：

    ```toml
    [plugins.'io.containerd.grpc.v1.cri']
      enable_cdi = true
      cdi_spec_dirs = ["/etc/cdi", "/var/run/cdi"]
    ```

    配置完毕后，重启 containerd 服务：

    ```bash
    systemctl restart containerd
    ```

=== "CRI-O"

    CRI-O 自 v1.23 起默认启用 CDI 支持，无需额外配置。CDI Spec 默认搜索 `/etc/cdi` 和 `/var/run/cdi` 目录。

#### 生成 CDI 配置

CDI Spec 文件为 JSON 格式，放置在 `/etc/cdi/` 目录下。不同硬件厂商有不同的生成工具。例如：

| 设备类型 | 生成工具 | 参考文档 |
|:--------|:--------|:--------|
| NVIDIA GPU | `nvidia-ctk cdi generate` | [NVIDIA CDI 文档](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/cdi-support.html) |
| RDMA/InfiniBand | `rdma-cdi` 或手动编写 | [RDMA 网卡 CDI 生成工具](https://github.com/Nativu5/rdma-cdi) |

使用工具生成配置后，可使用以下命令验证：

```bash
# 直接查看 spec 文件目录
ls /etc/cdi/
```

#### 配置 GRES 使用 CDI

在 `/etc/crane/config.yaml` 的 Gres 配置中，通过 `DeviceCDIRegex` 或 `DeviceCDIList` 字段为每个设备指定 CDI 完全限定名称。CDI 名称与设备文件（DeviceFileRegex / DeviceFileList）按顺序一一对应。

??? "示例配置"
    ```yaml
    Nodes:
    - name: "node01"
        cpu: 64
        memory: 256G
        gres:
        - name: rdma
            type: cx5vf
            DeviceFileList:
            - /dev/infiniband/uverbs3,/dev/infiniband/rdma_cm
            - /dev/infiniband/uverbs4,/dev/infiniband/rdma_cm
            DeviceCDIList:
            - rdma/mlx5_3=0000:17:00.2
            - rdma/mlx5_4=0000:17:00.3
    ```

完整的 Gres 配置说明请参见 [集群配置 - Gres 配置](./configuration/config.md#gres配置)。


### 高级特性

鹤思容器功能具备一些高级特性，需要额外配置：

- **Fake Root (User Namespace)**: 通过用户命名空间实现容器内“假 root”权限隔离，需配置 **SubID**。如需挂载目录，且底层文件系统不支持 ID Mapped Mounts，则需安装 **BindFs**。

#### SubID

!!! note
    鹤思系统支持自动管理 SubUID/SubGID 范围（“Managed” 模式）。如需自行管理 SubID，请按以下说明操作。

“Managed” 模式下，鹤思通过 `BaseOffset + ID × RangeSize` 计算每个用户的 SubUID/SubGID 范围起始值，并维护 `/etc/subuid` 等系统文件。若该模式无法满足需求，可手动管理或使用外部服务。

“Unmanaged” 模式下，鹤思仅通过 `shadow-utils` 的 API 获取系统中的 SubID 信息。因此，管理员必须确保集群内所有节点上的用户具有一致的、不冲突的 SubID 范围。管理员可以手动填写 `/etc/subuid` 和 `/etc/subgid` 文件，或通过 LDAP 服务器集中管理。

部分系统可通过 sssd 服务从 LDAP 服务器获取 SubID 信息。请参考 FreeIPA、OpenLDAP 等解决方案的文档进行配置。

要切换 SubID 管理模式，或对 Managed 模式的参数进行调整，请参见下文的[容器配置说明](#容器配置说明)部分。

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
  Dns:
    ClusterDomain: "cluster.local"
    Servers: ["127.0.0.1"]
    Searches: []
    Options: []
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

  # DNS 配置
  Dns:
    ClusterDomain: "cluster.local"
    Servers: ["127.0.0.1"]
    Searches: []
    Options: []

  # SubUID/SubGID 配置
  SubId:
    # 是否自动管理 SubID 范围
    Managed: true
    # 每个用户的 SubUID/SubGID 范围大小
    RangeSize: 65536
    # SubUID/SubGID 范围的基础偏移量
    BaseOffset: 100000
  
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

### DNS 配置

容器 DNS 用于向用户提供集群内容器的域名解析服务。鹤思会为每个容器生成独一的 hostname，需要容器 DNS 才能在其他容器中解析该 hostname。

| 字段 | 类型 | 默认值 | 说明 |
|:-----|:-----|:-------|:-----|
| `Dns.ClusterDomain` | string | `cluster.local` | 集群域名。会作为**首位**搜索域追加到 `searches` |
| `Dns.Servers` | string[] | `['127.0.0.1']` | DNS 服务器列表（仅支持 IPv4） |
| `Dns.Searches` | string[] | `[]` | 额外搜索域列表 |
| `Dns.Options` | string[] | `[]` | DNS 选项（如 `ndots:5`） |

### SubID 配置

SubID（从属用户/组 ID）配置用于容器用户命名空间的安全隔离。

| 字段 | 类型 | 默认值 | 说明 |
|:-----|:-----|:-------|:-----|
| `Managed` | bool | `true` | 是否由鹤思自动管理 SubID 范围。<br>- `true`：自动添加和验证 SubID 范围<br>- `false`：管理员自行配置 |
| `RangeSize` | int | `65536` | 每个用户的 SubUID/SubGID 范围大小。必须大于 0，建议值为 65536 |
| `BaseOffset` | int | `100000` | SubID 范围的基础偏移量。用于计算每个用户的范围：`start = BaseOffset + uid × RangeSize` |

### BindFs 配置

BindFs 用于实现宿主机目录到容器内的用户 ID 映射挂载，解决用户命名空间下的权限问题。

| 字段 | 类型 | 默认值 | 说明 |
|:-----|:-----|:-------|:-----|
| `Enabled` | bool | `false` | 是否启用 BindFs 功能 |
| `BindfsBinary` | string | `bindfs` | bindfs 可执行文件路径 |
| `FusermountBinary` | string | `fusermount3` | fusermount 可执行文件路径（用于卸载 FUSE 文件系统） |
| `MountBaseDir` | string | `/mnt/crane` | BindFs 挂载点的基础目录。可以是绝对路径，或相对于 `CraneBaseDir` 的相对路径 |

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
