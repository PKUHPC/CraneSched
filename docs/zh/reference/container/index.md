# 鹤思容器功能

鹤思容器功能在传统的基于进程的作业之外，为集群用户提供容器化运行环境，以“容器作业 + 容器作业步”为核心模型，允许在同一作业内复用资源、提交容器作业步，并与传统批处理/交互式作业步协同运行。

!!! note "功能定位"
    鹤思系统 **不基于** Kubernetes，而是独立实现的 HPC + AI 混合调度系统。鹤思容器功能聚焦于“批处理与计算型作业调度”，区别于面向“服务型编排与自治”的 Kubernetes。

## 特性一览

<div class="grid cards" markdown>

- :material-cube-outline: **容器化运行**

    ---

    容器镜像与命令统一封装，环境一致、可复现。

- :material-sitemap: **统一调度**

    ---

    沿用分区、账户、QoS、预留等调度策略。

- :material-layers-triple: **作业内复用**

    ---

    容器作业启动 Pod 后，可追加多个容器作业步。

- :material-transition: **混合作业步**

    ---

    同一作业内可同时使用批处理脚本、容器作业步、交互式作业步。

- :material-console-line: **运行期交互**

    ---

    通过 Attach/Exec 进入容器作业步进行调试与排障。

- :material-shield-account: **安全隔离**

    ---

    使用 UID Mapping/ Idmapped Mount 功能为普通用户提供安全的 Fake Root 体验。

</div>

## 基本概念

- **容器作业 (Container Job)**：分配资源、创建并维持一个 Pod，承载后续提交的容器作业步。但容器作业内也可包含非容器作业步（如批处理脚本、交互式作业步）。
- **容器作业步 (Container Step)**：在容器作业内追加的实际执行单元，对应于 Pod 中的至少一个容器。每个容器作业步可指定独立的镜像、命令、环境变量和挂载等配置。
- **Pod 元数据（Pod Metadata）**：作业级容器配置（如 DNS、端口等），在提交容器作业时创建，用于定义 Pod 的整体属性。
- **容器元数据（Container Metadata）**：作业步级容器配置（如命令、环境变量、挂载等），在提交容器作业步时指定，用于定义容器的具体行为。

## 功能入口

容器功能可通过 ccon 命令和 cbatch 命令使用。具体使用方法请参考 [ccon 命令手册](../../command/ccon.md) 和 [cbatch 命令手册](../../command/cbatch.md)。


=== "cbatch --pod"
    以批处理脚本为入口创建容器作业。作业开始时，自动在分配的节点上启动 Pod。脚本作为 Primary Step 运行，可追加容器作业步，进行复杂的容器编排，并可与非容器的作业步混用，提供类似 Slurm 的批处理脚本体验。

=== "ccon"
    直接以容器作业步作为 Primary Step 创建容器作业，适合只包含容器作业步的简单作业，提供类似 Docker/Kubernetes 的命令行体验。

## 相关文档

- [核心概念](concepts.md)
- [快速上手](quickstart.md)
- [使用示例](examples.md)
- [运行期操作与排错](troubleshooting.md)
- [ccon 命令手册](../../command/ccon.md)
- [容器功能部署](../../deployment/container.md)
