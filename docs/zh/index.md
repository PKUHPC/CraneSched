---
hide:
  - navigation
---

# 鹤思

<p class="lead">面向高性能计算和人工智能工作负载的分布式调度系统 — 专为性能、规模和简单性而构建。</p>

[快速开始](deployment/index.md){ .md-button .md-button--primary } [体验演示](https://hpc.pku.edu.cn/demo/cranesched){ .md-button } [GitHub](https://github.com/PKUHPC/CraneSched){ .md-button }

---

## 为什么选择鹤思?

<div class="grid cards" markdown>

- :material-speedometer: **高性能**

    ---

    每秒超过 10 万次调度决策，快速的作业资源匹配。

- :material-arrow-expand: **可扩展性**

    ---

    经过验证的设计，支持百万核心集群和大规模部署。

- :material-account-cog: **易用性**

    ---

    为用户和管理员提供简洁一致的命令行界面（cbatch、cqueue、crun、calloc、cinfo 等）。

- :material-shield-lock: **安全性**

    ---

    内置基于角色的访问控制（RBAC）和加密通信。

- :material-heart-pulse: **弹性**

    ---

    自动作业恢复，无单点故障，快速状态恢复。

- :material-source-repository: **开源**

    ---

    社区驱动，具有可插拔架构的可扩展性。

</div>

---

## 快速开始

<div class="grid cards" markdown>

- :material-rocket-launch: **部署后端**（Rocky Linux 9）

    ---

    生产环境推荐。

    [打开指南 →](deployment/backend/Rocky9.md)

- :material-cog: **配置集群**

    ---

    配置数据库、分区、节点和策略。

    [数据库](deployment/configuration/database.md) • [配置](deployment/configuration/config.md)

- :material-code-tags: **部署前端**

    ---

    用户工具和服务（CLI、cfored、cplugind）。

    [打开指南 →](deployment/frontend/frontend.md)

- :material-console-line: **运行您的第一个作业**

    ---

    批处理：[cbatch](command/cbatch.md) • 交互式：[crun](command/crun.md)、[calloc](command/calloc.md)

</div>

---

## 架构

![鹤思架构](images/Architecture.png)

鹤思引入了 Resource Manager 以同时支持 HPC 和 AI 工作负载:

- HPC 作业：Cgroup Manager 分配资源并提供基于 cgroup 的隔离。
- AI 作业：Container Manager 使用 Kubernetes 进行资源分配和容器生命周期管理。

---

## 命令行参考

- 用户命令：[cbatch](command/cbatch.md)、[cqueue](command/cqueue.md)、[crun](command/crun.md)、[calloc](command/calloc.md)、[cinfo](command/cinfo.md)
- 管理员命令：[cacct](command/cacct.md)、[cacctmgr](command/cacctmgr.md)、[ceff](command/ceff.md)、[ccontrol](command/ccontrol.md)、[ccancel](command/ccancel.md)
- 退出代码：[参考](reference/exit_code.md)

---

## 链接

- 演示集群：<https://hpc.pku.edu.cn/demo/cranesched>
- 后端：<https://github.com/PKUHPC/CraneSched>
- 前端：<https://github.com/PKUHPC/CraneSched-FrontEnd>

---

## 许可证

鹤思采用 AGPLv3 和商业许可证双重许可。有关商业许可，请查看 `LICENSE` 文件或联系 mayinping@pku.edu.cn。
