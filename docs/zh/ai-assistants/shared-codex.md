# 集群共享 Codex

[CraneSched-Codex](https://github.com/PKUHPC/CraneSched-Codex) 供管理员在共享登录节点上
提供开箱即用的 Codex。RPM 包含固定版本的 Codex、鹤思 Skill、系统默认配置和本机
loopback proxy；真实上游 endpoint 与凭据由管理员在安装后配置，不进入 RPM 或 Git。

这是一项可选服务。只想在个人 AI 助手中使用鹤思知识的用户，安装
[鹤思 Skill](skill.md) 即可。

## 适用范围

当前 RPM 面向 x86_64 EL9（Rocky Linux、AlmaLinux 或 RHEL 9）共享节点。它提供：

- 用户无需配置共享 API Key 即可运行 `codex` 的 Managed Default；
- 由 root-only proxy 代持的管理员凭据；
- 系统级鹤思 Skill 和只读 CraneSched 命令默认规则；
- 用户通过自己的 provider 和凭据覆盖默认配置的 BYOK 能力。

它不提供多用户认证、按用户限额或用量统计。任何能访问本机 proxy 的用户都可能
消耗共享额度，管理员必须在上游和站点侧另行治理。

## 管理员配置

从 [最新 Release](https://github.com/PKUHPC/CraneSched-Codex/releases/latest) 下载 RPM，
在目标节点安装：

```bash
sudo dnf install ./cranesched-codex-*.el9.x86_64.rpm
```

创建 `/etc/codex/proxy-upstream.conf`，保持 `root:root`、权限 `0600`，并用
`sudoedit` 填写两行内容：完整的 Responses endpoint 和 bearer token。不要把 token
放入命令行、环境变量、日志、Issue 或 PR。

```bash
sudo install -d -m 0755 -o root -g root /etc/codex
sudo install -m 0600 -o root -g root /dev/null \
  /etc/codex/proxy-upstream.conf
sudoedit /etc/codex/proxy-upstream.conf
sudo chown root:root /etc/codex/proxy-upstream.conf
sudo chmod 0600 /etc/codex/proxy-upstream.conf
sudo systemctl enable --now cranesched-codex-proxy.service
```

检查安装、服务和仅监听本机的端口：

```bash
rpm -q cranesched-codex
codex --version
sudo systemctl is-active cranesched-codex-proxy.service
sudo ss -ltnp '( sport = :617 )'
codex --strict-config doctor --json
```

`ss` 应只显示 `127.0.0.1:617`。完成最小真实请求验证后，普通用户可直接运行
`codex`，也可以用自己的 BYOK provider 覆盖系统默认值。用户操作方式参见
[使用集群共享 Codex](shared-codex-user-guide.md)。

配置文件格式、升级、轮换、回滚、故障排查和卸载步骤以
[完整安装与配置指南](https://github.com/PKUHPC/CraneSched-Codex/blob/main/docs/installation-and-configuration.md)
为准；部署前还应阅读[架构与安全边界](https://github.com/PKUHPC/CraneSched-Codex/blob/main/docs/architecture-and-security.md)。
