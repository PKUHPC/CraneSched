# PAM 模块配置

本指南说明如何配置鹤思 PAM 模块以控制用户对计算节点的访问。

!!! warning
    - 仅在**集群完全部署并运行后**配置 PAM。
    - **仔细测试配置** — 错误配置可能会导致您无法通过 SSH 访问。
    - 编辑 PAM 配置时，请始终保持备用 SSH 会话处于打开状态。

## 目的

PAM 模块（`pam_crane.so`）确保只有具有活动作业的用户才能登录到计算节点，防止对集群资源的未经授权访问。

## 安装步骤

### 1. 复制 PAM 模块

构建鹤思后，将 PAM 模块复制到系统库：

```bash
cp build/src/Misc/Pam/pam_crane.so /usr/lib64/security/
```

### 2. 编辑 PAM 配置

编辑 `/etc/pam.d/sshd` 并添加以下行：

- 在 `account include password-auth` **之前**添加 `account required pam_crane.so`
- 在 `session include password-auth` **之后**添加 `session required pam_crane.so`

**配置示例：**

```bash
#%PAM-1.0
auth       required     pam_sepermit.so
auth       substack     password-auth
auth       include      postlogin
# Used with polkit to reauthorize users in remote sessions
-auth      optional     pam_reauthorize.so prepare
account    required     pam_crane.so
account    required     pam_nologin.so
account    include      password-auth
password   include      password-auth
# pam_selinux.so close should be the first session rule
session    required     pam_selinux.so close
session    required     pam_loginuid.so
# pam_selinux.so open should only be followed by sessions to be executed in the user context
session    required     pam_selinux.so open env_params
session    required     pam_namespace.so
session    optional     pam_keyinit.so force revoke
session    include      password-auth
session    required     pam_crane.so
session    include      postlogin
# Used with polkit to reauthorize users in remote sessions
-session   optional     pam_reauthorize.so prepare
```

!!! warning
    行 `session required pam_crane.so` 必须放在 `session include password-auth` **之后**。

## 测试

1. **保持现有 SSH 会话处于打开状态**作为备份
2. 使用常规用户账户测试 SSH 登录
3. 验证用户只能访问他们有活动作业的节点

## 多节点部署

如果您有多个计算节点，请将 PAM 模块部署到所有节点：

```bash
# 使用 pdsh
pdcp -w crane[01-04] /usr/lib64/security/pam_crane.so /usr/lib64/security/

# 手动复制 PAM 配置或使用配置管理工具
```

## 故障排除

**SSH 被锁定**：启动进入单用户模式或使用控制台访问恢复 `/etc/pam.d/sshd`。

**PAM 模块未加载**：检查文件权限和 SELinux 上下文：
```bash
ls -lZ /usr/lib64/security/pam_crane.so
```

**用户有作业但无法登录**：验证 `craned` 正在运行并且可以与 `cranectld` 通信。
