# Shared Cluster Codex

[CraneSched-Codex](https://github.com/PKUHPC/CraneSched-Codex) lets administrators provide a
ready-to-use Codex on shared login nodes. Its RPM contains a pinned Codex, the CraneSched Skill,
system defaults, and a loopback proxy. The administrator configures the real upstream endpoint and
credential after installation; neither is stored in the RPM or Git.

This service is optional. Users who only want CraneSched guidance in a personal AI assistant can
install the [CraneSched Skill](skill.md) instead.

## Scope

The current RPM targets shared x86_64 EL9 nodes such as Rocky Linux, AlmaLinux, and RHEL 9. It provides:

- a Managed Default that lets users run `codex` without configuring the shared API key;
- a root-only proxy that holds the administrator's upstream credential;
- the system-wide CraneSched Skill and default rules for read-only CraneSched commands;
- BYOK, allowing users to override the default with their own provider and credential.

It does not provide multi-user authentication, per-user quotas, or usage accounting. Any local user
who can reach the proxy may consume the shared quota, so those controls must be implemented by the
site or upstream provider.

## Administrator setup

Download the RPM from the [latest Release](https://github.com/PKUHPC/CraneSched-Codex/releases/latest)
and install it on each target node:

```bash
sudo dnf install ./cranesched-codex-*.el9.x86_64.rpm
```

Create `/etc/codex/proxy-upstream.conf` as a `root:root`, mode `0600` file. Use `sudoedit` to enter
exactly two lines: the full Responses endpoint and its bearer token. Never place the token in command
arguments, environment variables, logs, Issues, or pull requests.

```bash
sudo install -d -m 0755 -o root -g root /etc/codex
sudo install -m 0600 -o root -g root /dev/null \
  /etc/codex/proxy-upstream.conf
sudoedit /etc/codex/proxy-upstream.conf
sudo chown root:root /etc/codex/proxy-upstream.conf
sudo chmod 0600 /etc/codex/proxy-upstream.conf
sudo systemctl enable --now cranesched-codex-proxy.service
```

Verify the package, service, and loopback-only listener:

```bash
rpm -q cranesched-codex
codex --version
sudo systemctl is-active cranesched-codex-proxy.service
sudo ss -ltnp '( sport = :617 )'
codex --strict-config doctor --json
```

`ss` should show only `127.0.0.1:617`. After a minimal real-request check, users can run `codex`
directly or override the system default with a personal BYOK provider. See
[Using Shared Codex](shared-codex-user-guide.md) for the user workflow.

Follow the
[full installation and configuration guide](https://github.com/PKUHPC/CraneSched-Codex/blob/main/docs/installation-and-configuration.md)
for the exact file format, upgrades, rotation, rollback, troubleshooting, and removal. Review the
[architecture and security boundaries](https://github.com/PKUHPC/CraneSched-Codex/blob/main/docs/architecture-and-security.md)
before production deployment.
