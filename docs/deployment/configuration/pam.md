# PAM Module Configuration

This guide explains how to configure the CraneSched PAM module to control user access to compute nodes.

!!! warning
    - Only configure PAM **after the cluster is fully deployed and running**.
    - **Test the configuration carefully** — misconfiguration can lock you out of SSH access.
    - Always keep a backup SSH session open when editing PAM configuration.

## Purpose

The PAM module (`pam_crane.so`) ensures that only users with active jobs can log in to compute nodes, preventing unauthorized access to cluster resources.

## Installation Steps

### 1. Copy PAM Module

After building CraneSched, copy the PAM module to the system library:

```bash
cp build/src/Misc/Pam/pam_crane.so /usr/lib64/security/
```

### 2. Edit PAM Configuration

Edit `/etc/pam.d/sshd` and add the following lines:

- Add `account required pam_crane.so` **before** `account include password-auth`
- Add `session required pam_crane.so` **after** `session include password-auth`

**Example configuration:**

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
    The line `session required pam_crane.so` must be placed **after** `session include password-auth`.

## Testing

1. **Keep an existing SSH session open** as a backup
2. Test SSH login with a regular user account
3. Verify that users can only access nodes where they have active jobs

## Deployment to Multiple Nodes

If you have multiple compute nodes, deploy the PAM module to all of them:

```bash
# Using pdsh
pdcp -w crane[01-04] /usr/lib64/security/pam_crane.so /usr/lib64/security/

# Manually copy PAM configuration or use a configuration management tool
```

## Troubleshooting

**Locked out of SSH**: Boot into single-user mode or use console access to restore `/etc/pam.d/sshd`.

**PAM module not loading**: Check file permissions and SELinux context:
```bash
ls -lZ /usr/lib64/security/pam_crane.so
```

**Users can't log in with jobs**: Verify that `craned` is running and can communicate with `cranectld`.
