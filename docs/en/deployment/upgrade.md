# Upgrade from v1.1.3

This runbook is for control-plane administrators upgrading CraneSched v1.1.3
to the current release.

!!! warning
    The current release does not guarantee forward compatibility for pending
    or running jobs. Drain or cancel every active job before stopping
    CraneCtld. This procedure does not restore active runtime state from the
    v1.1.3 EmbeddedDB. See
    [issue #950](https://github.com/PKUHPC/CraneSched/issues/950) for the
    planned compatibility work.

## Preparation

Before the maintenance window, copy `scripts/upgrade_data.py` from the
target release source tree to the v1.1.3 control node, for example as
`/root/crane-upgrade/upgrade_data.py`. The script is not installed by the
CraneSched RPM/DEB. Install `python3-pyyaml` on RHEL-compatible systems or
`python3-yaml` on Debian-based systems. The control node must also provide
`mongosh`, `mongodump`, `mongorestore`, and `ccontrol`. The command blocks below
use Bash syntax and assume one root shell:

```bash
crane_upgrade_tool=/root/crane-upgrade/upgrade_data.py
test -r "$crane_upgrade_tool"
```

The tool reads `/etc/crane/config.yaml` and follows its `DbConfigPath` setting.
`DbName` is the MongoDB database name in that file; its v1.1.3 default is
`crane_db`. Administrators do not enter `DbHost`, `DbPort`, `DbUser`, `DbName`,
or `CraneCtldDbPath` on the command line.

A completed backup contains:

| Path | Contents |
| --- | --- |
| `etc-crane/` | Complete copy of `/etc/crane/` |
| `database.yaml.active` | Database configuration selected by `DbConfigPath` |
| `mongodb/` | `mongodump` of the configured database |
| `cranectld-runtime/` | Entire CraneCtld runtime directory containing `CraneCtldDbPath` |
| `manifest.json` | Resolved paths, MongoDB connection settings, maximum IDs, and next IDs |

Store the backup outside the CraneCtld runtime directory and retain it through
upgrade acceptance and the required backup-retention period.

## Upgrade SOP

### 1. Block submissions and drain jobs

Block `cbatch`, `crun`, `calloc`, `ccon`, and other submission entry points at
the login gateway or access-control layer. Keep the block in place until every
verification step in this runbook passes.

Cancel pending jobs and allow running jobs to finish:

```bash
ccancel -t Pending
cqueue -t all --json
```

If running jobs cannot finish during the maintenance window, cancel and check
them again:

```bash
ccancel -t Running
cqueue -t all --json
```

The `cqueue` response must contain an empty `job_info_list` before continuing.

### 2. Stop services

Stop services on their respective hosts. Stop `craned` on every compute node:

```bash
sudo systemctl stop cfored
sudo systemctl stop craned
sudo systemctl stop cranectld
sudo systemctl stop cplugind
systemctl is-active cfored cranectld cplugind || true
systemctl is-active craned || true
```

Each check must print `inactive`. The backup script also checks for a remaining
`cranectld` process. Keep `mongod` running because the backup tool must connect
to it.

### 3. Back up databases and record IDs

Run this in one shell on the v1.1.3 control node:

```bash
crane_backup_dir="/var/backups/crane/upgrade-$(date -u +%Y%m%dT%H%M%SZ)"
python3 "$crane_upgrade_tool" --output "$crane_backup_dir"
```

The output directory must not already exist. The tool creates it with mode
`0700` and refuses to run while `cranectld` is active. When MongoDB
authentication is enabled, `mongosh` and `mongodump` each prompt for the
database password.

The tool performs these operations:

1. Resolve MongoDB and EmbeddedDB locations from the default Crane configuration.
2. Query the maximum job ID and job database ID in historical data.
3. Back up configuration, the complete MongoDB database, and the entire
   CraneCtld runtime directory.
4. Record both independent maximum IDs and next safe IDs in `manifest.json`.

The script prints an `[INFO] Starting`/`[INFO] Completed` pair, or the resolved
result, for configuration loading, ID queries, configuration copies, the
MongoDB dump, runtime copy, and manifest write. A completed backup must print
`SUCCESS: Backup completed: <backup-directory>` and both maximum and next IDs.
A failure prints `error: <reason>` and leaves `BACKUP_INCOMPLETE`.

After backup completion, have the same script read and validate the backup:

```bash
python3 "$crane_upgrade_tool" \
  --validate-backup "$crane_backup_dir"
```

Successful validation must print `SUCCESS: Backup validated:` followed by the
backup directory, MongoDB database, CraneCtld runtime directory, and both next
IDs. `manifest.json` never contains the MongoDB
password. The script queries and records the IDs; administrators do not need
to understand the internal MongoDB layout, query IDs, or construct reset
commands manually.

### 4. Clear the old CraneCtld runtime directory

Do not parse the manifest or construct individual EmbeddedDB filenames. Have
the script validate the backup, confirm that `cranectld` is stopped, and clear
the runtime directory recorded in the manifest:

```bash
python3 "$crane_upgrade_tool" \
  --clear-runtime-from "$crane_backup_dir"
```

The script removes only the directory contents and preserves the directory,
owner, and permissions. It rejects unsafe broad paths, overlapping
configuration and runtime directories, incomplete backups, and a running
`cranectld`. Continue only after it prints `[INFO] Confirmed empty CraneCtld
runtime directory:` and `SUCCESS: CraneCtld runtime directory cleared:`, both
followed by the same path. Do not restore the old EmbeddedDB before starting
the upgraded release.

### 5. Install and start the release

Install the target packages or binaries while retaining the backed-up site
configuration. Start only the plugin service and CraneCtld first:

```bash
sudo systemctl daemon-reload
sudo systemctl start cplugind
sudo systemctl start cranectld
systemctl is-active cranectld
```

On its first start, CraneCtld automatically upgrades the historical job
database when required. Inspect the service state and the complete log from the
current boot:

```bash
systemctl is-active cranectld
journalctl -u cranectld -b --no-pager
```

`cranectld` must remain `active`, and the log must contain no database
initialization or upgrade error. Successful historical reads through `cacct` in
step 7 are the final acceptance criterion. Keep the backup from step 3 through
acceptance and the required retention period.

### 6. Restore next-job IDs

Clearing EmbeddedDB removes both counters. Restore them before reopening
submissions. The counters are calculated independently and need not match.
Have the backup script read and validate the manifest and invoke both
`ccontrol reset` operations:

```bash
python3 "$crane_upgrade_tool" \
  --restore-job-ids-from "$crane_backup_dir"
```

The script rejects incomplete backups and manifests whose maximum and next IDs
are inconsistent. Continue only after it prints both `[INFO] Completed: restore
... ID` messages, `SUCCESS: Job ID counters restored`, and both next IDs.

Then start `craned` on every compute node and start the interactive frontend:

```bash
sudo systemctl start craned
sudo systemctl start cfored
systemctl is-active craned
systemctl is-active cfored
```

Both checks must print `active`.

### 7. Verify the upgrade

Complete these checks before removing the submission block.

Check services on their respective hosts and read historical jobs:

```bash
systemctl is-active cranectld cplugind
systemctl is-active craned
systemctl is-active cfored
cqueue -t all --json
cacct -t all -F
cacct -t all --json
```

The queue must be empty. Verify the job ID, state, CPU, memory, submit time, and
end time for at least one completed v1.1.3 job.

Keep external submissions blocked. Have the script submit a held canary, verify
its ID, cancel it, and read the cancelled record through `cacct`:

```bash
python3 "$crane_upgrade_tool" \
  --run-canary-from "$crane_backup_dir"
cqueue -t all --json
```

The script must print `[INFO]` messages confirming the ID match, cancellation,
and the `cacct` read, followed by `SUCCESS: Upgrade canary passed: <job-id>` and
the `cacct` JSON. `cqueue` must then be empty. Remove the submission block only
after every check passes.

## Rollback SOP

Perform a complete rollback if CraneCtld cannot start, the database upgrade reports an
error, `cacct` cannot read historical jobs, or canary verification fails:

1. Reapply the submission block and stop the upgraded `cfored`, `craned`,
   `cranectld`, and `cplugind` services.
2. Reinstall the retained v1.1.3 package or binaries, but do not start services.
3. Have the script restore configuration, MongoDB, and the CraneCtld runtime
   directory from the same backup:

   ```bash
   python3 "$crane_upgrade_tool" \
     --rollback-from "$crane_backup_dir"
   ```

   The script first restores the complete MongoDB database named in the
   manifest, then restores configuration and runtime data. Replaced
   new-release configuration and runtime directories are moved to timestamped
   `failed-upgrade` paths. When MongoDB authentication is enabled, the command
   prompts for the password. Each operation prints `[INFO] Starting` and
   `[INFO] Completed`. The command must print `SUCCESS: Rollback data restored`
   and the restored MongoDB, configuration, runtime, and `failed-upgrade` paths.
   Do not start services if any operation reports an error.

4. Start the v1.1.3 services and inspect their output:

   ```bash
   sudo systemctl start cplugind cranectld
   sudo systemctl start craned
   sudo systemctl start cfored
   systemctl is-active cplugind cranectld
   systemctl is-active craned
   systemctl is-active cfored
   cqueue -t all --json
   cacct -t all --json
   ```

   Every service must print `active`, `cqueue` must match the expected
   pre-upgrade state, and `cacct` must read pre-upgrade history before
   submissions are reopened.

A rollback must restore the v1.1.3 binaries, configuration, MongoDB database,
and CraneCtld runtime directory together. Never mix data from different
releases.
