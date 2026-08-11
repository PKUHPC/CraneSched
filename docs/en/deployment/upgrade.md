# Upgrade Guide

This guide describes the current CraneSched database upgrade procedure for
control-plane administrators. It covers a maintenance-window upgrade from a
v1.1.3 database to a release containing the v0-to-v1 schema migration.

!!! warning
    The current release does not guarantee forward compatibility for pending
    or running jobs. Drain or cancel every active job before stopping CraneCtld.
    An interrupted job from a newer release can leave a step or array child in
    MongoDB without its job or array parent. This is a risk for later upgrades;
    the v1.1.3 source database itself has no job-step records. The current
    procedure detects this but does not reconstruct the missing runtime state.
    See
    [issue #950](https://github.com/PKUHPC/CraneSched/issues/950) for the
    planned forward-compatibility work.

## Backups

Create a timestamped backup directory and keep it until the upgraded cluster
passes verification.

| Data | Back up | Purpose |
| --- | --- | --- |
| Configuration | `/etc/crane/` (including `config.yaml`, `database.yaml`, `plugin.yaml`, certificates, and local overrides) | Restore service and database settings |
| MongoDB | The complete database named by `DbName` in `database.yaml` | Preserve job history, accounts, users, QoS, and the migration source |
| EmbeddedDB | Every file belonging to `CraneCtldDbPath` | Preserve runtime state and next-ID counters for rollback |

Resolve a relative `CraneCtldDbPath` against `CraneBaseDir` (or
`CraneSharedBaseDir` for Keepalived) before copying it. With RocksDB, the actual
directory is `<resolved-CraneCtldDbPath>.rocksdb/`. v1.1.3 used a legacy
Unqlite/BerkeleyDB layout with three files:

```text
<resolved-CraneCtldDbPath>var
<resolved-CraneCtldDbPath>fix
<resolved-CraneCtldDbPath>resv
```

The v1.1.3 source has no job-step records and no `step_var` or `step_fix` files.
The three files above are its complete legacy EmbeddedDB set. Newer releases may
create additional step stores, but they are not part of the v1.1.3 source
backup. The paths in the running installation take precedence; back up the whole
parent directory when in doubt. Example MongoDB backup (replace placeholders;
let the tooling prompt for a password when appropriate):

```bash
mongodump --host <DbHost> --port <DbPort> \
  --username <DbUser> --authenticationDatabase admin \
  --db <DbName> --out <backup-dir>/mongodb
```

Record the CraneSched version, package/Git version, `DbName`, resolved
EmbeddedDB path, and backup timestamp. Never overwrite an earlier backup.

## Upgrade

Perform all steps in one maintenance window.

### 1. Block submissions and drain jobs

Block new calls to `cbatch`, `crun`, `calloc`, `ccon`, and other submission
frontends at the login gateway or access-control layer. Keep the block until
verification is complete.

Cancel jobs that have not started, then let running jobs finish when possible:

```bash
ccancel -t Pending
cqueue -t all --json
```

The queue command must report an empty `job_info_list`. If running jobs cannot
finish in the maintenance window, cancel them explicitly and check again:

```bash
ccancel -t Running
cqueue -t all --json
```

Do not continue while any pending or running job remains. The v1.1.3 `cqueue`
has no step-query mode, and no step records need to be drained for this upgrade.

### 2. Stop services

Run these commands on the relevant hosts. Stop submission/interactive
frontends first, then compute daemons, the scheduler, and its plugin service:

```bash
sudo systemctl stop cfored
sudo systemctl stop craned       # every compute node
sudo systemctl stop cranectld
sudo systemctl stop cplugind
```

Verify that no old Crane process still holds the EmbeddedDB. Do not stop
`mongod`; it can remain available for the database dump.

### 3. Back up and clear EmbeddedDB

Copy `/etc/crane/`, dump the configured MongoDB database, and copy all
EmbeddedDB files after the processes have stopped. Move the live EmbeddedDB to
a dated backup location instead of deleting it:

```bash
sudo mv <resolved-CraneCtldDbPath>.rocksdb \
  <backup-dir>/embedded.db.rocksdb.v1.1.3
```

Move all three legacy files (`var`, `fix`, and `resv`) when that backend is in
use. This upgrade deliberately starts with an empty EmbeddedDB; do not restore
the old runtime database before starting the new version.

### 4. Start the new version and restore ID counters

Install the new package or binaries and keep the existing configuration unless
the release notes require a change:

```bash
sudo systemctl daemon-reload
sudo systemctl start cplugind
sudo systemctl start cranectld
sudo systemctl start craned       # every compute node
sudo systemctl start cfored
```

CraneCtld automatically migrates MongoDB schema v0 to v1. It copies and
converts the source, then swaps it into `job_table`; the original is retained
as `task_table_backup_v0`. Keep that collection until acceptance and backup
retention are complete.

Since EmbeddedDB was cleared, calculate the maximum IDs in MongoDB before
allowing new submissions:

```javascript
use <DbName>
db.job_table.aggregate([
  {$group: {
    _id: null,
    max_job_id: {$max: "$job_id"},
    max_job_db_id: {$max: "$job_db_id"}
  }}
]).forEach(printjson)
```

Set each counter to one greater than its maximum (use `1` for an empty
collection):

```bash
ccontrol reset next-job-id <max_job_id+1>
ccontrol reset next-job-db-id <max_job_db_id+1>
```

This step is required because clearing EmbeddedDB also removes its counters;
without it, a new job can reuse an ID already in MongoDB.

## Verification

Keep submissions blocked while completing every check below.

### Services and schema

```bash
systemctl is-active cranectld craned cfored cplugind
journalctl -u cranectld -b --no-pager | \
  rg "schema version|Migrating schema v0 -> v1|Schema migration v0 -> v1 completed|Migrated db schema"
```

The log must show successful migration and no migration error. Verify the
persisted version and collection swap:

```javascript
use <DbName>
db.metadata_table.findOne({_id: "db_schema_version"})
db.getCollectionNames().filter(n => /^(task_table|job_table)(_backup_v0)?$/.test(n))
```

The metadata document must contain `version: 1`; `job_table` must exist and
`task_table_backup_v0` should remain available for rollback.

### Queue, accounting, and consistency

```bash
cqueue -t all --json
cacct -t all -F
cacct -t all --json
```

Read at least one normal completed job and an array job if the production
database has one. The v1.1.3 fixture used for this migration has one normal job
and no array or job-step records. For a pure v1.1.3 database, every migrated job
should have `has_job_info: true`; count exceptions for manual review:

```javascript
db.job_table.countDocuments({has_job_info: {$ne: true}})
```

An exception is not expected from v1.1.3 data. Preserve the affected job IDs
and investigate before reopening submissions.

Check that every array child has a parent. This query should return no documents
in a consistent database:

```javascript
db.job_table.aggregate([
  {$match: {array_job_id: {$gte: 0}, array_task_id: {$gte: 0}}},
  {$lookup: {
    from: "job_table", localField: "array_job_id",
    foreignField: "job_id", as: "parent"
  }},
  {$match: {$expr: {$eq: [{$size: "$parent"}, 0]}}},
  {$project: {_id: 0, job_id: 1, array_job_id: 1, array_task_id: 1}}
])
```

Any result is an orphan array child. Preserve the output with the upgrade
record and escalate it through the site recovery process; do not claim that
the interrupted job was restored.

### New-job smoke test

Submit one held canary, confirm its ID is greater than all historical IDs, then
cancel it and read it with `cacct`:

```bash
cbatch --hold --json -J upgrade-canary --wrap 'true'
cqueue -t all --json
ccancel <canary-job-id>
cacct -j <canary-job-id> --json
```

Only remove the submission block after the canary is visible, cancellation
succeeds, and `cacct` returns its record.

## Rollback

Rollback if CraneCtld cannot start, migration logs an error, the schema version
is not `1`, or historical jobs cannot be read:

1. Reapply the submission block and stop the new `cfored`, `craned`,
   `cranectld`, and `cplugind` services.
2. Restore `/etc/crane/` from the backup.
3. Drop only the configured `<DbName>` and restore that database from the
   MongoDB dump, so collections absent from the dump do not remain. Confirm
   the database name before running the destructive command:

   ```bash
   mongosh --host <DbHost> --port <DbPort> <DbName> \
     --eval 'db.dropDatabase()'
   mongorestore --host <DbHost> --port <DbPort> \
     --db <DbName> <backup-dir>/mongodb/<DbName>
   ```

4. Restore the EmbeddedDB files to their original absolute paths.
5. Start the old-version services and verify `cqueue` and `cacct` before
   reopening submissions.

Rollback is a complete return to the backed-up configuration, MongoDB database,
and EmbeddedDB snapshot. Do not run a new version with an old EmbeddedDB in a
mixed state.
