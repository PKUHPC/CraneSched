# License Usage Guide

Crane can help manage software licenses by allocating available licenses to jobs during scheduling. 
If licenses are unavailable, jobs will remain in the queue until licenses become available. 
In Crane, licenses are essentially shared resources, meaning they are not bound to a specific host, 
but are associated with the entire cluster.

Crane currently supports configuring licenses in the following ways:

* **Local licenses:** Defined in the `/etc/crane/config.yaml` file of a specific cluster, and only valid locally for that cluster.
* **Remote licenses:** Remote licenses are stored in the database and configured using the `cacctmgr` command. 
  Remote licenses are dynamic.

## Local Licenses

Local licenses are defined using the `Licenses` option in `/etc/crane/config.yaml`.

```yaml
Licenses:
  - name: fluent
    quantity: 30
  - name: ansys
    quantity: 100
```

You can view configured licenses using the `ccontrol` command.

```bash
$ ccontrol show lic
LicenseName=ansys
        Total=100 Used=0  Free=100
LicenseName=fluent
        Total=30 Used=0  Free=30
```

Jobs can request licenses via the `-L` or `--licenses` option. Multiple licenses may be requested at once.

```bash
cbatch -L fluent:2 script.sh
# AND relationship: all conditions must be satisfied for the job to run
cbatch -L fluent:2,ansys:1 script.sh
# OR relationship: any one condition being satisfied is enough to run.
# The scheduler will try to allocate the first license in the list. If unavailable, it tries the next, and so on.
cbatch -L fluent:2|ansys:1 script.sh
```

## Remote Licenses

Remote licenses do not include any built-in integration with third-party license managers. 
The `Server` and `ServerType` parameters specified during creation serve informational purposes only. 
they **do not** mean Crane will interact with those servers automatically.

It is the system administrator’s responsibility to implement integration with external systems. 
This includes ensuring that:

- Only users who request remote licenses through Crane can check out licenses from the license server.
- The number of licenses in Crane stays synchronized with the actual license server (see Dynamic Licenses below).

### Use Case

A site has two license servers: one provides 100 Nastran licenses via FlexNet, 
and another provides 50 Matlab licenses via Reprise License Management. 
The site has two clusters, fluid and pdf, used for simulation jobs requiring both software packages. 
The administrator wants to split the Nastran licenses evenly between the two clusters, 
and allocate 70% of the Matlab licenses to the pdf cluster and the remaining 30% to the fluid cluster.

When adding licenses using `cacctmgr`, the admin must specify the license count 
and the percentage allocation for each cluster. 
This can be done in one step or multiple steps.

**Single step:**

```bash
$ cacctmgr add resource nastran cluster=fluid,pdf \
  server=flex_host servertype=flexlm \
  count=100 allowed=50 type=license
Resource added successfully
```

**Multiple steps:**

```bash
$ cacctmgr add resource matlab count=50 server=rlm_host \
  servertype=rlm type=license
Resource added successfully
$ cacctmgr add resource matlab server=rlm_host \
  cluster=pdf allowed=70
Resource added successfully
$ cacctmgr add resource matlab server=rlm_host \
  cluster=fluid allowed=30
Resource added successfully
```

`cacctmgr` will show the total number of configured licenses:

```bash
$ cacctmgr show resource
|---------|-----------|---------|-------|--------------|-----------|------------|-------|
| NAME    | SERVER    | TYPE    | COUNT | LASTCONSUMED | ALLOCATED | SERVERTYPE | FLAGS |
|---------|-----------|---------|-------|--------------|-----------|------------|-------|
| matlab  | rlm_host  | License | 50    | 0            | 100%      | rlm        |       |
| nastran | flex_host | License | 100   | 0            | 100%      | flexlm     |       |
|---------|-----------|---------|-------|--------------|-----------|------------|-------|
$ cacctmgr show resource withclusters
|---------|-----------|---------|-------|--------------|-----------|------------|----------|---------|-------|
| NAME    | SERVER    | TYPE    | COUNT | LASTCONSUMED | ALLOCATED | SERVERTYPE | CLUSTERS | ALLOWED | FLAGS |
|---------|-----------|---------|-------|--------------|-----------|------------|----------|---------|-------|
| matlab  | rlm_host  | License | 50    | 0            | 100%      | rlm        | fluid    | 30%     |       |
| matlab  | rlm_host  | License | 50    | 0            | 100%      | rlm        | pdf      | 70%     |       |
| nastran | flex_host | License | 100   | 0            | 100%      | flexlm     | fluid    | 50%     |       |
| nastran | flex_host | License | 100   | 0            | 100%      | flexlm     | pdf      | 50%     |       |
|---------|-----------|---------|-------|--------------|-----------|------------|----------|---------|-------|
```

Configured licenses can now be viewed in each cluster with `ccontrol show lic`:

```bash
# On cluster "pdf"
$ ccontrol show lic
LicenseName=matlab@rlm_host
    Total=35 Used=0 Free=35 Reserved=0 Remote=yes
    LastConsumed=0 LastDeficit=0 LastUpdate=2025-11-28T13:05:44
LicenseName=nastran@flex_host
    Total=50 Used=0 Free=50 Reserved=0 Remote=yes
    LastConsumed=0 LastDeficit=0 LastUpdate=2025-11-28T13:05:44

# On cluster "fluid"
$ ccontrol show lic
LicenseName=matlab@rlm_host
    Total=15 Used=0 Free=15 Reserved=0 Remote=yes
    LastConsumed=0 LastDeficit=0 LastUpdate=2025-11-28T13:05:44
LicenseName=nastran@flex_host
    Total=50 Used=0 Free=50 Reserved=0 Remote=yes
    LastConsumed=0 LastDeficit=0 LastUpdate=2025-11-28T13:05:44
```

When submitting jobs that use remote licenses, you must specify both the license name and server:

```bash
$ cbatch -L nastran@flex_host script.sh
```

You can modify license percentages and counts as follows:

```bash
$ cacctmgr modify resource name=matlab server=rlm_host set \
  count=200
Modify information succeeded
$ cacctmgr modify resource name=matlab server=rlm_host \
  cluster=pdf set allowed=60
Modify information succeeded
$ cacctmgr show resource withclusters
|---------|-----------|---------|-------|--------------|-----------|------------|----------|---------|-------|
| NAME    | SERVER    | TYPE    | COUNT | LASTCONSUMED | ALLOCATED | SERVERTYPE | CLUSTERS | ALLOWED | FLAGS |
|---------|-----------|---------|-------|--------------|-----------|------------|----------|---------|-------|
| matlab  | rlm_host  | License | 200   | 0            | 90%       | rlm        | pdf      | 60%     |       |
| matlab  | rlm_host  | License | 200   | 0            | 90%       | rlm        | fluid    | 30%     |       |
| nastran | flex_host | License | 100   | 0            | 100%      | flexlm     | fluid    | 50%     |       |
| nastran | flex_host | License | 100   | 0            | 100%      | flexlm     | pdf      | 50%     |       |
|---------|-----------|---------|-------|--------------|-----------|------------|----------|---------|-------|
```

Licenses can be deleted per cluster or entirely:

```bash
$ cacctmgr delete resource matlab server=rlm_host cluster=fluid
Successfully deleted Resource 'matlab@rlm_host'.
$ cacctmgr delete resource nastran server=flex_host
Successfully deleted Resource 'nastran@flex_host'.
$ cacctmgr show resource withclusters
|--------|----------|---------|-------|--------------|-----------|------------|----------|---------|-------|
| NAME   | SERVER   | TYPE    | COUNT | LASTCONSUMED | ALLOCATED | SERVERTYPE | CLUSTERS | ALLOWED | FLAGS |
|--------|----------|---------|-------|--------------|-----------|------------|----------|---------|-------|
| matlab | rlm_host | License | 200   | 0            | 60%       | rlm        | pdf      | 60%     |       |
|--------|----------|---------|-------|--------------|-----------|------------|----------|---------|-------|
```

If the **Absolute** flag is set, the `allowed` values for each cluster will be treated as **absolute counts** rather than percentages.

Example usage:

```bash
$ cacctmgr add resource deluxe cluster=fluid,pdf count=150 allowed=70 \
  server=flex_host servertype=flexlm flags=absolute type=license
Resource added successfully
$ cacctmgr show resource withclusters
|--------|-----------|---------|-------|--------------|-----------|------------|----------|---------|----------|
| NAME   | SERVER    | TYPE    | COUNT | LASTCONSUMED | ALLOCATED | SERVERTYPE | CLUSTERS | ALLOWED | FLAGS    |
|--------|-----------|---------|-------|--------------|-----------|------------|----------|---------|----------|
| deluxe | flex_host | License | 150   | 0            | 140       | flexlm     | pdf      | 70      | Absolute |
| deluxe | flex_host | License | 150   | 0            | 140       | flexlm     | fluid    | 70      | Absolute |
|--------|-----------|---------|-------|--------------|-----------|------------|----------|---------|----------|
$ cacctmgr update resource name=deluxe server=flex_host cluster=fluid set allowed=25
Modify information succeeded.
$ cacctmgr show resource withclusters
|--------|-----------|---------|-------|--------------|-----------|------------|----------|---------|----------|
| NAME   | SERVER    | TYPE    | COUNT | LASTCONSUMED | ALLOCATED | SERVERTYPE | CLUSTERS | ALLOWED | FLAGS    |
|--------|-----------|---------|-------|--------------|-----------|------------|----------|---------|----------|
| deluxe | flex_host | License | 150   | 0            | 95        | flexlm     | fluid    | 25      | Absolute |
| deluxe | flex_host | License | 150   | 0            | 95        | flexlm     | pdf      | 70      | Absolute |
|--------|-----------|---------|-------|--------------|-----------|------------|----------|---------|----------|
```

You may also set `AllLicenseResourcesAbsolute=yes` in `/etc/crane/config.yaml` 
to make all newly created licenses use Absolute mode by default 
(CraneCtld restart required).

## Dynamic Licenses

The `LastConsumed` field of remote licenses is designed to be periodically updated 
with the actual consumption from the license server.

Below is an example script for FlexLM’s `lmstat` command; 
similar scripts can be written for other license systems.

```bash
#!/bin/bash

set -euxo pipefail

LMSTAT=/opt/foobar/bin/lmstat
LICENSE=foobar
SERVER=db

consumed=$(${LMSTAT} | grep "Users of ${LICENSE}"|sed "s/.*Total of \([0-9]\+\) licenses in use)/\1/")

cacctmgr update resource name=${LICENSE} server=${SERVER} set lastconsumed=${consumed}
```

When `cacctmgr` updates `LastConsumed`, the new value is automatically pushed to the Crane controller. 
The controller uses this value to compute the `LastDeficit`,
the number of licenses “missing” from the cluster’s perspective that must be reserved.

For example, suppose a cluster is allocated 80 out of 100 available foobar licenses:

```bash
$ cacctmgr add resource foobar server=db count=100 flags=absolute cluster=blackhole allowed=80 type=license
Resource added successfully
$ ccontrol show lic
LicenseName=foobar@db 
        Total=80 Used=0 Free=80 Reserved=0 Remote=yes
        LastConsumed=0 LastDeficit=0 LastUpdated=2025-12-01 11:02:23
```

Now suppose a cron job updates `LastConsumed` to 30, 
even though the cluster itself has not allocated any licenses:

```bash
$ cacctmgr update resource name=foobar server=db set lastconsumed=30
Modify information succeeded.
$ ccontrol show lic
LicenseName=foobar@db 
        Total=80 Used=0 Free=70 Reserved=0 Remote=yes
        LastConsumed=30 LastDeficit=10 LastUpdated=2025-12-01 11:03:0
```

Note that the cluster now sees a deficit of 10 licenses, 
and thus will only schedule up to 70 licenses. 
Even though 80 licenses are allocated to the cluster, 
10 licenses appear to be missing and untrackable. 
Crane cannot allocate these licenses to jobs because jobs might fail if they attempt to check out unavailable licenses.

If the next script update reduces `LastConsumed` to 20, the deficit disappears, 
and all 80 allocated licenses become available again:

```bash
$ cacctmgr update resource name=foobar server=db set lastconsumed=20
Modify information succeeded.
$ ccontrol show lic
LicenseName=foobar@db 
        Total=80 Used=0 Free=80 Reserved=0 Remote=yes
        LastConsumed=20 LastDeficit=0 LastUpdated=2025-12-01 11:10:48
```
