# creport - Query Job-Related Statistics

**creport is mainly used to query job statistics related to users and accounts within a specified time range in the
cluster.**

```bash
creport [<OPTION>] [<COMMAND>]
```

## Common Options

The following options apply to most subcommands. To avoid duplication, they are not repeated in each subcommand section.

**-S, --start-time string**

:   Specify the query start time (default is 00:00:00 of the previous day), format: `2023-03-14T10:00:00`.

**-E, --end-time string**

:   Specify the query end time (default is 23:59:59 of the previous day), format: `2023-03-14T10:00:00`.

**-t, --time string**

:   Specify the time unit for output job data (default is minutes).

**-C, --config string**

:   Path to configuration file (default: `/etc/crane/config.yaml`).

**-h, --help**

:   Show help information for the creport command.

## user Subcommands

### user topusage - Display User Resource Consumption Ranking

```bash
creport user topusage [--start-time=...] [--end-time=...] [--account=...] ...
```

#### Command Line Options

Subcommand-specific options (common options are listed above):

**-A, --account string**

:   Specify accounts to query, use commas to separate multiple accounts.

**-u, --user string**

:   Specify users to query, use commas to separate multiple users.

**--group bool**

:   Group all accounts of each user together (default is separate entries for each user-account reference).

**--topcount uint32**

:   Specify the number of output entries (default is 10).

#### Output Fields

- **CLUSTER**: Cluster name
- **LOGIN**: Username
- **PROPER NAME**: Full Linux system name
- **ACCOUNT**: Account name
- **USED**: The sum of total CPUs * runtime for each job under the user

#### Sample Output

```bash
$ creport  user topusage -S=2024-09-01T01:00:00 -E=2025-09-01T00:20:10
--------------------------------------------------------------------------------
Top 10 Users 2024-09-01T01:00:00 - 2025-09-01T00:20:10 (31533610 secs)
Usage reported in CPU Minutes
|-------------|---------|-------------|------------|------|--------|
| CLUSTER     | LOGIN   | PROPER NAME | ACCOUNT    | USED | ENERGY |
|-------------|---------|-------------|------------|------|--------|
| crane       | user_5  | user_5      | account_5  | 0.67 | 0      |
| crane       | user_3  | user_3      | account_3  | 0.33 | 0      |
| crane       | user_4  | user_4      | account_4  | 0.33 | 0      |
| crane       | user_9  | user_9      | account_9  | 0.33 | 0      |
| crane       | user_10 | user_10     | account_10 | 0.33 | 0      |
| crane       | user_7  | user_7      | account_7  | 0.33 | 0      |
| crane       | user_2  | user_2      | account_2  | 0.33 | 0      |
| crane       | user_8  | user_8      | account_8  | 0.33 | 0      |
| crane       | user_6  | user_6      | account_6  | 0.33 | 0      |
|-------------|---------|-------------|------------|------|--------|
```

## cluster Subcommands
<!---
### cluster utilization - Display Overall Cluster Utilization

```bash
creport cluster utilization [--start-time=...] [--end-time=...] ...
```

#### Command Line Options

No subcommand-specific options (see Common Options above).

#### Output Fields

- **CLUSTER**: Cluster name
- **ALLOCATE**: Total resources actually allocated by all jobs during the statistics interval (CPU minutes), i.e., sum
  of allocated cores × runtime minutes
- **DOWN**: Total downtime caused by node failures, maintenance, etc. during the statistics interval (CPU minutes),
  i.e., sum of down cores × downtime minutes
- **PLANNED**: Time when jobs are queued but resources are not allocated (CPU minutes), usually indicates resource
  shortage or queue overflow
- **REPORTED**: Theoretical maximum available time for all resources during the statistics interval (CPU minutes), i.e.,
  total cluster cores × time span

#### Sample Output

```bash
$ creport  cluster  utilization  -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
--------------------------------------------------------------------------------
Cluster Utilization 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Usage reported in CPU Minutes
|-------------|----------|------|---------|----------|
| CLUSTER     | ALLOCATE | DOWN | PLANNED | REPORTED |
|-------------|----------|------|---------|----------|
| crane       | 3.33     | -    | -       | -        |
|-------------|----------|------|---------|----------|
```
--->
### cluster accountutilizationbyuser - Display Account-User Resource Utilization

```bash
creport cluster accountutilizationbyuser [--start-time=...] [--end-time=...] [--account=...] ...
```

#### Command Line Options

Subcommand-specific options (common options are listed above):

**-A, --account string**

:   Specify accounts to query, use commas to separate multiple accounts.

**-u, --user string**

:   Specify users to query, use commas to separate multiple users.

#### Output Fields

- **CLUSTER**: Cluster name
- **ACCOUNT**: Account name
- **LOGIN**: Username
- **PROPER NAME**: Full Linux system name
- **USED**: The sum of total CPUs * runtime for each job under the user
- **ENERGY**: Energy consumed by jobs

#### Sample Output

```bash
$ creport  cluster  accountutilizationbyuser  -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
--------------------------------------------------------------------------------
Cluster/Account/User Utilization 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Usage reported in CPU Minutes
|-------------|------------|---------|-------------|------|--------|
| CLUSTER     | ACCOUNT    | LOGIN   | PROPER NAME | USED | ENERGY |
|-------------|------------|---------|-------------|------|--------|
| crane       | account_10 | user_10 | user_10     | 0.33 | 0      |
| crane       | account_2  | user_2  | user_2      | 0.33 | 0      |
| crane       | account_3  | user_3  | user_3      | 0.33 | 0      |
| crane       | account_4  | user_4  | user_4      | 0.33 | 0      |
| crane       | account_5  | user_5  | user_5      | 0.67 | 0      |
| crane       | account_6  | user_6  | user_6      | 0.33 | 0      |
| crane       | account_7  | user_7  | user_7      | 0.33 | 0      |
| crane       | account_8  | user_8  | user_8      | 0.33 | 0      |
| crane       | account_9  | user_9  | user_9      | 0.33 | 0      |
|-------------|------------|---------|-------------|------|--------|
```

### cluster userutilizationbyaccount - Display User-Account Resource Utilization

```bash
creport cluster userutilizationbyaccount [--start-time=...] [--end-time=...] [--user=...] ...
```

#### Command Line Options

Subcommand-specific options (common options are listed above):

**-A, --account string**

:   Specify accounts to query, use commas to separate multiple accounts.

**-u, --user string**

:   Specify users to query, use commas to separate multiple users.

#### Output Fields

- **CLUSTER**: Cluster name
- **LOGIN**: Username
- **PROPER NAME**: Full Linux system name
- **ACCOUNT**: Account name
- **USED**: The sum of total CPUs * runtime for each job under the user
- **ENERGY**: Energy consumed by jobs

#### Sample Output

```bash
$ creport  cluster  userutilizationbyaccount  -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
--------------------------------------------------------------------------------
Cluster/User/Account Utilization 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Usage reported in CPU Minutes
|-------------|---------|-------------|------------|------|--------|
| CLUSTER     | LOGIN   | PROPER NAME | ACCOUNT    | USED | ENERGY |
|-------------|---------|-------------|------------|------|--------|
| crane       | user_10 | user_10     | account_10 | 0.33 | 0      |
| crane       | user_2  | user_2      | account_2  | 0.33 | 0      |
| crane       | user_3  | user_3      | account_3  | 0.33 | 0      |
| crane       | user_4  | user_4      | account_4  | 0.33 | 0      |
| crane       | user_5  | user_5      | account_5  | 0.67 | 0      |
| crane       | user_6  | user_6      | account_6  | 0.33 | 0      |
| crane       | user_7  | user_7      | account_7  | 0.33 | 0      |
| crane       | user_8  | user_8      | account_8  | 0.33 | 0      |
| crane       | user_9  | user_9      | account_9  | 0.33 | 0      |
|-------------|---------|-------------|------------|------|--------|
```

### cluster userutilizationbywckey - Display User-WCKEY Resource Utilization

```bash
creport cluster userutilizationbywckey [--start-time=...] [--end-time=...] [--user=...] ...
```

#### Command Line Options

Subcommand-specific options (common options are listed above):

**-u, --user string**

:   Specify users to query, use commas to separate multiple users.

#### Output Fields

- **CLUSTER**: Cluster name
- **LOGIN**: Username
- **PROPER NAME**: Full Linux system name
- **WCKEY**: WCKEY name
- **USED**: The sum of total CPUs * runtime for each job under the user

#### Sample Output

```bash
$ creport  cluster  userutilizationbywckey  -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
--------------------------------------------------------------------------------
Cluster/Account/User Utilization 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Usage reported in CPU Minutes
|-------------|---------|-------------|-------|------|
| CLUSTER     | LOGIN   | PROPER NAME | WCKEY | USED |
|-------------|---------|-------------|-------|------|
| crane       | user_9  | user_9      |       | 0.33 |
| crane       | user_8  | user_8      |       | 0.33 |
| crane       | user_10 | user_10     |       | 0.33 |
| crane       | user_7  | user_7      |       | 0.33 |
| crane       | user_3  | user_3      |       | 0.33 |
| crane       | user_4  | user_4      |       | 0.33 |
| crane       | user_6  | user_6      |       | 0.33 |
| crane       | user_2  | user_2      |       | 0.33 |
| crane       | user_5  | user_5      |       | 0.67 |
|-------------|---------|-------------|-------|------|
```

### cluster wckeyutilizationbyuser - Display WCKEY-User Resource Utilization

```bash
creport cluster wckeyutilizationbyuser [--start-time=...] [--end-time=...] [--wckeys=...] ...
```

#### Command Line Options

Subcommand-specific options (common options are listed above):

**-w, --wckeys string**

:   Specify WCKEYs to query, use commas to separate multiple WCKEYs.

#### Output Fields

- **CLUSTER**: Cluster name
- **WCKEY**: WCKEY name
- **LOGIN**: Username
- **PROPER NAME**: Full Linux system name
- **USED**: The sum of total CPUs * runtime for each job under the user

#### Sample Output

```bash
$ creport  cluster  wckeyutilizationbyuser  -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
--------------------------------------------------------------------------------
Cluster/WCKey/User Utilization 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Usage reported in CPU Minutes
|-------------|-------|---------|-------------|------|
| CLUSTER     | WCKEY | LOGIN   | PROPER NAME | USED |
|-------------|-------|---------|-------------|------|
| crane       |       | user_3  | user_3      | 0.33 |
| crane       |       | user_8  | user_8      | 0.33 |
| crane       |       | user_7  | user_7      | 0.33 |
| crane       |       | user_10 | user_10     | 0.33 |
| crane       |       | user_5  | user_5      | 0.67 |
| crane       |       | user_2  | user_2      | 0.33 |
| crane       |       | user_9  | user_9      | 0.33 |
| crane       |       | user_4  | user_4      | 0.33 |
| crane       |       | user_6  | user_6      | 0.33 |
|-------------|-------|---------|-------------|------|
```

### cluster accountutilizationbyqos - Display Account-QOS Resource Utilization

```bash
creport cluster accountutilizationbyqos [--start-time=...] [--end-time=...] [--account=...] [--qos=...] ...
```

#### Command Line Options

Subcommand-specific options (common options are listed above):

**-A, --account string**

:   Specify accounts to query, use commas to separate multiple accounts.

**-q, --qos string**

:   Specify QOS to query, use commas to separate multiple QOS.

#### Output Fields

- **CLUSTER**: Cluster name
- **ACCOUNT**: Account name
- **QOS**: QOS name
- **USED**: The sum of total CPUs * runtime for each job under the user
- **ENERGY**: Energy consumed by jobs

#### Sample Output

```bash
$ creport cluster accountutilizationbyqos -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
--------------------------------------------------------------------------------
Cluster/Account/Qos Utilization 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Usage reported in CPU Minutes
|-------------|------------|-----------|------|--------|
| CLUSTER     | ACCOUNT    | QOS       | USED | ENERGY |
|-------------|------------|-----------|------|--------|
| crane       | account_10 | UNLIMITED | 0.33 | 0      |
| crane       | account_2  | UNLIMITED | 0.33 | 0      |
| crane       | account_3  | UNLIMITED | 0.33 | 0      |
| crane       | account_4  | UNLIMITED | 0.33 | 0      |
| crane       | account_5  | UNLIMITED | 0.67 | 0      |
| crane       | account_6  | UNLIMITED | 0.33 | 0      |
| crane       | account_7  | UNLIMITED | 0.33 | 0      |
| crane       | account_8  | UNLIMITED | 0.33 | 0      |
| crane       | account_9  | UNLIMITED | 0.33 | 0      |
|-------------|------------|-----------|------|--------|
```

## job Subcommands

### Common Options for job Subcommands

The following options apply to all job subcommands. To avoid duplication, they are not repeated in each subcommand section.

**--gid string**

:   Specify the gid to query, use commas to separate multiple gids.

**--grouping string**

:   Comma-separated list of size groupings (default: `50,100,250,500,1000`).

**--printjobcount bool**

:   Report will print the number of jobs in the range instead of the used time.

**-j, --jobs string**

:   Specify job IDs to query, use commas to separate multiple IDs (e.g., `-j=2,3,4`).

**-n, --nodes string**

:   Specify node names to query, use commas to separate multiple nodes.

**-p, --partition string**

:   Specify partitions to query, use commas to separate multiple partitions.

### job sizesbyaccount - Display Job Size Distribution Grouped by Account

```bash
creport job sizesbyaccount [--start-time=...] [--end-time=...] [--account=...] ...
```

#### Command Line Options

Subcommand-specific options (common options are listed in "Common Options for job Subcommands"):

**-A, --account string**

:   Specify accounts to query, use commas to separate multiple accounts.

#### Output Fields

- **CLUSTER**: Cluster name
- **ACCOUNT**: Account name
- **0-49 CPUs**: CPU minutes in the 0-49 CPUs range
- **50-249 CPUs**: CPU minutes in the 50-249 CPUs range
- **250-499 CPUs**: CPU minutes in the 250-499 CPUs range
- **500-999 CPUs**: CPU minutes in the 500-999 CPUs range
- **>= 1000 CPUs**: CPU minutes in the >=1000 CPUs range
- **TOTAL CPU TIME**: Total CPU minutes for all jobs under the specific account
- **% OF CLUSTER**: Percentage of total cluster job CPU minutes occupied

#### Sample Output

```bash
$ creport  job  sizesbyaccount  -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
------------------------------------------------------------------------------------------------------------------------------------
Job Sizes 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Time reported in Minutes
+-------------+------------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
| CLUSTER     | ACCOUNT    | 0-49 CPUS | 50-249 CPUS | 250-499 CPUS | 500-999 CPUS | >= 1000 CPUS| TOTAL CPU TIME | % OF CLUSTER   |
+-------------+------------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
| crane       | account_10 |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_2  |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_3  |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_4  |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_5  |      0.67 |        0.00 |         0.00 |         0.00 |        0.00 |           0.67 | 20.00%         |
| crane       | account_6  |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_7  |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | crane      |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_9  |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
+-------------+------------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
```

### job sizesbywckey - Display Job Size Distribution Grouped by WCKEY

```bash
creport job sizesbywckey [--start-time=...] [--end-time=...] [--wckeys=...] ...
```

#### Command Line Options

Subcommand-specific options (common options are listed in "Common Options for job Subcommands"):

**-w, --wckeys string**

:   Specify WCKEYs to query, use commas to separate multiple WCKEYs.

#### Output Fields

- **CLUSTER**: Cluster name
- **WCKEY**: WCKEY name
- **0-49 CPUs**: CPU minutes in the 0-49 CPUs range
- **50-249 CPUs**: CPU minutes in the 50-249 CPUs range
- **250-499 CPUs**: CPU minutes in the 250-499 CPUs range
- **500-999 CPUs**: CPU minutes in the 500-999 CPUs range
- **>= 1000 CPUs**: CPU minutes in the >=1000 CPUs range
- **TOTAL CPU TIME**: Total CPU minutes for all jobs under the specific account
- **% OF CLUSTER**: Percentage of total cluster job CPU minutes occupied

#### Sample Output

```bash
$ creport  job  sizesbywckey  -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
----------------------------------------------------------------------------------------------------
Job Sizes by Wckey 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Time reported in Minutes
+-------------+-------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
| CLUSTER     | WCKEY | 0-49 CPUS | 50-249 CPUS | 250-499 CPUS | 500-999 CPUS | >= 1000 CPUS| TOTAL CPU TIME | % OF CLUSTER   |
+-------------+-------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
| crane       |       |      3.33 |        0.00 |         0.00 |         0.00 |        0.00 |           3.33 | 100.00%        |
+-------------+-------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
```

### job sizesbyaccountandwckey - Display Job Size Distribution Grouped by WCKEY and Account

```bash
creport job sizesbyaccountandwckey [--start-time=...] [--end-time=...] [--wckeys=...] ...
```

#### Command Line Options

Subcommand-specific options (common options are listed in "Common Options for job Subcommands"):

**-A, --account string**

:   Specify accounts to query, use commas to separate multiple accounts.

**-w, --wckeys string**

:   Specify WCKEYs to query, use commas to separate multiple WCKEYs.

#### Output Fields

- **CLUSTER**: Cluster name
- **ACCOUNT:WCKEY**: Combination of account name and WCKEY name (format: `<ACCOUNT>:<WCKEY>`)
- **0-49 CPUs**: CPU minutes in the 0-49 CPUs range
- **50-249 CPUs**: CPU minutes in the 50-249 CPUs range
- **250-499 CPUs**: CPU minutes in the 250-499 CPUs range
- **500-999 CPUs**: CPU minutes in the 500-999 CPUs range
- **>= 1000 CPUs**: CPU minutes in the >=1000 CPUs range
- **TOTAL CPU TIME**: Total CPU minutes for all jobs under the specific account
- **% OF CLUSTER**: Percentage of total cluster job CPU minutes occupied

#### Sample Output

```bash
$ creport  job  sizesbyaccountandwckey  -S=2024-09-01T00:00:00 -E=2025-09-01T00:00:00
------------------------------------------------------------------------------------------------------------------------------------
Job Sizes 2024-09-01T00:00:00 - 2025-09-01T00:00:00 (31536000 secs)
Time reported in Minutes
+-------------+----------------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
| CLUSTER     | ACCOUNT:WCKEY  | 0-49 CPUS | 50-249 CPUS | 250-499 CPUS | 500-999 CPUS | >= 1000 CPUS| TOTAL CPU TIME | % OF CLUSTER   |
+-------------+----------------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
| crane       | account_10:    |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_2:     |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_3:     |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_4:     |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_5:     |      0.67 |        0.00 |         0.00 |         0.00 |        0.00 |           0.67 | 20.00%         |
| crane       | account_6:     |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_7:     |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_8:     |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
| crane       | account_9:     |      0.33 |        0.00 |         0.00 |         0.00 |        0.00 |           0.33 | 10.00%         |
+-------------+----------------+-----------+-------------+--------------+--------------+-------------+----------------+----------------+
```

## Related Commands

- [cqueue](cqueue.md) - View job queue (current/pending jobs)
- [cbatch](cbatch.md) - Submit batch jobs
- [ccancel](ccancel.md) - Cancel jobs
- [ceff](ceff.md) - View job efficiency statistics
- [cacct](cacct.md) - Query completed jobs
