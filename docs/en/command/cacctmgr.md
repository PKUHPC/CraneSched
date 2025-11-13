# cacctmgr - Account Manager

**cacctmgr manages accounts, users, and Quality of Service (QoS) settings in the CraneSched system using SQL-style commands.**

!!! note
    This command uses a SQL-style syntax for managing cluster resources. The syntax is: `cacctmgr <ACTION> <ENTITY> [OPTIONS]`

## User Roles

CraneSched has four user privilege levels:

- **Admin (System Administrator)**: Typically the root user, has full permissions to create, read, update, and delete any account or user
- **Operator (Platform Administrator)**: Has complete permissions over the account system
- **Coordinator (Account Coordinator)**: Has permissions over users in the same account and child accounts, including adding users
- **None (Regular User)**: Can only query information within their own account, cannot modify user or account information

## Command Syntax

```bash
cacctmgr <ACTION> <ENTITY> [ID] [OPTIONS]
```

### Global Options

- **-h, --help**: Display help information
- **-C, --config string**: Configuration file path (default: `/etc/crane/config.yaml`)
- **-v, --version**: Display cacctmgr version
- **-J, --json**: Output in JSON format
- **-f, --force**: Force operation without confirmation

## Actions

- **add**: Create a new account, user, or QoS
- **delete**: Remove an account, user, or QoS
- **block**: Block an account or user from using the system
- **unblock**: Unblock a previously blocked account or user
- **modify**: Change attributes using SQL-style SET clause
- **show**: Display information about entities
- **reset**: Reset user certificate

## Entities

- **account**: User account in the system
- **user**: Individual user
- **qos**: Quality of Service settings
- **transaction**: Transaction log (for show action only)

---

## 1. Account Management

### 1.1 Add Account

**Syntax:**
```bash
cacctmgr add account <name> [OPTIONS]
```

**Options:**

- **Description=<desc>**: Account description
- **Parent=<parent>**: Parent account name
- **DefaultQos=<qos>**: Default QoS for the account
- **Partition=<part1,part2,...>**: Allowed partitions (comma-separated)
- **QosList=<qos1,qos2,...>**: Allowed QoS list (comma-separated)
- **Name=<name1,name2,...>**: Batch create multiple accounts (comma-separated)

**Examples:**

Create a simple account:
```bash
cacctmgr add account PKU Description="Peking University" Partition=CPU,GPU QosList=normal,high
```

Create a child account:
```bash
cacctmgr add account ComputingCentre Description="Computing Center" Parent=PKU
```

Batch create accounts:
```bash
cacctmgr add account dept Name=CS,Math,Physics Parent=PKU
```

### 1.2 Delete Account

**Syntax:**
```bash
cacctmgr delete account <name>
```

**Note:** You cannot delete an account that has child accounts or users. Remove them first.

**Example:**
```bash
cacctmgr delete account ComputingCentre
```

### 1.3 Modify Account

**Syntax:**
```bash
cacctmgr modify account where Name=<account> set <ATTRIBUTE>=<value>
```

**Attributes:**

- **Description=<desc>**: Set description
- **DefaultQos=<qos>**: Set default QoS
- **AllowedPartition=<partitions>**: Set allowed partitions (overwrites)
- **AllowedPartition+=<partitions>**: Add partitions to allowed list
- **AllowedPartition-=<partitions>**: Remove partitions from allowed list
- **AllowedQos=<qos_list>**: Set allowed QoS list (overwrites)
- **AllowedQos+=<qos_list>**: Add QoS to allowed list
- **AllowedQos-=<qos_list>**: Remove QoS from allowed list

**Examples:**

Change account description:
```bash
cacctmgr modify account where Name=ComputingCentre set Description="HPC Computing Center"
```

Add partitions to allowed list:
```bash
cacctmgr modify account where Name=PKU set AllowedPartition+=GPU2,GPU3
```

Remove partitions from allowed list:
```bash
cacctmgr modify account where Name=PKU set AllowedPartition-=GPU
```

Set allowed partitions (replace existing):
```bash
cacctmgr modify account where Name=PKU set AllowedPartition=CPU,GPU
```

### 1.4 Show Accounts

**Syntax:**
```bash
cacctmgr show account [name] [OPTIONS]
```

**Options:**

- **Name=<name1,name2,...>**: Show specific accounts only

**Examples:**

Show all accounts:
```bash
cacctmgr show account
```

Show specific account:
```bash
cacctmgr show account PKU
```

Show multiple accounts:
```bash
cacctmgr show account Name=PKU,ComputingCentre
```

### 1.5 Block/Unblock Account

**Syntax:**
```bash
cacctmgr block account <name>
cacctmgr unblock account <name>
```

**Examples:**

Block an account:
```bash
cacctmgr block account ComputingCentre
```

Unblock an account:
```bash
cacctmgr unblock account ComputingCentre
```

---

## 2. User Management

### 2.1 Add User

**Syntax:**
```bash
cacctmgr add user <name> Account=<account> [OPTIONS]
```

**Required:**

- **Account=<account>**: Account the user belongs to (required)

**Options:**

- **Coordinator=true|false**: Set user as account coordinator
- **Level=<level>**: User admin level (none/operator/admin, default: none)
- **Partition=<part1,part2,...>**: Allowed partitions (comma-separated)
- **Name=<name1,name2,...>**: Batch create multiple users (comma-separated)

**Note:** The user must exist as a Linux system user (create with `useradd` first).

**Examples:**

Create a simple user:
```bash
useradd alice
cacctmgr add user alice Account=PKU
```

Create user with specific permissions:
```bash
useradd bob
cacctmgr add user bob Account=PKU Level=operator Partition=CPU,GPU
```

Create user as coordinator:
```bash
useradd charlie
cacctmgr add user charlie Account=PKU Coordinator=true
```

Batch create users:
```bash
useradd user1 && useradd user2 && useradd user3
cacctmgr add user batch Account=PKU Name=user1,user2,user3
```

### 2.2 Delete User

**Syntax:**
```bash
cacctmgr delete user <name> [Account=<account>]
```

**Options:**

- **Account=<account>**: Specify account context (optional)
- **Name=<name1,name2,...>**: Delete multiple users (comma-separated)

**Special:**
- If name is `ALL` and `--force` is set, all users from the specified account will be deleted

**Examples:**

Delete a user:
```bash
cacctmgr delete user alice
```

Delete user from specific account:
```bash
cacctmgr delete user bob Account=PKU
```

Delete all users from an account (with force):
```bash
cacctmgr delete user ALL Account=PKU --force
```

### 2.3 Modify User

**Syntax:**
```bash
cacctmgr modify user where Name=<user> [Account=<account>] [Partition=<partitions>] set <ATTRIBUTE>=<value>
```

**Where Clause:**

- **Name=<user>**: User to modify (required)
- **Account=<account>**: Account context (optional)
- **Partition=<partitions>**: Partition context (optional)

**Attributes:**

- **AdminLevel=<level>**: Set admin level (none/operator/admin)
- **DefaultAccount=<account>**: Set default account
- **DefaultQos=<qos>**: Set default QoS
- **AllowedPartition=<partitions>**: Set allowed partitions (overwrites)
- **AllowedPartition+=<partitions>**: Add partitions
- **AllowedPartition-=<partitions>**: Remove partitions
- **AllowedQos=<qos_list>**: Set allowed QoS (overwrites)
- **AllowedQos+=<qos_list>**: Add QoS
- **AllowedQos-=<qos_list>**: Remove QoS

**Examples:**

Change user admin level:
```bash
cacctmgr modify user where Name=alice Account=PKU set AdminLevel=operator
```

Remove partition from user:
```bash
cacctmgr modify user where Name=bob set AllowedPartition-=GPU
```

Add QoS to user:
```bash
cacctmgr modify user where Name=charlie set AllowedQos+=high
```

### 2.4 Show Users

**Syntax:**
```bash
cacctmgr show user [name] [OPTIONS]
```

**Options:**

- **Accounts=<account>**: Show users of specific account only
- **Name=<name1,name2,...>**: Show specific users only

**Examples:**

Show all users:
```bash
cacctmgr show user
```

Show specific user:
```bash
cacctmgr show user alice
```

Show users in an account:
```bash
cacctmgr show user Accounts=PKU
```

### 2.5 Block/Unblock User

**Syntax:**
```bash
cacctmgr block user <name> Account=<account>
cacctmgr unblock user <name> Account=<account>
```

**Required:**

- **Account=<account>**: Account context is required for user block/unblock

**Examples:**

Block a user:
```bash
cacctmgr block user alice Account=PKU
```

Unblock a user:
```bash
cacctmgr unblock user alice Account=PKU
```

### 2.6 Reset User Certificate

**Syntax:**
```bash
cacctmgr reset <name>
```

**Special:**
- If name is `all`, all users' certificates will be reset

**Examples:**

Reset single user certificate:
```bash
cacctmgr reset alice
```

Reset all users' certificates:
```bash
cacctmgr reset all
```

---

## 3. QoS Management

### 3.1 Add QoS

**Syntax:**
```bash
cacctmgr add qos <name> [OPTIONS]
```

**Options:**

- **Description=<desc>**: QoS description
- **Priority=<priority>**: Priority value (higher = higher priority)
- **MaxJobsPerUser=<num>**: Maximum concurrent jobs per user
- **MaxCpusPerUser=<num>**: Maximum CPUs per user
- **MaxTimeLimitPerTask=<seconds>**: Maximum runtime per task (in seconds)
- **Name=<name1,name2,...>**: Batch create multiple QoS (comma-separated)

**Examples:**

Create a QoS:
```bash
cacctmgr add qos normal Description="Normal QoS" Priority=1000 MaxJobsPerUser=10 MaxCpusPerUser=100
```

Create high-priority QoS:
```bash
cacctmgr add qos high Description="High Priority" Priority=5000 MaxJobsPerUser=20 MaxCpusPerUser=200 MaxTimeLimitPerTask=86400
```

Batch create QoS:
```bash
cacctmgr add qos batch Name=low,medium,high Priority=500
```

### 3.2 Delete QoS

**Syntax:**
```bash
cacctmgr delete qos <name>
```

**Options:**

- **Name=<name1,name2,...>**: Delete multiple QoS (comma-separated)

**Example:**
```bash
cacctmgr delete qos low
```

### 3.3 Modify QoS

**Syntax:**
```bash
cacctmgr modify qos where Name=<qos> set <ATTRIBUTE>=<value>
```

**Attributes:**

- **Description=<desc>**: Set description
- **MaxCpusPerUser=<num>**: Set max CPUs per user
- **MaxJobsPerUser=<num>**: Set max jobs per user
- **MaxTimeLimitPerTask=<seconds>**: Set max time per task (seconds)
- **Priority=<priority>**: Set priority

**Examples:**

Change QoS priority:
```bash
cacctmgr modify qos where Name=normal set Priority=2000
```

Update resource limits:
```bash
cacctmgr modify qos where Name=high set MaxJobsPerUser=50 MaxCpusPerUser=500
```

### 3.4 Show QoS

**Syntax:**
```bash
cacctmgr show qos [name] [OPTIONS]
```

**Options:**

- **Name=<name1,name2,...>**: Show specific QoS only

**Examples:**

Show all QoS:
```bash
cacctmgr show qos
```

Show specific QoS:
```bash
cacctmgr show qos normal
```

---

## 4. Transaction Log

### 4.1 Show Transaction Log

**Syntax:**
```bash
cacctmgr show transaction where [OPTIONS]
```

**Where Options:**

- **Actor=<username>**: Filter by user who performed the action
- **Target=<target>**: Filter by target entity
- **Action=<action>**: Filter by action type
- **Info=<info>**: Filter by additional information
- **StartTime=<timestamp>**: Filter by start time

**Example:**

Show all transactions:
```bash
cacctmgr show transaction
```

Show transactions by specific user:
```bash
cacctmgr show transaction where Actor=admin
```

Show transactions for a specific account:
```bash
cacctmgr show transaction where Target=PKU
```

---

## Usage Examples

### Complete Workflow Example

```bash
# 1. Create a QoS policy
cacctmgr add qos standard Description="Standard Queue" Priority=1000 MaxJobsPerUser=10

# 2. Create root account with partitions and QoS
cacctmgr add account University Description="University Account" Partition=CPU,GPU QosList=standard

# 3. Create department account under University
cacctmgr add account CS Description="Computer Science Dept" Parent=University

# 4. Create Linux user
useradd professor

# 5. Add user to account with coordinator privileges
cacctmgr add user professor Account=CS Level=operator Coordinator=true

# 6. Create student users
useradd student1 && useradd student2
cacctmgr add user batch Account=CS Name=student1,student2

# 7. Limit student1 to CPU partition only
cacctmgr modify user where Name=student1 Account=CS set AllowedPartition=CPU

# 8. View all users in CS account
cacctmgr show user Accounts=CS

# 9. Block a user temporarily
cacctmgr block user student2 Account=CS

# 10. Unblock the user
cacctmgr unblock user student2 Account=CS

# 11. View transaction history
cacctmgr show transaction where Actor=professor
```

### JSON Output Example

Get results in JSON format for scripting:
```bash
cacctmgr show account --json
cacctmgr show user Accounts=PKU --json
cacctmgr show qos --json
```

---

## Permission Matrix

| Action | Admin | Operator | Coordinator | User |
|--------|-------|----------|-------------|------|
| Add Account | ✓ | ✓ | ✗ | ✗ |
| Delete Account | ✓ | ✓ | ✗ | ✗ |
| Modify Account | ✓ | ✓ | Own account | ✗ |
| Add User | ✓ | ✓ | Same account | ✗ |
| Delete User | ✓ | ✓ | Same account | ✗ |
| Modify User | ✓ | ✓ | Same account | ✗ |
| Add QoS | ✓ | ✓ | ✗ | ✗ |
| Delete QoS | ✓ | ✓ | ✗ | ✗ |
| Modify QoS | ✓ | ✓ | ✗ | ✗ |
| Show (Query) | ✓ | ✓ | ✓ | ✓ (own account) |
| Block/Unblock | ✓ | ✓ | Same account | ✗ |

---

## Important Notes

1. **Linux User Requirement**: Before adding a user to cacctmgr, the user must exist as a Linux system user (create using `useradd`)

2. **Account Hierarchy**: When deleting accounts, ensure no child accounts or users exist under that account

3. **Inheritance**: Users inherit partition and QoS settings from their parent account unless explicitly overridden

4. **Coordinator Privileges**: Coordinators can manage users within their account but cannot modify their own account's parent

5. **SQL-Style Syntax**: The new syntax uses SQL-like `where` and `set` clauses for modify operations, with `+=` and `-=` operators for list modifications

## See Also

- [cbatch](cbatch.md) - Submit batch jobs
- [cqueue](cqueue.md) - View job queue
- [cacct](cacct.md) - View job accounting information
