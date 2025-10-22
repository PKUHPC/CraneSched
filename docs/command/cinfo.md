# cinfo - View Node and Partition Status

**cinfo queries resource information for partition nodes.**

View the status of partition nodes:
~~~bash
cinfo
~~~

**cinfo Output Example**

![cinfo](../images/cinfo/cinfo_running.png)

## Main Output Fields

- **PARTITION**: Partition name
- **AVAIL**: Partition availability status
  - up: Available
  - down: Unavailable
- **NODES**: Number of nodes
- **STATE**: Node states
  - **idle**: Idle
  - mix: Some cores on node are available
  - alloc: Node is fully allocated
  - down: Node is unavailable
- **NODELIST**: List of nodes

## Main Options

- **-h/--help**: Display help
- **-C/--config string**: Path to configuration file (default: "/etc/crane/config.yaml")
- **-d/--dead**: Display non-responding nodes only
- **-i/--iterate uint**: Refresh query results at specified intervals (seconds). For example, `-i=3` outputs results every 3 seconds
- **--json**: Output command execution results in JSON format
- **-n/--nodes string**: Display specified node information (comma-separated for multiple nodes). Example: `cinfo -n crane01,crane02`
- **-N/--noheader**: Hide table headers in output
- **-p/--partition string**: Display specified partition information (comma-separated for multiple partitions). Example: `cinfo -p CPU,GPU`
- **-r/--responding**: Display responding nodes only
- **-t/--states string**: Display nodes with specified states only. States can be (case-insensitive): IDLE, MIX, ALLOC, and DOWN. Examples:
  - `-t idle,mix`
  - `-t=alloc`
- **-v/--version**: Query version number

### Format Specifiers (-o/--format)

The `--format` option allows customized output formatting. Fields are identified by a percent sign (%) followed by a character or string. Use a dot (.) and a number between % and the format character or string to specify a minimum width for the field.

**Supported Format Identifiers** (case-insensitive):

| Identifier | Full Name | Description |
|------------|-----------|-------------|
| %p | Partition | Display all partitions in the current environment |
| %a | Avail | Display the availability state of the partition |
| %n | Nodes | Display the number of partition nodes |
| %s | State | Display the status of partition nodes |
| %l | NodeList | Display all node lists in the partition |

Each format specifier or string can be modified with a width specifier (e.g., "%.5j"). If the width is specified, the field will be formatted to at least that width. If the format is invalid or unrecognized, the program will terminate with an error message.

**Format Example:**
```bash
# Display partition name (min width 5), state (min width 6), and status
cinfo --format "%.5partition %.6a %s"
```

## Usage Examples

**Display all partitions and nodes:**
```bash
cinfo
```
![cinfo](../images/cinfo/cinfo_running.png)

**Display help:**
```bash
cinfo -h
```
![cinfo](../images/cinfo/cinfo_h.png)

**Hide table header:**
```bash
cinfo -N
```
![cinfo](../images/cinfo/cinfo_n.png)

**Show only non-responding nodes:**
```bash
cinfo -d
```
![cinfo](../images/cinfo/cinfo_d.png)

**Auto-refresh every 3 seconds:**
```bash
cinfo -i 3
```
![cinfo](../images/cinfo/cinfo_i3.png)

**Display specific nodes:**
```bash
cinfo -n crane01,crane02,crane03
```
![cinfo](../images/cinfo/cinfo_n123.png)

**Display specific partitions:**
```bash
cinfo -p GPU,CPU
```
![cinfo](../images/cinfo/cinfo_p.png)

**Show only responding nodes:**
```bash
cinfo -r
```
![cinfo](../images/cinfo/cinfo_r.png)

**Filter by node state:**
```bash
cinfo -t IDLE
```
![cinfo](../images/cinfo/cinfo_t.png)

**Display version:**
```bash
cinfo -v
```
![cinfo](../images/cinfo/cinfo_v.png)

**JSON output:**
```bash
cinfo --json
```

## Node State Filtering

The `-t/--states` option allows filtering nodes by their states. Multiple states can be specified as a comma-separated list:

```bash
# Show idle and mixed nodes
cinfo -t idle,mix

# Show only allocated nodes
cinfo -t alloc

# Show only down nodes
cinfo -t down
```

**Note:** The `-t/--states`, `-r/--responding`, and `-d/--dead` options are mutually exclusive. Only one can be specified at a time.

## Related Commands

- [cqueue](cqueue.md) - View job queue
- [ccontrol](ccontrol.md) - Control cluster resources
- [cacctmgr](cacctmgr.md) - Manage accounts and partitions
