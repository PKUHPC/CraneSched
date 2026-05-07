# Job Preemption

CraneSched supports QoS-based job preemption. When a high-priority QoS job cannot obtain resources, the scheduler can terminate lower-priority QoS jobs to free up capacity.

## Configuration

Enable preemption in `config.yaml`:

```yaml
Preempt:
  PreemptType: qos       # none | qos | partition
  PreemptMode: CANCEL    # OFF | CANCEL
```

| Field | Description |
| :--- | :--- |
| `PreemptType` | Trigger mechanism. `none` disables preemption; `qos` uses per-QoS preempt lists; `partition` uses partition priority (not yet implemented) |
| `PreemptMode` | Action taken on preempted jobs. `OFF` disables; `CANCEL` immediately cancels the preempted job |

## QoS Preempt List

Each QoS can define a `preempt` list naming other QoS entries it is allowed to preempt.

### Creating a QoS with Preemption

```bash
# Create a low-priority QoS
cacctmgr add qos LowQos Priority=500

# Create a high-priority QoS that can preempt LowQos
cacctmgr add qos HighQos Priority=2000 Preempt=LowQos PreemptMode=CANCEL
```

### Modifying Preemption Settings

```bash
# Set preempt targets
cacctmgr modify qos where Name=HighQos set Preempt=LowQos,MediumQos

# Clear preempt list
cacctmgr modify qos where Name=HighQos set Preempt=""

# Change preempt mode
cacctmgr modify qos where Name=HighQos set PreemptMode=CANCEL
```

### Viewing Preemption Info

```bash
cacctmgr show qos format=Name,Priority,Preempt,PreemptMode
```

## Preemption Behavior

### Trigger Conditions

Preemption is triggered when ALL of the following are true:

1. Cluster configuration has `PreemptType` set to something other than `none`
2. The pending job's QoS has a non-empty `preempt` list
3. Target nodes have running jobs belonging to a preemptable QoS
4. Preempting those jobs would free enough resources for the new job

### CANCEL Mode

Preempted jobs receive an immediate cancel signal. Their status transitions to `Cancelled` and released resources become available for new jobs.

## Example

```bash
# 1. Create two QoS levels
cacctmgr add qos Background Priority=100
cacctmgr add qos Interactive Priority=5000 Preempt=Background PreemptMode=CANCEL

# 2. Grant account access to both QoS
cacctmgr modify account where Name=MyAccount set AllowedQos+=Background,Interactive

# 3. Submit a low-priority job that fills a node
cbatch -p CPU --exclusive -q Background long_job.sh

# 4. Submit a high-priority job (triggers preemption)
cbatch -p CPU -q Interactive short_job.sh
# → Background job is cancelled, Interactive job starts immediately
```
