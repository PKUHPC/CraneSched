# cwrapper - Slurm Command Compatibility

`cwrapper` provides basic command compatibility for users familiar with Slurm. Invoke the entry points directly:

```bash
cwrapper sacct [options]
cwrapper squeue [options]
cwrapper sinfo [options]
```

Administrators can also configure Slurm command aliases by following the [frontend deployment guide](../deployment/frontend/frontend.md).

The compatibility entry points interpret options according to Slurm semantics, so some short options and format specifiers differ from the native CraneSched commands. The sections below cover the main differences for `sacct`, `squeue`, and `sinfo`. Run each entry point with `--help` for its complete options and specifiers, for example `cwrapper sinfo --help`. Use the native commands to access advanced CraneSched features.

## sacct

`cwrapper sacct` queries job accounting information and corresponds to the native command [`cacct`](cacct.md).

| Function | Slurm entry point | Native command `cacct` |
|----------|-------------------|------------------------|
| Omit the header | `-n/--noheader` | `-N` |
| Filter by partition | `-r/--partition` | `-p` |
| Filter by state | `-s/--state` | `-t` |
| Filter by node | `-N/--nodelist` | `-w` |
| Filter by job name | `--name` | `-n` |
| Filter by submit time | `--submittime` | `-s` |

The compatibility options `-p/--parsable` and `-P/--parsable2` both produce pipe-delimited output. `-p` retains a trailing `|` on each line; `-P` omits the trailing delimiter.

## squeue

`cwrapper squeue` queries the job queue and corresponds to the native command [`cqueue`](cqueue.md).

The compatibility entry point uses `-h/--noheader` to omit the header and `--help` to display help. The native command uses `cqueue -N` to omit the header.

The compatibility entry point interprets `-o` format specifiers as follows:

| Specifier | Meaning |
|-----------|---------|
| `%T` | Extended state, such as `PENDING` |
| `%t` | Compact state, such as `PD` |
| `%R` | Pending reason in parentheses for pending jobs, or the allocated node list |
| `%r` | Pending reason |

In the native command `cqueue`, `%T` represents the job type and `%t` represents the job state.

## sinfo

`cwrapper sinfo` queries node and partition status and corresponds to the native command [`cinfo`](cinfo.md).

| Function | Slurm entry point | Native command `cinfo` |
|----------|-------------------|------------------------|
| Omit the header | `-h/--noheader`; help is available through `--help` only | `-N` |
| Custom format | `-o` takes Slurm specifiers, such as `%P %a %l %D %t %T %N %E %F` | `-o` takes `%p %a %n %s %l` |

The compatibility entry point also supports these output options:

- `-O` selects fields by name, for example `Partition,Nodes:.5,StateLong`.
- `-N` displays a node-oriented view with one line per node and partition combination.
- `-R` displays the two columns `REASON NODELIST` by default.

The compatibility entry point displays these six columns by default, with `TIMELIMIT` always set to `infinite`:

```text
PARTITION AVAIL TIMELIMIT NODES STATE NODELIST
```

The native command `cinfo` displays five columns by default:

```text
PARTITION AVAIL NODES STATE NODELIST
```
