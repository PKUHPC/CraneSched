# Cgroup v2 device policies

`managed_devices` is an ordinary HASH from `(type, major, minor)` to a stable
index in `[0, 4096)`. Missing keys allow access, so entries must never be evicted.
Different slots can refer to the same device file and share its index. A slot
with several device files allows all of their indices.

The shared `craned_device_access` program reads three permission bitmaps from
`BPF_MAP_TYPE_CGROUP_STORAGE` using `bpf_get_local_storage()`. The kernel supplies
the storage of the effective program attachment, including when the process is
in an arbitrary descendant. The access path does not query a cgroup ID, walk
ancestors, or cache a resolved parent. Each policy value occupies 1544 bytes.

Policies use `BPF_F_ALLOW_OVERRIDE`: the closest attached Crane policy is a
complete allocation. The scheduler must supply allocations within the enclosing
job/step allocation. A task without independent GRES inherits its step policy;
the allocator accepts an explicit `apply_device_policy` flag so an independent
empty task policy is representable. `SetDeviceAccess({})` denies every managed
device for the selected operations. Unmanaged devices remain accessible.

Craned initializes the table from the device manager's discovered file metadata.
The existing `step_spec` carries allocated slots to Supervisor; the initialization
message additionally carries slot-to-index associations. Supervisor opens the
same pinned runtime and does not rediscover or renumber devices.

The program is pinned last, after both maps and the initial device table are
ready, under `/sys/fs/bpf/crane_devices_v1`. A process-shared file lock serializes
initialization and policy publication. Newly created attachments have zeroed,
not-ready storage and deny managed devices until a complete policy is published.
Userspace updates replace the complete storage buffer under kernel RCU; they do
not clear a running policy in place. Processes enter new cgroups after successful
publication.

Normal daemon shutdown only closes descriptors. Restart validates the map ABI,
program/map identities, device table and existing attachments, then reuses them
without rewriting running policies. Pins deliberately survive idle periods.
Removing a cgroup releases its storage through the kernel; failed removal leaves
the policy attached. There is no global per-cgroup map to sweep.

`Reconfigure` is reserved and currently returns an error. A changed device set,
incomplete bootstrap or incompatible pins require explicit recovery; ordinary
startup never silently renumbers devices. This ABI does not adopt the previous
`craned_dev_map` implementation or replace its attachments on running jobs.
An upgrade must account for those jobs and legacy pins separately.

## Local verification

Build with `CRANE_ENABLE_BPF=ON` and `CRANE_ENABLE_TESTS=ON`. The
`misc_bpf_device_test` target checks all permission combinations and bitmap
boundaries without privileges. The two kernel tests are opt-in:

```sh
sudo env CRANE_BPF_TEST_OBJECT=/absolute/build/src/Misc/BPF/cgroup_dev_bpf.o \
  /absolute/build/test/Misc/misc_bpf_device_test
```

The kernel tests require permission to create cgroups, mount a private bpffs and
load device BPF programs. They use a uniquely named temporary cgroup, migrate
only child processes, and remove their own cgroups and pins. They exercise deep
inheritance, task overrides, read/write/mknod, empty and non-ready policies,
updates, restart, Supervisor connection, capacity limits and kernel reclamation.
They do not start Crane daemons or require GPUs. On Linux 6.6, the legacy storage
iterator may return a non-entry sentinel; tests count only keys with a readable
value.

This covers descendants of the cgroups to which Crane applies policies. The
existing CRI container parent selection is a separate integration boundary:
containers currently placed under the job cgroup inherit the job's policy.
