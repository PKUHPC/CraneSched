/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include "DevicePolicy.h"
// Linux BPF types must be declared before the helper prototypes.

#include <bpf/bpf_helpers.h>

// A missing entry means unmanaged, so this must never evict entries.
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, MAX_MANAGED_DEVICES);
  __uint(map_flags, BPF_F_RDONLY_PROG);
  __type(key, struct DeviceKey);
  __type(value, __u32);
} managed_devices SEC(".maps");

// The kernel selects storage belonging to the effective attachment, even
// when the process lives in an arbitrary descendant without its own program.
struct {
  __uint(type, BPF_MAP_TYPE_CGROUP_STORAGE);
  __type(key, struct bpf_cgroup_storage_key);
  __type(value, struct DevicePolicy);
} device_policies SEC(".maps");

SEC("cgroup/dev")
int craned_device_access(struct bpf_cgroup_dev_ctx *ctx) {
  struct DeviceKey key = {
      .type = ctx->access_type & 0xffff,
      .major = ctx->major,
      .minor = ctx->minor,
  };
  __u32 *index = bpf_map_lookup_elem(&managed_devices, &key);
  if (!index) return 1;
  if (*index >= MAX_MANAGED_DEVICES) return 0;
  struct DevicePolicy *policy = bpf_get_local_storage(&device_policies, 0);
  return DevicePolicyAllows(policy, *index, ctx->access_type >> 16);
}

char _license[] SEC("license") = "GPL";
