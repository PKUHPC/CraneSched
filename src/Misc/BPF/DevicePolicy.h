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

// Shared userspace/BPF ABI. Changing this layout requires clearing old pins
// before starting the new daemon.
#ifndef CRANE_BPF_DEVICE_POLICY_H
#define CRANE_BPF_DEVICE_POLICY_H

#include <linux/bpf.h>
#include <linux/types.h>

#define MAX_MANAGED_DEVICES 4096U
#define DEVICE_WORDS (MAX_MANAGED_DEVICES / 64U)

struct DeviceKey {
  __u32 type;
  __u32 major;
  __u32 minor;
};

struct DevicePolicy {
  __u32 ready;
  __u32 reserved;
  __u64 read_bits[DEVICE_WORDS];
  __u64 write_bits[DEVICE_WORDS];
  __u64 mknod_bits[DEVICE_WORDS];
};

static inline int DevicePolicyAllows(const struct DevicePolicy *policy,
                                     __u32 index, __u32 requested) {
  if (index >= MAX_MANAGED_DEVICES || !policy->ready) return 0;
  __u32 word = index >> 6;
  __u64 bit = 1ULL << (index & 63);
  __u32 allowed = 0;
  if (policy->read_bits[word] & bit) allowed |= BPF_DEVCG_ACC_READ;
  if (policy->write_bits[word] & bit) allowed |= BPF_DEVCG_ACC_WRITE;
  if (policy->mknod_bits[word] & bit) allowed |= BPF_DEVCG_ACC_MKNOD;
  return (requested & allowed) == requested;
}

#endif
