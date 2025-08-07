// DO NOT
#include <linux/bpf.h>
// REORDER
#include <bpf/bpf_helpers.h>
// THESE
#include <bpf/bpf_core_read.h>
// HEADERS
#include <stdint.h>

enum BPF_PERMISSION { ALLOW = 0, DENY };

#pragma pack(push, 8)
struct BpfKey {
  uint64_t cgroup_id;
  uint32_t major;
  uint32_t minor;
};
#pragma pack(pop)

#pragma pack(push, 8)
struct BpfDeviceMeta {
  uint32_t major;
  uint32_t minor;
  int permission;
  int16_t access;
  int16_t type;
};
#pragma pack(pop)

#define MAX_ENTRIES 4096

struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __type(key, struct BpfKey);
  __type(value, struct BpfDeviceMeta);
  __uint(max_entries, MAX_ENTRIES);
  __uint(pinning, LIBBPF_PIN_BY_NAME);
} craned_dev_map SEC(".maps");

SEC("cgroup/dev")
int craned_device_access(struct bpf_cgroup_dev_ctx *ctx) {
  // Using CO-RE to read ctx fields
  uint32_t major = BPF_CORE_READ(ctx, major);
  uint32_t minor = BPF_CORE_READ(ctx, minor);
  uint32_t access_type = BPF_CORE_READ(ctx, access_type);

  struct BpfKey key = {bpf_get_current_cgroup_id(), major, minor};
  // value.major in map(0,0,0) contains log level
  struct BpfKey log_key = {(uint64_t)0, (uint32_t)0, (uint32_t)0};
  struct BpfDeviceMeta *log_level_meta;

  log_level_meta =
      (struct BpfDeviceMeta *)bpf_map_lookup_elem(&craned_dev_map, &log_key);
  int enable_logging;
  if (!log_level_meta) {
    enable_logging = 0;
  } else {
    enable_logging = log_level_meta->major;
  }

  if (enable_logging) bpf_printk("ctx cgroup ID : %lu\n", key.cgroup_id);
  struct BpfDeviceMeta *meta;

  meta = (struct BpfDeviceMeta *)bpf_map_lookup_elem(&craned_dev_map, &key);
  if (!meta) {
    if (enable_logging) {
      bpf_printk("BpfDeviceMeta not found for key cgroup ID: %llu,\n",
                 key.cgroup_id);
      bpf_printk("Access allowed for device major=%d, minor=%d\n", major,
                 minor);
    }
    return 1;
  }

  short type = access_type & 0xFFFF;
  short access = access_type >> 16;

  if (enable_logging)
    bpf_printk("meta Device major=%d, minor=%d, access_type=%d\n", meta->major,
               meta->minor, meta->access);

  if (major == meta->major && minor == meta->minor) {
    if (meta->permission == DENY) {
      int flag = 1;
      if (access & BPF_DEVCG_ACC_READ)
        if (meta->access & BPF_DEVCG_ACC_READ) {
          if (enable_logging)
            bpf_printk("Read access denied for device major=%d, minor=%d\n",
                       major, minor);
          flag &= 0;
        }
      if (access & BPF_DEVCG_ACC_WRITE)
        if (meta->access & BPF_DEVCG_ACC_WRITE) {
          if (enable_logging)
            bpf_printk("Write access denied for device major=%d, minor=%d\n",
                       major, minor);
          flag &= 0;
        }
      if (access & BPF_DEVCG_ACC_MKNOD)
        if (meta->access & BPF_DEVCG_ACC_MKNOD) {
          if (enable_logging)
            bpf_printk("Write access denied for device major=%d, minor=%d\n",
                       major, minor);
          flag &= 0;
        }
      return flag;
    }
  }

  if (enable_logging)
    bpf_printk("Access allowed for device major=%d, minor=%d\n", major, minor);
  return 1;
}

char _license[] SEC("license") = "GPL";