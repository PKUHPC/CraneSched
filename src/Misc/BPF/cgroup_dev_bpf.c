#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
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
  short access;
  short type;
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
  struct BpfKey key = {bpf_get_current_cgroup_id(), ctx->major, ctx->minor};
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
      bpf_printk("BpfDeviceMeta not found for key cgroup ID: %d,\n",
                 key.cgroup_id);
      bpf_printk("Access allowed for device major=%d, minor=%d\n", ctx->major,
                 ctx->minor);
    }
    return 1;
  }

  short type = ctx->access_type & 0xFFFF;
  short access = ctx->access_type >> 16;

  if (enable_logging)
    bpf_printk("meta Device major=%d, minor=%d, access_type=%d\n", meta->major,
               meta->minor, meta->access);

  if (ctx->major == meta->major && ctx->minor == meta->minor) {
    if (meta->permission == DENY) {
      int flag = 1;
      if (access & BPF_DEVCG_ACC_READ)
        if (meta->access & BPF_DEVCG_ACC_READ) {
          if (enable_logging)
            bpf_printk("Read access denied for device major=%d, minor=%d\n",
                       ctx->major, ctx->minor);
          flag &= 0;
        }
      if (access & BPF_DEVCG_ACC_WRITE)
        if (meta->access & BPF_DEVCG_ACC_WRITE) {
          if (enable_logging)
            bpf_printk("Write access denied for device major=%d, minor=%d\n",
                       ctx->major, ctx->minor);
          flag &= 0;
        }
      if (access & BPF_DEVCG_ACC_MKNOD)
        if (meta->access & BPF_DEVCG_ACC_MKNOD) {
          if (enable_logging)
            bpf_printk("Write access denied for device major=%d, minor=%d\n",
                       ctx->major, ctx->minor);
          flag &= 0;
        }
      return flag;
    }
  }

  if (enable_logging)
    bpf_printk("Access allowed for device major=%d, minor=%d\n", ctx->major,
               ctx->minor);
  return 1;
}

char _license[] SEC("license") = "GPL";
