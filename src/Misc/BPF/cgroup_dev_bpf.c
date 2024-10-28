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
#ifdef BPF_DEBUG
  bpf_printk("ctx cgroup ID : %lu\n", key.cgroup_id);
#endif
  struct BpfDeviceMeta *meta;

  meta = (struct BpfDeviceMeta *)bpf_map_lookup_elem(&craned_dev_map, &key);
  if (!meta) {
#ifdef BPF_DEBUG
    bpf_printk("BpfDeviceMeta not found for key cgroup ID: %d,\n", key.cgroup_id);
    bpf_printk("Access allowed for device major=%d, minor=%d\n", ctx->major,
            ctx->minor);
#endif
    return 1;
  }

  short type = ctx->access_type & 0xFFFF;
  short access = ctx->access_type >> 16;

// bpf_printk("Device major=%d, minor=%d, access_type=%d\n", ctx->major,
// ctx->minor, access);
#ifdef BPF_DEBUG
  bpf_printk("meta Device major=%d, minor=%d, access_type=%d\n", meta->major,
             meta->minor, meta->access);
#endif

  if (ctx->major == meta->major && ctx->minor == meta->minor) {
//     if (meta->permission == ALLOW) {
//       int flag = 0;
//       if (access & BPF_DEVCG_ACC_READ)
//         if (meta->access & BPF_DEVCG_ACC_READ) {
// #ifdef BPF_DEBUG
//           bpf_printk("Read access allowed for device major=%d, minor=%d\n",
//                      ctx->major, ctx->minor);
// #endif
//           flag |= 1;
//         }
//       if (access & BPF_DEVCG_ACC_WRITE)
//         if (meta->access & BPF_DEVCG_ACC_WRITE) {
// #ifdef BPF_DEBUG
//           bpf_printk("Write access allowed for device major=%d, minor=%d\n",
//                      ctx->major, ctx->minor);
// #endif
//           flag |= 1;
//         }
//       if (access & BPF_DEVCG_ACC_MKNOD)
//         if (meta->access & BPF_DEVCG_ACC_MKNOD) {
// #ifdef BPF_DEBUG
//           bpf_printk("Write access allowed for device major=%d, minor=%d\n",
//                      ctx->major, ctx->minor);
// #endif
//           flag |= 1;
//         }
//       return flag;
//     } else 
    if (meta->permission == DENY) {
      int flag = 1;
      if (access & BPF_DEVCG_ACC_READ)
        if (meta->access & BPF_DEVCG_ACC_READ) {
#ifdef BPF_DEBUG
          bpf_printk("Read access denied for device major=%d, minor=%d\n",
                     ctx->major, ctx->minor);
#endif
          flag &= 0;
        }
      if (access & BPF_DEVCG_ACC_WRITE)
        if (meta->access & BPF_DEVCG_ACC_WRITE) {
#ifdef BPF_DEBUG
          bpf_printk("Write access denied for device major=%d, minor=%d\n",
                     ctx->major, ctx->minor);
#endif
          flag &= 0;
        }
      if (access & BPF_DEVCG_ACC_MKNOD)
        if (meta->access & BPF_DEVCG_ACC_MKNOD) {
#ifdef BPF_DEBUG

          bpf_printk("Write access denied for device major=%d, minor=%d\n",
                     ctx->major, ctx->minor);
#endif
          flag &= 0;
        }
      return flag;
    }
  }

#ifdef BPF_DEBUG
  bpf_printk("Access allowed for device major=%d, minor=%d\n", ctx->major,
             ctx->minor);
#endif
  return 1;
}

char _license[] SEC("license") = "GPL";
