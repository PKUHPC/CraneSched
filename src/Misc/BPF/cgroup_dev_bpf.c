#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

enum BPF_PERMISSION{
    ALLOW = 0,
    DENY
};

struct BpfKey {
  unsigned long long cgroup_id;
  unsigned int major;
  unsigned int minor;
};

struct BpfDeviceMeta {
    unsigned int major;
    unsigned int minor;
    int permission;
    short access;
    short type;
};
#define MAX_ENTRIES 4096
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, struct BpfKey);
    __type(value, struct BpfDeviceMeta);
    __uint(max_entries, MAX_ENTRIES);
    __uint(pinning, LIBBPF_PIN_BY_NAME);
} dev_map SEC(".maps");                     

SEC("cgroup/dev")
int device_access(struct bpf_cgroup_dev_ctx *ctx) {

    struct BpfKey key = {bpf_get_current_cgroup_id(),ctx->major,ctx->minor};;
    bpf_printk("ctx cgroup ID : %lu\n",key.cgroup_id);

    struct BpfDeviceMeta *meta;

    meta = (struct BpfDeviceMeta*)bpf_map_lookup_elem(&dev_map, &key);
    if (!meta) {
        bpf_printk("BpfDeviceMeta not found for key %d\n", key);
        return 1;
    }

    short type = ctx->access_type & 0xFFFF;
    short access = ctx->access_type >> 16;

    // bpf_printk("Device major=%d, minor=%d, access_type=%d\n", ctx->major, ctx->minor, access);
    bpf_printk("meta Device major=%d, minor=%d, access_type=%d\n", meta->major, meta->minor, meta->access);

    if (ctx->major == meta->major && ctx->minor == meta->minor) {

        if(meta->permission == ALLOW){
            int flag = 0;
            if (access & BPF_DEVCG_ACC_READ)
                if (meta->access & BPF_DEVCG_ACC_READ){
                    bpf_printk("Read access allowed for device major=%d, minor=%d\n", ctx->major, ctx->minor);
                    flag |= 1;
                }
            if (access & BPF_DEVCG_ACC_WRITE)
                if (meta->access & BPF_DEVCG_ACC_WRITE){
                    bpf_printk("Write access allowed for device major=%d, minor=%d\n", ctx->major, ctx->minor);
                    flag |= 1;
                }
            if(access & BPF_DEVCG_ACC_MKNOD)
                if(meta->access & BPF_DEVCG_ACC_MKNOD){
                    bpf_printk("Write access allowed for device major=%d, minor=%d\n", ctx->major, ctx->minor);
                    flag |= 1;
                }
            return flag;
        } else if(meta->permission == DENY){
            int flag = 1;
            if (access & BPF_DEVCG_ACC_READ)
                if (meta->access & BPF_DEVCG_ACC_READ){
                    bpf_printk("Read access denied for device major=%d, minor=%d\n", ctx->major, ctx->minor);
                    flag &= 0;
                }
            if (access & BPF_DEVCG_ACC_WRITE)
                if (meta->access & BPF_DEVCG_ACC_WRITE){
                    bpf_printk("Write access denied for device major=%d, minor=%d\n", ctx->major, ctx->minor);
                    flag &= 0;
                }
            if(access & BPF_DEVCG_ACC_MKNOD)
                if(meta->access & BPF_DEVCG_ACC_MKNOD){
                    bpf_printk("Write access denied for device major=%d, minor=%d\n", ctx->major, ctx->minor);
                    flag &= 0;
                }
            return flag;
        }
    }


    bpf_printk("Access denied for device major=%d, minor=%d\n", ctx->major, ctx->minor);
    return  1;  
}

char _license[] SEC("license") = "GPL";
