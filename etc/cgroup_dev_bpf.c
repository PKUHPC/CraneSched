#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

enum BPF_PERMISSION{
    ALLOW = 0,
    DENY
};

struct BpfDeviceMeta {
    unsigned int major;
    unsigned int minor;
    int permission;
    short access;
    short type;
};

#define MAX_ENTRIES 128
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, int);
    __type(value, struct BpfDeviceMeta);
    __uint(max_entries, MAX_ENTRIES);
    __uint(pinning, LIBBPF_PIN_BY_NAME);
} dev_map SEC(".maps");                     

SEC("cgroup/dev")
int device_access(struct bpf_cgroup_dev_ctx *ctx) {

    int key = (ctx->major << 16) | ctx->minor; 

    struct BpfDeviceMeta *meta;

    meta = bpf_map_lookup_elem(&dev_map, &key);
    if (!meta) {
        bpf_printk("BpfDeviceMeta not found for key %d\n", key);
        return 1;
    }

    short type = ctx->access_type & 0xFFFF;
    short access = ctx->access_type >> 16;

    bpf_printk("Device major=%d, minor=%d, access_type=%d\n", ctx->major, ctx->minor, access);
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

    return  1;  
}

char _license[] SEC("license") = "GPL";
