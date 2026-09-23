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

#include <bpf/bpf.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <sched.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <thread>

#include "BpfRuntime.h"

namespace {
using Craned::Common::BpfRuntimeInfo;
using Craned::Common::ManagedDeviceKeysBySlot;
namespace fs = std::filesystem;

TEST(DevicePolicy, PermissionCombinationsAndBitmapBoundaries) {
  for (uint32_t index : {0U, 63U, 64U, 4095U}) {
    for (uint32_t allowed = 0; allowed < 8; ++allowed) {
      DevicePolicy policy{};
      policy.ready = 1;
      auto bit = 1ULL << (index & 63);
      if (allowed & BPF_DEVCG_ACC_READ) policy.read_bits[index >> 6] = bit;
      if (allowed & BPF_DEVCG_ACC_WRITE) policy.write_bits[index >> 6] = bit;
      if (allowed & BPF_DEVCG_ACC_MKNOD) policy.mknod_bits[index >> 6] = bit;
      for (uint32_t requested = 1; requested < 8; ++requested) {
        EXPECT_EQ(DevicePolicyAllows(&policy, index, requested),
                  (requested & allowed) == requested);
        EXPECT_FALSE(DevicePolicyAllows(&policy, index ^ 1, requested));
      }
      EXPECT_FALSE(
          DevicePolicyAllows(&policy, MAX_MANAGED_DEVICES, BPF_DEVCG_ACC_READ));
      EXPECT_FALSE(DevicePolicyAllows(&policy, UINT32_MAX, BPF_DEVCG_ACC_READ));
      EXPECT_FALSE(DevicePolicyAllows(&policy, index, 8));
      policy.ready = 0;
      EXPECT_FALSE(DevicePolicyAllows(&policy, index, BPF_DEVCG_ACC_READ));
    }
  }
}

// Opt-in local kernel test. Only child processes enter a private test cgroup;
// no Crane service, existing cgroup or existing BPF pin is modified.
class BpfDeviceKernel : public testing::Test {
 protected:
  void SetUp() override {
    const char* object = std::getenv("CRANE_BPF_TEST_OBJECT");
    if (!object) GTEST_SKIP() << "Set CRANE_BPF_TEST_OBJECT for kernel tests";
    ASSERT_EQ(unshare(CLONE_NEWNS), 0) << strerror(errno);
    ASSERT_EQ(mount(nullptr, "/", nullptr, MS_REC | MS_PRIVATE, nullptr), 0);
    char pattern[] = "/tmp/crane-bpf-test-XXXXXX";
    const char* directory = mkdtemp(pattern);
    ASSERT_NE(directory, nullptr);
    temp_ = directory;
    paths_ = {object, temp_ / "bpf" / "devices", temp_ / "lock"};
    fs::create_directory(temp_ / "bpf");
    ASSERT_EQ(mount("bpf", (temp_ / "bpf").c_str(), "bpf", 0, nullptr), 0)
        << strerror(errno);
    mounted_ = true;
    cgroup_ = fs::path("/sys/fs/cgroup") / temp_.filename();
    ASSERT_TRUE(fs::create_directory(cgroup_));
    groups_.push_back(cgroup_);
    device_keys_by_slot_ = {
        {"null", {{BPF_DEVCG_DEV_CHAR, 1, 3}}},
        {"zero", {{BPF_DEVCG_DEV_CHAR, 1, 5}}},
        {"pair", {{BPF_DEVCG_DEV_CHAR, 1, 3}, {BPF_DEVCG_DEV_CHAR, 1, 5}}},
        {"block", {{BPF_DEVCG_DEV_BLOCK, 1, 3}}}};
    runtime_ = std::make_unique<BpfRuntimeInfo>(paths_);
    auto result = runtime_->Initialize(device_keys_by_slot_);
    ASSERT_TRUE(result.has_value()) << result.error();
  }

  void TearDown() override {
    runtime_.reset();
    for (auto it = groups_.rbegin(); it != groups_.rend(); ++it) {
      EXPECT_EQ(rmdir(it->c_str()), 0) << *it << ": " << strerror(errno);
    }
    if (!temp_.empty()) {
      // These are the three pins owned by this test's private bpffs mount.
      for (const char* name :
           {"device_access", "device_policies", "managed_devices"}) {
        std::error_code ec;
        fs::remove(paths_.pins / name, ec);
      }
      rmdir(paths_.pins.c_str());
      for (const char* name :
           {"device_access", "device_policies", "managed_devices"}) {
        std::error_code ec;
        fs::remove(temp_ / "bpf" / "capacity" / name, ec);
      }
      rmdir((temp_ / "bpf" / "capacity").c_str());
      if (mounted_) {
        EXPECT_EQ(umount((temp_ / "bpf").c_str()), 0);
      }
      rmdir((temp_ / "bpf").c_str());
      unlink((temp_ / "lock").c_str());
      EXPECT_EQ(rmdir(temp_.c_str()), 0);
    }
  }

  fs::path Group(const fs::path& parent, const char* name) {
    auto path = parent / name;
    if (fs::create_directory(path)) groups_.push_back(path);
    return path;
  }

  int RunIn(const fs::path& group, const std::function<int()>& operation) {
    pid_t child = fork();
    if (child < 0) return -1;
    if (child == 0) {
      std::ofstream procs(group / "cgroup.procs");
      procs << getpid();
      procs.close();
      if (procs.fail()) _exit(254);
      _exit(operation());
    }
    int status{};
    if (waitpid(child, &status, 0) < 0 || !WIFEXITED(status)) return -1;
    return WEXITSTATUS(status);
  }

  // Return errno from an open inside the target cgroup. In particular, denial
  // must be EPERM rather than an unrelated open/migration failure.
  int OpenIn(const fs::path& group, const char* device, int flags) {
    return RunIn(group, [=] {
      int fd = open(device, flags | O_CLOEXEC);
      int error = fd < 0 ? errno : 0;
      if (fd >= 0) close(fd);
      return error;
    });
  }

  int MknodIn(const fs::path& group) {
    return RunIn(group, [this] {
      if (mknod((temp_ / "node").c_str(), S_IFCHR | 0600, makedev(1, 3)) < 0)
        return errno;
      unlink((temp_ / "node").c_str());
      return 0;
    });
  }

  uint32_t PinnedId(const char* name, bool program = false) {
    int fd = bpf_obj_get((paths_.pins / name).c_str());
    if (fd < 0) return 0;
    bpf_map_info map{};
    bpf_prog_info prog{};
    uint32_t size = program ? sizeof(prog) : sizeof(map);
    int result = bpf_obj_get_info_by_fd(
        fd, program ? static_cast<void*>(&prog) : static_cast<void*>(&map),
        &size);
    close(fd);
    return result < 0 ? 0 : (program ? prog.id : map.id);
  }

  void AttachUnready(const fs::path& group) {
    int prog_fd = bpf_obj_get((paths_.pins / "device_access").c_str());
    int cg_fd = open(group.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    EXPECT_GE(prog_fd, 0);
    EXPECT_GE(cg_fd, 0);
    EXPECT_EQ(bpf_prog_attach(prog_fd, cg_fd, BPF_CGROUP_DEVICE,
                              BPF_F_ALLOW_OVERRIDE),
              0);
    if (cg_fd >= 0) close(cg_fd);
    if (prog_fd >= 0) close(prog_fd);
  }

  int PolicyCount() {
    int fd = bpf_obj_get((paths_.pins / "device_policies").c_str());
    if (fd < 0) return -1;
    bpf_cgroup_storage_key key{}, next{};
    const bpf_cgroup_storage_key* previous = nullptr;
    int count = 0;
    while (bpf_map_get_next_key(fd, previous, &next) == 0) {
      key = next;
      previous = &key;
      // Linux 6.6's legacy storage iterator can return the list sentinel as
      // an extra key. Count only entries that can actually be looked up.
      DevicePolicy policy{};
      if (bpf_map_lookup_elem(fd, &key, &policy) == 0)
        ++count;
      else if (errno != ENOENT) {
        close(fd);
        return -1;
      }
    }
    if (errno != ENOENT) count = -1;
    close(fd);
    return count;
  }

  fs::path temp_, cgroup_;
  bool mounted_{};
  std::vector<fs::path> groups_;
  BpfRuntimeInfo::Paths paths_;
  ManagedDeviceKeysBySlot device_keys_by_slot_;
  std::unique_ptr<BpfRuntimeInfo> runtime_;
};

TEST_F(BpfDeviceKernel, InheritanceUpdatesRestartAndReclamation) {
  auto indices = runtime_->DeviceIndicesBySlot();
  EXPECT_EQ(indices.at("null").front(), indices.at("pair").front());
  EXPECT_EQ(indices.at("zero").front(), indices.at("pair").back());
  EXPECT_NE(indices.at("null"), indices.at("block"));
  EXPECT_EQ(PolicyCount(), 0);
  auto step = Group(cgroup_, "overflow");
  step = Group(step, "job");
  step = Group(step, "step");
  ASSERT_TRUE(runtime_->SetDeviceAccess(step, {"null"}, true, true, true));
  auto task = Group(step, "task");
  auto leaf = task;
  for (int i = 0; i < 70; ++i) leaf = Group(leaf, "user");
  EXPECT_EQ(OpenIn(leaf, "/dev/null", O_RDWR), 0);
  EXPECT_EQ(OpenIn(leaf, "/dev/zero", O_RDONLY), EPERM);
  EXPECT_EQ(OpenIn(leaf, "/dev/full", O_RDONLY), 0);  // Unmanaged device.
  EXPECT_EQ(MknodIn(leaf), 0);
  EXPECT_EQ(PolicyCount(), 1);  // No policy allocated for the leaf/ancestors.

  ASSERT_TRUE(runtime_->SetDeviceAccess(task, {}, true, true, true));
  EXPECT_EQ(OpenIn(leaf, "/dev/null", O_RDONLY), EPERM);
  EXPECT_EQ(OpenIn(leaf, "/dev/zero", O_RDONLY), EPERM);
  EXPECT_EQ(OpenIn(leaf, "/dev/full", O_RDONLY), 0);
  EXPECT_EQ(MknodIn(leaf), EPERM);
  EXPECT_EQ(PolicyCount(), 2);
  EXPECT_EQ(RunIn(leaf,
                  [&] {
                    if (rmdir(leaf.c_str()) == 0 || errno != EBUSY) return 253;
                    int fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
                    if (fd < 0) return errno;
                    close(fd);
                    return 0;
                  }),
            EPERM);
  ASSERT_TRUE(runtime_->SetDeviceAccess(step, {"pair"}, true, true, true));
  EXPECT_EQ(OpenIn(leaf, "/dev/zero", O_RDONLY), EPERM);  // Task stays local.
  ASSERT_TRUE(runtime_->SetDeviceAccess(task, {"zero"}, true, true, true));
  EXPECT_EQ(OpenIn(leaf, "/dev/zero", O_RDWR), 0);
  EXPECT_EQ(OpenIn(leaf, "/dev/null", O_RDONLY), EPERM);
  EXPECT_FALSE(runtime_->SetDeviceAccess(task, {"unknown"}, true, true, true));
  EXPECT_EQ(OpenIn(leaf, "/dev/zero", O_RDWR), 0);  // Failed update preserved.

  const auto program_id = PinnedId("device_access", true);
  const auto devices_id = PinnedId("managed_devices");
  const auto policies_id = PinnedId("device_policies");
  ASSERT_NE(program_id, 0);
  runtime_.reset();
  EXPECT_EQ(OpenIn(leaf, "/dev/null", O_RDONLY), EPERM);
  runtime_ = std::make_unique<BpfRuntimeInfo>(paths_);
  ASSERT_TRUE(runtime_->Initialize(device_keys_by_slot_));
  EXPECT_EQ(runtime_->DeviceIndicesBySlot(), indices);
  EXPECT_EQ(PinnedId("device_access", true), program_id);
  EXPECT_EQ(PinnedId("managed_devices"), devices_id);
  EXPECT_EQ(PinnedId("device_policies"), policies_id);
  EXPECT_EQ(OpenIn(leaf, "/dev/zero", O_RDWR), 0);
  auto changed = device_keys_by_slot_;
  changed["added"] = {{BPF_DEVCG_DEV_CHAR, 1, 7}};
  EXPECT_FALSE(runtime_->Initialize(changed));
  EXPECT_EQ(runtime_->DeviceIndicesBySlot(), indices);

  BpfRuntimeInfo supervisor(paths_);
  ASSERT_TRUE(supervisor.Connect(indices));
  ASSERT_TRUE(supervisor.SetDeviceAccess(task, {}, true, false, true));
  EXPECT_EQ(OpenIn(leaf, "/dev/null", O_RDONLY), EPERM);
  EXPECT_EQ(OpenIn(leaf, "/dev/null", O_WRONLY), 0);
  EXPECT_EQ(OpenIn(leaf, "/dev/null", O_RDWR), EPERM);

  // A non-ready attachment rejects managed devices before publication.
  auto staging = Group(step, "staging");
  AttachUnready(staging);
  EXPECT_EQ(OpenIn(staging, "/dev/null", O_RDONLY), EPERM);
  EXPECT_EQ(OpenIn(staging, "/dev/full", O_RDONLY), 0);

  // Remove only this test's cgroups. Kernel RCU/workqueue reclamation is async.
  for (auto it = groups_.rbegin(); it != groups_.rend(); ++it)
    ASSERT_EQ(rmdir(it->c_str()), 0) << *it << ": " << strerror(errno);
  groups_.clear();
  for (int i = 0; i < 200 && PolicyCount() != 0; ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  EXPECT_EQ(PolicyCount(), 0);
  EXPECT_EQ(PinnedId("device_access", true), program_id);
}

TEST_F(BpfDeviceKernel, CapacityAndInvalidIndex) {
  // A separate, empty runtime tests the exact unique-device capacity.
  BpfRuntimeInfo::Paths capacity_paths = paths_;
  capacity_paths.pins = temp_ / "bpf" / "capacity";
  BpfRuntimeInfo capacity(capacity_paths);
  ManagedDeviceKeysBySlot devices;
  auto& keys = devices["all"];
  for (uint32_t i = 0; i <= MAX_MANAGED_DEVICES; ++i)
    keys.push_back({BPF_DEVCG_DEV_CHAR, 1, i});
  EXPECT_FALSE(capacity.Initialize(devices));
  EXPECT_FALSE(fs::exists(capacity_paths.pins));
  keys.pop_back();
  auto result = capacity.Initialize(devices);
  ASSERT_TRUE(result) << result.error();
  EXPECT_EQ(capacity.DeviceIndicesBySlot().at("all").size(),
            MAX_MANAGED_DEVICES);
  for (const char* name :
       {"device_access", "managed_devices", "device_policies"})
    ASSERT_TRUE(fs::remove(capacity_paths.pins / name));
  ASSERT_TRUE(fs::remove(capacity_paths.pins));

  auto task = Group(cgroup_, "task");
  ASSERT_TRUE(runtime_->SetDeviceAccess(task, {"null"}, true, true, true));
  int fd = bpf_obj_get((paths_.pins / "managed_devices").c_str());
  ASSERT_GE(fd, 0);
  DeviceKey key{BPF_DEVCG_DEV_CHAR, 1, 3};
  uint32_t invalid = MAX_MANAGED_DEVICES;
  EXPECT_EQ(bpf_map_update_elem(fd, &key, &invalid, BPF_EXIST), 0);
  close(fd);
  EXPECT_EQ(OpenIn(task, "/dev/null", O_RDONLY), EPERM);
  BpfRuntimeInfo recovered(paths_);
  EXPECT_FALSE(recovered.Initialize(device_keys_by_slot_));
}

TEST_F(BpfDeviceKernel, StepAndTaskPoliciesSurviveRuntimeRestart) {
  auto job = Group(Group(cgroup_, "overflow"), "job_1");
  auto step = Group(job, "step_2");
  auto system = Group(step, "system");
  auto user = Group(step, "user");
  auto inherited_task = Group(user, "task_1");
  auto independent_task = Group(user, "task_2");
  auto denied_task = Group(user, "task_3");
  auto inherited_leaf = Group(inherited_task, "user_created");
  auto independent_leaf = Group(independent_task, "user_created");
  auto denied_leaf = Group(denied_task, "user_created");
  ASSERT_TRUE(runtime_->SetDeviceAccess(job, {"pair"}, true, true, true));
  ASSERT_TRUE(runtime_->SetDeviceAccess(system, {"null"}, true, true, true));
  ASSERT_TRUE(runtime_->SetDeviceAccess(user, {"pair"}, true, true, true));
  ASSERT_TRUE(
      runtime_->SetDeviceAccess(independent_task, {"zero"}, true, true, true));
  ASSERT_TRUE(runtime_->SetDeviceAccess(denied_task, {}, true, true, true));

  auto check_access = [&] {
    EXPECT_EQ(OpenIn(system, "/dev/null", O_RDWR), 0);
    EXPECT_EQ(OpenIn(system, "/dev/zero", O_RDONLY), EPERM);
    EXPECT_EQ(OpenIn(user, "/dev/null", O_RDWR), 0);
    EXPECT_EQ(OpenIn(user, "/dev/zero", O_RDWR), 0);
    EXPECT_EQ(OpenIn(inherited_leaf, "/dev/null", O_RDWR), 0);
    EXPECT_EQ(OpenIn(inherited_leaf, "/dev/zero", O_RDWR), 0);
    EXPECT_EQ(OpenIn(independent_leaf, "/dev/null", O_RDONLY), EPERM);
    EXPECT_EQ(OpenIn(independent_leaf, "/dev/zero", O_RDWR), 0);
    EXPECT_EQ(OpenIn(denied_leaf, "/dev/null", O_RDONLY), EPERM);
    EXPECT_EQ(OpenIn(denied_leaf, "/dev/zero", O_RDONLY), EPERM);
    EXPECT_EQ(OpenIn(denied_leaf, "/dev/full", O_RDONLY), 0);
    EXPECT_EQ(PolicyCount(), 5);  // No policy for inherited tasks/descendants.
  };
  check_access();

  const auto program_id = PinnedId("device_access", true);
  const auto devices_id = PinnedId("managed_devices");
  const auto policies_id = PinnedId("device_policies");
  runtime_.reset();
  check_access();

  // Reopen the runtime only. No Job/Step/Task policy replay, attachment, or
  // policy validation is needed to preserve enforcement across a restart.
  runtime_ = std::make_unique<BpfRuntimeInfo>(paths_);
  ASSERT_TRUE(runtime_->Initialize(device_keys_by_slot_));
  EXPECT_EQ(PinnedId("device_access", true), program_id);
  EXPECT_EQ(PinnedId("managed_devices"), devices_id);
  EXPECT_EQ(PinnedId("device_policies"), policies_id);
  check_access();
}
}  // namespace
