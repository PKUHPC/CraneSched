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

#include "BpfRuntime.h"

#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <memory>
#include <set>
#include <system_error>
#include <tuple>
#include <utility>

namespace Craned::Common {
namespace {

std::unexpected<std::string> Error(const std::string& operation,
                                   int error = errno) {
  return std::unexpected(operation + ": " +
                         std::system_category().message(error));
}

class FileDescriptor {
 public:
  explicit FileDescriptor(int fd = -1) : m_fd_(fd) {}
  ~FileDescriptor() {
    if (m_fd_ >= 0) close(m_fd_);
  }
  FileDescriptor(FileDescriptor&& other) noexcept
      : m_fd_(std::exchange(other.m_fd_, -1)) {}
  FileDescriptor(const FileDescriptor&) = delete;
  FileDescriptor& operator=(const FileDescriptor&) = delete;
  int Get() const { return m_fd_; }

 private:
  int m_fd_;
};

std::expected<FileDescriptor, std::string> LockRuntime(
    const std::filesystem::path& path) {
  FileDescriptor fd(open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600));
  if (fd.Get() < 0) return Error("Open BPF runtime lock");
  while (flock(fd.Get(), LOCK_EX) < 0) {
    if (errno != EINTR) return Error("Lock BPF runtime");
  }
  return fd;
}

using DeviceIdentity = std::tuple<uint32_t, uint32_t, uint32_t>;
DeviceIdentity Identity(const DeviceKey& key) {
  return {key.type, key.major, key.minor};
}

std::expected<bpf_map_info, std::string> MapInfo(int fd) {
  bpf_map_info info{};
  uint32_t size = sizeof(info);
  if (bpf_obj_get_info_by_fd(fd, &info, &size) < 0)
    return Error("Read BPF map metadata");
  return info;
}

}  // namespace

static_assert(sizeof(DeviceKey) == 12);
static_assert(sizeof(DevicePolicy) == 1544);

BpfRuntimeInfo::BpfRuntimeInfo() = default;
BpfRuntimeInfo::BpfRuntimeInfo(Paths paths) : m_paths_(std::move(paths)) {}
BpfRuntimeInfo::~BpfRuntimeInfo() { Close_(); }

void BpfRuntimeInfo::Close_() {
  // Pins and cgroup attachments outlive this process. Never unpin on a normal
  // shutdown, even when there are currently no policy cgroups.
  for (int* fd : {&m_program_fd_, &m_devices_fd_, &m_policies_fd_}) {
    if (*fd >= 0) close(std::exchange(*fd, -1));
  }
  m_program_id_ = 0;
}

BpfResult BpfRuntimeInfo::Open_() {
  if (m_program_fd_ >= 0) return {};
  m_program_fd_ = bpf_obj_get((m_paths_.pins / "device_access").c_str());
  if (m_program_fd_ < 0) return Error("Open pinned device program");
  m_devices_fd_ = bpf_obj_get((m_paths_.pins / "managed_devices").c_str());
  m_policies_fd_ = bpf_obj_get((m_paths_.pins / "device_policies").c_str());
  auto result = Validate_();
  if (!result) Close_();
  return result;
}

BpfResult BpfRuntimeInfo::Validate_() {
  auto devices = MapInfo(m_devices_fd_);
  auto policies = MapInfo(m_policies_fd_);
  if (!devices) return std::unexpected(devices.error());
  if (!policies) return std::unexpected(policies.error());
  if (devices->type != BPF_MAP_TYPE_HASH ||
      devices->key_size != sizeof(DeviceKey) ||
      devices->value_size != sizeof(uint32_t) ||
      devices->max_entries != MAX_MANAGED_DEVICES ||
      policies->type != BPF_MAP_TYPE_CGROUP_STORAGE ||
      policies->key_size != sizeof(bpf_cgroup_storage_key) ||
      policies->value_size != sizeof(DevicePolicy))
    return std::unexpected("Incompatible pinned BPF device ABI");

  std::array<uint32_t, 2> map_ids{};
  bpf_prog_info info{};
  info.nr_map_ids = map_ids.size();
  info.map_ids = reinterpret_cast<uint64_t>(map_ids.data());
  uint32_t size = sizeof(info);
  if (bpf_obj_get_info_by_fd(m_program_fd_, &info, &size) < 0)
    return Error("Read pinned device program metadata");
  const std::set<uint32_t> actual(map_ids.begin(), map_ids.end());
  if (info.type != BPF_PROG_TYPE_CGROUP_DEVICE ||
      info.nr_map_ids != map_ids.size() ||
      actual != std::set<uint32_t>{devices->id, policies->id})
    return std::unexpected("Pinned device program references different maps");
  m_program_id_ = info.id;

  m_indices_.clear();
  DeviceKey key{}, next{};
  const DeviceKey* previous = nullptr;
  while (bpf_map_get_next_key(m_devices_fd_, previous, &next) == 0) {
    uint32_t index;
    if (bpf_map_lookup_elem(m_devices_fd_, &next, &index) < 0)
      return Error("Read managed device index");
    if (index >= MAX_MANAGED_DEVICES || !m_indices_.insert(index).second)
      return std::unexpected("Invalid or duplicate managed device index");
    key = next;
    previous = &key;
  }
  if (errno != ENOENT) return Error("Enumerate managed devices");
  return {};
}

BpfResult BpfRuntimeInfo::Create_(const BpfDeviceCatalog& catalog) {
  std::map<DeviceIdentity, uint32_t> assigned;
  for (const auto& [slot, keys] : catalog) {
    if (keys.empty()) return std::unexpected("Empty device slot: " + slot);
    for (const auto& key : keys) {
      if (key.type != BPF_DEVCG_DEV_CHAR && key.type != BPF_DEVCG_DEV_BLOCK)
        return std::unexpected("Invalid managed device type for " + slot);
      assigned.try_emplace(Identity(key), 0);
    }
  }
  if (assigned.size() > MAX_MANAGED_DEVICES)
    return std::unexpected("Too many managed device files (maximum 4096)");

  // Maps are deliberately not auto-pinned by libbpf: publish the program last,
  // after the entire index table is installed. All managers use the same lock.
  std::unique_ptr<bpf_object, decltype(&bpf_object__close)> object(
      bpf_object__open_file(m_paths_.object.c_str(), nullptr),
      bpf_object__close);
  if (!object) return Error("Open device BPF object");
  int rc = bpf_object__load(object.get());
  if (rc) return Error("Load device BPF object", -rc);
  auto* program =
      bpf_object__find_program_by_name(object.get(), "craned_device_access");
  auto* devices = bpf_object__find_map_by_name(object.get(), "managed_devices");
  auto* policies =
      bpf_object__find_map_by_name(object.get(), "device_policies");
  if (!program || !devices || !policies)
    return std::unexpected("Device BPF object is missing a program or map");

  uint32_t index = 0;
  for (auto& [identity, value] : assigned) {
    const auto& [type, major, minor] = identity;
    DeviceKey key{type, major, minor};
    value = index++;
    if (bpf_map_update_elem(bpf_map__fd(devices), &key, &value, BPF_NOEXIST) <
        0)
      return Error("Initialize managed device index");
  }

  std::error_code ec;
  std::filesystem::create_directories(m_paths_.pins, ec);
  if (ec) return std::unexpected("Create BPF pin directory: " + ec.message());
  // Never replace partial state left by another bootstrap. It needs explicit
  // recovery; an ordinary restart must not invent a new device numbering.
  for (const char* name :
       {"managed_devices", "device_policies", "device_access"})
    if (std::filesystem::exists(m_paths_.pins / name))
      return std::unexpected(
          "Incomplete pinned BPF runtime; recovery required");

  std::vector<std::filesystem::path> published;
  for (const auto& [name, fd] : std::array<std::pair<const char*, int>, 3>{
           {{"managed_devices", bpf_map__fd(devices)},
            {"device_policies", bpf_map__fd(policies)},
            {"device_access", bpf_program__fd(program)}}}) {
    auto path = m_paths_.pins / name;
    if (bpf_obj_pin(fd, path.c_str()) < 0) {
      auto error = Error("Pin device BPF state");
      for (const auto& own_pin : published)
        std::filesystem::remove(own_pin, ec);
      return error;
    }
    published.push_back(std::move(path));
  }
  return Open_();
}

BpfResult BpfRuntimeInfo::ResolveIndices_(const BpfDeviceCatalog& catalog) {
  BpfDeviceIndices resolved;
  std::set<DeviceIdentity> configured;
  for (const auto& [slot, keys] : catalog) {
    if (keys.empty()) return std::unexpected("Empty device slot: " + slot);
    for (const auto& key : keys) {
      uint32_t index;
      if (bpf_map_lookup_elem(m_devices_fd_, &key, &index) < 0)
        return std::unexpected(
            "Device catalog changed; Reconfigure required: " + slot);
      resolved[slot].push_back(index);
      configured.insert(Identity(key));
    }
  }
  if (configured.size() != m_indices_.size())
    return std::unexpected("Managed device set changed; Reconfigure required");
  m_device_indices_ = std::move(resolved);
  return {};
}

BpfResult BpfRuntimeInfo::Initialize(const BpfDeviceCatalog& catalog) {
  std::lock_guard lock(m_mutex_);
  auto process_lock = LockRuntime(m_paths_.lock);
  if (!process_lock) return std::unexpected(process_lock.error());
  if (m_program_fd_ < 0) {
    auto result = Open_();
    if (!result) {
      std::error_code ec;
      const bool exists =
          std::filesystem::exists(m_paths_.pins / "device_access", ec);
      if (exists || ec) return result;
      result = Create_(catalog);
      if (!result) return result;
    }
  }
  return ResolveIndices_(catalog);
}

BpfResult BpfRuntimeInfo::Connect(const BpfDeviceIndices& indices) {
  std::lock_guard lock(m_mutex_);
  auto process_lock = LockRuntime(m_paths_.lock);
  if (!process_lock) return std::unexpected(process_lock.error());
  if (auto result = Open_(); !result) return result;
  for (const auto& [slot, values] : indices) {
    if (values.empty()) return std::unexpected("Empty device slot: " + slot);
    for (uint32_t index : values)
      if (!m_indices_.contains(index))
        return std::unexpected("Unknown device index for slot: " + slot);
  }
  m_device_indices_ = indices;
  return {};
}

BpfDeviceIndices BpfRuntimeInfo::DeviceIndices() const {
  std::lock_guard lock(m_mutex_);
  return m_device_indices_;
}

std::expected<bool, std::string> BpfRuntimeInfo::Attached_(
    int cgroup_fd) const {
  uint32_t count = 0, flags = 0;
  if (bpf_prog_query(cgroup_fd, BPF_CGROUP_DEVICE, 0, &flags, nullptr, &count) <
      0)
    return Error("Query device program attachment");
  if (!count) return false;
  std::vector<uint32_t> ids(count);
  if (bpf_prog_query(cgroup_fd, BPF_CGROUP_DEVICE, 0, &flags, ids.data(),
                     &count) < 0)
    return Error("Read device program attachment");
  if (count != 1 || ids.front() != m_program_id_ ||
      flags != BPF_F_ALLOW_OVERRIDE)
    return std::unexpected(
        "Cgroup has a different device program or attach mode");
  return true;
}

BpfResult BpfRuntimeInfo::SetDeviceAccess(
    const std::filesystem::path& cgroup,
    const std::unordered_set<std::string>& slots, bool read, bool write,
    bool mknod) {
  std::lock_guard lock(m_mutex_);
  if (m_program_fd_ < 0)
    return std::unexpected("BPF runtime is not initialized");
  auto process_lock = LockRuntime(m_paths_.lock);
  if (!process_lock) return std::unexpected(process_lock.error());

  DevicePolicy policy{};
  // A false flag means this operation is not restricted, matching the existing
  // SetDeviceAccess contract. Allocated devices allow all three operations.
  for (uint32_t index : m_indices_) {
    auto word = index >> 6;
    uint64_t bit = 1ULL << (index & 63);
    if (!read) policy.read_bits[word] |= bit;
    if (!write) policy.write_bits[word] |= bit;
    if (!mknod) policy.mknod_bits[word] |= bit;
  }
  for (const auto& slot : slots) {
    auto it = m_device_indices_.find(slot);
    if (it == m_device_indices_.end())
      return std::unexpected("Unknown allocated device slot: " + slot);
    for (uint32_t index : it->second) {
      auto word = index >> 6;
      uint64_t bit = 1ULL << (index & 63);
      policy.read_bits[word] |= bit;
      policy.write_bits[word] |= bit;
      policy.mknod_bits[word] |= bit;
    }
  }
  policy.ready = 1;

  FileDescriptor fd(open(cgroup.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
  if (fd.Get() < 0) return Error("Open policy cgroup");
  struct stat statbuf{};
  if (fstat(fd.Get(), &statbuf) < 0) return Error("Stat policy cgroup");
  bpf_cgroup_storage_key key{};
  key.cgroup_inode_id = statbuf.st_ino;
  key.attach_type = BPF_CGROUP_DEVICE;

  auto attached = Attached_(fd.Get());
  if (!attached) return std::unexpected(attached.error());
  if (!*attached && bpf_prog_attach(m_program_fd_, fd.Get(), BPF_CGROUP_DEVICE,
                                    BPF_F_ALLOW_OVERRIDE) < 0)
    return Error("Attach device program");

  // Attach allocates zeroed storage (ready=0). Publish the complete value with
  // one CGROUP_STORAGE update, which replaces the buffer under kernel RCU.
  // Processes are migrated into new cgroups only after this succeeds.
  if (bpf_map_update_elem(m_policies_fd_, &key, &policy, BPF_EXIST) < 0) {
    auto error = Error("Publish device policy");
    if (!*attached)
      bpf_prog_detach2(m_program_fd_, fd.Get(), BPF_CGROUP_DEVICE);
    return error;
  }
  return {};
}

BpfResult BpfRuntimeInfo::RecoverPolicy(const std::filesystem::path& cgroup,
                                        bool local_policy) {
  std::lock_guard lock(m_mutex_);
  if (m_program_fd_ < 0)
    return std::unexpected("BPF runtime is not initialized");
  auto process_lock = LockRuntime(m_paths_.lock);
  if (!process_lock) return std::unexpected(process_lock.error());
  FileDescriptor fd(open(cgroup.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
  if (fd.Get() < 0) return Error("Open recovered policy cgroup");
  if (local_policy) return RecoverLocalPolicy_(fd.Get());

  auto attached = Attached_(fd.Get());
  if (!attached) return std::unexpected(attached.error());
  if (*attached)
    return std::unexpected("Inherited task has an unexpected device policy");
  FileDescriptor parent_fd(
      openat(fd.Get(), "..", O_RDONLY | O_DIRECTORY | O_CLOEXEC));
  if (parent_fd.Get() < 0) return Error("Open recovered task's step cgroup");
  return RecoverLocalPolicy_(parent_fd.Get());
}

BpfResult BpfRuntimeInfo::RecoverLocalPolicy_(int cgroup_fd) const {
  auto attached = Attached_(cgroup_fd);
  if (!attached) return std::unexpected(attached.error());
  if (!*attached)
    return std::unexpected("Recovered cgroup has no device policy");
  struct stat statbuf{};
  if (fstat(cgroup_fd, &statbuf) < 0)
    return Error("Stat recovered policy cgroup");
  bpf_cgroup_storage_key key{};
  key.cgroup_inode_id = statbuf.st_ino;
  key.attach_type = BPF_CGROUP_DEVICE;
  DevicePolicy policy{};
  if (bpf_map_lookup_elem(m_policies_fd_, &key, &policy) < 0)
    return Error("Read recovered device policy");
  if (!policy.ready)
    return std::unexpected("Recovered device policy is not ready");
  // Do not reattach or rewrite a running cgroup on an ordinary daemon restart.
  return {};
}

BpfResult BpfRuntimeInfo::Reconfigure(const BpfDeviceCatalog&) {
  return std::unexpected("BPF device Reconfigure is not implemented");
}

}  // namespace Craned::Common
