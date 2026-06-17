/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

#include "CgroupV2Fs.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <fstream>
#include <sstream>

#include "crane/Tracing.h"

namespace Craned::Common {
namespace {

constexpr std::chrono::milliseconds kKillPollTimeout{500};
constexpr std::chrono::milliseconds kKillPollInterval{10};
constexpr int kJanitorMaxRetries = 20;

int64_t MsSince(std::chrono::steady_clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now() - start)
      .count();
}

std::string PathKey(const std::filesystem::path& path) {
  return path.lexically_normal().string();
}

bool IsNoEnt(int err) { return err == ENOENT || err == ENOTDIR; }

std::optional<std::string> ReadSmallFile(const std::filesystem::path& path,
                                         int* err_out) {
  int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    if (err_out != nullptr) *err_out = errno;
    return std::nullopt;
  }

  std::string content;
  std::array<char, 4096> buf{};
  while (true) {
    ssize_t n = read(fd, buf.data(), buf.size());
    if (n > 0) {
      content.append(buf.data(), static_cast<size_t>(n));
      continue;
    }
    if (n == 0) break;
    if (errno == EINTR) continue;
    if (err_out != nullptr) *err_out = errno;
    close(fd);
    return std::nullopt;
  }

  close(fd);
  if (err_out != nullptr) *err_out = 0;
  return content;
}

bool WriteFile(const std::filesystem::path& path, std::string_view value,
               int* err_out) {
  int fd = open(path.c_str(), O_WRONLY | O_CLOEXEC);
  if (fd < 0) {
    if (err_out != nullptr) *err_out = errno;
    return false;
  }

  size_t written = 0;
  while (written < value.size()) {
    ssize_t n = write(fd, value.data() + written, value.size() - written);
    if (n > 0) {
      written += static_cast<size_t>(n);
      continue;
    }
    if (n == -1 && errno == EINTR) continue;
    if (err_out != nullptr) *err_out = errno;
    close(fd);
    return false;
  }

  close(fd);
  if (err_out != nullptr) *err_out = 0;
  return true;
}

bool MakeDirIfNeeded(const std::filesystem::path& path, bool* created,
                     int* err_out) {
  if (mkdir(path.c_str(), 0755) == 0) {
    if (created != nullptr) *created = true;
    if (err_out != nullptr) *err_out = 0;
    return true;
  }
  if (errno == EEXIST) {
    if (created != nullptr) *created = false;
    if (err_out != nullptr) *err_out = 0;
    return true;
  }
  if (created != nullptr) *created = false;
  if (err_out != nullptr) *err_out = errno;
  return false;
}

std::optional<CgConstant::Controller> ControllerByV2Name(
    std::string_view name) {
  using CgConstant::Controller;
  if (name == "cpu") return Controller::CPU_CONTROLLER_V2;
  if (name == "memory") return Controller::MEMORY_CONTROLLER_V2;
  if (name == "io") return Controller::IO_CONTROLLER_V2;
  if (name == "cpuset") return Controller::CPUSET_CONTROLLER_V2;
  if (name == "pids") return Controller::PIDS_CONTROLLER_V2;
  return std::nullopt;
}

ControllerFlags ParseControllerList(std::string_view text) {
  ControllerFlags flags = NO_CONTROLLER_FLAG;
  std::istringstream iss{std::string{text}};
  std::string token;
  while (iss >> token) {
    if (!token.empty() && (token.front() == '+' || token.front() == '-'))
      token.erase(token.begin());
    if (auto controller = ControllerByV2Name(token); controller.has_value())
      flags |= ControllerFlags{*controller};
  }
  return flags;
}

std::vector<CgConstant::Controller> OrderedV2Controllers() {
  using CgConstant::Controller;
  return {Controller::CPU_CONTROLLER_V2, Controller::MEMORY_CONTROLLER_V2,
          Controller::IO_CONTROLLER_V2, Controller::CPUSET_CONTROLLER_V2,
          Controller::PIDS_CONTROLLER_V2};
}

std::string FormatEnableOps(ControllerFlags flags) {
  std::string ops;
  for (auto controller : OrderedV2Controllers()) {
    if (!(flags & controller)) continue;
    if (!ops.empty()) ops += " ";
    ops += "+";
    ops += CgConstant::GetControllerStringView(controller);
  }
  return ops;
}

std::string FormatControllerNames(ControllerFlags flags) {
  std::string names;
  for (auto controller : OrderedV2Controllers()) {
    if (!(flags & controller)) continue;
    if (!names.empty()) names += ",";
    names += CgConstant::GetControllerStringView(controller);
  }
  return names;
}

bool StatInode(const std::filesystem::path& path, ino_t* inode, int* err_out) {
  struct stat st {};
  if (stat(path.c_str(), &st) != 0) {
    if (err_out != nullptr) *err_out = errno;
    return false;
  }
  if (inode != nullptr) *inode = st.st_ino;
  if (err_out != nullptr) *err_out = 0;
  return true;
}

}  // namespace

struct CgroupV2FsBackend::NodeState {
  explicit NodeState(std::filesystem::path p) : path(std::move(p)) {}

  std::filesystem::path path;
  ino_t inode{0};
  ControllerFlags subtree_enabled{NO_CONTROLLER_FLAG};
  bool exists{false};
  absl::Mutex mu;
};

class CgroupV2FsBackend::Janitor {
 public:
  explicit Janitor(CgroupV2FsBackend* backend) : backend_(backend) {
    worker_ = std::thread([this] { WorkerLoop_(); });
  }

  ~Janitor() { Stop(); }

  void Enqueue(std::filesystem::path path) {
    std::lock_guard lk(mu_);
    if (stopped_) return;
    queue_.push_back({std::move(path), 0, std::chrono::steady_clock::now()});
    cv_.notify_one();
  }

  bool Drain(std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      {
        std::lock_guard lk(mu_);
        if (queue_.empty() && in_flight_ == 0) return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{20});
    }
    return false;
  }

  void Stop() {
    {
      std::lock_guard lk(mu_);
      if (stopped_) return;
      stopped_ = true;
      cv_.notify_all();
    }
    if (worker_.joinable()) worker_.join();
  }

 private:
  struct Item {
    std::filesystem::path path;
    int retry_count;
    std::chrono::steady_clock::time_point first_enqueue;
  };

  void WorkerLoop_() {
    while (true) {
      Item item;
      {
        std::unique_lock lk(mu_);
        cv_.wait(lk, [this] { return stopped_ || !queue_.empty(); });
        if (queue_.empty()) {
          if (stopped_) break;
          continue;
        }
        item = std::move(queue_.front());
        queue_.pop_front();
        ++in_flight_;
      }

      auto begin = std::chrono::steady_clock::now();
      CRANE_TRACE_SCOPE_NAMED(span, "cgroup/v2_janitor");
      span.SetAttribute("path", item.path.string());
      span.SetAttribute("retry_count", item.retry_count);
      int err = 0;
      bool ok = backend_->RecursiveRmdir_(item.path, &err);
      span.SetAttribute("delete_elapsed_ms", MsSince(begin));
      span.SetAttribute("errno", err);

      {
        std::lock_guard lk(mu_);
        --in_flight_;
        span.SetAttribute("queue_len", static_cast<int64_t>(queue_.size()));
      }

      if (ok) {
        backend_->ForgetPath_(item.path);
        continue;
      }

      span.SetStatus(crane::StatusCode::kError, "delete_failed");
      {
        std::lock_guard lk(mu_);
        if (stopped_) continue;
      }
      if (item.retry_count >= kJanitorMaxRetries) {
        CRANE_WARN("Cgroup v2 janitor gave up deleting {} after {} retries: {}",
                   item.path.string(), item.retry_count, strerror(err));
        continue;
      }

      std::this_thread::sleep_for(
          std::min(std::chrono::milliseconds{5000},
                   std::chrono::milliseconds{200 * (item.retry_count + 1)}));
      item.retry_count++;
      Enqueue(std::move(item.path));
    }
  }

  CgroupV2FsBackend* backend_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Item> queue_;
  size_t in_flight_{0};
  bool stopped_{false};
  std::thread worker_;
};

CgroupV2FsBackend::CgroupV2FsBackend(CgroupV2CleanupMode cleanup_mode)
    : CgroupV2FsBackend(cleanup_mode, CgConstant::kSystemCgPathPrefix) {}

CgroupV2FsBackend::CgroupV2FsBackend(CgroupV2CleanupMode cleanup_mode,
                                     std::filesystem::path root_path)
    : root_path_(std::move(root_path)), cleanup_mode_(cleanup_mode) {
  janitor_ = std::make_unique<Janitor>(this);
}

CgroupV2FsBackend::~CgroupV2FsBackend() {
  if (janitor_) {
    if (!janitor_->Drain(std::chrono::seconds{5})) {
      CRANE_WARN("Cgroup v2 janitor did not drain before backend shutdown");
    }
    janitor_->Stop();
  }
}

bool CgroupV2FsBackend::Probe(ControllerFlags mounted_controllers) {
  int err = 0;
  if (!ReadSmallFile(root_path_ / "cgroup.controllers", &err).has_value()) {
    CRANE_WARN("Cgroup v2 fast path disabled: cannot read {}: {}",
               (root_path_ / "cgroup.controllers").string(), strerror(err));
    return false;
  }
  if (!(mounted_controllers & CgConstant::Controller::CPU_CONTROLLER_V2) ||
      !(mounted_controllers & CgConstant::Controller::MEMORY_CONTROLLER_V2) ||
      !(mounted_controllers & CgConstant::Controller::IO_CONTROLLER_V2) ||
      !(mounted_controllers & CgConstant::Controller::CPUSET_CONTROLLER_V2)) {
    CRANE_WARN("Cgroup v2 fast path disabled: required controllers missing");
    return false;
  }
  ControllerFlags required =
      NO_CONTROLLER_FLAG | CgConstant::Controller::CPU_CONTROLLER_V2 |
      CgConstant::Controller::MEMORY_CONTROLLER_V2 |
      CgConstant::Controller::IO_CONTROLLER_V2 |
      CgConstant::Controller::CPUSET_CONTROLLER_V2;
  if (!EnsureSubtreeControllers_(root_path_, required)) {
    CRANE_WARN(
        "Cgroup v2 fast path disabled: failed to prepare root subtree "
        "controllers");
    return false;
  }
  CRANE_INFO("Cgroup v2 fast path enabled cleanup_mode={}",
             cleanup_mode_ == CgroupV2CleanupMode::ASYNC_RMDIR ? "async_rmdir"
                                                               : "sync_rmdir");
  return true;
}

CraneExpected<CgroupV2CreateResult> CgroupV2FsBackend::CreateOrOpen(
    const std::string& cgroup_name, ControllerFlags required_controllers,
    bool retrieve) {
  return CreateOrOpenUnlocked_(cgroup_name, required_controllers, retrieve);
}

bool CgroupV2FsBackend::WriteControllerFile(
    const std::string& cgroup_name, CgConstant::ControllerFile controller_file,
    std::string_view value) {
  auto begin = std::chrono::steady_clock::now();
  auto path = FullPath_(cgroup_name) /
              CgConstant::GetControllerFileStringView(controller_file);
  CRANE_TRACE_SCOPE_NAMED(span, "cgroup/v2_resource_write");
  span.SetAttribute("cgroup_name", cgroup_name);
  span.SetAttribute("file", path.filename().string());
  span.SetAttribute("bytes", static_cast<int64_t>(value.size()));

  int err = 0;
  bool ok = WriteFile(path, value, &err);
  span.SetAttribute("errno", err);
  span.SetAttribute("elapsed_ms", MsSince(begin));
  if (!ok) {
    span.SetStatus(crane::StatusCode::kError, "write_failed");
    CRANE_WARN("Failed to write cgroup v2 file {} value {}: {}", path.string(),
               value, strerror(err));
  }
  return ok;
}

bool CgroupV2FsBackend::MigrateProcIn(const std::string& cgroup_name,
                                      pid_t pid) {
  // This path is used by the forked task child before exec().  Do not create
  // spans or log here: async log/export locks inherited across fork can
  // deadlock the child before it execs the user task.
  int err = 0;
  bool ok = WriteFile(FullPath_(cgroup_name) / "cgroup.procs",
                      std::to_string(pid), &err);
  return ok;
}

bool CgroupV2FsBackend::SignalAllProcesses(const std::string& cgroup_name,
                                           int signum) {
  std::vector<pid_t> pids;
  int err = 0;
  if (!ListProcessesRecursive_(FullPath_(cgroup_name), &pids, &err)) {
    CRANE_WARN("Failed to list processes in cgroup {}: {}", cgroup_name,
               strerror(err));
    return false;
  }
  for (pid_t pid : pids) {
    if (kill(-pid, signum) != 0 && errno != ESRCH) {
      CRANE_WARN("Failed to signal process group {} in cgroup {}: {}", pid,
                 cgroup_name, strerror(errno));
      return false;
    }
  }
  return true;
}

bool CgroupV2FsBackend::KillAllProcesses(const std::string& cgroup_name,
                                         int signum) {
  auto begin = std::chrono::steady_clock::now();
  CRANE_TRACE_SCOPE_NAMED(span, "cgroup/v2_kill");
  span.SetAttribute("cgroup_name", cgroup_name);
  span.SetAttribute("signum", signum);

  if (signum != SIGKILL || !kill_supported_.load(std::memory_order_relaxed)) {
    bool ok = SignalAllProcesses(cgroup_name, signum);
    span.SetAttribute("mode", "signal");
    span.SetAttribute("poll_ms", 0);
    span.SetAttribute("elapsed_ms", MsSince(begin));
    if (!ok) span.SetStatus(crane::StatusCode::kError, "signal_failed");
    return ok;
  }

  int err = 0;
  auto kill_path = FullPath_(cgroup_name) / "cgroup.kill";
  if (access(kill_path.c_str(), W_OK) != 0 && errno == ENOENT) {
    kill_supported_.store(false, std::memory_order_relaxed);
    bool ok = SignalAllProcesses(cgroup_name, signum);
    span.SetAttribute("mode", "signal_no_cgroup_kill");
    span.SetAttribute("poll_ms", 0);
    span.SetAttribute("errno", ENOENT);
    span.SetAttribute("elapsed_ms", MsSince(begin));
    if (!ok) span.SetStatus(crane::StatusCode::kError, "signal_failed");
    return ok;
  }

  bool ok = WriteFile(kill_path, "1", &err);
  if (!ok && err == ENOENT) {
    kill_supported_.store(false, std::memory_order_relaxed);
    ok = SignalAllProcesses(cgroup_name, signum);
    span.SetAttribute("mode", "signal_no_cgroup_kill");
  } else {
    span.SetAttribute("mode", "cgroup.kill");
  }

  int64_t poll_ms = 0;
  bool empty = false;
  if (ok) {
    empty = WaitPopulated_(FullPath_(cgroup_name), false, kKillPollTimeout,
                           &poll_ms);
  }
  span.SetAttribute("poll_ms", poll_ms);
  span.SetAttribute("populated_final", !empty);
  span.SetAttribute("errno", err);
  span.SetAttribute("elapsed_ms", MsSince(begin));
  if (!ok || !empty) span.SetStatus(crane::StatusCode::kError, "kill_failed");
  return ok && empty;
}

bool CgroupV2FsBackend::Empty(const std::string& cgroup_name) {
  bool populated = false;
  int err = 0;
  if (!ReadPopulated_(FullPath_(cgroup_name), &populated, &err)) {
    if (IsNoEnt(err)) return true;
    CRANE_WARN("Failed to read cgroup.events for {}: {}", cgroup_name,
               strerror(err));
    return false;
  }
  return !populated;
}

bool CgroupV2FsBackend::Destroy(const std::string& cgroup_name) {
  auto path = FullPath_(cgroup_name);
  if (cleanup_mode_ == CgroupV2CleanupMode::ASYNC_RMDIR) {
    janitor_->Enqueue(path);
    return true;
  }

  int err = 0;
  bool ok = RecursiveRmdir_(path, &err);
  if (!ok && !IsNoEnt(err)) {
    CRANE_WARN("Failed to remove cgroup v2 directory {}: {}", path.string(),
               strerror(err));
  }
  if (ok) ForgetPath_(path);
  return ok || IsNoEnt(err);
}

bool CgroupV2FsBackend::DrainJanitor(std::chrono::milliseconds timeout) {
  return janitor_ == nullptr || janitor_->Drain(timeout);
}

std::filesystem::path CgroupV2FsBackend::FullPath_(
    const std::string& cgroup_name) const {
  return root_path_ / cgroup_name;
}

std::shared_ptr<CgroupV2FsBackend::NodeState> CgroupV2FsBackend::GetNode_(
    const std::filesystem::path& path) {
  auto key = PathKey(path);
  absl::MutexLock lk(&nodes_mu_);
  auto it = nodes_.find(key);
  if (it != nodes_.end()) return it->second;
  auto node = std::make_shared<NodeState>(path);
  nodes_.emplace(key, node);
  return node;
}

bool CgroupV2FsBackend::RefreshNodeLocked_(NodeState& node, int* err_out) {
  ino_t inode = 0;
  if (!StatInode(node.path, &inode, err_out)) {
    node.exists = false;
    node.inode = 0;
    node.subtree_enabled = NO_CONTROLLER_FLAG;
    return false;
  }

  int err = 0;
  auto subtree =
      ReadSmallFile(node.path / "cgroup.subtree_control", &err);
  if (!subtree.has_value()) {
    if (err_out != nullptr) *err_out = err;
    return false;
  }
  node.exists = true;
  node.inode = inode;
  node.subtree_enabled = ParseControllerList(*subtree);
  if (err_out != nullptr) *err_out = 0;
  return true;
}

bool CgroupV2FsBackend::EnsureSubtreeControllers_(
    const std::filesystem::path& parent, ControllerFlags required_controllers) {
  auto begin = std::chrono::steady_clock::now();
  CRANE_TRACE_SCOPE_NAMED(span, "cgroup/v2_subtree_enable");
  span.SetAttribute("parent", parent.string());
  span.SetAttribute("controllers", FormatControllerNames(required_controllers));

  auto node = GetNode_(parent);
  absl::MutexLock lk(&node->mu);

  int err = 0;
  bool cache_valid = false;
  if (node->exists) {
    ino_t inode = 0;
    if (StatInode(parent, &inode, &err) && inode == node->inode) {
      cache_valid = true;
    }
  }

  if (!cache_valid && !RefreshNodeLocked_(*node, &err)) {
    span.SetAttribute("errno", err);
    span.SetAttribute("elapsed_ms", MsSince(begin));
    span.SetStatus(crane::StatusCode::kError, "refresh_failed");
    return false;
  }

  ControllerFlags missing = required_controllers & ~node->subtree_enabled;
  if (!missing) {
    span.SetAttribute("cache_hit", cache_valid);
    span.SetAttribute("write_count", 0);
    span.SetAttribute("errno", 0);
    span.SetAttribute("elapsed_ms", MsSince(begin));
    return true;
  }

  if (cache_valid && !RefreshNodeLocked_(*node, &err)) {
    span.SetAttribute("errno", err);
    span.SetAttribute("elapsed_ms", MsSince(begin));
    span.SetStatus(crane::StatusCode::kError, "refresh_failed");
    return false;
  }

  missing = required_controllers & ~node->subtree_enabled;
  if (!missing) {
    span.SetAttribute("cache_hit", false);
    span.SetAttribute("write_count", 0);
    span.SetAttribute("errno", 0);
    span.SetAttribute("elapsed_ms", MsSince(begin));
    return true;
  }

  auto controllers = ReadSmallFile(parent / "cgroup.controllers", &err);
  if (!controllers.has_value()) {
    span.SetAttribute("errno", err);
    span.SetAttribute("elapsed_ms", MsSince(begin));
    span.SetStatus(crane::StatusCode::kError, "controllers_read_failed");
    return false;
  }
  ControllerFlags available = ParseControllerList(*controllers);
  ControllerFlags unavailable = missing & ~available;
  if (unavailable) {
    CRANE_WARN("Cannot enable unavailable cgroup v2 controllers {} at {}",
               FormatControllerNames(unavailable), parent.string());
    span.SetAttribute("errno", ENODEV);
    span.SetAttribute("elapsed_ms", MsSince(begin));
    span.SetStatus(crane::StatusCode::kError, "controller_unavailable");
    return false;
  }

  std::string ops = FormatEnableOps(missing);
  bool ok = WriteFile(parent / "cgroup.subtree_control", ops, &err);
  span.SetAttribute("cache_hit", false);
  span.SetAttribute("write_count", ok ? 1 : 0);
  span.SetAttribute("errno", err);
  span.SetAttribute("elapsed_ms", MsSince(begin));
  if (!ok) {
    span.SetStatus(crane::StatusCode::kError, "subtree_write_failed");
    CRANE_WARN("Failed to enable cgroup v2 controllers {} at {}: {}", ops,
               parent.string(), strerror(err));
    return false;
  }

  node->subtree_enabled |= missing;
  return true;
}

CraneExpected<CgroupV2CreateResult> CgroupV2FsBackend::CreateOrOpenUnlocked_(
    const std::string& cgroup_name, ControllerFlags required_controllers,
    bool retrieve) {
  auto target = FullPath_(cgroup_name);
  int err = 0;
  ino_t inode = 0;
  if (retrieve) {
    if (!StatInode(target, &inode, &err)) {
      return std::unexpected(CraneErrCode::ERR_CGROUP);
    }
    auto node = GetNode_(target);
    absl::MutexLock lk(&node->mu);
    if (!node->exists || node->inode != inode)
      node->subtree_enabled = NO_CONTROLLER_FLAG;
    node->exists = true;
    node->inode = inode;
    return CgroupV2CreateResult{.inode = inode, .created = false};
  }

  std::vector<std::filesystem::path> created_paths;
  std::filesystem::path current = root_path_;
  auto relative = std::filesystem::path(cgroup_name).lexically_normal();
  for (const auto& part : relative) {
    if (part.empty() || part == ".") continue;
    if (part == "..") {
      CRANE_WARN("Invalid cgroup v2 path contains '..': {}", cgroup_name);
      return std::unexpected(CraneErrCode::ERR_CGROUP);
    }

    if (!EnsureSubtreeControllers_(current, required_controllers)) {
      for (auto it = created_paths.rbegin(); it != created_paths.rend(); ++it) {
        int cleanup_err = 0;
        RecursiveRmdir_(*it, &cleanup_err);
      }
      return std::unexpected(CraneErrCode::ERR_CGROUP);
    }

    current /= part;
    bool created = false;
    auto begin = std::chrono::steady_clock::now();
    CRANE_TRACE_SCOPE_NAMED(span, "cgroup/v2_mkdir");
    span.SetAttribute("path", current.string());
    bool ok = MakeDirIfNeeded(current, &created, &err);
    span.SetAttribute("existed", !created);
    span.SetAttribute("errno", err);
    span.SetAttribute("elapsed_ms", MsSince(begin));
    if (!ok) {
      span.SetStatus(crane::StatusCode::kError, "mkdir_failed");
      for (auto it = created_paths.rbegin(); it != created_paths.rend(); ++it) {
        int cleanup_err = 0;
        RecursiveRmdir_(*it, &cleanup_err);
      }
      return std::unexpected(CraneErrCode::ERR_CGROUP);
    }
    if (created) created_paths.push_back(current);

    if (!StatInode(current, &inode, &err)) {
      span.SetStatus(crane::StatusCode::kError, "stat_failed");
      for (auto it = created_paths.rbegin(); it != created_paths.rend(); ++it) {
        int cleanup_err = 0;
        RecursiveRmdir_(*it, &cleanup_err);
      }
      return std::unexpected(CraneErrCode::ERR_CGROUP);
    }
    auto node = GetNode_(current);
    absl::MutexLock lk(&node->mu);
    if (!node->exists || node->inode != inode)
      node->subtree_enabled = NO_CONTROLLER_FLAG;
    node->exists = true;
    node->inode = inode;
  }

  return CgroupV2CreateResult{.inode = inode, .created = !created_paths.empty()};
}

bool CgroupV2FsBackend::ReadPopulated_(const std::filesystem::path& path,
                                       bool* populated, int* err_out) {
  int err = 0;
  auto events = ReadSmallFile(path / "cgroup.events", &err);
  if (!events.has_value()) {
    if (err_out != nullptr) *err_out = err;
    return false;
  }

  std::istringstream iss{*events};
  std::string key;
  uint64_t value = 0;
  while (iss >> key >> value) {
    if (key == "populated") {
      if (populated != nullptr) *populated = value != 0;
      if (err_out != nullptr) *err_out = 0;
      return true;
    }
  }
  if (err_out != nullptr) *err_out = ENODATA;
  return false;
}

bool CgroupV2FsBackend::WaitPopulated_(const std::filesystem::path& path,
                                       bool expected,
                                       std::chrono::milliseconds timeout,
                                       int64_t* poll_ms_out) {
  auto begin = std::chrono::steady_clock::now();
  auto deadline = begin + timeout;
  while (true) {
    bool populated = false;
    int err = 0;
    if (ReadPopulated_(path, &populated, &err) && populated == expected) {
      if (poll_ms_out != nullptr) *poll_ms_out = MsSince(begin);
      return true;
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      if (poll_ms_out != nullptr) *poll_ms_out = MsSince(begin);
      return false;
    }
    std::this_thread::sleep_for(kKillPollInterval);
  }
}

bool CgroupV2FsBackend::RecursiveRmdir_(const std::filesystem::path& path,
                                        int* err_out) {
  std::error_code ec;
  if (!std::filesystem::exists(path, ec)) {
    if (err_out != nullptr) *err_out = ec ? ec.value() : ENOENT;
    return true;
  }

  for (const auto& entry : std::filesystem::directory_iterator(path, ec)) {
    if (ec) {
      if (err_out != nullptr) *err_out = ec.value();
      return false;
    }
    if (entry.is_directory(ec)) {
      int child_err = 0;
      if (!RecursiveRmdir_(entry.path(), &child_err)) {
        if (err_out != nullptr) *err_out = child_err;
        return false;
      }
    }
  }

  if (!std::filesystem::remove(path, ec) && ec) {
    if (err_out != nullptr) *err_out = ec.value();
    return false;
  }
  if (err_out != nullptr) *err_out = 0;
  return true;
}

bool CgroupV2FsBackend::ListProcessesRecursive_(const std::filesystem::path& path,
                                                std::vector<pid_t>* pids,
                                                int* err_out) {
  int err = 0;
  auto procs = ReadSmallFile(path / "cgroup.procs", &err);
  if (!procs.has_value() && !IsNoEnt(err)) {
    if (err_out != nullptr) *err_out = err;
    return false;
  }
  if (procs.has_value()) {
    std::istringstream iss{*procs};
    pid_t pid = 0;
    while (iss >> pid) pids->push_back(pid);
  }

  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(path, ec)) {
    if (ec) {
      if (err_out != nullptr) *err_out = ec.value();
      return false;
    }
    if (entry.is_directory(ec) &&
        !ListProcessesRecursive_(entry.path(), pids, err_out)) {
      return false;
    }
  }
  if (err_out != nullptr) *err_out = 0;
  return true;
}

void CgroupV2FsBackend::ForgetPath_(const std::filesystem::path& path) {
  auto key = PathKey(path);
  absl::MutexLock lk(&nodes_mu_);
  nodes_.erase(key);
}

}  // namespace Craned::Common
