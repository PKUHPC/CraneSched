/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

#pragma once

#include "CgroupManager.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <filesystem>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

namespace Craned::Common {

struct CgroupV2CreateResult {
  uint64_t inode{0};
  bool created{false};
};

class CgroupV2FsBackend {
 public:
  explicit CgroupV2FsBackend(CgroupV2CleanupMode cleanup_mode);
  CgroupV2FsBackend(CgroupV2CleanupMode cleanup_mode,
                    std::filesystem::path root_path);
  ~CgroupV2FsBackend();

  CgroupV2FsBackend(const CgroupV2FsBackend&) = delete;
  CgroupV2FsBackend(CgroupV2FsBackend&&) = delete;
  CgroupV2FsBackend& operator=(const CgroupV2FsBackend&) = delete;
  CgroupV2FsBackend& operator=(CgroupV2FsBackend&&) = delete;

  bool Probe(ControllerFlags mounted_controllers);

  CraneExpected<CgroupV2CreateResult> CreateOrOpen(
      const std::string& cgroup_name, ControllerFlags required_controllers,
      bool retrieve);

  bool WriteControllerFile(const std::string& cgroup_name,
                           CgConstant::ControllerFile controller_file,
                           std::string_view value);
  bool MigrateProcIn(const std::string& cgroup_name, pid_t pid);
  bool SignalAllProcesses(const std::string& cgroup_name, int signum);
  bool KillAllProcesses(const std::string& cgroup_name, int signum);
  bool Empty(const std::string& cgroup_name);
  bool Destroy(const std::string& cgroup_name);

  bool DrainJanitor(std::chrono::milliseconds timeout);
  CgroupV2CleanupMode CleanupMode() const { return cleanup_mode_; }

 private:
  struct NodeState;
  class Janitor;

  std::filesystem::path FullPath_(const std::string& cgroup_name) const;
  std::shared_ptr<NodeState> GetNode_(const std::filesystem::path& path);
  bool RefreshNodeLocked_(NodeState& node, int* err_out);
  bool EnsureSubtreeControllers_(const std::filesystem::path& parent,
                                 ControllerFlags required_controllers);
  CraneExpected<CgroupV2CreateResult> CreateOrOpenUnlocked_(
      const std::string& cgroup_name, ControllerFlags required_controllers,
      bool retrieve);
  bool ReadPopulated_(const std::filesystem::path& path, bool* populated,
                      int* err_out);
  bool WaitPopulated_(const std::filesystem::path& path, bool expected,
                      std::chrono::milliseconds timeout,
                      int64_t* poll_ms_out);
  bool RecursiveRmdir_(const std::filesystem::path& path, int* err_out);
  bool ListProcessesRecursive_(const std::filesystem::path& path,
                               std::vector<pid_t>* pids, int* err_out);
  void ForgetPath_(const std::filesystem::path& path);

  std::filesystem::path root_path_{CgConstant::kSystemCgPathPrefix};
  CgroupV2CleanupMode cleanup_mode_;
  std::atomic_bool kill_supported_{true};

  absl::Mutex nodes_mu_;
  absl::flat_hash_map<std::string, std::shared_ptr<NodeState>> nodes_;

  std::unique_ptr<Janitor> janitor_;
};

}  // namespace Craned::Common
