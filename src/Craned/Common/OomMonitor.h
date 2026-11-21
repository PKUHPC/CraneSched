// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

#include <cstdint>
#include <expected>
#include <filesystem>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

namespace Craned::Common {

// Owned by the supervisor's event loop. Register before launching processes;
// sample and close each watch before removing its cgroup. v1 eventfd
// notifications describe OOM pressure, not a count of killed processes.
class OomMonitor {
 public:
  struct Evidence {
    // Counter delta or a workload-local OOM notification.
    std::string workload;
    // v1 notification also observed at the job boundary.
    std::string ancestor;
  };

  OomMonitor();
  ~OomMonitor();
  OomMonitor(const OomMonitor&) = delete;
  OomMonitor& operator=(const OomMonitor&) = delete;

  // Paths refer to the memory controller, not the v1 unified name space.
  bool Start(const std::filesystem::path& step_path,
             const std::filesystem::path& job_path, bool cgroup_v2);
  bool AddTask(const std::filesystem::path& task_path);
  void FinishTask(const std::filesystem::path& task_path);
  // Sample the step and ancestor; task evidence is retained by FinishTask.
  const Evidence& Sample();
  void Stop();
  const Evidence& GetEvidence() const { return m_evidence_; }

  // Missing counters are unsupported, not a successful read of zero. I/O and
  // malformed data have distinct error codes. No failcnt/under_oom heuristics.
  static std::expected<uint64_t, std::error_code> ReadOomKillCount(
      const std::filesystem::path& path, bool cgroup_v2);

 private:
  struct Watch;
  static std::unique_ptr<Watch> MakeWatch_(const std::filesystem::path& path,
                                           bool cgroup_v2, bool events_only);
  // Sample the step plus this path, or every watch if the path is empty.
  const Evidence& Sample_(const std::filesystem::path& sample_path);

  bool m_cgroup_v2_{false};
  bool m_started_{false};
  std::filesystem::path m_step_path_;
  std::unique_ptr<Watch> m_job_watch_;
  std::vector<std::unique_ptr<Watch>> m_watches_;
  Evidence m_evidence_;
};

}  // namespace Craned::Common
