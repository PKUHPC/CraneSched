// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

#include "OomMonitor.h"

#include <fcntl.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <fstream>
#include <optional>
#include <sstream>

#include "crane/Logger.h"

namespace Craned::Common {
namespace {

std::error_code SystemError() {
  return {errno ? errno : EIO, std::generic_category()};
}

}  // namespace

struct OomMonitor::Watch {
  std::filesystem::path path;
  uint64_t baseline{0};
  bool cgroup_v2{false};
  bool notified{false};
  bool read_error_logged{false};
  int event_fd{-1};
  int control_fd{-1};

  ~Watch() {
    if (event_fd != -1) close(event_fd);
    if (control_fd != -1) close(control_fd);
  }

  std::expected<bool, std::error_code> Sample() {
    if (event_fd == -1) {
      auto count = ReadOomKillCount(path, cgroup_v2);
      if (!count) return std::unexpected(count.error());
      return *count > baseline;
    }

    uint64_t count;
    ssize_t n;
    do {
      n = read(event_fd, &count, sizeof(count));
    } while (n < 0 && errno == EINTR);
    if (n == sizeof(count)) {
      notified |= count != 0;
    } else if (n < 0 && errno == EAGAIN) {
      // eventfd retains notifications until read, even without a poll thread.
    } else {
      return std::unexpected(n < 0 ? SystemError()
                                   : std::make_error_code(std::errc::io_error));
    }
    return notified;
  }

  void LogError(const std::error_code& error) {
    if (read_error_logged) return;
    read_error_logged = true;
    CRANE_WARN("[OOM] Cannot sample {}: {}", path, error.message());
  }
};

OomMonitor::OomMonitor() = default;
OomMonitor::~OomMonitor() = default;

std::expected<uint64_t, std::error_code> OomMonitor::ReadOomKillCount(
    const std::filesystem::path& path, bool cgroup_v2) {
  errno = 0;
  std::ifstream input(path /
                      (cgroup_v2 ? "memory.events" : "memory.oom_control"));
  if (!input.is_open()) return std::unexpected(SystemError());

  std::optional<uint64_t> count;
  bool valid_v1_control = false;
  bool valid_v1_state = false;
  std::string line;
  while (std::getline(input, line)) {
    std::istringstream fields(line);
    std::string key, value, extra;
    if (!(fields >> key)) continue;
    if (!(fields >> value) || (fields >> extra))
      return std::unexpected(std::make_error_code(std::errc::invalid_argument));
    uint64_t number;
    auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), number);
    if (error != std::errc{} || end != value.data() + value.size())
      return std::unexpected(std::make_error_code(std::errc::invalid_argument));
    if (key == "oom_kill") {
      if (count.has_value())
        return std::unexpected(
            std::make_error_code(std::errc::invalid_argument));
      count = number;
    } else if (key == "oom_kill_disable") {
      valid_v1_control = number <= 1;
    } else if (key == "under_oom") {
      valid_v1_state = number <= 1;
    }
  }
  if (input.bad())
    return std::unexpected(std::make_error_code(std::errc::io_error));
  if (count) return *count;
  if (cgroup_v2 || (valid_v1_control && valid_v1_state))
    return std::unexpected(std::make_error_code(std::errc::not_supported));
  return std::unexpected(std::make_error_code(std::errc::invalid_argument));
}

std::unique_ptr<OomMonitor::Watch> OomMonitor::MakeWatch_(
    const std::filesystem::path& path, bool cgroup_v2, bool events_only) {
  auto watch = std::make_unique<Watch>();
  watch->path = path;
  watch->cgroup_v2 = cgroup_v2;
  auto count = ReadOomKillCount(path, cgroup_v2);
  if (count && !events_only) {
    watch->baseline = *count;
    CRANE_DEBUG("[OOM] Monitoring oom_kill counter at {} (baseline {})", path,
                *count);
    return watch;
  }
  if (!count && (cgroup_v2 || count.error() != std::errc::not_supported)) {
    CRANE_WARN("[OOM] Cannot initialize counter at {}: {}", path,
               count.error().message());
    return nullptr;
  }

  // v1 kernels without oom_kill (e.g. Linux 3.10) require event registration.
  watch->control_fd =
      open((path / "memory.oom_control").c_str(), O_RDONLY | O_CLOEXEC);
  if (watch->control_fd != -1)
    watch->event_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  if (watch->control_fd == -1 || watch->event_fd == -1) {
    CRANE_WARN("[OOM] Cannot open notification descriptors at {}: {}", path,
               SystemError().message());
    return nullptr;
  }
  int registration_fd =
      open((path / "cgroup.event_control").c_str(), O_WRONLY | O_CLOEXEC);
  if (registration_fd == -1) {
    CRANE_WARN("[OOM] Cannot open cgroup.event_control at {}: {}", path,
               SystemError().message());
    return nullptr;
  }
  auto registration =
      std::to_string(watch->event_fd) + " " + std::to_string(watch->control_fd);
  ssize_t n;
  do {
    n = write(registration_fd, registration.data(), registration.size());
  } while (n < 0 && errno == EINTR);
  auto error = SystemError();
  close(registration_fd);
  if (n != static_cast<ssize_t>(registration.size())) {
    CRANE_WARN("[OOM] Cannot register notification at {}: {}", path,
               error.message());
    return nullptr;
  }
  CRANE_DEBUG("[OOM] Monitoring v1 OOM notifications at {}", path);
  return watch;
}

bool OomMonitor::Start(const std::filesystem::path& step_path,
                       const std::filesystem::path& job_path, bool cgroup_v2) {
  m_watches_.clear();
  m_job_watch_.reset();
  m_evidence_ = {};
  m_cgroup_v2_ = cgroup_v2;
  m_step_path_ = step_path;
  m_started_ = true;

  auto count = ReadOomKillCount(step_path, cgroup_v2);
  // Register the ancestor first, before any child watcher or workload starts.
  // Parent-limit notifications reach sibling steps on old v1 kernels.
  if (!cgroup_v2 && !count && count.error() == std::errc::not_supported)
    m_job_watch_ = MakeWatch_(job_path, false, true);
  auto watch = MakeWatch_(step_path, cgroup_v2, false);
  if (!watch) return false;
  bool scoped = watch->event_fd == -1 || m_job_watch_ != nullptr;
  m_watches_.push_back(std::move(watch));
  if (!scoped)
    CRANE_WARN(
        "[OOM] Ancestor monitoring unavailable for {}; v1 notifications "
        "cannot be attributed to this step",
        step_path);
  return scoped;
}

bool OomMonitor::AddTask(const std::filesystem::path& task_path) {
  if (!m_started_) return false;
  if (m_cgroup_v2_) return true;  // memory.events includes descendants.
  if (task_path.parent_path() != m_step_path_) return false;
  auto watch = MakeWatch_(task_path, false, false);
  if (!watch) return false;
  m_watches_.push_back(std::move(watch));
  return true;
}

const OomMonitor::Evidence& OomMonitor::Sample() {
  return Sample_(m_step_path_);
}

const OomMonitor::Evidence& OomMonitor::Sample_(
    const std::filesystem::path& sample_path) {
  std::string event_path;
  // Read descendants before the ancestor. A parent notification must be
  // observed before treating its propagated child notification as local.
  for (auto& watch : m_watches_) {
    // A task's final counters are collected at FinishTask, before removal.
    // Avoid reading every active task's files on every other task's exit.
    if (!sample_path.empty() && watch->path != m_step_path_ &&
        watch->path != sample_path)
      continue;
    auto detected = watch->Sample();
    if (!detected) {
      watch->LogError(detected.error());
    } else if (*detected) {
      if (watch->event_fd == -1 && m_evidence_.workload.empty()) {
        m_evidence_.workload =
            "Detected oom_kill counter growth in " + watch->path.string();
      } else if (watch->event_fd != -1) {
        event_path = watch->path.string();
      }
    }
  }
  bool ancestor_known = false;
  bool ancestor_oom = false;
  if (m_job_watch_) {
    auto detected = m_job_watch_->Sample();
    ancestor_known = detected.has_value();
    if (!detected) {
      m_job_watch_->LogError(detected.error());
    } else if (*detected) {
      ancestor_oom = true;
      m_evidence_.ancestor = "OOM notification at job cgroup " +
                             m_job_watch_->path.string() +
                             "; victim step is unknown";
    }
  }
  if (!event_path.empty() && ancestor_known && !ancestor_oom &&
      m_evidence_.workload.empty())
    m_evidence_.workload = "OOM notification in workload cgroup " + event_path;
  return m_evidence_;
}

void OomMonitor::FinishTask(const std::filesystem::path& task_path) {
  Sample_(task_path);
  std::erase_if(m_watches_,
                [&](const auto& watch) { return watch->path == task_path; });
}

void OomMonitor::Stop() {
  Sample_({});
  m_watches_.clear();
  m_job_watch_.reset();
  m_started_ = false;
}

}  // namespace Craned::Common
