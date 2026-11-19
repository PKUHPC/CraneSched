/**
 * Copyright (c) 2024 Peking University and Peking University
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

#include "crane/Logger.h"

#include <absl/cleanup/cleanup.h>
#include <sys/stat.h>

#include "crane/OS.h"

static LoggerSinks default_sinks{};
static spdlog::async_overflow_policy default_overflow_policy =
    spdlog::async_overflow_policy::block;

namespace {
void SecureLogFile(const spdlog::filename_t& filename, bool create) {
  int flags = O_WRONLY | O_APPEND | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK;
  int fd = open(filename.c_str(), flags | (create ? O_CREAT : 0), 0600);
  if (fd == -1) {
    if (!create && errno == ENOENT) return;
    throw spdlog::spdlog_ex("Failed to open private log " + filename, errno);
  }
  absl::Cleanup close_fd = [fd] { close(fd); };
  struct stat st{};
  if (fstat(fd, &st) != 0)
    throw spdlog::spdlog_ex("Failed to stat log " + filename, errno);
  if (!S_ISREG(st.st_mode) || st.st_nlink != 1)
    throw spdlog::spdlog_ex("Log must be a regular, singly linked file: " +
                            filename);
  if (fchown(fd, geteuid(), getegid()) != 0 || fchmod(fd, 0600) != 0)
    throw spdlog::spdlog_ex("Failed to secure log " + filename, errno);
}

spdlog::file_event_handlers PrivateLogFileHandlers(
    const std::filesystem::path& path, uint64_t max_files) {
  // Match the rotating sink's limit before inspecting retained files.
  if (max_files > 200000)
    throw spdlog::spdlog_ex("rotating sink: max_files cannot exceed 200000");
  // Supervisor inherits Craned's identity. Keep its logs private to that
  // identity, including when Craned uses a permissive umask in daemon mode.
  auto result = util::os::EnsureServiceDirectory(path.parent_path(), 0700);
  if (!result)
    throw spdlog::spdlog_ex(
        "Failed to secure log directory " + path.parent_path().string(),
        result.error().value());

  // Repair retained files from previous supervisor runs before rotation can
  // rename them. Older files beyond the configured count remain protected by
  // the private directory.
  for (uint64_t i = 1; i <= max_files; ++i)
    SecureLogFile(spdlog::sinks::rotating_file_sink_mt::calc_filename(path, i),
                  false);

  spdlog::file_event_handlers handlers;
  handlers.before_open = [](const spdlog::filename_t& filename) {
    // Pre-create each file privately before spdlog opens it, also on rotation.
    // Do not change the process-wide umask inherited by user tasks.
    SecureLogFile(filename, true);
  };
  return handlers;
}
}  // namespace

std::optional<spdlog::level::level_enum> StrToLogLevel(
    const std::string& level) {
  if (level == "trace") {
    return spdlog::level::trace;
  } else if (level == "debug") {
    return spdlog::level::debug;
  } else if (level == "info") {
    return spdlog::level::info;
  } else if (level == "warn") {
    return spdlog::level::warn;
  } else if (level == "error") {
    return spdlog::level::err;
  } else if (level == "off") {
    return spdlog::level::off;
  } else {
    return std::nullopt;
  }
}

void InitLogger(spdlog::level::level_enum level,
                const std::string& log_file_path, bool enable_console,
                uint64_t max_file_size, uint64_t max_file_num,
                size_t async_queue_size, size_t async_thread_count,
                bool async_block_when_full, bool private_file) {
  std::vector<spdlog::sink_ptr> sinks;
  auto handlers = private_file
                      ? PrivateLogFileHandlers(log_file_path, max_file_num)
                      : spdlog::file_event_handlers{};
  auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      log_file_path, max_file_size, max_file_num, false, handlers);
  file_sink->set_level(level);
  file_sink->set_pattern(kLogPattern);
  default_sinks.file_sink = file_sink;
  sinks.push_back(file_sink);

  if (enable_console) {
    auto console_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
    console_sink->set_level(level);
    console_sink->set_pattern(kLogPattern);
    sinks.push_back(console_sink);

    default_sinks.console_sink = console_sink;
  } else {
    default_sinks.console_sink.reset();
  }

  spdlog::init_thread_pool(async_queue_size, async_thread_count);
  default_overflow_policy = async_block_when_full
                                ? spdlog::async_overflow_policy::block
                                : spdlog::async_overflow_policy::overrun_oldest;
  auto logger = std::make_shared<spdlog::async_logger>(
      "default", sinks.begin(), sinks.end(), spdlog::thread_pool(),
      default_overflow_policy);
  spdlog::set_default_logger(logger);

  spdlog::flush_on(spdlog::level::err);
  spdlog::flush_every(std::chrono::seconds(1));

  spdlog::set_level(level);
}

std::shared_ptr<spdlog::async_logger> AddLogger(
    const std::string& name, spdlog::level::level_enum level,
    const std::filesystem::path& log_file_path, bool enable_console,
    uint64_t max_file_size, uint64_t max_file_num) {
  std::vector<spdlog::sink_ptr> sinks;
  auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      log_file_path, max_file_size, max_file_num);
  file_sink->set_level(level);
  file_sink->set_pattern(kLogPattern);
  sinks.push_back(file_sink);

  if (enable_console) {
    if (!default_sinks.console_sink) {
      default_sinks.console_sink =
          std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
      default_sinks.console_sink->set_level(level);

      default_sinks.console_sink->set_pattern(kLogPattern);
    }
    sinks.push_back(default_sinks.console_sink);
  }
  auto logger = std::make_shared<spdlog::async_logger>(
      name, sinks.begin(), sinks.end(), spdlog::thread_pool(),
      default_overflow_policy);
  logger->set_level(level);
  spdlog::register_logger(logger);
  return logger;
}

std::shared_ptr<spdlog::async_logger> AddLogger(const std::string& name,
                                                spdlog::level::level_enum level,
                                                bool enable_console) {
  std::vector<spdlog::sink_ptr> sinks;
  sinks.push_back(default_sinks.file_sink);

  if (enable_console) {
    if (!default_sinks.console_sink) {
      default_sinks.console_sink =
          std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
      default_sinks.console_sink->set_level(level);

      default_sinks.console_sink->set_pattern(kLogPattern);
    }
    sinks.push_back(default_sinks.console_sink);
  }

  auto logger = std::make_shared<spdlog::async_logger>(
      name, sinks.begin(), sinks.end(), spdlog::thread_pool(),
      default_overflow_policy);
  logger->set_level(level);
  spdlog::register_logger(logger);
  return logger;
}
