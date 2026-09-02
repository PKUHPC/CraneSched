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

#include "SupervisorLogGcTask.h"
// Precompiled header comes first.

namespace Craned {

namespace {

bool ParseSupervisorLogFileName_(const std::filesystem::path& file_path,
                                 job_id_t* job_id, step_id_t* step_id) {
  static const std::regex kLogNameRegex(R"(^([0-9]+)\.([0-9]+)\.log$)");

  std::smatch match;
  const auto file_name = file_path.filename().string();
  if (!std::regex_match(file_name, match, kLogNameRegex)) return false;

  uint32_t parsed_job_id{};
  uint32_t parsed_step_id{};
  const auto job_text = match[1].str();
  const auto step_text = match[2].str();
  const auto [job_ptr, job_ec] = std::from_chars(
      job_text.data(), job_text.data() + job_text.size(), parsed_job_id);
  const auto [step_ptr, step_ec] = std::from_chars(
      step_text.data(), step_text.data() + step_text.size(), parsed_step_id);

  if (job_ec != std::errc{} || step_ec != std::errc{} ||
      job_ptr != job_text.data() + job_text.size() ||
      step_ptr != step_text.data() + step_text.size()) {
    return false;
  }

  *job_id = parsed_job_id;
  *step_id = parsed_step_id;
  return true;
}

bool IsActiveStep_(const NodeGcContext& ctx, job_id_t job_id,
                   step_id_t step_id) {
  auto job_it = ctx.active_steps.find(job_id);
  if (job_it == ctx.active_steps.end()) return false;
  return job_it->second.contains(step_id);
}

std::optional<absl::Time> ToAbslTime_(
    const std::filesystem::file_time_type& file_time) {
  using namespace std::chrono;

  const auto system_now = system_clock::now();
  const auto file_now = std::filesystem::file_time_type::clock::now();
  const auto delta = file_time - file_now;
  const auto system_tp =
      time_point_cast<system_clock::duration>(system_now + delta);
  return absl::FromChrono(system_tp);
}

}  // namespace

NodeGcTaskRunStats SupervisorLogGcTask::Run(const NodeGcContext& ctx) {
  NodeGcTaskRunStats stats{};
  const auto& gc_conf = g_config.CranedConf.NodeGarbageCollection.LogCleanup;
  const auto retention = absl::Seconds(gc_conf.RetentionSec);
  const auto expire_before = ctx.now - retention;
  const auto& log_dir = g_config.Supervisor.LogDir;

  std::error_code ec;
  if (!std::filesystem::exists(log_dir, ec)) {
    CRANE_DEBUG("[NodeGC] task='{}' skip because log dir '{}' does not exist.",
                Name(), log_dir.string());
    return stats;
  }

  if (!std::filesystem::is_directory(log_dir, ec)) {
    CRANE_WARN(
        "[NodeGC] task='{}' skip because log dir '{}' is not a directory.",
        Name(), log_dir.string());
    return stats;
  }

  for (const auto& entry : std::filesystem::directory_iterator(log_dir, ec)) {
    if (ec) {
      CRANE_ERROR("[NodeGC] task='{}' failed to iterate '{}': {}.", Name(),
                  log_dir.string(), ec.message());
      break;
    }

    const bool is_regular_file = entry.is_regular_file(ec);
    if (ec) {
      stats.skipped++;
      ec.clear();
      continue;
    }
    if (!is_regular_file) {
      stats.skipped++;
      continue;
    }

    job_id_t job_id{};
    step_id_t step_id{};
    if (!ParseSupervisorLogFileName_(entry.path(), &job_id, &step_id)) {
      stats.skipped++;
      continue;
    }

    stats.scanned++;

    if (IsActiveStep_(ctx, job_id, step_id)) {
      stats.skipped++;
      continue;
    }

    const auto last_write_time = entry.last_write_time(ec);
    if (ec) {
      CRANE_WARN("[NodeGC] task='{}' failed to get mtime for '{}': {}.", Name(),
                 entry.path().string(), ec.message());
      stats.skipped++;
      ec.clear();
      continue;
    }

    auto write_absl_time = ToAbslTime_(last_write_time);
    if (!write_absl_time.has_value()) {
      stats.skipped++;
      continue;
    }

    if (write_absl_time.value() > expire_before) {
      stats.skipped++;
      continue;
    }

    if (stats.deleted >= gc_conf.MaxDeletePerCycle) {
      stats.skipped++;
      continue;
    }

    const bool removed = std::filesystem::remove(entry.path(), ec);
    if (ec) {
      CRANE_WARN("[NodeGC] task='{}' failed to delete '{}': {}.", Name(),
                 entry.path().string(), ec.message());
      stats.skipped++;
      ec.clear();
      continue;
    }
    if (!removed) {
      stats.skipped++;
      continue;
    }

    stats.deleted++;
  }

  return stats;
}

}  // namespace Craned
