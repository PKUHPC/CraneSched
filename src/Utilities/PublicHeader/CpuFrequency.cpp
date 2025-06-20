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

#include "crane/CpuFrequency.h"

#include <sys/stat.h>

#include <filesystem>
#include <fstream>

#include "crane/Logger.h"
#include "spdlog/fmt/bundled/color.h"

namespace crane {

void CpuFrequency::Init(uint32_t cpu_num) {
  std::string path = fmt::format("{}cpu0/cpufreq", PATH_TO_CPU);

  if (!std::filesystem::exists(path)) {
    CRANE_ERROR("CPU frequency setting not configured for this node");
    return;
  }

  if (!std::filesystem::is_directory(path)) {
    CRANE_ERROR("{} not a directory", path);
    return;
  }

  CRANE_DEBUG("Gathering cpu frequency information for {} cpus", cpu_num);
  m_cpu_freq_data_.resize(cpu_num);

  for (uint32_t i = 0; i < cpu_num; ++i) {
    std::string gov_path =
        fmt::format("{}{}/cpufreq/scaling_available_governors", PATH_TO_CPU, i);
    std::ifstream infile(gov_path);
    if (!infile.is_open()) continue;
    std::string value;
    if (!std::getline(infile, value)) {
      infile.close();
      continue;
    }

    if (value == "performance") {
      m_cpu_freq_data_[i].avail_governors |= GOV_PERFORMANCE;
    } else if (value == "userspace") {
      m_cpu_freq_data_[i].avail_governors |= GOV_USERSPACE;
    } else if (value == "conservative") {
      m_cpu_freq_data_[i].avail_governors |= GOV_CONSERVATIVE;
    } else if (value == "ondemand") {
      m_cpu_freq_data_[i].avail_governors |= GOV_ONDEMAND;
    } else if (value == "powersave") {
      m_cpu_freq_data_[i].avail_governors |= GOV_POWERSAVE;
    } else if (value == "conservative") {
      m_cpu_freq_data_[i].avail_governors |= GOV_CONSERVATIVE;
    }
    infile.close();

    std::string freq_path = fmt::format(
        "{}{}/cpufreq/scaling_available_frequencies", PATH_TO_CPU, i);
    std::ifstream freq_file(gov_path);
    if (!freq_file.is_open()) {
      /* Do not log errors here; the scaling_available_frequencies file
       * does not exist when using the intel_pstate driver.
       * In this case, the frequency needs to be inferred from min/max.
       */
      CRANE_DEBUG("CPU {} does not support intel_pstate", i);
      DeriveAvailFreq_(i);
    } else {
      bool all_avail = false;
      uint32_t freq;
      for (uint32_t j = 0; j < (FREQ_LIST_MAX - 1); j++) {
        if (!(freq_file >> freq) && freq_file.eof()) {
          all_avail = true;
          break;
        }
        m_cpu_freq_data_[i].avail_freq.emplace_back(freq);
      }
      freq_file.close();
      std::sort(m_cpu_freq_data_[i].avail_freq.begin(),
                m_cpu_freq_data_[i].avail_freq.end());
      if (!all_avail) CRANE_ERROR("all available frequencies not scanned");
    }
  }
}

void CpuFrequency::CpuFreqValidateAndSet(const std::string& low,
                                         const std::string& high,
                                         const std::string& governor,
                                         const std::vector<uint32_t> cpu_ids) {
  CRANE_DEBUG("cpu freq request: min={}, max={}, governor={}", low, high, governor);

  if (m_cpu_freq_data_.empty()) return ;

  for (const auto cpu_id : cpu_ids) {
    if (cpu_id >= m_cpu_freq_data_.size()) {
      CRANE_ERROR("cpu_freq_validate: index {} exceeds cpu count {}", cpu_id, m_cpu_freq_data_.size());
      return ;
    }
    // TODO: set cpu freq
  }
}

bool CpuFrequency::DeriveAvailFreq_(uint32_t cpu_idx) {

  uint32_t min_freq = CpuFreqGetScalingFreq_(cpu_idx, "scaling_min_freq");
  if (min_freq == 0) return false;

  uint32_t max_freq = CpuFreqGetScalingFreq_(cpu_idx, "scaling_max_freq");
  if (max_freq == 0) return false;

  uint32_t delta_freq = (max_freq - min_freq) / (FREQ_LIST_MAX - 1);

  m_cpu_freq_data_[cpu_idx].avail_freq.resize(FREQ_LIST_MAX);
  for (uint32_t i = 0; i < (FREQ_LIST_MAX - 1); i++)
    m_cpu_freq_data_[cpu_idx].avail_freq[i] = min_freq + (delta_freq * i);

  m_cpu_freq_data_[cpu_idx].avail_freq[FREQ_LIST_MAX - 1] = max_freq;

  return true;
}

uint32_t CpuFrequency::CpuFreqGetScalingFreq_(uint32_t cpu_idx,
                                              const std::string& option) {
  std::string path =
      fmt::format("{}{}/cpufreq/{}", PATH_TO_CPU, cpu_idx, option);
  uint32_t freq;

  std::ifstream infile(path);
  if (!infile.is_open()) {
    CRANE_ERROR("Could not open {}", path);
    return 0;
  }

  if (!(infile >> freq)) {
    CRANE_ERROR("Could not read {}", path);
    return 0;
  }

  infile.close();

  return freq;
}

} // namespace crane