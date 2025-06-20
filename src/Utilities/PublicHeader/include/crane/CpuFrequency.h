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

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace crane {

inline constexpr std::string_view PATH_TO_CPU = "/sys/devices/system/cpu/";
inline constexpr uint32_t FREQ_LIST_MAX = 64;

// Governor flags
enum GovernorFlag : uint8_t {
  GOV_CONSERVATIVE  = 0x01,
  GOV_ONDEMAND      = 0x02,
  GOV_PERFORMANCE   = 0x04,
  GOV_POWERSAVE     = 0x08,
  GOV_USERSPACE     = 0x10,
  GOV_SCHEDUTIL     = 0x20
};

class CpuFrequency {
public:
  CpuFrequency() = default;
  ~CpuFrequency() = default;

  void Init(uint32_t cpu_num);

  void CpuFreqValidateAndSet(const std::string& low, const std::string& high,
                             const std::string& governor,
                             const std::vector<uint32_t> cpu_ids);

private:
  bool DeriveAvailFreq_(uint32_t cpu_idx);

  uint32_t CpuFreqGetScalingFreq_(uint32_t cpu_idx, const std::string& option);

  struct CpuFreqData{
    uint8_t avail_governors;
    std::vector<uint32_t> avail_freq;
    bool org_set;
    std::string org_governor;
    std::string org_frequency;
    std::string org_min_freq;
    std::string org_max_freq;
    std::string new_frequency;
    std::string new_governor;
    std::string new_min_freq;
    std::string new_max_freq;
  };

  std::vector<CpuFreqData> m_cpu_freq_data_;
};

} // crane