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
#include <list>
#include <string>
#include <vector>

namespace crane {

inline constexpr std::string_view kPathToCpu = "/sys/devices/system/cpu/";
inline constexpr uint32_t kFreqListMax = 64;
inline constexpr uint32_t kGovNameLen = 24;
inline constexpr uint32_t kInvalidFreq = 0xffffffff;

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
                             const std::string& governor, uint32_t job_id,
                             const std::string& cpu_ids_str);

  void CpuFreqReset(uint32_t job_id);

private:
  static std::list<uint32_t> ParseCpuList_(const std::string& cpu_ids_str);
  static uint32_t CpuFreqGetScalingFreq_(uint32_t cpu_idx, const std::string& option);
  static bool CpuFreqSetGov_(int cpu_idx, const std::string& governor, uint32_t job_id);
  static bool CpuFreqSetScalingFreq_(int cpu_idx, uint32_t freq, const std::string& option, uint32_t job_id);
  static int SetCpuOwnerLock_(int cpu_id, uint32_t job_id);
  static int FdLockRetry_(int fd);

  bool DeriveAvailFreq_(uint32_t cpu_idx);
  void CpuFreqSetupData_(const std::string& low, const std::string& high,
                         const std::string& governor, int cpu_idx);
  bool CpuFreqCurrentState_(int cpu_idx);
  bool CpuFreqGetCurGov_(int cpu_idx);
  uint32_t CpuFreqFreqSpecNum_(const std::string& value, int cpu_idx);
  int TestCpuOwnerLock_(int cpu_id, uint32_t job_id);


  struct CpuFreqData{
    uint8_t avail_governors;
    std::vector<uint32_t> avail_freq;
    bool org_set;
    std::string org_governor;
    uint32_t org_frequency;
    uint32_t org_min_freq;
    uint32_t org_max_freq;
    std::string new_governor;
    uint32_t new_frequency;
    uint32_t new_min_freq;
    uint32_t new_max_freq;
  };

  std::vector<CpuFreqData> m_cpu_freq_data_;
};

} // namespace crane