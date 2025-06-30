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

#include <sys/file.h>
#include <sys/stat.h>

#include <filesystem>
#include <fstream>

#include "absl/strings/str_split.h"
#include "crane/Logger.h"
#include "crane/OS.h"
#include "spdlog/fmt/bundled/color.h"

namespace crane {

void CpuFrequency::Init(uint32_t cpu_num) {
  std::string path = fmt::format("{}cpu0/cpufreq", kPathToCpu);

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
    CpuFreqData& freq_data = m_cpu_freq_data_[i];

    std::string gov_path =
        fmt::format("{}cpu{}/cpufreq/scaling_available_governors", kPathToCpu, i);
    std::ifstream infile(gov_path);
    if (!infile.is_open()) continue;
    std::string value;
    if (!std::getline(infile, value)) {
      infile.close();
      continue;
    }

    if (value == "performance") {
      freq_data.avail_governors |= GOV_PERFORMANCE;
    } else if (value == "userspace") {
      freq_data.avail_governors |= GOV_USERSPACE;
    } else if (value == "conservative") {
      freq_data.avail_governors |= GOV_CONSERVATIVE;
    } else if (value == "ondemand") {
      freq_data.avail_governors |= GOV_ONDEMAND;
    } else if (value == "powersave") {
      freq_data.avail_governors |= GOV_POWERSAVE;
    } else if (value == "schedutil") {
      freq_data.avail_governors |= GOV_SCHEDUTIL;
    }

    infile.close();

    std::string freq_path = fmt::format(
        "{}cpu{}/cpufreq/scaling_available_frequencies", kPathToCpu, i);

    std::ifstream freq_file(freq_path);
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
      for (uint32_t j = 0; j < (kFreqListMax - 1); j++) {
        if (!(freq_file >> freq) && freq_file.eof()) {
          all_avail = true;
          break;
        }
        freq_data.avail_freq.emplace_back(freq);
      }
      freq_file.close();
      std::ranges::sort(freq_data.avail_freq);
      if (!all_avail) CRANE_ERROR("all available frequencies not scanned");
    }

    freq_data.new_frequency = kInvalidFreq;
    freq_data.new_max_freq = kInvalidFreq;
    freq_data.new_min_freq = kInvalidFreq;
    freq_data.new_governor.clear();
  }
}

void CpuFrequency::CpuFreqValidateAndSet(const std::string& low,
                                         const std::string& high,
                                         const std::string& governor,
                                         const uint32_t job_id,
                                         const std::string& cpu_ids_str) {
  CRANE_DEBUG("task {} cpu {} freq request: min={}, max={}, governor={}",
              job_id, cpu_ids_str, low, high, governor);

  if (m_cpu_freq_data_.empty()) return;

  std::list<uint32_t> cpu_ids = ParseCpuList_(cpu_ids_str);
  for (uint32_t cpu_idx : cpu_ids) {
    if (cpu_idx >= m_cpu_freq_data_.size()) {
      CRANE_ERROR("index {} exceeds cpu count {}", cpu_idx,
                  m_cpu_freq_data_.size());
      return;
    }
  }

  for (uint32_t cpu_idx : cpu_ids) {
    CpuFreqSetupData_(low, high, governor, cpu_idx);

    auto& freq_data = m_cpu_freq_data_[cpu_idx];

    if (freq_data.new_frequency == kInvalidFreq &&
        freq_data.new_min_freq == kInvalidFreq &&
        freq_data.new_max_freq == kInvalidFreq &&
        freq_data.new_governor.empty())
      continue;

    CRANE_DEBUG(
        "cpu_freq: current_state cpu={} org_min={} org_freq={} org_max={} "
        "org_gpv={}",
        cpu_idx, freq_data.org_min_freq, freq_data.org_frequency,
        freq_data.org_max_freq, freq_data.org_governor);

    // According to the kernel documentation, the maximum frequency must be set
    // before the minimum frequency.
    if (freq_data.new_max_freq != kInvalidFreq) {
      if (freq_data.org_frequency > freq_data.new_max_freq) {
        if (!CpuFreqSetGov_(cpu_idx, "userspace", job_id)) return;

        if (!CpuFreqSetScalingFreq_(cpu_idx, freq_data.new_max_freq,
                                    "scaling_setspeed", job_id))
          continue;

        // If no new governor is specified, restore the original governor.
        if (freq_data.new_governor.empty()) {
          if (!CpuFreqSetGov_(cpu_idx, freq_data.org_governor, job_id))
            continue;
        }

        // set scaling_max_freq
        if (!CpuFreqSetScalingFreq_(cpu_idx, freq_data.new_max_freq,
                                    "scaling_max_freq", job_id))
          continue;
      }
    }

    if (freq_data.new_min_freq != kInvalidFreq) {
      if (freq_data.org_frequency < freq_data.new_min_freq) {
        if (!CpuFreqSetGov_(cpu_idx, "userspace", job_id)) continue;
        if (!CpuFreqSetScalingFreq_(cpu_idx, freq_data.new_min_freq,
                                    "scaling_setspeed", job_id))
          continue;
        if (freq_data.new_governor.empty()) {
          if (!CpuFreqSetGov_(cpu_idx, freq_data.org_governor, job_id))
            continue;
        }
      }
      if (CpuFreqSetScalingFreq_(cpu_idx, freq_data.new_min_freq,
                                 "scaling_min_freq", job_id))
        continue;
    }

    if (freq_data.new_frequency != kInvalidFreq) {
      if (freq_data.org_frequency != freq_data.new_frequency) {
        if (!CpuFreqSetGov_(cpu_idx, "userspace", job_id)) continue;
      }
      if (!CpuFreqSetScalingFreq_(cpu_idx, freq_data.new_frequency,
                                  "scaling_setspeed", job_id))
        continue;
    }

    if (!freq_data.new_governor.empty()) {
      if (!CpuFreqSetGov_(cpu_idx, freq_data.new_governor, job_id)) continue;
    }

    CRANE_DEBUG("task {} set cpu freq", job_id);
  }
}

void CpuFrequency::CpuFreqReset(uint32_t job_id) {
  for (uint32_t i = 0; i < m_cpu_freq_data_.size(); i++) {
    CpuFreqData& freq_data = m_cpu_freq_data_[i];

    if (freq_data.new_frequency == kInvalidFreq &&
        freq_data.new_max_freq == kInvalidFreq &&
        freq_data.new_min_freq == kInvalidFreq &&
        freq_data.new_governor.empty())
      continue;

    if (!TestCpuOwnerLock_(i, job_id))
      continue;

    if (freq_data.new_frequency != kInvalidFreq) {
      if (!CpuFreqSetGov_(i, "userspace", job_id)) continue;
      if (!CpuFreqSetScalingFreq_(i, freq_data.org_frequency,
                                  "scaling_setspeed", job_id))
        continue;
    }

    if (freq_data.new_max_freq != kInvalidFreq) {
      if (!CpuFreqSetScalingFreq_(i, freq_data.org_frequency,
                                  "scaling_max_freq", job_id))
        continue;
    }

    if (freq_data.new_min_freq != kInvalidFreq) {
      if (!CpuFreqSetScalingFreq_(i, freq_data.org_min_freq, "scaling_min_freq",
                                  job_id))
        continue;
    }

    if (!freq_data.new_governor.empty()) {
      if (!CpuFreqSetGov_(i, freq_data.org_governor, job_id)) continue;
    }

    CRANE_DEBUG("task {} reset cpu {}", job_id, i);
  }
}

std::list<uint32_t> CpuFrequency::ParseCpuList_(
    const std::string& cpu_ids_str) {
  std::list<uint32_t> result;
  for (absl::string_view token : absl::StrSplit(cpu_ids_str, ',')) {
    token = absl::StripAsciiWhitespace(token);
    auto dash_pos = token.find('-');
    if (dash_pos == absl::string_view::npos) {
      result.push_back(std::stoi(std::string(token)));
    } else {
      int start = std::stoi(std::string(token.substr(0, dash_pos)));
      int end = std::stoi(std::string(token.substr(dash_pos + 1)));
      for (int i = start; i <= end; ++i) {
        result.push_back(i);
      }
    }
  }
  return result;
}

bool CpuFrequency::DeriveAvailFreq_(uint32_t cpu_idx) {
  uint32_t min_freq = CpuFreqGetScalingFreq_(cpu_idx, "scaling_min_freq");
  if (min_freq == 0) return false;

  uint32_t max_freq = CpuFreqGetScalingFreq_(cpu_idx, "scaling_max_freq");
  if (max_freq == 0) return false;

  uint32_t delta_freq = (max_freq - min_freq) / (kFreqListMax - 1);

  m_cpu_freq_data_[cpu_idx].avail_freq.resize(kFreqListMax);
  for (uint32_t i = 0; i < (kFreqListMax - 1); i++)
    m_cpu_freq_data_[cpu_idx].avail_freq[i] = min_freq + (delta_freq * i);

  m_cpu_freq_data_[cpu_idx].avail_freq[kFreqListMax - 1] = max_freq;

  return true;
}

uint32_t CpuFrequency::CpuFreqGetScalingFreq_(uint32_t cpu_idx,
                                              const std::string& option) {
  std::string path =
      fmt::format("{}cpu{}/cpufreq/{}", kPathToCpu, cpu_idx, option);
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

void CpuFrequency::CpuFreqSetupData_(const std::string& low,
                                     const std::string& high,
                                     const std::string& governor,
                                     uint32_t cpu_idx) {
  if (!CpuFreqCurrentState_(cpu_idx)) return;

  CpuFreqData& freq_data = m_cpu_freq_data_[cpu_idx];

  if (governor == "conservative") {
    if ((freq_data.avail_governors & GOV_CONSERVATIVE) != 0)
      freq_data.new_governor = governor;
  } else if (governor == "performance") {
    if ((freq_data.avail_governors & GOV_PERFORMANCE) != 0)
      freq_data.new_governor = governor;
  } else if (governor == "powersave") {
    if ((freq_data.avail_governors & GOV_POWERSAVE) != 0)
      freq_data.new_governor = governor;
  } else if (governor == "ondemand") {
    if ((freq_data.avail_governors & GOV_ONDEMAND) != 0)
      freq_data.new_governor = governor;
  } else if (governor == "userspace") {
    if ((freq_data.avail_governors & GOV_USERSPACE) != 0)
      freq_data.new_governor = governor;
  } else if (governor == "schedutil") {
    if ((freq_data.avail_governors & GOV_SCHEDUTIL) != 0)
      freq_data.new_governor = governor;
  } else {
    CRANE_ERROR("Invalid governor {}", governor);
    return;
  }

  // --cpu-freq=<frequency value or alias>
  if (governor == "userspace") {
    if (low.empty()) return;
    // Power capping: Set the maximum, minimum, and current frequency to the
    // same value under the userspace governor.
    uint32_t freq = CpuFreqFreqSpecNum_(low, cpu_idx);
    freq_data.new_frequency = freq;
    freq_data.new_min_freq = freq;
    freq_data.new_max_freq = freq;
    return;
  }

  // --cpu-freq=<low-high:governor>
  if (!low.empty() && !high.empty()) {
    uint32_t freq = CpuFreqFreqSpecNum_(low, cpu_idx);
    freq_data.new_min_freq = freq;
    freq = CpuFreqFreqSpecNum_(high, cpu_idx);
    freq_data.new_max_freq = freq;
  }

  // --cpu-freq=<governor> or <low-high:governor>
  if (!governor.empty()) freq_data.new_governor = governor;

  if (freq_data.new_frequency != kInvalidFreq) {
    if (freq_data.new_frequency < freq_data.org_min_freq)
      freq_data.new_min_freq = freq_data.new_frequency;
    if (freq_data.new_frequency > freq_data.org_max_freq)
      freq_data.new_max_freq = freq_data.new_frequency;
  }
}

bool CpuFrequency::CpuFreqCurrentState_(uint32_t cpu_idx) {
  static int freq_file = -1;
  uint32_t freq;

  if (m_cpu_freq_data_[cpu_idx].org_set) return true;

  if (freq_file == -1) {
    if (CpuFreqGetScalingFreq_(cpu_idx, "cpuinfo_cur_freq") != 0U)
      freq_file = 0;
    else
      freq_file = 1;
  }

  if (freq_file == 0)
    freq = CpuFreqGetScalingFreq_(cpu_idx, "cpuinfo_cur_freq");
  else
    freq = CpuFreqGetScalingFreq_(cpu_idx, "scaling_cur_freq");

  if (freq == 0) return false;

  m_cpu_freq_data_[cpu_idx].org_frequency = freq;

  freq = CpuFreqGetScalingFreq_(cpu_idx, "scaling_min_freq");
  if (freq == 0) return false;
  m_cpu_freq_data_[cpu_idx].org_min_freq = freq;

  freq = CpuFreqGetScalingFreq_(cpu_idx, "scaling_max_freq");
  if (freq == 0) return false;
  m_cpu_freq_data_[cpu_idx].org_max_freq = freq;

  if (!CpuFreqGetCurGov_(cpu_idx)) return false;

  m_cpu_freq_data_[cpu_idx].org_set = true;
  return true;
}

bool CpuFrequency::CpuFreqGetCurGov_(uint32_t cpu_idx) {
  std::string path =
      fmt::format("{}cpu{}/cpufreq/scaling_governor", kPathToCpu, cpu_idx);

  if (!std::filesystem::exists(path)) {
    CRANE_ERROR("CPU frequency setting not configured for this node");
    return false;
  }

  if (!std::filesystem::is_directory(path)) {
    CRANE_ERROR("{} not a directory", path);
    return false;
  }

  std::ifstream infile(path);
  if (!infile.is_open()) {
    CRANE_ERROR("Could not open scaling_governor {}", path);
    return false;
  }

  std::string value;
  if (!std::getline(infile, value)) {
    infile.close();
    CRANE_ERROR("Could not read scaling_governor {}", path);
    return false;
  }

  if (value.size() >= kGovNameLen) {
    CRANE_ERROR("scaling_governor {} is too long", value);
    infile.close();
    return false;
  }

  m_cpu_freq_data_[cpu_idx].org_governor = std::move(value);
  infile.close();

  auto size = m_cpu_freq_data_[cpu_idx].org_governor.size();
  if ((size > 0) && (m_cpu_freq_data_[cpu_idx].org_governor[size - 1] == '\n'))
    m_cpu_freq_data_[cpu_idx].org_governor.resize(size - 1);

  return true;
}

uint32_t CpuFrequency::CpuFreqFreqSpecNum_(const std::string& value,
                                           uint32_t cpu_idx) {
  if (m_cpu_freq_data_.empty() || m_cpu_freq_data_[cpu_idx].avail_freq.empty())
    return kInvalidFreq;

  size_t nfreq = m_cpu_freq_data_.size();
  CpuFreqData& freq_data = m_cpu_freq_data_[cpu_idx];

  if (value == "low") return freq_data.avail_freq[0];

  if (value == "medium") {
    if (nfreq == 1) return freq_data.avail_freq[0];
    uint32_t fx = (nfreq - 1) / 2;
    return freq_data.avail_freq[fx];
  }
  if (value == "highm1") {
    if (nfreq == 1) return freq_data.avail_freq[0];
    uint32_t fx = nfreq - 2;
    return freq_data.avail_freq[fx];
  }
  if (value == "high") {
    return freq_data.avail_freq[nfreq - 1];
  }

  try {
    uint32_t freq = std::stoul(value);
    return freq;
  } catch (const std::exception& e) {
    return kInvalidFreq;
  }

  return kInvalidFreq;
}

bool CpuFrequency::CpuFreqSetGov_(uint32_t cpu_idx, const std::string& governor,
                                  uint32_t job_id) {
  std::string path =
      fmt::format("{}cpu{}/cpufreq/scaling_governor", kPathToCpu, cpu_idx);

  int fd = SetCpuOwnerLock_(cpu_idx, job_id);
  if (fd < 0) return false;

  std::ofstream ofs(path, std::ios::out | std::ios::trunc);
  if (ofs) {
    ofs << governor << '\n';
  } else {
    CRANE_ERROR("{} Can not set CPU {} governor: {}", job_id, cpu_idx,
                governor);

    flock(fd, LOCK_UN);
    close(fd);
    return false;
  }

  flock(fd, LOCK_UN);
  close(fd);

  return true;
}

bool CpuFrequency::CpuFreqSetScalingFreq_(uint32_t cpu_idx, uint32_t freq,
                                          const std::string& option,
                                          uint32_t job_id) {
  std::string path =
      fmt::format("{}cpu{}/cpufreq/{}", kPathToCpu, cpu_idx, option);

  int fd = SetCpuOwnerLock_(cpu_idx, job_id);
  if (fd < 0) return false;

  std::ofstream ofs(path, std::ios::out | std::ios::trunc);
  if (ofs) {
    ofs << freq << '\n';
  } else {
    CRANE_ERROR("{} Can not set CPU {} option: {}", job_id, cpu_idx, freq);

    flock(fd, LOCK_UN);
    close(fd);
    return false;
  }

  flock(fd, LOCK_UN);
  close(fd);

  return true;
}

/*
 * The purpose of this set of locks is to prevent race conditions when changing
 * the CPU frequency or governor. Specifically, when a job ends, the CPU
 * frequency should only be reset if it was the last job to set the CPU
 * frequency. Due to the existence of gang scheduling and the cancellation of
 * suspended/running jobs, timing issues may occur. _set_cpu_owner_lock - Set
 * the specified job as the owner of the CPU; the file is locked on exit
 * _test_cpu_owner_lock - Test whether the specified job owns the CPU
 */
int CpuFrequency::SetCpuOwnerLock_(uint32_t cpu_id, uint32_t job_id) {
  // lock dir TODO: use config base dir
  std::string tmp = fmt::format("{}/cpu", kDefaultCraneBaseDir, cpu_id);
  if (!util::os::CreateFolders(tmp)) return -1;

  tmp = fmt::format("{}/cpu/{}", kDefaultCraneBaseDir, cpu_id);
  int fd = open(tmp.c_str(), O_CREAT | O_RDWR, 0600);
  if (fd < 0) {
    CRANE_ERROR("open {} error", tmp);
    return fd;
  }

  int rc = FdLockRetry_(fd);
  if (rc < 0) {
    CRANE_ERROR("flock {} error", tmp);
    close(fd);
    return -1;
  }

  ssize_t wr = pwrite(fd, &job_id, sizeof(job_id), 0);
  if (wr != sizeof(job_id)) {
    CRANE_ERROR("Failed to write job_id to {}: {}", tmp, strerror(errno));
    close(fd);
    return -1;
  }

  return fd;
}

bool CpuFrequency::TestCpuOwnerLock_(uint32_t cpu_id, uint32_t job_id) {
  std::string tmp = fmt::format("{}/cpu", kDefaultCraneBaseDir);

  if ((mkdir(tmp.c_str(), 0700) != 0) && (errno != EEXIST)) {
    CRANE_ERROR("mkdir %s failed: %m", tmp);
    return false;
  }

  tmp = fmt::format("{}/cpu/{}", kDefaultCraneBaseDir, cpu_id);
  int fd = open(tmp.c_str(), O_RDWR, 0600);

  if (fd < 0) {
    CRANE_ERROR("open {} error", tmp);
    return false;
  }

  int rc = FdLockRetry_(fd);
  if (rc < 0) {
    CRANE_ERROR("flock {} error", tmp);
    close(fd);
    return false;
  }

  uint32_t in_job_id;
  ssize_t n = pread(fd, &in_job_id, sizeof(in_job_id), 0);
  if (n < 0) {
    flock(fd, LOCK_UN);
    close(fd);
    return false;
  }

  flock(fd, LOCK_UN);

  if (job_id != in_job_id) {
    CRANE_DEBUG("CPU %d now owned by job %u rather than job %u", cpu_id,
                in_job_id, job_id);
    close(fd);
    return false;
  }

  close(fd);
  CRANE_DEBUG("CPU {} owned by job {} as expected", cpu_id, job_id);

  return true;
}

int CpuFrequency::FdLockRetry_(int fd) {
  int rc = -1;

  for (int i = 0; i < 10; i++) {
    if (i > 0) usleep(1000);
    rc = flock(fd, LOCK_EX | LOCK_NB);
    if (rc == 0) break;
    if ((errno != EACCES) && (errno != EAGAIN)) break;
  }

  return rc;
}

}  // namespace crane