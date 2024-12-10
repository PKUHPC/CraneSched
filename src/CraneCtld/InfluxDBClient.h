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

#include <string>

namespace Ctld {

struct NodeStats {
  double avg_power{0.0};
  double cpu_util{0.0};
  double mem_util{0.0};
  double gpu_util{0.0};
  double total_energy{0.0};
  absl::Duration time_span;
};

class InfluxDBClient {
 public:
  InfluxDBClient();
  ~InfluxDBClient() = default;

  InfluxDBClient(const InfluxDBClient&) = delete;
  InfluxDBClient& operator=(const InfluxDBClient&) = delete;

  InfluxDBClient(InfluxDBClient&&) = default;
  InfluxDBClient& operator=(InfluxDBClient&&) = default;

  std::string Query(const std::string& flux_query) const;

  bool QueryNodeEnergyInfo(const std::string& node_id, int64_t window,
                           NodeStats* stats) const;

  double QueryTaskEnergyEfficiency(
      double requested_cpu, double requested_mem_gb,
      absl::Duration time_window = absl::Hours(24)) const;

 private:
  class CurlWrapper;
  struct QueryResult;

  std::string BuildUtilPowerQuery_(const std::string& node_id,
                                   int64_t duration) const;
  std::string BuildEnergyQuery_(const std::string& node_id,
                                int64_t duration) const;
  std::string BuildTaskEfficiencyQuery_(double requested_cpu,
                                        double requested_mem_gb,
                                        int64_t duration) const;

  bool ParseUtilPowerResponse_(const std::string& response,
                               NodeStats* stats) const;
  bool ParseEnergyResponse_(const std::string& response,
                            NodeStats* stats) const;
  bool ParseTaskEfficiencyResponse_(const std::string& response,
                                    double* efficiency) const;

  std::string m_url_;
  std::string m_token_;
  std::string m_org_;
};

}  // namespace Ctld