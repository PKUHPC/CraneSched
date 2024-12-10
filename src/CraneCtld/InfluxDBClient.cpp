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

#include "InfluxDBClient.h"

#include <csv.h>
#include <curl/curl.h>

#include <sstream>

#include "CtldPublicDefs.h"

namespace Ctld {

namespace {

constexpr char kContentType[] = "Content-Type: application/vnd.flux";
constexpr char kAuthFormat[] = "Authorization: Token {}";

size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
  auto* buffer = static_cast<std::string*>(userp);
  buffer->append(static_cast<char*>(contents), size * nmemb);
  return size * nmemb;
}

}  // namespace

struct InfluxDBClient::QueryResult {
  bool success{false};
  std::string data;
  std::string error;
};

class InfluxDBClient::CurlWrapper {
 public:
  CurlWrapper() : curl_(curl_easy_init()), headers_(nullptr) {}

  ~CurlWrapper() {
    if (headers_) curl_slist_free_all(headers_);
    if (curl_) curl_easy_cleanup(curl_);
  }

  QueryResult Execute(const std::string& url, const std::string& query,
                      const std::string& token) {
    QueryResult result;
    if (!curl_) {
      result.error = "Failed to initialize CURL";
      return result;
    }

    SetupCurlOptions(url, query, token);

    CURLcode res = curl_easy_perform(curl_);
    if (res != CURLE_OK) {
      result.error = curl_easy_strerror(res);
      return result;
    }

    result.success = true;
    result.data = buffer_;
    return result;
  }

 private:
  void SetupCurlOptions(const std::string& url, const std::string& query,
                        const std::string& token) {
    headers_ = curl_slist_append(headers_, kContentType);
    headers_ =
        curl_slist_append(headers_, fmt::format(kAuthFormat, token).c_str());

    curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl_, CURLOPT_POST, 1L);
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, query.c_str());
    curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers_);
    curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &buffer_);
  }

  CURL* curl_;
  struct curl_slist* headers_;
  std::string buffer_;
};

InfluxDBClient::InfluxDBClient()
    : m_url_(g_config.InfluxDbUrl),
      m_token_(g_config.InfluxDbToken),
      m_org_(g_config.InfluxDbOrg) {}

std::string InfluxDBClient::Query(const std::string& flux_query) const {
  CurlWrapper curl;
  std::string url = m_url_ + "/api/v2/query?org=" + m_org_;

  auto result = curl.Execute(url, flux_query, m_token_);
  if (!result.success) {
    CRANE_ERROR("InfluxDB query failed: {}", result.error);
    return {};
  }

  return result.data;
}

std::string InfluxDBClient::BuildUtilPowerQuery_(const std::string& node_id,
                                                 int64_t duration) const {
  return fmt::format(
      "from(bucket: \"{}\") "
      "|> range(start: -{}s) "
      "|> filter(fn: (r) => r[\"node_id\"] == \"{}\") "
      "|> filter(fn: (r) => r._field =~ "
      "/ipmi_power_w|cpu_utilization|memory_utilization|gpu_utilization/) "
      "|> mean() "
      "|> pivot(rowKey:[], columnKey: [\"_field\"], valueColumn: \"_value\")",
      g_config.InfluxDbNodeBucket, duration, node_id);
}

std::string InfluxDBClient::BuildEnergyQuery_(const std::string& node_id,
                                              int64_t duration) const {
  return fmt::format(
      "from(bucket: \"{}\") "
      "|> range(start: -{}s) "
      "|> filter(fn: (r) => r[\"node_id\"] == \"{}\") "
      "|> filter(fn: (r) => r._field == \"total_energy_j\") "
      "|> sum() "
      "|> yield(name: \"sum\")",
      g_config.InfluxDbNodeBucket, duration, node_id);
}

bool InfluxDBClient::ParseUtilPowerResponse_(const std::string& response,
                                             NodeStats* stats) const {
  try {
    std::istringstream stream(response);
    io::CSVReader<4, io::trim_chars<' ', '\t'>,
                  io::double_quote_escape<',', '"'>>
        reader("util_power_resp", stream);

    reader.read_header(io::ignore_extra_column, "cpu_utilization",
                       "memory_utilization", "gpu_utilization", "ipmi_power_w");

    double cpu_util, mem_util, gpu_util, power;
    if (reader.read_row(cpu_util, mem_util, gpu_util, power)) {
      stats->cpu_util = cpu_util;
      stats->mem_util = mem_util;
      stats->gpu_util = gpu_util;
      stats->avg_power = power;
      return true;
    }
  } catch (const io::error::base& e) {
    CRANE_ERROR("Failed to parse utilization power CSV: {}", e.what());
  }
  return false;
}

bool InfluxDBClient::ParseEnergyResponse_(const std::string& response,
                                          NodeStats* stats) const {
  try {
    std::istringstream stream(response);
    io::CSVReader<1, io::trim_chars<' ', '\t'>,
                  io::double_quote_escape<',', '"'>>
        reader("energy_resp", stream);

    reader.read_header(io::ignore_extra_column, "_value");

    double total_energy;
    if (reader.read_row(total_energy)) {
      stats->total_energy = total_energy;
      return true;
    }
  } catch (const io::error::base& e) {
    CRANE_ERROR("Failed to parse energy CSV: {}", e.what());
  }
  return false;
}

bool InfluxDBClient::QueryNodeEnergyInfo(const std::string& node_id,
                                         int64_t window,
                                         NodeStats* stats) const {
  auto util_power_resp = Query(BuildUtilPowerQuery_(node_id, window));
  auto energy_resp = Query(BuildEnergyQuery_(node_id, window));

  if (util_power_resp.empty() || energy_resp.empty()) {
    CRANE_WARN("Empty response for node {} with window {}s", node_id, window);
    return false;
  }

  if (!ParseUtilPowerResponse_(util_power_resp, stats) ||
      !ParseEnergyResponse_(energy_resp, stats)) {
    return false;
  }

  stats->time_span = absl::Seconds(window);

  CRANE_DEBUG(
      "Node {} stats in {}s window: CPU={:.1f}%, Mem={:.1f}%, GPU={:.1f}%, "
      "Power={:.1f}W, Energy={:.1f}J",
      node_id, window, stats->cpu_util, stats->mem_util, stats->gpu_util,
      stats->avg_power, stats->total_energy);

  return true;
}

std::string InfluxDBClient::BuildTaskEfficiencyQuery_(
    double requested_cpu, double requested_mem_gb,
    int64_t window_seconds) const {
  return fmt::format(
      "from(bucket: \"{}\") "
      "|> range(start: -{}s) "
      "|> filter(fn: (r) => r._measurement == \"task_energy\") "
      "|> filter(fn: (r) => "
      "    r.requested_cpu >= {} and r.requested_cpu <= {} and "
      "    r.requested_mem_gb >= {} and r.requested_mem_gb <= {}) "
      "|> filter(fn: (r) => r._field == \"energy_efficiency\") "
      "|> mean()",
      g_config.InfluxDbTaskBucket, window_seconds, requested_cpu * 0.8,
      requested_cpu * 1.2, requested_mem_gb * 0.8, requested_mem_gb * 1.2);
}

bool InfluxDBClient::ParseTaskEfficiencyResponse_(const std::string& response,
                                                  double* efficiency) const {
  try {
    std::istringstream stream(response);
    io::CSVReader<1, io::trim_chars<' ', '\t'>,
                  io::double_quote_escape<',', '"'>>
        reader("energy_efficiency", stream);

    reader.read_header(io::ignore_extra_column, "_value");

    double value;
    if (reader.read_row(value)) {
      if (value <= 0.0) {
        CRANE_WARN("Invalid efficiency value: {}", value);
        return false;
      }
      *efficiency = value;
      CRANE_DEBUG("Task energy efficiency: {}", value);
      return true;
    }
  } catch (const io::error::base& e) {
    CRANE_ERROR("Failed to parse energy efficiency CSV: {}", e.what());
  }
  return false;
}

double InfluxDBClient::QueryTaskEnergyEfficiency(
    double requested_cpu, double requested_mem_gb,
    absl::Duration duration) const {
  try {
    CRANE_DEBUG(
        "Querying task energy efficiency for "
        "CPU={:.2f} cores, Memory={:.2f}GB",
        requested_cpu, requested_mem_gb);

    int64_t duration_seconds = absl::ToInt64Seconds(duration);

    auto query = BuildTaskEfficiencyQuery_(requested_cpu, requested_mem_gb,
                                           duration_seconds);
    CRANE_DEBUG("Executing InfluxDB query: {}", query);

    std::string resp = Query(query);
    if (resp.empty()) {
      CRANE_WARN(
          "Empty response from InfluxDB query, using default efficiency");
      return 1.0;
    }

    double efficiency;
    if (!ParseTaskEfficiencyResponse_(resp, &efficiency)) {
      CRANE_WARN("Failed to get valid efficiency, using default value");
      return 1.0;
    }

    return efficiency;

  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to query task energy efficiency: {}", e.what());
    return 1.0;
  }
}

}  // namespace Ctld