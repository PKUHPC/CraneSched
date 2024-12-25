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

template <size_t N, typename Callback>
bool ParseInfluxCsvImpl(const std::string& response,
                        const std::string& parser_name,
                        const std::vector<std::string>& headers,
                        Callback&& callback) {
  try {
    CRANE_DEBUG("Raw CSV response for {}:\n{}", parser_name, response);

    std::istringstream stream(response);
    io::CSVReader<N, io::trim_chars<' ', '\t'>,
                  io::double_quote_escape<',', '"'>>
        reader(parser_name, stream);

    CRANE_DEBUG("Looking for headers in {}: {}", parser_name,
                fmt::join(headers, ", "));

    auto read_headers = [&]<size_t... I>(std::index_sequence<I...>) {
      reader.read_header(io::ignore_extra_column, headers[I]...);
    };
    read_headers(std::make_index_sequence<N>{});

    auto read_values = [&]<size_t... I>(std::index_sequence<I...>) {
      std::array<std::string, N> values;
      if (!reader.read_row((values[I])...)) {
        CRANE_ERROR("No data found in CSV {}", parser_name);
        return std::optional<std::vector<std::string>>();
      }
      return std::optional<std::vector<std::string>>(
          std::vector<std::string>(values.begin(), values.end()));
    };

    auto values = read_values(std::make_index_sequence<N>{});
    if (!values) {
      return false;
    }

    return callback(*values);
  } catch (const io::error::base& e) {
    CRANE_ERROR("Failed to parse CSV {}: {}", parser_name, e.what());
    return false;
  }
}

template <typename Callback>
bool ParseInfluxCsv(const std::string& response, const std::string& parser_name,
                    const std::vector<std::string>& headers,
                    Callback&& callback) {
  switch (headers.size()) {
  case 1:
    return ParseInfluxCsvImpl<1>(response, parser_name, headers, callback);
  case 2:
    return ParseInfluxCsvImpl<2>(response, parser_name, headers, callback);
  case 3:
    return ParseInfluxCsvImpl<3>(response, parser_name, headers, callback);
  case 4:
    return ParseInfluxCsvImpl<4>(response, parser_name, headers, callback);
  default:
    CRANE_ERROR("Unsupported number of headers: {}", headers.size());
    return false;
  }
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
      result.error = "network error: " + std::string(curl_easy_strerror(res));
      return result;
    }

    long http_code;
    curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &http_code);
    if (http_code != 200) {
      result.error = "server error: HTTP " + std::to_string(http_code);
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

std::string InfluxDBClient::Query_(const std::string& flux_query) const {
  CurlWrapper curl;
  std::string url = m_url_ + "/api/v2/query?org=" + m_org_;

  auto result = curl.Execute(url, flux_query, m_token_);
  if (!result.success) {
    CRANE_ERROR("InfluxDB query failed: {}", result.error);
    return {};
  }

  return result.data;
}

std::string InfluxDBClient::BuildNodeStatsQuery_(const std::string& node_id,
                                                 int64_t window_seconds) const {
  return fmt::format(
      "union(\n"
      "  tables: [\n"
      "    from(bucket: \"{0}\") "
      "|> range(start: -{1}s) "
      "|> filter(fn: (r) => r.node_id == \"{2}\") "
      "|> filter(fn: (r) => r._field == \"cpu_utilization\" or "
      "                     r._field == \"memory_utilization\" or "
      "                     r._field == \"gpu_utilization\" or "
      "                     r._field == \"ipmi_power_w\") "
      "|> mean() "
      "|> pivot(rowKey:[], columnKey: [\"_field\"], valueColumn: \"_value\"),\n"
      "    from(bucket: \"{0}\") "
      "|> range(start: -{1}s) "
      "|> filter(fn: (r) => r.node_id == \"{2}\") "
      "|> filter(fn: (r) => r._field == \"total_energy_j\") "
      "|> sum() "
      "|> yield(name: \"sum\")\n"
      "  ]\n"
      ")",
      g_config.InfluxDbNodeBucket, window_seconds, node_id);
}

bool InfluxDBClient::ParseNodeStatsResponse_(const std::string& response,
                                             NodeStats* stats) const {
  std::vector<std::string> headers = {"cpu_utilization", "memory_utilization",
                                      "gpu_utilization", "ipmi_power_w",
                                      "total_energy_j"};

  return ParseInfluxCsv(response, "node_stats_resp", headers,
                        [stats](const std::vector<std::string>& row) {
                          stats->cpu_util = std::stod(row[0]);
                          stats->mem_util = std::stod(row[1]);
                          stats->gpu_util = std::stod(row[2]);
                          stats->avg_power = std::stod(row[3]);
                          stats->total_energy = std::stod(row[4]);
                          return true;
                        });
}

bool InfluxDBClient::QueryNodeEnergyInfo(const std::string& node_id,
                                         int64_t window,
                                         NodeStats* stats) const {
  auto response = Query_(BuildNodeStatsQuery_(node_id, window));

  if (response.empty()) {
    CRANE_WARN("Empty response for node {} with window {}s", node_id, window);
    return false;
  }

  if (!ParseNodeStatsResponse_(response, stats)) {
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

std::string InfluxDBClient::BuildTaskStatsQuery_(const std::string& node_id,
                                                 int64_t window_seconds) const {
  return fmt::format(
      "from(bucket: \"{}\") "
      "|> range(start: -{}s) "
      "|> filter(fn: (r) => r[\"node_id\"] == \"{}\") "
      "|> filter(fn: (r) => r._field == \"total_energy_j\" or "
      "                     r._field == \"cpu_utilization\" or "
      "                     r._field == \"memory_usage_mb\" or "
      "                     r._field == \"duration\") "
      "|> mean() "
      "|> pivot(rowKey:[], columnKey: [\"_field\"], valueColumn: \"_value\")",
      g_config.InfluxDbTaskBucket, window_seconds, node_id);
}

bool InfluxDBClient::ParseTaskStatsResponse_(const std::string& response,
                                             double* efficiency) const {
  static const std::vector<std::string> kHeaders = {
      "total_energy_j", "cpu_utilization", "memory_usage_mb", "duration"};

  return ParseInfluxCsv(
      response, "task_efficiency", kHeaders,
      [efficiency](const std::vector<std::string>& row) {
        double total_energy = std::stod(row[0]);
        double cpu_util = std::stod(row[1]);
        double mem_usage = std::stod(row[2]);
        double duration = std::stod(row[3]);

        if (total_energy <= 0.0 || duration <= 0.0) {
          CRANE_WARN("Invalid values: energy={}, duration={}", total_energy,
                     duration);
          return false;
        }

        *efficiency = (cpu_util * mem_usage) / (total_energy * duration);

        CRANE_DEBUG(
            "Task stats: energy={:.2f}J, cpu={:.2f}%, mem={:.2f}MB, "
            "duration={:.2f}s, efficiency={:.2f}",
            total_energy, cpu_util, mem_usage, duration, *efficiency);
        return true;
      });
}

double InfluxDBClient::QueryTaskEnergyInfo(const std::string& node_id,
                                           int64_t window) const {
  try {
    auto query = BuildTaskStatsQuery_(node_id, window);
    CRANE_DEBUG("Executing InfluxDB query: {}", query);

    std::string resp = Query_(query);
    if (resp.empty()) {
      CRANE_WARN(
          "Empty response from InfluxDB query, using default efficiency");
      return 1.0;
    }

    double efficiency;
    if (!ParseTaskStatsResponse_(resp, &efficiency)) {
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