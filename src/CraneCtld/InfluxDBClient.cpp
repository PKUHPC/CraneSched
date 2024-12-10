#include "InfluxDBClient.h"

#include <curl/curl.h>
#include <bsoncxx/json.hpp>
#include <sstream>
#include "csv.h" // fast-cpp-csv-parser

namespace Ctld {

constexpr const char* kEnergyTaskBucket = "energy_task";
constexpr const char* kEnergyNodeBucket = "energy_node";

// 用于处理HTTP响应的回调函数
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
  ((std::string*)userp)->append((char*)contents, size * nmemb);
  return size * nmemb;
}

bool InfluxDBClient::Write(const std::string& bucket, const std::string& org,
                          const std::string& measurement, const std::string& field,
                          double value) const {
    CURL* curl;
    CURLcode res;
    
    curl = curl_easy_init();
    if(curl) {
        std::string url = m_url_ + "/api/v2/write?bucket=" + bucket + "&org=" + org;
        
        // 构建行协议数据
        std::stringstream data;
        data << measurement << " " << field << "=" << value;
        std::string lineProtocol = data.str();

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, lineProtocol.c_str());

        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: text/plain; charset=utf-8");
        headers = curl_slist_append(headers, ("Authorization: Token " + m_token_).c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        res = curl_easy_perform(curl);
        
        curl_easy_cleanup(curl);
        curl_slist_free_all(headers);
        
        return res == CURLE_OK;
    }
    return false;
}

std::string InfluxDBClient::Query(const std::string& flux_query) const {
    CURL* curl;
    CURLcode res;
    std::string readBuffer;

    curl = curl_easy_init();
    if(curl) {
        std::string url = m_url_ + "/api/v2/query?org=" + m_org_;
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, flux_query.c_str());

        // 设置HTTP头
        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/vnd.flux");
        headers = curl_slist_append(headers, ("Authorization: Token " + m_token_).c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);

        res = curl_easy_perform(curl);
        if(res != CURLE_OK) {
            // 处理错误
            fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
        }

        curl_easy_cleanup(curl);
        curl_slist_free_all(headers);
    }
    return readBuffer;
}

bool InfluxDBClient::QueryNodeEnergyInfo(const std::string& node_id, 
                                        int64_t window,
                                        NodeStats* stats) const {
  std::string util_power_query = fmt::format(
      "from(bucket: \"{}\") "
      "|> range(start: -{}s) "
      "|> filter(fn: (r) => r[\"node_id\"] == \"{}\") "
      "|> filter(fn: (r) => r._field =~ /ipmi_power_w|cpu_utilization|memory_utilization|gpu_utilization/) "
      "|> mean() "
      "|> pivot(rowKey:[], columnKey: [\"_field\"], valueColumn: \"_value\")",  // 将字段转为列
      kEnergyNodeBucket, window, node_id);

  std::string energy_query = fmt::format(
      "from(bucket: \"{}\") "
      "|> range(start: -{}s) "
      "|> filter(fn: (r) => r[\"node_id\"] == \"{}\") "
      "|> filter(fn: (r) => r._field == \"total_energy_j\") "
      "|> sum() "
      "|> yield(name: \"sum\")",
      kEnergyNodeBucket, window, node_id);

  std::string util_power_resp = Query(util_power_query);
  std::string energy_resp = Query(energy_query);

  if (util_power_resp.empty() || energy_resp.empty()) {
    CRANE_WARN("Empty response for node {} with window {}s", node_id, window);
    return false;
  }

  try {
    std::istringstream util_power_stream(util_power_resp);
    io::CSVReader<4, io::trim_chars<' ', '\t'>, io::double_quote_escape<',', '"'>> 
        util_power_in("util_power_resp", util_power_stream);
    
    util_power_in.read_header(io::ignore_extra_column,
                             "cpu_utilization",
                             "memory_utilization", 
                             "gpu_utilization",
                             "ipmi_power_w");

    double cpu_util, mem_util, gpu_util, power;
    if (util_power_in.read_row(cpu_util, mem_util, gpu_util, power)) {
      stats->cpu_util = cpu_util;
      stats->mem_util = mem_util;
      stats->gpu_util = gpu_util;
      stats->current_power = power;
      stats->avg_power = power;
    }

    std::istringstream energy_stream(energy_resp);
    io::CSVReader<1, io::trim_chars<' ', '\t'>, io::double_quote_escape<',', '"'>> 
        energy_in("energy_resp", energy_stream);
    
    energy_in.read_header(io::ignore_extra_column, "_value");

    double total_energy;
    if (energy_in.read_row(total_energy)) {
      stats->total_energy = total_energy;
    }

    stats->time_span = absl::Seconds(window);
    CRANE_DEBUG("Node {} stats in {}s window: CPU={:.1f}%, Mem={:.1f}%, GPU={:.1f}%, "
                "Power={:.1f}W, Energy={:.1f}J",
                node_id, window, 
                stats->cpu_util, stats->mem_util, stats->gpu_util,
                stats->avg_power, stats->total_energy);

    return true;
  } catch (const io::error::base& e) {
    CRANE_ERROR("Failed to parse InfluxDB CSV response for node {}: {}", 
                node_id, e.what());
    return false;
  }
}

double InfluxDBClient::QueryTaskEnergyEfficiency(double requested_cpu, 
                                               double requested_mem_gb,
                                               absl::Duration time_window) const {
  try {
    CRANE_DEBUG("Querying task energy efficiency for CPU={:.2f} cores, Memory={:.2f}GB", 
                requested_cpu, requested_mem_gb);

    std::string query = fmt::format(
        "from(bucket: \"energy_task\") "
        "|> range(start: -{}s) "
        "|> filter(fn: (r) => r._field == \"total_energy_j\") "
        "|> drop(columns: [\"task_id\", \"node_id\"]) "
        "|> mean(column: \"_value\")",
        absl::ToInt64Seconds(time_window));
    
    CRANE_DEBUG("Executing InfluxDB query: {}", query);
    
    std::string resp = Query(query);
    if (resp.empty()) {
      CRANE_WARN("Empty response from InfluxDB query");
      return 1.0;
    }

    // TODO: 解析csv

    CRANE_DEBUG("InfluxDB response: {}", resp);

    return 1.0;
  } catch (const std::exception& e) {
    CRANE_ERROR("Failed to query task energy efficiency: {}", e.what());
    return 1.0;
  }
}
}