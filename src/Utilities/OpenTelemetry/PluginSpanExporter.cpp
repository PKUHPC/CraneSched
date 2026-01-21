#include "crane/PluginSpanExporter.h"

#ifdef CRANE_ENABLE_TRACING

#  include <chrono>
#  include <fstream>
#  include <iomanip>
#  include <iostream>
#  include <sstream>

#  include "crane/PluginClient.h"
#  include "opentelemetry/nostd/variant.h"
#  include "opentelemetry/sdk/trace/recordable.h"
#  include "opentelemetry/sdk/trace/simple_processor_factory.h"
#  include "opentelemetry/sdk/trace/span_data.h"

namespace crane {

PluginSpanExporter::PluginSpanExporter(const PluginSpanConfig& config)
    : config_(config) {}

PluginSpanExporter::~PluginSpanExporter() {}

std::unique_ptr<opentelemetry::sdk::trace::Recordable>
PluginSpanExporter::MakeRecordable() noexcept {
  return std::make_unique<opentelemetry::sdk::trace::SpanData>();
}

// Helper to escape characters for InfluxDB Line Protocol
// For Tag Key, Tag Value, Field Key: Escape spaces, commas, equals signs
std::string EscapeTag(const std::string& input) {
  std::string output;
  output.reserve(input.size());
  for (char c : input) {
    if (c == ',' || c == '=' || c == ' ') {
      output += '\\';
    }
    output += c;
  }
  return output;
}

// For Field Value (String): Escape double quotes, backslash
std::string EscapeStringField(const std::string& input) {
  std::string output;
  output.reserve(input.size());
  for (char c : input) {
    if (c == '"' || c == '\\') {
      output += '\\';
    }
    output += c;
  }
  return output;
}

bool PluginSpanExporter::Shutdown(std::chrono::microseconds timeout) noexcept {
  is_shutdown_ = true;
  return true;
}

opentelemetry::sdk::common::ExportResult PluginSpanExporter::Export(
    const opentelemetry::nostd::span<
        std::unique_ptr<opentelemetry::sdk::trace::Recordable>>&
        spans) noexcept {
  if (is_shutdown_) {
    return opentelemetry::sdk::common::ExportResult::kFailure;
  }

  std::stringstream ss;

  for (const auto& recordable : spans) {
    auto span_data =
        static_cast<opentelemetry::sdk::trace::SpanData*>(recordable.get());

    // Format: measurement,tag1=val1,tag2=val2 field1=val1,field2="string"
    // timestamp

    ss << EscapeTag(config_.measurement);

    // Tags
    ss << ",service_name="
       << EscapeTag(config_.measurement.empty() ? "crane" : "crane");

    // Resource attributes
    const auto& resource = span_data->GetResource();
    auto resource_attr = resource.GetAttributes();
    if (resource_attr.find("service.name") != resource_attr.end()) {
      auto val = resource_attr.at("service.name");
      if (opentelemetry::nostd::holds_alternative<std::string>(val)) {
        ss << ",service="
           << EscapeTag(opentelemetry::nostd::get<std::string>(val));
      }
    }

    ss << ",span_name=" << EscapeTag(std::string(span_data->GetName()));

    char trace_id_hex[33] = {0};
    span_data->GetTraceId().ToLowerBase16(
        opentelemetry::nostd::span<char, 32>{trace_id_hex, 32});
    ss << ",trace_id=" << trace_id_hex;

    char span_id_hex[17] = {0};
    span_data->GetSpanId().ToLowerBase16(
        opentelemetry::nostd::span<char, 16>{span_id_hex, 16});
    ss << ",span_id=" << span_id_hex;

    // Fields
    ss << " ";  // Split tags and fields

    auto duration = span_data->GetDuration();
    auto duration_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
    ss << "duration_ns=" << duration_ns << "i";

    // Attributes as fields
    for (const auto& kv : span_data->GetAttributes()) {
      const auto& key = kv.first;
      const auto& value = kv.second;

      ss << "," << EscapeTag(key) << "=";

      if (opentelemetry::nostd::holds_alternative<bool>(value)) {
        ss << (opentelemetry::nostd::get<bool>(value) ? "true" : "false");
      } else if (opentelemetry::nostd::holds_alternative<int32_t>(value)) {
        ss << opentelemetry::nostd::get<int32_t>(value) << "i";
      } else if (opentelemetry::nostd::holds_alternative<int64_t>(value)) {
        ss << opentelemetry::nostd::get<int64_t>(value) << "i";
      } else if (opentelemetry::nostd::holds_alternative<uint32_t>(value)) {
        ss << opentelemetry::nostd::get<uint32_t>(value) << "i";
      } else if (opentelemetry::nostd::holds_alternative<uint64_t>(value)) {
        ss << opentelemetry::nostd::get<uint64_t>(value) << "i";
      } else if (opentelemetry::nostd::holds_alternative<double>(value)) {
        ss << opentelemetry::nostd::get<double>(value);
      } else if (opentelemetry::nostd::holds_alternative<std::string>(value)) {
        ss << "\""
           << EscapeStringField(opentelemetry::nostd::get<std::string>(value))
           << "\"";
      } else {
        ss << "\"" << "unsupported_type" << "\"";
      }
    }

    // Timestamp
    auto start_time = span_data->GetStartTime();
    auto start_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        start_time.time_since_epoch())
                        .count();
    ss << " " << start_ns << "\n";
  }

  SendData(ss.str());

  return opentelemetry::sdk::common::ExportResult::kSuccess;
}

void PluginSpanExporter::SendData(const std::string& data) {
  // Temporary output to file for testing
  std::ofstream out("/nfs/home/interntwo/crane/output/traces.log",
                    std::ios::app);
  if (out.is_open()) {
    out << data;
    out.close();
  }

  if (!g_plugin_client) {
    return;
  }

  std::vector<std::string> records;
  std::stringstream ss(data);
  std::string segment;
  while (std::getline(ss, segment)) {
    if (!segment.empty()) {
      records.push_back(segment);
    }
  }

  if (!records.empty()) {
    g_plugin_client->InsertSpansHookAsync(records);
  }
}

}  // namespace crane

#endif  // CRANE_ENABLE_TRACING
