#include "crane/InfluxDbExporter.h"

#ifdef CRANE_ENABLE_TRACING

#  include <curl/curl.h>

#  include <chrono>
#  include <iomanip>
#  include <iostream>
#  include <sstream>

#  include "opentelemetry/nostd/variant.h"
#  include "opentelemetry/sdk/trace/recordable.h"
#  include "opentelemetry/sdk/trace/simple_processor_factory.h"

// Note: In detailed implementation, we should cast Recordable to the specific
// type we created. We will use the default SpanData recordable from the SDK.
// Since opentelemetry-cpp doesn't expose a simple "GetAttributes" on the
// interface easily without casting, we rely on the fact that we return a
// SpanData object in MakeRecordable.

// However, SpanData is not fully exposed as a public header in all versions or
// requires specific includes. Let's check headers.
#  include "opentelemetry/sdk/trace/simple_processor.h"

// To access SpanData, we can use the pattern from OStreamExporter:
// it assumes the recordable is printable or accessible.
// Actually, standard Exporters use `opentelemetry::sdk::trace::Recordable`.
// But to get data, we need to specific methods.
// The standard SDK provided Recordable has methods like SetName, SetStartTime
// etc. But to READ them, we usually need the concrete class `SpanData`. Let's
// assume we use the default Recordable from SDK which is `SpanData`.

// Since we cannot easily import SpanData definition without correct dependency
// paths which might vary, we'll try to use the IterateAttributes if available
// or we might need to copy necessary headers. Wait, SpanData is usually in
// `opentelemetry/sdk/trace/span_data.h`.

#  include "opentelemetry/sdk/trace/span_data.h"

namespace crane {

InfluxDbExporter::InfluxDbExporter(const InfluxDbConfig& config)
    : config_(config) {
  curl_global_init(CURL_GLOBAL_ALL);
}

InfluxDbExporter::~InfluxDbExporter() { curl_global_cleanup(); }

std::unique_ptr<opentelemetry::sdk::trace::Recordable>
InfluxDbExporter::MakeRecordable() noexcept {
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

bool InfluxDbExporter::Shutdown(std::chrono::microseconds timeout) noexcept {
  is_shutdown_ = true;
  return true;
}

opentelemetry::sdk::common::ExportResult InfluxDbExporter::Export(
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
    // Note: functionality limitations in specific implementation, using
    // hardcoded tags or extracting from somewhere else Ideally we should inject
    // Resource tags here. SpanData has GetResource().

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

    char trace_id_hex[33];
    span_data->GetTraceId().ToLowerBase16(trace_id_hex);
    ss << ",trace_id=" << trace_id_hex;

    char span_id_hex[17];
    span_data->GetSpanId().ToLowerBase16(span_id_hex);
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

void InfluxDbExporter::SendData(const std::string& data) {
  CURL* curl = curl_easy_init();
  if (!curl) return;

  std::string write_url = config_.url + "/api/v2/write?org=" + config_.org +
                          "&bucket=" + config_.bucket + "&precision=ns";

  struct curl_slist* headers = nullptr;
  headers = curl_slist_append(
      headers, ("Authorization: Token " + config_.token).c_str());
  headers =
      curl_slist_append(headers, "Content-Type: text/plain; charset=utf-8");

  curl_easy_setopt(curl, CURLOPT_URL, write_url.c_str());
  curl_easy_setopt(curl, CURLOPT_POST, 1L);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data.c_str());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  // In production code, we should handle errors and maybe use a connection pool
  // or async client. For this implementation, we use simple blocking curl.
  CURLcode res = curl_easy_perform(curl);

  if (res != CURLE_OK) {
    std::cerr << "[InfluxDbExporter] Upload failed: " << curl_easy_strerror(res)
              << std::endl;
  }

  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
}

}  // namespace crane

#endif  // CRANE_ENABLE_TRACING
