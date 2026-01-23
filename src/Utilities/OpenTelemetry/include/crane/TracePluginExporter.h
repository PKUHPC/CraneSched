#pragma once

#include <functional>
#include <type_traits>
#include <variant>
#include <vector>

#include "crane/Logger.h"
#include "crane/PluginClient.h"
#include "opentelemetry/sdk/trace/exporter.h"
#include "opentelemetry/sdk/trace/span_data.h"
#include "protos/Plugin.grpc.pb.h"
#include "protos/Plugin.pb.h"

namespace crane {

class TracePluginExporter : public opentelemetry::sdk::trace::SpanExporter {
 public:
  using PluginClientGetter = std::function<plugin::PluginClient*()>;
  using PluginEnabledGetter = std::function<bool()>;

  TracePluginExporter(PluginClientGetter client_getter,
                      PluginEnabledGetter enabled_getter)
      : m_client_getter_(std::move(client_getter)),
        m_enabled_getter_(std::move(enabled_getter)) {}

  std::unique_ptr<opentelemetry::sdk::trace::Recordable> MakeRecordable()
      noexcept override {
    return std::unique_ptr<opentelemetry::sdk::trace::Recordable>(
        new opentelemetry::sdk::trace::SpanData());
  }

  opentelemetry::sdk::common::ExportResult Export(
      const opentelemetry::nostd::span<
          std::unique_ptr<opentelemetry::sdk::trace::Recordable>>& spans)
      noexcept override {
    if (!m_enabled_getter_() || !m_client_getter_()) {
      return opentelemetry::sdk::common::ExportResult::kSuccess;
    }

    auto* client = m_client_getter_();
    if (!client) {
      return opentelemetry::sdk::common::ExportResult::kSuccess;
    }

    CRANE_TRACE("TracePluginExporter exporting {} spans", spans.size());

    std::vector<crane::grpc::plugin::SpanInfo> span_infos;
    span_infos.reserve(spans.size());

    for (auto& recordable : spans) {
      auto span = std::unique_ptr<opentelemetry::sdk::trace::SpanData>(
          static_cast<opentelemetry::sdk::trace::SpanData*>(
              recordable.release()));

      crane::grpc::plugin::SpanInfo info;

      char trace_id_buf[32];
      span->GetTraceId().ToLowerBase16(trace_id_buf);
      info.set_trace_id(std::string(trace_id_buf, 32));

      char span_id_buf[16];
      span->GetSpanId().ToLowerBase16(span_id_buf);
      info.set_span_id(std::string(span_id_buf, 16));

      char parent_span_id_buf[16];
      span->GetParentSpanId().ToLowerBase16(parent_span_id_buf);
      info.set_parent_span_id(std::string(parent_span_id_buf, 16));

      info.set_name(std::string(span->GetName()));

      auto start_ts = span->GetStartTime().time_since_epoch();
      info.mutable_start_time()->set_seconds(
          std::chrono::duration_cast<std::chrono::seconds>(start_ts).count());
      info.mutable_start_time()->set_nanos(
          std::chrono::duration_cast<std::chrono::nanoseconds>(start_ts)
              .count() %
          1000000000);

      auto end_ts = span->GetStartTime().time_since_epoch() + span->GetDuration();
      info.mutable_end_time()->set_seconds(
          std::chrono::duration_cast<std::chrono::seconds>(end_ts).count());
      info.mutable_end_time()->set_nanos(
          std::chrono::duration_cast<std::chrono::nanoseconds>(end_ts).count() %
          1000000000);

      for (const auto& [key, value] : span->GetAttributes()) {
        std::visit(
            [&](auto&& arg) {
              using T = std::decay_t<decltype(arg)>;
              if constexpr (std::is_same_v<T, bool>) {
                (*info.mutable_attributes())[key] = arg ? "true" : "false";
              } else if constexpr (std::is_arithmetic_v<T>) {
                (*info.mutable_attributes())[key] = std::to_string(arg);
              } else if constexpr (std::is_same_v<T, std::string>) {
                (*info.mutable_attributes())[key] = arg;
              } else if constexpr (std::is_same_v<T, const char*>) {
                (*info.mutable_attributes())[key] = arg;
              }
            },
            value);
      }

      span_infos.emplace_back(std::move(info));
    }

    client->TraceHookAsync(std::move(span_infos));

    return opentelemetry::sdk::common::ExportResult::kSuccess;
  }

  bool Shutdown(std::chrono::microseconds timeout) noexcept override {
    return true;
  }
  bool ForceFlush(std::chrono::microseconds timeout) noexcept override {
    return true;
  }

 private:
  PluginClientGetter m_client_getter_;
  PluginEnabledGetter m_enabled_getter_;
};

}  // namespace crane
