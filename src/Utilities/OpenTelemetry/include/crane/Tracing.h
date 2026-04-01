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

/**
 * @file Tracing.h
 * @brief CraneSched Tracing Infrastructure
 *
 * Provides RAII-based OpenTelemetry span management with both compile-time
 * and runtime control. All tracing overhead is eliminated at compile time
 * when CRANE_ENABLE_TRACING is not defined.
 *
 * Two-level gating:
 *   - Compile-time: CRANE_ENABLE_TRACING macro (set via CMake option)
 *   - Runtime: crane::g_tracing_enabled atomic bool (set via config.yaml)
 *
 * ============================================================================
 * Quick Reference
 * ============================================================================
 *
 * 1) CRANE_TRACE_SCOPE(name)
 *    Create an RAII span covering the current scope. The span automatically
 *    ends when the scope exits. Uses a fixed variable name `_crane_scope_span_`
 *    so only ONE per C++ scope. Use with
 * CRANE_TRACE_SET_ATTR/CRANE_TRACE_EVENT.
 *
 *      void ProcessJob(int job_id) {
 *        CRANE_TRACE_SCOPE("ProcessJob");
 *        CRANE_TRACE_SET_ATTR("job_id", job_id);
 *        // ... work ...
 *        CRANE_TRACE_EVENT("validation_done");
 *        // ... more work ...
 *        // span ends automatically here
 *      }
 *
 * 2) CRANE_TRACE_SCOPE_NAMED(var, name)
 *    Same as CRANE_TRACE_SCOPE but with a custom variable name. Use this
 *    when you need to reference the span explicitly (e.g., to create children
 *    or set attributes conditionally).
 *
 *      void ScheduleBatch() {
 *        CRANE_TRACE_SCOPE_NAMED(batch_span, "ScheduleBatch");
 *        batch_span.SetAttribute("batch_size", jobs.size());
 *
 *        for (auto& job : jobs) {
 *          CRANE_TRACE_CHILD_NAMED(job_span, batch_span, "ScheduleJob");
 *          job_span.SetAttribute("job_id", job.id);
 *          // job_span ends at loop iteration boundary
 *        }
 *        // batch_span ends here, measuring total duration
 *      }
 *
 * 3) CRANE_TRACE_POINT(name)
 *    Create an instant span (starts and ends immediately). Use for marking
 *    that an event occurred without measuring duration.
 *
 *      CRANE_TRACE_POINT("ConfigReloaded");
 *
 * 4) CRANE_TRACE_POINT_ATTR(name, key, value)
 *    Same as CRANE_TRACE_POINT but with one attribute attached.
 *
 *      CRANE_TRACE_POINT_ATTR("Alloc Job", "job_id", job->TaskId());
 *
 * 5) CRANE_TRACE_CHILD_NAMED(var, parent, name)
 *    Create a child span under an existing ScopedSpan. The child inherits
 *    the parent's trace context, forming a parent-child hierarchy.
 *    Can be nested to arbitrary depth.
 *
 *      CRANE_TRACE_SCOPE_NAMED(root, "HandleRequest");
 *      {
 *        CRANE_TRACE_CHILD_NAMED(parse, root, "ParseRequest");
 *        // ...
 *      }  // parse ends here
 *      {
 *        CRANE_TRACE_CHILD_NAMED(exec, root, "ExecuteRequest");
 *        {
 *          CRANE_TRACE_CHILD_NAMED(db, exec, "DatabaseQuery");
 *          // Nested 3 levels: root -> exec -> db
 *        }
 *      }
 *
 * 6) CRANE_TRACE_SET_ATTR(key, value)
 *    Set an attribute on the span created by CRANE_TRACE_SCOPE in the
 *    current scope. Only works with CRANE_TRACE_SCOPE (not NAMED variant).
 *
 *      CRANE_TRACE_SCOPE("MyOp");
 *      CRANE_TRACE_SET_ATTR("node_count", 42);
 *
 * 7) CRANE_TRACE_EVENT(event_name)
 *    Add a timestamped event (annotation) to the span created by
 *    CRANE_TRACE_SCOPE. Unlike CRANE_TRACE_POINT which creates a separate
 *    span, events are recorded WITHIN the parent span's timeline.
 *
 *      CRANE_TRACE_SCOPE("BatchCommit");
 *      // ... prepare ...
 *      CRANE_TRACE_EVENT("prepare_done");   // timestamp marker inside span
 *      // ... write to DB ...
 *      CRANE_TRACE_EVENT("db_write_done");  // another timestamp marker
 *      // span ends with full duration measurement
 *
 * ============================================================================
 * ScopedSpan Class API (for advanced usage beyond macros)
 * ============================================================================
 *
 *   void End()
 *     Manually end the span early. Idempotent -- safe to call multiple times.
 *     After End(), SetAttribute/AddEvent/CreateChild become no-ops.
 *     The destructor also calls End(), so this is optional.
 *
 *   void SetAttribute(string_view key, const T& value)
 *     Attach a key-value attribute to the span.
 *
 *   ScopedSpan CreateChild(string_view name)
 *     Create a child span linked to this span's trace context.
 *     Returns a no-op ScopedSpan if this span is inactive.
 *
 *   void AddEvent(string_view event_name)
 *     Record a timestamped event within this span.
 *
 *   bool IsActive()
 *     Returns true if the span was created and not yet ended.
 *
 *   SpanContext GetContext()
 *     Get the span's context for cross-RPC propagation.
 *
 * ============================================================================
 * TRACE_POINT vs AddEvent
 * ============================================================================
 *
 *   CRANE_TRACE_POINT:
 *     Creates an independent span with its own trace_id/span_id.
 *     Shows up as a separate entry in the tracing backend.
 *     Use when there is no parent span in the current scope.
 *
 *   AddEvent / CRANE_TRACE_EVENT:
 *     Adds a timestamp annotation WITHIN an existing span.
 *     Does NOT create a new span. Shows as a sub-event of the parent.
 *     Use for marking timepoints inside a long-running operation.
 */

#pragma once

#include <atomic>
#include <string>
#include <string_view>
#include <utility>

#ifdef CRANE_ENABLE_TRACING
#  include "crane/TracerManager.h"
#  include "opentelemetry/trace/propagation/http_trace_context.h"
#  include "opentelemetry/trace/provider.h"
#  include "opentelemetry/trace/span.h"
#  include "opentelemetry/trace/tracer.h"
#endif

namespace crane {

/// Runtime tracing enable flag.
/// Compile-time gate: CRANE_ENABLE_TRACING (CMake option).
/// Runtime gate: this flag, controlled by config.yaml `Tracing.Enabled`.
/// Both must be true for spans to be created.
inline std::atomic<bool> g_tracing_enabled{false};

#ifdef CRANE_ENABLE_TRACING

using StatusCode = opentelemetry::trace::StatusCode;

/// RAII wrapper around an OpenTelemetry span.
///
/// - Constructor starts the span (if both compile-time and runtime enabled).
/// - Destructor calls End() automatically.
/// - End() is idempotent, safe to call manually for early release.
/// - All methods are no-ops if the span was not created.
/// - Movable but not copyable.
class ScopedSpan {
 public:
  /// Construct a root span (no parent).
  ScopedSpan(
      std::string_view name,
      opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer)
      : tracer_(std::move(tracer)), ended_(false) {
    if (g_tracing_enabled.load(std::memory_order_relaxed) && tracer_) {
      span_ = tracer_->StartSpan(std::string(name));
    } else {
      ended_ = true;
    }
  }

  /// Construct a child span linked to a parent context.
  ScopedSpan(
      std::string_view name,
      opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer,
      const opentelemetry::trace::SpanContext& parent_ctx)
      : tracer_(std::move(tracer)), ended_(false) {
    if (g_tracing_enabled.load(std::memory_order_relaxed) && tracer_) {
      opentelemetry::trace::StartSpanOptions opts;
      opts.parent = parent_ctx;
      span_ = tracer_->StartSpan(std::string(name), opts);
    } else {
      ended_ = true;
    }
  }

  ScopedSpan(const ScopedSpan&) = delete;
  ScopedSpan& operator=(const ScopedSpan&) = delete;

  ScopedSpan(ScopedSpan&& other) noexcept
      : tracer_(std::move(other.tracer_)),
        span_(std::move(other.span_)),
        ended_(other.ended_) {
    other.ended_ = true;
  }

  ScopedSpan& operator=(ScopedSpan&& other) noexcept {
    if (this != &other) {
      End();
      tracer_ = std::move(other.tracer_);
      span_ = std::move(other.span_);
      ended_ = other.ended_;
      other.ended_ = true;
    }
    return *this;
  }

  ~ScopedSpan() { End(); }

  /// End the span manually. Idempotent -- calling multiple times is safe.
  /// After End(), all other methods become no-ops.
  void End() {
    if (!ended_ && span_) {
      span_->End();
      ended_ = true;
    }
  }

  /// Attach a key-value attribute to this span.
  template <typename T>
  void SetAttribute(std::string_view key, const T& value) {
    if (span_ && !ended_) {
      span_->SetAttribute(std::string(key), value);
    }
  }

  /// Create a child span linked to this span's trace context.
  /// Returns a no-op ScopedSpan if this span is not active.
  [[nodiscard]] ScopedSpan CreateChild(std::string_view child_name) const {
    if (span_ && !ended_) {
      return ScopedSpan(child_name, tracer_, span_->GetContext());
    }
    return ScopedSpan();
  }

  /// Check if the span was created and not yet ended.
  [[nodiscard]] bool IsActive() const { return span_ && !ended_; }

  /// Get the span context for cross-RPC trace propagation.
  [[nodiscard]] opentelemetry::trace::SpanContext GetContext() const {
    if (span_) {
      return span_->GetContext();
    }
    return opentelemetry::trace::SpanContext::GetInvalid();
  }

  /// Record a timestamped event within this span's timeline.
  void AddEvent(std::string_view event_name) {
    if (span_ && !ended_) {
      span_->AddEvent(std::string(event_name));
    }
  }

  /// Set the span's status code (OK, ERROR, or UNSET).
  void SetStatus(opentelemetry::trace::StatusCode code,
                 std::string_view description = {}) {
    if (span_ && !ended_) {
      span_->SetStatus(code, std::string(description));
    }
  }

 private:
  /// No-op constructor (for when tracing is disabled at runtime).
  ScopedSpan() : ended_(true) {}

  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer_;
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;
  bool ended_ = true;
};

/// Non-RAII span for long-lived operations that cross scope boundaries.
///
/// Unlike ScopedSpan, ManualSpan does NOT auto-end on destruction.
/// The caller MUST call End() explicitly when the operation completes.
/// Designed for storing on persistent objects (e.g., JobInCtld).
///
/// Example:
///   CRANE_TRACE_MANUAL(span, "job/lifecycle");
///   span.SetAttribute("job_id", 123);
///   job->lifecycle_span_ = std::move(span);
///   // ... hours later, in a different thread ...
///   job->lifecycle_span_.End();
class ManualSpan {
 public:
  ManualSpan() = default;

  ManualSpan(
      std::string_view name,
      opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer)
      : tracer_(std::move(tracer)) {
    if (g_tracing_enabled.load(std::memory_order_relaxed) && tracer_) {
      span_ = tracer_->StartSpan(std::string(name));
    }
  }

  ManualSpan(
      std::string_view name,
      opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer,
      const opentelemetry::trace::SpanContext& parent_ctx)
      : tracer_(std::move(tracer)) {
    if (g_tracing_enabled.load(std::memory_order_relaxed) && tracer_) {
      opentelemetry::trace::StartSpanOptions opts;
      opts.parent = parent_ctx;
      span_ = tracer_->StartSpan(std::string(name), opts);
    }
  }

  ~ManualSpan() = default;  // Does NOT call End()

  ManualSpan(const ManualSpan&) = delete;
  ManualSpan& operator=(const ManualSpan&) = delete;
  ManualSpan(ManualSpan&&) noexcept = default;
  ManualSpan& operator=(ManualSpan&&) noexcept = default;

  void End() {
    if (span_) {
      span_->End();
      span_.reset();
    }
  }

  template <typename T>
  void SetAttribute(std::string_view key, const T& value) {
    if (span_) span_->SetAttribute(std::string(key), value);
  }

  void AddEvent(std::string_view event_name) {
    if (span_) span_->AddEvent(std::string(event_name));
  }

  void SetStatus(StatusCode code, std::string_view desc = {}) {
    if (span_) span_->SetStatus(code, std::string(desc));
  }

  [[nodiscard]] bool IsActive() const { return span_ != nullptr; }

  [[nodiscard]] opentelemetry::trace::SpanContext GetContext() const {
    return span_ ? span_->GetContext()
                 : opentelemetry::trace::SpanContext::GetInvalid();
  }

 private:
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer_;
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;
};

// ============================================================================
// Cross-process trace context propagation (W3C TraceContext format)
// ============================================================================

/// Serialize a SpanContext to W3C traceparent format:
/// "00-{trace_id_32hex}-{span_id_16hex}-{flags_2hex}" (55 chars)
inline std::string SerializeTraceParent(
    const opentelemetry::trace::SpanContext& ctx) {
  if (!ctx.IsValid()) return {};
  std::string result(55, '\0');
  result[0] = '0';
  result[1] = '0';
  result[2] = '-';
  ctx.trace_id().ToLowerBase16(
      opentelemetry::nostd::span<char, 32>(&result[3], 32));
  result[35] = '-';
  ctx.span_id().ToLowerBase16(
      opentelemetry::nostd::span<char, 16>(&result[36], 16));
  result[52] = '-';
  ctx.trace_flags().ToLowerBase16(
      opentelemetry::nostd::span<char, 2>(&result[53], 2));
  return result;
}

/// Deserialize a W3C traceparent string to SpanContext.
/// Returns SpanContext::GetInvalid() on any parse failure (never throws).
inline opentelemetry::trace::SpanContext DeserializeTraceParent(
    std::string_view tp) {
  namespace prop = opentelemetry::trace::propagation;
  if (tp.size() != 55 || tp[0] != '0' || tp[1] != '0')
    return opentelemetry::trace::SpanContext::GetInvalid();
  opentelemetry::trace::TraceId trace_id =
      prop::HttpTraceContext::TraceIdFromHex(tp.substr(3, 32));
  opentelemetry::trace::SpanId span_id =
      prop::HttpTraceContext::SpanIdFromHex(tp.substr(36, 16));
  opentelemetry::trace::TraceFlags flags =
      prop::HttpTraceContext::TraceFlagsFromHex(tp.substr(53, 2));
  if (!trace_id.IsValid() || !span_id.IsValid())
    return opentelemetry::trace::SpanContext::GetInvalid();
  return opentelemetry::trace::SpanContext(trace_id, span_id, flags, true);
}

// ============================================================================
// Macros (CRANE_ENABLE_TRACING is defined)
// ============================================================================

/// Create an RAII span for the current scope.
/// Only ONE per C++ scope (uses fixed variable name `_crane_scope_span_`).
/// Pair with CRANE_TRACE_SET_ATTR / CRANE_TRACE_EVENT.
#  define CRANE_TRACE_SCOPE(name)                                    \
    ::crane::ScopedSpan _crane_scope_span_(                          \
        name, ::crane::TracerManager::GetInstance().GetTracerSafe())

/// Create a named RAII span for explicit reference.
/// Use when you need .SetAttribute(), .CreateChild(), or .End() on it.
#  define CRANE_TRACE_SCOPE_NAMED(var, name)                         \
    ::crane::ScopedSpan var(                                         \
        name, ::crane::TracerManager::GetInstance().GetTracerSafe())

/// Create an instant span (starts and ends immediately).
/// Use for event markers with no duration.
#  define CRANE_TRACE_POINT(name)                                       \
    do {                                                                \
      ::crane::ScopedSpan _crane_tp_(                                   \
          name, ::crane::TracerManager::GetInstance().GetTracerSafe()); \
    } while (0)

/// Create an instant span with one attribute.
#  define CRANE_TRACE_POINT_ATTR(name, key, value)                      \
    do {                                                                \
      ::crane::ScopedSpan _crane_tp_(                                   \
          name, ::crane::TracerManager::GetInstance().GetTracerSafe()); \
      _crane_tp_.SetAttribute(key, value);                              \
    } while (0)

/// Create a child span under an existing parent ScopedSpan.
/// Supports arbitrary nesting depth.
#  define CRANE_TRACE_CHILD_NAMED(var, parent, name) \
    auto var = (parent).CreateChild(name)

/// Set an attribute on the CRANE_TRACE_SCOPE span in the current scope.
#  define CRANE_TRACE_SET_ATTR(key, value)      \
    _crane_scope_span_.SetAttribute(key, value)

/// Add an event to the CRANE_TRACE_SCOPE span in the current scope.
#  define CRANE_TRACE_EVENT(event_name) _crane_scope_span_.AddEvent(event_name)

/// Set the status on the CRANE_TRACE_SCOPE span in the current scope.
#  define CRANE_TRACE_SET_STATUS(code, desc) \
    _crane_scope_span_.SetStatus(code, desc)

/// Create a child span from a serialized W3C traceparent string.
/// Falls back to a root span if the traceparent is empty or invalid.
#  define CRANE_TRACE_SCOPE_FROM_REMOTE(var, name, traceparent_str)      \
    ::crane::ScopedSpan var = [&]() -> ::crane::ScopedSpan {             \
      auto _tp_ctx_ = ::crane::DeserializeTraceParent(traceparent_str);  \
      if (_tp_ctx_.IsValid())                                            \
        return ::crane::ScopedSpan(                                      \
            name, ::crane::TracerManager::GetInstance().GetTracerSafe(), \
            _tp_ctx_);                                                   \
      return ::crane::ScopedSpan(                                        \
          name, ::crane::TracerManager::GetInstance().GetTracerSafe());  \
    }()

/// Create a ManualSpan (non-RAII, caller must call End()).
#  define CRANE_TRACE_MANUAL(var, name)                              \
    ::crane::ManualSpan var(                                         \
        name, ::crane::TracerManager::GetInstance().GetTracerSafe())

/// Create a ManualSpan as child of a remote traceparent.
#  define CRANE_TRACE_MANUAL_FROM_REMOTE(var, name, traceparent_str)     \
    ::crane::ManualSpan var = [&]() -> ::crane::ManualSpan {             \
      auto _tp_ = ::crane::DeserializeTraceParent(traceparent_str);      \
      if (_tp_.IsValid())                                                \
        return ::crane::ManualSpan(                                      \
            name, ::crane::TracerManager::GetInstance().GetTracerSafe(), \
            _tp_);                                                       \
      return ::crane::ManualSpan(                                        \
          name, ::crane::TracerManager::GetInstance().GetTracerSafe());  \
    }()

#else  // CRANE_ENABLE_TRACING not defined

// ============================================================================
// No-op stub (zero overhead when tracing is compiled out)
// ============================================================================

/// Stub StatusCode so call sites compile without #ifdef guards.
enum class StatusCode { kUnset = 0, kOk = 1, kError = 2 };

class ScopedSpan {
 public:
  ScopedSpan() = default;
  void End() {}
  template <typename T>
  void SetAttribute(std::string_view, const T&) {}
  [[nodiscard]] ScopedSpan CreateChild(std::string_view) const { return {}; }
  [[nodiscard]] bool IsActive() const { return false; }
  void AddEvent(std::string_view) {}
  void SetStatus(StatusCode, std::string_view = {}) {}
};

#  define CRANE_TRACE_SCOPE(name) (void)0
#  define CRANE_TRACE_SCOPE_NAMED(var, name) ::crane::ScopedSpan var
#  define CRANE_TRACE_POINT(name) (void)0
#  define CRANE_TRACE_POINT_ATTR(name, key, value) (void)0
#  define CRANE_TRACE_CHILD_NAMED(var, parent, name) ::crane::ScopedSpan var
#  define CRANE_TRACE_SET_ATTR(key, value) (void)0
#  define CRANE_TRACE_EVENT(event_name) (void)0
#  define CRANE_TRACE_SET_STATUS(code, desc) (void)0
#  define CRANE_TRACE_SCOPE_FROM_REMOTE(var, name, tp) ::crane::ScopedSpan var

class ManualSpan {
 public:
  ManualSpan() = default;
  void End() {}
  template <typename T>
  void SetAttribute(std::string_view, const T&) {}
  void AddEvent(std::string_view) {}
  void SetStatus(StatusCode, std::string_view = {}) {}
  [[nodiscard]] bool IsActive() const { return false; }
};

#  define CRANE_TRACE_MANUAL(var, name) ::crane::ManualSpan var
#  define CRANE_TRACE_MANUAL_FROM_REMOTE(var, name, tp) ::crane::ManualSpan var

#endif  // CRANE_ENABLE_TRACING

}  // namespace crane
