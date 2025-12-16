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

#ifdef CRANE_ENABLE_TRACING

#  include "crane/TracerManager.h"

#  define CRANE_TRACE_BEGIN(span_name)                               \
    auto __crane_span =                                              \
        crane::TracerManager::GetInstance().CreateSpan(span_name);   \
    ::crane::_internal::ScopedSpan __crane_scoped_span(__crane_span)

#  define CRANE_TRACE_SET_ATTRIBUTE(key, value)                       \
    do {                                                              \
      if (::crane::_internal::g_current_span) {                       \
        ::crane::_internal::g_current_span->SetAttribute(key, value); \
      }                                                               \
    } while (0)

#  define CRANE_TRACE_ADD_EVENT(event_name)                       \
    do {                                                          \
      if (::crane::_internal::g_current_span) {                   \
        ::crane::_internal::g_current_span->AddEvent(event_name); \
      }                                                           \
    } while (0)

#  define CRANE_TRACE_END(status)                                \
    do {                                                         \
      if (::crane::_internal::g_current_span) {                  \
        if (status == "OK") {                                    \
          ::crane::_internal::g_current_span->SetStatus(         \
              opentelemetry::trace::StatusCode::kOk);            \
        } else {                                                 \
          ::crane::_internal::g_current_span->SetStatus(         \
              opentelemetry::trace::StatusCode::kError, status); \
        }                                                        \
        ::crane::_internal::g_current_span->End();               \
      }                                                          \
    } while (0)

#else

#  define CRANE_TRACE_BEGIN(span_name)
#  define CRANE_TRACE_SET_ATTRIBUTE(key, value)
#  define CRANE_TRACE_ADD_EVENT(event_name)
#  define CRANE_TRACE_END(status)

#endif  // CRANE_ENABLE_TRACING

namespace crane::_internal {

#ifdef CRANE_ENABLE_TRACING

extern thread_local opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>
    g_current_span;

class ScopedSpan {
 public:
  explicit ScopedSpan(
      opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span)
      : span_(span), prev_span_(g_current_span) {
    g_current_span = span_;
  }

  ~ScopedSpan() {
    if (span_) {
      span_->End();
    }
    g_current_span = prev_span_;
  }

  ScopedSpan(const ScopedSpan&) = delete;
  ScopedSpan& operator=(const ScopedSpan&) = delete;

 private:
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> prev_span_;
};

#endif  // CRANE_ENABLE_TRACING

}  // namespace crane::_internal
