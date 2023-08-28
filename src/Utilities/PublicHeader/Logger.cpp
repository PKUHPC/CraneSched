#include "crane/Logger.h"

void InitLogger(spdlog::level::level_enum level,
                const std::string& log_file_path) {
  spdlog::set_pattern("[%^%L%$ %C-%m-%d %s:%#] %v");

  auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      log_file_path, 1048576 * 50 /*MB*/, 3);
  file_sink->set_level(level);

  auto console_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
  console_sink->set_level(level);

  spdlog::set_level(level);

  spdlog::init_thread_pool(256, 1);
  auto logger = std::make_shared<spdlog::async_logger>(
      "default", spdlog::sinks_init_list{file_sink, console_sink},
      spdlog::thread_pool(), spdlog::async_overflow_policy::block);

  spdlog::set_default_logger(logger);

  spdlog::flush_on(spdlog::level::err);
  spdlog::flush_every(std::chrono::seconds(1));
}