//
// Created by root on 3/9/24.
//

#include <yaml-cpp/yaml.h>

#include "PredGrpcServer.h"
#include "PredictorPublicDefs.h"

void ParseConfig(int argc, char** argv) {
  // TODO: Modify the default config path
  std::string global_config_path = kDefaultConfigPath;
  std::string pred_config_path = kDefaultPredConfigPath;
  if (std::filesystem::exists(global_config_path)) {
    try {
      // Read global configuration to get config path for predictor
      YAML::Node global_config = YAML::LoadFile(global_config_path);
      if (global_config["PredConfigPath"])
        g_config.PredConfigPath =
            global_config["PredConfigPath"].as<std::string>();

      // Read predictor configuration
      YAML::Node pred_config = YAML::LoadFile(g_config.PredConfigPath);

      // Load debug level
      if (pred_config["PredDebugLevel"])
        g_config.PredDebugLevel =
            pred_config["PredDebugLevel"].as<std::string>();
      else
        g_config.PredDebugLevel = "info";

      // Load log file
      if (pred_config["PredLogFile"])
        g_config.PredLogFile = pred_config["PredLogFile"].as<std::string>();
      else
        g_config.PredLogFile = Predictor::kPredDefaultLogPath;

      spdlog::level::level_enum log_level;
      if (g_config.PredDebugLevel == "trace") {
        log_level = spdlog::level::trace;
      } else if (g_config.PredDebugLevel == "debug") {
        log_level = spdlog::level::debug;
      } else if (g_config.PredDebugLevel == "info") {
        log_level = spdlog::level::info;
      } else if (g_config.PredDebugLevel == "warn") {
        log_level = spdlog::level::warn;
      } else if (g_config.PredDebugLevel == "error") {
        log_level = spdlog::level::err;
      } else {
        fmt::print(stderr, "Illegal debug-level format.");
        std::exit(1);
      }

      InitLogger(log_level, g_config.PredLogFile);

      // Load gRPC listen address and port
      if (pred_config["PredListenAddr"])
        g_config.PredListenAddr =
            pred_config["PredListenAddr"].as<std::string>();
      else
        g_config.PredListenAddr = "0.0.0.0";

      if (pred_config["PredListenPort"])
        g_config.PredListenPort =
            pred_config["PredListenPort"].as<std::string>();
      else
        g_config.PredListenPort = "50051";

      // Load model path
      if (pred_config["PredModelPath"])
        g_config.PredModelPath = pred_config["PredModelPath"].as<std::string>();
      else {
        CRANE_CRITICAL("Model path is not specified in the config file.");
        std::exit(1);
      }
    } catch (YAML::BadFile& e) {
      CRANE_CRITICAL("Error when reading config file {}: {}",
                     global_config_path, e.what());
      std::exit(1);
    }
  } else {
    CRANE_CRITICAL("Global config file {} doesn't exist.", global_config_path);
    std::exit(1);
  }
}

void InitializePredGlobalVariables() {
  g_pred_server = std::make_unique<Predictor::PredServer>();
}

void DestroyPredGlobalVariables() { g_pred_server.reset(); }

void StartServer() {
  InitializePredGlobalVariables();

  g_pred_server->Wait();

  DestroyPredGlobalVariables();
}

int main(int argc, char** argv) {
  ParseConfig(argc, argv);
  StartServer();

  return 0;
}
