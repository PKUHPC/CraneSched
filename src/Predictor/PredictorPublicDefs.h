//
// Created by root on 3/12/24.
//

#ifndef CRANE_PREDICTORPUBLICDEFS_H
#define CRANE_PREDICTORPUBLICDEFS_H

#include "PredictorPreCompiledHeader.h"

namespace Predictor {

inline const char* kPredDefaultLogPath = "/tmp/cranectld/predictor.log";

struct Config {
  std::string PredConfigPath;

  std::string PredDebugLevel;
  std::string PredLogFile;

  std::string PredListenAddr;
  std::string PredListenPort;
  std::string PredModelPath;
};

}  // namespace Predictor

inline Predictor::Config g_config;

#endif  // CRANE_PREDICTORPUBLICDEFS_H
