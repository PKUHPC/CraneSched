//
// Created by root on 3/9/24.
//

#include "PredGrpcServer.h"

void InitializePredGlobalVariables() {
  g_pred_server = std::make_unique<Predictor::PredServer>();
}

void DestroyPredGlobalVariables() {
  g_pred_server.reset();
}

void StartServer() {
  InitializePredGlobalVariables();

  g_pred_server->Wait();

  DestroyPredGlobalVariables();
}

int main(int argc, char** argv) {
  StartServer();

  return 0;
}
