//
// Created by root on 3/12/24.
//

#pragma once

#include "PredictorPublicDefs.h"
#include "TimeEstimator.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Predictor {

using crane::grpc::CranePred;
using grpc::Channel;
using grpc::Server;

class PredServer;

class CranePredServiceImpl final : public crane::grpc::CranePred::Service {
 public:
  explicit CranePredServiceImpl(PredServer* server)
      : m_predictor_server_(server) {}
  grpc::Status TaskEstimation(grpc::ServerContext* context,
                              const crane::grpc::TaskEstimationRequest* request,
                              crane::grpc::TaskEstimationReply* reply) override;

  grpc::Status ReportExecutionTime(
      grpc::ServerContext* context,
      const crane::grpc::TaskExecutionTimeAck* request,
      ::google::protobuf::Empty* reply) override;

 private:
  PredServer* m_predictor_server_;
};

class PredServer {
 public:
  explicit PredServer();

  inline void Wait() { m_server_->Wait(); }

  void EstimateRunTime(const crane::grpc::TaskEstimationRequest* request,
                       crane::grpc::TaskEstimationReply* reply);

  void RecordRunTime(const crane::grpc::TaskExecutionTimeAck* request);

 private:
  std::unique_ptr<ITimeEstimator> m_time_estimator_;

  std::unique_ptr<CranePredServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;
  friend class CranePredServiceImpl;
};

}  // namespace Predictor

inline std::unique_ptr<Predictor::PredServer> g_pred_server;
