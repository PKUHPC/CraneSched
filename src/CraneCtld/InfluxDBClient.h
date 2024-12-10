#pragma once

#include <string>

#include "CtldPublicDefs.h"

namespace Ctld{
struct NodeStats {
  double current_power{0.0};    
  double avg_power{0.0};        
  double total_energy{0.0};     
  double cpu_util{0.0};         
  double mem_util{0.0};         
  double gpu_util{0.0};         
  absl::Duration time_span;     
};

class InfluxDBClient {
 public:
  InfluxDBClient()
      : m_url_(g_config.InfluxDbUrl), m_token_(g_config.InfluxDbToken), m_org_(g_config.InfluxDbOrg) {}

  std::string Query(const std::string& flux_query) const;

  bool QueryNodeEnergyInfo(const std::string& node_id, 
                          int64_t window,
                          NodeStats* stats) const;

  double QueryTaskEnergyEfficiency(double requested_cpu, 
                                 double requested_mem_gb,
                                 absl::Duration time_window = absl::Hours(24)) const;

 private:
  std::string m_url_;
  std::string m_token_;
  std::string m_org_;
}; 
}