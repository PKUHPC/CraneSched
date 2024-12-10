#pragma once

#include <string>

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
  InfluxDBClient(const std::string& url, const std::string& token, const std::string& org)
      : m_url_(url), m_token_(token), m_org_(org) {}

  std::string Query(const std::string& flux_query) const;
  bool Write(const std::string& bucket, const std::string& org, 
             const std::string& measurement, const std::string& field, 
             double value) const;

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