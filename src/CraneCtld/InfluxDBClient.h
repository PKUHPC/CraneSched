#pragma once

#include <string>
#include <vector>

class InfluxDBClient {
 public:
  InfluxDBClient(const std::string& url, const std::string& token, const std::string& org)
      : m_url(url), m_token(token), m_org(org) {}

  std::string Query(const std::string& flux_query) const;
  bool Write(const std::string& bucket, const std::string& org, 
             const std::string& measurement, const std::string& field, 
             double value) const;

 private:
  std::string m_url;
  std::string m_token;
  std::string m_org;
}; 