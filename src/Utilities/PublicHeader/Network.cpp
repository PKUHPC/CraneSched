/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "crane/Network.h"

#include "crane/Logger.h"

namespace crane {

namespace internal {

class HostsMap {
 public:
  HostsMap() {
    if (!ParseHosts_())
      CRANE_ERROR("Failed to parse file /etc/hosts, DNS service will be used");
  }

  bool FindIpv4OfHostname(std::string const& hostname,
                          std::string* ipv4) const {
    auto it = m_hostname_to_ipv4_map_.find(hostname);
    if (it == m_hostname_to_ipv4_map_.end()) return false;

    ipv4->assign(it->second);
    return true;
  }

  bool FindFirstHostnameOfIpv4(std::string const& ipv4,
                               std::string* hostname) const {
    auto it = m_ipv4_to_hostnames_map_.find(ipv4);
    if (it == m_ipv4_to_hostnames_map_.end()) return false;

    const auto& hostnames = it->second;
    hostname->assign(*hostnames.begin());
    return true;
  }

 private:
  bool ParseHosts_() {
    // read hosts file
    std::ifstream hosts_ifstream(kHostFilePath);
    std::string line;
    if (!hosts_ifstream.is_open()) {
      CRANE_ERROR("Failed to parse hosts from file {}", kHostFilePath);
      return false;
    }

    while (getline(hosts_ifstream, line)) {
      if (line.empty() || line[0] == '#') {
        continue;
      }

      // Split the line into IP and hostnames using space as delimiter
      // Absl:: SkipWhitespace() treats consecutive delimiters as one and
      // ignores the beginning and end delimiters
      std::vector<absl::string_view> parts =
          absl::StrSplit(line, absl::ByAnyChar(" \t"), absl::SkipWhitespace());

      if (!parts.empty()) {
        std::string ip = std::string(parts[0]);

        for (size_t i = 1; i < parts.size(); ++i) {
          if (parts[i][0] == '#') {
            break;
          }
          m_hostname_to_ipv4_map_[std::string(parts[i])] = ip;
          m_ipv4_to_hostnames_map_[ip].emplace(parts[i]);
        }
      }
    }

    hosts_ifstream.close();

    return true;
  }

  absl::flat_hash_map<std::string /*hostname*/, std::string /*ipv4*/>
      m_hostname_to_ipv4_map_;

  absl::flat_hash_map<std::string /*ipv4*/,
                      absl::flat_hash_set<std::string> /*hostnames*/>
      m_ipv4_to_hostnames_map_;
};

HostsMap g_hosts_map;

}  // namespace internal

bool ResolveHostnameFromIpv4(const std::string& addr, std::string* hostname) {
  if (internal::g_hosts_map.FindFirstHostnameOfIpv4(addr, hostname))
    return true;

  struct sockaddr_in sa {}; /* input */
  socklen_t len;            /* input */
  char hbuf[NI_MAXHOST];

  std::memset(&sa, 0, sizeof(struct sockaddr_in));

  /* For IPv4*/
  sa.sin_family = AF_INET;
  sa.sin_addr.s_addr = inet_addr(addr.c_str());
  len = sizeof(struct sockaddr_in);

  int r;
  if ((r = getnameinfo((struct sockaddr*)&sa, len, hbuf, sizeof(hbuf), nullptr,
                       0, NI_NAMEREQD))) {
    CRANE_TRACE("Error in getnameinfo when resolving hostname for {}: {}",
                addr.c_str(), gai_strerror(r));
    return false;
  } else {
    hostname->assign(hbuf);
    return true;
  }
}

bool ResolveHostnameFromIpv6(const std::string& addr, std::string* hostname) {
  struct sockaddr_in6 sa6 {}; /* input */
  socklen_t len;              /* input */
  char hbuf[NI_MAXHOST];

  std::memset(&sa6, 0, sizeof(struct sockaddr_in6));

  /* For IPv4*/
  sa6.sin6_family = AF_INET6;
  in6_addr addr6{};
  inet_pton(AF_INET6, addr.c_str(), &addr6);
  sa6.sin6_addr = addr6;

  len = sizeof(struct sockaddr_in6);

  int r;
  if ((r = getnameinfo((struct sockaddr*)&sa6, len, hbuf, sizeof(hbuf), nullptr,
                       0, NI_NAMEREQD))) {
    CRANE_TRACE("Error in getnameinfo when resolving hostname for {}: {}",
                addr.c_str(), gai_strerror(r));
    return false;
  } else {
    hostname->assign(hbuf);
    return true;
  }
}

bool ResolveIpv4FromHostname(const std::string& hostname, std::string* addr) {
  if (internal::g_hosts_map.FindIpv4OfHostname(hostname, addr)) return true;

  struct addrinfo hints {};
  struct addrinfo *res, *tmp;
  char host[256];

  hints.ai_family = AF_INET;

  int ret = getaddrinfo(hostname.c_str(), nullptr, &hints, &res);
  if (ret != 0) {
    CRANE_WARN("Error in getaddrinfo when resolving hostname {}: {}", hostname,
               gai_strerror(ret));
    return false;
  }

  for (tmp = res; tmp != nullptr; tmp = tmp->ai_next) {
    getnameinfo(tmp->ai_addr, tmp->ai_addrlen, host, sizeof(host), nullptr, 0,
                NI_NUMERICHOST);
    addr->assign(host);
  }

  freeaddrinfo(res);
  return true;
}

bool IsAValidIpv4Address(const std::string& ipv4) {
  static const LazyRE2 kIpv4Re = {
      R"(^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}$)"};
  return RE2::FullMatch(ipv4, *kIpv4Re);
}

}  // namespace crane