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

  bool FindIpv4OfHostname(std::string const& hostname, ipv4_t* ipv4) const {
    auto it = m_hostname_to_ipv4_map_.find(hostname);
    if (it == m_hostname_to_ipv4_map_.end()) return false;

    *ipv4 = it->second;
    return true;
  }

  bool FindIpv6OfHostname(std::string const& hostname, ipv6_t* ipv6) const {
    auto it = m_hostname_to_ipv6_map_.find(hostname);
    if (it == m_hostname_to_ipv6_map_.end()) return false;

    *ipv6 = it->second;
    return true;
  }

  bool FindFirstHostnameOfIpv4(ipv4_t ipv4, std::string* hostname) const {
    auto it = m_ipv4_to_hostnames_map_.find(ipv4);
    if (it == m_ipv4_to_hostnames_map_.end()) return false;

    const auto& hostnames = it->second;
    hostname->assign(*hostnames.begin());
    return true;
  }

  bool FindFirstHostnameOfIpv6(const ipv6_t& ipv6,
                               std::string* hostname) const {
    auto it = m_ipv6_to_hostnames_map_.find(ipv6);
    if (it == m_ipv6_to_hostnames_map_.end()) return false;

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
          switch (crane::GetIpAddrVer(ip)) {
          case -1:
            CRANE_ERROR("host file line \'{}\' parse failed", line);
            break;
          case 4:
            ipv4_t ipv4;
            crane::StrToIpv4(ip, &ipv4);
            m_hostname_to_ipv4_map_[std::string(parts[i])] = ipv4;
            m_ipv4_to_hostnames_map_[ipv4].emplace(parts[i]);
            break;
          case 6:
            ipv6_t ipv6;
            crane::StrToIpv6(ip, &ipv6);
            m_hostname_to_ipv6_map_[std::string(parts[i])] = ipv6;
            m_ipv6_to_hostnames_map_[ipv6].emplace(parts[i]);
            break;
          }
        }
      }
    }

    hosts_ifstream.close();

    return true;
  }

  absl::flat_hash_map<std::string /*hostname*/, ipv6_t> m_hostname_to_ipv6_map_;

  absl::flat_hash_map<ipv6_t, absl::flat_hash_set<std::string> /*hostnames*/>
      m_ipv6_to_hostnames_map_;

  absl::flat_hash_map<std::string /*hostname*/, ipv4_t> m_hostname_to_ipv4_map_;

  absl::flat_hash_map<ipv4_t, absl::flat_hash_set<std::string> /*hostnames*/>
      m_ipv4_to_hostnames_map_;
};

std::unique_ptr<HostsMap> g_hosts_map;

}  // namespace internal

void InitializeNetworkFunctions() {
  internal::g_hosts_map = std::make_unique<internal::HostsMap>();
}

bool ResolveHostnameFromIpv4(ipv4_t addr, std::string* hostname) {
  if (internal::g_hosts_map->FindFirstHostnameOfIpv4(addr, hostname))
    return true;

  struct sockaddr_in sa {};
  char hbuf[NI_MAXHOST];

  std::string ipv4_str = Ipv4ToStr(addr);
  /* For IPv4*/
  sa.sin_family = AF_INET;
  sa.sin_addr.s_addr = inet_addr(ipv4_str.c_str());

  socklen_t len = sizeof(struct sockaddr_in);

  int r = getnameinfo((struct sockaddr*)&sa, len, hbuf, sizeof(hbuf), nullptr,
                      0, NI_NAMEREQD);
  if (r != 0) {
    CRANE_TRACE("Error in getnameinfo when resolving hostname for {}: {}",
                ipv4_str.c_str(), gai_strerror(r));
    return false;
  }

  hostname->assign(hbuf);
  return true;
}

bool ResolveHostnameFromIpv6(const ipv6_t& addr, std::string* hostname) {
  if (internal::g_hosts_map->FindFirstHostnameOfIpv6(addr, hostname))
    return true;

  struct sockaddr_in6 sa6 {};
  char hbuf[NI_MAXHOST];

  /* For IPv6*/
  sa6.sin6_family = AF_INET6;
  in6_addr addr6{};
  std::string ipv6_str = Ipv6ToStr(addr);
  inet_pton(AF_INET6, ipv6_str.c_str(), &addr6);
  sa6.sin6_addr = addr6;

  socklen_t len = sizeof(struct sockaddr_in6);

  int r = getnameinfo((struct sockaddr*)&sa6, len, hbuf, sizeof(hbuf), nullptr,
                      0, NI_NAMEREQD);
  if (r != 0) {
    CRANE_TRACE("Error in getnameinfo when resolving hostname for {}: {}",
                ipv6_str.c_str(), gai_strerror(r));
    return false;
  }

  hostname->assign(hbuf);
  return true;
}

bool ResolveIpv4FromHostname(const std::string& hostname, ipv4_t* addr) {
  if (internal::g_hosts_map->FindIpv4OfHostname(hostname, addr)) return true;

  struct addrinfo hints {};
  struct addrinfo* res;
  char host[NI_MAXHOST];

  hints.ai_family = AF_INET;

  int ret = getaddrinfo(hostname.c_str(), nullptr, &hints, &res);
  if (ret != 0) {
    CRANE_WARN("Error in getaddrinfo when resolving ipv4 from hostname {}: {}",
               hostname, gai_strerror(ret));
    return false;
  }

  for (struct addrinfo* tmp = res; tmp != nullptr; tmp = tmp->ai_next) {
    ret = getnameinfo(tmp->ai_addr, tmp->ai_addrlen, host, sizeof(host),
                      nullptr, 0, NI_NUMERICHOST);
    auto ipv4_sockaddr = (struct sockaddr_in*)tmp->ai_addr;
    if (ret == 0)
      *addr =
          ntohl(static_cast<struct in_addr>(ipv4_sockaddr->sin_addr).s_addr);
    else
      CRANE_ERROR("error getnameinfo {}", gai_strerror(ret));
  }

  freeaddrinfo(res);
  return true;
}

bool ResolveIpv6FromHostname(const std::string& hostname, ipv6_t* addr) {
  if (internal::g_hosts_map->FindIpv6OfHostname(hostname, addr)) return true;

  struct addrinfo hints {};
  struct addrinfo *res, *tmp;
  char host[NI_MAXHOST];

  hints.ai_family = AF_INET6;

  int ret = getaddrinfo(hostname.c_str(), nullptr, &hints, &res);
  if (ret != 0) {
    CRANE_WARN("Error in getaddrinfo when resolving ipv6 from hostname {}: {}",
               hostname, gai_strerror(ret));
    return false;
  }

  for (tmp = res; tmp != nullptr; tmp = tmp->ai_next) {
    ret = getnameinfo(tmp->ai_addr, tmp->ai_addrlen, host, sizeof(host),
                      nullptr, 0, NI_NUMERICHOST);
    auto ipv6_sockaddr = (struct sockaddr_in6*)tmp->ai_addr;
    if (ret == 0) {
      for (int i = 0; i < 4; ++i) {
        *addr = 0;
        uint32_t part = ntohl(ipv6_sockaddr->sin6_addr.s6_addr32[i]);
        *addr = (*addr << 32) | part;
      }
    } else
      CRANE_ERROR("error getnameinfo {}", gai_strerror(ret));
  }

  freeaddrinfo(res);
  return true;
}

bool StrToIpv4(const std::string& ip, ipv4_t* addr) {
  struct in_addr ipv4_addr;
  if (inet_pton(AF_INET, ip.c_str(), &ipv4_addr) != 1) {
    CRANE_ERROR("Error converting ipv4 {} to uint32_t", ip);
    return false;
  }

  *addr = ntohl(ipv4_addr.s_addr);

  return true;
}

bool StrToIpv6(const std::string& ip, ipv6_t* addr) {
  struct in6_addr ipv6_addr;
  if (inet_pton(AF_INET6, ip.c_str(), &ipv6_addr) != 1) {
    CRANE_ERROR("Error converting ipv6 {} to uint128", ip);
    return false;
  }

  *addr = 0;
  for (int i = 0; i < 4; ++i) {
    uint32_t part = ntohl(ipv6_addr.s6_addr32[i]);
    *addr = (*addr << 32) | part;
  }

  return true;
}

std::string Ipv4ToStr(ipv4_t addr) {
  return fmt::format("{}.{}.{}.{}", (addr >> 24) & 0xff, (addr >> 16) & 0xff,
                     (addr >> 8) & 0xff, addr & 0xff);
}

std::string Ipv6ToStr(const ipv6_t& addr) {
  uint64_t high = absl::Uint128High64(addr);
  uint64_t low = absl::Uint128Low64(addr);
  std::vector<std::string> hex_vec;
  for (int i = 0; i < 4; ++i)
    hex_vec.push_back(fmt::format("{:x}", (high >> (48 - i * 16)) & 0xffff));

  for (int i = 0; i < 4; ++i)
    hex_vec.push_back(fmt::format("{:x}", (low >> (48 - i * 16)) & 0xffff));

  return absl::StrJoin(hex_vec, ":");
}

int GetIpAddrVer(const std::string& ip) {
  char buf[INET6_ADDRSTRLEN];
  if (inet_pton(AF_INET, ip.c_str(), buf)) return 4;
  if (inet_pton(AF_INET6, ip.c_str(), buf)) return 6;

  return -1;
}

bool FindTcpInodeByPort(const std::string& tcp_path, int port, ino_t* inode) {
  std::ifstream tcp_in(tcp_path, std::ios::in);
  std::string tcp_line;
  std::string port_hex = fmt::format("{:0>4X}", port);
  if (tcp_in) {
    getline(tcp_in, tcp_line);  // Skip the header line
    while (getline(tcp_in, tcp_line)) {
      tcp_line = absl::StripAsciiWhitespace(tcp_line);
      std::vector<std::string> tcp_line_vec =
          absl::StrSplit(tcp_line, absl::ByAnyChar(" :"), absl::SkipEmpty());
      CRANE_TRACE("Checking port {} == {}", port_hex, tcp_line_vec[2]);
      if (port_hex == tcp_line_vec[2]) {
        *inode = std::stoul(tcp_line_vec[13]);
        CRANE_TRACE("Inode num for port {} is {}", port, *inode);
        return true;
      }
    }
  } else {  // can't find file
    CRANE_ERROR("Can't open file: {}", tcp_path);
  }
  return false;
}

}  // namespace crane