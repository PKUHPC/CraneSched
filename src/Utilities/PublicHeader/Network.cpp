/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include "crane/Network.h"

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <linux/if_packet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

#include <ranges>

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

  struct sockaddr_in sa{};
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

  struct sockaddr_in6 sa6{};
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

  struct addrinfo hints{};
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

  struct addrinfo hints{};
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
      if (IN6_IS_ADDR_LINKLOCAL(&ipv6_sockaddr->sin6_addr)) {
        // Check if it is a link-local address, which must have a scope ID (e.g.
        // %eth0). We don't support link-local address as it's too complex to
        // handle.
        CRANE_TRACE("Skipping link-local address of {}", hostname);
        continue;
      }

      *addr = 0;
      for (int i = 0; i < 4; ++i) {
        uint32_t part = ntohl(ipv6_sockaddr->sin6_addr.s6_addr32[i]);
        *addr = (*addr << 32) | part;
      }
    } else {
      CRANE_ERROR("Error in getnameinfo {}", gai_strerror(ret));
    }
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

std::vector<NetworkInterface> GetNetworkInterfaces() {
  std::unordered_map<std::string, NetworkInterface> interface_map;
  constexpr int kMaxRetries = 10;
  constexpr int kRetryIntervalMs = 3000;  // Retry every 3 seconds

  for (int retry = 0; retry < kMaxRetries; ++retry) {
    interface_map.clear();
    bool has_valid_ip = false;

    struct ifaddrs *ifaddr, *ifa;
    if (getifaddrs(&ifaddr) == -1) {
      CRANE_ERROR("getifaddrs failed: {}", strerror(errno));
      if (retry < kMaxRetries - 1) {
        CRANE_WARN("Retrying to get network interfaces ({}/{})", retry + 1,
                   kMaxRetries);
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kRetryIntervalMs));
        continue;
      }
      return {};
    }

    for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
      if (ifa->ifa_addr == nullptr) continue;

      std::string if_name(ifa->ifa_name);

      if (!interface_map.contains(if_name)) {
        interface_map[if_name].name = if_name;
      }

      int family = ifa->ifa_addr->sa_family;

      switch (family) {
      case AF_INET: {  // IPv4
        struct sockaddr_in* addr = (struct sockaddr_in*)ifa->ifa_addr;
        ipv4_t ipv4_addr = ntohl(addr->sin_addr.s_addr);
        // Ignore loopback address 127.0.0.1
        if ((ipv4_addr & 0xFF000000) != 0x7F000000) {
          has_valid_ip = true;
        }
        interface_map[if_name].ipv4_addresses.emplace_back(ipv4_addr);
        break;
      }
      case AF_INET6: {  // IPv6
        struct sockaddr_in6* addr = (struct sockaddr_in6*)ifa->ifa_addr;
        ipv6_t ipv6_addr = 0;
        for (int i = 0; i < 4; ++i) {
          uint32_t part = ntohl(addr->sin6_addr.s6_addr32[i]);
          ipv6_addr = (ipv6_addr << 32) | part;
        }
        if (!IN6_IS_ADDR_LOOPBACK(&addr->sin6_addr) &&
            !IN6_IS_ADDR_LINKLOCAL(&addr->sin6_addr)) {
          has_valid_ip = true;
        }
        interface_map[if_name].ipv6_addresses.emplace_back(ipv6_addr);
        break;
      }
      case AF_PACKET: {  // MAC
        struct sockaddr_ll* s = (struct sockaddr_ll*)ifa->ifa_addr;
        if (s->sll_halen >= 6) {
          interface_map[if_name].mac_address =
              fmt::format("{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                          s->sll_addr[0], s->sll_addr[1], s->sll_addr[2],
                          s->sll_addr[3], s->sll_addr[4], s->sll_addr[5]);
        }
        break;
      }
      }
    }

    freeifaddrs(ifaddr);

    // If we found at least one valid non-loopback IP address, consider network
    // ready
    if (has_valid_ip) {
      CRANE_INFO(
          "Successfully retrieved network interfaces, found {} interfaces",
          interface_map.size());
      break;
    }

    if (retry < kMaxRetries - 1) {
      CRANE_WARN(
          "No valid network interface IP found, waiting for network service to "
          "start ({}/{})",
          retry + 1, kMaxRetries);
      std::this_thread::sleep_for(std::chrono::milliseconds(kRetryIntervalMs));
    } else {
      CRANE_ERROR(
          "Failed to find valid network interface IPs after multiple attempts");
    }
  }

  return interface_map | std::views::values | std::views::common |
         std::ranges::to<std::vector>();
}

NetworkInterface::NetworkInterface(
    const crane::grpc::NetworkInterface& grpc_interface) {
  name = grpc_interface.name();
  mac_address = grpc_interface.mac_address();

  for (const auto& addr : grpc_interface.ipv4_addresses()) {
    ipv4_t ipv4;
    if (StrToIpv4(addr, &ipv4)) {
      ipv4_addresses.push_back(ipv4);
    }
  }

  for (const auto& addr : grpc_interface.ipv6_addresses()) {
    ipv6_t ipv6;
    if (StrToIpv6(addr, &ipv6)) {
      ipv6_addresses.push_back(ipv6);
    }
  }
}

NetworkInterface::operator crane::grpc::NetworkInterface() const {
  crane::grpc::NetworkInterface interface;
  interface.set_name(name);
  interface.set_mac_address(mac_address);

  for (const auto& addr : ipv4_addresses) {
    interface.add_ipv4_addresses(Ipv4ToStr(addr));
  }

  for (const auto& addr : ipv6_addresses) {
    interface.add_ipv6_addresses(Ipv6ToStr(addr));
  }

  return interface;
}

}  // namespace crane