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

#pragma once

// System Headers
#include <arpa/inet.h>
#include <netdb.h>

// Library Headers
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <absl/strings/string_view.h>
#include <re2/re2.h>

#include <fstream>
#include <string>
#include "protos/PublicDefs.pb.h"

using ipv4_t = uint32_t;
using ipv6_t = absl::uint128;

namespace crane {

void InitializeNetworkFunctions();

bool ResolveHostnameFromIpv4(ipv4_t addr, std::string* hostname);

bool ResolveHostnameFromIpv6(const ipv6_t& addr, std::string* hostname);

bool ResolveIpv4FromHostname(const std::string& hostname, ipv4_t* addr);

bool ResolveIpv6FromHostname(const std::string& hostname, ipv6_t* addr);

bool StrToIpv4(const std::string& ip, ipv4_t* addr);

bool StrToIpv6(const std::string& ip, ipv6_t* addr);

std::string Ipv4ToStr(ipv4_t addr);

std::string Ipv6ToStr(const ipv6_t& addr);

/// @param ip string of ip address
/// @return -1 if ip is not a valid ipv4 or ipv6 address, otherwise 4 is return
/// for IPv4 or 6 is returned for IPv6.
int GetIpAddrVer(const std::string& ip);

bool FindTcpInodeByPort(const std::string& tcp_path, int port, ino_t* inode);

struct NetworkInterface {
  std::string name;
  std::string mac_address;
  std::vector<ipv4_t> ipv4_addresses;
  std::vector<ipv6_t> ipv6_addresses;

  static NetworkInterface FromGrpc(const crane::grpc::NetworkInterface& grpc_interface) {
    NetworkInterface interface;
    interface.name = grpc_interface.name();
    interface.mac_address = grpc_interface.mac_address();
    
    for (const auto& addr : grpc_interface.ipv4_addresses()) {
      ipv4_t ipv4;
      if (StrToIpv4(addr, &ipv4)) {
        interface.ipv4_addresses.push_back(ipv4);
      }
    }
    
    for (const auto& addr : grpc_interface.ipv6_addresses()) {
      ipv6_t ipv6;
      if (StrToIpv6(addr, &ipv6)) {
        interface.ipv6_addresses.push_back(ipv6);
      }
    }
    
    return interface;
  }

  crane::grpc::NetworkInterface ToGrpc() const {
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
};

std::vector<NetworkInterface> GetNetworkInterfaces();

}  // namespace crane