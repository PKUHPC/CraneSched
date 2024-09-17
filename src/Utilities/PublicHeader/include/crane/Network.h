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

}  // namespace crane