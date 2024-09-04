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

namespace crane {

void InitializeNetworkFunctions();

bool ResolveHostnameFromIpv4(uint32_t addr, std::string* hostname);

bool ResolveHostnameFromIpv6(absl::uint128 addr, std::string* hostname);

bool ResolveIpv4FromHostname(const std::string& hostname, uint32_t* addr);

bool ResolveIpv6FromHostname(const std::string& hostname, absl::uint128* addr);

bool Ipv4ToUint32(const std::string& ip, uint32_t* addr);

bool Ipv6ToUint128(const std::string& ip, absl::uint128* addr);

std::string Ipv4ToString(uint32_t addr);

std::string Ipv6ToString(absl::uint128 addr);

// -1 on error, 4 for IPv4, 6 for IPv6
int GetIpAddressVersion(const std::string& ip);

bool FindTcpInodeByPort(int port, ino_t* inode, const std::string& tcp_path);
}  // namespace crane