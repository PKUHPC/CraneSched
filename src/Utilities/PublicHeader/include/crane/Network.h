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
#include <absl/strings/str_split.h>
#include <absl/strings/string_view.h>
#include <re2/re2.h>

#include <fstream>
#include <string>

namespace crane {

namespace internal {

void InitializeNetworkUtility();

}

bool ResolveHostnameFromIpv4(const std::string& addr, std::string* hostname);

bool ResolveHostnameFromIpv6(const std::string& addr, std::string* hostname);

bool ResolveIpv4FromHostname(const std::string& hostname, std::string* addr);

bool IsAValidIpv4Address(const std::string& ipv4);

}  // namespace crane