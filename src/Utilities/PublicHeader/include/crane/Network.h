#pragma once

#include <arpa/inet.h>
#include <netdb.h>

#include <string>

namespace crane {

bool ResolveHostnameFromIpv4(const std::string& addr, std::string* hostname);

bool ResolveHostnameFromIpv6(const std::string& addr, std::string* hostname);

bool ResolveIpv4FromHostname(const std::string& hostname, std::string* addr);

bool IsAValidIpv4Address(const std::string& ipv4);

}  // namespace crane