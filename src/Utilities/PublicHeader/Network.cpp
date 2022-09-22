#include "crane/Network.h"

#include <cstring>
#include <regex>

#include "crane/PublicHeader.h"

namespace crane {

bool ResolveHostnameFromIpv4(const std::string& addr, std::string* hostname) {
  struct sockaddr_in sa; /* input */
  socklen_t len;         /* input */
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
  struct sockaddr_in6 sa6; /* input */
  socklen_t len;           /* input */
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
  struct addrinfo hints {};
  struct addrinfo *res, *tmp;
  char host[256];

  hints.ai_family = AF_INET;

  int ret = getaddrinfo(hostname.c_str(), nullptr, &hints, &res);
  if (ret != 0) {
    CRANE_WARN("Error in getaddrinfo when resolving hostname {}: {}",
               hostname.c_str(), gai_strerror(ret));
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
  std::regex ipv4_re(R"(^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)(\.(?!$)|$)){4}$)");
  std::smatch ipv4_group;
  if (!std::regex_match(ipv4, ipv4_group, ipv4_re)) {
    return false;
  }
  return true;
}

}  // namespace crane