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

#include <absl/synchronization/blocking_counter.h>
#include <absl/synchronization/mutex.h>
#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netdb.h>
#include <sys/epoll.h>

#include "crane/Network.h"
#include "crane/String.h"

TEST(NetworkFunc, ResolveHostName) {
  struct sockaddr_in sa; /* input */
  socklen_t len;         /* input */
  char hbuf[NI_MAXHOST];

  memset(&sa, 0, sizeof(struct sockaddr_in));

  /* For IPv4*/
  sa.sin_family = AF_INET;
  sa.sin_addr.s_addr = inet_addr("172.16.1.11");
  len = sizeof(struct sockaddr_in);

  if (getnameinfo((struct sockaddr*)&sa, len, hbuf, sizeof(hbuf), NULL, 0,
                  NI_NAMEREQD)) {
    printf("could not resolve hostname\n");
  } else {
    printf("host=%s\n", hbuf);
  }
}

TEST(NetworkFunc, ResolveIpv4FromHostname) {
  using crane::ResolveIpv4FromHostname;

  std::string hostname{"123.123.123.123"};
  std::string ipv4;
  bool ok = ResolveIpv4FromHostname(hostname, &ipv4);
  GTEST_LOG_(INFO) << "Resolve succeeded: " << ok;

  if (ok) {
    GTEST_LOG_(INFO) << "Resolved hostname " << hostname << " to " << ipv4;
  }
}

struct AresData {
  ares_channel channel;

  absl::BlockingCounter* counter;
  std::string hostname;
  std::string ipv4;
};

void name_cb(void* arg, int status, int timeouts, char* node, char* service) {
  auto* data = reinterpret_cast<AresData*>(arg);

  if (status != ARES_SUCCESS) {
    std::cerr << "Failed to getnameinfo of " << data->hostname << ": "
              << ares_strerror(status) << std::endl;
    return;
  }

  data->ipv4.assign(node);
  data->counter->DecrementCount();
}

void addr_cb(void* arg, int status, int timeouts,
             struct ares_addrinfo* result) {
  (void)timeouts;
  auto* data = reinterpret_cast<AresData*>(arg);

  if (status != ARES_SUCCESS) {
    std::cerr << "Failed to resolve " << data->hostname << ": "
              << ares_strerror(status) << std::endl;
    return;
  }

  struct ares_addrinfo_node* it;
  for (it = result->nodes; it != nullptr; it = it->ai_next) {
    ares_getnameinfo(data->channel, it->ai_addr, it->ai_addrlen,
                     ARES_NI_NUMERICHOST, name_cb, arg);
  }

  ares_freeaddrinfo(result);
}

TEST(NetworkFunc, ResolveIpv4FromHostnameAres) {
  using crane::ResolveIpv4FromHostname;

  constexpr int host_num = 10000;
  std::list<std::string> hostnames;
  util::ParseHostList(fmt::format("h[1-{}]", host_num), &hostnames);

  if (ares_library_init(ARES_LIB_INIT_ALL) != 0) {
    std::cerr << "ares_library_init failed" << std::endl;
    exit(1);
  }

  ares_channel channel;
  if (ares_init(&channel) != ARES_SUCCESS) {
    std::cerr << "ares_init failed" << std::endl;
    ares_library_cleanup();
    exit(1);
  }

  struct ares_addrinfo_hints hints {};
  hints.ai_family = PF_INET;

  auto ares_data = std::make_unique_for_overwrite<AresData[]>(host_num);
  absl::BlockingCounter bc(host_num);

  int idx = 0;
  for (const auto& hostname : hostnames) {
    ares_data[idx].channel = channel;
    ares_data[idx].counter = &bc;
    ares_data[idx].hostname = hostname;
    ares_getaddrinfo(channel, hostname.c_str(), nullptr, &hints, addr_cb,
                     (void*)&ares_data[idx]);
    idx++;
  }

  bc.Wait();

  for (idx = 0; idx < host_num; idx++) {
    std::cout << ares_data[idx].hostname << " -> " << ares_data[idx].ipv4
              << std::endl;
  }

  ares_destroy(channel);
  ares_library_cleanup();
}

TEST(NetworkFunc, IsAValidIpv4Address) {
  using crane::IsAValidIpv4Address;

  std::string ip1{"1.2.3.4"};
  std::string ip2{"2.3.4.777"};

  EXPECT_TRUE(IsAValidIpv4Address(ip1));
  EXPECT_FALSE(IsAValidIpv4Address(ip2));
}
