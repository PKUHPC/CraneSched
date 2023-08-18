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

  if (getnameinfo((struct sockaddr *)&sa, len, hbuf, sizeof(hbuf), NULL, 0,
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

void callback(void* arg, int status, int timeouts, struct hostent* host) {
  (void)timeouts;  // 未使用
  std::string hostname = *(static_cast<std::string*>(arg));

  if (status != ARES_SUCCESS) {
    std::cerr << "Failed to resolve " << hostname << ": "
              << ares_strerror(status) << std::endl;
    return;
  }

  if (host->h_addrtype == AF_INET && host->h_length == sizeof(struct in_addr)) {
    char ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, host->h_addr, ip, sizeof(ip));
    std::cout << hostname << " -> " << ip << std::endl;
  }
}

TEST(NetworkFunc, ResolveIpv4FromHostnameAres) {
  using crane::ResolveIpv4FromHostname;

  ares_channel channel;
  int nfds;
  fd_set readers, writers;
  struct timeval *tvp, tv{};
  std::list<std::string> hostnames;

  util::ParseHostList("h[1-1000]", &hostnames);

  //  //before
  //  for (const auto& hostname : hostnames) {
  //    std::string ipv4;
  //    ResolveIpv4FromHostname(hostname, &ipv4);
  //    std::cout << hostname << " -> " << ipv4 << std::endl;
  //  }
  //  //

  if (ares_library_init(ARES_LIB_INIT_ALL) != 0) {
    std::cerr << "ares_library_init failed" << std::endl;
    exit(1);
  }

  if (ares_init(&channel) != ARES_SUCCESS) {
    std::cerr << "ares_init failed" << std::endl;
    ares_library_cleanup();
    exit(1);
  }

  int epoll_fd = epoll_create1(0);
  if (epoll_fd == -1) {
    perror("epoll_create1");
    exit(1);
  }

  // Start hostname resolutions
  for (const auto& hostname : hostnames) {
    ares_gethostbyname(channel, hostname.c_str(), AF_INET, callback,
                       (void*)&hostname);
  }

  // Use epoll for the event loop
  while (true) {
    struct epoll_event events[10];
    int num_fds =
        epoll_wait(epoll_fd, events, 10, 0);  // wait up to 1000ms (1 second)

    if (num_fds == -1) {
      perror("epoll_wait");
      exit(1);
    }

    if (num_fds == 0) {  // timeout
      ares_process_fd(channel, ARES_SOCKET_BAD, ARES_SOCKET_BAD);
      std::cout << "time out" << std::endl;
    } else {
      for (int i = 0; i < num_fds; i++) {
        ares_socket_t rfd =
            (events[i].events & EPOLLIN) ? events[i].data.fd : ARES_SOCKET_BAD;
        ares_socket_t wfd =
            (events[i].events & EPOLLOUT) ? events[i].data.fd : ARES_SOCKET_BAD;
        std::cout << "close" << std::endl;
        ares_process_fd(channel, rfd, wfd);
      }
    }
  }

  // Print results
  //  for (const auto& [hostname, ip] : hostname_to_ip_map) {
  //    std::cout << hostname << " -> " << ip << std::endl;
  //  }

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
