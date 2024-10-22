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

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netdb.h>

#include "crane/Network.h"

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

TEST(NetworkFunc, IsAValidIpv4Address) {
  using crane::IsAValidIpv4Address;

  std::string ip1{"1.2.3.4"};
  std::string ip2{"2.3.4.777"};

  EXPECT_TRUE(IsAValidIpv4Address(ip1));
  EXPECT_FALSE(IsAValidIpv4Address(ip2));
}
