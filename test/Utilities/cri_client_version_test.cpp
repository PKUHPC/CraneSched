/**
 * Copyright (c) 2025 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

#include <gtest/gtest.h>

#ifdef SIGABRT
#  undef SIGABRT
#endif

#include "crane/CriClient.h"

namespace {

TEST(CriClientVersionTest, AcceptsContainerd17AndNewer) {
  EXPECT_TRUE(cri::CriClient::IsRuntimeVersionSupported("1.7.0"));
  EXPECT_TRUE(cri::CriClient::IsRuntimeVersionSupported("1.7.28"));
  EXPECT_TRUE(cri::CriClient::IsRuntimeVersionSupported("2.2.1"));
  EXPECT_TRUE(cri::CriClient::IsRuntimeVersionSupported("v2.2.1"));
}

TEST(CriClientVersionTest, RejectsOlderOrMalformedVersions) {
  EXPECT_FALSE(cri::CriClient::IsRuntimeVersionSupported("1.6.32"));
  EXPECT_FALSE(cri::CriClient::IsRuntimeVersionSupported("0.25.0"));
  EXPECT_FALSE(cri::CriClient::IsRuntimeVersionSupported("1"));
  EXPECT_FALSE(cri::CriClient::IsRuntimeVersionSupported("1.x.0"));
  EXPECT_FALSE(cri::CriClient::IsRuntimeVersionSupported("1.7."));
  EXPECT_FALSE(cri::CriClient::IsRuntimeVersionSupported("1.7"));
  EXPECT_FALSE(cri::CriClient::IsRuntimeVersionSupported("1.7.0-rc"));
  EXPECT_FALSE(cri::CriClient::IsRuntimeVersionSupported("1.7.0-rc1"));
  EXPECT_FALSE(cri::CriClient::IsRuntimeVersionSupported("1.7.0-alpha1"));
  EXPECT_FALSE(cri::CriClient::IsRuntimeVersionSupported("1.7.0-beta2"));
  EXPECT_FALSE(cri::CriClient::IsRuntimeVersionSupported("1.7alpha"));
  EXPECT_FALSE(cri::CriClient::IsRuntimeVersionSupported("1.7..0"));
}

TEST(CriClientVersionTest, AcceptsRuntimeVendorSuffix) {
  EXPECT_TRUE(cri::CriClient::IsRuntimeVersionSupported("2.3.2-k3s2"));
  EXPECT_TRUE(cri::CriClient::IsRuntimeVersionSupported("1.7.0+build.1"));
}

TEST(CriClientVersionTest, ParsesMajorAndMinor) {
  auto parsed = cri::CriClient::ParseRuntimeVersionMajorMinor("1.7.0");
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(parsed->first, 1U);
  EXPECT_EQ(parsed->second, 7U);
}

}  // namespace
