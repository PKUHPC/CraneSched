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

#include <gtest/gtest.h>

#include "crane/Network.h"
#include "crane/PublicHeader.h"
constexpr std::array slots = {"/dev/nvidia0", "/dev/nvidia1", "/dev/nvidia2",
                              "/dev/nvidia3", "/dev/nvidia4", "/dev/nvidia5",
                              "/dev/nvidia6", "/dev/nvidia7"};

TEST(DEDICATED_RES_NODE, le_gt) {
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert(
      {slots[0], slots[1], slots[3], slots[2]});
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});
  ASSERT_PRED2([](const auto& lhs, const auto& rhs) { return !(lhs <= rhs); },
               resourceInNode, resourceInNode2);
}

TEST(DEDICATED_RES_NODE, le_lt1) {
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0]});
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});
  ASSERT_LE(resourceInNode, resourceInNode2);
}

TEST(DEDICATED_RES_NODE, le_lt2) {
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0]});
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});
  ASSERT_LE(resourceInNode, resourceInNode2);
}

TEST(DEDICATED_RES_NODE, le_lt3) {
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0]});

  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});
  resourceInNode2["GPU"]["A200"].insert({slots[0], slots[1], slots[3]});
  ASSERT_LE(resourceInNode, resourceInNode2);
}

TEST(DEDICATED_RES_NODE, le_nle) {
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0], slots[2]});
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});
  ASSERT_PRED2([](const auto& lhs, const auto& rhs) { return !(lhs <= rhs); },
               resourceInNode, resourceInNode2);
}

TEST(DEDICATED_RES_NODE, le_equ) {
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0], slots[1]});
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1]});
  ASSERT_LE(resourceInNode, resourceInNode2);
}

TEST(DEDICATED_RES_NODE, le_equ2) {
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0], slots[1]});
  resourceInNode["XPU"]["A100"].insert({slots[0], slots[1]});
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1]});
  resourceInNode2["XPU"]["A100"].insert({slots[0], slots[1]});
  ASSERT_LE(resourceInNode, resourceInNode2);
}

TEST(DEDICATED_RES_NODE, equ_equ) {
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0], slots[1]});
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1]});
  ASSERT_EQ(resourceInNode, resourceInNode2);
}

TEST(DEDICATED_RES_NODE, equ_lt) {
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0]});
  resourceInNode["XPU"]["X100"];
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1]});
  resourceInNode2["TPU"]["T100"];
  ASSERT_PRED2([](const auto& lhs, const auto& rhs) { return !(lhs == rhs); },
               resourceInNode, resourceInNode2);
}

TEST(DEDICATED_RES_NODE, equ_gt) {
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});
  resourceInNode["XPU"]["X100"];
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1]});
  resourceInNode2["TPU"]["T100"];
  ASSERT_PRED2([](const auto& lhs, const auto& rhs) { return !(lhs == rhs); },
               resourceInNode, resourceInNode2);
}

TEST(DEDICATED_RES_NODE, plus1) {
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2 += resourceInNode;
  ASSERT_EQ(resourceInNode, resourceInNode2);
}

TEST(DEDICATED_RES_NODE, plus2) {
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["XPU"]["X100"].insert({slots[1], slots[6], slots[5]});
  auto temp = resourceInNode2;
  temp += resourceInNode;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});
  ASSERT_EQ(temp, resourceInNode2);
}

TEST(DEDICATED_RES_NODE, plus3) {
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});
  resourceInNode2["XPU"]["X100"].insert({slots[1], slots[6], slots[5]});
  auto temp = resourceInNode2;
  temp += resourceInNode;
  ASSERT_EQ(temp, resourceInNode2);
}

TEST(DEDICATED_RES_NODE, minus1) {
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});
  resourceInNode2["XPU"]["X100"].insert({slots[1], slots[6], slots[5]});
  resourceInNode2 -= resourceInNode;
  DedicatedResourceInNode res;
  res["XPU"]["X100"].insert({slots[1], slots[6], slots[5]});
  ASSERT_EQ(res, resourceInNode2);
}

TEST(DEDICATED_RES_NODE, minus2) {
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0]});
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0]});
  resourceInNode2 -= resourceInNode;
  DedicatedResourceInNode res;
  res.name_type_slots_map.clear();
  ASSERT_EQ(res, resourceInNode2);
}

TEST(DEDICATED_RES_NODE, req_map) {
  std::unordered_map<std::string /*name*/,
                     std::pair<uint64_t /*untyped req count*/,
                               std::unordered_map<std::string /*type*/,
                                                  uint64_t /*type total*/>>>
      req;
  req["GPU"] = {1, {{"A100", 1}}};
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert(
      {slots[0], slots[1], slots[3], slots[2]});
  ASSERT_LE(req, resourceInNode);
}

TEST(DEDICATED_RES_NODE, req_map2) {
  using req_t = std::unordered_map<
      std::string /*name*/,
      std::pair<
          uint64_t /*untyped req count*/,
          std::unordered_map<std::string /*type*/, uint64_t /*type total*/>>>;
  req_t req;
  req["GPU"] = {4, {{"A100", 1}}};
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert(
      {slots[0], slots[1], slots[3], slots[2]});
  ASSERT_PRED2([](const auto& lhs, const auto& rhs) { return !(lhs <= rhs); },
               req, resourceInNode);
}

TEST(DEDICATED_RES_NODE, req_map3) {
  using req_t = std::unordered_map<
      std::string /*name*/,
      std::pair<
          uint64_t /*untyped req count*/,
          std::unordered_map<std::string /*type*/, uint64_t /*type total*/>>>;
  req_t req;
  req["GPU"] = {1, {}};
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0]});
  ASSERT_LE(req, resourceInNode);
}

TEST(DEDICATED_RES_NODE, req_map4) {
  using req_t = std::unordered_map<
      std::string /*name*/,
      std::pair<
          uint64_t /*untyped req count*/,
          std::unordered_map<std::string /*type*/, uint64_t /*type total*/>>>;
  req_t req;
  req["GPU"] = {4, {{"A100", 1}}};
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["B100"].insert(
      {slots[0], slots[1], slots[3], slots[2]});
  ASSERT_PRED2([](const auto& lhs, const auto& rhs) { return !(lhs <= rhs); },
               req, resourceInNode);
}

TEST(DEDICATED_RES_NODE, req_map5) {
  ASSERT_EQ(6, crane::GetIpAddrVer("3001:0da8:82a3:0:0:8B2E:0270:7224"));
  ASSERT_EQ(6, crane::GetIpAddrVer("::1"));
  ASSERT_EQ(6, crane::GetIpAddrVer("ff::1:2"));
  ASSERT_EQ(6, crane::GetIpAddrVer("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
  ASSERT_EQ(6, crane::GetIpAddrVer("ff01::01"));
  ASSERT_EQ(4, crane::GetIpAddrVer("10.11.82.1"));
  ASSERT_EQ(4, crane::GetIpAddrVer("127.0.0.1"));
  ASSERT_EQ(-1, crane::GetIpAddrVer("lijunlin"));
  using req_t = std::unordered_map<
      std::string /*name*/,
      std::pair<
          uint64_t /*untyped req count*/,
          std::unordered_map<std::string /*type*/, uint64_t /*type total*/>>>;
  req_t req;
  req["GPU"] = {4, {{"A100", 1}}};
  DedicatedResourceInNode resourceInNode;
  resourceInNode["XPU"]["B100"].insert(
      {slots[0], slots[1], slots[3], slots[2]});
  ASSERT_PRED2([](const auto& lhs, const auto& rhs) { return !(lhs <= rhs); },
               req, resourceInNode);
}
