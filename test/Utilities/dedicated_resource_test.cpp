#include <gtest/gtest.h>

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

TEST(DEDICATED_RES, le_lt1) {
  DedicatedResource lhs, rhs;
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0], slots[1]});
  lhs["node1"] = resourceInNode;
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});

  rhs["node1"] = resourceInNode2;

  ASSERT_LE(resourceInNode, resourceInNode2);
}

TEST(DEDICATED_RES, le_lt2) {
  DedicatedResource lhs, rhs;
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0], slots[1]});
  lhs["node1"] = resourceInNode;
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});

  rhs["node1"] = resourceInNode2;
  rhs["node2"] = resourceInNode2;

  ASSERT_LE(resourceInNode, resourceInNode2);
}

TEST(DEDICATED_RES, le_lt3) {
  DedicatedResource lhs, rhs;
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0], slots[1]});
  lhs["node1"] = resourceInNode;
  lhs["node2"];
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});

  rhs["node1"] = resourceInNode2;

  ASSERT_LE(resourceInNode, resourceInNode2);
}

TEST(DEDICATED_RES, le_lt4) {
  DedicatedResource lhs, rhs;
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0], slots[1]});
  lhs["node1"] = resourceInNode;
  lhs["node2"]["TPU"]["X100"];
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});

  rhs["node1"] = resourceInNode2;

  ASSERT_LE(resourceInNode, resourceInNode2);
}

TEST(DEDICATED_RES, le_lt6) {
  DedicatedResource lhs, rhs;
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0], slots[1]});
  lhs["node1"] = resourceInNode;
  lhs["node2"];
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});

  rhs["node1"] = resourceInNode2;
  rhs["node2"] = resourceInNode2;

  ASSERT_LE(resourceInNode, resourceInNode2);
}

TEST(DEDICATED_RES, le_nle) {
  DedicatedResource lhs, rhs;
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0], slots[1], slots[2]});
  lhs["node1"] = resourceInNode;
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});

  rhs["node1"] = resourceInNode2;

  ASSERT_PRED2([](const auto& lhs, const auto& rhs) { return !(lhs <= rhs); },
               lhs, rhs);
}

TEST(DEDICATED_RES, le_equ) {
  DedicatedResource lhs, rhs;
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});
  lhs["node1"] = resourceInNode;
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});

  rhs["node1"] = resourceInNode2;

  ASSERT_LE(lhs, rhs);
}

TEST(DEDICATED_RES, le_gt) {
  DedicatedResource lhs, rhs;
  DedicatedResourceInNode resourceInNode;
  resourceInNode["GPU"]["A100"].insert(
      {slots[0], slots[1], slots[3], slots[2]});
  lhs["node1"] = resourceInNode;
  DedicatedResourceInNode resourceInNode2;
  resourceInNode2["GPU"]["A100"].insert({slots[0], slots[1], slots[3]});

  rhs["node1"] = resourceInNode2;

  ASSERT_PRED2([](const auto& lhs, const auto& rhs) { return !(lhs <= rhs); },
               lhs, rhs);
}
