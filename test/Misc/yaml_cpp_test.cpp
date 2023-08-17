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

#include <gtest/gtest.h>
#include <spdlog/fmt/fmt.h>
#include <yaml-cpp/yaml.h>

#include <string>

TEST(YAML, Sample) {
  std::string config_yaml_path = "/etc/crane/config.yaml";

  try {
    YAML::Node node = YAML::LoadFile(config_yaml_path);

    if (node["ControlMachine"]) {
      fmt::print("ControlMachine: {}\n",
                 node["ControlMachine"].as<std::string>());
    }
  } catch (YAML::BadFile &e) {
    fmt::print(stderr, "Config file {} doesn't exist.", config_yaml_path);
  }
}