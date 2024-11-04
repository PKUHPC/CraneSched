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
#include <spdlog/fmt/fmt.h>
#include <yaml-cpp/yaml.h>

#include <format>
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