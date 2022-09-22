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