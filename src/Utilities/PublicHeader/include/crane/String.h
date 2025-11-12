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

#pragma once

#include <absl/strings/ascii.h>
#include <absl/strings/str_join.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <re2/re2.h>
#include <spdlog/fmt/fmt.h>
#include <yaml-cpp/yaml.h>
#include <zlib.h>

#include <charconv>
#include <expected>
#include <filesystem>
#include <fstream>
#include <list>
#include <ranges>
#include <string>

#include "crane/PublicHeader.h"

namespace util {

template <typename T = std::string, typename YamlNode, typename DefaultType>
  requires requires(const YamlNode &node) {
    { node.template as<T>() };
  } && std::convertible_to<DefaultType, T>
T YamlValueOr(const YamlNode &node, const DefaultType &default_value) {
  return node ? node.template as<T>() : default_value;
}

using CertPair = std::pair<std::string,   // CN
                           std::string>;  // serial number

uint32_t Crc32Of(std::string_view data, uint32_t seed = 0);
uint32_t Adler32Of(std::string_view data, uint32_t seed = 1);

std::string ReadFileIntoString(std::filesystem::path const &p);

std::string ReadableMemory(uint64_t memory_bytes);

uint64_t ParseMemory(const std::string &mem);

bool ParseHostList(const std::string &host_str,
                   std::list<std::string> *host_list);

bool FoundFirstNumberWithoutBrackets(const std::string &input, int *start,
                                     int *end);

std::string RemoveBracketsWithoutDashOrComma(const std::string &input);

bool HostNameListToStr_(std::list<std::string> &host_list,
                        std::list<std::string> *res_list);

template <std::ranges::range T>
std::string HostNameListToStr(T const &host_list)
  requires std::same_as<std::ranges::range_value_t<T>, std::string>
{
  std::list<std::string> source_list{host_list.begin(), host_list.end()};
  while (true) {
    std::list<std::string> res_list;
    if (HostNameListToStr_(source_list, &res_list)) {
      res_list.sort();
      std::string host_name_str{absl::StrJoin(res_list, ",")};
      // Remove brackets containing single numbers
      // Temporary "[]" can be used to mark that the number has been processed,
      // so "[]" cannot be removed during the processing and can only be deleted
      // at the end
      return RemoveBracketsWithoutDashOrComma(host_name_str);
    }
    source_list = res_list;
  }
}

void SetCurrentThreadName(const std::string &name);

bool ConvertStringToInt64(const std::string &s, int64_t *val);

std::string ReadableTypedDeviceMap(const DeviceMap &dedicated_resource);
std::string ReadableDresInNode(const ResourceInNode &dedicated_resource);

std::string ReadableGrpcDresInNode(
    const crane::grpc::DedicatedResourceInNode &dres_in_node);

std::string GenerateCommaSeparatedString(int val);

uint32_t CalcConfigCRC32(const YAML::Node &config);

std::string SlugDns1123(std::string_view s, size_t max_len);

std::expected<CertPair, std::string> ParseCertificate(
    const std::string &cert_pem);

template <typename YamlNode>
std::optional<std::string> ParseCertConfig(const std::string &cert_name,
                                           const YamlNode &tls_config,
                                           std::string *file_path,
                                           std::string *file_content) {
  if (tls_config[cert_name]) {
    *file_path = tls_config[cert_name].template as<std::string>();
    try {
      *file_content = util::ReadFileIntoString(*file_path);
    } catch (const std::exception &e) {
      return fmt::format("Read {} error: {}", cert_name, e.what());
    }
    if (file_content->empty())
      return fmt::format(
          "UseTls is true, but the file specified by {} is empty", cert_name);
  } else {
    return fmt::format("UseTls is true, but {} is empty", cert_name);
  }
  return std::nullopt;
}

template <typename Map>
auto FlattenMapView(const Map &m) {
  return m | std::views::transform([](const auto &kv) {
           const auto &key = kv.first;
           const auto &values = kv.second;
           return values | std::views::transform([&key](const auto &v) {
                    return std::pair{key, v};
                  });
         }) |
         std::views::join;
}

std::string StepIdsToString(const job_id_t job_id, const step_id_t step_id);
std::string StepIdPairToString(const std::pair<job_id_t, step_id_t> &step);

std::string StepToDIdString(const crane::grpc::StepToD &step_to_d);
template <typename Range>
std::string StepToDRangeIdString(const Range &step_to_d_range) {
  return absl::StrJoin(step_to_d_range | std::views::transform(StepToDIdString),
                       ",");
}

template <typename Map>
std::string JobStepsToString(const Map &m) {
  auto step_strs_view =
      m | std::views::transform([](const auto &kv) {
        return kv.second |
               std::views::transform([key = kv.first](const auto &step_id) {
                 return StepIdsToString(key, step_id);
               });
      }) |
      std::views::join | std::ranges::to<std::vector>();
  return absl::StrJoin(step_strs_view, ",");
}

namespace Internal {
// clang-format off
constexpr std::array<std::string_view, crane::grpc::TaskStatus_ARRAYSIZE>
    CraneStepStatusStrArr = {
        // 0 - 4
        "Pending",
        "Running",
        "Completed",
        "Failed",
        "ExceedTimeLimit",
        // 5 - 9
        "Cancelled",
        "OutOfMemory",
        "Configuring",
        "Configured",
        "Completing",
        // 10 - 14
        "Invalid",
        "Invalid",
        "Invalid",
        "Invalid",
        "Invalid",
        // 15
        "Invalid",
};
// clang-format on
};  // namespace Internal

std::string StepStatusToString(const crane::grpc::TaskStatus &status);
}  // namespace util