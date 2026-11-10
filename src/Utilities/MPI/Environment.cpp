/**
 * Copyright (c) 2026 Peking University and Peking University
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

#include "Environment.h"

#include <algorithm>
#include <array>

namespace mpi {
namespace {
bool IsInheritedIdentityVariable(std::string_view name) {
  static constexpr std::array<std::string_view, 12> kPmiIdentityVariables{
      "PMI_FD",         "PMI_RANK",       "PMI_SIZE",  "PMI_JOBID",
      "PMI_LOCAL_RANK", "PMI_LOCAL_SIZE", "PMI_PORT",  "PMI_SPAWNED",
      "PMI_ID",         "PMIX_NAMESPACE", "PMIX_RANK", "PMIX_SERVER_URI"};
  return std::ranges::any_of(
      kPmiIdentityVariables, [name](std::string_view prefix) {
        return name == prefix ||
               (prefix == "PMIX_SERVER_URI" && name.starts_with(prefix));
      });
}

}  // namespace

void MergeLaunchEnvironment(std::unordered_map<std::string, std::string>& env,
                            const LaunchInfo& launch) {
  std::erase_if(env, [](const auto& item) {
    return IsInheritedIdentityVariable(item.first);
  });
  for (const auto& [name, value] : launch.env)
    env.insert_or_assign(name, value);
}
}  // namespace mpi
