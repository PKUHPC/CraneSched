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

#pragma once

#ifdef CRANE_ENABLE_HWLOC
#  include <hwloc.h>
#endif

#include "crane/PublicHeader.h"

namespace Craned::Common {

class CpuTopoDetector {
 public:
  static NodeTopoInfo DetectActual();
  static ConfiguredNodeTopology NormalizeConfiguredTopology(
      ConfiguredNodeTopology config, const std::string& node_name);
  static NodeTopoInfo SelectNodeTopology(
      const ConfiguredNodeTopology& configured, const NodeTopoInfo& actual,
      bool config_overrides);

 private:
#ifdef CRANE_ENABLE_HWLOC
  static bool LoadTopology_(hwloc_topology_t* topology);
  static NodeTopoInfo BuildFromHwloc_(hwloc_topology_t topology);
#endif
  static NodeTopoInfo ParseFromProcCpuinfo_();
};

}  // namespace Craned::Common
