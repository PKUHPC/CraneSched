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

#include <list>

#include "PmixColl.h"
#include "crane/Lock.h"
#include "parallel_hashmap/phmap.h"
namespace pmix {

struct PmixNameSpace {
  std::string m_namespace_;
  uint32_t m_node_num_ = 0; /* number of nodes in this namespace */
  int m_node_id_ = 0; /* relative position of this node in this step */
  uint32_t m_task_num_ = 0; /* total number of tasks in this namespace */
  std::vector<uint32_t> m_task_cnts_; /* Number of tasks on each node of namespace */
  std::string m_task_map_packed_; /* Packed task mapping information */
  std::vector<uint32_t> m_task_map_; /* i'th task is located on task_map[i] node */
  std::vector<std::string> m_hostlist_;
};

void PmixLibModexInvoke(pmix_modex_cbfunc_t mdx_fn, bool result, const char *data, size_t ndata,
                        void *cbdata, void *rel_fn, void *rel_data);

class PmixState {
 public:
  PmixState() = default;

  void Init(const std::string &hostname);

  std::shared_ptr<Coll> PmixStateCollGet(CollType type, const std::vector<pmix_proc_t>& ranges,
                         size_t nranges);

  std::optional<PmixNameSpace> PmixNamespaceGet(const std::string& pmix_namespace);

  std::string GetHostNameByRank(const PmixNameSpace& pmix_namespace, uint32_t rank);

  std::string GetHostname();

  void AddNameSpace(const PmixNameSpace& pmix_namespace);

 private:
  std::string m_hostname_;

  util::rw_mutex m_mutex_;
  std::list<std::shared_ptr<Coll>> m_coll_list_;

  using NamespaceMap = phmap::parallel_flat_hash_map<
      std::string,  // username
      PmixNameSpace, phmap::priv::hash_default_hash<std::string>,
      phmap::priv::hash_default_eq<std::string>,
      std::allocator<std::pair<const std::string, PmixNameSpace>>, 4,
      std::shared_mutex>;

  NamespaceMap m_namespace_map_;
};

inline std::unique_ptr<PmixState> g_pmix_state_ptr;

} // namespace pmix