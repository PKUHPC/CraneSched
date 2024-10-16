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

 #pragma once

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#include "crane/Lock.h"
#include "protos/Crane.grpc.pb.h"
#include "protos/Crane.pb.h"

namespace Ctld {

using crane::grpc::Craned;
using grpc::Channel;
using grpc::Server;

class CtldForCranedServer;

class CtldForCranedServiceImpl final : public crane::grpc::CraneCtldForCraned::Service {
 public:
  explicit CtldForCranedServiceImpl(CtldForCranedServer* server) : m_ctld_for_craned_server_(server) {}
  grpc::Status TaskStatusChange(
      grpc::ServerContext *context,
      const crane::grpc::TaskStatusChangeRequest *request,
      crane::grpc::TaskStatusChangeReply *response) override;

  grpc::Status CranedRegister(
      grpc::ServerContext *context,
      const crane::grpc::CranedRegisterRequest *request,
      crane::grpc::CranedRegisterReply *response) override;
 private:
  CtldForCranedServer *m_ctld_for_craned_server_;
};

class CtldForCranedServer {
 public:
  /***
   * User must make sure that this constructor is called only once!
   * @param listen_address The "[Address]:[Port]" of CraneCtld.
   */
  explicit CtldForCranedServer(const Config::CraneCtldListenConf &listen_conf);

  inline void Wait() { m_server_->Wait(); }

 private:
  template <typename K, typename V,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using HashMap = absl::flat_hash_map<K, V, Hash>;

  template <typename K,
            typename Hash = absl::container_internal::hash_default_hash<K>>
  using HashSet = absl::flat_hash_set<K, Hash>;

  using Mutex = util::mutex;

  std::unique_ptr<CtldForCranedServiceImpl> m_service_impl_;
  std::unique_ptr<Server> m_server_;

  inline static std::mutex s_sigint_mtx;
  inline static std::condition_variable s_sigint_cv;
  static void signal_handler_func(int) { s_sigint_cv.notify_one(); };

  friend class CtldForCranedServiceImpl;
};

}  // namespace Ctld
 
inline std::unique_ptr<Ctld::CtldForCranedServer> g_ctld_for_craned_server;