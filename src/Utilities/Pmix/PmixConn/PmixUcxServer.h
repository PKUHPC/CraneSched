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

#include "PmixASyncServer.h"
#include "PmixCommon.h"

namespace pmix {

class PmixUcxServer: public PmixASyncServer {
public:
  explicit PmixUcxServer() = default;

  bool Init(const Config& config) override;

  void Shutdown() override {  }

  void Wait() override { }

private:
  // std::unique_ptr<PmixGrpcServiceImpl> m_service_impl_;
  // std::unique_ptr<Server> m_server_;
  //
  // friend class PmixASyncServiceImpl;
};

} // namespace pmix