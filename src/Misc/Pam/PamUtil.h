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

#include <security/_pam_macros.h>
#include <security/pam_ext.h>
#include <security/pam_modules.h>
#include <syslog.h>
#include <unistd.h>

#include <cstdint>
#include <mutex>
#include <string>

#include "crane/PublicHeader.h"

bool PamGetUserName(pam_handle_t *pamh, std::string *username);

void PamSendMsgToClient(pam_handle_t *pamh, const char *mesg);

bool PamGetRemoteUid(pam_handle_t *pamh, const char *user_name, uid_t *uid);

bool PamGetRemoteAddressPort(pam_handle_t *pamh, std::string *address,
                             uint16_t *port);

bool GrpcQueryPortFromCraned(pam_handle_t *pamh, uid_t uid,
                             const std::string &remote_address,
                             uint16_t port_to_query, uint32_t *task_id);

bool GrpcMigrateSshProcToCgroupAndSetEnv(pam_handle_t *pamh, pid_t pid,
                                         task_id_t task_id);

struct PamConfig {
  std::string CraneConfigFilePath;
  std::string CraneBaseDir;
  std::string CranedUnixSockPath;
};

inline PamConfig g_pam_config;

void LoadCraneConfig(pam_handle_t *pamh, int argc, const char **argv,
                     bool *initialized);
