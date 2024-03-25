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

bool PamGetRemoteAddressPort(pam_handle_t *pamh, uint8_t addr[4],
                             uint16_t *port);

bool GrpcQueryPortFromCraned(pam_handle_t *pamh, uid_t uid,
                             const std::string &remote_address,
                             uint16_t port_to_query, uint32_t *task_id);

bool GrpcMigrateSshProcToCgroup(pam_handle_t *pamh, pid_t pid,
                                task_id_t task_id);

struct PamConfig {
  std::string CraneConfigFilePath;
  std::string CraneBaseDir;
  std::string CranedUnixSockPath;
};

inline PamConfig g_pam_config;

void LoadCraneConfig(int argc, const char **argv, bool *initialized);
