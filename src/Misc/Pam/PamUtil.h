#pragma once

#include <security/_pam_macros.h>
#include <security/pam_ext.h>
#include <security/pam_modules.h>
#include <syslog.h>

#include <cstdint>
#include <string>

bool PamGetUserName(pam_handle_t *pamh, std::string *username);

void PamSendMsgToClient(pam_handle_t *pamh, const char *mesg);

bool PamGetRemoteUid(pam_handle_t *pamh, const char *user_name, uid_t *uid);

bool PamGetRemoteAddressPort(pam_handle_t *pamh, uint8_t addr[4],
                             uint16_t *port);

bool GrpcQueryPortFromCraned(pam_handle_t *pamh, uid_t uid,
                             const std::string &craned_address,
                             const std::string &craned_port,
                             uint16_t port_to_query, uint32_t *task_id,
                             std::string *cgroup_path);

bool GrpcMigrateSshProcToCgroup(pam_handle_t *pamh, pid_t pid,
                                const char *cgroup_path);
