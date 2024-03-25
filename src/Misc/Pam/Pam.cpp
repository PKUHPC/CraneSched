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

#include <fmt/format.h>

#include "PamUtil.h"

#define PAM_STR_TRUE ("T")
#define PAM_STR_FALSE ("F")
#define PAM_ITEM_AUTH_RESULT ("AUTH_RES")
#define PAM_ITEM_TASK_ID ("TASK_ID")

static std::once_flag g_init_flag;
static bool g_module_initialized{false};

void clean_up_cb(pam_handle_t *pamh, void *data, int error_status) {
  free(data);
};

extern "C" {

[[maybe_unused]] int pam_sm_acct_mgmt(pam_handle_t *pamh, int flags, int argc,
                                      const char **argv) {
  std::call_once(g_init_flag, LoadCraneConfig, pamh, argc, argv,
                 &g_module_initialized);

  int rc;
  bool ok;
  uid_t uid;
  std::string username;

  /* Asking the application for a username */
  ok = PamGetUserName(pamh, &username);
  if (!ok) {
    pam_syslog(pamh, LOG_ERR, "[Crane] Failed to get username");
    return PAM_SESSION_ERR;
  }

  if (username == "root") {
    pam_syslog(pamh, LOG_ERR, "[Crane] Allow root to log in");
    return PAM_SUCCESS;
  }

  if (!g_module_initialized) {
    pam_syslog(pamh, LOG_ERR,
               "[Crane] Pam module failed to read configuration. "
               "Only root is allowed.");
    return PAM_SESSION_ERR;
  }

  ok = PamGetRemoteUid(pamh, username.c_str(), &uid);
  if (!ok) {
    return PAM_USER_UNKNOWN;
  }

  uint8_t addr[4];
  uint16_t port;
  ok = PamGetRemoteAddressPort(pamh, addr, &port);

  if (!ok) {
    PamSendMsgToClient(
        pamh, "Crane: Cannot resolve src address and port in pam module.");
    return PAM_AUTH_ERR;
  }

  uint32_t task_id;

  std::string remote_address =
      fmt::format("{}.{}.{}.{}", addr[0], addr[1], addr[2], addr[3]);

  pam_syslog(pamh, LOG_ERR, "[Crane] Try to query %s for remote port %hu",
             remote_address.c_str(), port);

  ok = GrpcQueryPortFromCraned(pamh, uid, remote_address, port, &task_id);

  if (ok) {
    pam_syslog(pamh, LOG_ERR,
               "[Crane] Accepted ssh connection with remote port %hu ", port);

    char *auth_result = strdup(PAM_STR_TRUE);
    char *task_id_str = strdup(std::to_string(task_id).c_str());

    pam_set_data(pamh, PAM_ITEM_AUTH_RESULT, auth_result, clean_up_cb);
    pam_set_data(pamh, PAM_ITEM_TASK_ID, task_id_str, clean_up_cb);

    return PAM_SUCCESS;
  } else {
    char *auth_result = strdup(PAM_STR_FALSE);
    pam_set_data(pamh, PAM_ITEM_AUTH_RESULT, auth_result, clean_up_cb);

    pam_syslog(pamh, LOG_ERR,
               "[Crane] Rejected ssh connection with remote port %hu ", port);
    PamSendMsgToClient(pamh, "Rejected by CraneD PAM Module.");
    return PAM_PERM_DENIED;
  }
}

[[maybe_unused]] int pam_sm_open_session(pam_handle_t *pamh, int flags,
                                         int argc, const char **argv) {
  int task_id;
  char *auth_result;
  char *task_id_str;

  bool ok;
  std::string username;

  /* Asking the application for a username */
  ok = PamGetUserName(pamh, &username);
  if (!ok) {
    pam_syslog(pamh, LOG_ERR, "[Crane] Failed to get username");
    return PAM_SESSION_ERR;
  }

  if (username == "root") {
    pam_syslog(
        pamh, LOG_ERR,
        "[Crane] Allow root to open a session without resource restriction");
    return PAM_SUCCESS;
  }

  if (!g_module_initialized) {
    pam_syslog(pamh, LOG_ERR,
               "[Crane] Pam module failed to read configuration. "
               "Only root is allowed.");
    return PAM_SESSION_ERR;
  }

  pam_get_data(pamh, PAM_ITEM_AUTH_RESULT, (const void **)&auth_result);
  if (strcmp(auth_result, PAM_STR_TRUE) == 0) {
    pam_get_data(pamh, PAM_ITEM_TASK_ID, (const void **)&task_id_str);

    pam_syslog(
        pamh, LOG_ERR,
        "[Crane] open_session retrieved task_id: %s. Moving it to cgroups",
        task_id_str);

    task_id = std::atoi(task_id_str);

    ok = GrpcMigrateSshProcToCgroup(pamh, getpid(), task_id);
    if (ok)
      return PAM_SUCCESS;
    else
      return PAM_SESSION_ERR;
  } else {
    // If auth result is false, it indicates that system administrator allow a
    // user with no task running to log in, and then we just let it pass.
    return PAM_SUCCESS;
  }
}

// This function is required once pam_sm_open_session is written.
[[maybe_unused]] int pam_sm_close_session(pam_handle_t *pamh, int flags,
                                          int argc, const char **argv) {
  return PAM_IGNORE;
}
}