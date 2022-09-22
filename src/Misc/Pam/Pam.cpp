#include <fmt/format.h>

#include <cstdint>

#include "PamUtil.h"
#include "crane/PublicHeader.h"

#define PAM_STR_TRUE ("T")
#define PAM_STR_FALSE ("F")
#define PAM_ITEM_AUTH_RESULT ("AUTH_RES")
#define PAM_ITEM_TASK_ID ("TASK_ID")
#define PAM_ITEM_CG_PATH ("CG_PATH")

void clean_up_cb(pam_handle_t *pamh, void *data, int error_status) {
  free(data);
};

extern "C" {

int pam_sm_acct_mgmt(pam_handle_t *pamh, int flags, int argc,
                     const char **argv) {
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
  std::string cgroup_path;

  std::string server_address =
      fmt::format("{}.{}.{}.{}", addr[0], addr[1], addr[2], addr[3]);

  pam_syslog(pamh, LOG_ERR, "[Crane] Try to query %s for remote port %hu",
             server_address.c_str(), port);

  ok = GrpcQueryPortFromCraned(pamh, uid, server_address, kCranedDefaultPort,
                               port, &task_id, &cgroup_path);

  if (ok) {
    pam_syslog(pamh, LOG_ERR,
               "[Crane] Accepted ssh connection with remote port %hu ", port);

    char *auth_result = strdup(PAM_STR_TRUE);
    char *task_id_str = strdup(std::to_string(task_id).c_str());
    char *cgroup_path_str = strdup(cgroup_path.c_str());

    pam_set_data(pamh, PAM_ITEM_AUTH_RESULT, auth_result, clean_up_cb);
    pam_set_data(pamh, PAM_ITEM_TASK_ID, task_id_str, clean_up_cb);
    pam_set_data(pamh, PAM_ITEM_CG_PATH, cgroup_path_str, clean_up_cb);

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

int pam_sm_open_session(pam_handle_t *pamh, int flags, int argc,
                        const char **argv) {
  int task_id;
  char *auth_result;
  char *task_id_str;
  char *cgroup_path_str;

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

  pam_get_data(pamh, PAM_ITEM_AUTH_RESULT, (const void **)&auth_result);
  if (strcmp(auth_result, PAM_STR_TRUE) == 0) {
    pam_get_data(pamh, PAM_ITEM_TASK_ID, (const void **)&task_id_str);
    pam_get_data(pamh, PAM_ITEM_CG_PATH, (const void **)&cgroup_path_str);

    pam_syslog(pamh, LOG_ERR,
               "[Crane] open_session retrieved task_id: %s, cg_path: %s",
               task_id_str, cgroup_path_str);

    task_id = std::atoi(task_id_str);

    ok = GrpcMigrateSshProcToCgroup(pamh, getpid(), cgroup_path_str);
    if (ok)
      return PAM_SUCCESS;
    else
      return PAM_SESSION_ERR;
  } else {
    // If auth result is false, it indicates that system administrator allow a
    // user with no task running to log im, and then we just let it pass.
    return PAM_SUCCESS;
  }
}

// This function is required once pam_sm_open_session is written.
int pam_sm_close_session(pam_handle_t *pamh, int flags, int argc,
                         const char **argv) {
  return PAM_IGNORE;
}
}