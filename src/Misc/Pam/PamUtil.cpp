#include "PamUtil.h"

#include <grpc++/grpc++.h>
#include <pwd.h>
#include <fmt/format.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <unordered_map>

#include "crane/PublicHeader.h"
#include "protos/Crane.grpc.pb.h"

bool PamGetUserName(pam_handle_t *pamh, std::string *username) {
  int rc;
  char *p_username = nullptr;
  rc = pam_get_item(pamh, PAM_USER, (const void **)&p_username);
  if (p_username == nullptr || rc != PAM_SUCCESS) {
    pam_syslog(pamh, LOG_ERR, "[Crane] No username in PAM_USER? Fail!");
    return false;
  } else {
    username->assign(p_username);
    return true;
  }
}

void PamSendMsgToClient(pam_handle_t *pamh, const char *mesg) {
  int rc;
  struct pam_conv *conv;
  void *dummy; /* needed to eliminate warning
                * dereferencing type-punned pointer will
                * break strict-aliasing rules */
  struct pam_message msg[1];
  const struct pam_message *pmsg[1];
  struct pam_response *prsp;

  // Get conversation function to talk with app.
  rc = pam_get_item(pamh, PAM_CONV, (const void **)&dummy);
  conv = (struct pam_conv *)dummy;
  if (rc != PAM_SUCCESS) {
    pam_syslog(pamh, LOG_ERR, "unable to get pam_conv: %s",
               pam_strerror(pamh, rc));
    return;
  }

  // Construct msg to send to app.
  msg[0].msg_style = PAM_ERROR_MSG;
  msg[0].msg = mesg;
  pmsg[0] = &msg[0];
  prsp = nullptr;

  /*  Send msg to app and free the (meaningless) rsp.
   */
  rc = conv->conv(1, pmsg, &prsp, conv->appdata_ptr);
  if (rc != PAM_SUCCESS)
    pam_syslog(pamh, LOG_ERR, "unable to converse with app: %s",
               pam_strerror(pamh, rc));
  if (prsp != nullptr) _pam_drop_reply(prsp, 1);
}

bool PamGetRemoteUid(pam_handle_t *pamh, const char *user_name, uid_t *uid) {
  size_t buf_size;
  char *buf;
  struct passwd pwd, *pwd_result;
  int rc;

  /* Calculate buffer size for getpwnam_r */
  buf_size = sysconf(_SC_GETPW_R_SIZE_MAX);
  if (buf_size == -1) buf_size = 16384; /* take a large guess */

  buf = new char[buf_size];
  errno = 0;
  rc = getpwnam_r(user_name, &pwd, buf, buf_size, &pwd_result);
  if (pwd_result == nullptr) {
    if (rc == 0) {
      pam_syslog(pamh, LOG_ERR, "[Crane] getpwnam_r could not locate user %s",
                 user_name);
    } else {
      pam_syslog(pamh, LOG_ERR, "[Crane] getpwnam_r: %s", strerror(errno));
    }

    delete buf;
    return false;
  }
  *uid = pwd.pw_uid;
  delete buf;

  return true;
}

bool PamGetRemoteAddressPort(pam_handle_t *pamh, uint8_t addr[4],
                             uint16_t *port) {
  std::ifstream tcp_file("/proc/net/tcp");
  std::string line;
  if (!tcp_file) {
    pam_syslog(pamh, LOG_ERR, "[Crane] Failed to open /proc/net/tcp");
    return PAM_PERM_DENIED;
  }

  pam_syslog(pamh, LOG_ERR, "[Crane] /proc/net/tcp opened.");

  std::unordered_map<ino_t, std::string> inode_addr_port_map;

  pam_syslog(pamh, LOG_ERR, "[Crane] inode_addr_port_map inited.");

  std::getline(tcp_file, line);
  while (std::getline(tcp_file, line)) {
    boost::trim(line);
    std::vector<std::string> line_vec;
    boost::split(line_vec, line, boost::is_any_of(" "),
                 boost::token_compress_on);

    // 2nd row is remote address and 9th row is inode num.
    pam_syslog(pamh, LOG_ERR, "[Crane] TCP conn %s %s, inode: %s",
               line_vec[0].c_str(), line_vec[2].c_str(), line_vec[9].c_str());
    ino_t inode_num = std::stoul(line_vec[9]);
    inode_addr_port_map.emplace(inode_num, line_vec[2]);
  }

#ifndef NDEBUG
  std::string output;
  for (auto &&[k, v] : inode_addr_port_map) {
    output += fmt::format("{}:{} ", k, v);
  }

  pam_syslog(pamh, LOG_ERR, "[Crane] inode_addr_port_map: %s", output.c_str());
#endif

  std::string fds_path = "/proc/self/fd";
  for (const auto &entry : std::filesystem::directory_iterator(fds_path)) {
    // entry must have call stat() once in its implementation.
    // So entry.is_socket() points to the real file.
    if (entry.is_socket()) {
      pam_syslog(pamh, LOG_ERR, "[Crane] Checking socket fd %s",
                 entry.path().c_str());
      struct stat stat_buf {};
      // stat() will resolve symbol link.
      if (stat(entry.path().c_str(), &stat_buf) != 0) {
        pam_syslog(pamh, LOG_ERR, "[Crane] stat failed for socket fd %s",
                   entry.path().c_str());
        continue;
      } else {
        pam_syslog(pamh, LOG_ERR, "[Crane] inode num for socket fd %s is %lu",
                   entry.path().c_str(), stat_buf.st_ino);
      }

      auto iter = inode_addr_port_map.find(stat_buf.st_ino);
      if (iter == inode_addr_port_map.end()) {
        pam_syslog(pamh, LOG_ERR,
                   "[Crane] inode num %lu not found in /proc/net/tcp",
                   stat_buf.st_ino);
      } else {
        std::vector<std::string> addr_port_hex;
        boost::split(addr_port_hex, iter->second, boost::is_any_of(":"));

        const std::string &addr_hex = addr_port_hex[0];
        const std::string &port_hex = addr_port_hex[1];
        pam_syslog(pamh, LOG_ERR,
                   "[Crane] hex addr and port for inode num %lu: %s:%s",
                   stat_buf.st_ino, addr_hex.c_str(), port_hex.c_str());

        for (int i = 0; i < 4; i++) {
          std::string addr_part = addr_hex.substr(6 - 2 * i, 2);
          addr[i] = std::stoul(addr_part, nullptr, 16);
          pam_syslog(pamh, LOG_ERR,
                     "[Crane] Transform %d part of hex addr: %s to int %hhu", i,
                     addr_part.c_str(), addr[i]);
        }

        *port = std::stoul(port_hex, nullptr, 16);

        pam_syslog(pamh, LOG_ERR,
                   "[Crane] inode num %lu found in /proc/net/tcp: "
                   "%hhu.%hhu.%hhu.%hhu:%hu",
                   stat_buf.st_ino, addr[0], addr[1], addr[2], addr[3], *port);

        return true;
      }
    }
  }

  return false;
}

bool GrpcQueryPortFromCraned(pam_handle_t *pamh, uid_t uid,
                             const std::string &craned_address,
                             const std::string &craned_port,
                             uint16_t port_to_query, uint32_t *task_id,
                             std::string *cgroup_path) {
  using grpc::Channel;
  using grpc::ClientContext;
  using grpc::Status;

  std::string craned_unix_socket_address =
      fmt::format("unix://{}", kDefaultCranedUnixSockPath);

  std::shared_ptr<Channel> channel = grpc::CreateChannel(
      craned_unix_socket_address, grpc::InsecureChannelCredentials());

  pam_syslog(pamh, LOG_ERR, "[Crane] Channel to %s created",
             craned_unix_socket_address.c_str());

  //  bool connected =
  //  channel->WaitForConnected(std::chrono::system_clock::now() +
  //                                             std::chrono::milliseconds(500));
  //  if (!connected) {
  //    pam_syslog(pamh, LOG_ERR, "Failed to establish the channel to %s",
  //               craned_unix_socket_address.c_str());
  //    return false;
  //  }

  std::unique_ptr<crane::grpc::Craned::Stub> stub =
      crane::grpc::Craned::NewStub(channel);

  if (!stub) {
    pam_syslog(pamh, LOG_ERR, "[Crane] Failed to create Stub to %s",
               craned_unix_socket_address.c_str());
    return false;
  }

  crane::grpc::QueryTaskIdFromPortForwardRequest request;
  crane::grpc::QueryTaskIdFromPortForwardReply reply;
  ClientContext context;
  Status status;

  request.set_target_craned_address(craned_address);
  request.set_target_craned_port(craned_port);
  request.set_ssh_remote_port(port_to_query);
  request.set_uid(uid);

  status = stub->QueryTaskIdFromPortForward(&context, request, &reply);
  if (!status.ok()) {
    pam_syslog(pamh, LOG_ERR, "QueryTaskIdFromPort gRPC call failed: %s | %s",
               status.error_message().c_str(), status.error_details().c_str());
    return false;
  }

  if (reply.ok()) {
    pam_syslog(pamh, LOG_ERR,
               "ssh client with remote port %u belongs to task #%u",
               port_to_query, reply.task_id());
    *task_id = reply.task_id();
    *cgroup_path = reply.cgroup_path();
    return true;
  } else {
    pam_syslog(pamh, LOG_ERR,
               "ssh client with remote port %u doesn't belong to any task",
               port_to_query);
    return false;
  }
}

bool GrpcMigrateSshProcToCgroup(pam_handle_t *pamh, pid_t pid,
                                const char *cgroup_path) {
  using grpc::Channel;
  using grpc::ClientContext;
  using grpc::Status;

  std::string craned_unix_socket_address =
      fmt::format("unix://{}", kDefaultCranedUnixSockPath);

  std::shared_ptr<Channel> channel = grpc::CreateChannel(
      craned_unix_socket_address, grpc::InsecureChannelCredentials());

  pam_syslog(pamh, LOG_ERR, "[Crane] Channel to %s created",
             craned_unix_socket_address.c_str());

  std::unique_ptr<crane::grpc::Craned::Stub> stub =
      crane::grpc::Craned::NewStub(channel);

  if (!stub) {
    pam_syslog(pamh, LOG_ERR, "[Crane] Failed to create Stub to %s",
               craned_unix_socket_address.c_str());
    return false;
  }

  crane::grpc::MigrateSshProcToCgroupRequest request;
  crane::grpc::MigrateSshProcToCgroupReply reply;
  ClientContext context;
  Status status;

  request.set_pid(pid);
  request.set_cgroup_path(cgroup_path);

  status = stub->MigrateSshProcToCgroup(&context, request, &reply);
  if (!status.ok()) {
    pam_syslog(pamh, LOG_ERR,
               "[Crane] GrpcMigrateSshProcToCgroup gRPC call failed: %s | %s",
               status.error_message().c_str(), status.error_details().c_str());
    return false;
  }

  if (reply.ok()) {
    pam_syslog(pamh, LOG_ERR, "[Crane] GrpcMigrateSshProcToCgroup succeeded.");
    return true;
  } else {
    pam_syslog(pamh, LOG_ERR, "[Crane] GrpcMigrateSshProcToCgroup failed.");
    return false;
  }
}
