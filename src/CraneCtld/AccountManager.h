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

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#include "DbClient.h"
#include "crane/Lock.h"
#include "crane/Pointer.h"

namespace Ctld {

class AccountManager {
 public:
  using AccountMutexSharedPtr =
      util::ScopeConstSharedPtr<Account, util::rw_mutex>;
  using AccountMapMutexSharedPtr = util::ScopeConstSharedPtr<
      std::unordered_map<std::string, std::unique_ptr<Account>>,
      util::rw_mutex>;
  using UserMutexSharedPtr = util::ScopeConstSharedPtr<User, util::rw_mutex>;
  using UserMapMutexSharedPtr = util::ScopeConstSharedPtr<
      std::unordered_map<std::string, std::unique_ptr<User>>, util::rw_mutex>;
  using QosMutexSharedPtr = util::ScopeConstSharedPtr<Qos, util::rw_mutex>;
  using QosMapMutexSharedPtr = util::ScopeConstSharedPtr<
      std::unordered_map<std::string, std::unique_ptr<Qos>>, util::rw_mutex>;

  using SuccessOrErrCode = std::expected<bool, crane::grpc::ErrCode>;
  using CraneErrCode = crane::grpc::ErrCode;

  struct Result {
    bool ok{false};
    std::string reason;
  };

  AccountManager();

  ~AccountManager() = default;

  SuccessOrErrCode AddUser(uint32_t uid, User&& new_user);

  SuccessOrErrCode AddAccount(uint32_t uid, Account&& new_account);

  SuccessOrErrCode AddQos(uint32_t uid, const Qos& new_qos);

  SuccessOrErrCode DeleteUser(uint32_t uid, const std::string& name,
                              const std::string& account);

  SuccessOrErrCode DeleteAccount(uint32_t uid, const std::string& name);

  SuccessOrErrCode DeleteQos(uint32_t uid, const std::string& name);

  SuccessOrErrCode QueryUserInfo(uint32_t uid, const std::string& name,
                                 std::unordered_map<uid_t, User>* res_user_map);

  SuccessOrErrCode QueryAccountInfo(
      uint32_t uid, const std::string& name,
      std::unordered_map<std::string, Account>* res_account_map);

  SuccessOrErrCode QueryQosInfo(
      uint32_t uid, const std::string& name,
      std::unordered_map<std::string, Qos>* res_qos_map);

  UserMutexSharedPtr GetExistedUserInfo(const std::string& name);
  UserMapMutexSharedPtr GetAllUserInfo();

  AccountMutexSharedPtr GetExistedAccountInfo(const std::string& name);
  AccountMapMutexSharedPtr GetAllAccountInfo();

  QosMutexSharedPtr GetExistedQosInfo(const std::string& name);
  QosMapMutexSharedPtr GetAllQosInfo();

  /* ---------------------------------------------------------------------------
   * ModifyUser-related functions
   * ---------------------------------------------------------------------------
   */
  SuccessOrErrCode ModifyAdminLevel(const uint32_t uid, const std::string& name,
                                    const std::string& value);
  SuccessOrErrCode ModifyUserDefaultQos(uint32_t uid, const std::string& name,
                                        const std::string& partition,
                                        std::string account,
                                        const std::string& value, bool force);
  SuccessOrErrCode ModifyUserAllowedParition(
      const crane::grpc::OperatorType& operatorType, uint32_t uid,
      const std::string& name, std::string account, const std::string& value);
  SuccessOrErrCode ModifyUserAllowedQos(
      const crane::grpc::OperatorType& operatorType, uint32_t uid,
      const std::string& name, const std::string& partition,
      std::string account, const std::string& value, bool force);
  SuccessOrErrCode DeleteUserAllowedPartiton(uint32_t uid,
                                             const std::string& name,
                                             std::string account,
                                             const std::string& value);
  SuccessOrErrCode DeleteUserAllowedQos(uint32_t uid, const std::string& name,
                                        const std::string& partition,
                                        std::string account,
                                        const std::string& value, bool force);

  SuccessOrErrCode ModifyAccount(const crane::grpc::OperatorType& operatorType,
                                 const uint32_t uid, const std::string& name,
                                 const crane::grpc::ModifyField& modifyField,
                                 const std::string& value, bool force);

  SuccessOrErrCode ModifyQos(const uint32_t uid, const std::string& name,
                             const crane::grpc::ModifyField& modifyField,
                             const std::string& value);

  SuccessOrErrCode BlockAccount(uint32_t uid, const std::string& name,
                                bool block);

  SuccessOrErrCode BlockUser(uint32_t uid, const std::string& name,
                             const std::string& account, bool block);

  bool CheckUserPermissionToPartition(const std::string& name,
                                      const std::string& account,
                                      const std::string& partition);

  result::result<void, std::string> CheckEnableState(const std::string& account,
                                                     const std::string& user);

  result::result<void, std::string> CheckAndApplyQosLimitOnTask(
      const std::string& user, const std::string& account, TaskInCtld* task);

  result::result<void, std::string> CheckUidIsAdmin(uint32_t uid);

  /* ---------------------------------------------------------------------------
   * ModifyUser-related functions(no block)
   * ---------------------------------------------------------------------------
   */
  SuccessOrErrCode CheckAddUserAllowedPartition(const User* user,
                                                const Account* account_ptr,
                                                const std::string& account,
                                                const std::string& partition);
  SuccessOrErrCode CheckSetUserAllowedPartition(const User* user,
                                                const Account* account_ptr,
                                                const std::string& account,
                                                const std::string& partition);
  SuccessOrErrCode CheckAddUserAllowedQos(const User* user,
                                          const Account* account_ptr,
                                          const std::string& account,
                                          const std::string& partition,
                                          const std::string& qos_str);
  SuccessOrErrCode CheckSetUserAllowedQos(
      const User* user, const Account* account_ptr, const std::string& account,
      const std::string& partition, const std::string& qos_str, bool force);
  SuccessOrErrCode CheckSetUserAdminLevel(const User& user,
                                          const std::string& level,
                                          User::AdminLevel* new_level);
  SuccessOrErrCode CheckSetUserDefaultQos(const User& user,
                                          const std::string& account,
                                          const std::string& partition,
                                          const std::string& qos);
  SuccessOrErrCode CheckDeleteUserAllowedPartition(
      const User& user, const std::string& account,
      const std::string& partition);
  SuccessOrErrCode CheckDeleteUserAllowedQos(const User& user,
                                             const std::string& account,
                                             const std::string& partition,
                                             const std::string& qos,
                                             bool force);

  /* ---------------------------------------------------------------------------
   * ModifyAccount-related functions(no block)
   * ---------------------------------------------------------------------------
   */
  SuccessOrErrCode CheckAddAccountAllowedPartition(
      const Account* account_ptr, const std::string& account,
      const std::string& partition);
  SuccessOrErrCode CheckAddAccountAllowedQos(const Account* account_ptr,
                                             const std::string& account,
                                             const std::string& qos);
  SuccessOrErrCode CheckSetAccountDescription(const Account* account_ptr,
                                              const std::string& account,
                                              const std::string& description);
  SuccessOrErrCode CheckSetAccountAllowedPartition(
      const Account* account_ptr, const std::string& account,
      const std::string& partitions, bool force);
  SuccessOrErrCode CheckSetAccountAllowedQos(const Account* account_ptr,
                                             const std::string& account,
                                             const std::string& qos_list,
                                             bool force);
  SuccessOrErrCode CheckSetAccountDefaultQos(const Account* account_ptr,
                                             const std::string& account,
                                             const std::string& qos);
  SuccessOrErrCode CheckDeleteAccountAllowedPartition(
      const Account* account_ptr, const std::string& account,
      const std::string& partition, bool force);
  SuccessOrErrCode CheckDeleteAccountAllowedQos(const Account* account_ptr,
                                                const std::string& account,
                                                const std::string& qos,
                                                bool force);

  /*
   * Check if the operating user exists
   */
  SuccessOrErrCode CheckOpUserExisted(uint32_t uid, const User** op_user);

  SuccessOrErrCode CheckOpUserIsAdmin(uint32_t uid);

  SuccessOrErrCode CheckOperatorPrivilegeHigher(uint32_t uid,
                                                User::AdminLevel admin_level);

  SuccessOrErrCode CheckOpUserHasPermissionToAccount(uint32_t uid,
                                                     const std::string& account,
                                                     bool read_only_priv,
                                                     bool is_add);

  SuccessOrErrCode CheckOpUserHasModifyPermission(uint32_t uid,
                                                  const User* user,
                                                  const std::string& name,
                                                  std::string& account,
                                                  bool read_only_priv);

  /*
   * Check if the operating user is the coordinator of the target user's
   * specified account.
   */
  SuccessOrErrCode CheckUserPermissionOnAccount(const User& op_user,
                                                const std::string& account,
                                                bool read_only_priv);

  /**
   * Check whether the operating user has permissions to access the target user.
   * Permissions are granted if any of the following three conditions are met:
   * 1. The operating user is the same as the target user.
   * 2. The operating user's level is higher than the target user's level.
   * 3. The operating user is the coordinator of the target user's specified
   * account. If the read_only_priv is true, it means the operating user is the
   * coordinator of any of the target user's accounts."
   */
  SuccessOrErrCode CheckUserPermissionOnUser(const User& op_user,
                                             const User* user,
                                             const std::string& name,
                                             std::string& account,
                                             bool read_only_priv);

  SuccessOrErrCode CheckPartitionIsAllowed(const Account* account_ptr,
                                           const std::string& account,
                                           const std::string& partition,
                                           bool check_parent);

  SuccessOrErrCode CheckQosIsAllowed(const Account* account_ptr,
                                     const std::string& account,
                                     const std::string& qos_str,
                                     bool check_parent);

  bool IsOperatorPrivilegeSameAndHigher(const User& op_user,
                                        User::AdminLevel admin_level);

  /**
   * @param[in] uid is system uid of user.
   * @param[in] account is the target that uid wants to query or modify.
   * If its value is an empty string, the permission check fails of course,
   * but level_of_uid will be filled and thus the function serves as a
   * query function for user level.
   * @param[in] read_only_priv specifies the permission type.
   * If true, the function checks if uid has read/query only permission
   * to specified account and the check will pass
   * if the list of accounts the uid belongs to,
   * which includes all the accounts this uid coordinates
   * (guaranteed by account rule),
   * contains any account which is is the parent of target account
   * (including itself).
   * If false, the function checks if uid has read/query only permission
   * to specified account and the check will pass
   * if the list of accounts coordinated by uid,
   * contains any account which is is the parent of target account
   * (including itself).
   * @param[out] level_of_uid will be written with the user level of uid
   * when function returns if both uid and user information exist.
   * @return True if both uid and corresponding user exists and the permission
   * check is passed, otherwise False.
   */
  AccountManager::Result HasPermissionToAccount(
      uint32_t uid, const std::string& account, bool read_only_priv,
      User::AdminLevel* level_of_uid = nullptr);

  bool HasPermissionToUser(uint32_t uid, const std::string& target_user,
                           bool read_only_priv,
                           User::AdminLevel* level_of_uid = nullptr);

 private:
  void InitDataMap_();

  const User* GetUserInfoNoLock_(const std::string& name);
  const User* GetExistedUserInfoNoLock_(const std::string& name);

  const Account* GetAccountInfoNoLock_(const std::string& name);
  const Account* GetExistedAccountInfoNoLock_(const std::string& name);

  const Qos* GetQosInfoNoLock_(const std::string& name);
  const Qos* GetExistedQosInfoNoLock_(const std::string& name);

  bool IncQosReferenceCountInDb_(const std::string& name, int num);

  SuccessOrErrCode AddUser_(const User* find_user, const Account* find_account,
                            User&& new_user);

  SuccessOrErrCode AddAccount_(const Account* find_account,
                               const Account* find_parent,
                               Account&& new_account);

  SuccessOrErrCode AddQos_(const Qos* find_qos, const Qos& new_qos);

  SuccessOrErrCode DeleteUser_(const User& user, const std::string& account);

  SuccessOrErrCode DeleteAccount_(const Account& account);

  SuccessOrErrCode DeleteQos_(const std::string& name);

  SuccessOrErrCode AddUserAllowedPartition_(const User& user,
                                            const Account& account,
                                            const std::string& partition);
  SuccessOrErrCode AddUserAllowedQos_(const User& user, const Account& account,
                                      const std::string& partition,
                                      const std::string& qos);

  SuccessOrErrCode SetUserAdminLevel_(const std::string& name,
                                      const User::AdminLevel& new_level);
  SuccessOrErrCode SetUserDefaultQos_(const User& user,
                                      const std::string& account,
                                      const std::string& partition,
                                      const std::string& qos);
  SuccessOrErrCode SetUserAllowedPartition_(const User& user,
                                            const Account& account,
                                            const std::string& partitions);
  SuccessOrErrCode SetUserAllowedQos_(const User& user, const Account& account,
                                      const std::string& partition,
                                      const std::string& qos_list_str,
                                      bool force);

  SuccessOrErrCode DeleteUserAllowedPartition_(const User& user,
                                               const std::string& account,
                                               const std::string& partition);
  SuccessOrErrCode DeleteUserAllowedQos_(const User& user,
                                         const std::string& qos,
                                         const std::string& account,
                                         const std::string& partition,
                                         bool force);

  SuccessOrErrCode AddAccountAllowedPartition_(const std::string& name,
                                               const std::string& partition);
  SuccessOrErrCode AddAccountAllowedQos_(const Account& account,
                                         const std::string& qos);

  SuccessOrErrCode SetAccountDescription_(const std::string& name,
                                          const std::string& description);
  SuccessOrErrCode SetAccountDefaultQos_(const Account& account,
                                         const std::string& qos);
  SuccessOrErrCode SetAccountAllowedPartition_(const Account& account,
                                               const std::string& partitions,
                                               bool force);
  SuccessOrErrCode SetAccountAllowedQos_(const Account& account,
                                         const std::string& qos_list_str,
                                         bool force);

  SuccessOrErrCode DeleteAccountAllowedPartition_(const Account& account,
                                                  const std::string& partition,
                                                  bool force);
  SuccessOrErrCode DeleteAccountAllowedQos_(const Account& account,
                                            const std::string& qos, bool force);

  SuccessOrErrCode BlockUser_(const std::string& name,
                              const std::string& account, bool block);

  SuccessOrErrCode BlockAccount_(const std::string& name, bool block);

  bool IsAllowedPartitionOfAnyNodeNoLock_(const Account* account,
                                          const std::string& partition,
                                          int depth = 0);

  bool IsDefaultQosOfAnyNodeNoLock_(const Account* account,
                                    const std::string& qos);
  bool IsDefaultQosOfAnyPartitionNoLock_(const User* user,
                                         const std::string& qos);

  int DeleteAccountAllowedQosFromDBNoLock_(const std::string& name,
                                           const std::string& qos);
  bool DeleteAccountAllowedQosFromMapNoLock_(const std::string& name,
                                             const std::string& qos);
  bool DeleteUserAllowedQosOfAllPartitionFromDBNoLock_(
      const std::string& name, const std::string& account,
      const std::string& qos);
  bool DeleteUserAllowedQosOfAllPartitionFromMapNoLock_(
      const std::string& name, const std::string& account,
      const std::string& qos);

  bool DeleteAccountAllowedPartitionFromDBNoLock_(const std::string& name,
                                                  const std::string& partition);
  bool DeleteAccountAllowedPartitionFromMapNoLock_(
      const std::string& name, const std::string& partition);

  bool PaternityTestNoLock_(const std::string& parent,
                            const std::string& child);
  bool PaternityTestNoLockDFS_(const std::string& parent,
                               const std::string& child);

  std::unordered_map<std::string /*account name*/, std::unique_ptr<Account>>
      m_account_map_;
  util::rw_mutex m_rw_account_mutex_;
  std::unordered_map<std::string /*user name*/, std::unique_ptr<User>>
      m_user_map_;
  util::rw_mutex m_rw_user_mutex_;
  std::unordered_map<std::string /*Qos name*/, std::unique_ptr<Qos>> m_qos_map_;
  util::rw_mutex m_rw_qos_mutex_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::AccountManager> g_account_manager;