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

  template <typename T>
  using CraneExpected = std::expected<T, CraneErrCode>;

  AccountManager();

  ~AccountManager() = default;

  CraneExpected<void> AddUser(uint32_t uid, const User& new_user);

  CraneExpected<void> AddAccount(uint32_t uid, const Account& new_account);

  CraneExpected<void> AddQos(uint32_t uid, const Qos& new_qos);

  CraneExpected<void> DeleteUser(uint32_t uid, const std::string& name,
                                 const std::string& account);

  CraneExpected<void> DeleteAccount(uint32_t uid, const std::string& name);

  CraneExpected<void> DeleteQos(uint32_t uid, const std::string& name);

  CraneExpected<void> QueryUserInfo(
      uint32_t uid, const std::string& name,
      std::unordered_map<uid_t, User>* res_user_map);

  CraneExpected<void> QueryAccountInfo(
      uint32_t uid, const std::string& name,
      std::unordered_map<std::string, Account>* res_account_map);

  CraneExpected<void> QueryQosInfo(
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
  CraneExpected<void> ModifyAdminLevel(uint32_t uid, const std::string& name,
                                       const std::string& value);
  CraneExpected<void> ModifyUserDefaultQos(uint32_t uid,
                                           const std::string& name,
                                           const std::string& partition,
                                           const std::string& account,
                                           const std::string& value);
  CraneExpected<void> ModifyUserAllowedPartition(
      crane::grpc::OperationType operation_type, uint32_t uid,
      const std::string& name, const std::string& account,
      const std::string& value);
  CraneExpected<void> ModifyUserAllowedQos(
      crane::grpc::OperationType operation_type, uint32_t uid,
      const std::string& name, const std::string& partition,
      const std::string& account, const std::string& value, bool force);
  CraneExpected<void> DeleteUserAllowedPartition(uint32_t uid,
                                                 const std::string& name,
                                                 const std::string& account,
                                                 const std::string& value);
  CraneExpected<void> DeleteUserAllowedQos(
      uint32_t uid, const std::string& name, const std::string& partition,
      const std::string& account, const std::string& value, bool force);

  CraneExpected<void> ModifyAccount(crane::grpc::OperationType operation_type,
                                    uint32_t uid, const std::string& name,
                                    crane::grpc::ModifyField modify_field,
                                    const std::string& value, bool force);

  CraneExpected<void> ModifyQos(uint32_t uid, const std::string& name,
                                crane::grpc::ModifyField modify_field,
                                const std::string& value);

  CraneExpected<void> BlockAccount(uint32_t uid, const std::string& name,
                                   bool block);

  CraneExpected<void> BlockUser(uint32_t uid, const std::string& name,
                                const std::string& account, bool block);

  bool CheckUserPermissionToPartition(const std::string& name,
                                      const std::string& account,
                                      const std::string& partition);

  std::expected<void, std::string> CheckIfUserOfAccountIsEnabled(
      const std::string& user, const std::string& account);

  std::expected<void, std::string> CheckAndApplyQosLimitOnTask(
      const std::string& user, const std::string& account, TaskInCtld* task);

  std::expected<void, std::string> CheckUidIsAdmin(uint32_t uid);

  CraneExpected<void> CheckIfUidHasPermOnUser(uint32_t uid,
                                              const std::string& username,
                                              bool read_only_priv);

  CraneErrCodeExpected<void> CheckModifyPartitionAllowAccounts(
      uint32_t uid, const std::string& partition_name,
      const std::unordered_set<std::string>& allow_accounts);

 private:
  void InitDataMap_();

  CraneExpected<const User*> GetUserInfoByUidNoLock_(uint32_t uid);

  const User* GetUserInfoNoLock_(const std::string& name);
  const User* GetExistedUserInfoNoLock_(const std::string& name);

  const Account* GetAccountInfoNoLock_(const std::string& name);
  const Account* GetExistedAccountInfoNoLock_(const std::string& name);

  const Qos* GetQosInfoNoLock_(const std::string& name);
  const Qos* GetExistedQosInfoNoLock_(const std::string& name);

  /* ---------------------------------------------------------------------------
   * ModifyUser-related functions(no lock)
   * ---------------------------------------------------------------------------
   */
  CraneExpected<void> CheckAddUserAllowedPartitionNoLock_(
      const User* user, const Account* account, const std::string& partition);
  CraneExpected<void> CheckSetUserAllowedPartitionNoLock_(
      const Account* account, const std::string& partition);
  CraneExpected<void> CheckAddUserAllowedQosNoLock_(
      const User* user, const Account* account, const std::string& partition,
      const std::string& qos_str);
  CraneExpected<void> CheckSetUserAllowedQosNoLock_(
      const User* user, const Account* account, const std::string& partition,
      const std::string& qos_str, bool force);
  CraneExpected<void> CheckSetUserDefaultQosNoLock_(
      const User& user, const std::string& account,
      const std::string& partition, const std::string& qos);
  CraneExpected<void> CheckDeleteUserAllowedPartitionNoLock_(
      const User& user, const std::string& account,
      const std::string& partition);
  CraneExpected<void> CheckDeleteUserAllowedQosNoLock_(
      const User& user, const std::string& account,
      const std::string& partition, const std::string& qos, bool force);

  /* ---------------------------------------------------------------------------
   * ModifyAccount-related functions(no lock)
   * ---------------------------------------------------------------------------
   */
  CraneExpected<void> CheckAddAccountAllowedPartitionNoLock_(
      const Account* account, const std::string& partition);
  CraneExpected<void> CheckAddAccountAllowedQosNoLock_(const Account* account,
                                                       const std::string& qos);
  CraneExpected<void> CheckSetAccountDescriptionNoLock_(const Account* account);
  CraneExpected<void> CheckSetAccountAllowedPartitionNoLock_(
      const Account* account, const std::string& partitions, bool force);
  CraneExpected<void> CheckSetAccountAllowedQosNoLock_(
      const Account* account, const std::string& qos_list, bool force);
  CraneExpected<void> CheckSetAccountDefaultQosNoLock_(const Account* account,
                                                       const std::string& qos);
  CraneExpected<void> CheckDeleteAccountAllowedPartitionNoLock_(
      const Account* account, const std::string& partition, bool force);
  CraneExpected<void> CheckDeleteAccountAllowedQosNoLock_(
      const Account* account, const std::string& qos, bool force);

  // Compare the user's permission levels for operations.
  CraneExpected<void> CheckIfUserHasHigherPrivThan_(
      const User& op_user, User::AdminLevel admin_level);

  // Determine if the operating user has permissions for the account,
  // e.g. admin or coordinator
  CraneExpected<void> CheckIfUserHasPermOnAccountNoLock_(
      const User& op_user, const std::string& account, bool read_only_priv);

  /**
   * Check whether the operating user has permissions to access the target user.
   * Permissions are granted if any of the following three conditions are met:
   * 1. The operating user is the same as the target user.
   * 2. The operating user's level is higher than the target user's level.
   * 3. The operating user is the coordinator of the target user's account.
   * If the read_only_priv is true, it means the operating user is the
   * coordinator of any target user's account.
   */
  CraneExpected<void> CheckIfUserHasPermOnUserNoLock_(const User& op_user,
                                                      const User* user,
                                                      bool read_only_priv);

  // Determine if the operating user has permissions for a specific account of a
  // particular user.
  // 1. The operating user's permissions are greater than the target user's.
  // 2. The operating user is the coordinator of the account.
  CraneExpected<void> CheckIfUserHasPermOnUserOfAccountNoLock_(
      const User& op_user, const User* user, std::string* account,
      bool read_only_priv);

  CraneExpected<void> CheckPartitionIsAllowedNoLock_(
      const Account* account, const std::string& partition, bool check_parent,
      bool is_user);

  CraneExpected<void> CheckQosIsAllowedNoLock_(const Account* account,
                                               const std::string& qos_str,
                                               bool check_parent, bool is_user);

  bool IncQosReferenceCountInDb_(const std::string& name, int num);

  CraneExpected<void> AddUser_(const User& user, const Account* account,
                               const User* stale_user);

  CraneExpected<void> AddAccount_(const Account& account, const Account* parent,
                                  const Account* stale_account);

  CraneExpected<void> AddQos_(const Qos& qos, const Qos* stale_qos);

  CraneExpected<void> DeleteUser_(const User& user, const std::string& account);

  CraneExpected<void> DeleteAccount_(const Account& account);

  CraneExpected<void> DeleteQos_(const std::string& name);

  CraneExpected<void> AddUserAllowedPartition_(const User& user,
                                               const Account& account,
                                               const std::string& partition);
  CraneExpected<void> AddUserAllowedQos_(const User& user,
                                         const Account& account,
                                         const std::string& partition,
                                         const std::string& qos);

  CraneExpected<void> SetUserAdminLevel_(const std::string& name,
                                         User::AdminLevel new_level);
  CraneExpected<void> SetUserDefaultQos_(const User& user,
                                         const std::string& account,
                                         const std::string& partition,
                                         const std::string& qos);
  CraneExpected<void> SetUserAllowedPartition_(const User& user,
                                               const Account& account,
                                               const std::string& partitions);
  CraneExpected<void> SetUserAllowedQos_(const User& user,
                                         const Account& account,
                                         const std::string& partition,
                                         const std::string& qos_list_str,
                                         bool force);

  CraneExpected<void> DeleteUserAllowedPartition_(const User& user,
                                                  const std::string& account,
                                                  const std::string& partition);
  CraneExpected<void> DeleteUserAllowedQos_(const User& user,
                                            const std::string& qos,
                                            const std::string& account,
                                            const std::string& partition,
                                            bool force);

  CraneExpected<void> AddAccountAllowedPartition_(const std::string& name,
                                                  const std::string& partition);
  CraneExpected<void> AddAccountAllowedQos_(const Account& account,
                                            const std::string& qos);

  CraneExpected<void> SetAccountDescription_(const std::string& name,
                                             const std::string& description);
  CraneExpected<void> SetAccountDefaultQos_(const Account& account,
                                            const std::string& qos);
  CraneExpected<void> SetAccountAllowedPartition_(
      const Account& account, const std::string& partitions);
  CraneExpected<void> SetAccountAllowedQos_(const Account& account,
                                            const std::string& qos_list_str);

  CraneExpected<void> DeleteAccountAllowedPartition_(
      const Account& account, const std::string& partition);
  CraneExpected<void> DeleteAccountAllowedQos_(const Account& account,
                                               const std::string& qos);

  CraneExpected<void> BlockUser_(const std::string& name,
                                 const std::string& account, bool block);

  CraneExpected<void> BlockAccount_(const std::string& name, bool block);

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