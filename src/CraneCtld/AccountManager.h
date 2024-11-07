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
#include "range/v3/view/unique.hpp"

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

  using CraneErrCode = crane::grpc::ErrCode;

  template <typename T>
  using CraneExpected = std::expected<T, CraneErrCode>;

  struct Result {
    bool ok{false};
    std::string reason;
  };

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
  CraneExpected<void> ModifyAdminLevel(const uint32_t uid,
                                       const std::string& name,
                                       const std::string& value);
  CraneExpected<void> ModifyUserDefaultQos(uint32_t uid,
                                           const std::string& name,
                                           const std::string& partition,
                                           std::string account,
                                           const std::string& value);
  CraneExpected<void> ModifyUserAllowedPartition(
      crane::grpc::OperatorType operator_type, uint32_t uid,
      const std::string& name, std::string account, const std::string& value);
  CraneExpected<void> ModifyUserAllowedQos(
      crane::grpc::OperatorType operator_type, uint32_t uid,
      const std::string& name, const std::string& partition,
      std::string account, const std::string& value, bool force);
  CraneExpected<void> DeleteUserAllowedPartition(uint32_t uid,
                                                 const std::string& name,
                                                 const std::string& account,
                                                 const std::string& value);
  CraneExpected<void> DeleteUserAllowedQos(
      uint32_t uid, const std::string& name, const std::string& partition,
      std::string account, const std::string& value, bool force);

  CraneExpected<void> ModifyAccount(crane::grpc::OperatorType operator_type,
                                    const uint32_t uid, const std::string& name,
                                    crane::grpc::ModifyField modify_field,
                                    const std::string& value, bool force);

  CraneExpected<void> ModifyQos(const uint32_t uid, const std::string& name,
                                crane::grpc::ModifyField modify_field,
                                const std::string& value);

  CraneExpected<void> BlockAccount(uint32_t uid, const std::string& name,
                                   bool block);

  CraneExpected<void> BlockUser(uint32_t uid, const std::string& name,
                                std::string account, bool block);

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
  CraneExpected<void> CheckAddUserAllowedPartition(
      const User* user, const Account* account, const std::string& partition);
  CraneExpected<void> CheckSetUserAllowedPartition(
      const Account* account, const std::string& partition);
  CraneExpected<void> CheckAddUserAllowedQos(const User* user,
                                             const Account* account,
                                             const std::string& partition,
                                             const std::string& qos_str);
  CraneExpected<void> CheckSetUserAllowedQos(const User* user,
                                             const Account* account,
                                             const std::string& partition,
                                             const std::string& qos_str,
                                             bool force);
  CraneExpected<void> CheckSetUserAdminLevel(const std::string& level,
                                             User::AdminLevel* new_level);
  CraneExpected<void> CheckSetUserDefaultQos(const User& user,
                                             const std::string& account,
                                             const std::string& partition,
                                             const std::string& qos);
  CraneExpected<void> CheckDeleteUserAllowedPartition(
      const User& user, const std::string& account,
      const std::string& partition);
  CraneExpected<void> CheckDeleteUserAllowedQos(const User& user,
                                                const std::string& account,
                                                const std::string& partition,
                                                const std::string& qos,
                                                bool force);

  /* ---------------------------------------------------------------------------
   * ModifyAccount-related functions(no block)
   * ---------------------------------------------------------------------------
   */
  CraneExpected<void> CheckAddAccountAllowedPartition(
      const Account* account, const std::string& partition);
  CraneExpected<void> CheckAddAccountAllowedQos(const Account* account,
                                                const std::string& qos);
  CraneExpected<void> CheckSetAccountDescription(const Account* account);
  CraneExpected<void> CheckSetAccountAllowedPartition(
      const Account* account, const std::string& partitions, bool force);
  CraneExpected<void> CheckSetAccountAllowedQos(const Account* account,
                                                const std::string& qos_list,
                                                bool force);
  CraneExpected<void> CheckSetAccountDefaultQos(const Account* account,
                                                const std::string& qos);
  CraneExpected<void> CheckDeleteAccountAllowedPartition(
      const Account* account, const std::string& partition, bool force);
  CraneExpected<void> CheckDeleteAccountAllowedQos(const Account* account,
                                                   const std::string& qos,
                                                   bool force);

  /*
   * Check if the operating user exists
   */
  CraneExpected<const User*> GetUserInfoByUid(uint32_t uid);

  CraneExpected<void> CheckOpUserIsAdmin(uint32_t uid);

  CraneExpected<void> CheckOperatorPrivilegeHigher(
      uint32_t uid, User::AdminLevel admin_level);

  CraneExpected<void> CheckOpUserHasPermissionToAccount(
      uint32_t uid, const std::string& account, bool read_only_priv,
      bool is_add);

  CraneExpected<void> CheckOpUserHasModifyPermission(uint32_t uid,
                                                     const User* user,
                                                     std::string* account,
                                                     bool read_only_priv);

  /*
   * Check if the operating user is the coordinator of the target user's
   * specified account.
   */
  CraneExpected<void> CheckUserPermissionOnAccount(const User& op_user,
                                                   const std::string& account,
                                                   bool read_only_priv);

  /**
   * Check whether the operating user has permissions to access the target user.
   * Permissions are granted if any of the following three conditions are met:
   * 1. The operating user is the same as the target user.
   * 2. The operating user's level is higher than the target user's level.
   * 3. The operating user is the coordinator of the target user's specified
   * account. If the read_only_priv is true, it means the operating user is the
   * coordinator of any target user's account.
   */
  CraneExpected<void> CheckUserPermissionOnUser(const User& op_user,
                                                const User* user,
                                                bool read_only_priv,
                                                std::string* account);

  CraneExpected<void> CheckPartitionIsAllowed(const Account* account,
                                              const std::string& partition,
                                              bool check_parent, bool is_user);

  CraneExpected<void> CheckQosIsAllowed(const Account* account,
                                        const std::string& qos_str,
                                        bool check_parent, bool is_user);

  bool IsOperatorPrivilegeSameOrHigher(const User& op_user,
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