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

  CraneExpected<bool> AddUser(uint32_t uid, const User& new_user);

  CraneExpected<bool> AddAccount(uint32_t uid, const Account& new_account);

  CraneExpected<bool> AddQos(uint32_t uid, const Qos& new_qos);

  CraneExpected<bool> DeleteUser(uint32_t uid, const std::string& name,
                                 const std::string& account);

  CraneExpected<bool> DeleteAccount(uint32_t uid, const std::string& name);

  CraneExpected<bool> DeleteQos(uint32_t uid, const std::string& name);

  CraneExpected<bool> QueryUserInfo(
      uint32_t uid, const std::string& name,
      std::unordered_map<uid_t, User>* res_user_map);

  CraneExpected<bool> QueryAccountInfo(
      uint32_t uid, const std::string& name,
      std::unordered_map<std::string, Account>* res_account_map);

  CraneExpected<bool> QueryQosInfo(
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
  CraneExpected<bool> ModifyAdminLevel(const uint32_t uid,
                                       const std::string& name,
                                       const std::string& value);
  CraneExpected<bool> ModifyUserDefaultQos(
      uint32_t uid, const std::string& name, const std::string& partition,
      std::string account, const std::string& value, bool force);
  CraneExpected<bool> ModifyUserAllowedParition(
      const crane::grpc::OperatorType& operatorType, uint32_t uid,
      const std::string& name, std::string account, const std::string& value);
  CraneExpected<bool> ModifyUserAllowedQos(
      const crane::grpc::OperatorType& operatorType, uint32_t uid,
      const std::string& name, const std::string& partition,
      std::string account, const std::string& value, bool force);
  CraneExpected<bool> DeleteUserAllowedPartiton(uint32_t uid,
                                                const std::string& name,
                                                std::string account,
                                                const std::string& value);
  CraneExpected<bool> DeleteUserAllowedQos(
      uint32_t uid, const std::string& name, const std::string& partition,
      std::string account, const std::string& value, bool force);

  CraneExpected<bool> ModifyAccount(
      const crane::grpc::OperatorType& operatorType, const uint32_t uid,
      const std::string& name, const crane::grpc::ModifyField& modifyField,
      const std::string& value, bool force);

  CraneExpected<bool> ModifyQos(const uint32_t uid, const std::string& name,
                                const crane::grpc::ModifyField& modifyField,
                                const std::string& value);

  CraneExpected<bool> BlockAccount(uint32_t uid, const std::string& name,
                                   bool block);

  CraneExpected<bool> BlockUser(uint32_t uid, const std::string& name,
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
  CraneExpected<bool> CheckAddUserAllowedPartition(
      const User* user, const Account* account_ptr, const std::string& account,
      const std::string& partition);
  CraneExpected<bool> CheckSetUserAllowedPartition(
      const User* user, const Account* account_ptr, const std::string& account,
      const std::string& partition);
  CraneExpected<bool> CheckAddUserAllowedQos(const User* user,
                                             const Account* account_ptr,
                                             const std::string& account,
                                             const std::string& partition,
                                             const std::string& qos_str);
  CraneExpected<bool> CheckSetUserAllowedQos(
      const User* user, const Account* account_ptr, const std::string& account,
      const std::string& partition, const std::string& qos_str, bool force);
  CraneExpected<bool> CheckSetUserAdminLevel(const User& user,
                                             const std::string& level,
                                             User::AdminLevel* new_level);
  CraneExpected<bool> CheckSetUserDefaultQos(const User& user,
                                             const std::string& account,
                                             const std::string& partition,
                                             const std::string& qos);
  CraneExpected<bool> CheckDeleteUserAllowedPartition(
      const User& user, const std::string& account,
      const std::string& partition);
  CraneExpected<bool> CheckDeleteUserAllowedQos(const User& user,
                                                const std::string& account,
                                                const std::string& partition,
                                                const std::string& qos,
                                                bool force);

  /* ---------------------------------------------------------------------------
   * ModifyAccount-related functions(no block)
   * ---------------------------------------------------------------------------
   */
  CraneExpected<bool> CheckAddAccountAllowedPartition(
      const Account* account_ptr, const std::string& account,
      const std::string& partition);
  CraneExpected<bool> CheckAddAccountAllowedQos(const Account* account_ptr,
                                                const std::string& account,
                                                const std::string& qos);
  CraneExpected<bool> CheckSetAccountDescription(
      const Account* account_ptr, const std::string& account,
      const std::string& description);
  CraneExpected<bool> CheckSetAccountAllowedPartition(
      const Account* account_ptr, const std::string& account,
      const std::string& partitions, bool force);
  CraneExpected<bool> CheckSetAccountAllowedQos(const Account* account_ptr,
                                                const std::string& account,
                                                const std::string& qos_list,
                                                bool force);
  CraneExpected<bool> CheckSetAccountDefaultQos(const Account* account_ptr,
                                                const std::string& account,
                                                const std::string& qos);
  CraneExpected<bool> CheckDeleteAccountAllowedPartition(
      const Account* account_ptr, const std::string& account,
      const std::string& partition, bool force);
  CraneExpected<bool> CheckDeleteAccountAllowedQos(const Account* account_ptr,
                                                   const std::string& account,
                                                   const std::string& qos,
                                                   bool force);

  /*
   * Check if the operating user exists
   */
  CraneExpected<const User*> GetUserInfoByUid(uint32_t uid);

  CraneExpected<bool> CheckOpUserIsAdmin(uint32_t uid);

  CraneExpected<bool> CheckOperatorPrivilegeHigher(
      uint32_t uid, User::AdminLevel admin_level);

  CraneExpected<bool> CheckOpUserHasPermissionToAccount(
      uint32_t uid, const std::string& account, bool read_only_priv,
      bool is_add);

  CraneExpected<bool> CheckOpUserHasModifyPermission(uint32_t uid,
                                                     const User* user,
                                                     const std::string& name,
                                                     std::string& account,
                                                     bool read_only_priv);

  /*
   * Check if the operating user is the coordinator of the target user's
   * specified account.
   */
  CraneExpected<bool> CheckUserPermissionOnAccount(const User& op_user,
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
  CraneExpected<bool> CheckUserPermissionOnUser(const User& op_user,
                                                const User* user,
                                                const std::string& name,
                                                std::string& account,
                                                bool read_only_priv);

  CraneExpected<bool> CheckPartitionIsAllowed(const Account* account_ptr,
                                              const std::string& account,
                                              const std::string& partition,
                                              bool check_parent, bool is_user);

  CraneExpected<bool> CheckQosIsAllowed(const Account* account_ptr,
                                        const std::string& account,
                                        const std::string& qos_str,
                                        bool check_parent, bool is_user);

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

  CraneExpected<bool> AddUser_(const User& user, const Account* account,
                               const User* stale_user);

  CraneExpected<bool> AddAccount_(const Account& account, const Account* parent,
                                  const Account* stale_account);

  CraneExpected<bool> AddQos_(const Qos* find_qos, const Qos& new_qos);

  CraneExpected<bool> DeleteUser_(const User& user, const std::string& account);

  CraneExpected<bool> DeleteAccount_(const Account& account);

  CraneExpected<bool> DeleteQos_(const std::string& name);

  CraneExpected<bool> AddUserAllowedPartition_(const User& user,
                                               const Account& account,
                                               const std::string& partition);
  CraneExpected<bool> AddUserAllowedQos_(const User& user,
                                         const Account& account,
                                         const std::string& partition,
                                         const std::string& qos);

  CraneExpected<bool> SetUserAdminLevel_(const std::string& name,
                                         const User::AdminLevel new_level);
  CraneExpected<bool> SetUserDefaultQos_(const User& user,
                                         const std::string& account,
                                         const std::string& partition,
                                         const std::string& qos);
  CraneExpected<bool> SetUserAllowedPartition_(const User& user,
                                               const Account& account,
                                               const std::string& partitions);
  CraneExpected<bool> SetUserAllowedQos_(const User& user,
                                         const Account& account,
                                         const std::string& partition,
                                         const std::string& qos_list_str,
                                         bool force);

  CraneExpected<bool> DeleteUserAllowedPartition_(const User& user,
                                                  const std::string& account,
                                                  const std::string& partition);
  CraneExpected<bool> DeleteUserAllowedQos_(const User& user,
                                            const std::string& qos,
                                            const std::string& account,
                                            const std::string& partition,
                                            bool force);

  CraneExpected<bool> AddAccountAllowedPartition_(const std::string& name,
                                                  const std::string& partition);
  CraneExpected<bool> AddAccountAllowedQos_(const Account& account,
                                            const std::string& qos);

  CraneExpected<bool> SetAccountDescription_(const std::string& name,
                                             const std::string& description);
  CraneExpected<bool> SetAccountDefaultQos_(const Account& account,
                                            const std::string& qos);
  CraneExpected<bool> SetAccountAllowedPartition_(const Account& account,
                                                  const std::string& partitions,
                                                  bool force);
  CraneExpected<bool> SetAccountAllowedQos_(const Account& account,
                                            const std::string& qos_list_str,
                                            bool force);

  CraneExpected<bool> DeleteAccountAllowedPartition_(
      const Account& account, const std::string& partition, bool force);
  CraneExpected<bool> DeleteAccountAllowedQos_(const Account& account,
                                               const std::string& qos,
                                               bool force);

  CraneExpected<bool> BlockUser_(const std::string& name,
                                 const std::string& account, bool block);

  CraneExpected<bool> BlockAccount_(const std::string& name, bool block);

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