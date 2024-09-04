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

  struct Result {
    bool ok{false};
    std::string reason;
  };

  AccountManager();

  ~AccountManager() = default;

  Result AddUser(User&& new_user);

  Result AddAccount(Account&& new_account);

  Result AddQos(const Qos& new_qos);

  Result DeleteUser(const std::string& name, const std::string& account);

  Result RemoveUserFromAccount(const std::string& name,
                               const std::string& account);

  Result DeleteAccount(const std::string& name);

  Result DeleteQos(const std::string& name);

  UserMutexSharedPtr GetExistedUserInfo(const std::string& name);
  UserMapMutexSharedPtr GetAllUserInfo();

  AccountMutexSharedPtr GetExistedAccountInfo(const std::string& name);
  AccountMapMutexSharedPtr GetAllAccountInfo();

  QosMutexSharedPtr GetExistedQosInfo(const std::string& name);
  QosMapMutexSharedPtr GetAllQosInfo();

  Result ModifyUser(
      const crane::grpc::ModifyEntityRequest_OperatorType& operatorType,
      const std::string& name, const std::string& partition,
      std::string account,
      const crane::grpc::ModifyEntityRequest_ModifyField& modifyField,
      const std::string& value, bool force);
  Result ModifyAccount(
      const crane::grpc::ModifyEntityRequest_OperatorType& operatorType,
      const std::string& name,
      const crane::grpc::ModifyEntityRequest_ModifyField& modifyField,
      const std::string& value, bool force);

  Result ModifyQos(
      const std::string& name,
      const crane::grpc::ModifyEntityRequest_ModifyField& modifyField,
      const std::string& value);

  Result BlockAccount(const std::string& name, bool block);

  Result BlockUser(const std::string& name, const std::string& account,
                   bool block);

  bool CheckUserPermissionToPartition(const std::string& name,
                                      const std::string& account,
                                      const std::string& partition);

  result::result<void, std::string> CheckEnableState(const std::string& account,
                                                     const std::string& user);

  result::result<void, std::string> CheckAndApplyQosLimitOnTask(
      const std::string& user, const std::string& account, TaskInCtld* task);

  Result FindUserLevelAccountsOfUid(uint32_t uid, User::AdminLevel* level,
                                    std::list<std::string>* accounts);

  result::result<void, std::string> CheckUidIsAdmin(uint32_t uid);

  Result CheckAddUserAllowedPartition(const User& user, const Account& account,
                                      const std::string& partition);
  Result CheckAddUserAllowedQos(const User& user, const Account& account,
                                const std::string& partition,
                                const std::string& qos);
  Result CheckSetUserAdminLevel(const User& user, const std::string& level,
                                User::AdminLevel* new_level);
  Result CheckSetUserDefaultQos(const User& user, const std::string& account,
                                const std::string& partition,
                                const std::string& qos);
  Result CheckSetUserAllowedPartition(const User& user, const Account& account,
                                      const std::string& partitions);
  Result CheckSetUserAllowedQos(const User& user, const Account& account,
                                const std::string& partition,
                                const std::string& qos_list_str, bool force);
  Result CheckDeleteUserAllowedPartition(const User& user,
                                         const std::string& account,
                                         const std::string& partition);
  Result CheckDeleteUserAllowedQos(const User& user, const std::string& account,
                                   const std::string& partition,
                                   const std::string& qos, bool force);

  // ModifyAccount
  Result CheckAddAccountAllowedPartition(const Account& account,
                                         const std::string& partition);
  Result CheckAddAccountAllowedQos(const Account& account,
                                   const std::string& qos);
  Result CheckSetAccountDescription(const Account& account,
                                    const std::string& description);
  Result CheckSetAccountAllowedPartition(const Account& account,
                                         const std::string& partitions,
                                         bool force);
  Result CheckSetAccountAllowedQos(const Account& account,
                                   const std::string& qos_list, bool force);
  Result CheckSetAccountDefaultQos(const Account& account,
                                   const std::string& default_qos);
  Result CheckDeleteAccountAllowedPartition(const Account& account,
                                            const std::string& partition,
                                            bool force);
  Result CheckDeleteAccountAllowedQos(const Account& account,
                                      const std::string& qos, bool force);

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

  AccountManager::Result HasPermissionToUser(
      uint32_t uid, const std::string& target_user, bool read_only_priv,
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

  Result AddUser_(const User* find_user, const Account* find_account,
                  User&& new_user);

  Result AddAccount_(const Account* find_account, const Account* find_parent,
                     Account&& new_account);

  Result AddQos_(const Qos* find_qos, const Qos& new_qos);

  Result DeleteUser_(const User& user, const std::string& account);

  Result DeleteAccount_(const Account& account);

  Result DeleteQos_(const std::string& name);

  Result AddUserAllowedPartition_(const User& user, const Account& account,
                                  const std::string& partition);
  Result AddUserAllowedQos_(const User& user, const Account& account,
                            const std::string& partition,
                            const std::string& qos);

  Result SetUserAdminLevel_(const std::string& name,
                            const User::AdminLevel& new_level);
  Result SetUserDefaultQos_(const User& user, const std::string& account,
                            const std::string& partition,
                            const std::string& qos);
  Result SetUserAllowedPartition_(const User& user, const Account& account,
                                  const std::string& partitions);
  Result SetUserAllowedQos_(const User& user, const Account& account,
                            const std::string& partition,
                            const std::string& qos_list_str, bool force);

  Result DeleteUserAllowedPartition_(const User& user,
                                     const std::string& account,
                                     const std::string& partition);
  Result DeleteUserAllowedQos_(const User& user, const std::string& qos,
                               const std::string& account,
                               const std::string& partition, bool force);

  Result AddAccountAllowedPartition_(const std::string& name,
                                     const std::string& partition);
  AccountManager::Result AddAccountAllowedQos_(const std::string& name,
                                               const std::string& qos);

  Result SetAccountDescription_(const std::string& name,
                                const std::string& description);
  Result SetAccountDefaultQos_(const std::string& name, const std::string& qos);
  Result SetAccountAllowedPartition_(const std::string& name,
                                     const std::string& partitions, bool force);
  Result SetAccountAllowedQos_(const std::string& name,
                               const std::string& qos_list_str, bool force);

  Result DeleteAccountAllowedPartition_(const std::string& name,
                                        const std::string& partition,
                                        bool force);
  Result DeleteAccountAllowedQos_(const std::string& name,
                                  const std::string& qos, bool force);

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