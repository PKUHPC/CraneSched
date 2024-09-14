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

  Result AddUser(uint32_t uid, User&& new_user);

  Result AddAccount(uint32_t uid, Account&& new_account);

  Result AddQos(uint32_t uid, const Qos& new_qos);

  Result DeleteUser(uint32_t uid, const std::string& name, const std::string& account);

  Result DeleteAccount(uint32_t uid, const std::string& name);

  Result DeleteQos(uint32_t uid, const std::string& name);

  Result QueryUserInfo(uint32_t uid, const std::string& name, std::unordered_map<uid_t, User>* res_user_map);

  Result QueryAccountInfo(uint32_t uid, const std::string& name, std::unordered_map<std::string, Account>* res_account_map);

  Result QueryQosInfo(uint32_t uid, const std::string& name, std::unordered_map<std::string, Qos>* res_qos_map);

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
  Result ModifyAdminLevel(
      const uint32_t uid, 
      const std::string& name, const std::string& value);
  Result ModifyUserDefaultQos(
      uint32_t uid, 
      const std::string& name, 
      const std::string& partition, 
      std::string account, const std::string& value, bool force);
  Result ModifyUserAllowedParition(
      const crane::grpc::OperatorType& operatorType,
      uint32_t uid,
      const std::string& name,
      std::string account,
      const std::string& value);
  Result ModifyUserAllowedQos(
      const crane::grpc::OperatorType& operatorType,
      uint32_t uid,
      const std::string& name, const std::string& partition,
      std::string account,
      const std::string& value, bool force);
  Result DeleteUserAllowedPartiton(
      uint32_t uid,
      const std::string& name,
      std::string account,
      const std::string& value);
  Result DeleteUserAllowedQos(
      uint32_t uid,
      const std::string& name, const std::string& partition,
      std::string account,
      const std::string& value, bool force);

  Result ModifyAccount(
      const crane::grpc::OperatorType& operatorType,
      const uint32_t uid,
      const std::string& name,
      const crane::grpc::ModifyField& modifyField,
      const std::string& value, bool force);
      
  Result ModifyQos(
      const uint32_t uid,
      const std::string& name,
      const crane::grpc::ModifyField& modifyField,
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

/* ---------------------------------------------------------------------------
 * ModifyUser-related functions
 * ---------------------------------------------------------------------------
 */
  Result CheckModifyUserAllowedParition(const crane::grpc::OperatorType& operatorType, const User* user, const Account* account_ptr, const std::string& account, const std::string& partition);
  Result CheckModifyUserAllowedQos(const crane::grpc::OperatorType& operatorType, const User* user, const Account* account_ptr, const std::string& account, const std::string& partition, const std::string& qos_str, bool force);
  Result CheckSetUserAdminLevel(const User& user, const std::string& level,
                                User::AdminLevel* new_level);
  Result CheckSetUserDefaultQos(const User& user, const std::string& account,
                                const std::string& partition,
                                const std::string& qos);
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
                                   const std::string& qos);
  Result CheckDeleteAccountAllowedPartition(const Account& account,
                                            const std::string& partition,
                                            bool force);
  Result CheckDeleteAccountAllowedQos(const Account& account,
                                      const std::string& qos, bool force);

 
  /*
   * Check if the operating user exists
   */
  Result CheckOpUserExisted(uint32_t uid, const User** op_user);

  Result CheckOpUserIsAdmin(uint32_t uid);

  Result CheckOperatorPrivilegeHigher(uint32_t uid, User::AdminLevel admin_level);

  Result CheckOpUserHasPermissionToAccount(uint32_t uid, const std::string& account, bool read_only_priv);

  Result CheckOpUserHasModifyPermission(uint32_t uid, const User* user, const std::string& name, std::string& account, bool read_only_priv);

  /*
   * Check if the operating user is the coordinator of the target user's specified account.
   */
  Result CheckUserPermissionOnAccount(const User& op_user, const std::string& account, bool read_only_priv);
  
 /*
  * Check whether the operating user has permissions to access the target user.
  * Permissions are granted if any of the following three conditions are met:
  * 1. The operating user is the same as the target user.
  * 2. The operating user's level is higher than the target user's level.
  * 3. The operating user is the coordinator of the target user's specified account. 
  *    If the account is empty, it means the operating user is the coordinator of any of the target user's accounts."
  */
  Result CheckUserPermissionOnUser(const User& op_user, const User* user, const std::string& name, const std::string& account, bool read_only_priv);

  bool IsOperatorPrivilegeSameAndHigher(const User& op_user, User::AdminLevel admin_level);

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
  AccountManager::Result AddAccountAllowedQos_(const Account& account,
                                               const std::string& qos);

  Result SetAccountDescription_(const std::string& name,
                                const std::string& description);
  Result SetAccountDefaultQos_(const Account& account, const std::string& qos);
  Result SetAccountAllowedPartition_(const Account& account,
                                     const std::string& partitions, bool force);
  Result SetAccountAllowedQos_(const Account& account,
                               const std::string& qos_list_str, bool force);

  Result DeleteAccountAllowedPartition_(const Account& account,
                                        const std::string& partition,
                                        bool force);
  Result DeleteAccountAllowedQos_(const Account& account,
                                  const std::string& qos, bool force);

  Result BlockUser_(const std::string& name, const std::string& account,
                    bool block);

  Result BlockAccount_(const std::string& name, bool block);

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