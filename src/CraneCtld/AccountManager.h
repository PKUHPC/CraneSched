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
#include "Security/VaultClient.h"
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
  using WckeyMutexSharedPtr = util::ScopeConstSharedPtr<Wckey, util::rw_mutex>;

  using TxnAction = crane::grpc::TxnAction;

  AccountManager();

  ~AccountManager() = default;

  CraneExpected<void> AddUser(uint32_t uid, const User& new_user);

  CraneExpected<void> AddAccount(uint32_t uid, const Account& new_account);

  CraneExpected<void> AddQos(uint32_t uid, const Qos& new_qos);

  CraneExpected<void> AddUserWckey(uint32_t uid, const Wckey& wckey);

  CraneExpected<void> DeleteUser(uint32_t uid, const std::string& name,
                                 const std::string& account);

  CraneExpected<void> DeleteAccount(uint32_t uid, const std::string& name);

  CraneExpected<void> DeleteQos(uint32_t uid, const std::string& name);

  CraneExpected<void> DeleteWckey(uint32_t uid, const std::string& name,
                                  const std::string& user_name);

  CraneExpected<std::set<User>> QueryAllUserInfo(uint32_t uid);

  CraneExpected<std::vector<Wckey>> QueryAllWckeyInfo(
      uint32_t uid, std::unordered_set<std::string> wckey_list);

  CraneExpected<User> QueryUserInfo(uint32_t uid, const std::string& username);

  CraneExpected<std::set<Account>> QueryAllAccountInfo(uint32_t uid);

  CraneExpected<Account> QueryAccountInfo(uint32_t uid,
                                          const std::string& account);

  CraneExpected<std::set<Qos>> QueryAllQosInfo(uint32_t uid);

  CraneExpected<Qos> QueryQosInfo(uint32_t uid, const std::string& qos);

  UserMutexSharedPtr GetExistedUserInfo(const std::string& name);
  UserMapMutexSharedPtr GetAllUserInfo();

  AccountMutexSharedPtr GetExistedAccountInfo(const std::string& name);
  AccountMapMutexSharedPtr GetAllAccountInfo();

  QosMutexSharedPtr GetExistedQosInfo(const std::string& name);
  QosMapMutexSharedPtr GetAllQosInfo();

  WckeyMutexSharedPtr GetExistedWckeyInfo(const std::string& name,
                                          const std::string& user_name);
  CraneExpected<std::string> GetExistedDefaultWckeyName(
      const std::string& user_name);

  /* ---------------------------------------------------------------------------
   * ModifyUser-related functions
   * ---------------------------------------------------------------------------
   */

  std::vector<CraneExpectedRich<void>> ModifyAccount(
      uint32_t uid, const std::string& account_name,
      const std::vector<crane::grpc::ModifyFieldOperation>& operations,
      bool force);

  std::vector<CraneExpectedRich<void>> ModifyUser(
      uint32_t uid, const std::string& name, const std::string& account,
      const std::string& partition,
      const std::vector<crane::grpc::ModifyFieldOperation>& operations,
      bool force);

  CraneExpected<void> ModifyQos(uint32_t uid, const std::string& name,
                                crane::grpc::ModifyField modify_field,
                                const std::string& value);
  CraneExpected<void> ModifyDefaultWckey(uint32_t uid, const std::string& name,
                                         const std::string& user_name);
  std::vector<CraneExpectedRich<void>> ModifyQos(
      uint32_t uid, const std::string& name,
      const std::vector<crane::grpc::ModifyFieldOperation>& operation);

  CraneExpected<void> BlockAccount(uint32_t uid, const std::string& name,
                                   bool block);

  CraneExpected<void> BlockUser(uint32_t uid, const std::string& name,
                                const std::string& account, bool block);

  CraneExpected<std::list<Txn>> QueryTxnList(
      uint32_t uid,
      const std::unordered_map<std::string, std::string>& conditions,
      int64_t start_time, int64_t end_time);

  bool CheckUserPermissionToPartition(const std::string& name,
                                      const std::string& account,
                                      const std::string& partition);

  CraneExpected<void> CheckIfUserOfAccountIsEnabled(const std::string& user,
                                                    const std::string& account);

  CraneExpected<void> CheckQosLimitOnTask(const std::string& user,
                                          const std::string& account,
                                          TaskInCtld* task);

  CraneExpected<std::string> CheckUidIsAdmin(uint32_t uid);

  CraneExpected<void> CheckIfUidHasPermOnUser(uint32_t uid,
                                              const std::string& username,
                                              bool read_only_priv);

  CraneExpected<void> CheckModifyPartitionAcl(
      uint32_t uid, const std::string& partition_name,
      const std::unordered_set<std::string>& accounts);

  CraneExpected<std::string> SignUserCertificate(uint32_t uid,
                                                 const std::string& csr_content,
                                                 const std::string& alt_names);

  CraneExpected<void> ResetUserCertificate(uint32_t uid,
                                           const std::string& username);

 private:
  void InitDataMap_();

  CraneExpected<const User*> GetUserInfoByUidNoLock_(uint32_t uid);

  const User* GetUserInfoNoLock_(const std::string& name);
  const User* GetExistedUserInfoNoLock_(const std::string& name);

  const Wckey* GetExistedWckeyInfoNoLock_(const std::string& name,
                                          const std::string& user_name);
  const Wckey* GetWckeyInfoNoLock_(const std::string& name,
                                   const std::string& user_name);

  const Account* GetAccountInfoNoLock_(const std::string& name);
  const Account* GetExistedAccountInfoNoLock_(const std::string& name);

  const Qos* GetQosInfoNoLock_(const std::string& name);
  const Qos* GetExistedQosInfoNoLock_(const std::string& name);

  /* ---------------------------------------------------------------------------
   * ModifyUser-related functions(no lock)
   * ---------------------------------------------------------------------------
   */
  std::vector<CraneExpectedRich<void>> CheckModifyAccountOperations(
      const Account* account,
      const std::vector<crane::grpc::ModifyFieldOperation>& operations,
      bool force);
  std::vector<CraneExpectedRich<void>> CheckModifyUserOperations(
      const User* op_user, const Account* account_ptr,
      const std::string& actual_account, const std::string& partition,
      const std::vector<crane::grpc::ModifyFieldOperation>& operations,
      bool force, User* res_user, std::string* log);

  std::vector<CraneExpectedRich<void>> CheckAddUserAllowedPartitionNoLock_(
      User* user, const Account* account,
      const std::unordered_set<std::string>& partition_list);
  CraneExpectedRich<void> CheckSetUserAllowedPartitionNoLock_(
      const Account* account,
      const std::unordered_set<std::string>& partition_list, User* user);
  std::vector<CraneExpectedRich<void>> CheckAddUserAllowedQosNoLock_(
      User* user, const Account* account, const std::string& partition,
      const std::unordered_set<std::string>& qos_list);
  CraneExpectedRich<void> CheckSetUserAllowedQosNoLock_(
      User* user, const Account* account, const std::string& partition,
      const std::unordered_set<std::string>& qos_list, std::string& default_qos,
      bool force);
  CraneExpected<void> CheckSetUserDefaultQosNoLock_(
      User* user, const std::string& account, const std::string& partition,
      const std::string& qos);
  CraneExpectedRich<void> CheckAndDeleteUserAllowedPartitionNoLock_(
      User* user, const std::string& account, const std::string& partition);
  CraneExpected<void> CheckDeleteUserAllowedQosNoLock_(
      const User& user, const std::string& account,
      const std::string& partition, const std::string& qos, bool force);

  /* ---------------------------------------------------------------------------
   * ModifyAccount-related functions(no lock)
   * ---------------------------------------------------------------------------
   */
  CraneExpectedRich<void> CheckAndModifyUserDefaultAccountNoLock_(
      const User* op_user, User* user, const std::string& value);
  CraneExpectedRich<void> CheckAndModifyUserAdminLevelNoLock_(
      const User* op_user, User* user, const std::string& value);
  CraneExpected<void> CheckAddAccountAllowedPartitionNoLock_(
      const Account* account, const std::string& partition);
  CraneExpected<void> CheckAddAccountAllowedQosNoLock_(const Account* account,
                                                       const std::string& qos);
  CraneExpected<void> CheckSetAccountDescriptionNoLock_(const Account* account);
  CraneExpectedRich<void> CheckSetAccountAllowedPartitionNoLock_(
      const Account* account,
      const std::unordered_set<std::string>& partition_list, bool force);
  CraneExpectedRich<void> CheckSetAccountAllowedQosNoLock_(
      const Account* account, const std::unordered_set<std::string>& qos_list,
      bool force);
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
                                               const std::string& qos,
                                               bool check_parent, bool is_user);

  bool IncQosReferenceCountInDb_(const std::string& name, int num);

  CraneExpected<void> AddUser_(const std::string& actor_name, const User& user,
                               const Account* account, const User* stale_user);

  CraneExpected<void> AddAccount_(const std::string& actor_name,
                                  const Account& account, const Account* parent,
                                  const Account* stale_account);

  CraneExpected<void> AddQos_(const std::string& actor_name, const Qos& qos,
                              const Qos* stale_qos);

  CraneExpected<void> AddWckey_(const Wckey& wckey, const Wckey* stale_wckey,
                                const User* user);

  CraneExpected<void> DeleteUser_(const std::string& actor_name,
                                  const User& user, const std::string& account);

  CraneExpected<void> DeleteAccount_(const std::string& actor_name,
                                     const Account& account);

  CraneExpected<void> DeleteQos_(const std::string& actor_name,
                                 const std::string& name);

  CraneExpected<void> AddUserAllowedPartition_(
      const std::string& actor_name, const User& user, const Account& account,
      const std::unordered_set<std::string>& partition_list, User& res_user);
  CraneExpected<void> AddUserAllowedQos_(
      const std::string& actor_name, const User& user, const Account& account,
      const std::string& partition,
      const std::unordered_set<std::string>& qos_list, User& res_user);
  CraneExpected<void> AddUserAllowedPartition_(const std::string& actor_name,
                                               const User& user,
                                               const Account& account,
                                               const std::string& partition);
  CraneExpected<void> DeleteWckey_(const std::string& name,
                                   const std::string& user_name);
  CraneExpected<void> AddUserAllowedQos_(const std::string& actor_name,
                                         const User& user,
                                         const Account& account,
                                         const std::string& partition,
                                         const std::string& qos);

  CraneExpected<void> SetUserAdminLevel_(const std::string& actor_name,
                                         const std::string& name,
                                         User::AdminLevel new_level);

  CraneExpected<void> SetUserDefaultAccount_(const std::string& actor_name,
                                             const std::string& user,
                                             const std::string& def_account);

  CraneExpected<void> SetUserDefaultQos_(const std::string& actor_name,
                                         const User& user,
                                         const std::string& account,
                                         const std::string& partition,
                                         const std::string& qos,
                                         User& res_user);
                                         const std::string& qos);
  CraneExpected<void> SetUserDefaultWckey_(const std::string& new_def_wckey,
                                           const std::string& user);
  CraneExpectedRich<void> SetUserAllowedPartition_(
      const std::string& actor_name, const User& user, const Account& account,
      const std::unordered_set<std::string>& partition_list, User& res_user);
  CraneExpectedRich<void> SetUserAllowedQos_(
      const std::string& actor_name, const User& user, const Account& account,
      const std::string& partition, const std::string& default_qos,
      std::unordered_set<std::string>&& qos_list, User& res_user, bool force);

  CraneExpected<void> DeleteUserAllowedPartition_(const std::string& actor_name,
                                                  const User& user,
                                                  const std::string& account,
                                                  const std::string& partition);
  CraneExpected<void> DeleteUserAllowedQos_(User* user, const std::string& qos,
                                            const std::string& account,
                                            const std::string& partition);

  CraneExpected<void> AddAccountAllowedPartition_(const std::string& actor_name,
                                                  const std::string& name,
                                                  const std::string& partition);
  CraneExpected<void> AddAccountAllowedQos_(const std::string& actor_name,
                                            const Account& account,
                                            const std::string& qos);

  CraneExpected<void> SetAccountDescription_(const std::string& actor_name,
                                             const std::string& name,
                                             const std::string& description);
  CraneExpected<void> SetAccountDefaultQos_(const std::string& actor_name,
                                            const Account& account,
                                            const std::string& qos);
  CraneExpectedRich<void> SetAccountAllowedPartition_(
      const std::string& actor_name, const Account& account,
      std::unordered_set<std::string>& partition_list);
  CraneExpectedRich<void> SetAccountAllowedQos_(
      const std::string& actor_name, const Account& account,
      std::unordered_set<std::string>& qos_list, std::list<int>& change_num,
      const std::string& default_qos);

  CraneExpected<void> DeleteAccountAllowedPartition_(
      const std::string& actor_name, const Account& account,
      const std::string& partition);
  CraneExpected<int> DeleteAccountAllowedQos_(const std::string& actor_name,
                                              const Account& account,
                                              const std::string& qos);

  CraneExpected<void> BlockUserNoLock_(const std::string& actor_name,
                                       const std::string& name,
                                       const std::string& account, bool block);

  CraneExpected<void> BlockAccountNoLock_(const std::string& actor_name,
                                          const std::string& name, bool block);

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

  void AddTxnLogToDB_(const std::string& actor_name, const std::string& target,
                      TxnAction action, const std::string& info);

  bool PaternityTestNoLock_(const std::string& parent,
                            const std::string& child);
  bool PaternityTestNoLockDFS_(const std::string& parent,
                               const std::string& child);
  struct WckeyKey {
    std::string name;
    std::string user_name;

    template <typename H>
    friend H AbslHashValue(H h, const WckeyKey& key) {
      return H::combine(std::move(h), key.name, key.user_name);
    }
    bool operator==(const WckeyKey& other) const {
      return name == other.name && user_name == other.user_name;
    }
  };

  struct WcKeyHash {
    std::size_t operator()(const WckeyKey& key) const {
      return absl::Hash<WckeyKey>{}(key);
    }
  };

  std::unordered_map<std::string /*account name*/, std::unique_ptr<Account>>
      m_account_map_;
  util::rw_mutex m_rw_account_mutex_;
  std::unordered_map<std::string /*user name*/, std::unique_ptr<User>>
      m_user_map_;
  util::rw_mutex m_rw_user_mutex_;
  std::unordered_map<std::string /*Qos name*/, std::unique_ptr<Qos>> m_qos_map_;
  util::rw_mutex m_rw_qos_mutex_;
  std::unordered_map<WckeyKey, std::unique_ptr<Wckey>, WcKeyHash> m_wckey_map_;
  util::rw_mutex m_rw_wckey_mutex_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::AccountManager> g_account_manager;