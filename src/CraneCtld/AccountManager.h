#pragma once

#include "CtldPublicDefs.h"
// Precompiled header comes first!

#include "DbClient.h"
#include "crane/Lock.h"
#include "crane/Pointer.h"

namespace Ctld {

class AccountManager {
 public:
  using AccountMutexSharedPtr = util::ScopeSharedPtr<Account, util::rw_mutex>;
  using AccountMapMutexSharedPtr = util::ScopeSharedPtr<
      std::unordered_map<std::string, std::unique_ptr<Account>>,
      util::rw_mutex>;
  using UserMutexSharedPtr = util::ScopeSharedPtr<User, util::rw_mutex>;
  using UserMapMutexSharedPtr = util::ScopeSharedPtr<
      std::unordered_map<std::string, std::unique_ptr<User>>, util::rw_mutex>;
  using QosMutexSharedPtr = util::ScopeSharedPtr<Qos, util::rw_mutex>;
  using QosMapMutexSharedPtr = util::ScopeSharedPtr<
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
      std::string account, const std::string& item, const std::string& value,
      bool force);
  Result ModifyAccount(
      const crane::grpc::ModifyEntityRequest_OperatorType& operatorType,
      const std::string& name, const std::string& item,
      const std::string& value, bool force);

  Result ModifyQos(const std::string& name, const std::string& item,
                   const std::string& value, bool force);

  Result BlockAccount(const std::string& name, bool block);

  Result BlockUser(const std::string& name, const std::string& account,
                   bool block);

  bool CheckUserPermissionToPartition(const std::string& name,
                                      const std::string& account,
                                      const std::string& partition);

  bool CheckAccountEnableState(const std::string& name);

  Result CheckAndApplyQosLimitOnTask(const std::string& user,
                                     const std::string& account,
                                     TaskInCtld* task);

  bool PaternityTest(const std::string& parent, const std::string& child);

  Result FindUserLevelAccountOfUid(uint32_t uid, User::AdminLevel* level,
                                   std::string* account);

 private:
  void InitDataMap_();

  const User* GetUserInfoNoLock_(const std::string& name);
  const User* GetExistedUserInfoNoLock_(const std::string& name);

  const Account* GetAccountInfoNoLock_(const std::string& name);
  const Account* GetExistedAccountInfoNoLock_(const std::string& name);

  const Qos* GetQosInfoNoLock_(const std::string& name);
  const Qos* GetExistedQosInfoNoLock_(const std::string& name);

  bool IncQosReferenceCountInDb(const std::string& name, int num);

  Result AddUserAllowedPartition(const std::string& name,
                                 const std::string& account,
                                 const std::string& partition);
  Result AddUserAllowedQos(const std::string& name, const std::string& qos,
                           const std::string& account,
                           const ::std::string& partition);

  Result SetUserAdminLevel(const std::string& name, const std::string& level);
  Result SetUserDefaultQos(const std::string& name, const std::string& qos,
                           const std::string& account,
                           const std::string& partition);
  Result SetUserAllowedPartition(const std::string& name,
                                 const std::string& account,
                                 const std::string& rhs);
  Result SetUserAllowedQos(const std::string& name, const std::string& account,
                           const std::string& partition, const std::string& rhs,
                           bool force);

  Result DeleteUserAllowedPartition(const std::string& name,
                                    const std::string& account,
                                    const std::string& partition);
  Result DeleteUserAllowedQos(const std::string& name, const std::string& qos,
                              const std::string& account,
                              const ::std::string& partition, bool force);

  Result AddAccountAllowedPartition(const std::string& name,
                                    const std::string& rhs);
  AccountManager::Result AddAccountAllowedQos(const std::string& name,
                                              const std::string& qos);

  Result SetAccountDescription(const std::string& name,
                               const std::string& description);
  Result SetAccountDefaultQos(const std::string& name, const std::string& qos);
  Result SetAccountAllowedPartition(const std::string& name,
                                    const std::string& rhs, bool force);
  Result SetAccountAllowedQos(const std::string& name, const std::string& rhs,
                              bool force);

  Result DeleteAccountAllowedPartition(const std::string& name,
                                       const std::string& partition,
                                       bool force);
  Result DeleteAccountAllowedQos(const std::string& name,
                                 const std::string& qos, bool force);

  bool IsAllowedPartitionOfAnyNode(const Account* account,
                                   const std::string& partition);

  bool IsDefaultQosOfAnyNode(const Account* account, const std::string& qos);
  bool IsDefaultQosOfAnyPartition(const User* user, const std::string& qos);

  int DeleteAccountAllowedQosFromDB_(const std::string& name,
                                     const std::string& qos);
  bool DeleteAccountAllowedQosFromMap_(const std::string& name,
                                       const std::string& qos);
  int DeleteUserAllowedQosOfAllPartitionFromDB_(const std::string& name,
                                                const std::string& account,
                                                const std::string& qos);
  bool DeleteUserAllowedQosOfAllPartitionFromMap_(const std::string& name,
                                                  const std::string& account,
                                                  const std::string& qos);

  bool DeleteAccountAllowedPartitionFromDB_(const std::string& name,
                                            const std::string& partition);
  bool DeleteAccountAllowedPartitionFromMap_(const std::string& name,
                                             const std::string& partition);

  bool DeleteUserAllowedPartitionFromDB_(const std::string& name,
                                         const std::string& account,
                                         const std::string& partition);

  bool PaternityTestNoLock_(const std::string& parent,
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