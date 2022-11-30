#pragma once

#include <string>
#include <unordered_map>

#include "CtldPublicDefs.h"
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

  Result DeleteUser(const std::string& name);

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
      const std::string& lhs, const std::string& rhs);
  Result ModifyAccount(
      const crane::grpc::ModifyEntityRequest_OperatorType& operatorType,
      const std::string& name, const std::string& lhs, const std::string& rhs);

  Result ModifyQos(const std::string& name, const std::string& lhs,
                   const std::string& rhs);

  bool CheckUserPermissionToPartition(const std::string& name,
                                      const std::string& partition);

 private:
  void InitDataMap_();

  const User* GetUserInfoNoLock_(const std::string& name);
  const User* GetExistedUserInfoNoLock_(const std::string& name);

  const Account* GetAccountInfoNoLock_(const std::string& name);
  const Account* GetExistedAccountInfoNoLock_(const std::string& name);

  const Qos* GetQosInfoNoLock_(const std::string& name);
  const Qos* GetExistedQosInfoNoLock_(const std::string& name);

  bool IsDefaultQosOfAnyNode(const Account& account, const std::string& qos);
  bool IsDefaultQosOfAnyPartition(const User& user, const std::string& qos);

  bool DeleteAccountAllowedQosFromDB_(const std::string& name,
                                      const std::string& qos);
  bool DeleteAccountAllowedQosFromMap_(const std::string& name,
                                       const std::string& qos);
  bool DeleteUserAllowedQosOfAllPartitionFromDB_(const std::string& name,
                                                 const std::string& qos,
                                                 bool force);
  bool DeleteUserAllowedQosOfAllPartitionFromMap_(const std::string& name,
                                                  const std::string& qos,
                                                  bool force);

  bool DeleteAccountAllowedPartitionFromDB_(const std::string& name,
                                            const std::string& partition);
  bool DeleteAccountAllowedPartitionFromMap_(const std::string& name,
                                             const std::string& partition);

  bool DeleteUserAllowedPartitionFromDB_(const std::string& name,
                                         const std::string& partition);

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