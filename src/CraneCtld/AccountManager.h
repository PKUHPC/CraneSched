#pragma once

#include <string>
#include <unordered_map>

#include "CtldPublicDefs.h"
#include "DbClient.h"
#include "crane/Lock.h"

namespace Ctld {

class AccountManager {
 public:
  struct Result {
    bool ok{false};
    std::optional<std::string> reason;
  };

  AccountManager();

  ~AccountManager() = default;

  Result AddUser(User&& new_user);

  Result AddAccount(Account&& new_account);

  Result AddQos(const Qos& new_qos);

  Result DeleteUser(const std::string& name);

  Result DeleteAccount(const std::string& name);

  Result DeleteQos(const std::string& name);

  bool GetExistedUserInfo(const std::string& name, User* user);
  void GetAllUserInfo(std::list<User>* user_list);

  bool GetExistedAccountInfo(const std::string& name, Account* account);
  void GetAllAccountInfo(std::list<Account>* account_list);

  bool GetExistedQosInfo(const std::string& name, Qos* qos);
  void GetAllQosInfo(std::list<Qos>* qos_list);

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

  bool GetUserInfoNoLock_(const std::string& name, User* user);
  bool GetExistedUserInfoNoLock_(const std::string& name, User* user);

  bool GetAccountInfoNoLock_(const std::string& name, Account* account);
  bool GetExistedAccountInfoNoLock_(const std::string& name, Account* account);

  bool GetQosInfoNoLock_(const std::string& name, Qos* qos);
  bool GetExistedQosInfoNoLock_(const std::string& name, Qos* qos);

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
  bool DeleteUserAllowedPartitionFromMap_(const std::string& name,
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