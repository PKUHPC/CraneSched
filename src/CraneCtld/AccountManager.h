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

  Result AddUser(User& new_user);

  Result AddAccount(Account& new_account);

  Result AddQos(Qos& new_qos);

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
      const std::string& itemLeft, const std::string& itemRight);
  Result ModifyAccount(
      const crane::grpc::ModifyEntityRequest_OperatorType& operatorType,
      const std::string& name, const std::string& itemLeft,
      const std::string& itemRight);

  Result ModifyQos(const std::string& name, const std::string& itemLeft,
                   const std::string& itemRight);

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

  bool IsDefaultQosOfAnyNode(Account& account, const std::string& qos);
  bool IsDefaultQosOfAnyPartition(User& user, const std::string& qos);

  bool DeleteAccountAllowedQosFromDB_(Account& account, const std::string& qos,
                                      mongocxx::client_session* session);
  bool DeleteAccountAllowedQosFromMap_(Account& account,
                                       const std::string& qos);
  bool DeleteUserAllowedQosOfAllPartitionFromDB(
      User& user, const std::string& qos, bool force,
      mongocxx::client_session* session);
  bool DeleteUserAllowedQosOfAllPartitionFromMap(User& user,
                                                 const std::string& qos,
                                                 bool force);

  bool DeleteAccountAllowedPartitionFromDB(Account& account,
                                           const std::string& partition,
                                           mongocxx::client_session* session);
  bool DeleteAccountAllowedPartitionFromMap(Account& account,
                                            const std::string& partition);

  bool DeleteUserAllowedPartitionFromDB(User& user,
                                        const std::string& partition,
                                        mongocxx::client_session* session);
  bool DeleteUserAllowedPartitionFromMap(User& user,
                                         const std::string& partition);

  std::unordered_map<std::string /*account name*/, std::unique_ptr<Account>>
      m_account_map_;
  util::rw_mutex rw_account_mutex;
  std::unordered_map<std::string /*user name*/, std::unique_ptr<User>>
      m_user_map_;
  util::rw_mutex rw_user_mutex;
  std::unordered_map<std::string /*Qos name*/, std::unique_ptr<Qos>> m_qos_map_;
  util::rw_mutex rw_qos_mutex;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::AccountManager> g_account_manager;