#pragma once

#include <string>
#include <unordered_map>

#include "CtldPublicDefs.h"
#include "DbClient.h"

namespace Ctld {

class AccountManager {
 public:
  struct Result {
    bool ok{false};
    std::optional<std::string> reason;
  };

  AccountManager();

  ~AccountManager() = default;

  Result AddUser(Ctld::User& new_user);

  Result AddAccount(Ctld::Account& new_account);

  Result AddQos(Ctld::Qos& new_qos);

  Result DeleteUser(const std::string& name);

  Result DeleteAccount(const std::string& name);

  Result DeleteQos(const std::string& name);

  Ctld::User* GetUserInfo(const std::string& name);
  Ctld::User* GetExistedUserInfo(const std::string& name);
  void GetAllUserInfo(std::list<Ctld::User>& user_list);

  Ctld::Account* GetAccountInfo(const std::string& name);
  Ctld::Account* GetExistedAccountInfo(const std::string& name);
  void GetAllAccountInfo(std::list<Ctld::Account>& account_list);

  bool GetQosInfo(const std::string& name, Ctld::Qos* qos);
  std::unordered_map<std::string, std::unique_ptr<Ctld::Qos>>& GetAllQosInfo();

  Result ModifyUser(
      const crane::grpc::ModifyEntityRequest_OperatorType& operatorType,
      const std::string& name, const std::string& partition,
      const std::string& itemLeft, const std::string& itemRight);
  Result ModifyAccount(
      const crane::grpc::ModifyEntityRequest_OperatorType& operatorType,
      const std::string& name, const std::string& itemLeft,
      const std::string& itemRight);

  std::list<std::string> GetUserAllowedPartition(const std::string& name);
  //  std::list<std::string> GetAccountAllowedPartition(const std::string&
  //  name);

 private:
  void InitDataMap();

  bool IsDefaultQosOfAnyPartition(Ctld::User* user, const std::string& qos);

  bool DeleteUserAllowedQosOfAllPartition(Ctld::User* user,
                                          const std::string& qos, bool force);

  bool DeleteUserAllowedPartition(Ctld::User* user,
                                  const std::string& partition);

  bool IsDefaultQosOfAnyNode(Ctld::Account* account, const std::string& qos);

  bool DeleteAccountAllowedQos(Ctld::Account* account, const std::string& qos,
                               bool force);

  bool DeleteAccountAllowedQos_(Ctld::Account* account, const std::string& qos);

  bool DeleteAccountAllowedPartition(Ctld::Account* account,
                                     const std::string& partition);

  std::unordered_map<std::string /*account name*/,
                     std::unique_ptr<Ctld::Account>>
      m_account_map_;
  std::unordered_map<std::string /*user name*/, std::unique_ptr<Ctld::User>>
      m_user_map_;
  std::unordered_map<std::string /*Qos name*/, std::unique_ptr<Ctld::Qos>>
      m_qos_map_;
};

}  // namespace Ctld

inline std::unique_ptr<Ctld::AccountManager> g_account_manager;