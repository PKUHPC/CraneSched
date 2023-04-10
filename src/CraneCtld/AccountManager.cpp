#include "AccountManager.h"

#include "crane/PasswordEntry.h"

namespace Ctld {

AccountManager::AccountManager() { InitDataMap_(); }

AccountManager::Result AccountManager::AddUser(User&& new_user) {
  // When you add a new user, you can only associate it with the default account
  if (new_user.account_map.size() != 1 ||
      new_user.account_map.begin()->first != new_user.default_account) {
    CRANE_ERROR("The added user does not comply with system rules");
    return Result{false, "Crane system error"};
  }

  if (new_user.default_account.empty()) {
    // User must specify an account
    return Result{false, fmt::format("Please specify the user's account")};
  }
  std::string object_account = new_user.default_account;

  util::write_lock_guard user_guard(m_rw_user_mutex_);
  const std::string name = new_user.name;

  // Avoid duplicate insertion
  const User* find_user = GetUserInfoNoLock_(name);
  if (find_user && !find_user->deleted) {
    if (find_user->account_map.contains(object_account)) {
      return Result{false,
                    fmt::format("The user '{}' already have account '{}'", name,
                                object_account)};
    } else {
      // Add user to account
      new_user = *find_user;
      new_user.account_map[object_account];
      // return AddUserToAccount(find_user, new_user.default_account);
    }
  }

  // Check whether the account exists
  util::write_lock_guard account_guard(m_rw_account_mutex_);
  const Account* find_account = GetExistedAccountInfoNoLock_(object_account);
  if (!find_account) {
    return Result{false, fmt::format("unknown account '{}'", object_account)};
  }

  const std::list<std::string>& parent_allowed_partition =
      find_account->allowed_partition;
  if (!new_user.account_map[object_account].allowed_partition_qos_map.empty()) {
    // Check if user's allowed partition is a subset of parent's allowed
    // partition
    for (auto&& [partition, qos] :
         new_user.account_map[object_account].allowed_partition_qos_map) {
      if (std::find(parent_allowed_partition.begin(),
                    parent_allowed_partition.end(),
                    partition) != parent_allowed_partition.end()) {
        qos.first = find_account->default_qos;
        qos.second = find_account->allowed_qos_list;
      } else {
        return Result{
            false, fmt::format("Partition '{}' is not allowed in account '{}'",
                               partition, find_account->name)};
      }
    }
  } else {
    // Inherit
    for (const auto& partition : parent_allowed_partition) {
      new_user.account_map[object_account]
          .allowed_partition_qos_map[partition] =
          std::pair<std::string, std::list<std::string>>{
              find_account->default_qos,
              std::list<std::string>{find_account->allowed_qos_list}};
    }
  }
  new_user.account_map[object_account].enable = true;

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        // Update the user's account's users_list
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                     "$addToSet", object_account, "users",
                                     name);

        if (find_user) {
          // There is a same user but was deleted or user would like to add user
          // to a new account,here will overwrite it with the same name
          g_db_client->UpdateUser(new_user);
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$set",
                                       name, "creation_time",
                                       ToUnixSeconds(absl::Now()));
        } else {
          // Insert the new user
          g_db_client->InsertUser(new_user);
        }
        for (const auto& qos : find_account->allowed_qos_list) {
          IncQosReferenceCountInDb(
              qos, static_cast<int>(find_account->allowed_partition.size()));
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  m_account_map_[object_account]->users.emplace_back(name);
  m_user_map_[name] = std::make_unique<User>(std::move(new_user));
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  for (const auto& qos : find_account->allowed_qos_list) {
    m_qos_map_[qos]->reference_count += find_account->allowed_partition.size();
  }

  return Result{true};
}

AccountManager::Result AccountManager::AddAccount(Account&& new_account) {
  util::write_lock_guard account_guard(m_rw_account_mutex_);
  const std::string name = new_account.name;

  // Avoid duplicate insertion
  const Account* find_account = GetAccountInfoNoLock_(name);
  if (find_account && !find_account->deleted) {
    return Result{
        false,
        fmt::format("The account '{}' already exists in the database", name)};
  }

  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  for (const auto& qos : new_account.allowed_qos_list) {
    const Qos* find_qos = GetExistedQosInfoNoLock_(qos);
    if (!find_qos) {
      return Result{false, fmt::format("Qos '{}' does not exist", qos)};
    }
  }

  if (!new_account.parent_account.empty()) {
    // Check whether the account's parent account exists
    const Account* find_parent =
        GetExistedAccountInfoNoLock_(new_account.parent_account);
    if (!find_parent) {
      return Result{
          false,
          fmt::format("The parent account '{}' doesn't exist in the database",
                      new_account.parent_account)};
    }

    if (new_account.allowed_partition.empty()) {
      // Inherit
      new_account.allowed_partition =
          std::list<std::string>{find_parent->allowed_partition};
    } else {
      // check allowed partition authority
      for (const auto& par : new_account.allowed_partition) {
        if (std::find(find_parent->allowed_partition.begin(),
                      find_parent->allowed_partition.end(), par) ==
            find_parent->allowed_partition.end()) {  // not find
          return Result{
              false,
              fmt::format(
                  "Parent account '{}' does not have access to partition '{}'",
                  new_account.parent_account, par)};
        }
      }
    }

    if (new_account.allowed_qos_list.empty()) {
      // Inherit
      new_account.allowed_qos_list =
          std::list<std::string>{find_parent->allowed_qos_list};
    } else {
      // check allowed qos list authority
      for (const auto& qos : new_account.allowed_qos_list) {
        if (std::find(find_parent->allowed_qos_list.begin(),
                      find_parent->allowed_qos_list.end(),
                      qos) ==
            find_parent->allowed_qos_list.end()) {  // not find
          return Result{
              false, fmt::format(
                         "Parent account '{}' does not have access to qos '{}'",
                         new_account.parent_account, qos)};
        }
      }
    }
  } else {  // No parent account
    // Check whether partitions exists
    for (const auto& p : new_account.allowed_partition) {
      if (!g_config.Partitions.contains(p)) {
        return Result{false, fmt::format("Partition '{}' does not exist", p)};
      }
    }
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        if (!new_account.parent_account.empty()) {
          // update the parent account's child_account_list
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$addToSet", new_account.parent_account,
                                       "child_accounts", name);
        }

        if (find_account) {
          // There is a same account but was deleted,here will delete the
          // original account and overwrite it with the same name
          g_db_client->UpdateAccount(new_account);
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$set", name, "creation_time",
                                       ToUnixSeconds(absl::Now()));
        } else {
          // Insert the new account
          g_db_client->InsertAccount(new_account);
        }
        for (const auto& qos : new_account.allowed_qos_list) {
          IncQosReferenceCountInDb(qos, 1);
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }
  if (!new_account.parent_account.empty()) {
    m_account_map_[new_account.parent_account]->child_accounts.emplace_back(
        name);
  }
  for (const auto& qos : new_account.allowed_qos_list) {
    m_qos_map_[qos]->reference_count += new_account.allowed_partition.size();
  }
  m_account_map_[name] = std::make_unique<Account>(std::move(new_account));

  return Result{true};
}

AccountManager::Result AccountManager::AddQos(const Qos& new_qos) {
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const Qos* find_qos = GetQosInfoNoLock_(new_qos.name);
  if (find_qos && !find_qos->deleted) {
    return Result{false, fmt::format("Qos '{}' already exists in the database",
                                     new_qos.name)};
  }

  if (find_qos) {
    // There is a same qos but was deleted,here will delete the original
    // qos and overwrite it with the same name
    mongocxx::client_session::with_transaction_cb callback =
        [&](mongocxx::client_session* session) {
          g_db_client->UpdateQos(new_qos);
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS, "$set",
                                       new_qos.name, "creation_time",
                                       ToUnixSeconds(absl::Now()));
        };

    if (!g_db_client->CommitTransaction(callback)) {
      return Result{false, "Can't update the deleted qos to database"};
    }
  } else {
    // Insert the new qos
    if (!g_db_client->InsertQos(new_qos)) {
      return Result{false, "Can't insert the new qos to database"};
    }
  }

  m_qos_map_[new_qos.name] = std::make_unique<Qos>(new_qos);

  return Result{true};
}

AccountManager::Result AccountManager::DeleteUser(const std::string& name,
                                                  const std::string& account) {
  if (!account.empty()) {
    return RemoveUserFromAccount(name, account);
  }

  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* user = GetExistedUserInfoNoLock_(name);
  if (!user) {
    return Result{false,
                  fmt::format("User '{}' doesn't exist in the database", name)};
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        // delete form the parent accounts' users list
        for (const auto& kv : user->account_map) {
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$pull", kv.first,
                                       /*account name*/ "users", name);
        }

        // Delete the user
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$set",
                                     name, "deleted", true);
        for (const auto& [acc_name, item] : user->account_map) {
          for (const auto& [par_name, pair] : item.allowed_partition_qos_map) {
            for (const auto& qos : pair.second) {
              IncQosReferenceCountInDb(qos, -1);
            }
          }
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  util::write_lock_guard account_guard(m_rw_account_mutex_);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  for (const auto& kv : user->account_map) {
    m_account_map_[kv.first]->users.remove(name);
  }
  m_user_map_[name]->deleted = true;
  for (const auto& [acc_name, item] : user->account_map) {
    for (const auto& [par_name, pair] : item.allowed_partition_qos_map) {
      for (const auto& qos : pair.second) {
        m_qos_map_[qos]->reference_count--;
      }
    }
  }

  return Result{true};
}

AccountManager::Result AccountManager::RemoveUserFromAccount(
    const std::string& name, const std::string& account) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* user = GetExistedUserInfoNoLock_(name);
  if (!user) {
    return Result{false, fmt::format("Unknown user '{}'", name)};
  }
  if (!user->account_map.contains(account)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account)};
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        // delete form the parent accounts' users list
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                     "$pull", account, /*account name*/ "users",
                                     name);

        // Delete the account from user account_map
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$unset",
                                     name, "account_map." + account,
                                     std::string(""));

        for (const auto& [par_name, pair] :
             user->account_map.at(account).allowed_partition_qos_map) {
          for (const auto& qos : pair.second) {
            IncQosReferenceCountInDb(qos, -1);
          }
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  util::write_lock_guard account_guard(m_rw_account_mutex_);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  m_account_map_[account]->users.remove(name);
  for (const auto& [par_name, pair] :
       user->account_map.at(account).allowed_partition_qos_map) {
    for (const auto& qos : pair.second) {
      m_qos_map_[qos]->reference_count--;
    }
  }
  m_user_map_[name]->account_map.erase(account);

  return Result{true};
}

AccountManager::Result AccountManager::DeleteAccount(const std::string& name) {
  util::write_lock_guard account_guard(m_rw_account_mutex_);
  const Account* account = GetExistedAccountInfoNoLock_(name);

  if (!account) {
    return Result{
        false, fmt::format("Account '{}' doesn't exist in the database", name)};
  }

  if (!account->child_accounts.empty() || !account->users.empty()) {
    return Result{false, "This account has child account or users"};
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        if (!account->parent_account.empty()) {
          // delete form the parent account's child account list
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$pull", account->parent_account,
                                       "child_accounts", name);
        }
        // Delete the account
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$set",
                                     name, "deleted", true);
        for (const auto& qos : account->allowed_qos_list) {
          IncQosReferenceCountInDb(qos, -1);
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  if (!account->parent_account.empty()) {
    m_account_map_[account->parent_account]->child_accounts.remove(name);
  }
  m_account_map_[name]->deleted = true;
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  for (const auto& qos : account->allowed_qos_list) {
    m_qos_map_[qos]->reference_count--;
  }

  return Result{true};
}

AccountManager::Result AccountManager::DeleteQos(const std::string& name) {
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  const Qos* qos = GetExistedQosInfoNoLock_(name);

  if (!qos) {
    return Result{false, fmt::format("Qos '{}' not exists in database", name)};
  } else if (qos->reference_count != 0) {
    return Result{false,
                  fmt::format("There still has {} references to this qos",
                              qos->reference_count)};
  }

  if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS, "$set",
                                    name, "deleted", true)) {
    return Result{false, fmt::format("Delete qos '{}' failed", name)};
  }
  m_qos_map_[name]->deleted = true;

  return Result{true};
}

AccountManager::UserMutexSharedPtr AccountManager::GetExistedUserInfo(
    const std::string& name) {
  m_rw_user_mutex_.lock_shared();

  const User* user = GetExistedUserInfoNoLock_(name);
  if (!user) {
    m_rw_user_mutex_.unlock_shared();
    return UserMutexSharedPtr{nullptr};
  } else {
    return UserMutexSharedPtr{user, &m_rw_user_mutex_};
  }
}

AccountManager::UserMapMutexSharedPtr AccountManager::GetAllUserInfo() {
  m_rw_user_mutex_.lock_shared();

  if (m_user_map_.empty()) {
    m_rw_user_mutex_.unlock_shared();
    return UserMapMutexSharedPtr{nullptr};
  } else {
    return UserMapMutexSharedPtr{&m_user_map_, &m_rw_user_mutex_};
  }
}

AccountManager::AccountMutexSharedPtr AccountManager::GetExistedAccountInfo(
    const std::string& name) {
  m_rw_account_mutex_.lock_shared();

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    m_rw_account_mutex_.unlock_shared();
    return AccountMutexSharedPtr{nullptr};
  } else {
    return AccountMutexSharedPtr{account, &m_rw_account_mutex_};
  }
}

AccountManager::AccountMapMutexSharedPtr AccountManager::GetAllAccountInfo() {
  m_rw_account_mutex_.lock_shared();

  if (m_account_map_.empty()) {
    m_rw_account_mutex_.unlock_shared();
    return AccountMapMutexSharedPtr{nullptr};
  } else {
    return AccountMapMutexSharedPtr{&m_account_map_, &m_rw_account_mutex_};
  }
}

AccountManager::QosMutexSharedPtr AccountManager::GetExistedQosInfo(
    const std::string& name) {
  m_rw_qos_mutex_.lock_shared();

  const Qos* qos = GetExistedQosInfoNoLock_(name);
  if (!qos) {
    m_rw_qos_mutex_.unlock_shared();
    return QosMutexSharedPtr{nullptr};
  } else {
    return QosMutexSharedPtr{qos, &m_rw_qos_mutex_};
  }
}

AccountManager::QosMapMutexSharedPtr AccountManager::GetAllQosInfo() {
  m_rw_qos_mutex_.lock_shared();

  if (m_qos_map_.empty()) {
    m_rw_qos_mutex_.unlock_shared();
    return QosMapMutexSharedPtr{nullptr};
  } else {
    return QosMapMutexSharedPtr{&m_qos_map_, &m_rw_qos_mutex_};
  }
}

AccountManager::Result AccountManager::ModifyUser(
    const crane::grpc::ModifyEntityRequest_OperatorType& operatorType,
    const std::string& name, const std::string& partition, std::string account,
    const std::string& lhs, const std::string& rhs, bool force) {
  if (account.empty()) {
    auto p = GetExistedUserInfo(name);
    if (!p) {
      return Result{false, fmt::format("Unknown user '{}'", name)};
    }
    account = p->default_account;
  }

  switch (operatorType) {
    case crane::grpc::ModifyEntityRequest_OperatorType_Add:
      if (lhs == "allowed_partition") {
        return AddUserAllowedPartition(name, account, rhs);
      } else if (lhs == "allowed_qos_list") {
        return AddUserAllowedQos(name, rhs, account, partition);
      } else {
        return Result{false, fmt::format("Field '{}' can't be added", lhs)};
      }

    case crane::grpc::ModifyEntityRequest_OperatorType_Overwrite:
      if (lhs == "admin_level") {
        return SetUserAdminLevel(name, rhs);
      } else if (lhs == "default_qos") {
        return SetUserDefaultQos(name, rhs, account, partition);
      } else if (lhs == "allowed_partition") {
        return SetUserAllowedPartition(name, account, rhs);
      } else if (lhs == "allowed_qos_list") {
        return SetUserAllowedQos(name, account, partition, rhs, force);
      } else {
        return Result{false, fmt::format("Field '{}' can't be set", lhs)};
      }

    case crane::grpc::ModifyEntityRequest_OperatorType_Delete:
      if (lhs == "allowed_partition") {
        return DeleteUserAllowedPartition(name, account, rhs);
      } else if (lhs == "allowed_qos_list") {
        return DeleteUserAllowedQos(name, rhs, account, partition, force);
      } else {
        return Result{false, fmt::format("Field '{}' can't be deleted", lhs)};
      }

    default:
      return Result{false, fmt::format("Unknown field '{}'", lhs)};
  }
}

AccountManager::Result AccountManager::ModifyAccount(
    const crane::grpc::ModifyEntityRequest_OperatorType& operatorType,
    const std::string& name, const std::string& lhs, const std::string& rhs,
    bool force) {
  switch (operatorType) {
    case crane::grpc::ModifyEntityRequest_OperatorType_Add:
      if (lhs == "allowed_partition") {
        return AddAccountAllowedPartition(name, rhs);
      } else if (lhs == "allowed_qos_list") {
        return AddAccountAllowedQos(name, rhs);
      } else {
        return Result{false, fmt::format("Field '{}' can't be added", lhs)};
      }

    case crane::grpc::ModifyEntityRequest_OperatorType_Overwrite:
      if (lhs == "description") {
        return SetAccountDescription(name, rhs);
      } else if (lhs == "allowed_partition") {
        return SetAccountAllowedPartition(name, rhs, force);
      } else if (lhs == "allowed_qos_list") {
        return SetAccountAllowedQos(name, rhs, force);
      } else if (lhs == "default_qos") {
        return SetAccountDefaultQos(name, rhs);
      } else {
        return Result{false, fmt::format("Field '{}' can't be set", lhs)};
      }

    case crane::grpc::ModifyEntityRequest_OperatorType_Delete:
      if (lhs == "allowed_partition") {
        return DeleteAccountAllowedPartition(name, rhs, force);
      } else if (lhs == "allowed_qos_list") {
        return DeleteAccountAllowedQos(name, rhs, force);
      } else {
        return Result{false, fmt::format("Field '{}' can't be deleted", lhs)};
      }

    default:
      return Result{true};
  }
}

AccountManager::Result AccountManager::ModifyQos(const std::string& name,
                                                 const std::string& lhs,
                                                 const std::string& rhs,
                                                 bool force) {
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const Qos* p = GetExistedQosInfoNoLock_(name);
  if (!p) {
    return Result{false, fmt::format("Qos '{}' not existed in database", name)};
  }

  if (lhs == "description") {
    if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS, "$set",
                                      name, lhs, rhs)) {
      return Result{false, "Fail to update the database"};
    }
  } else {
    if (!g_db_client->UpdateEntityOne(
            MongodbClient::EntityType::QOS, "$set", name, lhs,
            std::stol(
                rhs) /*uint32 Type Stores data based on long(int64_t)*/)) {
      return Result{false, "Fail to update the database"};
    }
  }

  // To avoid frequently judging lhs, obtain the modified qos of the Mongodb
  Qos qos;
  g_db_client->SelectQos("name", name, &qos);
  *m_qos_map_[name] = std::move(qos);

  return Result{true};
}

AccountManager::Result AccountManager::BlockAccount(const std::string& name,
                                                    bool block) {
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }

  if (account->enable != block) {
    return Result{false, fmt::format("Account '{}' is already blocked", name)};
  }

  if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$set",
                                    name, "enable", !block)) {
    return Result{false, "Can't update the database"};
  }
  m_account_map_[name]->enable = !block;

  return Result{true};
}

AccountManager::Result AccountManager::BlockUser(const std::string& name,
                                                 const std::string& account,
                                                 bool block) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* user = GetExistedUserInfoNoLock_(name);
  if (!user) {
    return Result{false, fmt::format("Unknown user '{}'", name)};
  }
  if (!user->account_map.contains(account)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account)};
  }

  if (user->account_map.at(account).enable != block) {
    return Result{false,
                  fmt::format("User '{}' is already blocked under account '{}'",
                              name, account)};
  }

  if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$set",
                                    name, "account_map." + account + ".enable",
                                    !block)) {
    return Result{false, "Can't update the database"};
  }
  m_user_map_[name]->account_map[account].enable = !block;

  return Result{true};
}

bool AccountManager::CheckUserPermissionToPartition(
    const std::string& name, const std::string& account,
    const std::string& partition) {
  UserMutexSharedPtr user_share_ptr = GetExistedUserInfo(name);
  if (!user_share_ptr) {
    return false;
  }

  if (user_share_ptr->uid == 0 ||
      user_share_ptr->account_map.at(account)
          .allowed_partition_qos_map.contains(partition)) {
    return true;
  }
  return false;
}

bool AccountManager::CheckAccountEnableState(const std::string& name) {
  util::read_lock_guard guard(m_rw_account_mutex_);
  std::string p_str = name;
  const Account* account;
  do {
    account = GetExistedAccountInfoNoLock_(p_str);
    if (!account->enable) {
      return false;
    }
    p_str = account->parent_account;
  } while (!p_str.empty());
  return true;
}

AccountManager::Result AccountManager::CheckAndApplyQosLimitOnTask(
    const std::string& user, const std::string& account, TaskInCtld* task) {
  UserMutexSharedPtr user_share_ptr = GetExistedUserInfo(user);
  if (!user_share_ptr) {
    return Result{false, fmt::format("Unknown user '{}'", user)};
  }

  if (task->uid != 0) {
    auto partition_it =
        user_share_ptr->account_map.at(account).allowed_partition_qos_map.find(task->partition_id);
    if (partition_it == user_share_ptr->account_map.at(account).allowed_partition_qos_map.end())
      return Result{false, "Partition is not allowed for this user."};

    if (task->qos.empty()) {
      // Default qos
      task->qos = partition_it->second.first;
    } else {
      // Check whether task.qos in the qos list
      if (std::find(partition_it->second.second.begin(),
                    partition_it->second.second.end(),
                    task->qos) == partition_it->second.second.end()) {
        return Result{
            false,
            fmt::format(
                "The qos '{}' you set is not in partition's allowed qos list",
                task->qos)};
      }
    }
    if (task->qos.empty()) return Result{true};
  } else {
    if (task->qos.empty()) {
      task->qos = kUnlimitedQosName;
    }
  }

  QosMutexSharedPtr qos_share_ptr = GetExistedQosInfo(task->qos);

  if (task->time_limit == absl::ZeroDuration())
    task->time_limit = qos_share_ptr->max_time_limit_per_task;
  else if (task->time_limit > qos_share_ptr->max_time_limit_per_task)
    return Result{false, "QOSTimeLimit"};

  if (task->cpus_per_task > qos_share_ptr->max_cpus_per_user)
    return Result{false, "QOSResourceLimit"};

  return Result{true};
}

bool AccountManager::PaternityTest(const std::string& parent,
                                   const std::string& child) {
  util::read_lock_guard account_guard(m_rw_account_mutex_);
  if (parent == child || GetExistedAccountInfoNoLock_(parent) == nullptr ||
      GetExistedAccountInfoNoLock_(child) == nullptr) {
    return false;
  }
  return PaternityTestNoLock_(parent, child);
}

AccountManager::Result AccountManager::FindUserLevelAccountOfUid(
    uint32_t uid, User::AdminLevel* level, std::string* account) {
  PasswordEntry entry(uid);
  if (!entry.Valid()) {
    return Result{false,
                  fmt::format("Uid {} not found on the controller node", uid)};
  }

  UserMutexSharedPtr ptr = GetExistedUserInfo(entry.Username());
  if (!ptr) {
    return Result{
        false,
        fmt::format(
            "Permission error: User '{}' not found in the account database",
            entry.Username())};
  }
  if (level != nullptr) *level = ptr->admin_level;
  if (account != nullptr) *account = ptr->default_account;

  return Result{true};
}

void AccountManager::InitDataMap_() {
  std::list<User> user_list;
  g_db_client->SelectAllUser(&user_list);
  for (auto& user : user_list) {
    m_user_map_[user.name] = std::make_unique<User>(user);
  }

  std::list<Account> account_list;
  g_db_client->SelectAllAccount(&account_list);
  for (auto& account : account_list) {
    m_account_map_[account.name] = std::make_unique<Account>(account);
  }

  std::list<Qos> qos_list;
  g_db_client->SelectAllQos(&qos_list);
  for (auto& qos : qos_list) {
    m_qos_map_[qos.name] = std::make_unique<Qos>(qos);
  }
}

/**
 *
 * @param name
 * @note copy user info from m_user_map_
 * @return bool
 */
const User* AccountManager::GetUserInfoNoLock_(const std::string& name) {
  auto find_res = m_user_map_.find(name);
  if (find_res == m_user_map_.end()) {
    return nullptr;
  } else {
    return find_res->second.get();
  }
}

/*
 * Get the user info form mongodb and deletion flag marked false
 */
const User* AccountManager::GetExistedUserInfoNoLock_(const std::string& name) {
  const User* user = GetUserInfoNoLock_(name);
  if (user && !user->deleted) {
    return user;
  } else {
    return nullptr;
  }
}

const Account* AccountManager::GetAccountInfoNoLock_(const std::string& name) {
  auto find_res = m_account_map_.find(name);
  if (find_res == m_account_map_.end()) {
    return nullptr;
  } else {
    return find_res->second.get();
  }
}

const Account* AccountManager::GetExistedAccountInfoNoLock_(
    const std::string& name) {
  const Account* account = GetAccountInfoNoLock_(name);
  if (account && !account->deleted) {
    return account;
  } else {
    return nullptr;
  }
}

const Qos* AccountManager::GetQosInfoNoLock_(const std::string& name) {
  auto find_res = m_qos_map_.find(name);
  if (find_res == m_qos_map_.end()) {
    return nullptr;
  } else {
    return find_res->second.get();
  }
}

const Qos* AccountManager::GetExistedQosInfoNoLock_(const std::string& name) {
  const Qos* qos = GetQosInfoNoLock_(name);
  if (qos && !qos->deleted) {
    return qos;
  } else {
    return nullptr;
  }
}

bool AccountManager::IncQosReferenceCountInDb(const std::string& name,
                                              int num) {
  return g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS, "$inc",
                                      name, "reference_count", num);
}

AccountManager::Result AccountManager::AddUserAllowedPartition(
    const std::string& name, const std::string& account,
    const std::string& partition) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* p = GetExistedUserInfoNoLock_(name);
  if (!p) {
    return Result{false, fmt::format("Unknown user '{}'", name)};
  }
  User user(*p);

  if (!p->account_map.contains(account)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account)};
  }

  util::read_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account_ptr = GetExistedAccountInfoNoLock_(account);

  // check if new partition existed
  if (!g_config.Partitions.contains(partition)) {
    return Result{false, fmt::format("Partition '{}' not existed", partition)};
  }
  // check if account has access to new partition
  if (std::find(account_ptr->allowed_partition.begin(),
                account_ptr->allowed_partition.end(),
                partition) == account_ptr->allowed_partition.end()) {
    return Result{false, fmt::format("User '{}''s account '{}' is not allowed "
                                     "to use the partition '{}'",
                                     name, user.default_account, partition)};
  }

  // check if add item already the user's allowed partition
  if (user.account_map[account].allowed_partition_qos_map.contains(partition)) {
    return Result{
        false, fmt::format("The partition '{}' is already in "
                           "user '{}''s allowed partition under account '{}'",
                           partition, name, account)};
  }

  // Update the map
  user.account_map[account].allowed_partition_qos_map[partition] =
      std::pair<std::string, std::list<std::string>>{
          account_ptr->default_qos,
          std::list<std::string>{account_ptr->allowed_qos_list}};

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateUser(user);
        for (const auto& qos : user.account_map[account]
                                   .allowed_partition_qos_map[partition]
                                   .second) {
          IncQosReferenceCountInDb(qos, 1);
        }
      };

  // Update to database
  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  m_user_map_[name]->account_map[account].allowed_partition_qos_map =
      user.account_map[account].allowed_partition_qos_map;
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  for (const auto& qos :
       user.account_map[account].allowed_partition_qos_map[partition].second) {
    m_qos_map_[qos]->reference_count += 1;
  }

  return Result{true};
}

AccountManager::Result AccountManager::AddUserAllowedQos(
    const std::string& name, const std::string& qos, const std::string& account,
    const std::string& partition) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* p = GetExistedUserInfoNoLock_(name);
  if (!p) {
    return Result{false, fmt::format("Unknown user '{}'", name)};
  }
  User user(*p);
  if (!p->account_map.contains(account)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account)};
  }

  util::read_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account_ptr = GetExistedAccountInfoNoLock_(account);

  // check if qos existed
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  if (!GetExistedQosInfoNoLock_(qos)) {
    return Result{false, fmt::format("Qos '{}' not existed", qos)};
  }

  // check if account has access to new qos
  if (std::find(account_ptr->allowed_qos_list.begin(),
                account_ptr->allowed_qos_list.end(),
                qos) == account_ptr->allowed_qos_list.end()) {
    return Result{
        false,
        fmt::format("Sorry, account '{}' is not allowed to use the qos '{}'",
                    account, qos)};
  }

  // check if add item already the user's allowed qos
  int change_num = 0;
  if (partition.empty()) {
    // add to all partition
    for (auto& [par, pair] :
         user.account_map[account].allowed_partition_qos_map) {
      std::list<std::string>& list = pair.second;
      if (std::find(list.begin(), list.end(), qos) == list.end()) {
        list.emplace_back(qos);
        change_num++;
      }
    }

    if (change_num == 0) {
      return Result{false, fmt::format("Qos '{}' is already in user '{}''s "
                                       "allowed qos of all partition",
                                       qos, name)};
    }
  } else {
    // add to exacted partition
    auto iter =
        user.account_map[account].allowed_partition_qos_map.find(partition);
    if (iter == user.account_map[account].allowed_partition_qos_map.end()) {
      return Result{
          false,
          fmt::format("Partition '{}' is not in user '{}''s allowed partition",
                      partition, name)};
    }

    std::list<std::string>& list = iter->second.second;
    if (std::find(list.begin(), list.end(), qos) != list.end()) {
      return Result{false, fmt::format("Qos '{}' is already in user '{}''s "
                                       "allowed qos of partition '{}'",
                                       qos, name, partition)};
    }
    list.push_back(qos);
    change_num = 1;
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateUser(user);
        IncQosReferenceCountInDb(qos, change_num);
      };

  // Update to database
  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  m_user_map_[name]->account_map[account].allowed_partition_qos_map =
      user.account_map[account].allowed_partition_qos_map;
  m_qos_map_[qos]->reference_count += change_num;

  return Result{true};
}

AccountManager::Result AccountManager::SetUserAdminLevel(
    const std::string& name, const std::string& level) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* user = GetExistedUserInfoNoLock_(name);
  if (!user) {
    return Result{false, fmt::format("Unknown user '{}'", name)};
  }

  User::AdminLevel new_level;
  if (level == "none") {
    new_level = User::None;
  } else if (level == "operator") {
    new_level = User::Operator;
  } else if (level == "admin") {
    new_level = User::Admin;
  } else {
    return Result{false, fmt::format("Unknown admin level '{}'", level)};
  }

  if (new_level == user->admin_level) {
    return Result{false,
                  fmt::format("User '{}' is already a {} role", name, level)};
  }

  // Update to database
  if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$set",
                                    name, "admin_level",
                                    static_cast<int>(new_level))) {
    return Result{false, "Fail to update data in database"};
  }

  m_user_map_[name]->admin_level = new_level;

  return Result{true};
}

AccountManager::Result AccountManager::SetUserDefaultQos(
    const std::string& name, const std::string& qos, const std::string& account,
    const std::string& partition) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* p = GetExistedUserInfoNoLock_(name);
  if (!p) {
    return Result{false, fmt::format("Unknown user '{}'", name)};
  }
  User user(*p);
  if (!p->account_map.contains(account)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account)};
  }

  if (partition.empty()) {
    bool is_changed = false;
    for (auto& [par, pair] :
         user.account_map[account].allowed_partition_qos_map) {
      if (std::find(pair.second.begin(), pair.second.end(), qos) !=
              pair.second.end() &&
          qos != pair.first) {
        is_changed = true;
        pair.first = qos;
      }
    }

    if (!is_changed) {
      return Result{false, fmt::format("Qos '{}' not in allowed qos list "
                                       "or is already the default qos",
                                       qos)};
    }
  } else {
    auto iter =
        user.account_map[account].allowed_partition_qos_map.find(partition);

    if (std::find(iter->second.second.begin(), iter->second.second.end(),
                  qos) == iter->second.second.end()) {
      return Result{false,
                    fmt::format("Qos '{}' not in allowed qos list", qos)};
    }

    if (iter->second.first == qos) {
      return Result{false,
                    fmt::format("Qos '{}' is already the default qos", qos)};
    }
    iter->second.first = qos;
  }

  // Update to database
  if (!g_db_client->UpdateUser(user)) {
    return Result{false, "Fail to update data in database"};
  }

  m_user_map_[name]->account_map[account].allowed_partition_qos_map =
      user.account_map[account].allowed_partition_qos_map;

  return Result{true};
}

AccountManager::Result AccountManager::SetUserAllowedPartition(
    const std::string& name, const std::string& account,
    const std::string& rhs) {
  std::list<std::string> partition_list;
  boost::split(partition_list, rhs, boost::is_any_of(","));
  partition_list.remove_if([](const std::string& v) { return v.empty(); });
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* p = GetExistedUserInfoNoLock_(name);
  if (!p) {
    return Result{false, fmt::format("Unknown user '{}'", name)};
  }
  User user(*p);

  if (!p->account_map.contains(account)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account)};
  }

  util::read_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account_ptr = GetExistedAccountInfoNoLock_(account);

  for (const auto& par : partition_list) {
    // check if partition existed
    if (!g_config.Partitions.contains(par)) {
      return Result{false, fmt::format("Partition '{}' not existed", par)};
    }
    // check if account has access to new partition
    if (std::find(account_ptr->allowed_partition.begin(),
                  account_ptr->allowed_partition.end(),
                  par) == account_ptr->allowed_partition.end()) {
      return Result{false,
                    fmt::format("User '{}''s account '{}' is not allowed "
                                "to use the partition '{}'",
                                name, user.default_account, par)};
    }
  }

  // Update the map
  user.account_map[account]
      .allowed_partition_qos_map.clear();  // clear the partitions
  for (const auto& par : partition_list) {
    user.account_map[account].allowed_partition_qos_map[par] =
        std::pair<std::string, std::list<std::string>>{
            account_ptr->default_qos,
            std::list<std::string>{account_ptr->allowed_qos_list}};
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateUser(user);

        for (const auto& [par, pair] :
             p->account_map.at(account).allowed_partition_qos_map) {
          for (const auto& qos : pair.second) {
            IncQosReferenceCountInDb(qos, -1);
          }
        }

        for (const auto& qos : account_ptr->allowed_qos_list) {
          IncQosReferenceCountInDb(qos, partition_list.size());
        }
      };

  // Update to database
  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  m_user_map_[name]->account_map[account].allowed_partition_qos_map =
      user.account_map[account].allowed_partition_qos_map;

  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  for (const auto& [par, pair] :
       p->account_map.at(account).allowed_partition_qos_map) {
    for (const auto& qos : pair.second) {
      m_qos_map_[qos]->reference_count -= 1;
    }
  }
  for (const auto& qos : account_ptr->allowed_qos_list) {
    m_qos_map_[qos]->reference_count += partition_list.size();
  }

  return Result{true};
}

AccountManager::Result AccountManager::SetUserAllowedQos(
    const std::string& name, const std::string& account,
    const std::string& partition, const std::string& rhs, bool force) {
  std::list<std::string> qos_list;
  boost::split(qos_list, rhs, boost::is_any_of(","));
  qos_list.remove_if([](const std::string& v) { return v.empty(); });

  util::write_lock_guard user_guard(m_rw_user_mutex_);
  const User* p = GetExistedUserInfoNoLock_(name);
  if (!p) {
    return Result{false, fmt::format("Unknown user '{}'", name)};
  }
  if (!p->account_map.contains(account)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account)};
  }
  User user(*p);

  util::read_lock_guard account_guard(m_rw_account_mutex_);
  const Account* account_ptr = GetExistedAccountInfoNoLock_(account);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  for (const auto& qos : qos_list) {
    // check if qos existed
    if (!GetExistedQosInfoNoLock_(qos)) {
      return Result{false, fmt::format("Qos '{}' not existed", qos)};
    }
    // check if account has access to new qos
    if (std::find(account_ptr->allowed_qos_list.begin(),
                  account_ptr->allowed_qos_list.end(),
                  qos) == account_ptr->allowed_qos_list.end()) {
      return Result{
          false,
          fmt::format("Sorry, account '{}' is not allowed to use the qos '{}'",
                      account, qos)};
    }
  }

  if (partition.empty()) {
    // Set the qos of all partition
    for (auto& [par, pair] :
         user.account_map[account].allowed_partition_qos_map) {
      if (std::find(qos_list.begin(), qos_list.end(), pair.first) ==
          qos_list.end()) {
        if (!force) {
          return Result{
              false,
              fmt::format("Qos '{}' is default qos of partition '{}',but not "
                          "found in new qos list.Ignoring this constraint with "
                          "forced operation, the default qos is randomly "
                          "replaced with one of the items in the new qos list",
                          pair.first, par)};
        } else {
          pair.first = qos_list.empty() ? "" : qos_list.front();
        }
      }
      pair.second.assign(qos_list.begin(), qos_list.end());
    }
  } else {
    // Set the qos of a specified partition
    auto iter =
        user.account_map[account].allowed_partition_qos_map.find(partition);
    if (iter == user.account_map[account].allowed_partition_qos_map.end()) {
      return Result{false,
                    fmt::format("Partition '{}' not in allowed partition list",
                                partition)};
    }

    if (std::find(qos_list.begin(), qos_list.end(), iter->second.first) ==
        qos_list.end()) {
      if (!force) {
        return Result{
            false,
            fmt::format("Qos '{}' is default qos of partition '{}',but not "
                        "found in new qos list.Ignoring this constraint with "
                        "forced operation, the default qos is randomly "
                        "replaced with one of the items in the new qos list",
                        iter->second.first, partition)};
      } else {
        iter->second.first = qos_list.empty() ? "" : qos_list.front();
      }
    }
    iter->second.second.assign(qos_list.begin(), qos_list.end());
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateUser(user);
        // Reduce the previous reference count
        if (partition.empty()) {
          for (const auto& [par, pair] :
               p->account_map.at(account).allowed_partition_qos_map) {
            for (const auto& qos : pair.second) {
              IncQosReferenceCountInDb(qos, -1);
            }
          }
        } else {
          for (const auto& qos : p->account_map.at(account)
                                     .allowed_partition_qos_map.at(partition)
                                     .second) {
            IncQosReferenceCountInDb(qos, -1);
          }
        }
        // Increase the current reference count
        for (const auto& qos : qos_list) {
          IncQosReferenceCountInDb(
              qos, user.account_map[account].allowed_partition_qos_map.size());
        }
      };

  // Update to database
  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  m_user_map_[name]->account_map[account].allowed_partition_qos_map =
      user.account_map[account].allowed_partition_qos_map;
  // Reduce the previous reference count
  if (partition.empty()) {
    for (const auto& [par, pair] :
         p->account_map.at(account).allowed_partition_qos_map) {
      for (const auto& qos : pair.second) {
        m_qos_map_[qos]->reference_count -= 1;
      }
    }
  } else {
    for (const auto& qos : p->account_map.at(account)
                               .allowed_partition_qos_map.at(partition)
                               .second) {
      m_qos_map_[qos]->reference_count -= 1;
    }
  }
  // Increase the current reference count
  for (const auto& qos : qos_list) {
    m_qos_map_[qos]->reference_count -=
        user.account_map[account].allowed_partition_qos_map.size();
  }

  return Result{true};
}

AccountManager::Result AccountManager::DeleteUserAllowedPartition(
    const std::string& name, const std::string& account,
    const std::string& partition) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* user = GetExistedUserInfoNoLock_(name);
  if (!user) {
    return Result{false, fmt::format("Unknown user '{}'", name)};
  }
  if (!user->account_map.contains(account)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account)};
  }

  if (!user->account_map.at(account).allowed_partition_qos_map.contains(
          partition)) {
    return Result{
        false,
        fmt::format(
            "Partition '{}' is not in user '{}''s allowed partition list",
            partition, name)};
  }

  // Update to database
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        DeleteUserAllowedPartitionFromDB_(name, account, partition);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  for (const auto& qos : user->account_map.at(account)
                             .allowed_partition_qos_map.at(partition)
                             .second) {
    m_qos_map_[qos]->reference_count -= 1;
  }
  m_user_map_[name]->account_map[account].allowed_partition_qos_map.erase(
      partition);

  return Result{true};
}

AccountManager::Result AccountManager::DeleteUserAllowedQos(
    const std::string& name, const std::string& qos, const std::string& account,
    const std::string& partition, bool force) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* p = GetExistedUserInfoNoLock_(name);
  if (!p) {
    return Result{false, fmt::format("Unknown user '{}'", name)};
  }
  if (!p->account_map.contains(account)) {
    return Result{false, fmt::format("User '{}' doesn't belong to account '{}'",
                                     name, account)};
  }
  User user(*p);

  int change_num = 0;
  if (partition.empty()) {
    // Delete the qos of all partition
    for (auto& [par, pair] :
         user.account_map[account].allowed_partition_qos_map) {
      if (std::find(pair.second.begin(), pair.second.end(), qos) !=
          pair.second.end()) {
        change_num++;
        pair.second.remove(qos);

        if (pair.first == qos) {
          if (!force) {
            return Result{
                false, fmt::format(
                           "Qos '{}' is default qos of partition '{}'.Ignoring "
                           "this constraint with forced deletion, the deleted "
                           "default qos is randomly replaced with one of the "
                           "remaining items in the qos list",
                           qos, par)};
          } else {
            pair.first = pair.second.empty() ? "" : pair.second.front();
          }
        }
      }
    }

    if (change_num == 0) {
      return Result{
          false, fmt::format(
                     "Qos '{}' not in allowed qos list of all partition", qos)};
    }
  } else {
    // Delete the qos of a specified partition
    auto iter =
        user.account_map[account].allowed_partition_qos_map.find(partition);

    if (iter == user.account_map[account].allowed_partition_qos_map.end()) {
      return Result{false,
                    fmt::format("Partition '{}' not in allowed partition list",
                                partition)};
    }

    if (std::find(iter->second.second.begin(), iter->second.second.end(),
                  qos) == iter->second.second.end()) {
      return Result{
          false,
          fmt::format("Qos '{}' not in allowed qos list of partition '{}'", qos,
                      partition)};
    }
    iter->second.second.remove(qos);
    change_num = 1;

    if (qos == iter->second.first) {
      if (!force)
        return Result{
            false,
            fmt::format("Qos '{}' is default qos of partition '{}'.Ignoring "
                        "this constraint with forced deletion, the deleted "
                        "default qos is randomly replaced with one of the "
                        "remaining items in the qos list",
                        qos, partition)};
      else
        iter->second.first =
            iter->second.second.empty() ? "" : iter->second.second.front();
    }
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateUser(user);
        IncQosReferenceCountInDb(qos, -change_num);
      };

  // Update to database
  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  m_user_map_[name]->account_map[account].allowed_partition_qos_map =
      user.account_map[account].allowed_partition_qos_map;
  m_qos_map_[qos]->reference_count -= change_num;

  return Result{true};
}

AccountManager::Result AccountManager::AddAccountAllowedPartition(
    const std::string& name, const std::string& partition) {
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }

  // check if the partition existed
  if (g_config.Partitions.find(partition) == g_config.Partitions.end()) {
    return Result{false, fmt::format("Partition '{}' not existed", partition)};
  }
  // Check if parent account has access to the partition
  if (!account->parent_account.empty()) {
    const Account* parent =
        GetExistedAccountInfoNoLock_(account->parent_account);
    if (std::find(parent->allowed_partition.begin(),
                  parent->allowed_partition.end(),
                  partition) == parent->allowed_partition.end()) {
      return Result{false, fmt::format("Parent account '{}' does not "
                                       "have access to partition '{}'",
                                       account->parent_account, partition)};
    }
  }

  if (std::find(account->allowed_partition.begin(),
                account->allowed_partition.end(),
                partition) != account->allowed_partition.end()) {
    return Result{
        false,
        fmt::format("Partition '{}' is already in allowed partition list",
                    partition)};
  }

  if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                    "$addToSet", name, "allowed_partition",
                                    partition)) {
    return Result{false, "Can't update the  database"};
  }
  m_account_map_[name]->allowed_partition.emplace_back(partition);

  return Result{true};
}

AccountManager::Result AccountManager::AddAccountAllowedQos(
    const std::string& name, const std::string& qos) {
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }

  // check if the qos existed
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  if (!GetExistedQosInfoNoLock_(qos)) {
    return Result{false, fmt::format("Qos '{}' not existed", qos)};
  }

  // Check if parent account has access to the qos
  if (!account->parent_account.empty()) {
    const Account* parent =
        GetExistedAccountInfoNoLock_(account->parent_account);

    if (std::find(parent->allowed_qos_list.begin(),
                  parent->allowed_qos_list.end(),
                  qos) == parent->allowed_qos_list.end()) {
      return Result{
          false,
          fmt::format("Parent account '{}' does not have access to qos '{}'",
                      account->parent_account, qos)};
    }
  }

  if (std::find(account->allowed_qos_list.begin(),
                account->allowed_qos_list.end(),
                qos) != account->allowed_qos_list.end()) {
    return Result{false,
                  fmt::format("Qos '{}' is already in allowed qos list", qos)};
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                     "$addToSet", name, "allowed_qos_list",
                                     qos);
        IncQosReferenceCountInDb(qos, 1);
      };

  // Update to database
  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  m_account_map_[name]->allowed_qos_list.emplace_back(qos);
  m_qos_map_[qos]->reference_count++;

  return Result{true};
}

AccountManager::Result AccountManager::SetAccountDescription(
    const std::string& name, const std::string& description) {
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }

  if (description == account->description) {
    return Result{false, "Description content not change"};
  }

  if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$set",
                                    name, "description", description)) {
    return Result{false, "Can't update the  database"};
  }

  m_account_map_[name]->description = description;

  return Result{true};
}

AccountManager::Result AccountManager::SetAccountDefaultQos(
    const std::string& name, const std::string& qos) {
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }

  if (account->default_qos == qos) {
    return Result{false,
                  fmt::format("Qos '{}' is already the default qos", qos)};
  }

  if (std::find(account->allowed_qos_list.begin(),
                account->allowed_qos_list.end(),
                qos) == account->allowed_qos_list.end()) {
    return Result{false, fmt::format("Qos '{}' not in allowed qos list", qos)};
  }

  if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$set",
                                    name, "default_qos", qos)) {
    return Result{false, "Can't update the database"};
  }
  m_account_map_[name]->default_qos = qos;

  return Result{true};
}

AccountManager::Result AccountManager::SetAccountAllowedPartition(
    const std::string& name, const std::string& rhs, bool force) {
  std::list<std::string> partition_list;
  boost::split(partition_list, rhs, boost::is_any_of(","));
  partition_list.remove_if([](const std::string& v) { return v.empty(); });

  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }
  // check if the partition existed
  for (const auto& p : partition_list) {
    if (!g_config.Partitions.contains(p)) {
      return Result{false, fmt::format("Partition '{}' not existed", p)};
    }
  }

  // Check if parent account has access to the partition
  if (!account->parent_account.empty()) {
    const Account* parent =
        GetExistedAccountInfoNoLock_(account->parent_account);
    for (const auto& p : partition_list) {
      if (std::find(parent->allowed_partition.begin(),
                    parent->allowed_partition.end(),
                    p) == parent->allowed_partition.end()) {
        return Result{false, fmt::format("Parent account '{}' does not "
                                         "have access to partition '{}'",
                                         account->parent_account, p)};
      }
    }
  }

  std::list<std::string> deleted_partition;
  for (const auto& par : account->allowed_partition) {
    if (std::find(partition_list.begin(), partition_list.end(), par) ==
        partition_list.end()) {
      if (!force && IsAllowedPartitionOfAnyNode(account, par)) {
        return Result{
            false,
            fmt::format("partition '{}' in allowed partition list before is "
                        "used by some descendant node of the account "
                        "'{}'.Ignoring this constraint with forced operation",
                        par, name)};
      }
      deleted_partition.emplace_back(par);
    }
  }

  int add_num = 0;
  for (const auto& par : partition_list) {
    if (std::find(account->allowed_partition.begin(),
                  account->allowed_partition.end(),
                  par) == account->allowed_partition.end()) {
      add_num++;
    }
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        for (const auto& par : deleted_partition) {
          DeleteAccountAllowedPartitionFromDB_(account->name, par);
        }

        if (add_num > 0) {
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$set", name, "allowed_partition",
                                       partition_list);
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  for (const auto& par : deleted_partition) {
    DeleteAccountAllowedPartitionFromMap_(account->name, par);
  }
  m_account_map_[name]->allowed_partition.assign(partition_list.begin(),
                                                 partition_list.end());

  return Result{true};
}

AccountManager::Result AccountManager::SetAccountAllowedQos(
    const std::string& name, const std::string& rhs, bool force) {
  std::list<std::string> qos_list;
  boost::split(qos_list, rhs, boost::is_any_of(","));
  qos_list.remove_if([](const std::string& v) { return v.empty(); });

  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }
  // check if the qos existed
  m_rw_qos_mutex_.lock_shared();
  for (const auto& qos : qos_list) {
    if (!GetExistedQosInfoNoLock_(qos)) {
      return Result{false, fmt::format("Qos '{}' not existed", qos)};
    }
  }
  m_rw_qos_mutex_.unlock_shared();

  // Check if parent account has access to the qos
  if (!account->parent_account.empty()) {
    const Account* parent =
        GetExistedAccountInfoNoLock_(account->parent_account);
    for (const auto& qos : qos_list) {
      if (std::find(parent->allowed_qos_list.begin(),
                    parent->allowed_qos_list.end(),
                    qos) == parent->allowed_qos_list.end()) {
        return Result{
            false,
            fmt::format("Parent account '{}' does not have access to qos '{}'",
                        account->parent_account, qos)};
      }
    }
  }

  std::list<std::string> deleted_qos;
  for (const auto& qos : account->allowed_qos_list) {
    if (std::find(qos_list.begin(), qos_list.end(), qos) == qos_list.end()) {
      if (!force && IsDefaultQosOfAnyNode(account, qos)) {
        return Result{
            false,
            fmt::format("partition '{}' in allowed partition list before is "
                        "used by some descendant node of the account "
                        "'{}'.Ignoring this constraint with forced operation",
                        qos, name)};
      }
      deleted_qos.emplace_back(qos);
    }
  }

  std::list<std::string> add_qos;
  for (const auto& qos : qos_list) {
    if (std::find(account->allowed_qos_list.begin(),
                  account->allowed_qos_list.end(),
                  qos) == account->allowed_qos_list.end()) {
      add_qos.emplace_back(qos);
    }
  }

  std::list<int> change_num;
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        for (const auto& qos : deleted_qos) {
          int num = DeleteAccountAllowedQosFromDB_(account->name, qos);
          IncQosReferenceCountInDb(qos, -num);
          change_num.emplace_back(num);
        }

        if (!add_qos.empty()) {
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$set", name, "allowed_qos_list",
                                       qos_list);
          for (const auto& qos : add_qos) {
            IncQosReferenceCountInDb(qos, 1);
          }
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  for (const auto& qos : deleted_qos) {
    DeleteAccountAllowedQosFromMap_(account->name, qos);
    m_qos_map_[qos]->reference_count -= change_num.front();
    change_num.pop_front();
  }

  m_account_map_[name]->allowed_qos_list.assign(qos_list.begin(),
                                                qos_list.end());
  for (const auto& qos : add_qos) {
    m_qos_map_[qos]->reference_count++;
  }

  return Result{true};
}

AccountManager::Result AccountManager::DeleteAccountAllowedPartition(
    const std::string& name, const std::string& partition, bool force) {
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }

  if (!force && IsAllowedPartitionOfAnyNode(account, partition)) {
    return Result{
        false, fmt::format(
                   "partition '{}' is used by some descendant node of the "
                   "account '{}'.Ignoring this constraint with forced deletion",
                   partition, name)};
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        DeleteAccountAllowedPartitionFromDB_(account->name, partition);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  DeleteAccountAllowedPartitionFromMap_(account->name, partition);

  return Result{true};
}

AccountManager::Result AccountManager::DeleteAccountAllowedQos(
    const std::string& name, const std::string& qos, bool force) {
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    return Result{false, fmt::format("Unknown account '{}'", name)};
  }

  auto iter = std::find(account->allowed_qos_list.begin(),
                        account->allowed_qos_list.end(), qos);

  if (iter == account->allowed_qos_list.end()) {
    return Result{
        false, fmt::format("Qos '{}' is not in account '{}''s allowed qos list",
                           qos, name)};
  }

  if (!force && IsDefaultQosOfAnyNode(account, qos)) {
    return Result{
        false,
        fmt::format("Someone is using qos '{}' as default qos.Ignoring this "
                    "constraint with forced deletion, the deleted default "
                    "qos is randomly replaced with one of the remaining "
                    "items in the qos list",
                    qos)};
  }

  int change_num;
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        change_num = DeleteAccountAllowedQosFromDB_(account->name, qos);
        IncQosReferenceCountInDb(qos, -change_num);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return Result{false, "Fail to update data in database"};
  }

  DeleteAccountAllowedQosFromMap_(account->name, qos);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  m_qos_map_[qos]->reference_count -= change_num;

  return Result{true};
}

bool AccountManager::IsAllowedPartitionOfAnyNode(const Account* account,
                                                 const std::string& partition) {
  if (std::find(account->allowed_partition.begin(),
                account->allowed_partition.end(),
                partition) != account->allowed_partition.end()) {
    return true;
  }
  for (const auto& child : account->child_accounts) {
    if (IsAllowedPartitionOfAnyNode(GetExistedAccountInfoNoLock_(child),
                                    partition)) {
      return true;
    }
  }
  for (const auto& user : account->users) {
    const User* p = GetExistedUserInfoNoLock_(user);
    for (const auto& item : p->account_map) {
      if (item.second.allowed_partition_qos_map.contains(partition)) {
        return true;
      }
    }
  }
  return false;
}

bool AccountManager::IsDefaultQosOfAnyNode(const Account* account,
                                           const std::string& qos) {
  if (account->default_qos == qos) {
    return true;
  }
  for (const auto& child : account->child_accounts) {
    if (IsDefaultQosOfAnyNode(GetExistedAccountInfoNoLock_(child), qos)) {
      return true;
    }
  }
  util::read_lock_guard user_guard(m_rw_user_mutex_);
  for (const auto& user : account->users) {
    if (IsDefaultQosOfAnyPartition(GetExistedUserInfoNoLock_(user), qos)) {
      return true;
    }
  }
  return false;
}

bool AccountManager::IsDefaultQosOfAnyPartition(const User* user,
                                                const std::string& qos) {
  for (const auto& [account, item] : user->account_map) {
    if (std::any_of(item.allowed_partition_qos_map.begin(),
                    item.allowed_partition_qos_map.end(),
                    [&qos](const auto& p) { return p.second.first == qos; })) {
      return true;
    }
  }
  return false;
}

int AccountManager::DeleteAccountAllowedQosFromDB_(const std::string& name,
                                                   const std::string& qos) {
  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    CRANE_ERROR(
        "Operating on a non-existent account '{}', please check this item in "
        "database and restart cranectld",
        name);
    return false;
  }

  auto iter = std::find(account->allowed_qos_list.begin(),
                        account->allowed_qos_list.end(), qos);
  if (iter == account->allowed_qos_list.end()) {
    return false;
  }

  int change_num = 1;
  for (const auto& child : account->child_accounts) {
    change_num += DeleteAccountAllowedQosFromDB_(child, qos);
  }
  util::read_lock_guard user_guard(m_rw_user_mutex_);
  for (const auto& user : account->users) {
    change_num += DeleteUserAllowedQosOfAllPartitionFromDB_(user, name, qos);
  }
  g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$pull",
                               name, "allowed_qos_list", qos);

  if (account->default_qos == qos) {
    std::list<std::string> temp{account->allowed_qos_list};
    temp.remove(qos);
    std::string new_default = temp.empty() ? "" : temp.front();
    g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$set",
                                 name, "default_qos", new_default);
  }
  return change_num;
}

bool AccountManager::DeleteAccountAllowedQosFromMap_(const std::string& name,
                                                     const std::string& qos) {
  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    CRANE_ERROR(
        "Operating on a non-existent account '{}', please check this item in "
        "database and restart cranectld",
        name);
    return false;
  }

  if (std::find(account->allowed_qos_list.begin(),
                account->allowed_qos_list.end(),
                qos) == account->allowed_qos_list.end()) {
    return false;
  }

  for (const auto& child : account->child_accounts) {
    DeleteAccountAllowedQosFromMap_(child, qos);
  }
  util::write_lock_guard user_guard(m_rw_user_mutex_);
  for (const auto& user : account->users) {
    DeleteUserAllowedQosOfAllPartitionFromMap_(user, name, qos);
  }
  m_account_map_[name]->allowed_qos_list.remove(qos);
  if (account->default_qos == qos) {
    m_account_map_[name]->default_qos = account->allowed_qos_list.empty()
                                            ? ""
                                            : account->allowed_qos_list.front();
  }
  return true;
}

int AccountManager::DeleteUserAllowedQosOfAllPartitionFromDB_(
    const std::string& name, const std::string& account,
    const std::string& qos) {
  const User* p = GetExistedUserInfoNoLock_(name);
  if (!p) {
    CRANE_ERROR(
        "Operating on a non-existent user '{}', please check this item in "
        "database and restart cranectld",
        name);
    return false;
  }
  if (!p->account_map.contains(account)) {
    CRANE_ERROR("User '{}' doesn't belong to account '{}'", name, account);
    return false;
  }
  User user(*p);

  int change_num = 0;
  for (auto& [par, pair] :
       user.account_map[account].allowed_partition_qos_map) {
    auto iter = std::find(pair.second.begin(), pair.second.end(), qos);
    if (iter != pair.second.end()) {
      pair.second.remove(qos);
      change_num++;
    }

    if (pair.first == qos) {
      pair.first = pair.second.empty() ? "" : pair.second.front();
    }
  }
  g_db_client->UpdateUser(user);
  return change_num;
}

bool AccountManager::DeleteUserAllowedQosOfAllPartitionFromMap_(
    const std::string& name, const std::string& account,
    const std::string& qos) {
  const User* p = GetExistedUserInfoNoLock_(name);
  if (!p) {
    CRANE_ERROR(
        "Operating on a non-existent user '{}', please check this item in "
        "database and restart cranectld",
        name);
    return false;
  }
  if (!p->account_map.contains(account)) {
    CRANE_ERROR("User '{}' doesn't belong to account '{}'", name, account);
    return false;
  }
  User user(*p);

  for (auto& [par, qos_pair] :
       user.account_map[account].allowed_partition_qos_map) {
    qos_pair.second.remove(qos);
    if (qos_pair.first == qos) {
      qos_pair.first = qos_pair.second.empty() ? "" : qos_pair.second.front();
    }
  }
  m_user_map_[user.name]->account_map[account].allowed_partition_qos_map =
      user.account_map[account].allowed_partition_qos_map;
  return true;
}

bool AccountManager::DeleteAccountAllowedPartitionFromDB_(
    const std::string& name, const std::string& partition) {
  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    CRANE_ERROR(
        "Operating on a non-existent account '{}', please check this item in "
        "database and restart cranectld",
        name);
    return false;
  }

  auto iter = std::find(account->allowed_partition.begin(),
                        account->allowed_partition.end(), partition);
  if (iter == account->allowed_partition.end()) {
    return false;
  }

  for (const auto& child : account->child_accounts) {
    DeleteAccountAllowedPartitionFromDB_(child, partition);
  }
  util::write_lock_guard user_guard(m_rw_user_mutex_);
  for (const auto& user : account->users) {
    DeleteUserAllowedPartitionFromDB_(user, name, partition);
  }

  g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$pull",
                               account->name, "allowed_partition", partition);
  return true;
}

bool AccountManager::DeleteAccountAllowedPartitionFromMap_(
    const std::string& name, const std::string& partition) {
  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    CRANE_ERROR(
        "Operating on a non-existent account '{}', please check this item in "
        "database and restart cranectld",
        name);
    return false;
  }

  if (std::find(account->allowed_partition.begin(),
                account->allowed_partition.end(),
                partition) == account->allowed_partition.end()) {
    return false;
  }

  for (const auto& child : account->child_accounts) {
    DeleteAccountAllowedPartitionFromMap_(child, partition);
  }
  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  for (const auto& user : account->users) {
    for (const auto& qos : m_user_map_[user]
                               ->account_map[name]
                               .allowed_partition_qos_map[partition]
                               .second) {
      m_qos_map_[qos]->reference_count -= 1;
    }
    m_user_map_[user]->account_map[name].allowed_partition_qos_map.erase(
        partition);
  }
  m_account_map_[account->name]->allowed_partition.remove(partition);

  return true;
}

/**
 * @param name
 * @param account
 * @param partition
 * @note Recursive functions that operate on the Mongodb should be called
 * externally as a whole transaction, and should not use transactions internally
 * @return
 */
bool AccountManager::DeleteUserAllowedPartitionFromDB_(
    const std::string& name, const std::string& account,
    const std::string& partition) {
  const User* user = GetExistedUserInfoNoLock_(name);
  if (!user) {
    CRANE_ERROR(
        "Operating on a non-existent user '{}', please check this item in "
        "database and restart cranectld",
        name);
    return false;
  }
  if (!user->account_map.contains(account)) {
    CRANE_ERROR("User '{}' doesn't belong to account '{}'", name, account);
    return false;
  }

  if (!user->account_map.at(account).allowed_partition_qos_map.contains(
          partition)) {
    return false;
  }

  bool res = true;
  res &= g_db_client->UpdateEntityOne(
      Ctld::MongodbClient::EntityType::USER, "$unset", name,
      "account_map." + account + ".allowed_partition_qos_map." + partition,
      std::string(""));
  for (const auto& qos : user->account_map.at(account)
                             .allowed_partition_qos_map.at(partition)
                             .second) {
    res &= IncQosReferenceCountInDb(qos, -1);
  }

  return res;
}

bool AccountManager::PaternityTestNoLock_(const std::string& parent,
                                          const std::string& child) {
  for (const auto& child_of_account : m_account_map_[parent]->child_accounts) {
    if (child_of_account == child ||
        PaternityTestNoLock_(child_of_account, child)) {
      return true;
    }
  }
  return false;
}

}  // namespace Ctld