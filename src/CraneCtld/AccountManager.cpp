#include "AccountManager.h"

namespace Ctld {

AccountManager::AccountManager() { InitDataMap_(); }

AccountManager::Result AccountManager::AddUser(User&& new_user) {
  // Avoid duplicate insertion
  User find_user;
  m_rw_user_mutex_.lock_upgrade();

  bool find_user_res = GetUserInfoNoLock_(new_user.name, &find_user);
  if (find_user_res && !find_user.deleted) {
    m_rw_user_mutex_.unlock_upgrade();
    return Result{false,
                  fmt::format("The user {} already exists in the database",
                              new_user.name)};
  }

  if (new_user.account.empty()) {
    // User must specify an account
    m_rw_user_mutex_.unlock_upgrade();
    return Result{false, fmt::format("Please specify the user's account")};
  }

  // Check whether the user's account exists
  Account find_account;
  m_rw_account_mutex_.lock_upgrade();
  if (!GetExistedAccountInfoNoLock_(new_user.account, &find_account)) {
    m_rw_account_mutex_.unlock_upgrade();
    m_rw_user_mutex_.unlock_upgrade();
    return Result{false,
                  fmt::format("The account {} doesn't exist in the database",
                              new_user.account)};
  }

  std::list<std::string>& parent_allowed_partition =
      find_account.allowed_partition;
  if (!new_user.allowed_partition_qos_map.empty()) {
    // Check if user's allowed partition is a subset of parent's allowed
    // partition
    for (auto&& [partition, qos] : new_user.allowed_partition_qos_map) {
      if (std::find(parent_allowed_partition.begin(),
                    parent_allowed_partition.end(),
                    partition) != parent_allowed_partition.end()) {
        qos.first = find_account.default_qos;
        qos.second = std::list<std::string>{find_account.allowed_qos_list};
      } else {
        m_rw_account_mutex_.unlock_upgrade();
        m_rw_user_mutex_.unlock_upgrade();
        return Result{false,
                      fmt::format("Partition {} is not allowed in account {}",
                                  partition, find_account.name)};
      }
    }
  } else {
    // Inherit
    for (const auto& partition : parent_allowed_partition) {
      new_user.allowed_partition_qos_map[partition] =
          std::pair<std::string, std::list<std::string>>{
              find_account.default_qos,
              std::list<std::string>{find_account.allowed_qos_list}};
    }
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        // Update the user's account's users_list
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::Account,
                                     "$addToSet", new_user.account, "users",
                                     new_user.name);

        if (find_user_res) {
          // There is a same user but was deleted,here will delete the original
          // user and overwrite it with the same name
          g_db_client->UpdateUser(new_user);

        } else {
          // Insert the new user
          g_db_client->InsertUser(new_user);
        }
      };

  m_rw_account_mutex_.unlock_upgrade_and_lock();
  m_rw_user_mutex_.unlock_upgrade_and_lock();

  if (!g_db_client->CommitTransaction(callback)) {
    m_rw_account_mutex_.unlock();
    m_rw_user_mutex_.unlock();
    return Result{false, "Fail to update data in database"};
  }

  m_account_map_[new_user.account]->users.emplace_back(new_user.name);
  m_user_map_[new_user.name] = std::make_unique<User>(new_user);

  m_rw_account_mutex_.unlock();
  m_rw_user_mutex_.unlock();
  return Result{true};
}

AccountManager::Result AccountManager::AddAccount(Account&& new_account) {
  // Avoid duplicate insertion
  Account find_account;
  m_rw_account_mutex_.lock_upgrade();

  bool find_account_res =
      GetAccountInfoNoLock_(new_account.name, &find_account);
  if (find_account_res && !find_account.deleted) {
    m_rw_account_mutex_.unlock_upgrade();
    return Result{false,
                  fmt::format("The account {} already exists in the database",
                              new_account.name)};
  }

  util::read_lock_guard read_qos_lock_guard(m_rw_qos_mutex_);
  for (const auto& qos : new_account.allowed_qos_list) {
    Qos find_qos;
    if (!GetExistedQosInfoNoLock_(qos, &find_qos)) {
      m_rw_account_mutex_.unlock_upgrade();
      return Result{false, fmt::format("Qos {} not existed", qos)};
    }
  }

  if (!new_account.parent_account.empty()) {
    // Check whether the account's parent account exists
    Account find_parent;
    if (!GetExistedAccountInfoNoLock_(new_account.parent_account,
                                      &find_parent)) {
      m_rw_account_mutex_.unlock_upgrade();
      return Result{
          false,
          fmt::format("The parent account {} doesn't exist in the database",
                      new_account.parent_account)};
    }

    if (new_account.allowed_partition.empty()) {
      // Inherit
      new_account.allowed_partition =
          std::list<std::string>{find_parent.allowed_partition};
    } else {
      // check allowed partition authority
      for (const auto& par : new_account.allowed_partition) {
        if (std::find(find_parent.allowed_partition.begin(),
                      find_parent.allowed_partition.end(), par) ==
            find_parent.allowed_partition.end()) {  // not find
          m_rw_account_mutex_.unlock_upgrade();
          return Result{
              false,
              fmt::format(
                  "Parent account {} does not have access to partition {}",
                  new_account.parent_account, par)};
        }
      }
    }

    if (new_account.allowed_qos_list.empty()) {
      // Inherit
      new_account.allowed_qos_list =
          std::list<std::string>{find_parent.allowed_qos_list};
    } else {
      // check allowed qos list authority
      for (const auto& qos : new_account.allowed_qos_list) {
        if (std::find(find_parent.allowed_qos_list.begin(),
                      find_parent.allowed_qos_list.end(),
                      qos) == find_parent.allowed_qos_list.end()) {  // not find
          m_rw_account_mutex_.unlock_upgrade();
          return Result{
              false,
              fmt::format("Parent account {} does not have access to qos {}",
                          new_account.parent_account, qos)};
        }
      }
    }
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        if (!new_account.parent_account.empty()) {
          // update the parent account's child_account_list
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::Account,
                                       "$addToSet", new_account.parent_account,
                                       "child_accounts", new_account.name);
        }

        if (find_account_res) {
          // There is a same account but was deleted,here will delete the
          // original account and overwrite it with the same name
          g_db_client->UpdateAccount(new_account);
        } else {
          // Insert the new account
          g_db_client->InsertAccount(new_account);
        }
      };

  m_rw_account_mutex_.unlock_upgrade_and_lock();

  if (!g_db_client->CommitTransaction(callback)) {
    m_rw_account_mutex_.unlock();
    return Result{false, "Fail to update data in database"};
  }
  if (!new_account.parent_account.empty()) {
    m_account_map_[new_account.parent_account]->child_accounts.emplace_back(
        new_account.name);
  }
  m_account_map_[new_account.name] = std::make_unique<Account>(new_account);

  m_rw_account_mutex_.unlock();
  return Result{true};
}

AccountManager::Result AccountManager::AddQos(const Qos& new_qos) {
  Qos find_qos;
  m_rw_qos_mutex_.lock_upgrade();

  bool find_qos_res = GetQosInfoNoLock_(new_qos.name, &find_qos);
  if (find_qos_res && !find_qos.deleted) {
    m_rw_qos_mutex_.unlock_upgrade();
    return Result{false, fmt::format("Qos {} already exists in the database",
                                     new_qos.name)};
  }

  m_rw_qos_mutex_.unlock_upgrade_and_lock();
  if (find_qos_res) {
    // There is a same qos but was deleted,here will delete the original
    // qos and overwrite it with the same name
    if (!g_db_client->UpdateQos(new_qos)) {
      m_rw_qos_mutex_.unlock();
      return Result{false, "Can't update the deleted qos to database"};
    }
  } else {
    // Insert the new qos
    if (!g_db_client->InsertQos(new_qos)) {
      m_rw_qos_mutex_.unlock();
      return Result{false, "Can't insert the new qos to database"};
    }
  }

  m_qos_map_[new_qos.name] = std::make_unique<Qos>(new_qos);
  m_rw_qos_mutex_.unlock();
  return Result{true};
}

AccountManager::Result AccountManager::DeleteUser(const std::string& name) {
  User user;
  m_rw_user_mutex_.lock_upgrade();

  if (!GetExistedUserInfoNoLock_(name, &user)) {
    m_rw_user_mutex_.unlock_upgrade();
    return Result{false,
                  fmt::format("User {} doesn't exist in the database", name)};
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        // delete form the parent account's users list
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::Account,
                                     "$pull", user.account, "users", name);

        // Delete the user
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::User, "$set",
                                     name, "deleted", true);
      };

  m_rw_account_mutex_.lock();
  m_rw_user_mutex_.unlock_upgrade_and_lock();

  if (!g_db_client->CommitTransaction(callback)) {
    m_rw_account_mutex_.unlock();
    m_rw_user_mutex_.unlock();
    return Result{false, "Fail to update data in database"};
  }
  m_account_map_[user.account]->users.remove(name);
  m_user_map_[name]->deleted = true;

  m_rw_account_mutex_.unlock();
  m_rw_user_mutex_.unlock();
  return Result{true};
}

AccountManager::Result AccountManager::DeleteAccount(const std::string& name) {
  Account account;
  m_rw_account_mutex_.lock_upgrade();

  if (!GetExistedAccountInfoNoLock_(name, &account)) {
    m_rw_account_mutex_.unlock_upgrade();
    return Result{
        false, fmt::format("Account {} doesn't exist in the database", name)};
  }

  if (!account.child_accounts.empty() || !account.users.empty()) {
    m_rw_account_mutex_.unlock_upgrade();
    return Result{false, "This account has child account or users"};
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        if (!account.parent_account.empty()) {
          // delete form the parent account's child account list
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::Account,
                                       "$pull", account.parent_account,
                                       "child_accounts", name);
        }
        // Delete the account
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::Account, "$set",
                                     name, "deleted", true);
      };

  m_rw_account_mutex_.unlock_upgrade_and_lock();

  if (!g_db_client->CommitTransaction(callback)) {
    m_rw_account_mutex_.unlock();
    return Result{false, "Fail to update data in database"};
  }

  if (!account.parent_account.empty()) {
    m_account_map_[account.parent_account]->child_accounts.remove(name);
  }
  m_account_map_[name]->deleted = true;

  m_rw_account_mutex_.unlock();
  return Result{true};
}

AccountManager::Result AccountManager::DeleteQos(const std::string& name) {
  Qos qos;
  m_rw_qos_mutex_.lock_upgrade();

  if (!GetExistedQosInfoNoLock_(name, &qos)) {
    m_rw_qos_mutex_.unlock_upgrade();
    return Result{false, fmt::format("Qos {} not exists in database", name)};
  }

  m_rw_qos_mutex_.unlock_upgrade_and_lock();
  if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::Qos, "$set",
                                    name, "deleted", true)) {
    m_rw_qos_mutex_.unlock();
    return Result{false, fmt::format("Delete qos {} failed", name)};
  }
  m_qos_map_[name]->deleted = true;

  m_rw_qos_mutex_.unlock();
  return Result{true};
}

bool AccountManager::GetExistedUserInfo(const std::string& name, User* user) {
  util::read_lock_guard guard(m_rw_user_mutex_);
  return GetExistedUserInfoNoLock_(name, user);
}

void AccountManager::GetAllUserInfo(std::list<User>* user_list) {
  util::read_lock_guard guard(m_rw_user_mutex_);
  for (const auto& [name, user] : m_user_map_) {
    user_list->push_back(*user);
  }
}

bool AccountManager::GetExistedAccountInfo(const std::string& name,
                                           Account* account) {
  util::read_lock_guard guard(m_rw_account_mutex_);
  return GetExistedAccountInfoNoLock_(name, account);
}

void AccountManager::GetAllAccountInfo(std::list<Account>* account_list) {
  util::read_lock_guard guard(m_rw_account_mutex_);
  for (const auto& [name, account] : m_account_map_) {
    account_list->push_back(*account);
  }
}

bool AccountManager::GetExistedQosInfo(const std::string& name, Qos* qos) {
  util::read_lock_guard guard(m_rw_qos_mutex_);
  return GetExistedQosInfoNoLock_(name, qos);
}

void AccountManager::GetAllQosInfo(std::list<Qos>* qos_list) {
  util::read_lock_guard guard(m_rw_qos_mutex_);
  for (const auto& [name, qos] : m_qos_map_) {
    qos_list->push_back(*qos);
  }
}

AccountManager::Result AccountManager::ModifyUser(
    const crane::grpc::ModifyEntityRequest_OperatorType& operatorType,
    const std::string& name, const std::string& partition,
    const std::string& lhs, const std::string& rhs) {
  User user;
  m_rw_user_mutex_.lock_upgrade();

  if (!GetExistedUserInfoNoLock_(name, &user)) {
    m_rw_user_mutex_.unlock_upgrade();
    return Result{false, fmt::format("Unknown user {}", name)};
  }

  Account account;
  util::read_lock_guard read_account_lock_guard(m_rw_account_mutex_);

  GetExistedAccountInfoNoLock_(user.account, &account);
  switch (operatorType) {
    case crane::grpc::ModifyEntityRequest_OperatorType_Add:
      if (lhs == "allowed_partition") {
        // TODO: check if new partition existed

        // check if account has access to new partition
        if (std::find(account.allowed_partition.begin(),
                      account.allowed_partition.end(),
                      rhs) == account.allowed_partition.end()) {
          m_rw_user_mutex_.unlock_upgrade();
          return Result{
              false,
              fmt::format(
                  "User {}'s account {} not allow to use the partition {}",
                  name, user.account, rhs)};
        }

        // check if add item already the user's allowed partition
        for (const auto& par : user.allowed_partition_qos_map) {
          if (rhs == par.first) {
            m_rw_user_mutex_.unlock_upgrade();
            return Result{false, fmt::format("The partition {} is already in "
                                             "user {}'s allowed partition",
                                             rhs, name)};
          }
        }

        // Update the map
        user.allowed_partition_qos_map[rhs] =
            std::pair<std::string, std::list<std::string>>{
                account.default_qos,
                std::list<std::string>{account.allowed_qos_list}};

      } else if (lhs == "allowed_qos_list") {
        // check if qos existed
        Qos find_qos;
        m_rw_qos_mutex_.lock_shared();

        if (!GetExistedQosInfoNoLock_(rhs, &find_qos)) {
          m_rw_qos_mutex_.unlock_shared();
          m_rw_user_mutex_.unlock_upgrade();
          return Result{false, fmt::format("Qos {} not existed", rhs)};
        }

        m_rw_qos_mutex_.unlock_shared();
        // check if account has access to new qos
        if (std::find(account.allowed_qos_list.begin(),
                      account.allowed_qos_list.end(),
                      rhs) == account.allowed_qos_list.end()) {
          m_rw_user_mutex_.unlock_upgrade();
          return Result{
              false,
              fmt::format("Sorry, your account not allow to use the qos {}",
                          rhs)};
        }

        // check if add item already the user's allowed qos
        if (partition.empty()) {
          // add to all partition
          bool is_changed = false;
          for (auto& [par, qos] : user.allowed_partition_qos_map) {
            std::list<std::string>& list = qos.second;
            if (std::find(list.begin(), list.end(), rhs) == list.end()) {
              list.emplace_back(rhs);
              is_changed = true;
            }
          }

          if (!is_changed) {
            m_rw_user_mutex_.unlock_upgrade();
            return Result{false, fmt::format("Qos {} is already in user {}'s "
                                             "allowed qos of all partition",
                                             rhs, name)};
          }
        } else {
          // add to exacted partition
          auto iter = user.allowed_partition_qos_map.find(partition);
          if (iter == user.allowed_partition_qos_map.end()) {
            m_rw_user_mutex_.unlock_upgrade();
            return Result{
                false, fmt::format(
                           "Partition {} is not in user {}'s allowed partition",
                           partition, name)};
          }

          std::list<std::string>& list = iter->second.second;
          if (std::find(list.begin(), list.end(), rhs) != list.end()) {
            m_rw_user_mutex_.unlock_upgrade();
            return Result{false, fmt::format("Qos {} is already in user {}'s "
                                             "allowed qos of partition {}",
                                             rhs, name, partition)};
          }
          list.push_back(rhs);
        }
      } else {
        m_rw_user_mutex_.unlock_upgrade();
        return Result{false, fmt::format("Field {} can't be added", lhs)};
      }
      break;

    case crane::grpc::ModifyEntityRequest_OperatorType_Overwrite:
      if (lhs == "admin_level") {
        User::AdminLevel new_level;
        if (rhs == "none") {
          new_level = User::None;
        } else if (rhs == "operator") {
          new_level = User::Operator;
        } else if (rhs == "admin") {
          new_level = User::Admin;
        }

        if (new_level == user.admin_level) {
          m_rw_user_mutex_.unlock_upgrade();
          return Result{false,
                        fmt::format("User {} is already a {} role", name, rhs)};
        }
        user.admin_level = new_level;
      }
      //      else if (lhs == "account") {
      //        Account* new_account = GetAccountInfo(rhs);
      //        if(new_account == nullptr){
      //          return Result{ false, fmt::format("Account {} not existed",
      //          rhs)};
      //        }
      //        account->users.remove(name);
      //        new_account->users.emplace_back(name);
      //      }
      else if (lhs == "default_qos") {
        if (partition.empty()) {
          bool is_changed = false;
          for (auto& [par, qos] : user.allowed_partition_qos_map) {
            if (std::find(qos.second.begin(), qos.second.end(), rhs) !=
                    qos.second.end() &&
                rhs != qos.first) {
              is_changed = true;
              qos.first = rhs;
            }
          }

          if (!is_changed) {
            m_rw_user_mutex_.unlock_upgrade();
            return Result{false, fmt::format("Qos {} not in allowed qos list "
                                             "or is already the default qos",
                                             rhs)};
          }
        } else {
          auto iter = user.allowed_partition_qos_map.find(partition);

          if (std::find(iter->second.second.begin(), iter->second.second.end(),
                        rhs) == iter->second.second.end()) {
            m_rw_user_mutex_.unlock_upgrade();
            return Result{false,
                          fmt::format("Qos {} not in allowed qos list", rhs)};
          }

          if (iter->second.first == rhs) {
            m_rw_user_mutex_.unlock_upgrade();
            return Result{
                false, fmt::format("Qos {} is already the default qos", rhs)};
          }
          iter->second.first = rhs;
        }
      } else {
        m_rw_user_mutex_.unlock_upgrade();
        return Result{false, fmt::format("Field {} can't be set", lhs)};
      }
      break;

    case crane::grpc::ModifyEntityRequest_OperatorType_Delete:
      if (lhs == "allowed_partition") {
        auto iter = user.allowed_partition_qos_map.find(rhs);
        if (iter == user.allowed_partition_qos_map.end()) {
          m_rw_user_mutex_.unlock_upgrade();
          return Result{
              false,
              fmt::format(
                  "Partition {} is not in user {}'s allowed partition list",
                  rhs, name)};
        }
        user.allowed_partition_qos_map.erase(iter);

      } else if (lhs == "allowed_qos_list") {
        if (partition.empty()) {
          bool is_changed = false;
          for (auto& [par, qos] : user.allowed_partition_qos_map) {
            if (std::find(qos.second.begin(), qos.second.end(), rhs) !=
                    qos.second.end() &&
                qos.first != rhs) {
              is_changed = true;
              qos.second.remove(rhs);
            }
          }

          if (!is_changed) {
            m_rw_user_mutex_.unlock_upgrade();
            return Result{
                false,
                fmt::format("Qos {} not in allowed qos list or is default qos",
                            rhs)};
          }
        } else {
          auto iter = user.allowed_partition_qos_map.find(partition);

          if (iter == user.allowed_partition_qos_map.end()) {
            m_rw_user_mutex_.unlock_upgrade();
            return Result{
                false, fmt::format("Partition {} not in allowed partition list",
                                   partition)};
          }

          if (std::find(iter->second.second.begin(), iter->second.second.end(),
                        rhs) == iter->second.second.end()) {
            m_rw_user_mutex_.unlock_upgrade();
            return Result{
                false,
                fmt::format("Qos {} not in allowed qos list of partition {}",
                            rhs, partition)};
          }

          if (rhs == iter->second.first) {
            m_rw_user_mutex_.unlock_upgrade();
            return Result{
                false,
                fmt::format(
                    "Qos {} is default qos of partition {},can't be deleted",
                    rhs, partition)};
          }
          iter->second.second.remove(rhs);
        }
      } else {
        m_rw_user_mutex_.unlock_upgrade();
        return Result{false, fmt::format("Field {} can't be deleted", lhs)};
      }
      break;
    default:
      m_rw_user_mutex_.unlock_upgrade();
      return Result{false, fmt::format("Unknown field {}", lhs)};
      break;
  }

  // Update to database
  m_rw_user_mutex_.unlock_upgrade_and_lock();

  if (!g_db_client->UpdateUser(user)) {
    m_rw_user_mutex_.unlock();
    return Result{false, "Fail to update data in database"};
  }
  *m_user_map_[name] = user;

  m_rw_user_mutex_.unlock();
  return Result{true};
}

AccountManager::Result AccountManager::ModifyAccount(
    const crane::grpc::ModifyEntityRequest_OperatorType& operatorType,
    const std::string& name, const std::string& lhs, const std::string& rhs) {
  std::string opt;
  Account account;
  m_rw_account_mutex_.lock_upgrade();

  if (!GetExistedAccountInfoNoLock_(name, &account)) {
    m_rw_account_mutex_.unlock_upgrade();
    return Result{false, fmt::format("Unknown account {}", name)};
  }

  switch (operatorType) {
    case crane::grpc::ModifyEntityRequest_OperatorType_Add:
      opt = "$addToSet";
      if (lhs == "allowed_partition") {
        // TODO: check if the partition existed

        // Check if parent account has access to the partition
        if (!account.parent_account.empty()) {
          Account parent;
          GetExistedAccountInfoNoLock_(account.parent_account, &parent);
          if (std::find(parent.allowed_partition.begin(),
                        parent.allowed_partition.end(),
                        rhs) == parent.allowed_partition.end()) {
            m_rw_account_mutex_.unlock_upgrade();

            return Result{
                false,
                fmt::format(
                    "Parent account {} does not have access to partition {}",
                    account.parent_account, rhs)};
          }
        }

        if (std::find(account.allowed_partition.begin(),
                      account.allowed_partition.end(),
                      rhs) != account.allowed_partition.end()) {
          m_rw_account_mutex_.unlock_upgrade();

          return Result{
              false,
              fmt::format("Partition {} is already in allowed partition list",
                          rhs)};
        }

        m_rw_account_mutex_.unlock_upgrade_and_lock();

        if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::Account,
                                          opt, name, lhs, rhs)) {
          m_rw_account_mutex_.unlock();
          return Result{false, "Can't update the  database"};
        }
        m_account_map_[name]->allowed_partition.emplace_back(rhs);

        m_rw_account_mutex_.unlock();
        return Result{true};

      } else if (lhs == "allowed_qos_list") {
        // check if the qos existed
        Qos qos;
        m_rw_qos_mutex_.lock_shared();

        if (!GetExistedQosInfoNoLock_(rhs, &qos)) {
          m_rw_qos_mutex_.unlock_shared();
          m_rw_account_mutex_.unlock_upgrade();
          return Result{false, fmt::format("Qos {} not existed", rhs)};
        }
        m_rw_qos_mutex_.unlock_shared();

        // Check if parent account has access to the qos
        if (!account.parent_account.empty()) {
          Account parent;
          GetExistedAccountInfoNoLock_(account.parent_account, &parent);

          if (std::find(parent.allowed_qos_list.begin(),
                        parent.allowed_qos_list.end(),
                        rhs) == parent.allowed_qos_list.end()) {
            m_rw_account_mutex_.unlock_upgrade();
            return Result{
                false,
                fmt::format("Parent account {} does not have access to qos {}",
                            account.parent_account, rhs)};
          }
        }

        if (std::find(account.allowed_qos_list.begin(),
                      account.allowed_qos_list.end(),
                      rhs) != account.allowed_qos_list.end()) {
          m_rw_account_mutex_.unlock_upgrade();
          return Result{
              false, fmt::format("Qos {} is already in allowed qos list", rhs)};
        }

        m_rw_account_mutex_.unlock_upgrade_and_lock();
        if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::Account,
                                          opt, name, lhs, rhs)) {
          m_rw_account_mutex_.unlock();
          return Result{false, "Can't update the  database"};
        }
        m_account_map_[name]->allowed_qos_list.emplace_back(rhs);

        m_rw_account_mutex_.unlock();
        return Result{true};

      } else {
        m_rw_account_mutex_.unlock_upgrade();
        return Result{false, fmt::format("Field {} can't be added", lhs)};
      }
      break;

    case crane::grpc::ModifyEntityRequest_OperatorType_Overwrite:
      opt = "$set";
      if (lhs == "description") {
        if (rhs == account.description) {
          m_rw_account_mutex_.unlock_upgrade();
          return Result{false, "Description content not change"};
        }

        m_rw_account_mutex_.unlock_upgrade_and_lock();

        if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::Account,
                                          opt, name, lhs, rhs)) {
          m_rw_account_mutex_.unlock();
          return Result{false, "Can't update the  database"};
        }
        m_account_map_[name]->description = rhs;

        m_rw_account_mutex_.unlock();
        return Result{true};
      }
      //      else if (lhs == "parent_account") {
      //
      //        if(!g_db_client->UpdateEntityOne(MongodbClient::EntityType::Account,
      //        opt,
      //                                          name, lhs, rhs)){
      //          return Result{ false , "Can't update the  database"};
      //        }
      //        else {
      //          return Result{true};
      //        }
      //      }
      else if (lhs == "default_qos") {
        if (account.default_qos == rhs) {
          m_rw_account_mutex_.unlock_upgrade();
          return Result{false,
                        fmt::format("Qos {} is already the default qos", rhs)};
        }

        if (std::find(account.allowed_qos_list.begin(),
                      account.allowed_qos_list.end(),
                      rhs) == account.allowed_qos_list.end()) {
          m_rw_account_mutex_.unlock_upgrade();
          return Result{false,
                        fmt::format("Qos {} not in allowed qos list", rhs)};
        }

        m_rw_account_mutex_.unlock_upgrade_and_lock();

        if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::Account,
                                          opt, name, lhs, rhs)) {
          m_rw_account_mutex_.unlock();
          return Result{false, "Can't update the  database"};
        }
        m_account_map_[name]->default_qos = rhs;

        m_rw_account_mutex_.unlock();
        return Result{true};

      } else {
        m_rw_account_mutex_.unlock_upgrade();
        return Result{false, fmt::format("Field {} can't be set", lhs)};
      }
      break;

    case crane::grpc::ModifyEntityRequest_OperatorType_Delete:
      if (lhs == "allowed_partition") {
        m_rw_account_mutex_.unlock_upgrade_and_lock();
        m_rw_user_mutex_.lock();

        mongocxx::client_session::with_transaction_cb callback =
            [&](mongocxx::client_session* session) {
              DeleteAccountAllowedPartitionFromDB_(account.name, rhs);
            };

        if (!g_db_client->CommitTransaction(callback)) {
          m_rw_user_mutex_.unlock();
          m_rw_account_mutex_.unlock();
          return Result{false, "Fail to update data in database"};
        }
        DeleteAccountAllowedPartitionFromMap_(account.name, rhs);

        m_rw_user_mutex_.unlock();
        m_rw_account_mutex_.unlock();

      } else if (lhs == "allowed_qos_list") {
        m_rw_account_mutex_.unlock_upgrade_and_lock();
        m_rw_user_mutex_.lock();

        auto iter = std::find(account.allowed_qos_list.begin(),
                              account.allowed_qos_list.end(), rhs);

        if (iter == account.allowed_qos_list.end()) {
          m_rw_user_mutex_.unlock();
          m_rw_account_mutex_.unlock();
          return Result{
              false,
              fmt::format("Qos {} is not in account {}'s allowed qos list", rhs,
                          name)};
        }
        if (IsDefaultQosOfAnyNode(account, rhs) &&
            true) {  // true replace !force
          m_rw_user_mutex_.unlock();
          m_rw_account_mutex_.unlock();

          return Result{
              false,
              fmt::format("Someone is using qos {} as default qos", rhs)};
        }

        mongocxx::client_session::with_transaction_cb callback =
            [&](mongocxx::client_session* session) {
              DeleteAccountAllowedQosFromDB_(account.name, rhs);
            };

        if (!g_db_client->CommitTransaction(callback)) {
          m_rw_user_mutex_.unlock();
          m_rw_account_mutex_.unlock();
          return Result{false, "Fail to update data in database"};
        }
        DeleteAccountAllowedQosFromMap_(account.name, rhs);

        m_rw_user_mutex_.unlock();
        m_rw_account_mutex_.unlock();
        return Result{true};
      } else {
        m_rw_account_mutex_.unlock_upgrade();
        return Result{false, fmt::format("Field {} can't be deleted", lhs)};
      }
      break;
    default:
      m_rw_account_mutex_.unlock_upgrade();
      return Result{true};
      break;
  }
  return Result{true};
}

AccountManager::Result AccountManager::ModifyQos(const std::string& name,
                                                 const std::string& lhs,
                                                 const std::string& rhs) {
  Qos qos;
  m_rw_qos_mutex_.lock_upgrade();

  if (!GetExistedQosInfoNoLock_(name, &qos)) {
    m_rw_qos_mutex_.unlock_upgrade();
    return Result{false, fmt::format("Qos {} not existed in database", name)};
  }

  m_rw_qos_mutex_.unlock_upgrade_and_lock();

  if (lhs == "description") {
    qos.description = rhs;
    if (!g_db_client->UpdateEntityOne(MongodbClient::Qos, "$set", name, lhs,
                                      rhs)) {
      m_rw_qos_mutex_.unlock();
      return Result{false, "Fail to update the database"};
    }
  } else {
    qos.priority = std::stoi(rhs);
    if (!g_db_client->UpdateEntityOne(MongodbClient::Qos, "$set", name, lhs,
                                      std::stoi(rhs))) {
      m_rw_qos_mutex_.unlock();
      return Result{false, "Fail to update the database"};
    }
  }
  *m_qos_map_[name] = qos;

  m_rw_qos_mutex_.unlock();
  return Result{true};
}

bool AccountManager::CheckUserPermissionToPartition(
    const std::string& name, const std::string& partition) {
  User user;
  if (!GetExistedUserInfo(name, &user)) {
    return false;
  }

  if (user.allowed_partition_qos_map.find(partition) !=
      user.allowed_partition_qos_map.end()) {
    return true;
  }
  return false;
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
bool AccountManager::GetUserInfoNoLock_(const std::string& name, User* user) {
  auto find_res = m_user_map_.find(name);
  if (find_res == m_user_map_.end() || user == nullptr) {
    return false;
  } else {
    *user = *find_res->second;
    return true;
  }
}

/*
 * Get the user info form mongodb and deletion flag marked false
 */
bool AccountManager::GetExistedUserInfoNoLock_(const std::string& name,
                                               User* user) {
  if (GetUserInfoNoLock_(name, user) && !user->deleted) {
    return true;
  } else {
    return false;
  }
}

bool AccountManager::GetAccountInfoNoLock_(const std::string& name,
                                           Account* account) {
  auto find_res = m_account_map_.find(name);
  if (find_res == m_account_map_.end() || account == nullptr) {
    return false;
  } else {
    *account = *find_res->second;
    return true;
  }
}

bool AccountManager::GetExistedAccountInfoNoLock_(const std::string& name,
                                                  Account* account) {
  if (GetAccountInfoNoLock_(name, account) && !account->deleted) {
    return true;
  } else {
    return false;
  }
}

bool AccountManager::GetQosInfoNoLock_(const std::string& name, Qos* qos) {
  auto find_res = m_qos_map_.find(name);
  if (find_res == m_qos_map_.end() || qos == nullptr) {
    return false;
  } else {
    *qos = *find_res->second;
    return true;
  }
}

bool AccountManager::GetExistedQosInfoNoLock_(const std::string& name,
                                              Qos* qos) {
  if (GetQosInfoNoLock_(name, qos) && !qos->deleted) {
    return true;
  } else {
    return false;
  }
}

bool AccountManager::IsDefaultQosOfAnyNode(const Account& account,
                                           const std::string& qos) {
  bool res = false;
  if (account.default_qos == qos) {
    return true;
  }
  for (const auto& child : account.child_accounts) {
    Account child_account;
    GetExistedAccountInfoNoLock_(child, &child_account);
    res |= IsDefaultQosOfAnyNode(child_account, qos);
  }
  for (const auto& user : account.users) {
    User child_user;
    GetExistedUserInfoNoLock_(user, &child_user);
    res |= IsDefaultQosOfAnyPartition(child_user, qos);
  }
  return res;
}

bool AccountManager::IsDefaultQosOfAnyPartition(const User& user,
                                                const std::string& qos) {
  return std::any_of(user.allowed_partition_qos_map.begin(),
                     user.allowed_partition_qos_map.end(),
                     [&qos](const auto& p) { return p.second.first == qos; });
}

bool AccountManager::DeleteAccountAllowedQosFromDB_(const std::string& name,
                                                    const std::string& qos) {
  Account account;
  GetExistedAccountInfoNoLock_(name, &account);

  auto iter = std::find(account.allowed_qos_list.begin(),
                        account.allowed_qos_list.end(), qos);
  if (iter == account.allowed_qos_list.end()) {
    return false;
  }

  for (const auto& child : account.child_accounts) {
    DeleteAccountAllowedQosFromDB_(child, qos);
  }
  for (const auto& user : account.users) {
    DeleteUserAllowedQosOfAllPartitionFromDB_(user, qos, true);
  }
  g_db_client->UpdateEntityOne(MongodbClient::EntityType::Account, "$pull",
                               account.name, "allowed_qos_list", qos);
  return true;
}

bool AccountManager::DeleteAccountAllowedQosFromMap_(const std::string& name,
                                                     const std::string& qos) {
  Account account;
  GetExistedAccountInfoNoLock_(name, &account);

  if (std::find(account.allowed_qos_list.begin(),
                account.allowed_qos_list.end(),
                qos) == account.allowed_qos_list.end()) {
    return false;
  }

  for (const auto& child : account.child_accounts) {
    DeleteAccountAllowedQosFromMap_(child, qos);
  }
  for (const auto& user : account.users) {
    DeleteUserAllowedQosOfAllPartitionFromMap_(user, qos, true);
  }
  m_account_map_[account.name]->allowed_qos_list.remove(qos);
  return true;
}

bool AccountManager::DeleteUserAllowedQosOfAllPartitionFromDB_(
    const std::string& name, const std::string& qos, bool force) {
  User user;
  GetExistedUserInfoNoLock_(name, &user);

  bool isDefault = IsDefaultQosOfAnyPartition(user, qos);
  if (isDefault && !force) {
    return false;
  }

  for (auto& [par, qos_pair] : user.allowed_partition_qos_map) {
    qos_pair.second.remove(qos);
    if (qos_pair.first == qos) {
      if (qos_pair.second.empty()) {
        qos_pair.first = "";
      } else {
        qos_pair.first = qos_pair.second.front();
      }
    }
  }
  g_db_client->UpdateUser(user);
  return true;
}

bool AccountManager::DeleteUserAllowedQosOfAllPartitionFromMap_(
    const std::string& name, const std::string& qos, bool force) {
  User user;
  GetExistedUserInfoNoLock_(name, &user);

  bool isDefault = IsDefaultQosOfAnyPartition(user, qos);
  if (isDefault && !force) {
    return false;
  }
  for (auto& [par, qos_pair] : user.allowed_partition_qos_map) {
    qos_pair.second.remove(qos);
    if (qos_pair.first == qos) {
      if (qos_pair.second.empty()) {
        qos_pair.first = "";
      } else {
        qos_pair.first = qos_pair.second.front();
      }
    }
  }
  *m_user_map_[user.name] = user;
  return true;
}

bool AccountManager::DeleteAccountAllowedPartitionFromDB_(
    const std::string& name, const std::string& partition) {
  Account account;
  GetExistedAccountInfoNoLock_(name, &account);

  auto iter = std::find(account.allowed_partition.begin(),
                        account.allowed_partition.end(), partition);
  if (iter == account.allowed_partition.end()) {
    return false;
  }

  for (const auto& child : account.child_accounts) {
    DeleteAccountAllowedPartitionFromDB_(child, partition);
  }
  for (const auto& user : account.users) {
    DeleteUserAllowedPartitionFromDB_(user, partition);
  }

  g_db_client->UpdateEntityOne(MongodbClient::EntityType::Account, "$pull",
                               account.name, "allowed_partition", partition);
  return true;
}

bool AccountManager::DeleteAccountAllowedPartitionFromMap_(
    const std::string& name, const std::string& partition) {
  Account account;
  GetExistedAccountInfoNoLock_(name, &account);

  if (std::find(account.allowed_partition.begin(),
                account.allowed_partition.end(),
                partition) == account.allowed_partition.end()) {
    return false;
  }

  for (const auto& child : account.child_accounts) {
    DeleteAccountAllowedPartitionFromMap_(child, partition);
  }
  for (const auto& user : account.users) {
    DeleteUserAllowedPartitionFromMap_(user, partition);
  }
  m_account_map_[account.name]->allowed_partition.remove(partition);

  return true;
}

bool AccountManager::DeleteUserAllowedPartitionFromDB_(
    const std::string& name, const std::string& partition) {
  User user;
  GetExistedUserInfoNoLock_(name, &user);

  auto iter = user.allowed_partition_qos_map.find(partition);
  if (iter == user.allowed_partition_qos_map.end()) {
    return false;
  }
  user.allowed_partition_qos_map.erase(iter);
  g_db_client->UpdateUser(user);
  return true;
}

bool AccountManager::DeleteUserAllowedPartitionFromMap_(
    const std::string& name, const std::string& partition) {
  User user;
  GetExistedUserInfoNoLock_(name, &user);

  if (user.allowed_partition_qos_map.find(partition) ==
      user.allowed_partition_qos_map.end()) {
    return false;
  }
  m_user_map_[user.name]->allowed_partition_qos_map.erase(partition);
  return true;
}

}  // namespace Ctld