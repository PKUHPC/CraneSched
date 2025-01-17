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

#include "AccountManager.h"

#include "protos/PublicDefs.pb.h"
#include "range/v3/algorithm/contains.hpp"

namespace Ctld {

AccountManager::AccountManager() { InitDataMap_(); }

AccountManager::CraneExpected<void> AccountManager::AddUser(
    uint32_t uid, const User& new_user) {
  CraneExpected<void> result;

  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();

  result = CheckIfUserHasHigherPrivThan_(*op_user, new_user.admin_level);
  if (!result) return result;

  // User must specify an account
  if (new_user.default_account.empty())
    return std::unexpected(CraneErrCode::ERR_NO_ACCOUNT_SPECIFIED);

  const std::string& object_account = new_user.default_account;
  const std::string& name = new_user.name;

  // Avoid duplicate insertion
  const User* stale_user = GetUserInfoNoLock_(name);
  if (stale_user && !stale_user->deleted) {
    if (stale_user->account_to_attrs_map.contains(object_account))
      return std::unexpected(CraneErrCode::ERR_USER_DUPLICATE_ACCOUNT);
  }

  // Check whether the account exists
  const Account* account = GetExistedAccountInfoNoLock_(object_account);
  if (!account) return std::unexpected(CraneErrCode::ERR_INVALID_ACCOUNT);

  // Check if user's allowed partition is a subset of parent's allowed
  // partition
  for (const auto& [partition, qos] :
       new_user.account_to_attrs_map.at(object_account)
           .allowed_partition_qos_map) {
    result = CheckPartitionIsAllowedNoLock_(account, partition, false, true);
    if (!result) return result;
  }

  return AddUser_(new_user, account, stale_user);
}

AccountManager::CraneExpected<void> AccountManager::AddAccount(
    uint32_t uid, const Account& new_account) {
  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    util::read_lock_guard account_guard(m_rw_account_mutex_);
    CraneExpected<void> result;
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();
    // When creating an account without a parent, the permission level of
    // op_user must be Operator or higher.
    if (new_account.parent_account.empty())
      result = CheckIfUserHasHigherPrivThan_(*op_user, User::None);
    else {
      result = CheckIfUserHasPermOnAccountNoLock_(
          *op_user, new_account.parent_account, false);
      if (!result && result.error() == CraneErrCode::ERR_INVALID_ACCOUNT)
        return std::unexpected(CraneErrCode::ERR_INVALID_PARENTACCOUNT);
    }
    if (!result) return result;
  }

  util::write_lock_guard account_guard(m_rw_account_mutex_);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const std::string& name = new_account.name;
  // Avoid duplicate insertion
  const Account* stale_account = GetAccountInfoNoLock_(name);
  if (stale_account && !stale_account->deleted)
    return std::unexpected(CraneErrCode::ERR_DUPLICATE_ACCOUNT);

  const Account* find_parent = nullptr;
  if (!new_account.parent_account.empty()) {
    // Check whether the account's parent account exists
    find_parent = GetExistedAccountInfoNoLock_(new_account.parent_account);
    if (!find_parent)
      return std::unexpected(CraneErrCode::ERR_INVALID_PARENTACCOUNT);

    // check allowed partition authority
    for (const auto& par : new_account.allowed_partition) {
      if (!ranges::contains(find_parent->allowed_partition, par))  // not find
        return std::unexpected(CraneErrCode::ERR_PARENT_ALLOWED_PARTITION);
    }

    // check allowed qos list authority
    for (const auto& qos : new_account.allowed_qos_list) {
      if (!ranges::contains(find_parent->allowed_qos_list, qos))  // not find
        return std::unexpected(CraneErrCode::ERR_PARENT_ALLOWED_QOS);
    }
  } else {  // No parent account
    // Check whether partitions exists
    for (const auto& p : new_account.allowed_partition) {
      if (!g_config.Partitions.contains(p)) {
        return std::unexpected(CraneErrCode::ERR_INVALID_PARTITION);
      }
    }

    for (const auto& qos : new_account.allowed_qos_list) {
      const Qos* find_qos = GetExistedQosInfoNoLock_(qos);
      if (!find_qos) return std::unexpected(CraneErrCode::ERR_INVALID_QOS);
    }
  }

  // Check if default_qos is in allowed_qos_list.
  if (!new_account.default_qos.empty()) {
    if (!ranges::contains(new_account.allowed_qos_list,
                          new_account.default_qos))
      return std::unexpected(CraneErrCode::ERR_ALLOWED_DEFAULT_QOS);
  }

  return AddAccount_(new_account, find_parent, stale_account);
}

AccountManager::CraneExpected<void> AccountManager::AddQos(uint32_t uid,
                                                           const Qos& new_qos) {
  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();

    auto result = CheckIfUserHasHigherPrivThan_(*op_user, User::None);
    if (!result) return result;
  }

  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const Qos* find_qos = GetQosInfoNoLock_(new_qos.name);

  // Avoid duplicate insertion
  if (find_qos && !find_qos->deleted)
    return std::unexpected(CraneErrCode::ERR_DB_DUPLICATE_QOS);

  return AddQos_(new_qos, find_qos);
}

AccountManager::CraneExpected<void> AccountManager::DeleteUser(
    uint32_t uid, const std::string& name, const std::string& account) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();

  const User* user = GetExistedUserInfoNoLock_(name);
  if (!user) return std::unexpected(CraneErrCode::ERR_INVALID_USER);

  auto result = CheckIfUserHasHigherPrivThan_(*op_user, user->admin_level);
  if (!result) return result;

  // The provided account is invalid.
  if (!account.empty() && !user->account_to_attrs_map.contains(account))
    return std::unexpected(CraneErrCode::ERR_USER_ACCOUNT_MISMATCH);

  return DeleteUser_(*user, account);
}

AccountManager::CraneExpected<void> AccountManager::DeleteAccount(
    uint32_t uid, const std::string& name) {
  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    util::read_lock_guard account_guard(m_rw_account_mutex_);

    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();

    auto result = CheckIfUserHasPermOnAccountNoLock_(*op_user, name, false);
    if (!result) return result;
  }

  util::write_lock_guard account_guard(m_rw_account_mutex_);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) return std::unexpected(CraneErrCode::ERR_INVALID_ACCOUNT);

  // Cannot delete because there are child nodes.
  if (!account->child_accounts.empty() || !account->users.empty())
    return std::unexpected(CraneErrCode::ERR_DELETE_ACCOUNT);

  return DeleteAccount_(*account);
}

AccountManager::CraneExpected<void> AccountManager::DeleteQos(
    uint32_t uid, const std::string& name) {
  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();

    auto result = CheckIfUserHasHigherPrivThan_(*op_user, User::None);
    if (!result) return result;
  }

  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const Qos* qos = GetExistedQosInfoNoLock_(name);
  if (!qos) return std::unexpected(CraneErrCode::ERR_INVALID_QOS);

  // Cannot delete because the QoS is still in use.
  if (qos->reference_count != 0)
    return std::unexpected(CraneErrCode::ERR_DELETE_QOS);

  return DeleteQos_(name);
}

AccountManager::UserMutexSharedPtr AccountManager::GetExistedUserInfo(
    const std::string& name) {
  m_rw_user_mutex_.lock_shared();

  const User* user = GetExistedUserInfoNoLock_(name);
  if (!user) {
    m_rw_user_mutex_.unlock_shared();
    return UserMutexSharedPtr{nullptr};
  }

  return UserMutexSharedPtr{user, &m_rw_user_mutex_};
}

AccountManager::UserMapMutexSharedPtr AccountManager::GetAllUserInfo() {
  m_rw_user_mutex_.lock_shared();

  if (m_user_map_.empty()) {
    m_rw_user_mutex_.unlock_shared();
    return UserMapMutexSharedPtr{nullptr};
  }

  return UserMapMutexSharedPtr{&m_user_map_, &m_rw_user_mutex_};
}

AccountManager::AccountMutexSharedPtr AccountManager::GetExistedAccountInfo(
    const std::string& name) {
  m_rw_account_mutex_.lock_shared();

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    m_rw_account_mutex_.unlock_shared();
    return AccountMutexSharedPtr{nullptr};
  }

  return AccountMutexSharedPtr{account, &m_rw_account_mutex_};
}

AccountManager::AccountMapMutexSharedPtr AccountManager::GetAllAccountInfo() {
  m_rw_account_mutex_.lock_shared();

  if (m_account_map_.empty()) {
    m_rw_account_mutex_.unlock_shared();
    return AccountMapMutexSharedPtr{nullptr};
  }

  return AccountMapMutexSharedPtr{&m_account_map_, &m_rw_account_mutex_};
}

AccountManager::QosMutexSharedPtr AccountManager::GetExistedQosInfo(
    const std::string& name) {
  m_rw_qos_mutex_.lock_shared();

  const Qos* qos = GetExistedQosInfoNoLock_(name);
  if (!qos) {
    m_rw_qos_mutex_.unlock_shared();
    return QosMutexSharedPtr{nullptr};
  }

  return QosMutexSharedPtr{qos, &m_rw_qos_mutex_};
}

AccountManager::QosMapMutexSharedPtr AccountManager::GetAllQosInfo() {
  m_rw_qos_mutex_.lock_shared();

  if (m_qos_map_.empty()) {
    m_rw_qos_mutex_.unlock_shared();
    return QosMapMutexSharedPtr{nullptr};
  }

  return QosMapMutexSharedPtr{&m_qos_map_, &m_rw_qos_mutex_};
}

AccountManager::CraneExpected<void> AccountManager::QueryUserInfo(
    uint32_t uid, const std::string& name,
    std::unordered_map<uid_t, User>* res_user_map) {
  util::read_lock_guard user_guard(m_rw_user_mutex_);
  CraneExpected<void> result{};

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();

  if (name.empty()) {  // Query all users that can be queried.
    // Operators and above can query all users.
    if (CheckIfUserHasHigherPrivThan_(*op_user, User::None)) {
      // The rules for querying user information are the same as those for
      // querying accounts
      for (const auto& [user_name, user] : m_user_map_) {
        if (user->deleted) continue;
        res_user_map->try_emplace(user->uid, *user);
      }
    } else {
      util::read_lock_guard account_guard(m_rw_account_mutex_);
      std::queue<std::string> queue;
      for (const auto& [acct, item] : op_user->account_to_attrs_map) {
        queue.push(acct);
        while (!queue.empty()) {
          std::string father = queue.front();
          for (const auto& user : m_account_map_.at(father)->users) {
            res_user_map->try_emplace(m_user_map_.at(user)->uid,
                                      *(m_user_map_.at(user)));
          }
          queue.pop();
          for (const auto& child : m_account_map_.at(father)->child_accounts) {
            queue.push(child);
          }
        }
      }
    }
  } else {  // Query the specified user information.
    util::read_lock_guard account_guard(m_rw_account_mutex_);
    const User* user = GetExistedUserInfoNoLock_(name);
    result = CheckIfUserHasPermOnUserNoLock_(*op_user, user, true);
    if (!result) return result;
    res_user_map->try_emplace(user->uid, *user);
  }

  return result;
}

AccountManager::CraneExpected<void> AccountManager::QueryAccountInfo(
    uint32_t uid, const std::string& name,
    std::unordered_map<std::string, Account>* res_account_map) {
  User res_user;
  CraneExpected<void> result{};

  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    util::read_lock_guard account_guard(m_rw_account_mutex_);
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();
    if (!name.empty()) {
      result = CheckIfUserHasPermOnAccountNoLock_(*op_user, name, true);
      if (!result) return result;
    }
    res_user = *op_user;
  }

  util::read_lock_guard account_guard(m_rw_account_mutex_);
  if (name.empty()) {
    if (CheckIfUserHasHigherPrivThan_(res_user, User::None)) {
      // If an administrator user queries account information, all
      // accounts are returned, variable user_account not used
      for (const auto& [name, account] : m_account_map_) {
        if (account->deleted) continue;
        res_account_map->try_emplace(account->name, *account);
      }
    } else {
      // Otherwise, only all sub-accounts under your own accounts will be
      // returned
      std::queue<std::string> queue;
      for (const auto& [acct, item] : res_user.account_to_attrs_map) {
        // Z->A->B--->C->E
        //    |->D    |->F
        // If we query account C, [Z,A,B,C,E,F] is included.
        std::string p_name = m_account_map_.at(acct)->parent_account;
        while (!p_name.empty()) {
          res_account_map->try_emplace(p_name, *(m_account_map_.at(p_name)));
          p_name = m_account_map_.at(p_name)->parent_account;
        }

        queue.push(acct);
        while (!queue.empty()) {
          std::string father = queue.front();
          const auto& account_content = m_account_map_.at(father);
          res_account_map->try_emplace(account_content->name,
                                       *(account_content));
          queue.pop();
          for (const auto& child : account_content->child_accounts) {
            queue.push(child);
          }
        }
      }
    }
  } else {
    const Account* account = GetAccountInfoNoLock_(name);
    if (!account) return std::unexpected(CraneErrCode::ERR_INVALID_ACCOUNT);
    res_account_map->try_emplace(name, *account);
  }

  return result;
}

AccountManager::CraneExpected<void> AccountManager::QueryQosInfo(
    uint32_t uid, const std::string& name,
    std::unordered_map<std::string, Qos>* res_qos_map) {
  User res_user;
  CraneExpected<void> result{};
  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();
    res_user = *op_user;
  }

  util::read_lock_guard qos_guard(m_rw_qos_mutex_);
  if (name.empty()) {
    if (CheckIfUserHasHigherPrivThan_(res_user, User::None)) {
      for (const auto& [name, qos] : m_qos_map_) {
        if (qos->deleted) continue;
        res_qos_map->try_emplace(name, *qos);
      }
    } else {
      for (const auto& [acct, item] : res_user.account_to_attrs_map) {
        for (const auto& [part, part_qos_map] :
             item.allowed_partition_qos_map) {
          for (const auto& qos : part_qos_map.second) {
            res_qos_map->try_emplace(qos, *(m_qos_map_.at(qos)));
          }
        }
      }
    }
  } else {
    const Qos* qos = GetExistedQosInfoNoLock_(name);
    if (!qos) return std::unexpected(CraneErrCode::ERR_INVALID_QOS);

    if (res_user.admin_level < User::Operator) {
      bool found = false;
      for (const auto& [acct, item] : res_user.account_to_attrs_map) {
        for (const auto& [part, part_qos_map] :
             item.allowed_partition_qos_map) {
          for (const auto& part_qos : part_qos_map.second) {
            if (part_qos == name) found = true;
          }
        }
      }
      if (!found) return std::unexpected(CraneErrCode::ERR_ALLOWED_QOS);
    }
    res_qos_map->try_emplace(name, *qos);
  }

  return result;
}

AccountManager::CraneExpected<void> AccountManager::ModifyAdminLevel(
    uint32_t uid, const std::string& name, const std::string& value) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* user = GetExistedUserInfoNoLock_(name);
  if (!user) return std::unexpected(CraneErrCode::ERR_INVALID_USER);

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();

  auto result = CheckIfUserHasHigherPrivThan_(*op_user, user->admin_level);
  if (!result) return std::unexpected(CraneErrCode::ERR_PERMISSION_USER);

  User::AdminLevel new_level;
  if (value == "none")
    new_level = User::None;
  else if (value == "operator")
    new_level = User::Operator;
  else if (value == "admin")
    new_level = User::Admin;
  else
    return std::unexpected(CraneErrCode::ERR_INVALID_ADMIN_LEVEL);

  if (op_user->admin_level <= new_level)
    return std::unexpected(CraneErrCode::ERR_PERMISSION_USER);

  if (new_level == user->admin_level) return {};

  return SetUserAdminLevel_(name, new_level);
}

AccountManager::CraneExpected<void> AccountManager::ModifyUserDefaultQos(
    uint32_t uid, const std::string& name, const std::string& partition,
    const std::string& account, const std::string& value) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* p_target_user = GetExistedUserInfoNoLock_(name);
  CraneExpected<void> result{};
  // Account might be empty. In that case, ues user->default_account.
  std::string actual_account = account;
  {
    util::read_lock_guard account_guard(m_rw_account_mutex_);
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();
    result = CheckIfUserHasPermOnUserOfAccountNoLock_(*op_user, p_target_user,
                                                      &actual_account, false);
    if (!result) return result;
  }

  result = CheckSetUserDefaultQosNoLock_(*p_target_user, actual_account,
                                         partition, value);
  if (!result) return result;

  return SetUserDefaultQos_(*p_target_user, actual_account, partition, value);
}

AccountManager::CraneExpected<void> AccountManager::ModifyUserAllowedPartition(
    crane::grpc::OperationType operation_type, uint32_t uid,
    const std::string& name, const std::string& account,
    const std::string& value) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::read_lock_guard account_guard(m_rw_account_mutex_);

  const User* p = GetExistedUserInfoNoLock_(name);
  CraneExpected<void> result{};
  std::string actual_account = account;
  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();
  result = CheckIfUserHasPermOnUserOfAccountNoLock_(*op_user, p,
                                                    &actual_account, false);
  if (!result) return result;

  const Account* account_ptr = GetExistedAccountInfoNoLock_(actual_account);

  switch (operation_type) {
  case crane::grpc::OperationType::Add:
    result = CheckAddUserAllowedPartitionNoLock_(p, account_ptr, value);
    return !result ? result : AddUserAllowedPartition_(*p, *account_ptr, value);
  case crane::grpc::OperationType::Overwrite:
    result = CheckSetUserAllowedPartitionNoLock_(account_ptr, value);
    return !result ? result : SetUserAllowedPartition_(*p, *account_ptr, value);
  default:
    std::unreachable();
  }

  return result;
}

AccountManager::CraneExpected<void> AccountManager::ModifyUserAllowedQos(
    crane::grpc::OperationType operation_type, uint32_t uid,
    const std::string& name, const std::string& partition,
    const std::string& account, const std::string& value, bool force) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::read_lock_guard account_guard(m_rw_account_mutex_);
  util::read_lock_guard qos_guard(m_rw_qos_mutex_);

  const User* p = GetExistedUserInfoNoLock_(name);
  CraneExpected<void> result{};
  std::string actual_account = account;
  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();
  result = CheckIfUserHasPermOnUserOfAccountNoLock_(*op_user, p,
                                                    &actual_account, false);
  if (!result) return result;

  const Account* account_ptr = GetExistedAccountInfoNoLock_(actual_account);

  switch (operation_type) {
  case crane::grpc::OperationType::Add:
    result = CheckAddUserAllowedQosNoLock_(p, account_ptr, partition, value);
    return !result ? result
                   : AddUserAllowedQos_(*p, *account_ptr, partition, value);
  case crane::grpc::OperationType::Overwrite:
    result =
        CheckSetUserAllowedQosNoLock_(p, account_ptr, partition, value, force);
    return !result
               ? result
               : SetUserAllowedQos_(*p, *account_ptr, partition, value, force);
  default:
    std::unreachable();
  }

  return result;
}

AccountManager::CraneExpected<void> AccountManager::DeleteUserAllowedPartition(
    uint32_t uid, const std::string& name, const std::string& account,
    const std::string& value) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* p = GetExistedUserInfoNoLock_(name);
  CraneExpected<void> result;
  // Account might be empty. In that case, ues user->default_account.
  std::string actual_account = account;
  {
    util::read_lock_guard account_guard(m_rw_account_mutex_);
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();
    result = CheckIfUserHasPermOnUserOfAccountNoLock_(*op_user, p,
                                                      &actual_account, false);
    if (!result) return result;
  }

  result = CheckDeleteUserAllowedPartitionNoLock_(*p, actual_account, value);
  if (!result) return result;

  return DeleteUserAllowedPartition_(*p, actual_account, value);
}

AccountManager::CraneExpected<void> AccountManager::DeleteUserAllowedQos(
    uint32_t uid, const std::string& name, const std::string& partition,
    const std::string& account, const std::string& value, bool force) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* p = GetExistedUserInfoNoLock_(name);
  CraneExpected<void> result;
  std::string actual_account = account;
  {
    util::read_lock_guard account_guard(m_rw_account_mutex_);
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();
    result = CheckIfUserHasPermOnUserOfAccountNoLock_(*op_user, p,
                                                      &actual_account, false);
    if (!result) return result;
  }

  result = CheckDeleteUserAllowedQosNoLock_(*p, actual_account, partition,
                                            value, force);
  if (!result) return result;

  return DeleteUserAllowedQos_(*p, value, actual_account, partition, force);
}

AccountManager::CraneExpected<void> AccountManager::ModifyAccount(
    crane::grpc::OperationType operation_type, uint32_t uid,
    const std::string& name, crane::grpc::ModifyField modify_field,
    const std::string& value, bool force) {
  CraneExpected<void> result{};

  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    util::read_lock_guard account_guard(m_rw_account_mutex_);

    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();

    result = CheckIfUserHasPermOnAccountNoLock_(*op_user, name, false);
    if (!result) return result;
  }

  switch (operation_type) {
  case crane::grpc::OperationType::Add: {
    util::write_lock_guard account_guard(m_rw_account_mutex_);
    const Account* account = GetExistedAccountInfoNoLock_(name);
    switch (modify_field) {
    case crane::grpc::ModifyField::Partition: {
      result = CheckAddAccountAllowedPartitionNoLock_(account, value);
      return !result ? result : AddAccountAllowedPartition_(name, value);
    }

    case crane::grpc::ModifyField::Qos: {
      util::write_lock_guard qos_guard(m_rw_qos_mutex_);
      result = CheckAddAccountAllowedQosNoLock_(account, value);
      return !result ? result : AddAccountAllowedQos_(*account, value);
    }

    default:
      std::unreachable();
    }
  }

  case crane::grpc::OperationType::Overwrite:
    switch (modify_field) {
    case crane::grpc::ModifyField::Description: {
      util::write_lock_guard account_guard(m_rw_account_mutex_);
      const Account* account = GetExistedAccountInfoNoLock_(name);
      result = CheckSetAccountDescriptionNoLock_(account);
      return !result ? result : SetAccountDescription_(name, value);
    }
    case crane::grpc::ModifyField::Partition: {
      util::write_lock_guard user_guard(m_rw_user_mutex_);
      util::write_lock_guard account_guard(m_rw_account_mutex_);
      const Account* account = GetExistedAccountInfoNoLock_(name);
      result = CheckSetAccountAllowedPartitionNoLock_(account, value, force);
      return !result ? result : SetAccountAllowedPartition_(*account, value);
    }

    case crane::grpc::ModifyField::Qos: {
      util::write_lock_guard user_guard(m_rw_user_mutex_);
      util::write_lock_guard account_guard(m_rw_account_mutex_);
      util::write_lock_guard qos_guard(m_rw_qos_mutex_);
      const Account* account = GetExistedAccountInfoNoLock_(name);
      result = CheckSetAccountAllowedQosNoLock_(account, value, force);
      return !result ? result : SetAccountAllowedQos_(*account, value);
    }
    case crane::grpc::ModifyField::DefaultQos: {
      util::write_lock_guard account_guard(m_rw_account_mutex_);
      const Account* account = GetExistedAccountInfoNoLock_(name);
      result = CheckSetAccountDefaultQosNoLock_(account, value);
      return !result ? result : SetAccountDefaultQos_(*account, value);
    }

    default:
      std::unreachable();
    }

  case crane::grpc::OperationType::Delete:
    switch (modify_field) {
    case crane::grpc::ModifyField::Partition: {
      util::write_lock_guard user_guard(m_rw_user_mutex_);
      util::write_lock_guard account_guard(m_rw_account_mutex_);
      const Account* account = GetExistedAccountInfoNoLock_(name);
      result = CheckDeleteAccountAllowedPartitionNoLock_(account, value, force);
      return !result ? result : DeleteAccountAllowedPartition_(*account, value);
    }

    case crane::grpc::ModifyField::Qos: {
      util::write_lock_guard user_guard(m_rw_user_mutex_);
      util::write_lock_guard account_guard(m_rw_account_mutex_);
      util::write_lock_guard qos_guard(m_rw_qos_mutex_);
      const Account* account = GetExistedAccountInfoNoLock_(name);
      result = CheckDeleteAccountAllowedQosNoLock_(account, value, force);
      return !result ? result : DeleteAccountAllowedQos_(*account, value);
    }

    default:
      std::unreachable();
    }

  default:
    std::unreachable();
  }

  return result;
}

AccountManager::CraneExpected<void> AccountManager::ModifyQos(
    uint32_t uid, const std::string& name,
    crane::grpc::ModifyField modify_field, const std::string& value) {
  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();

    auto result = CheckIfUserHasHigherPrivThan_(*op_user, User::None);
    if (!result) return result;
  }

  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const Qos* p = GetExistedQosInfoNoLock_(name);
  if (!p) return std::unexpected(CraneErrCode::ERR_INVALID_QOS);

  std::string item = "";
  switch (modify_field) {
  case crane::grpc::ModifyField::Description:
    item = "description";
    break;
  case crane::grpc::ModifyField::Priority:
    item = "priority";
    break;
  case crane::grpc::ModifyField::MaxJobsPerUser:
    item = "max_jobs_per_user";
    break;
  case crane::grpc::ModifyField::MaxCpusPerUser:
    item = "max_cpus_per_user";
    break;
  case crane::grpc::ModifyField::MaxTimeLimitPerTask:
    item = "max_time_limit_per_task";
    break;
  default:
    std::unreachable();
  }

  bool value_is_number{false};
  int64_t value_number;
  if (item != Qos::FieldStringOfDescription()) {
    bool ok = util::ConvertStringToInt64(value, &value_number);
    if (!ok) return std::unexpected(CraneErrCode::ERR_CONVERT_TO_INTERGER);

    value_is_number = true;

    if (item == Qos::FieldStringOfMaxTimeLimitPerTask() &&
        !CheckIfTimeLimitSecIsValid(value_number))
      return std::unexpected(CraneErrCode::ERR_TIME_LIMIT);
  }

  mongocxx::client_session::with_transaction_cb callback;
  if (item == "description") {
    // Update to database
    callback = [&](mongocxx::client_session* session) {
      g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS, "$set", name,
                                   item, value);
    };

  } else {
    /* uint32 Type Stores data based on long(int64_t) */
    callback = [&](mongocxx::client_session* session) {
      g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS, "$set", name,
                                   item, value_number);
    };
  }

  if (!g_db_client->CommitTransaction(callback))
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);

  // To avoid frequently judging item, obtain the modified qos of the
  // Mongodb
  Qos qos;
  g_db_client->SelectQos("name", name, &qos);
  *m_qos_map_[name] = std::move(qos);

  return {};
}

AccountManager::CraneExpected<void> AccountManager::BlockAccount(
    uint32_t uid, const std::string& name, bool block) {
  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    util::read_lock_guard account_guard(m_rw_account_mutex_);

    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();

    auto result = CheckIfUserHasPermOnAccountNoLock_(*op_user, name, false);
    if (!result) return result;
  }

  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) return std::unexpected(CraneErrCode::ERR_INVALID_ACCOUNT);

  if (account->blocked == block) return {};

  return BlockAccount_(name, block);
}

AccountManager::CraneExpected<void> AccountManager::BlockUser(
    uint32_t uid, const std::string& name, const std::string& account,
    bool block) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  const User* user = GetExistedUserInfoNoLock_(name);
  std::string actual_account = account;
  {
    util::read_lock_guard account_guard(m_rw_account_mutex_);
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();
    auto result = CheckIfUserHasPermOnUserOfAccountNoLock_(
        *op_user, user, &actual_account, false);
    if (!result) return result;
  }

  if (user->account_to_attrs_map.at(actual_account).blocked == block) return {};

  return BlockUser_(name, actual_account, block);
}

bool AccountManager::CheckUserPermissionToPartition(
    const std::string& name, const std::string& account,
    const std::string& partition) {
  UserMutexSharedPtr user_share_ptr = GetExistedUserInfo(name);
  if (!user_share_ptr) return false;

  if (user_share_ptr->uid == 0 ||
      user_share_ptr->account_to_attrs_map.at(account)
          .allowed_partition_qos_map.contains(partition)) {
    return true;
  }

  return false;
}

std::expected<void, std::string> AccountManager::CheckIfUserOfAccountIsEnabled(
    const std::string& user, const std::string& account) {
  util::read_lock_guard user_guard(m_rw_user_mutex_);
  util::read_lock_guard account_guard(m_rw_account_mutex_);
  std::string account_name = account;

  do {
    const Account* account_ptr = GetExistedAccountInfoNoLock_(account_name);
    if (account_ptr->blocked) {
      return std::unexpected(
          fmt::format("Ancestor account '{}' is blocked", account_ptr->name));
    }
    account_name = account_ptr->parent_account;
  } while (!account_name.empty());

  const User* user_ptr = GetExistedUserInfoNoLock_(user);
  if (user_ptr->account_to_attrs_map.at(account).blocked) {
    return std::unexpected(fmt::format("User '{}' is blocked", user_ptr->name));
  }
  return {};
}

std::expected<void, std::string> AccountManager::CheckAndApplyQosLimitOnTask(
    const std::string& user, const std::string& account, TaskInCtld* task) {
  util::read_lock_guard user_guard(m_rw_user_mutex_);
  util::read_lock_guard qos_guard(m_rw_qos_mutex_);

  const User* user_share_ptr = GetExistedUserInfoNoLock_(user);
  if (!user_share_ptr) {
    return std::unexpected(fmt::format("Unknown user '{}'", user));
  }

  if (task->uid != 0) {
    auto partition_it = user_share_ptr->account_to_attrs_map.at(account)
                            .allowed_partition_qos_map.find(task->partition_id);
    if (partition_it == user_share_ptr->account_to_attrs_map.at(account)
                            .allowed_partition_qos_map.end())
      return std::unexpected("Partition is not allowed for this user.");

    if (task->qos.empty()) {
      // Default qos
      task->qos = partition_it->second.first;
      if (task->qos.empty())
        return std::unexpected(
            fmt::format("The user '{}' has no QOS available for this partition "
                        "'{}' to be used",
                        task->Username(), task->partition_id));
    } else {
      // Check whether task.qos in the qos list
      if (!ranges::contains(partition_it->second.second, task->qos))
        return std::unexpected(fmt::format(
            "The qos '{}' you set is not in partition's allowed qos list",
            task->qos));
    }
  } else {
    if (task->qos.empty()) {
      task->qos = kUnlimitedQosName;
    }
  }

  const Qos* qos_share_ptr = GetExistedQosInfoNoLock_(task->qos);
  if (!qos_share_ptr)
    return std::unexpected(fmt::format("Unknown QOS '{}'", task->qos));

  task->qos_priority = qos_share_ptr->priority;

  if (task->time_limit >= absl::Seconds(kTaskMaxTimeLimitSec)) {
    task->time_limit = qos_share_ptr->max_time_limit_per_task;
  } else if (task->time_limit > qos_share_ptr->max_time_limit_per_task)
    return std::unexpected("time-limit reached the user's limit.");

  if (static_cast<double>(task->cpus_per_task) >
      qos_share_ptr->max_cpus_per_user)
    return std::unexpected("cpus-per-task reached the user's limit.");

  return {};
}

std::expected<void, std::string> AccountManager::CheckUidIsAdmin(uint32_t uid) {
  util::read_lock_guard user_guard(m_rw_user_mutex_);
  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) {
    return std::unexpected("User is not a user of Crane.");
  }
  const User* user_ptr = user_result.value();

  if (user_ptr->admin_level >= User::Operator) return {};

  return std::unexpected("User has insufficient privilege.");
}

AccountManager::CraneExpected<void> AccountManager::CheckIfUidHasPermOnUser(
    uint32_t uid, const std::string& username, bool read_only_priv) {
  util::read_lock_guard user_guard(m_rw_user_mutex_);
  util::read_lock_guard account_guard(m_rw_account_mutex_);

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();

  const User* user = GetExistedUserInfoNoLock_(username);

  return CheckIfUserHasPermOnUserNoLock_(*op_user, user, read_only_priv);
}

  CraneErrCodeExpected<void> AccountManager::CheckModifyPartitionAllowedOrDeniedAccounts(
        uint32_t uid, const std::string& partition_name,
        const std::unordered_set<std::string>& accounts) {
  CraneErrCodeExpected<void> result{};

  util::read_lock_guard user_guard(m_rw_user_mutex_);
  util::read_lock_guard account_guard(m_rw_account_mutex_);

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();

  for (const auto& account_name : accounts) {
    const Account* account = GetAccountInfoNoLock_(account_name);
    result =
        CheckPartitionIsAllowedNoLock_(account, partition_name, false, false);
    if (!result) return std::unexpected(result.error());
  }

  return result;
}

AccountManager::CraneExpected<void>
AccountManager::CheckAddUserAllowedPartitionNoLock_(
    const User* user, const Account* account, const std::string& partition) {
  auto result = CheckPartitionIsAllowedNoLock_(account, partition, false, true);
  if (!result) return result;

  const std::string& account_name = account->name;
  if (user->account_to_attrs_map.at(account_name)
          .allowed_partition_qos_map.contains(partition)) {
    return std::unexpected(CraneErrCode::ERR_DUPLICATE_PARTITION);
  }

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckSetUserAllowedPartitionNoLock_(
    const Account* account, const std::string& partition) {
  auto result = CheckPartitionIsAllowedNoLock_(account, partition, false, true);
  if (!result) return result;

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckAddUserAllowedQosNoLock_(const User* user,
                                              const Account* account,
                                              const std::string& partition,
                                              const std::string& qos_str) {
  auto result = CheckQosIsAllowedNoLock_(account, qos_str, false, true);
  if (!result) return result;
  const std::string& account_name = account->name;
  //  check if add item already the user's allowed qos
  const auto& attrs_in_account_map =
      user->account_to_attrs_map.at(account_name);
  if (partition.empty()) {
    // When the user has no partition, QoS cannot be added.
    if (attrs_in_account_map.allowed_partition_qos_map.empty())
      return std::unexpected(CraneErrCode::ERR_USER_EMPTY_PARTITION);

    bool is_allowed = false;
    for (const auto& [par, pair] :
         attrs_in_account_map.allowed_partition_qos_map) {
      const std::list<std::string>& list = pair.second;
      if (!ranges::contains(list, qos_str)) {
        is_allowed = true;
        break;
      }
    }
    if (!is_allowed) return std::unexpected(CraneErrCode::ERR_DUPLICATE_QOS);
  } else {
    auto iter = attrs_in_account_map.allowed_partition_qos_map.find(partition);
    if (iter == attrs_in_account_map.allowed_partition_qos_map.end())
      return std::unexpected(CraneErrCode::ERR_ALLOWED_PARTITION);
    const std::list<std::string>& list = iter->second.second;
    if (ranges::contains(list, qos_str))
      return std::unexpected(CraneErrCode::ERR_DUPLICATE_QOS);
  }

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckSetUserAllowedQosNoLock_(const User* user,
                                              const Account* account,
                                              const std::string& partition,
                                              const std::string& qos_str,
                                              bool force) {
  auto result = CheckQosIsAllowedNoLock_(account, qos_str, false, true);
  if (!result) return result;
  const std::string& account_name = account->name;
  std::vector<std::string> qos_vec =
      absl::StrSplit(qos_str, ',', absl::SkipEmpty());

  std::unordered_map<std::string,
                     std::pair<std::string, std::list<std::string>>>
      cache_allowed_partition_qos_map;
  const auto& attrs_in_account_map =
      user->account_to_attrs_map.at(account_name);
  if (partition.empty()) {
    cache_allowed_partition_qos_map =
        attrs_in_account_map.allowed_partition_qos_map;
  } else {
    auto iter = attrs_in_account_map.allowed_partition_qos_map.find(partition);
    if (iter == attrs_in_account_map.allowed_partition_qos_map.end()) {
      return std::unexpected(CraneErrCode::ERR_ALLOWED_PARTITION);
    }
    cache_allowed_partition_qos_map.insert({iter->first, iter->second});
  }

  for (const auto& [par, pair] : cache_allowed_partition_qos_map) {
    if (!ranges::contains(qos_vec, pair.first)) {
      if (!force && !pair.first.empty())
        return std::unexpected(CraneErrCode::ERR_SET_ALLOWED_QOS);
    }
  }
  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckSetUserDefaultQosNoLock_(const User& user,
                                              const std::string& account,
                                              const std::string& partition,
                                              const std::string& qos) {
  const auto& attrs_in_account = user.account_to_attrs_map.at(account);
  if (partition.empty()) {
    bool is_allowed = false;
    for (const auto& [par, pair] : attrs_in_account.allowed_partition_qos_map) {
      if (ranges::contains(pair.second, qos) && qos != pair.first) {
        is_allowed = true;
        break;
      }
    }

    if (!is_allowed) return std::unexpected(CraneErrCode::ERR_SET_DEFAULT_QOS);
  } else {
    auto iter = attrs_in_account.allowed_partition_qos_map.find(partition);

    if (iter == attrs_in_account.allowed_partition_qos_map.end())
      return std::unexpected(CraneErrCode::ERR_ALLOWED_PARTITION);

    if (!ranges::contains(iter->second.second, qos))
      return std::unexpected(CraneErrCode::ERR_ALLOWED_QOS);

    if (iter->second.first == qos)
      return std::unexpected(CraneErrCode::ERR_DUPLICATE_DEFAULT_QOS);
  }

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckDeleteUserAllowedPartitionNoLock_(
    const User& user, const std::string& account,
    const std::string& partition) {
  if (!user.account_to_attrs_map.at(account).allowed_partition_qos_map.contains(
          partition))
    return std::unexpected(CraneErrCode::ERR_ALLOWED_PARTITION);

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckDeleteUserAllowedQosNoLock_(const User& user,
                                                 const std::string& account,
                                                 const std::string& partition,
                                                 const std::string& qos,
                                                 bool force) {
  const auto& attrs_in_account = user.account_to_attrs_map.at(account);
  if (partition.empty()) {
    bool is_allowed = false;
    for (const auto& [par, pair] : attrs_in_account.allowed_partition_qos_map) {
      if (ranges::contains(pair.second, qos)) {
        is_allowed = true;
        if (pair.first == qos && !force)
          return std::unexpected(CraneErrCode::ERR_IS_DEFAULT_QOS);
      }
      if (!is_allowed) return std::unexpected(CraneErrCode::ERR_ALLOWED_QOS);
    }
  } else {
    // Delete the qos of a specified partition
    auto iter = attrs_in_account.allowed_partition_qos_map.find(partition);

    if (iter == attrs_in_account.allowed_partition_qos_map.end())
      return std::unexpected(CraneErrCode::ERR_ALLOWED_PARTITION);

    if (!ranges::contains(iter->second.second, qos))
      return std::unexpected(CraneErrCode::ERR_ALLOWED_QOS);

    if (qos == iter->second.first && !force)
      return std::unexpected(CraneErrCode::ERR_IS_DEFAULT_QOS);
  }

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckAddAccountAllowedPartitionNoLock_(
    const Account* account, const std::string& partition) {
  auto result = CheckPartitionIsAllowedNoLock_(account, partition, true, false);
  if (!result) return result;

  if (ranges::contains(account->allowed_partition, partition))
    return std::unexpected(CraneErrCode::ERR_DUPLICATE_PARTITION);

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckAddAccountAllowedQosNoLock_(const Account* account,
                                                 const std::string& qos) {
  auto result = CheckQosIsAllowedNoLock_(account, qos, true, false);
  if (!result) return result;

  if (ranges::contains(account->allowed_qos_list, qos))
    return std::unexpected(CraneErrCode::ERR_DUPLICATE_QOS);

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckSetAccountDescriptionNoLock_(const Account* account) {
  if (!account) return std::unexpected(CraneErrCode::ERR_INVALID_ACCOUNT);

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckSetAccountAllowedPartitionNoLock_(
    const Account* account, const std::string& partitions, bool force) {
  auto result =
      CheckPartitionIsAllowedNoLock_(account, partitions, true, false);
  if (!result) return result;

  std::vector<std::string> partition_vec =
      absl::StrSplit(partitions, ',', absl::SkipEmpty());

  for (const auto& par : account->allowed_partition) {
    if (!ranges::contains(partition_vec, par)) {
      if (!force && IsAllowedPartitionOfAnyNodeNoLock_(account, par))
        return std::unexpected(CraneErrCode::ERR_CHILD_HAS_PARTITION);
    }
  }

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckSetAccountAllowedQosNoLock_(const Account* account,
                                                 const std::string& qos_list,
                                                 bool force) {
  auto result = CheckQosIsAllowedNoLock_(account, qos_list, true, false);
  if (!result) return result;

  std::vector<std::string> qos_vec =
      absl::StrSplit(qos_list, ',', absl::SkipEmpty());

  for (const auto& qos : account->allowed_qos_list) {
    if (!ranges::contains(qos_vec, qos)) {
      if (!force && IsDefaultQosOfAnyNodeNoLock_(account, qos))
        return std::unexpected(CraneErrCode::ERR_SET_ACCOUNT_QOS);
    }
  }

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckSetAccountDefaultQosNoLock_(const Account* account,
                                                 const std::string& qos) {
  auto result = CheckQosIsAllowedNoLock_(account, qos, false, false);
  if (!result) return result;

  if (account->default_qos == qos)
    return std::unexpected(CraneErrCode::ERR_DUPLICATE_DEFAULT_QOS);

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckDeleteAccountAllowedPartitionNoLock_(
    const Account* account, const std::string& partition, bool force) {
  auto result =
      CheckPartitionIsAllowedNoLock_(account, partition, false, false);
  if (!result) return result;

  if (!force && IsAllowedPartitionOfAnyNodeNoLock_(account, partition))
    return std::unexpected(CraneErrCode::ERR_CHILD_HAS_PARTITION);

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckDeleteAccountAllowedQosNoLock_(const Account* account,
                                                    const std::string& qos,
                                                    bool force) {
  auto result = CheckQosIsAllowedNoLock_(account, qos, false, false);
  if (!result) return result;

  if (!force && account->default_qos == qos)
    return std::unexpected(CraneErrCode::ERR_IS_DEFAULT_QOS);

  if (!force && IsDefaultQosOfAnyNodeNoLock_(account, qos))
    return std::unexpected(CraneErrCode::ERR_CHILD_HAS_DEFAULT_QOS);

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckIfUserHasHigherPrivThan_(const User& op_user,
                                              User::AdminLevel admin_level) {
  if (op_user.admin_level <= admin_level)
    return std::unexpected(CraneErrCode::ERR_PERMISSION_USER);

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckIfUserHasPermOnAccountNoLock_(const User& op_user,
                                                   const std::string& account,
                                                   bool read_only_priv) {
  if (account.empty())
    return std::unexpected(CraneErrCode::ERR_NO_ACCOUNT_SPECIFIED);

  const Account* account_ptr = GetExistedAccountInfoNoLock_(account);
  if (!account_ptr) return std::unexpected(CraneErrCode::ERR_INVALID_ACCOUNT);

  if (op_user.admin_level == User::None) {
    if (read_only_priv) {
      // In current implementation, if a user is the coordinator of an
      // account, it must exist in user->account_to_attrs_map.
      // This is guaranteed by the procedure of adding coordinator, where
      // the coordinator is specified only when adding a user to an account.
      // Thus, user->account_to_attrs_map must cover all the accounts he
      // coordinates, and it's ok to skip checking
      // user->coordinator_accounts.
      for (const auto& [acc, item] : op_user.account_to_attrs_map)
        if (acc == account || PaternityTestNoLock_(acc, account)) return {};
    } else {
      for (const auto& acc : op_user.coordinator_accounts)
        if (acc == account || PaternityTestNoLock_(acc, account)) return {};
    }

    return std::unexpected(CraneErrCode::ERR_USER_ALLOWED_ACCOUNT);
  }

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::CheckIfUserHasPermOnUserOfAccountNoLock_(const User& op_user,
                                                         const User* user,
                                                         std::string* account,
                                                         bool read_only_priv) {
  if (!user) return std::unexpected(CraneErrCode::ERR_INVALID_USER);

  if (account->empty()) *account = user->default_account;

  if (!user->account_to_attrs_map.contains(*account))
    return std::unexpected(CraneErrCode::ERR_USER_ACCOUNT_MISMATCH);

  if (op_user.admin_level < user->admin_level)
    return std::unexpected(CraneErrCode::ERR_PERMISSION_USER);

  return CheckIfUserHasPermOnAccountNoLock_(op_user, *account, read_only_priv);
}

AccountManager::CraneExpected<void>
AccountManager::CheckIfUserHasPermOnUserNoLock_(const User& op_user,
                                                const User* user,
                                                bool read_only_priv) {
  if (!user) return std::unexpected(CraneErrCode::ERR_INVALID_USER);

  if (CheckIfUserHasHigherPrivThan_(op_user, user->admin_level) ||
      op_user.name == user->name)
    return {};

  for (const auto& [acct, item] : user->account_to_attrs_map) {
    if (CheckIfUserHasPermOnAccountNoLock_(op_user, acct, read_only_priv))
      return {};
  }

  return std::unexpected(CraneErrCode::ERR_PERMISSION_USER);
}

AccountManager::CraneExpected<void>
AccountManager::CheckPartitionIsAllowedNoLock_(const Account* account,
                                               const std::string& partition,
                                               bool check_parent,
                                               bool is_user) {
  if (!account) return std::unexpected(CraneErrCode::ERR_INVALID_ACCOUNT);

  std::vector<std::string> partition_vec =
      absl::StrSplit(partition, ',', absl::SkipEmpty());

  for (const auto& part : partition_vec) {
    // check if new partition existed
    if (!g_config.Partitions.contains(part))
      return std::unexpected(CraneErrCode::ERR_INVALID_PARTITION);

    if (!check_parent) {
      // check if account has access to new partition
      if (!ranges::contains(account->allowed_partition, part)) {
        if (is_user)
          return std::unexpected(CraneErrCode::ERR_PARENT_ALLOWED_PARTITION);
        return std::unexpected(CraneErrCode::ERR_ALLOWED_PARTITION);
      }
    } else {
      // Check if parent account has access to the partition
      if (!account->parent_account.empty()) {
        const Account* parent =
            GetExistedAccountInfoNoLock_(account->parent_account);
        if (!ranges::contains(parent->allowed_partition, part)) {
          return std::unexpected(CraneErrCode::ERR_PARENT_ALLOWED_PARTITION);
        }
      }
    }
  }

  return {};
}

AccountManager::CraneExpected<void> AccountManager::CheckQosIsAllowedNoLock_(
    const Account* account, const std::string& qos_str, bool check_parent,
    bool is_user) {
  if (!account) return std::unexpected(CraneErrCode::ERR_INVALID_ACCOUNT);

  std::vector<std::string> qos_vec =
      absl::StrSplit(qos_str, ',', absl::SkipEmpty());

  for (const auto& qos : qos_vec) {
    // check if the qos existed
    if (!GetExistedQosInfoNoLock_(qos))
      return std::unexpected(CraneErrCode::ERR_INVALID_QOS);

    if (!check_parent) {
      // check if account has access to new qos
      if (!ranges::contains(account->allowed_qos_list, qos)) {
        if (is_user)
          return std::unexpected(CraneErrCode::ERR_PARENT_ALLOWED_QOS);
        return std::unexpected(CraneErrCode::ERR_ALLOWED_QOS);
      }
    } else {
      // Check if parent account has access to the qos
      if (!account->parent_account.empty()) {
        const Account* parent =
            GetExistedAccountInfoNoLock_(account->parent_account);
        for (const auto& qos : qos_vec) {
          if (!ranges::contains(parent->allowed_qos_list, qos))
            return std::unexpected(CraneErrCode::ERR_PARENT_ALLOWED_QOS);
        }
      }
    }
  }

  return {};
}

void AccountManager::InitDataMap_() {
  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::write_lock_guard account_guard(m_rw_account_mutex_);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

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

AccountManager::CraneExpected<const User*>
AccountManager::GetUserInfoByUidNoLock_(uint32_t uid) {
  PasswordEntry entry(uid);

  if (!entry.Valid()) {
    CRANE_ERROR("Uid {} not existed", uid);
    return std::unexpected(CraneErrCode::ERR_INVALID_UID);
  }

  const User* user = GetExistedUserInfoNoLock_(entry.Username());

  if (!user) {
    CRANE_ERROR("User '{}' is not a user of Crane", entry.Username());
    return std::unexpected(CraneErrCode::ERR_INVALID_OP_USER);
  }

  return user;
}

/*
 * Get the user info form mongodb
 */
const User* AccountManager::GetUserInfoNoLock_(const std::string& name) {
  auto find_res = m_user_map_.find(name);
  if (find_res == m_user_map_.end()) return nullptr;
  return find_res->second.get();
}

/*
 * Get the user info form mongodb and deletion flag marked false
 */
const User* AccountManager::GetExistedUserInfoNoLock_(const std::string& name) {
  const User* user = GetUserInfoNoLock_(name);
  if (user && !user->deleted) return user;
  return nullptr;
}

const Account* AccountManager::GetAccountInfoNoLock_(const std::string& name) {
  auto find_res = m_account_map_.find(name);
  if (find_res == m_account_map_.end()) return nullptr;

  return find_res->second.get();
}

const Account* AccountManager::GetExistedAccountInfoNoLock_(
    const std::string& name) {
  const Account* account = GetAccountInfoNoLock_(name);
  if (account && !account->deleted) return account;
  return nullptr;
}

const Qos* AccountManager::GetQosInfoNoLock_(const std::string& name) {
  auto find_res = m_qos_map_.find(name);
  if (find_res == m_qos_map_.end()) return nullptr;

  return find_res->second.get();
}

const Qos* AccountManager::GetExistedQosInfoNoLock_(const std::string& name) {
  const Qos* qos = GetQosInfoNoLock_(name);
  if (qos && !qos->deleted) return qos;
  return nullptr;
}

bool AccountManager::IncQosReferenceCountInDb_(const std::string& name,
                                               int num) {
  return g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS, "$inc",
                                      name, "reference_count", num);
}

AccountManager::CraneExpected<void> AccountManager::AddUser_(
    const User& user, const Account* account, const User* stale_user) {
  const std::string& object_account = user.default_account;
  const std::string& name = user.name;

  bool add_coordinator = false;
  if (!user.coordinator_accounts.empty()) add_coordinator = true;

  User res_user;
  if (stale_user && !stale_user->deleted) {
    res_user = *stale_user;
    res_user.account_to_attrs_map[object_account] =
        user.account_to_attrs_map.at(object_account);
    if (add_coordinator)
      res_user.coordinator_accounts.push_back(object_account);
  } else {
    res_user = user;
  }

  const std::list<std::string>& parent_allowed_partition =
      account->allowed_partition;
  if (!res_user.account_to_attrs_map[object_account]
           .allowed_partition_qos_map.empty()) {
    for (auto&& [partition, qos] : res_user.account_to_attrs_map[object_account]
                                       .allowed_partition_qos_map) {
      qos.first = account->default_qos;
      qos.second = account->allowed_qos_list;
    }
  } else {
    // Inherit
    for (const auto& partition : parent_allowed_partition) {
      res_user.account_to_attrs_map[object_account]
          .allowed_partition_qos_map[partition] =
          std::pair<std::string, std::list<std::string>>{
              account->default_qos,
              std::list<std::string>{account->allowed_qos_list}};
    }
  }
  res_user.account_to_attrs_map[object_account].blocked = false;

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        // Update the user's account
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                     "$addToSet", object_account, "users",
                                     name);
        if (add_coordinator) {
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$addToSet", object_account,
                                       "coordinators", name);
        }

        if (stale_user) {
          // There is a same user but was deleted or user would like to add
          // user to a new account,here will overwrite it with the same name
          g_db_client->UpdateUser(res_user);
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$set",
                                       name, "creation_time",
                                       ToUnixSeconds(absl::Now()));
        } else {
          // Insert the new user
          g_db_client->InsertUser(res_user);
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  m_account_map_[object_account]->users.emplace_back(name);
  if (add_coordinator) {
    m_account_map_[object_account]->coordinators.emplace_back(name);
  }
  m_user_map_[name] = std::make_unique<User>(std::move(res_user));

  return {};
}

AccountManager::CraneExpected<void> AccountManager::AddAccount_(
    const Account& account, const Account* parent,
    const Account* stale_account) {
  const std::string& name = account.name;

  Account res_account(account);
  if (parent != nullptr) {
    if (res_account.allowed_partition.empty()) {
      // Inherit
      res_account.allowed_partition =
          std::list<std::string>{parent->allowed_partition};
    }

    if (res_account.allowed_qos_list.empty()) {
      // Inherit
      res_account.allowed_qos_list =
          std::list<std::string>{parent->allowed_qos_list};
    }
  }

  if (res_account.default_qos.empty()) {
    if (!res_account.allowed_qos_list.empty())
      res_account.default_qos = res_account.allowed_qos_list.front();
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        if (!res_account.parent_account.empty()) {
          // update the parent account's child_account_list
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$addToSet", res_account.parent_account,
                                       "child_accounts", name);
        }

        if (stale_account) {
          // There is a same account but was deleted,here will delete the
          // original account and overwrite it with the same name
          g_db_client->UpdateAccount(res_account);
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$set", name, "creation_time",
                                       ToUnixSeconds(absl::Now()));
        } else {
          // Insert the new account
          g_db_client->InsertAccount(res_account);
        }
        for (const auto& qos : res_account.allowed_qos_list) {
          IncQosReferenceCountInDb_(qos, 1);
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }
  if (!res_account.parent_account.empty()) {
    m_account_map_[res_account.parent_account]->child_accounts.emplace_back(
        name);
  }
  for (const auto& qos : res_account.allowed_qos_list) {
    m_qos_map_[qos]->reference_count++;
  }
  m_account_map_[name] = std::make_unique<Account>(std::move(res_account));

  return {};
}

AccountManager::CraneExpected<void> AccountManager::AddQos_(
    const Qos& qos, const Qos* stale_qos) {
  if (stale_qos) {
    // There is a same qos but was deleted,here will delete the original
    // qos and overwrite it with the same name
    mongocxx::client_session::with_transaction_cb callback =
        [&](mongocxx::client_session* session) {
          g_db_client->UpdateQos(qos);
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS, "$set",
                                       qos.name, "creation_time",
                                       ToUnixSeconds(absl::Now()));
        };

    if (!g_db_client->CommitTransaction(callback)) {
      return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
    }
  } else {
    // Insert the new qos
    if (!g_db_client->InsertQos(qos))
      return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  m_qos_map_[qos.name] = std::make_unique<Qos>(qos);

  return {};
}

AccountManager::CraneExpected<void> AccountManager::DeleteUser_(
    const User& user, const std::string& account) {
  const std::string& name = user.name;

  std::list<std::string> remove_accounts;
  std::list<std::string> remove_coordinator_accounts;

  User res_user(user);

  if (account.empty()) {
    // Delete the user
    for (const auto& kv : user.account_to_attrs_map) {
      remove_accounts.emplace_back(kv.first);
    }
    for (const auto& coordinatorAccount : user.coordinator_accounts) {
      remove_coordinator_accounts.emplace_back(coordinatorAccount);
    }
    res_user.deleted = true;
  } else {
    // Remove user from account
    remove_accounts.emplace_back(account);
    if (ranges::contains(user.coordinator_accounts, account))
      remove_coordinator_accounts.emplace_back(account);

    res_user.account_to_attrs_map.erase(account);
    if (res_user.default_account == account) {
      if (res_user.account_to_attrs_map.empty())
        res_user.deleted = true;
      else
        res_user.default_account = res_user.account_to_attrs_map.begin()->first;
    }
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        // delete form the parent accounts' users list
        for (const auto& remove_account : remove_accounts) {
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$pull", remove_account,
                                       /*account name*/ "users", name);
        }
        for (const auto& coordinatorAccount : remove_coordinator_accounts) {
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$pull", coordinatorAccount,
                                       /*account name*/ "coordinators", name);
        }

        g_db_client->UpdateUser(res_user);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  for (auto& remove_account : remove_accounts) {
    m_account_map_[remove_account]->users.remove(name);
  }
  for (auto& coordinatorAccount : remove_coordinator_accounts) {
    m_account_map_[coordinatorAccount]->coordinators.remove(name);
  }

  m_user_map_[name] = std::make_unique<User>(std::move(res_user));

  return {};
}

AccountManager::CraneExpected<void> AccountManager::DeleteAccount_(
    const Account& account) {
  const std::string& name = account.name;

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        if (!account.parent_account.empty()) {
          // delete form the parent account's child account list
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$pull", account.parent_account,
                                       "child_accounts", name);
        }
        // Delete the account
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$set",
                                     name, "deleted", true);
        for (const auto& qos : account.allowed_qos_list) {
          IncQosReferenceCountInDb_(qos, -1);
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  if (!account.parent_account.empty()) {
    m_account_map_[account.parent_account]->child_accounts.remove(name);
  }
  m_account_map_[name]->deleted = true;

  for (const auto& qos : account.allowed_qos_list) {
    m_qos_map_[qos]->reference_count--;
  }

  return {};
}

AccountManager::CraneExpected<void> AccountManager::DeleteQos_(
    const std::string& name) {
  if (!g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS, "$set",
                                    name, "deleted", true)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }
  m_qos_map_[name]->deleted = true;

  return {};
}

AccountManager::CraneExpected<void> AccountManager::AddUserAllowedPartition_(
    const User& user, const Account& account, const std::string& partition) {
  const std::string& name = user.name;
  const std::string& account_name = account.name;

  User res_user(user);

  // Update the map
  res_user.account_to_attrs_map[account_name]
      .allowed_partition_qos_map[partition] =
      std::pair<std::string, std::list<std::string>>{
          account.default_qos,
          std::list<std::string>{account.allowed_qos_list}};

  // Update to database
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateUser(res_user);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  m_user_map_[name]
      ->account_to_attrs_map[account_name]
      .allowed_partition_qos_map =
      res_user.account_to_attrs_map[account_name].allowed_partition_qos_map;

  return {};
}

AccountManager::CraneExpected<void> AccountManager::AddUserAllowedQos_(
    const User& user, const Account& account, const std::string& partition,
    const std::string& qos) {
  const std::string& name = user.name;
  const std::string& account_name = account.name;

  User res_user(user);

  if (partition.empty()) {
    // add to all partition
    for (auto& [par, pair] : res_user.account_to_attrs_map[account_name]
                                 .allowed_partition_qos_map) {
      std::list<std::string>& list = pair.second;
      if (!ranges::contains(list, qos)) {
        if (pair.first.empty()) {
          pair.first = qos;
        }
        list.emplace_back(qos);
      }
    }
  } else {
    // add to exacted partition
    auto iter = res_user.account_to_attrs_map[account_name]
                    .allowed_partition_qos_map.find(partition);
    std::list<std::string>& list = iter->second.second;
    if (iter->second.first.empty()) {
      iter->second.first = qos;
    }
    list.push_back(qos);
  }

  // Update to database
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateUser(res_user);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  m_user_map_[name]
      ->account_to_attrs_map[account_name]
      .allowed_partition_qos_map =
      res_user.account_to_attrs_map[account_name].allowed_partition_qos_map;

  return {};
}

AccountManager::CraneExpected<void> AccountManager::SetUserAdminLevel_(
    const std::string& name, User::AdminLevel new_level) {
  // Update to database
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$set",
                                     name, "admin_level",
                                     static_cast<int>(new_level));
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  m_user_map_[name]->admin_level = new_level;

  return {};
}

AccountManager::CraneExpected<void> AccountManager::SetUserDefaultQos_(
    const User& user, const std::string& account, const std::string& partition,
    const std::string& qos) {
  const std::string& name = user.name;

  User res_user(user);

  if (partition.empty()) {
    for (auto& [par, pair] :
         res_user.account_to_attrs_map[account].allowed_partition_qos_map) {
      if (ranges::contains(pair.second, qos) && qos != pair.first)
        pair.first = qos;
    }
  } else {
    auto iter =
        res_user.account_to_attrs_map[account].allowed_partition_qos_map.find(
            partition);

    iter->second.first = qos;
  }

  // Update to database
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateUser(res_user);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  m_user_map_[name]->account_to_attrs_map[account].allowed_partition_qos_map =
      res_user.account_to_attrs_map[account].allowed_partition_qos_map;

  return {};
}

AccountManager::CraneExpected<void> AccountManager::SetUserAllowedPartition_(
    const User& user, const Account& account, const std::string& partitions) {
  const std::string& name = user.name;
  const std::string& account_name = account.name;

  std::vector<std::string> partition_vec =
      absl::StrSplit(partitions, ',', absl::SkipEmpty());

  User res_user(user);
  // Update the map
  res_user.account_to_attrs_map[account_name]
      .allowed_partition_qos_map.clear();  // clear the partitions
  for (const auto& par : partition_vec) {
    res_user.account_to_attrs_map[account_name].allowed_partition_qos_map[par] =
        std::pair<std::string, std::list<std::string>>{
            account.default_qos,
            std::list<std::string>{account.allowed_qos_list}};
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateUser(res_user);
      };

  // Update to database
  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  m_user_map_[name]
      ->account_to_attrs_map[account_name]
      .allowed_partition_qos_map =
      res_user.account_to_attrs_map[account_name].allowed_partition_qos_map;

  return {};
}

AccountManager::CraneExpected<void> AccountManager::SetUserAllowedQos_(
    const User& user, const Account& account, const std::string& partition,
    const std::string& qos_list_str, bool force) {
  const std::string& name = user.name;
  const std::string& account_name = account.name;

  std::vector<std::string> qos_vec =
      absl::StrSplit(qos_list_str, ',', absl::SkipEmpty());

  User res_user(user);
  if (partition.empty()) {
    // Set the qos of all partition
    for (auto& [par, pair] : res_user.account_to_attrs_map[account_name]
                                 .allowed_partition_qos_map) {
      if (!ranges::contains(qos_vec, pair.first))
        pair.first = qos_vec.empty() ? "" : qos_vec.front();
      pair.second.assign(qos_vec.begin(), qos_vec.end());
    }
  } else {
    // Set the qos of a specified partition
    auto iter = res_user.account_to_attrs_map[account_name]
                    .allowed_partition_qos_map.find(partition);

    if (!ranges::contains(qos_vec, iter->second.first))
      iter->second.first = qos_vec.empty() ? "" : qos_vec.front();

    iter->second.second.assign(qos_vec.begin(), qos_vec.end());
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateUser(res_user);
      };

  // Update to database
  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  m_user_map_[name]
      ->account_to_attrs_map[account_name]
      .allowed_partition_qos_map =
      res_user.account_to_attrs_map[account_name].allowed_partition_qos_map;

  return {};
}

AccountManager::CraneExpected<void> AccountManager::DeleteUserAllowedPartition_(
    const User& user, const std::string& account,
    const std::string& partition) {
  const std::string& name = user.name;

  // Update to database
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateEntityOne(
            Ctld::MongodbClient::EntityType::USER, "$unset", name,
            "account_to_attrs_map." + account + ".allowed_partition_qos_map." +
                partition,
            std::string(""));
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  m_user_map_[name]
      ->account_to_attrs_map[account]
      .allowed_partition_qos_map.erase(partition);

  return {};
}

AccountManager::CraneExpected<void> AccountManager::DeleteUserAllowedQos_(
    const User& user, const std::string& qos, const std::string& account,
    const std::string& partition, bool force) {
  const std::string& name = user.name;

  User res_user(user);

  if (partition.empty()) {
    // Delete the qos of all partition
    for (auto& [par, pair] :
         res_user.account_to_attrs_map[account].allowed_partition_qos_map) {
      if (ranges::contains(pair.second, qos)) {
        pair.second.remove(qos);
        if (pair.first == qos) {
          pair.first = pair.second.empty() ? "" : pair.second.front();
        }
      }
    }
  } else {
    // Delete the qos of a specified partition
    auto iter =
        res_user.account_to_attrs_map[account].allowed_partition_qos_map.find(
            partition);

    iter->second.second.remove(qos);

    if (qos == iter->second.first) {
      iter->second.first =
          iter->second.second.empty() ? "" : iter->second.second.front();
    }
  }

  // Update to database
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateUser(res_user);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  m_user_map_[name]->account_to_attrs_map[account].allowed_partition_qos_map =
      res_user.account_to_attrs_map[account].allowed_partition_qos_map;

  return {};
}

AccountManager::CraneExpected<void> AccountManager::AddAccountAllowedPartition_(
    const std::string& name, const std::string& partition) {
  // Update to database
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                     "$addToSet", name, "allowed_partition",
                                     partition);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }
  m_account_map_[name]->allowed_partition.emplace_back(partition);

  return {};
}

AccountManager::CraneExpected<void> AccountManager::AddAccountAllowedQos_(
    const Account& account, const std::string& qos) {
  const std::string& name = account.name;

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        if (account.default_qos.empty()) {
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$set", name, "default_qos", qos);
        }
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                     "$addToSet", name, "allowed_qos_list",
                                     qos);
        IncQosReferenceCountInDb_(qos, 1);
      };

  // Update to database
  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  if (account.default_qos.empty()) {
    m_account_map_[name]->default_qos = qos;
  }
  m_account_map_[name]->allowed_qos_list.emplace_back(qos);
  m_qos_map_[qos]->reference_count++;

  return {};
}

AccountManager::CraneExpected<void> AccountManager::SetAccountDescription_(
    const std::string& name, const std::string& description) {
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$set",
                                     name, "description", description);
      };

  // Update to database
  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  m_account_map_[name]->description = description;

  return {};
}

AccountManager::CraneExpected<void> AccountManager::SetAccountDefaultQos_(
    const Account& account, const std::string& qos) {
  const std::string& name = account.name;

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$set",
                                     name, "default_qos", qos);
      };

  // Update to database
  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }
  m_account_map_[name]->default_qos = qos;

  return {};
}

AccountManager::CraneExpected<void> AccountManager::SetAccountAllowedPartition_(
    const Account& account, const std::string& partitions) {
  const std::string& name = account.name;

  std::vector<std::string> partition_vec =
      absl::StrSplit(partitions, ',', absl::SkipEmpty());

  std::list<std::string> deleted_partition;
  for (const auto& par : account.allowed_partition) {
    if (!ranges::contains(partition_vec, par))
      deleted_partition.emplace_back(par);
  }

  int add_num = 0;
  for (const auto& par : partition_vec) {
    if (!ranges::contains(account.allowed_partition, par)) add_num++;
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        for (const auto& par : deleted_partition) {
          DeleteAccountAllowedPartitionFromDBNoLock_(account.name, par);
        }

        if (add_num > 0) {
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$set", name, "allowed_partition",
                                       partition_vec);
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  for (const auto& par : deleted_partition) {
    DeleteAccountAllowedPartitionFromMapNoLock_(account.name, par);
  }
  m_account_map_[name]->allowed_partition.assign(partition_vec.begin(),
                                                 partition_vec.end());

  return {};
}

AccountManager::CraneExpected<void> AccountManager::SetAccountAllowedQos_(
    const Account& account, const std::string& qos_list_str) {
  const std::string& name = account.name;

  std::vector<std::string> qos_vec =
      absl::StrSplit(qos_list_str, ',', absl::SkipEmpty());

  std::list<std::string> deleted_qos;
  for (const auto& qos : account.allowed_qos_list) {
    if (!ranges::contains(qos_vec, qos)) deleted_qos.emplace_back(qos);
  }

  std::list<std::string> add_qos;
  for (const auto& qos : qos_vec) {
    if (!ranges::contains(account.allowed_qos_list, qos))
      add_qos.emplace_back(qos);
  }

  std::list<int> change_num;
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        for (const auto& qos : deleted_qos) {
          int num = DeleteAccountAllowedQosFromDBNoLock_(account.name, qos);
          IncQosReferenceCountInDb_(qos, -num);
          change_num.emplace_back(num);
        }

        if (!add_qos.empty()) {
          Account temp;
          g_db_client->SelectAccount("name", name, &temp);
          if (temp.default_qos.empty()) {
            g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                         "$set", name, "default_qos",
                                         qos_vec.front());
          }

          g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                       "$set", name, "allowed_qos_list",
                                       qos_vec);
          for (const auto& qos : add_qos) {
            IncQosReferenceCountInDb_(qos, 1);
          }
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  for (const auto& qos : deleted_qos) {
    DeleteAccountAllowedQosFromMapNoLock_(account.name, qos);
    m_qos_map_[qos]->reference_count -= change_num.front();
    change_num.pop_front();
  }

  if (!add_qos.empty()) {
    if (account.default_qos.empty()) {
      m_account_map_[name]->default_qos = qos_vec.front();
    }
    m_account_map_[name]->allowed_qos_list.assign(qos_vec.begin(),
                                                  qos_vec.end());
    for (const auto& qos : add_qos) {
      m_qos_map_[qos]->reference_count++;
    }
  }

  return {};
}

AccountManager::CraneExpected<void>
AccountManager::DeleteAccountAllowedPartition_(const Account& account,
                                               const std::string& partition) {
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        DeleteAccountAllowedPartitionFromDBNoLock_(account.name, partition);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  DeleteAccountAllowedPartitionFromMapNoLock_(account.name, partition);

  return {};
}

AccountManager::CraneExpected<void> AccountManager::DeleteAccountAllowedQos_(
    const Account& account, const std::string& qos) {
  int change_num;
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        change_num = DeleteAccountAllowedQosFromDBNoLock_(account.name, qos);
        IncQosReferenceCountInDb_(qos, -change_num);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  DeleteAccountAllowedQosFromMapNoLock_(account.name, qos);
  m_qos_map_[qos]->reference_count -= change_num;

  return {};
}

AccountManager::CraneExpected<void> AccountManager::BlockUser_(
    const std::string& name, const std::string& account, bool block) {
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateEntityOne(
            MongodbClient::EntityType::USER, "$set", name,
            "account_to_attrs_map." + account + ".blocked", block);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  m_user_map_[name]->account_to_attrs_map[account].blocked = block;

  return {};
}

AccountManager::CraneExpected<void> AccountManager::BlockAccount_(
    const std::string& name, bool block) {
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$set",
                                     name, "blocked", block);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }
  m_account_map_[name]->blocked = block;

  return {};
}

/**
 * @note need read lock(m_rw_user_mutex_ && m_rw_account_mutex_)
 * @param account
 * @param partition
 * @return
 */
bool AccountManager::IsAllowedPartitionOfAnyNodeNoLock_(
    const Account* account, const std::string& partition, int depth) {
  if (depth > 0 && ranges::contains(account->allowed_partition, partition)) {
    return true;
  }
  for (const auto& child : account->child_accounts) {
    if (IsAllowedPartitionOfAnyNodeNoLock_(GetExistedAccountInfoNoLock_(child),
                                           partition, depth + 1)) {
      return true;
    }
  }
  for (const auto& user : account->users) {
    const User* p = GetExistedUserInfoNoLock_(user);
    for (const auto& item : p->account_to_attrs_map) {
      if (item.second.allowed_partition_qos_map.contains(partition)) {
        return true;
      }
    }
  }
  return false;
}

/**
 * @note need read lock(m_rw_user_mutex_ && m_rw_account_mutex_)
 * @param account
 * @param qos
 * @return
 */
bool AccountManager::IsDefaultQosOfAnyNodeNoLock_(const Account* account,
                                                  const std::string& qos) {
  if (account->default_qos == qos) {
    return true;
  }
  for (const auto& child : account->child_accounts) {
    if (IsDefaultQosOfAnyNodeNoLock_(GetExistedAccountInfoNoLock_(child),
                                     qos)) {
      return true;
    }
  }

  for (const auto& user : account->users) {
    if (IsDefaultQosOfAnyPartitionNoLock_(GetExistedUserInfoNoLock_(user),
                                          qos)) {
      return true;
    }
  }
  return false;
}

bool AccountManager::IsDefaultQosOfAnyPartitionNoLock_(const User* user,
                                                       const std::string& qos) {
  for (const auto& [account, item] : user->account_to_attrs_map) {
    if (std::any_of(item.allowed_partition_qos_map.begin(),
                    item.allowed_partition_qos_map.end(),
                    [&qos](const auto& p) { return p.second.first == qos; })) {
      return true;
    }
  }
  return false;
}

/**
 * @note need read lock(m_rw_user_mutex_ && m_rw_account_mutex_)
 */
int AccountManager::DeleteAccountAllowedQosFromDBNoLock_(
    const std::string& name, const std::string& qos) {
  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    CRANE_ERROR(
        "Operating on a non-existent account '{}', please check this item "
        "in database and restart cranectld",
        name);
    return false;
  }

  if (!ranges::contains(account->allowed_qos_list, qos)) return false;

  int change_num = 1;
  for (const auto& child : account->child_accounts) {
    change_num += DeleteAccountAllowedQosFromDBNoLock_(child, qos);
  }

  for (const auto& user : account->users) {
    DeleteUserAllowedQosOfAllPartitionFromDBNoLock_(user, name, qos);
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

/**
 * @note need write lock(m_rw_account_mutex_) and write
 * lock(m_rw_user_mutex_)
 * @param name
 * @param qos
 * @return
 */
bool AccountManager::DeleteAccountAllowedQosFromMapNoLock_(
    const std::string& name, const std::string& qos) {
  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    CRANE_ERROR(
        "Operating on a non-existent account '{}', please check this item "
        "in database and restart cranectld",
        name);
    return false;
  }

  if (!ranges::contains(account->allowed_qos_list, qos)) return false;

  for (const auto& child : account->child_accounts) {
    DeleteAccountAllowedQosFromMapNoLock_(child, qos);
  }

  for (const auto& user : account->users) {
    DeleteUserAllowedQosOfAllPartitionFromMapNoLock_(user, name, qos);
  }
  m_account_map_[name]->allowed_qos_list.remove(qos);
  if (account->default_qos == qos) {
    m_account_map_[name]->default_qos = account->allowed_qos_list.empty()
                                            ? ""
                                            : account->allowed_qos_list.front();
  }
  return true;
}

/**
 * @note need read lock(m_rw_user_mutex_)
 * @param name
 * @param account
 * @param qos
 * @return
 */
bool AccountManager::DeleteUserAllowedQosOfAllPartitionFromDBNoLock_(
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
  if (!p->account_to_attrs_map.contains(account)) {
    CRANE_ERROR("User '{}' doesn't belong to account '{}'", name, account);
    return false;
  }
  User user(*p);

  for (auto& [par, pair] :
       user.account_to_attrs_map[account].allowed_partition_qos_map) {
    if (ranges::contains(pair.second, qos)) pair.second.remove(qos);

    if (pair.first == qos) {
      pair.first = pair.second.empty() ? "" : pair.second.front();
    }
  }
  g_db_client->UpdateUser(user);
  return true;
}

/**
 * @note need write lock(m_rw_user_mutex_)
 * @param name
 * @param account
 * @param qos
 * @return
 */
bool AccountManager::DeleteUserAllowedQosOfAllPartitionFromMapNoLock_(
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
  if (!p->account_to_attrs_map.contains(account)) {
    CRANE_ERROR("User '{}' doesn't belong to account '{}'", name, account);
    return false;
  }
  User user(*p);

  for (auto& [par, qos_pair] :
       user.account_to_attrs_map[account].allowed_partition_qos_map) {
    qos_pair.second.remove(qos);
    if (qos_pair.first == qos) {
      qos_pair.first = qos_pair.second.empty() ? "" : qos_pair.second.front();
    }
  }
  m_user_map_[user.name]
      ->account_to_attrs_map[account]
      .allowed_partition_qos_map =
      user.account_to_attrs_map[account].allowed_partition_qos_map;
  return true;
}

/**
 * @note need read lock(m_rw_account_mutex_)
 * @param name
 * @param partition
 * @return
 */
bool AccountManager::DeleteAccountAllowedPartitionFromDBNoLock_(
    const std::string& name, const std::string& partition) {
  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    CRANE_ERROR(
        "Operating on a non-existent account '{}', please check this item "
        "in database and restart cranectld",
        name);
    return false;
  }

  if (!ranges::contains(account->allowed_partition, partition)) return false;

  for (const auto& child : account->child_accounts) {
    DeleteAccountAllowedPartitionFromDBNoLock_(child, partition);
  }

  for (const auto& user : account->users) {
    g_db_client->UpdateEntityOne(Ctld::MongodbClient::EntityType::USER,
                                 "$unset", user,
                                 "account_to_attrs_map." + name +
                                     ".allowed_partition_qos_map." + partition,
                                 std::string(""));
  }

  g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$pull",
                               account->name, "allowed_partition", partition);
  return true;
}

/**
 * @note need write lock(m_rw_account_mutex_) and write
 * lock(m_rw_user_mutex_)
 * @param name
 * @param partition
 * @return
 */
bool AccountManager::DeleteAccountAllowedPartitionFromMapNoLock_(
    const std::string& name, const std::string& partition) {
  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) {
    CRANE_ERROR(
        "Operating on a non-existent account '{}', please check this item "
        "in database and restart cranectld",
        name);
    return false;
  }

  if (!ranges::contains(account->allowed_partition, partition)) return false;

  for (const auto& child : account->child_accounts) {
    DeleteAccountAllowedPartitionFromMapNoLock_(child, partition);
  }

  for (const auto& user : account->users) {
    m_user_map_[user]
        ->account_to_attrs_map[name]
        .allowed_partition_qos_map.erase(partition);
  }
  m_account_map_[account->name]->allowed_partition.remove(partition);

  return true;
}

bool AccountManager::PaternityTestNoLock_(const std::string& parent,
                                          const std::string& child) {
  if (parent == child || GetExistedAccountInfoNoLock_(parent) == nullptr ||
      GetExistedAccountInfoNoLock_(child) == nullptr) {
    return false;
  }
  return PaternityTestNoLockDFS_(parent, child);
}

bool AccountManager::PaternityTestNoLockDFS_(const std::string& parent,
                                             const std::string& child) {
  for (const auto& child_of_account : m_account_map_[parent]->child_accounts) {
    if (child_of_account == child ||
        PaternityTestNoLockDFS_(child_of_account, child)) {
      return true;
    }
  }
  return false;
}

}  // namespace Ctld
