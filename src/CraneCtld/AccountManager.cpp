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

#include "AccountMetaContainer.h"
#include "Security/VaultClient.h"
#include "protos/PublicDefs.pb.h"
#include "range/v3/algorithm/contains.hpp"

namespace Ctld {

AccountManager::AccountManager() { InitDataMap_(); }

CraneExpected<void> AccountManager::AddUser(uint32_t uid,
                                            const User& new_user) {
  CraneExpected<void> result;

  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::write_lock_guard account_guard(m_rw_account_mutex_);

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();

  result = CheckIfUserHasHigherPrivThan_(*op_user, new_user.admin_level);
  if (!result) return result;

  if (new_user.name == "ALL")
    return std::unexpected(CraneErrCode::ERR_INVALID_USERNAME);

  // User must specify an account
  if (new_user.default_account.empty())
    return std::unexpected(CraneErrCode::ERR_NO_ACCOUNT_SPECIFIED);

  const std::string& object_account = new_user.default_account;
  const std::string& name = new_user.name;

  // Avoid duplicate insertion
  const User* stale_user = GetUserInfoNoLock_(name);
  if (stale_user && !stale_user->deleted) {
    if (stale_user->account_to_attrs_map.contains(object_account))
      return std::unexpected(CraneErrCode::ERR_USER_ALREADY_EXISTS);
    result = CheckIfUserHasHigherPrivThan_(*op_user, stale_user->admin_level);
    if (!result) return result;
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

  return AddUser_(op_user->name, new_user, account, stale_user);
}

CraneExpected<void> AccountManager::AddAccount(uint32_t uid,
                                               const Account& new_account) {
  std::string actor_name;
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
        return std::unexpected(CraneErrCode::ERR_INVALID_PARENT_ACCOUNT);
    }
    if (!result) return result;
    actor_name = op_user->name;
  }

  util::write_lock_guard account_guard(m_rw_account_mutex_);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const std::string& name = new_account.name;
  // Avoid duplicate insertion
  const Account* stale_account = GetAccountInfoNoLock_(name);
  if (stale_account && !stale_account->deleted)
    return std::unexpected(CraneErrCode::ERR_ACCOUNT_ALREADY_EXISTS);

  const Account* find_parent = nullptr;
  if (!new_account.parent_account.empty()) {
    // Check whether the account's parent account exists
    find_parent = GetExistedAccountInfoNoLock_(new_account.parent_account);
    if (!find_parent)
      return std::unexpected(CraneErrCode::ERR_INVALID_PARENT_ACCOUNT);

    // check allowed partition authority
    for (const auto& par : new_account.allowed_partition) {
      if (!ranges::contains(find_parent->allowed_partition, par))  // not find
        return std::unexpected(
            CraneErrCode::ERR_PARENT_ACCOUNT_PARTITION_MISSING);
    }

    // check allowed qos list authority
    for (const auto& qos : new_account.allowed_qos_list) {
      if (!ranges::contains(find_parent->allowed_qos_list, qos))  // not find
        return std::unexpected(CraneErrCode::ERR_PARENT_ACCOUNT_QOS_MISSING);
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
      return std::unexpected(CraneErrCode::ERR_DEFAULT_QOS_NOT_INHERITED);
  }

  return AddAccount_(actor_name, new_account, find_parent, stale_account);
}

CraneExpected<void> AccountManager::AddQos(uint32_t uid, const Qos& new_qos) {
  std::string actor_name;
  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();

    auto result = CheckIfUserHasHigherPrivThan_(*op_user, User::None);
    if (!result) return result;
    actor_name = op_user->name;
  }

  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const Qos* find_qos = GetQosInfoNoLock_(new_qos.name);

  // Avoid duplicate insertion
  if (find_qos && !find_qos->deleted)
    return std::unexpected(CraneErrCode::ERR_DB_QOS_ALREADY_EXISTS);

  return AddQos_(actor_name, new_qos, find_qos);
}

CraneExpected<void> AccountManager::AddUserWckey(uint32_t uid,
                                                 const Wckey& new_wckey) {
  if (new_wckey.name.empty())
    return std::unexpected(CraneErrCode::ERR_INVALID_WCKEY);
  util::write_lock_guard user_guard(m_rw_user_mutex_);
  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();

  const User* user_exist = GetExistedUserInfoNoLock_(new_wckey.user_name);
  if (user_exist == nullptr) {
    return std::unexpected(CraneErrCode::ERR_INVALID_USER);
  }

  if (op_user->uid != 0) {
    auto result =
        CheckIfUserHasHigherPrivThan_(*op_user, user_exist->admin_level);
    if (!result) return result;
  }

  util::write_lock_guard wckey_guard(m_rw_wckey_mutex_);
  // validate user existence under write lock
  const Wckey* find_wckey =
      GetWckeyInfoNoLock_(new_wckey.name, new_wckey.user_name);
  // Avoid duplicate insertion
  if (find_wckey && !find_wckey->deleted)
    return std::unexpected(CraneErrCode::ERR_WCKEY_ALREADY_EXISTS);

  return AddWckey_(new_wckey, find_wckey, user_exist);
}

CraneExpected<void> AccountManager::DeleteUser(uint32_t uid,
                                               const std::string& name,
                                               const std::string& account) {
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

  return DeleteUser_(op_user->name, *user, account);
}

CraneExpected<void> AccountManager::DeleteAccount(uint32_t uid,
                                                  const std::string& name) {
  std::string actor_name;
  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    util::read_lock_guard account_guard(m_rw_account_mutex_);

    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();

    auto result = CheckIfUserHasPermOnAccountNoLock_(*op_user, name, false);
    if (!result) return result;
    actor_name = op_user->name;
  }

  util::write_lock_guard account_guard(m_rw_account_mutex_);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) return std::unexpected(CraneErrCode::ERR_INVALID_ACCOUNT);

  // Cannot delete because there are child nodes.
  if (!account->child_accounts.empty() || !account->users.empty())
    return std::unexpected(CraneErrCode::ERR_ACCOUNT_HAS_CHILDREN);

  return DeleteAccount_(actor_name, *account);
}

CraneExpected<void> AccountManager::DeleteQos(uint32_t uid,
                                              const std::string& name) {
  std::string actor_name;
  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();

    auto result = CheckIfUserHasHigherPrivThan_(*op_user, User::None);
    if (!result) return result;
    actor_name = op_user->name;
  }

  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const Qos* qos = GetExistedQosInfoNoLock_(name);
  if (!qos) return std::unexpected(CraneErrCode::ERR_INVALID_QOS);

  // Cannot delete because the QoS is still in use.
  if (qos->reference_count != 0)
    return std::unexpected(CraneErrCode::ERR_QOS_REFERENCES_EXIST);

  return DeleteQos_(actor_name, name);
}

CraneExpected<void> AccountManager::DeleteWckey(uint32_t uid,
                                                const std::string& name,
                                                const std::string& user_name) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);
  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();

  const User* p_target_user = GetExistedUserInfoNoLock_(user_name);
  if (!p_target_user) return std::unexpected(CraneErrCode::ERR_INVALID_USER);
  if (op_user->uid != 0) {
    auto result =
        CheckIfUserHasHigherPrivThan_(*op_user, p_target_user->admin_level);
    if (!result) return result;
  }
  if (p_target_user->default_wckey == name) {
    return std::unexpected(CraneErrCode::ERR_DEL_DEFAULT_WCKEY);
  }

  util::write_lock_guard wckey_guard(m_rw_wckey_mutex_);
  const Wckey* wckey = GetExistedWckeyInfoNoLock_(name, user_name);
  if (!wckey) return std::unexpected(CraneErrCode::ERR_INVALID_WCKEY);

  return DeleteWckey_(name, user_name);
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

CraneExpected<std::string> AccountManager::GetExistedDefaultWckeyName(
    const std::string& user_name) {
  util::read_lock_guard user_guard(m_rw_user_mutex_);

  const User* user = GetExistedUserInfoNoLock_(user_name);
  if (!user) return std::unexpected(CraneErrCode::ERR_INVALID_USER);
  if (user->default_wckey.empty())
    return std::unexpected(CraneErrCode::ERR_NO_DEFAULT_WCKEY);

  return user->default_wckey;
}

AccountManager::WckeyMutexSharedPtr AccountManager::GetExistedWckeyInfo(
    const std::string& name, const std::string& user_name) {
  m_rw_wckey_mutex_.lock_shared();

  const Wckey* wckey = GetExistedWckeyInfoNoLock_(name, user_name);
  if (!wckey) {
    m_rw_wckey_mutex_.unlock_shared();
    return WckeyMutexSharedPtr{nullptr};
  }

  return WckeyMutexSharedPtr{wckey, &m_rw_wckey_mutex_};
}

CraneExpected<std::set<User>> AccountManager::QueryAllUserInfo(uint32_t uid) {
  std::set<User> res_user_set;

  util::read_lock_guard user_guard(m_rw_user_mutex_);

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();

  // Operators and above can query all users.
  if (CheckIfUserHasHigherPrivThan_(*op_user, User::None)) {
    // The rules for querying user information are the same as those for
    // querying accounts
    for (const auto& user : m_user_map_ | std::views::values) {
      if (user->deleted) continue;
      res_user_set.emplace(*user);
    }
  } else {
    util::read_lock_guard account_guard(m_rw_account_mutex_);
    std::queue<std::string> queue;
    for (const auto& acct : op_user->account_to_attrs_map | std::views::keys) {
      queue.push(acct);
      while (!queue.empty()) {
        std::string father = queue.front();
        for (const auto& user : m_account_map_.at(father)->users) {
          res_user_set.emplace(*(m_user_map_.at(user)));
        }
        queue.pop();
        for (const auto& child : m_account_map_.at(father)->child_accounts) {
          queue.push(child);
        }
      }
    }
  }

  return res_user_set;
}

CraneExpected<std::vector<Wckey>> AccountManager::QueryAllWckeyInfo(
    uint32_t uid, std::unordered_set<std::string> wckey_list) {
  std::vector<Wckey> res_wckey_list;

  util::read_lock_guard wckey_guard(m_rw_wckey_mutex_);
  util::read_lock_guard user_guard(m_rw_user_mutex_);

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();

  // Operators and above can query all wckeys.
  if (CheckIfUserHasHigherPrivThan_(*op_user, User::None)) {
    if (wckey_list.empty()) {
      for (const auto& wckey : m_wckey_map_ | std::views::values) {
        if (wckey->deleted) continue;
        res_wckey_list.emplace_back(*wckey);
      }
    } else {
      for (const auto& wckey : m_wckey_map_ | std::views::values) {
        if (wckey->deleted) continue;
        if (wckey_list.contains(wckey->name))
          res_wckey_list.emplace_back(*wckey);
      }
    }
  } else {
    return std::unexpected(CraneErrCode::ERR_PERMISSION_USER);
  }

  return res_wckey_list;
}

CraneExpected<User> AccountManager::QueryUserInfo(uint32_t uid,
                                                  const std::string& username) {
  util::read_lock_guard user_guard(m_rw_user_mutex_);

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();

  // Query the specified user information.
  util::read_lock_guard account_guard(m_rw_account_mutex_);

  const User* user = GetExistedUserInfoNoLock_(username);
  if (!user) return std::unexpected(CraneErrCode::ERR_INVALID_USER);

  auto result = CheckIfUserHasPermOnUserNoLock_(*op_user, user, true);
  if (!result) return std::unexpected(result.error());

  return *user;
}

CraneExpected<std::set<Account>> AccountManager::QueryAllAccountInfo(
    uint32_t uid) {
  User res_user;
  std::set<Account> res_account_set;

  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    util::read_lock_guard account_guard(m_rw_account_mutex_);
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    res_user = *user_result.value();
  }

  util::read_lock_guard account_guard(m_rw_account_mutex_);

  if (CheckIfUserHasHigherPrivThan_(res_user, User::None)) {
    // If an administrator user queries account information, all
    // accounts are returned, variable user_account not used
    for (const auto& account : m_account_map_ | std::views::values) {
      if (account->deleted) continue;
      res_account_set.emplace(*account);
    }
  } else {
    // Otherwise, only all sub-accounts under your own accounts will be
    // returned
    std::queue<std::string> queue;
    for (const auto& acct : res_user.account_to_attrs_map | std::views::keys) {
      // Z->A->B--->C->E
      //    |->D    |->F
      // If we query account C, [Z,A,B,C,E,F] is included.
      std::string p_name = m_account_map_.at(acct)->parent_account;
      while (!p_name.empty()) {
        res_account_set.emplace(*(m_account_map_.at(p_name)));
        p_name = m_account_map_.at(p_name)->parent_account;
      }

      queue.push(acct);
      while (!queue.empty()) {
        std::string father = queue.front();
        const auto& account_content = m_account_map_.at(father);
        res_account_set.emplace(*(account_content));
        queue.pop();
        for (const auto& child : account_content->child_accounts) {
          queue.push(child);
        }
      }
    }
  }

  return res_account_set;
}

CraneExpected<Account> AccountManager::QueryAccountInfo(
    uint32_t uid, const std::string& account) {
  User res_user;
  CraneExpected<void> result{};

  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    util::read_lock_guard account_guard(m_rw_account_mutex_);
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    res_user = *user_result.value();
  }

  util::read_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account_ptr = GetAccountInfoNoLock_(account);
  result = CheckIfUserHasPermOnAccountNoLock_(res_user, account, true);
  if (!result) return std::unexpected(result.error());

  return *account_ptr;
}

CraneExpected<std::set<Qos>> AccountManager::QueryAllQosInfo(uint32_t uid) {
  User res_user;

  std::set<Qos> res_qos_set;

  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();
    res_user = *op_user;
  }

  util::read_lock_guard qos_guard(m_rw_qos_mutex_);

  if (CheckIfUserHasHigherPrivThan_(res_user, User::None)) {
    for (const auto& qos : m_qos_map_ | std::views::values) {
      if (qos->deleted) continue;
      res_qos_set.emplace(*qos);
    }
  } else {
    for (const auto& item :
         res_user.account_to_attrs_map | std::views::values) {
      for (const auto& part_qos_map :
           item.allowed_partition_qos_map | std::views::values) {
        for (const auto& qos : part_qos_map.second) {
          res_qos_set.emplace(*(m_qos_map_.at(qos)));
        }
      }
    }
  }

  return res_qos_set;
}

CraneExpected<Qos> AccountManager::QueryQosInfo(uint32_t uid,
                                                const std::string& qos) {
  User res_user;

  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();
    res_user = *op_user;
  }

  util::read_lock_guard qos_guard(m_rw_qos_mutex_);

  const Qos* qos_ptr = GetExistedQosInfoNoLock_(qos);
  if (!qos_ptr) return std::unexpected(CraneErrCode::ERR_INVALID_QOS);

  if (res_user.admin_level < User::Operator) {
    bool found = false;
    for (const auto& item :
         res_user.account_to_attrs_map | std::views::values) {
      for (const auto& part_qos_map :
           item.allowed_partition_qos_map | std::views::values) {
        found = std::ranges::contains(part_qos_map.second, qos);
      }
    }
    if (!found) return std::unexpected(CraneErrCode::ERR_QOS_MISSING);
  }

  return *qos_ptr;
}

std::vector<CraneExpectedRich<void>>
AccountManager::CheckModifyAccountOperations(
    const Account* account,
    const std::vector<crane::grpc::ModifyFieldOperation>& operations,
    bool force) {
  std::vector<CraneExpectedRich<void>> rich_error_list;
  for (const auto& operation : operations) {
    if (operation.type() == crane::grpc::OperationType::Overwrite &&
        operation.modify_field() == crane::grpc::ModifyField::Partition) {
      std::unordered_set<std::string> partition_list(
          operation.value_list().begin(), operation.value_list().end());
      auto rich_result = CheckSetAccountAllowedPartitionNoLock_(
          account, partition_list, force);
      if (!rich_result) {
        if (rich_result.error().description().empty())
          rich_result.error().set_description(
              absl::StrJoin(operation.value_list(), ","));
        rich_error_list.emplace_back(rich_result);
      }
    } else if (operation.type() == crane::grpc::OperationType::Overwrite &&
               operation.modify_field() == crane::grpc::ModifyField::Qos) {
      std::unordered_set<std::string> qos_list(operation.value_list().begin(),
                                               operation.value_list().end());
      auto rich_result =
          CheckSetAccountAllowedQosNoLock_(account, qos_list, force);
      if (!rich_result) {
        if (rich_result.error().description().empty())
          rich_result.error().set_description(
              absl::StrJoin(operation.value_list(), ","));
        rich_error_list.emplace_back(rich_result);
      }
    } else {
      for (const auto& value : operation.value_list()) {
        switch (operation.type()) {
        case crane::grpc::OperationType::Add: {
          switch (operation.modify_field()) {
          case crane::grpc::ModifyField::Partition: {
            auto result =
                CheckAddAccountAllowedPartitionNoLock_(account, value);
            if (!result) {
              auto rich_error =
                  std::unexpected{FormatRichErr(result.error(), value)};
              rich_error_list.emplace_back(rich_error);
            }
            break;
          }
          case crane::grpc::ModifyField::Qos: {
            auto result = CheckAddAccountAllowedQosNoLock_(account, value);
            if (!result) {
              auto rich_error =
                  std::unexpected{FormatRichErr(result.error(), value)};
              rich_error_list.emplace_back(rich_error);
            }
            break;
          }
          default:
            std::unreachable();
          }
          break;
        }

        case crane::grpc::OperationType::Overwrite: {
          switch (operation.modify_field()) {
          case crane::grpc::ModifyField::Description: {
            auto result = CheckSetAccountDescriptionNoLock_(account);
            if (!result) {
              auto rich_error =
                  std::unexpected{FormatRichErr(result.error(), value)};
              rich_error_list.emplace_back(rich_error);
            }
            break;
          }
          case crane::grpc::ModifyField::DefaultQos: {
            auto result = CheckSetAccountDefaultQosNoLock_(account, value);
            if (!result) {
              auto rich_error =
                  std::unexpected{FormatRichErr(result.error(), value)};
              rich_error_list.emplace_back(rich_error);
            }
            break;
          }

          default:
            std::unreachable();
          }
          break;
        }

        case crane::grpc::OperationType::Delete: {
          switch (operation.modify_field()) {
          case crane::grpc::ModifyField::Partition: {
            auto result = CheckDeleteAccountAllowedPartitionNoLock_(
                account, value, force);
            if (!result) {
              auto rich_error =
                  std::unexpected{FormatRichErr(result.error(), value)};
              rich_error_list.emplace_back(rich_error);
            }
            break;
          }

          case crane::grpc::ModifyField::Qos: {
            auto result =
                CheckDeleteAccountAllowedQosNoLock_(account, value, force);
            if (!result) {
              auto rich_error =
                  std::unexpected{FormatRichErr(result.error(), value)};
              rich_error_list.emplace_back(rich_error);
            }
            break;
          }

          default:
            std::unreachable();
          }
          break;
        }

        default:
          std::unreachable();
        }
      }
    }
  }
  return rich_error_list;
}

std::vector<CraneExpectedRich<void>> AccountManager::ModifyAccount(
    uint32_t uid, const std::string& account_name,
    const std::vector<crane::grpc::ModifyFieldOperation>& operations,
    bool force) {
  std::string actor_name;
  std::vector<CraneExpectedRich<void>> rich_error_list;
  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    util::read_lock_guard account_guard(m_rw_account_mutex_);

    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) {
      rich_error_list.emplace_back(
          std::unexpected{FormatRichErr(user_result.error(), "")});
      return rich_error_list;
    }

    const User* op_user = user_result.value();

    auto result =
        CheckIfUserHasPermOnAccountNoLock_(*op_user, account_name, false);
    if (!result) {
      rich_error_list.emplace_back(
          std::unexpected{FormatRichErr(result.error(), "")});
      return rich_error_list;
    }
    actor_name = op_user->name;
  }

  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::write_lock_guard account_guard(m_rw_account_mutex_);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(account_name);
  if (!account) {
    rich_error_list.emplace_back(
        std::unexpected{FormatRichErr(CraneErrCode::ERR_INVALID_ACCOUNT, "")});
    return rich_error_list;
  }

  // helper stl
  std::unordered_set<std::string> partition_list;
  std::unordered_set<std::string> qos_list;
  std::list<int> change_num;
  int delete_qos_change_num;
  std::string default_qos = "";
  // check operations validity
  auto check_result = CheckModifyAccountOperations(account, operations, force);
  if (!check_result.empty()) {
    return check_result;
  }

  bool need_rollback = false;
  // do the modify operations in database
  g_db_client->StartTransaction();
  for (const auto& operation : operations) {
    if (operation.type() == crane::grpc::OperationType::Overwrite &&
        operation.modify_field() == crane::grpc::ModifyField::Partition) {
      partition_list.insert(operation.value_list().begin(),
                            operation.value_list().end());
      auto rich_result =
          SetAccountAllowedPartition_(actor_name, *account, partition_list);
      if (!rich_result) {
        need_rollback = true;
        break;
      }
    } else if (operation.type() == crane::grpc::OperationType::Overwrite &&
               operation.modify_field() == crane::grpc::ModifyField::Qos) {
      qos_list.insert(operation.value_list().begin(),
                      operation.value_list().end());
      if (!operation.value_list().empty())
        default_qos = operation.value_list()[0];
      auto rich_result = SetAccountAllowedQos_(actor_name, *account, qos_list,
                                               change_num, default_qos);
      if (!rich_result) {
        need_rollback = true;
        break;
      }
    } else {
      for (const auto& value : operation.value_list()) {
        if (need_rollback) break;
        switch (operation.type()) {
        case crane::grpc::OperationType::Add: {
          switch (operation.modify_field()) {
          case crane::grpc::ModifyField::Partition: {
            auto result =
                AddAccountAllowedPartition_(actor_name, account_name, value);
            if (!result) {
              need_rollback = true;
            }
            break;
          }

          case crane::grpc::ModifyField::Qos: {
            auto result = AddAccountAllowedQos_(actor_name, *account, value);
            if (!result) {
              need_rollback = true;
              break;
            }
            break;
          }

          default:
            std::unreachable();
          }
          break;
        }

        case crane::grpc::OperationType::Overwrite: {
          switch (operation.modify_field()) {
          case crane::grpc::ModifyField::Description: {
            auto result =
                SetAccountDescription_(actor_name, account_name, value);
            if (!result) {
              need_rollback = true;
            }
            break;
          }
          case crane::grpc::ModifyField::DefaultQos: {
            auto result = SetAccountDefaultQos_(actor_name, *account, value);
            if (!result) {
              need_rollback = true;
            }
            break;
          }

          default:
            std::unreachable();
          }
          break;
        }

        case crane::grpc::OperationType::Delete: {
          switch (operation.modify_field()) {
          case crane::grpc::ModifyField::Partition: {
            auto result =
                DeleteAccountAllowedPartition_(actor_name, *account, value);
            if (!result) {
              need_rollback = true;
            }
            break;
          }
          case crane::grpc::ModifyField::Qos: {
            auto result = DeleteAccountAllowedQos_(actor_name, *account, value);
            if (!result) {
              need_rollback = true;
            } else {
              delete_qos_change_num = result.value();
            }
            break;
          }
          default:
            std::unreachable();
          }
          break;
        }
        default:
          std::unreachable();
        }
      }
    }
  }

  if (need_rollback) {
    g_db_client->AbortTransaction();
    rich_error_list.emplace_back(
        std::unexpected{FormatRichErr(CraneErrCode::ERR_UPDATE_DATABASE, "")});
    return rich_error_list;
  } else {
    g_db_client->CommitTransactionWithoutCallback();
  }

  // do the modify operations in memory
  for (const auto& operation : operations) {
    if (operation.type() == crane::grpc::OperationType::Overwrite &&
        operation.modify_field() == crane::grpc::ModifyField::Partition) {
      std::list<std::string> deleted_partition;
      for (const auto& par : account->allowed_partition) {
        if (!ranges::contains(partition_list, par))
          deleted_partition.emplace_back(par);
      }
      for (const auto& par : deleted_partition) {
        DeleteAccountAllowedPartitionFromMapNoLock_(account->name, par);
      }
      m_account_map_[account_name]->allowed_partition.assign(
          std::make_move_iterator(partition_list.begin()),
          std::make_move_iterator(partition_list.end()));
    } else if (operation.type() == crane::grpc::OperationType::Overwrite &&
               operation.modify_field() == crane::grpc::ModifyField::Qos) {
      ;

      std::list<std::string> deleted_qos;
      for (const auto& qos : account->allowed_qos_list) {
        if (!ranges::contains(qos_list, qos)) deleted_qos.emplace_back(qos);
      }

      std::list<std::string> add_qos;
      for (const auto& qos : qos_list) {
        if (!ranges::contains(account->allowed_qos_list, qos))
          add_qos.emplace_back(qos);
      }

      for (const auto& qos : deleted_qos) {
        DeleteAccountAllowedQosFromMapNoLock_(account->name, qos);
        m_qos_map_[qos]->reference_count -= change_num.front();
        change_num.pop_front();
      }

      if (!add_qos.empty()) {
        if (account->default_qos.empty()) {
          m_account_map_[account_name]->default_qos = default_qos;
        }
        m_account_map_[account_name]->allowed_qos_list.assign(
            std::make_move_iterator(qos_list.begin()),
            std::make_move_iterator(qos_list.end()));
        for (const auto& qos : add_qos) {
          m_qos_map_[qos]->reference_count++;
        }
      }

    } else {
      for (const auto& value : operation.value_list()) {
        switch (operation.type()) {
        case crane::grpc::OperationType::Add: {
          switch (operation.modify_field()) {
          case crane::grpc::ModifyField::Partition: {
            m_account_map_[account_name]->allowed_partition.emplace_back(value);
            break;
          }

          case crane::grpc::ModifyField::Qos: {
            if (account->default_qos.empty()) {
              m_account_map_[account_name]->default_qos = value;
            }
            m_account_map_[account_name]->allowed_qos_list.emplace_back(value);
            m_qos_map_[value]->reference_count++;
            break;
          }

          default:
            std::unreachable();
          }
          break;
        }

        case crane::grpc::OperationType::Overwrite: {
          switch (operation.modify_field()) {
          case crane::grpc::ModifyField::Description: {
            m_account_map_[account_name]->description = value;
            break;
          }
          case crane::grpc::ModifyField::DefaultQos: {
            m_account_map_[account_name]->default_qos = value;
            break;
          }

          default:
            std::unreachable();
          }
          break;
        }

        case crane::grpc::OperationType::Delete: {
          switch (operation.modify_field()) {
          case crane::grpc::ModifyField::Partition: {
            DeleteAccountAllowedPartitionFromMapNoLock_(account->name, value);
            break;
          }
          case crane::grpc::ModifyField::Qos: {
            DeleteAccountAllowedQosFromMapNoLock_(account->name, value);
            m_qos_map_[value]->reference_count -= delete_qos_change_num;
            break;
          }
          default:
            std::unreachable();
          }
          break;
        }
        default:
          std::unreachable();
        }
      }
    }
  }

  return rich_error_list;
}

std::vector<CraneExpectedRich<void>> AccountManager::CheckModifyUserOperations(
    const User* op_user, const User* user, const Account* account_ptr,
    const std::string& actual_account, const std::string& partition,
    const std::vector<crane::grpc::ModifyFieldOperation>& operations,
    User& res_user, std::vector<std::string>& db_log, bool force) {
  const std::string& account_name = account_ptr->name;
  std::vector<CraneExpectedRich<void>> rich_error_list;
  for (const auto& operation : operations) {
    if (operation.type() == crane::grpc::OperationType::Delete) {
      switch (operation.modify_field()) {
      case crane::grpc::ModifyField::Partition: {
        for (const auto& value : operation.value_list()) {
          auto result = CheckDeleteUserAllowedPartitionNoLock_(
              *user, actual_account, value);
          if (!result) {
            auto rich_error =
                std::unexpected{FormatRichErr(result.error(), value)};
            rich_error_list.emplace_back(rich_error);
          }
        }
        break;
      }
      case crane::grpc::ModifyField::Qos: {
        break;
      }
      default:
        std::unreachable();
      }
    } else {
      switch (operation.modify_field()) {
      case crane::grpc::ModifyField::AdminLevel: {
        const auto& value = operation.value_list()[0];
        auto result =
            CheckIfUserHasHigherPrivThan_(*op_user, user->admin_level);
        if (!result) {
          rich_error_list.emplace_back(std::unexpected{
              FormatRichErr(CraneErrCode::ERR_PERMISSION_USER, "")});
        }
        User::AdminLevel new_level;
        if (value == "none")
          new_level = User::None;
        else if (value == "operator")
          new_level = User::Operator;
        else if (value == "admin")
          new_level = User::Admin;
        else {
          rich_error_list.emplace_back(std::unexpected{
              FormatRichErr(CraneErrCode::ERR_INVALID_ADMIN_LEVEL, "")});
        }

        if (op_user->admin_level <= new_level) {
          rich_error_list.emplace_back(std::unexpected{
              FormatRichErr(CraneErrCode::ERR_PERMISSION_USER, "")});
        }
        break;
      }
      case crane::grpc::ModifyField::Partition: {
        std::unordered_set<std::string> partition_list{
            operation.value_list().begin(), operation.value_list().end()};

        if (operation.type() == crane::grpc::OperationType::Add) {
          // has side effect
          auto rich_result_list = CheckAddUserAllowedPartitionNoLock_(
              user, account_ptr, partition_list, res_user);
          if (!rich_result_list.empty()) {
            rich_error_list.insert(rich_error_list.end(),
                                   rich_result_list.begin(),
                                   rich_result_list.end());
          }
          db_log.emplace_back(fmt::format("Add: account: {}, partition: {}",
                                          account_name,
                                          fmt::join(partition_list, ",")));
        } else if (operation.type() == crane::grpc::OperationType::Overwrite) {
          // has side effect
          auto rich_result = CheckSetUserAllowedPartitionNoLock_(
              account_ptr, partition_list, res_user);
          if (!rich_result) {
            rich_error_list.emplace_back(rich_result);
          }
          db_log.emplace_back(
              fmt::format("Set: account: {}, partition_list:[{}]", account_name,
                          fmt::join(partition_list, ",")));
        }

        break;
      }
      case crane::grpc::ModifyField::Qos: {
        std::unordered_set<std::string> qos_list(operation.value_list().begin(),
                                                 operation.value_list().end());
        if (operation.type() == crane::grpc::OperationType::Add) {
          // has side effect
          auto rich_result_list = CheckAddUserAllowedQosNoLock_(
              user, account_ptr, partition, qos_list, res_user);
          if (!rich_result_list.empty()) {
            rich_error_list.insert(rich_error_list.end(),
                                   rich_result_list.begin(),
                                   rich_result_list.end());
          }
          db_log.emplace_back(
              fmt::format("Add: account: {}, partition: {}, qos: {}",
                          account_name, partition, fmt::join(qos_list, ",")));
        } else if (operation.type() == crane::grpc::OperationType::Overwrite) {
          std::string default_qos = "";
          if (!operation.value_list().empty())
            default_qos = operation.value_list()[0];
          // has side effect
          auto rich_result = CheckSetUserAllowedQosNoLock_(
              user, account_ptr, partition, qos_list, default_qos, res_user,
              force);
          if (!rich_result) {
            rich_error_list.emplace_back(rich_result);
          }
          db_log.emplace_back(
              fmt::format("Set: account: {}, partition: {}, qos_list: {}",
                          account_name, partition, fmt::join(qos_list, ",")));
        }
        break;
      }
      case crane::grpc::ModifyField::DefaultQos: {
        // has side effect
        auto result =
            CheckSetUserDefaultQosNoLock_(*user, actual_account, partition,
                                          operation.value_list()[0], res_user);
        if (!result) {
          auto rich_error = std::unexpected{
              FormatRichErr(result.error(), operation.value_list()[0])};
          rich_error_list.emplace_back(rich_error);
        }
        db_log.emplace_back(
            fmt::format("account: {}, partition: {}, default_qos: {}",
                        actual_account, partition, operation.value_list()[0]));
        break;
      }
      case crane::grpc::ModifyField::DefaultAccount: {
        auto result =
            CheckIfUserHasHigherPrivThan_(*op_user, user->admin_level);
        if (!result) {
          auto rich_error = std::unexpected{
              FormatRichErr(CraneErrCode::ERR_PERMISSION_USER, "")};
          rich_error_list.emplace_back(rich_error);
        }
        if (!user->account_to_attrs_map.contains(operation.value_list()[0])) {
          {
            auto rich_error = std::unexpected{
                FormatRichErr(CraneErrCode::ERR_USER_ACCESS_TO_ACCOUNT_DENIED,
                              operation.value_list()[0])};
            rich_error_list.emplace_back(rich_error);
          }
        }
        break;
      }
      default:
        std::unreachable();
      }
    }
  }
  return rich_error_list;
}

std::vector<CraneExpectedRich<void>> AccountManager::ModifyUser(
    uint32_t uid, const std::string& name, const std::string& account,
    const std::string& partition,
    std::vector<crane::grpc::ModifyFieldOperation>& operations, bool force) {
  CraneExpected<void> result;
  std::vector<CraneExpectedRich<void>> rich_error_list;
  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::read_lock_guard account_guard(m_rw_account_mutex_);
  util::read_lock_guard qos_guard(m_rw_qos_mutex_);

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) {
    rich_error_list.emplace_back(
        std::unexpected{FormatRichErr(user_result.error(), "")});
    return rich_error_list;
  }

  const User* user = GetExistedUserInfoNoLock_(name);
  if (!user) {
    rich_error_list.emplace_back(
        std::unexpected{FormatRichErr(CraneErrCode::ERR_INVALID_USER, "")});
    return rich_error_list;
  }
  const User* op_user = user_result.value();
  std::string actual_account = account;
  std::string actor_name;

  result = CheckIfUserHasPermOnUserOfAccountNoLock_(*op_user, user,
                                                    &actual_account, false);
  if (!result) {
    rich_error_list.emplace_back(
        std::unexpected{FormatRichErr(result.error(), "")});
    return rich_error_list;
  }

  actor_name = op_user->name;

  const Account* account_ptr = GetExistedAccountInfoNoLock_(actual_account);
  if (!account_ptr) {
    rich_error_list.emplace_back(std::unexpected{
        FormatRichErr(CraneErrCode::ERR_INVALID_ACCOUNT, actual_account)});
    return rich_error_list;
  }

  User res_user(user);
  std::vector<std::string> db_log;
  // check user
  auto check_result =
      CheckModifyUserOperations(op_user, user, account_ptr, actual_account,
                                partition, operations, res_user, db_log, force);
  if (!check_result.empty()) {
    return check_result;
  }

  bool need_rollback = false;
  g_db_client->StartTransaction();
  // do the modify operations in database
  for (const auto& operation : operations) {
    if (need_rollback) break;
    if (operation.type() == crane::grpc::OperationType::Delete) {
      switch (operation.modify_field()) {
      case crane::grpc::ModifyField::Partition: {
        for (const auto& value : operation.value_list()) {
          result = DeleteUserAllowedPartition_(op_user->name, *user,
                                               actual_account, value);
          if (!result) {
            need_rollback = true;
          }
        }
        break;
      }
      case crane::grpc::ModifyField::Qos: {
        for (const auto& value : operation.value_list()) {
          result = DeleteUserAllowedQos_(op_user->name, *user, value,
                                         actual_account, partition, force);
          if (!result) {
            need_rollback = true;
          }
        }
        break;
      }
      default:
        std::unreachable();
      }
    } else {
      switch (operation.modify_field()) {
      case crane::grpc::ModifyField::AdminLevel: {
        User::AdminLevel new_level;
        auto value = operation.value_list()[0];
        // it has been checked yet
        if (value == "none")
          new_level = User::None;
        else if (value == "operator")
          new_level = User::Operator;
        else
          new_level = User::Admin;
        result = SetUserAdminLevel_(op_user->name, name, new_level);
        if (!result) {
          need_rollback = true;
        }
        break;
      }
      case crane::grpc::ModifyField::Partition:
      case crane::grpc::ModifyField::Qos:
      case crane::grpc::ModifyField::DefaultQos: {
        break;
      }
      case crane::grpc::ModifyField::DefaultAccount: {
        result = SetUserDefaultAccount_(op_user->name, name,
                                        operation.value_list()[0]);
        if (!result) {
          need_rollback = true;
        }
        break;
      }
      default:
        std::unreachable();
      }
    }
  }

  need_rollback = need_rollback || !g_db_client->UpdateUser(res_user);

  if (need_rollback) {
    g_db_client->AbortTransaction();
    rich_error_list.emplace_back(
        std::unexpected{FormatRichErr(CraneErrCode::ERR_UPDATE_DATABASE, "")});
    return rich_error_list;
  } else {
    g_db_client->CommitTransactionWithoutCallback();
  }

  for (const auto& log : db_log) {
    AddTxnLogToDB_(actor_name, name, TxnAction::ModifyUser, log);
  }

  for (const auto& operation : operations) {
    if (operation.type() == crane::grpc::OperationType::Delete) {
      switch (operation.modify_field()) {
      case crane::grpc::ModifyField::Partition: {
        for (const auto& value : operation.value_list()) {
          m_user_map_[user->name]
              ->account_to_attrs_map[account]
              .allowed_partition_qos_map.erase(value);
        }
        break;
      }
      case crane::grpc::ModifyField::Qos: {
        m_user_map_[user->name]
            ->account_to_attrs_map[account]
            .allowed_partition_qos_map =
            res_user.account_to_attrs_map[account].allowed_partition_qos_map;

        break;
      }
      default:
        std::unreachable();
      }
    } else {
      switch (operation.modify_field()) {
      case crane::grpc::ModifyField::AdminLevel: {
        User::AdminLevel new_level;
        auto value = operation.value_list()[0];
        // it has been checked yet
        if (value == "none")
          new_level = User::None;
        else if (value == "operator")
          new_level = User::Operator;
        else
          new_level = User::Admin;
        m_user_map_[name]->admin_level = new_level;
        break;
      }
      case crane::grpc::ModifyField::Partition: {
        m_user_map_[name]
            ->account_to_attrs_map[account_ptr->name]
            .allowed_partition_qos_map =
            res_user.account_to_attrs_map[account_ptr->name]
                .allowed_partition_qos_map;
        break;
      }
      case crane::grpc::ModifyField::Qos: {
        m_user_map_[name]
            ->account_to_attrs_map[account_ptr->name]
            .allowed_partition_qos_map =
            res_user.account_to_attrs_map[account_ptr->name]
                .allowed_partition_qos_map;
        break;
      }
      case crane::grpc::ModifyField::DefaultQos: {
        m_user_map_[name]
            ->account_to_attrs_map[account]
            .allowed_partition_qos_map =
            res_user.account_to_attrs_map[account].allowed_partition_qos_map;
        break;
      }
      case crane::grpc::ModifyField::DefaultAccount: {
        m_user_map_[name]->default_account = operation.value_list()[0];
        break;
      }
      default:
        std::unreachable();
      }
    }
  }
  return rich_error_list;
}

std::vector<CraneExpectedRich<void>> AccountManager::ModifyQos(
    uint32_t uid, const std::string& name,
    const std::vector<crane::grpc::ModifyFieldOperation>& operations) {
  std::vector<CraneExpectedRich<void>> rich_error_list;
  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) {
      rich_error_list.emplace_back(
          std::unexpected{FormatRichErr(user_result.error(), "")});
      return rich_error_list;
    }
    const User* op_user = user_result.value();

    auto result = CheckIfUserHasHigherPrivThan_(*op_user, User::None);
    if (!result) {
      rich_error_list.emplace_back(
          std::unexpected{FormatRichErr(result.error(), "")});
      return rich_error_list;
    }
  }
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  const Qos* p = GetExistedQosInfoNoLock_(name);
  if (!p) {
    rich_error_list.emplace_back(
        std::unexpected{FormatRichErr(CraneErrCode::ERR_INVALID_QOS, name)});
    return rich_error_list;
  }

  // check operations
  for (const auto& operation : operations) {
    auto value = operation.value_list()[0];
    auto item = Qos::GetModifyFieldStr(operation.modify_field());
    int64_t value_number;
    if (item != Qos::FieldStringOfDescription()) {
      bool ok = util::ConvertStringToInt64(value, &value_number);
      if (!ok) {
        rich_error_list.emplace_back(std::unexpected{
            FormatRichErr(CraneErrCode::ERR_CONVERT_TO_INTEGER, value)});
      }

      if (item == Qos::FieldStringOfMaxTimeLimitPerTask() &&
          !CheckIfTimeLimitSecIsValid(value_number)) {
        rich_error_list.emplace_back(std::unexpected{
            FormatRichErr(CraneErrCode::ERR_TIME_LIMIT, value)});
      }
    }
  }

  if (!rich_error_list.empty()) {
    return rich_error_list;
  }

  std::vector<Qos> qos_list;
  bool need_rollback = false;

  // do the modify operations in database

  g_db_client->StartTransaction();
  for (const auto& operation : operations) {
    auto value = operation.value_list()[0];
    auto item = Qos::GetModifyFieldStr(operation.modify_field());
    bool result = true;
    if (item == "description") {
      // Update to database
      result =
          result && g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS,
                                                 "$set", name, item, value);
    } else {
      /* uint32 Type Stores data based on long(int64_t) */
      int64_t value_number;
      util::ConvertStringToInt64(value, &value_number);
      result = result &&
               g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS,
                                            "$set", name, item, value_number);
    }
    Qos qos;
    result = result && g_db_client->SelectQos("name", name, &qos);
    if (!result) {
      need_rollback = true;
      break;
    }
    qos_list.emplace_back(qos);
  }
  if (need_rollback) {
    g_db_client->AbortTransaction();
    rich_error_list.emplace_back(
        std::unexpected{FormatRichErr(CraneErrCode::ERR_UPDATE_DATABASE, "")});
    return rich_error_list;
  } else {
    g_db_client->CommitTransactionWithoutCallback();
  }
  // do the modify operations in memory
  for (auto qos : qos_list) {
    *m_qos_map_[name] = std::move(qos);
  }
  return rich_error_list;
}

CraneExpected<void> AccountManager::ModifyDefaultWckey(
    uint32_t uid, const std::string& name, const std::string& user_name) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();

  const User* p_target_user = GetExistedUserInfoNoLock_(user_name);
  if (!p_target_user) return std::unexpected(CraneErrCode::ERR_INVALID_USER);
  if (op_user->uid != 0) {
    auto result =
        CheckIfUserHasHigherPrivThan_(*op_user, p_target_user->admin_level);
    if (!result) return result;
  }

  util::write_lock_guard wckey_guard(m_rw_wckey_mutex_);

  const Wckey* p = GetExistedWckeyInfoNoLock_(name, user_name);
  if (!p) return std::unexpected(CraneErrCode::ERR_INVALID_WCKEY);

  return SetUserDefaultWckey_(name, user_name);
}

CraneExpected<void> AccountManager::BlockAccount(uint32_t uid,
                                                 const std::string& name,
                                                 bool block) {
  std::string actor_name;
  {
    util::read_lock_guard user_guard(m_rw_user_mutex_);
    util::read_lock_guard account_guard(m_rw_account_mutex_);

    auto user_result = GetUserInfoByUidNoLock_(uid);
    if (!user_result) return std::unexpected(user_result.error());
    const User* op_user = user_result.value();

    auto result = CheckIfUserHasPermOnAccountNoLock_(*op_user, name, false);
    if (!result) return result;
    actor_name = op_user->name;
  }

  util::write_lock_guard account_guard(m_rw_account_mutex_);

  const Account* account = GetExistedAccountInfoNoLock_(name);
  if (!account) return std::unexpected(CraneErrCode::ERR_INVALID_ACCOUNT);

  if (account->blocked == block) return {};

  return BlockAccountNoLock_(actor_name, name, block);
}

CraneExpected<void> AccountManager::BlockUser(uint32_t uid,
                                              const std::string& name,
                                              const std::string& account,
                                              bool block) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());

  const User* user = GetExistedUserInfoNoLock_(name);
  if (!user) return std::unexpected(CraneErrCode::ERR_INVALID_USER);

  std::string actual_account = account;
  std::string actor_name;
  {
    util::read_lock_guard account_guard(m_rw_account_mutex_);
    auto result = CheckIfUserHasPermOnUserOfAccountNoLock_(
        *user_result.value(), user, &actual_account, false);
    if (!result) return result;
    actor_name = user_result.value()->name;
  }

  if (user->account_to_attrs_map.at(actual_account).blocked == block) return {};

  return BlockUserNoLock_(actor_name, name, actual_account, block);
}

CraneExpected<std::list<Txn>> AccountManager::QueryTxnList(
    uint32_t uid,
    const std::unordered_map<std::string, std::string>& conditions,
    int64_t start_time, int64_t end_time) {
  auto result = CheckUidIsAdmin(uid);
  if (!result) return std::unexpected(result.error());

  std::list<Txn> txn_list;
  g_db_client->SelectTxns(conditions, start_time, end_time, &txn_list);

  return std::move(txn_list);
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

CraneExpected<void> AccountManager::CheckIfUserOfAccountIsEnabled(
    const std::string& user, const std::string& account) {
  util::read_lock_guard user_guard(m_rw_user_mutex_);
  util::read_lock_guard account_guard(m_rw_account_mutex_);
  std::string account_name = account;

  do {
    const Account* account_ptr = GetExistedAccountInfoNoLock_(account_name);
    if (account_ptr->blocked) {
      CRANE_DEBUG("Ancestor account '{}' is blocked", account_ptr->name);
      return std::unexpected(CraneErrCode::ERR_BLOCKED_ACCOUNT);
    }
    account_name = account_ptr->parent_account;
  } while (!account_name.empty());

  const User* user_ptr = GetExistedUserInfoNoLock_(user);
  if (user_ptr->account_to_attrs_map.at(account).blocked) {
    CRANE_DEBUG("User '{}' is blocked", user_ptr->name);
    return std::unexpected(CraneErrCode::ERR_BLOCKED_USER);
  }
  return {};
}

CraneExpected<void> AccountManager::CheckQosLimitOnTask(
    const std::string& user, const std::string& account, TaskInCtld* task) {
  util::read_lock_guard user_guard(m_rw_user_mutex_);

  const User* user_share_ptr = GetExistedUserInfoNoLock_(user);
  if (!user_share_ptr) {
    CRANE_ERROR(
        "The current user {} is not in the user list when submitting the "
        "task",
        user);
    return std::unexpected(CraneErrCode::ERR_INVALID_OP_USER);
  }

  if (task->uid != 0) {
    auto partition_it = user_share_ptr->account_to_attrs_map.at(account)
                            .allowed_partition_qos_map.find(task->partition_id);
    if (partition_it == user_share_ptr->account_to_attrs_map.at(account)
                            .allowed_partition_qos_map.end()) {
      CRANE_ERROR(
          "This user {} does not have partition {} permission when "
          "submitting "
          "the task",
          user, task->partition_id);
      return std::unexpected(CraneErrCode::ERR_PARTITION_MISSING);
    }
    if (task->qos.empty()) {
      // Default qos
      task->qos = partition_it->second.first;
      if (task->qos.empty()) {
        CRANE_ERROR(
            "The user '{}' has no QOS available for this partition '{}' to "
            "be "
            "used",
            task->Username(), task->partition_id);
        return std::unexpected(CraneErrCode::ERR_HAS_NO_QOS_IN_PARTITION);
      }
    } else {
      // Check whether task.qos in the qos list
      if (!ranges::contains(partition_it->second.second, task->qos)) {
        CRANE_ERROR(
            "The set qos '{}' is not in partition's allowed qos list when "
            "submitting the task",
            task->qos);
        return std::unexpected(CraneErrCode::ERR_HAS_ALLOWED_QOS_IN_PARTITION);
      }
    }
  } else {
    if (task->qos.empty()) {
      task->qos = kUnlimitedQosName;
    }
  }
  return {};
}

CraneExpected<std::string> AccountManager::CheckUidIsAdmin(uint32_t uid) {
  util::read_lock_guard user_guard(m_rw_user_mutex_);
  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) {
    return std::unexpected(CraneErrCode::ERR_INVALID_USER);
  }
  const User* user_ptr = user_result.value();

  if (user_ptr->admin_level >= User::Operator) return {};

  return std::unexpected(CraneErrCode::ERR_USER_NO_PRIVILEGE);
}

CraneExpected<void> AccountManager::CheckIfUidHasPermOnUser(
    uint32_t uid, const std::string& username, bool read_only_priv) {
  util::read_lock_guard user_guard(m_rw_user_mutex_);
  util::read_lock_guard account_guard(m_rw_account_mutex_);

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());

  const User* user = GetExistedUserInfoNoLock_(username);
  if (!user) return std::unexpected(CraneErrCode::ERR_INVALID_USER);

  return CheckIfUserHasPermOnUserNoLock_(*user_result.value(), user,
                                         read_only_priv);
}

CraneExpected<std::string> AccountManager::SignUserCertificate(
    uint32_t uid, const std::string& csr_content,
    const std::string& alt_names) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();

  // Verify whether the serial number already exists in the user database.
  if (!op_user->cert_number.empty())
    return std::unexpected(CraneErrCode::ERR_DUPLICATE_CERTIFICATE);

  std::string common_name =
      std::format("{}.{}", uid, g_config.ListenConf.TlsConfig.DomainSuffix);
  auto sign_response =
      g_vault_client->Sign(csr_content, common_name, alt_names);
  if (!sign_response)
    return std::unexpected(CraneErrCode::ERR_SIGN_CERTIFICATE);

  // Save the serial number in the database.
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$set",
                                     op_user->name, "cert_number",
                                     sign_response->serial_number);
      };

  // Update to database
  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  m_user_map_[op_user->name]->cert_number = sign_response->serial_number;

  CRANE_INFO("The user {} successfully signed the certificate.", op_user->name);

  return sign_response->certificate;
}

CraneExpected<void> AccountManager::CheckModifyPartitionAcl(
    uint32_t uid, const std::string& partition_name,
    const std::unordered_set<std::string>& accounts) {
  CraneExpected<void> result{};

  util::read_lock_guard user_guard(m_rw_user_mutex_);
  util::read_lock_guard account_guard(m_rw_account_mutex_);

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();
  result = CheckIfUserHasHigherPrivThan_(*op_user, User::None);
  if (!result) return result;

  for (const auto& account_name : accounts) {
    const Account* account = GetAccountInfoNoLock_(account_name);
    if (!account) return std::unexpected(CraneErrCode::ERR_INVALID_ACCOUNT);
    result =
        CheckPartitionIsAllowedNoLock_(account, partition_name, false, false);
    if (!result) return std::unexpected(result.error());
  }

  return result;
}

CraneExpected<void> AccountManager::ResetUserCertificate(
    uint32_t uid, const std::string& username) {
  util::write_lock_guard user_guard(m_rw_user_mutex_);
  CraneExpected<void> result{};

  auto user_result = GetUserInfoByUidNoLock_(uid);
  if (!user_result) return std::unexpected(user_result.error());
  const User* op_user = user_result.value();

  const auto p_target_user = GetExistedUserInfoNoLock_(username);
  if (!p_target_user) return std::unexpected(CraneErrCode::ERR_INVALID_USER);

  result = CheckIfUserHasHigherPrivThan_(*op_user, User::None);
  if (!result) return result;

  if (!p_target_user->cert_number.empty() &&
      !g_vault_client->RevokeCert(p_target_user->cert_number))
    return std::unexpected(CraneErrCode::ERR_REVOKE_CERTIFICATE);

  // Save the serial number in the database.
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$set",
                                     p_target_user->name, "cert_number", "");
      };

  // Update to database
  if (!g_db_client->CommitTransaction(callback))
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);

  CRANE_DEBUG("Reset User {} Certificate {}", p_target_user->name,
              p_target_user->cert_number);

  m_user_map_[p_target_user->name]->cert_number = "";

  return result;
}

/******************************************
 * NOLOCK
 ******************************************/

std::vector<CraneExpectedRich<void>>
AccountManager::CheckAddUserAllowedPartitionNoLock_(
    const User* user, const Account* account,
    const std::unordered_set<std::string>& partition_list, User& res_user) {
  std::vector<CraneExpectedRich<void>> rich_error_list;
  const std::string& account_name = account->name;
  for (const auto& partition : partition_list) {
    auto result =
        CheckPartitionIsAllowedNoLock_(account, partition, false, true);
    if (!result) {
      rich_error_list.emplace_back(
          std::unexpected(FormatRichErr(result.error(), partition)));
      continue;
    }

    if (user->account_to_attrs_map.at(account_name)
            .allowed_partition_qos_map.contains(partition)) {
      rich_error_list.emplace_back(std::unexpected(FormatRichErr(
          CraneErrCode::ERR_PARTITION_ALREADY_EXISTS, partition)));
    }
  }
  if (!rich_error_list.empty()) {
    return rich_error_list;
  }
  for (const auto& partition : partition_list) {
    res_user.account_to_attrs_map[account_name]
        .allowed_partition_qos_map[partition] =
        std::pair<std::string, std::list<std::string>>{
            account->default_qos,
            std::list<std::string>{account->allowed_qos_list}};
  }

  return rich_error_list;
}

CraneExpectedRich<void> AccountManager::CheckSetUserAllowedPartitionNoLock_(
    const Account* account,
    const std::unordered_set<std::string>& partition_list, User& res_user) {
  res_user.account_to_attrs_map[account->name]
      .allowed_partition_qos_map.clear();  // clear the partitions
  for (const auto& partition : partition_list) {
    auto result =
        CheckPartitionIsAllowedNoLock_(account, partition, false, true);
    if (!result)
      return std::unexpected(FormatRichErr(result.error(), partition));

    res_user.account_to_attrs_map[account->name]
        .allowed_partition_qos_map[partition] =
        std::pair<std::string, std::list<std::string>>{
            account->default_qos,
            std::list<std::string>{account->allowed_qos_list}};
  }
  return {};
}

std::vector<CraneExpectedRich<void>>
AccountManager::CheckAddUserAllowedQosNoLock_(
    const User* user, const Account* account, const std::string& partition,
    const std::unordered_set<std::string>& qos_list, User& res_user) {
  const std::string& account_name = account->name;
  const std::string& name = user->name;
  //  check if add item already the user's allowed qos
  const auto& attrs_in_account_map =
      user->account_to_attrs_map.at(account_name);
  std::vector<CraneExpectedRich<void>> rich_error_list;
  for (const auto& qos : qos_list) {
    auto result = CheckQosIsAllowedNoLock_(account, qos, false, true);
    if (!result) {
      rich_error_list.emplace_back(
          std::unexpected(FormatRichErr(result.error(), qos)));
      continue;
    }
    if (partition.empty()) {
      // When the user has no partition, QoS cannot be added.
      if (attrs_in_account_map.allowed_partition_qos_map.empty()) {
        rich_error_list.emplace_back(std::unexpected(
            FormatRichErr(CraneErrCode::ERR_USER_EMPTY_PARTITION, partition)));
        break;
      }

      bool is_allowed = false;
      for (const auto& [par, pair] :
           attrs_in_account_map.allowed_partition_qos_map) {
        const std::list<std::string>& list = pair.second;
        if (!ranges::contains(list, qos)) {
          is_allowed = true;
          break;
        }
      }
      if (!is_allowed) {
        rich_error_list.emplace_back(std::unexpected(
            FormatRichErr(CraneErrCode::ERR_QOS_ALREADY_EXISTS, qos)));
      }
    } else {
      auto iter =
          attrs_in_account_map.allowed_partition_qos_map.find(partition);
      if (iter == attrs_in_account_map.allowed_partition_qos_map.end()) {
        rich_error_list.emplace_back(std::unexpected(
            FormatRichErr(CraneErrCode::ERR_PARTITION_MISSING, partition)));
      }
      const std::list<std::string>& list = iter->second.second;
      if (ranges::contains(list, qos)) {
        rich_error_list.emplace_back(std::unexpected(
            FormatRichErr(CraneErrCode::ERR_QOS_ALREADY_EXISTS, qos)));
      }
    }
  }
  if (!rich_error_list.empty()) {
    return rich_error_list;
  }
  for (const auto& qos : qos_list) {
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
  }
  return rich_error_list;
}

CraneExpectedRich<void> AccountManager::CheckSetUserAllowedQosNoLock_(
    const User* user, const Account* account, const std::string& partition,
    const std::unordered_set<std::string>& qos_list, std::string& default_qos,
    User& res_user, bool force) {
  for (const auto& qos : qos_list) {
    auto result = CheckQosIsAllowedNoLock_(account, qos, false, true);
    if (!result) return std::unexpected(FormatRichErr(result.error(), qos));
  }

  const std::string& account_name = account->name;

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
      return std::unexpected(
          FormatRichErr(CraneErrCode::ERR_PARTITION_MISSING, partition));
    }
    cache_allowed_partition_qos_map.insert({iter->first, iter->second});
  }

  for (const auto& [par, pair] : cache_allowed_partition_qos_map) {
    if (!ranges::contains(qos_list, pair.first)) {
      if (!force && !pair.first.empty())
        return std::unexpected(
            FormatRichErr(CraneErrCode::ERR_SET_ALLOWED_QOS, ""));
    }
  }

  if (partition.empty()) {
    // Set the qos of all partition
    for (auto& [par, pair] : res_user.account_to_attrs_map[account_name]
                                 .allowed_partition_qos_map) {
      if (!ranges::contains(qos_list, pair.first))
        pair.first = qos_list.empty() ? "" : default_qos;
      pair.second.assign(std::make_move_iterator(qos_list.begin()),
                         std::make_move_iterator(qos_list.end()));
    }
  } else {
    // Set the qos of a specified partition
    auto iter = res_user.account_to_attrs_map[account_name]
                    .allowed_partition_qos_map.find(partition);

    if (!ranges::contains(qos_list, iter->second.first))
      iter->second.first = qos_list.empty() ? "" : default_qos;

    iter->second.second.assign(std::make_move_iterator(qos_list.begin()),
                               std::make_move_iterator(qos_list.end()));
  }
  return {};
}

CraneExpected<void> AccountManager::CheckSetUserDefaultQosNoLock_(
    const User& user, const std::string& account, const std::string& partition,
    const std::string& qos, User& res_user) {
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
      return std::unexpected(CraneErrCode::ERR_PARTITION_MISSING);

    if (!ranges::contains(iter->second.second, qos))
      return std::unexpected(CraneErrCode::ERR_QOS_MISSING);

    if (iter->second.first == qos)
      return std::unexpected(CraneErrCode::ERR_DUPLICATE_DEFAULT_QOS);
  }

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
  return {};
}

CraneExpected<void> AccountManager::CheckDeleteUserAllowedPartitionNoLock_(
    const User& user, const std::string& account,
    const std::string& partition) {
  if (!user.account_to_attrs_map.at(account).allowed_partition_qos_map.contains(
          partition))
    return std::unexpected(CraneErrCode::ERR_PARTITION_MISSING);

  return {};
}

CraneExpected<void> AccountManager::CheckDeleteUserAllowedQosNoLock_(
    const User& user, const std::string& account, const std::string& partition,
    const std::string& qos, bool force) {
  const auto& attrs_in_account = user.account_to_attrs_map.at(account);
  if (partition.empty()) {
    bool is_allowed = false;
    for (const auto& [par, pair] : attrs_in_account.allowed_partition_qos_map) {
      if (ranges::contains(pair.second, qos)) {
        is_allowed = true;
        if (pair.first == qos && !force)
          return std::unexpected(
              CraneErrCode::ERR_DEFAULT_QOS_MODIFICATION_DENIED);
      }
      if (!is_allowed) return std::unexpected(CraneErrCode::ERR_QOS_MISSING);
    }
  } else {
    // Delete the qos of a specified partition
    auto iter = attrs_in_account.allowed_partition_qos_map.find(partition);

    if (iter == attrs_in_account.allowed_partition_qos_map.end())
      return std::unexpected(CraneErrCode::ERR_PARTITION_MISSING);

    if (!ranges::contains(iter->second.second, qos))
      return std::unexpected(CraneErrCode::ERR_QOS_MISSING);

    if (qos == iter->second.first && !force)
      return std::unexpected(CraneErrCode::ERR_DEFAULT_QOS_MODIFICATION_DENIED);
  }

  return {};
}

CraneExpected<void> AccountManager::CheckAddAccountAllowedPartitionNoLock_(
    const Account* account, const std::string& partition) {
  auto result = CheckPartitionIsAllowedNoLock_(account, partition, true, false);
  if (!result) return result;

  if (ranges::contains(account->allowed_partition, partition))
    return std::unexpected(CraneErrCode::ERR_PARTITION_ALREADY_EXISTS);

  return {};
}

CraneExpected<void> AccountManager::CheckAddAccountAllowedQosNoLock_(
    const Account* account, const std::string& qos) {
  auto result = CheckQosIsAllowedNoLock_(account, qos, true, false);
  if (!result) return result;

  if (ranges::contains(account->allowed_qos_list, qos))
    return std::unexpected(CraneErrCode::ERR_QOS_ALREADY_EXISTS);

  return {};
}

CraneExpected<void> AccountManager::CheckSetAccountDescriptionNoLock_(
    const Account* account) {
  if (!account) return std::unexpected(CraneErrCode::ERR_INVALID_ACCOUNT);

  return {};
}

CraneExpectedRich<void> AccountManager::CheckSetAccountAllowedPartitionNoLock_(
    const Account* account,
    const std::unordered_set<std::string>& partition_list, bool force) {
  for (const auto& partition : partition_list) {
    auto result =
        CheckPartitionIsAllowedNoLock_(account, partition, true, false);
    if (!result)
      return std::unexpected(FormatRichErr(result.error(), partition));
  }

  for (const auto& par : account->allowed_partition) {
    if (!ranges::contains(partition_list, par)) {
      if (!force && IsAllowedPartitionOfAnyNodeNoLock_(account, par))
        return std::unexpected(
            FormatRichErr(CraneErrCode::ERR_CHILD_HAS_PARTITION, ""));
    }
  }

  return {};
}

CraneExpectedRich<void> AccountManager::CheckSetAccountAllowedQosNoLock_(
    const Account* account, const std::unordered_set<std::string>& qos_list,
    bool force) {
  for (const auto& qos : qos_list) {
    auto result = CheckQosIsAllowedNoLock_(account, qos, true, false);
    if (!result) return std::unexpected(FormatRichErr(result.error(), qos));
  }

  for (const auto& qos : account->allowed_qos_list) {
    if (!ranges::contains(qos_list, qos)) {
      if (!force && IsDefaultQosOfAnyNodeNoLock_(account, qos))
        return std::unexpected(
            FormatRichErr(CraneErrCode::ERR_SET_ACCOUNT_QOS, ""));
    }
  }

  return {};
}

CraneExpected<void> AccountManager::CheckSetAccountDefaultQosNoLock_(
    const Account* account, const std::string& qos) {
  auto result = CheckQosIsAllowedNoLock_(account, qos, false, false);
  if (!result) return result;

  if (account->default_qos == qos)
    return std::unexpected(CraneErrCode::ERR_DUPLICATE_DEFAULT_QOS);

  return {};
}

CraneExpected<void> AccountManager::CheckDeleteAccountAllowedPartitionNoLock_(
    const Account* account, const std::string& partition, bool force) {
  auto result =
      CheckPartitionIsAllowedNoLock_(account, partition, false, false);
  if (!result) return result;

  if (!force && IsAllowedPartitionOfAnyNodeNoLock_(account, partition))
    return std::unexpected(CraneErrCode::ERR_CHILD_HAS_PARTITION);

  return {};
}

CraneExpected<void> AccountManager::CheckDeleteAccountAllowedQosNoLock_(
    const Account* account, const std::string& qos, bool force) {
  auto result = CheckQosIsAllowedNoLock_(account, qos, false, false);
  if (!result) return result;

  if (!force && account->default_qos == qos)
    return std::unexpected(CraneErrCode::ERR_DEFAULT_QOS_MODIFICATION_DENIED);

  if (!force && IsDefaultQosOfAnyNodeNoLock_(account, qos))
    return std::unexpected(CraneErrCode::ERR_CHILD_HAS_DEFAULT_QOS);

  return {};
}

CraneExpected<void> AccountManager::CheckIfUserHasHigherPrivThan_(
    const User& op_user, User::AdminLevel admin_level) {
  if (op_user.admin_level <= admin_level)
    return std::unexpected(CraneErrCode::ERR_PERMISSION_USER);

  return {};
}

CraneExpected<void> AccountManager::CheckIfUserHasPermOnAccountNoLock_(
    const User& op_user, const std::string& account, bool read_only_priv) {
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

    return std::unexpected(CraneErrCode::ERR_USER_ACCESS_TO_ACCOUNT_DENIED);
  }

  return {};
}

CraneExpected<void> AccountManager::CheckIfUserHasPermOnUserOfAccountNoLock_(
    const User& op_user, const User* user, std::string* account,
    bool read_only_priv) {
  CRANE_ASSERT(user != nullptr);

  if (account->empty()) *account = user->default_account;

  if (op_user.admin_level < user->admin_level)
    return std::unexpected(CraneErrCode::ERR_PERMISSION_USER);

  if (!user->account_to_attrs_map.contains(*account))
    return std::unexpected(CraneErrCode::ERR_USER_ACCOUNT_MISMATCH);

  return CheckIfUserHasPermOnAccountNoLock_(op_user, *account, read_only_priv);
}

CraneExpected<void> AccountManager::CheckIfUserHasPermOnUserNoLock_(
    const User& op_user, const User* user, bool read_only_priv) {
  CRANE_ASSERT(user != nullptr);

  if (CheckIfUserHasHigherPrivThan_(op_user, user->admin_level) ||
      op_user.name == user->name)
    return {};

  for (const auto& [acct, item] : user->account_to_attrs_map) {
    if (CheckIfUserHasPermOnAccountNoLock_(op_user, acct, read_only_priv))
      return {};
  }

  return std::unexpected(CraneErrCode::ERR_PERMISSION_USER);
}

CraneExpected<void> AccountManager::CheckPartitionIsAllowedNoLock_(
    const Account* account, const std::string& partition, bool check_parent,
    bool is_user) {
  CRANE_ASSERT(account != nullptr);

  // check if new partition existed
  if (!g_config.Partitions.contains(partition))
    return std::unexpected(CraneErrCode::ERR_INVALID_PARTITION);

  if (!check_parent) {
    // check if account has access to new partition
    if (!ranges::contains(account->allowed_partition, partition)) {
      if (is_user)
        return std::unexpected(
            CraneErrCode::ERR_PARENT_ACCOUNT_PARTITION_MISSING);
      return std::unexpected(CraneErrCode::ERR_PARTITION_MISSING);
    }
  } else {
    // Check if parent account has access to the partition
    if (!account->parent_account.empty()) {
      const Account* parent =
          GetExistedAccountInfoNoLock_(account->parent_account);
      if (!ranges::contains(parent->allowed_partition, partition))
        return std::unexpected(
            CraneErrCode::ERR_PARENT_ACCOUNT_PARTITION_MISSING);
    }
  }

  return {};
}

CraneExpected<void> AccountManager::CheckQosIsAllowedNoLock_(
    const Account* account, const std::string& qos, bool check_parent,
    bool is_user) {
  CRANE_ASSERT(account != nullptr);

  // check if the qos existed
  if (!GetExistedQosInfoNoLock_(qos))
    return std::unexpected(CraneErrCode::ERR_INVALID_QOS);

  if (!check_parent) {
    // check if account has access to new qos
    if (!ranges::contains(account->allowed_qos_list, qos)) {
      if (is_user)
        return std::unexpected(CraneErrCode::ERR_PARENT_ACCOUNT_QOS_MISSING);
      return std::unexpected(CraneErrCode::ERR_QOS_MISSING);
    }
  } else {
    // Check if parent account has access to the qos
    if (!account->parent_account.empty()) {
      const Account* parent =
          GetExistedAccountInfoNoLock_(account->parent_account);
      if (!ranges::contains(parent->allowed_qos_list, qos))
        return std::unexpected(CraneErrCode::ERR_PARENT_ACCOUNT_QOS_MISSING);
    }
  }

  return {};
}

void AccountManager::InitDataMap_() {
  util::write_lock_guard user_guard(m_rw_user_mutex_);
  util::write_lock_guard account_guard(m_rw_account_mutex_);
  util::write_lock_guard qos_guard(m_rw_qos_mutex_);
  util::write_lock_guard wckey_guard(m_rw_wckey_mutex_);

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

  std::list<Wckey> wckey_list;
  g_db_client->SelectAllWckey(&wckey_list);
  for (auto& wckey : wckey_list) {
    m_wckey_map_[{wckey.name, wckey.user_name}] =
        std::make_unique<Wckey>(wckey);
  }
}

CraneExpected<const User*> AccountManager::GetUserInfoByUidNoLock_(
    uint32_t uid) {
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

/*
 * Get the wckey info form mongodb
 */
const Wckey* AccountManager::GetWckeyInfoNoLock_(const std::string& name,
                                                 const std::string& user_name) {
  auto find_res = m_wckey_map_.find({name, user_name});
  if (find_res == m_wckey_map_.end()) return nullptr;
  return find_res->second.get();
}

/*
 * Get the wckey info form mongodb and deletion flag marked false
 */
const Wckey* AccountManager::GetExistedWckeyInfoNoLock_(
    const std::string& name, const std::string& user_name) {
  const Wckey* wckey = GetWckeyInfoNoLock_(name, user_name);
  if (wckey && !wckey->deleted) return wckey;
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

CraneExpected<void> AccountManager::AddUser_(const std::string& actor_name,
                                             const User& user,
                                             const Account* account,
                                             const User* stale_user) {
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
        AddTxnLogToDB_(actor_name, name, TxnAction::AddUser,
                       res_user.UserToString());
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

CraneExpected<void> AccountManager::AddAccount_(const std::string& actor_name,
                                                const Account& account,
                                                const Account* parent,
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
        AddTxnLogToDB_(actor_name, name, TxnAction::AddAccount,
                       res_account.AccountToString());
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

CraneExpected<void> AccountManager::AddQos_(const std::string& actor_name,
                                            const Qos& qos,
                                            const Qos* stale_qos) {
  // There is a same qos but was deleted,here will delete the original
  // qos and overwrite it with the same name
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        if (stale_qos) {
          g_db_client->UpdateQos(qos);
          g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS, "$set",
                                       qos.name, "creation_time",
                                       ToUnixSeconds(absl::Now()));
        } else {
          g_db_client->InsertQos(qos);
        }
        AddTxnLogToDB_(actor_name, qos.name, TxnAction::AddQos,
                       qos.QosToString());
      };

  if (!g_db_client->CommitTransaction(callback))
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);

  m_qos_map_[qos.name] = std::make_unique<Qos>(qos);

  return {};
}

CraneExpected<void> AccountManager::AddWckey_(const Wckey& wckey,
                                              const Wckey* stale_wckey,
                                              const User* user) {
  Wckey res_wckey;

  if (stale_wckey && !stale_wckey->deleted) {
    res_wckey = *stale_wckey;
  } else {
    res_wckey = wckey;
  }
  res_wckey.is_default = true;

  std::string old_def_wckey;
  bool update_wckey = false;
  if (!m_user_map_[user->name]->default_wckey.empty() &&
      m_user_map_[user->name]->default_wckey != res_wckey.name) {
    old_def_wckey = m_user_map_[user->name]->default_wckey;
    update_wckey = true;
  }

  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        if (stale_wckey) {
          g_db_client->UpdateWckey(res_wckey);
          MongodbClient::FilterFields filter_fields = {
              {"name", res_wckey.name},
              {"user_name", res_wckey.user_name}};
          g_db_client->UpdateEntityOneByFields(
              MongodbClient::EntityType::WCKEY, "$set", filter_fields,
              "creation_time", ToUnixSeconds(absl::Now()));
          if (update_wckey) {
            filter_fields = {{"name", old_def_wckey},
                             {"user_name", res_wckey.user_name}};
            g_db_client->UpdateEntityOneByFields(
                MongodbClient::EntityType::WCKEY, "$set", filter_fields,
                "is_default", false);
          }

          g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$set",
                                       res_wckey.user_name, "default_wckey",
                                       res_wckey.name);
        } else {
          g_db_client->InsertWckey(res_wckey);
          if (update_wckey) {
            MongodbClient::FilterFields filter_fields = {
                {"name", old_def_wckey},
                {"user_name", res_wckey.user_name}};
            g_db_client->UpdateEntityOneByFields(
                MongodbClient::EntityType::WCKEY, "$set", filter_fields,
                "is_default", false);
          }

          g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$set",
                                       res_wckey.user_name, "default_wckey",
                                       res_wckey.name);
        }
      };
  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }
  if (update_wckey &&
      m_wckey_map_.contains({old_def_wckey, res_wckey.user_name})) {
    m_wckey_map_[{old_def_wckey, res_wckey.user_name}]->is_default = false;
  }
  m_wckey_map_[{res_wckey.name, res_wckey.user_name}] =
      std::make_unique<Wckey>(res_wckey);
  m_user_map_[user->name]->default_wckey = res_wckey.name;

  return {};
}

CraneExpected<void> AccountManager::DeleteUser_(const std::string& actor_name,
                                                const User& user,
                                                const std::string& account) {
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

  if (g_config.VaultConf.Enabled) {
    if (res_user.deleted && !res_user.cert_number.empty() &&
        !g_vault_client->RevokeCert(res_user.cert_number))
      return std::unexpected(CraneErrCode::ERR_REVOKE_CERTIFICATE);
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
        AddTxnLogToDB_(
            actor_name, name, TxnAction::DeleteUser,
            fmt::format("remove_accounts: {}, remove_coordinator_accounts: {}, "
                        "deleted: {}",
                        fmt::join(remove_accounts, ", "),
                        fmt::join(remove_coordinator_accounts, ", "),
                        res_user.deleted));
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

  g_account_meta_container->DeleteUserResource(name);

  m_user_map_[name] = std::make_unique<User>(std::move(res_user));

  return {};
}

CraneExpected<void> AccountManager::DeleteAccount_(
    const std::string& actor_name, const Account& account) {
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
        AddTxnLogToDB_(actor_name, name, TxnAction::DeleteAccount, "");
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

CraneExpected<void> AccountManager::DeleteQos_(const std::string& actor_name,
                                               const std::string& name) {
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::QOS, "$set",
                                     name, "deleted", true);
        AddTxnLogToDB_(actor_name, name, TxnAction::DeleteQos, "");
      };

  if (!g_db_client->CommitTransaction(callback))
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);

  m_qos_map_[name]->deleted = true;

  return {};
}

CraneExpected<void> AccountManager::DeleteWckey_(const std::string& name,
                                                 const std::string& user_name) {
  // Update to database
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        MongodbClient::FilterFields filter_fields = {{"name", name},
                                                     {"user_name", user_name}};
        g_db_client->UpdateEntityOneByFields(MongodbClient::EntityType::WCKEY,
                                             "$set", filter_fields, "deleted",
                                             true);
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  if (m_wckey_map_[{name, user_name}])
    m_wckey_map_[{name, user_name}]->deleted = true;

  return {};
}

CraneExpected<void> AccountManager::AddUserAllowedPartition_(
    const std::string& actor_name, const User& user, const Account& account,
    const std::unordered_set<std::string>& partition_list, User& res_user) {
  const std::string& name = user.name;
  const std::string& account_name = account.name;

  // Update the map
  for (const auto& partition : partition_list) {
    res_user.account_to_attrs_map[account_name]
        .allowed_partition_qos_map[partition] =
        std::pair<std::string, std::list<std::string>>{
            account.default_qos,
            std::list<std::string>{account.allowed_qos_list}};
  }

  // Update to database
  bool result = g_db_client->UpdateUser(res_user);
  if (!result) return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  AddTxnLogToDB_(actor_name, name, TxnAction::ModifyUser,
                 fmt::format("Add: account: {}, partition: {}", account_name,
                             fmt::join(partition_list, ",")));

  return {};
}

CraneExpected<void> AccountManager::AddUserAllowedQos_(
    const std::string& actor_name, const User& user, const Account& account,
    const std::string& partition,
    const std::unordered_set<std::string>& qos_list, User& res_user) {
  const std::string& name = user.name;
  const std::string& account_name = account.name;

  for (const auto& qos : qos_list) {
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
  }

  bool result = g_db_client->UpdateUser(res_user);
  if (!result) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }
  AddTxnLogToDB_(
      actor_name, name, TxnAction::ModifyUser,
      fmt::format("Add: account: {}, partition: {}, qos: {}", account_name,
                  partition, fmt::join(qos_list, ",")));

  return {};
}

CraneExpected<void> AccountManager::SetUserAdminLevel_(
    const std::string& actor_name, const std::string& name,
    User::AdminLevel new_level) {
  // Update to database
  bool result = g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER,
                                             "$set", name, "admin_level",
                                             static_cast<int>(new_level));
  if (!result) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }
  AddTxnLogToDB_(
      actor_name, name, TxnAction::ModifyUser,
      fmt::format("admin_level: {}", User::AdminLevelToString(new_level)));

  return {};
}

CraneExpected<void> AccountManager::SetUserDefaultAccount_(
    const std::string& actor_name, const std::string& user,
    const std::string& def_account) {
  // Update to database

  bool result =
      g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$set",
                                   user, "default_account", def_account);
  if (!result) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }
  AddTxnLogToDB_(actor_name, user, TxnAction::ModifyUser,
                 fmt::format("default_account: {}", def_account));

  return {};
}

CraneExpected<void> AccountManager::SetUserDefaultWckey_(
    const std::string& new_def_wckey, const std::string& user_name) {
  WckeyKey new_wckey_key = {.name = new_def_wckey, .user_name = user_name};
  auto it = m_wckey_map_.find(new_wckey_key);
  if (it != m_wckey_map_.end() && it->second && it->second->is_default) {
    return {};
  }

  std::string old_def_wckey;
  bool need_update_table = false;
  auto& user = *(m_user_map_[user_name]);
  if (!user.default_wckey.empty() && user.default_wckey != new_def_wckey) {
    need_update_table = true;
    old_def_wckey = user.default_wckey;
  }

  // Update to database
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::USER, "$set",
                                     user_name, "default_wckey", new_def_wckey);
        // update wckey table
        MongodbClient::FilterFields filter_fields = {{"name", new_def_wckey},
                                                     {"user_name", user_name}};
        g_db_client->UpdateEntityOneByFields(MongodbClient::EntityType::WCKEY,
                                             "$set", filter_fields,
                                             "is_default", true);
        if (need_update_table) {
          filter_fields = {{"name", old_def_wckey}, {"user_name", user_name}};
          g_db_client->UpdateEntityOneByFields(MongodbClient::EntityType::WCKEY,
                                               "$set", filter_fields,
                                               "is_default", false);
        }
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  m_wckey_map_[new_wckey_key]->is_default = true;
  m_user_map_[user_name]->default_wckey = new_def_wckey;
  if (need_update_table) {
    auto old_it = m_wckey_map_.find({old_def_wckey, user_name});
    if (old_it != m_wckey_map_.end() && old_it->second)
      old_it->second->is_default = false;
  }

  return {};
}

CraneExpected<void> AccountManager::SetUserDefaultQos_(
    const std::string& actor_name, const User& user, const std::string& account,
    const std::string& partition, const std::string& qos, User& res_user) {
  const std::string& name = user.name;

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
  bool result = g_db_client->UpdateUser(res_user);
  if (!result) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }
  AddTxnLogToDB_(actor_name, name, TxnAction::ModifyUser,
                 fmt::format("account: {}, partition: {}, default_qos: {}",
                             account, partition, qos));

  return {};
}

CraneExpectedRich<void> AccountManager::SetUserAllowedPartition_(
    const std::string& actor_name, const User& user, const Account& account,
    const std::unordered_set<std::string>& partition_list, User& res_user) {
  const std::string& name = user.name;
  const std::string& account_name = account.name;

  // Update the map
  res_user.account_to_attrs_map[account_name]
      .allowed_partition_qos_map.clear();  // clear the partitions
  for (const auto& par : partition_list) {
    res_user.account_to_attrs_map[account_name].allowed_partition_qos_map[par] =
        std::pair<std::string, std::list<std::string>>{
            account.default_qos,
            std::list<std::string>{account.allowed_qos_list}};
  }

  bool result = g_db_client->UpdateUser(res_user);
  // Update to database
  if (!result)
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_UPDATE_DATABASE, ""));
  AddTxnLogToDB_(actor_name, name, TxnAction::ModifyUser,
                 fmt::format("Set: account: {}, partition_list:[{}]",
                             account_name, fmt::join(partition_list, ",")));

  return {};
}

CraneExpectedRich<void> AccountManager::SetUserAllowedQos_(
    const std::string& actor_name, const User& user, const Account& account,
    const std::string& partition, const std::string& default_qos,
    std::unordered_set<std::string>&& qos_list, User& res_user, bool force) {
  const std::string& name = user.name;
  const std::string& account_name = account.name;

  if (partition.empty()) {
    // Set the qos of all partition
    for (auto& [par, pair] : res_user.account_to_attrs_map[account_name]
                                 .allowed_partition_qos_map) {
      if (!ranges::contains(qos_list, pair.first))
        pair.first = qos_list.empty() ? "" : default_qos;
      pair.second.assign(std::make_move_iterator(qos_list.begin()),
                         std::make_move_iterator(qos_list.end()));
    }
  } else {
    // Set the qos of a specified partition
    auto iter = res_user.account_to_attrs_map[account_name]
                    .allowed_partition_qos_map.find(partition);

    if (!ranges::contains(qos_list, iter->second.first))
      iter->second.first = qos_list.empty() ? "" : default_qos;

    iter->second.second.assign(std::make_move_iterator(qos_list.begin()),
                               std::make_move_iterator(qos_list.end()));
  }

  bool result = g_db_client->UpdateUser(res_user);
  AddTxnLogToDB_(
      actor_name, name, TxnAction::ModifyUser,
      fmt::format("Set: account: {}, partition: {}, qos_list: {}", account_name,
                  partition, fmt::join(qos_list, ",")));

  // Update to database
  if (!result)
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_UPDATE_DATABASE, ""));

  return {};
}

CraneExpected<void> AccountManager::DeleteUserAllowedPartition_(
    const std::string& actor_name, const User& user, const std::string& account,
    const std::string& partition) {
  const std::string& name = user.name;
  bool result = g_db_client->UpdateEntityOne(
      Ctld::MongodbClient::EntityType::USER, "$unset", name,
      "account_to_attrs_map." + account + ".allowed_partition_qos_map." +
          partition,
      std::string(""));

  if (!result) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }
  AddTxnLogToDB_(
      actor_name, name, TxnAction::ModifyUser,
      fmt::format("Del: account: {}, partition: {}", account, partition));
  return {};
}

CraneExpected<void> AccountManager::DeleteUserAllowedQos_(
    const std::string& actor_name, const User& user, const std::string& qos,
    const std::string& account, const std::string& partition, bool force) {
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

  bool result = g_db_client->UpdateUser(res_user);
  if (!result) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }
  AddTxnLogToDB_(actor_name, name, TxnAction::ModifyUser,
                 fmt::format("Del: account: {}, partition: {}, qos: {}",
                             account, partition, qos));

  return {};
}

CraneExpected<void> AccountManager::AddAccountAllowedPartition_(
    const std::string& actor_name, const std::string& name,
    const std::string& partition) {
  // Update to database
  bool result = g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                             "$addToSet", name,
                                             "allowed_partition", partition);

  AddTxnLogToDB_(actor_name, name, TxnAction::ModifyAccount,
                 fmt::format("Add: partition: {}", partition));

  return result ? CraneExpected<void>()
                : std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
}

CraneExpected<void> AccountManager::AddAccountAllowedQos_(
    const std::string& actor_name, const Account& account,
    const std::string& qos) {
  const std::string& name = account.name;

  bool result = true;
  if (account.default_qos.empty()) {
    result = result &&
             g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                          "$set", name, "default_qos", qos);
  }
  result = result && g_db_client->UpdateEntityOne(
                         MongodbClient::EntityType::ACCOUNT, "$addToSet", name,
                         "allowed_qos_list", qos);
  result = result && IncQosReferenceCountInDb_(qos, 1);
  AddTxnLogToDB_(actor_name, name, TxnAction::ModifyAccount,
                 fmt::format("Add: qos: {}", qos));

  return result ? CraneExpected<void>()
                : std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
}

CraneExpected<void> AccountManager::SetAccountDescription_(
    const std::string& actor_name, const std::string& name,
    const std::string& description) {
  bool result =
      g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$set",
                                   name, "description", description);
  AddTxnLogToDB_(actor_name, name, TxnAction::ModifyAccount,
                 fmt::format("description: {}", description));

  return result ? CraneExpected<void>()
                : std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
}

CraneExpected<void> AccountManager::SetAccountDefaultQos_(
    const std::string& actor_name, const Account& account,
    const std::string& qos) {
  const std::string& name = account.name;

  bool result = g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT,
                                             "$set", name, "default_qos", qos);
  AddTxnLogToDB_(actor_name, name, TxnAction::ModifyAccount,
                 fmt::format("default_qos: {}", qos));

  return result ? CraneExpected<void>()
                : std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
}

CraneExpectedRich<void> AccountManager::SetAccountAllowedPartition_(
    const std::string& actor_name, const Account& account,
    std::unordered_set<std::string>& partition_list) {
  const std::string& name = account.name;

  std::list<std::string> deleted_partition;
  for (const auto& par : account.allowed_partition) {
    if (!ranges::contains(partition_list, par))
      deleted_partition.emplace_back(par);
  }

  int add_num = 0;
  for (const auto& par : partition_list) {
    if (!ranges::contains(account.allowed_partition, par)) add_num++;
  }
  bool result = true;
  // DB operation
  for (const auto& par : deleted_partition) {
    result =
        result && DeleteAccountAllowedPartitionFromDBNoLock_(account.name, par);
  }

  if (add_num > 0) {
    result = result && g_db_client->UpdateEntityOne(
                           MongodbClient::EntityType::ACCOUNT, "$set", name,
                           "allowed_partition", partition_list);
  }
  if (!result)
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_UPDATE_DATABASE, ""));

  AddTxnLogToDB_(
      actor_name, name, TxnAction::ModifyAccount,
      fmt::format("Set: partition_list: {}", fmt::join(partition_list, ",")));

  return {};
}

CraneExpectedRich<void> AccountManager::SetAccountAllowedQos_(
    const std::string& actor_name, const Account& account,
    std::unordered_set<std::string>& qos_list, std::list<int>& change_num,
    const std::string& default_qos) {
  const std::string& name = account.name;

  std::list<std::string> deleted_qos;
  for (const auto& qos : account.allowed_qos_list) {
    if (!ranges::contains(qos_list, qos)) deleted_qos.emplace_back(qos);
  }

  std::list<std::string> add_qos;
  for (const auto& qos : qos_list) {
    if (!ranges::contains(account.allowed_qos_list, qos))
      add_qos.emplace_back(qos);
  }

  for (const auto& qos : deleted_qos) {
    int num = DeleteAccountAllowedQosFromDBNoLock_(account.name, qos);
    IncQosReferenceCountInDb_(qos, -num);
    change_num.emplace_back(num);
  }

  bool result = true;

  if (!add_qos.empty()) {
    Account temp;
    result = result && g_db_client->SelectAccount("name", name, &temp);
    if (temp.default_qos.empty()) {
      result = result && g_db_client->UpdateEntityOne(
                             MongodbClient::EntityType::ACCOUNT, "$set", name,
                             "default_qos", default_qos);
    }

    result = result && g_db_client->UpdateEntityOne(
                           MongodbClient::EntityType::ACCOUNT, "$set", name,
                           "allowed_qos_list", qos_list);
    for (const auto& qos : add_qos) {
      result = result && IncQosReferenceCountInDb_(qos, 1);
    }
  }
  if (!result)
    return std::unexpected(
        FormatRichErr(CraneErrCode::ERR_UPDATE_DATABASE, ""));

  AddTxnLogToDB_(actor_name, name, TxnAction::ModifyAccount,
                 fmt::format("Set: qos_list: {}", fmt::join(qos_list, ",")));

  return {};
}

CraneExpected<void> AccountManager::DeleteAccountAllowedPartition_(
    const std::string& actor_name, const Account& account,
    const std::string& partition) {
  bool result =
      DeleteAccountAllowedPartitionFromDBNoLock_(account.name, partition);
  AddTxnLogToDB_(actor_name, account.name, TxnAction::ModifyAccount,
                 fmt::format("Del: partition: {}", partition));

  return result ? CraneExpected<void>()
                : std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
}

CraneExpected<int> AccountManager::DeleteAccountAllowedQos_(
    const std::string& actor_name, const Account& account,
    const std::string& qos) {
  int change_num;
  change_num = DeleteAccountAllowedQosFromDBNoLock_(account.name, qos);
  bool result = IncQosReferenceCountInDb_(qos, -change_num);
  if (!result) return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  AddTxnLogToDB_(actor_name, account.name, TxnAction::ModifyAccount,
                 fmt::format("Del: qos: {}", qos));

  return change_num;
}

CraneExpected<void> AccountManager::BlockUserNoLock_(
    const std::string& actor_name, const std::string& name,
    const std::string& account, bool block) {
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateEntityOne(
            MongodbClient::EntityType::USER, "$set", name,
            "account_to_attrs_map." + account + ".blocked", block);
        AddTxnLogToDB_(actor_name, name, TxnAction::ModifyUser,
                       fmt::format("Set: account_to_attrs_map.{}.blocked: {}",
                                   account, block));
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  m_user_map_[name]->account_to_attrs_map[account].blocked = block;

  return {};
}

CraneExpected<void> AccountManager::BlockAccountNoLock_(
    const std::string& actor_name, const std::string& name, bool block) {
  mongocxx::client_session::with_transaction_cb callback =
      [&](mongocxx::client_session* session) {
        g_db_client->UpdateEntityOne(MongodbClient::EntityType::ACCOUNT, "$set",
                                     name, "blocked", block);
        AddTxnLogToDB_(actor_name, name, TxnAction::ModifyAccount,
                       fmt::format("blocked: {}", block));
      };

  if (!g_db_client->CommitTransaction(callback)) {
    return std::unexpected(CraneErrCode::ERR_UPDATE_DATABASE);
  }

  m_account_map_[name]->blocked = block;

  return {};
}

/**
 * @note need read lock(m_rw_user_mutex_ && m_rw_account_mutex_)
 */
bool AccountManager::IsAllowedPartitionOfAnyNodeNoLock_(
    const Account* account, const std::string& partition, int depth) {
  if (depth > 0 && ranges::contains(account->allowed_partition, partition))
    return true;

  for (const auto& child : account->child_accounts) {
    if (IsAllowedPartitionOfAnyNodeNoLock_(GetExistedAccountInfoNoLock_(child),
                                           partition, depth + 1)) {
      return true;
    }
  }

  for (const auto& user : account->users) {
    const User* p = GetExistedUserInfoNoLock_(user);
    for (const auto& item : p->account_to_attrs_map) {
      if (item.second.allowed_partition_qos_map.contains(partition))
        return true;
    }
  }
  return false;
}

/**
 * @note need read lock(m_rw_user_mutex_ && m_rw_account_mutex_)
 */
bool AccountManager::IsDefaultQosOfAnyNodeNoLock_(const Account* account,
                                                  const std::string& qos) {
  CRANE_ASSERT(account != nullptr);
  if (account->default_qos == qos) return true;

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
  CRANE_ASSERT(user != nullptr);

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
 * @note need write lock(m_rw_account_mutex_) and write *
 * lock(m_rw_user_mutex_)
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

void AccountManager::AddTxnLogToDB_(const std::string& actor_name,
                                    const std::string& target, TxnAction action,
                                    const std::string& info) {
  Txn txn;
  txn.creation_time = ToUnixSeconds(absl::Now());
  txn.actor = actor_name;
  txn.target = target;
  txn.action = action;
  txn.info = info;
  g_db_client->InsertTxn(txn);
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
