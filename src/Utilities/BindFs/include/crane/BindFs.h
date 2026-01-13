/**
 * Copyright (c) 2025 Peking University and Peking University
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

#include <sys/types.h>

#include <expected>
#include <filesystem>

#include "crane/PasswordEntry.h"

namespace bindfs {

constexpr static std::string_view kBindFsBin = "/usr/bin/bindfs";
constexpr static std::string_view kFusermountBin = "/usr/bin/fusermount3";
constexpr static std::string_view kBindFsMountBaseDir = "/mnt/crane";

struct BindFsMetadata;

class IdMappedBindFs {
 public:
  IdMappedBindFs(std::filesystem::path source, const PasswordEntry& pwd,
                 uid_t kuid, gid_t kgid, uid_t uid_offset, gid_t gid_offset,
                 std::filesystem::path bindfs_bin = kBindFsBin,
                 std::filesystem::path fusermount_bin = kFusermountBin,
                 std::filesystem::path mount_base_dir = kBindFsMountBaseDir);

  IdMappedBindFs(const IdMappedBindFs&) = delete;
  IdMappedBindFs& operator=(const IdMappedBindFs&) = delete;

  IdMappedBindFs(IdMappedBindFs&&) = default;
  IdMappedBindFs& operator=(IdMappedBindFs&&) = default;

  ~IdMappedBindFs();

  std::filesystem::path GetMountedPath() { return m_target_; }
  bool ValidateMetadata(const BindFsMetadata& metadata) const;

 private:
  static bool CheckMountValid_(const std::filesystem::path& mount_path);

  std::expected<int, std::string> Mount_() noexcept;
  std::expected<int, std::string> Unmount_() noexcept;

  [[nodiscard]] std::string GetHashedMountPoint_() noexcept;
  [[nodiscard]] std::expected<void, std::string> CreateMountPoint_() noexcept;
  [[nodiscard]] std::expected<void, std::string> ReleaseMountPoint_() noexcept;

  uid_t m_kuid_;
  gid_t m_kgid_;
  uid_t m_uid_offset_;
  gid_t m_gid_offset_;

  std::string m_user_;
  std::string m_group_;

  std::filesystem::path m_bindfs_bin_;
  std::filesystem::path m_fusermount_bin_;
  std::filesystem::path m_mount_base_dir_;

  std::filesystem::path m_source_;
  std::filesystem::path m_target_;
  std::filesystem::path m_target_lock_;
};

// Metadata stored in lock file
struct BindFsMetadata {
  std::filesystem::path source;
  uint32_t uid_offset{0};
  uint32_t gid_offset{0};
  std::string user;
  std::string group;
  uint32_t counter{0};

  [[nodiscard]] static std::expected<BindFsMetadata, std::string> Unmarshal(
      const std::string& data) noexcept;

  std::string Marshal() const;
};

}  // namespace bindfs
