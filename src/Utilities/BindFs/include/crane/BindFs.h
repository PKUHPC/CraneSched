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

class IdMappedBindFs {
 public:
  IdMappedBindFs(std::filesystem::path source, const PasswordEntry& pwd,
                 uid_t uid, gid_t gid,
                 std::filesystem::path bindfs_bin = "bindfs",
                 std::filesystem::path fusermount_bin = "fusermount");

  IdMappedBindFs(const IdMappedBindFs&) = delete;
  IdMappedBindFs& operator=(const IdMappedBindFs&) = delete;

  IdMappedBindFs(IdMappedBindFs&&) = default;
  IdMappedBindFs& operator=(IdMappedBindFs&&) = default;

  ~IdMappedBindFs();

  std::filesystem::path GetMountedPath() { return m_target_; }

 private:
  constexpr static std::string_view kMountPrefix = "/mnt/crane";
  static bool CheckMountValid_(const std::filesystem::path& mount_path);

  int Mount_() noexcept;
  int Unmount_() noexcept;

  [[nodiscard]] std::string GetHashedMountPoint_() noexcept;
  [[nodiscard]] std::expected<void, std::string> CreateMountPoint_() noexcept;
  [[nodiscard]] std::expected<void, std::string> ReleaseMountPoint_() noexcept;

  uid_t m_uid_;
  gid_t m_gid_;
  std::string m_user_;
  std::string m_group_;

  SubIdRanges m_subuids_;
  SubIdRanges m_subgids_;

  std::filesystem::path m_bindfs_bin_;
  std::filesystem::path m_fusermount_bin_;

  std::filesystem::path m_source_;
  std::filesystem::path m_target_;
  std::filesystem::path m_target_lock_;
};
