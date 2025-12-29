#include "crane/BindFs.h"

#include <grp.h>
#include <subprocess/subprocess.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <unistd.h>

#include <expected>
#include <filesystem>
#include <format>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include "crane/FileLock.h"
#include "crane/Logger.h"
#include "crane/String.h"
#include "linux/magic.h"

// Serialization for BindFsMetadata
std::string BindFsMetadata::Marshal() const {
  nlohmann::json json;
  json["source"] = source.string();
  json["uid_offset"] = uid_offset;
  json["gid_offset"] = gid_offset;
  json["user"] = user;
  json["group"] = group;
  json["counter"] = counter;
  return json.dump();
}

std::expected<BindFsMetadata, std::string> BindFsMetadata::Unmarshal(
    const std::string& data) noexcept {
  try {
    nlohmann::json json = nlohmann::json::parse(data);

    BindFsMetadata metadata;
    metadata.source = json.at("source").get<std::string>();
    metadata.uid_offset = json.at("uid_offset").get<uint32_t>();
    metadata.gid_offset = json.at("gid_offset").get<uint32_t>();
    metadata.user = json.at("user").get<std::string>();
    metadata.group = json.at("group").get<std::string>();
    metadata.counter = json.at("counter").get<uint32_t>();

    return metadata;
  } catch (const nlohmann::json::parse_error& e) {
    return std::unexpected(std::format("JSON parse error: {}", e.what()));
  } catch (const nlohmann::json::out_of_range& e) {
    return std::unexpected(std::format("Missing required field: {}", e.what()));
  } catch (const nlohmann::json::type_error& e) {
    return std::unexpected(std::format("Type mismatch: {}", e.what()));
  } catch (const std::exception& e) {
    return std::unexpected(std::format("Unmarshal failed: {}", e.what()));
  }
}

bool IdMappedBindFs::CheckMountValid_(const std::filesystem::path& mount_path) {
  namespace fs = std::filesystem;
  std::error_code ec;
  bool exists = fs::exists(mount_path, ec);
  if (ec || !exists) {
    return false;
  }
  auto realpath = fs::canonical(mount_path, ec);
  if (ec) {
    return false;
  }

  struct statfs sfs{};
  if (statfs(realpath.c_str(), &sfs) != 0) {
    perror("statfs");
    return false;
  }

  return (sfs.f_type == FUSE_SUPER_MAGIC);
}

std::string IdMappedBindFs::GetHashedMountPoint_() noexcept {
  const auto& subuid_range = m_subuids_.At(0);
  const auto& subgid_range = m_subgids_.At(0);

  std::string p = std::format("u{}-{}\x1fg{}-{}\x1fs{}", subuid_range.start,
                              subuid_range.count, subgid_range.start,
                              subgid_range.count, m_source_.string());

  uint32_t a = util::Adler32Of(p);
  uint32_t c = util::Crc32Of(p);
  uint64_t h = (static_cast<uint64_t>(a) << 32) | static_cast<uint64_t>(c);

  return std::format("{:016x}", h);  // 16 hex chars, lowercase
}

int IdMappedBindFs::Mount_() noexcept {
  /*
 bindfs \
   --uid-offset=523288 \
   --gid-offset=523288 \
   --create-for-user=leo \
   --create-for-group=leo \
   -o allow_other \
   /home/leo/test /mnt/test
 */
  std::string uid_offset =
      std::format("--uid-offset={}", m_subuids_.At(0).start);
  std::string gid_offset =
      std::format("--gid-offset={}", m_subgids_.At(0).start);
  std::string user_opt =
      std::format("--create-for-user={}",
                  m_user_.empty() ? std::to_string(m_uid_) : m_user_);
  std::string group_opt =
      std::format("--create-for-group={}",
                  m_group_.empty() ? std::to_string(m_gid_) : m_group_);

  // clang-format off
  std::vector<const char*> args{
    m_bindfs_bin_.c_str(),
    uid_offset.c_str(),
    gid_offset.c_str(),
    user_opt.c_str(),
    group_opt.c_str(),
    "-o", "allow_other",
    m_source_.c_str(),  m_target_.c_str(),
    nullptr,
  };
  // clang-format on

  subprocess_s subprocess{};
  int result = subprocess_create(args.data(), 0, &subprocess);
  if (0 != result) return result;

  subprocess_join(&subprocess, &result);
  return result;
}

int IdMappedBindFs::Unmount_() noexcept {
  std::vector<const char*> args{m_fusermount_bin_.c_str(), "-u",
                                m_target_.c_str(), nullptr};

  subprocess_s subprocess{};
  int result = subprocess_create(args.data(), 0, &subprocess);
  if (result != 0) return result;

  subprocess_join(&subprocess, &result);
  return result;
}

std::expected<void, std::string> IdMappedBindFs::CreateMountPoint_() noexcept {
  namespace fs = std::filesystem;

  std::error_code ec;
  bool lock_exists = fs::exists(m_target_lock_, ec);
  if (ec) {
    return std::unexpected(std::format("Failed to stat lock file {}: {}",
                                       m_target_lock_.string(), ec.message()));
  }

  if (!lock_exists) {
    // Create parent directories if not exist
    // e.g., /mnt/crane (root, 711)
    //           - <uid> (user, 711)
    //             - <hash> (mount point)
    fs::create_directories(m_target_, ec);
    if (ec) {
      return std::unexpected(std::format("Failed to create directories {}: {}",
                                         m_target_.string(), ec.message()));
    }

    fs::path parent_path = m_target_.parent_path();  // <uid>
    fs::permissions(
        parent_path,
        fs::perms::owner_all | fs::perms::group_exec | fs::perms::others_exec,
        ec);
    if (ec) {
      return std::unexpected(std::format("Failed to chmod directory {}: {}",
                                         parent_path.string(), ec.message()));
    }

    if (chown(parent_path.c_str(), m_uid_, m_gid_) != 0) {
      return std::unexpected(std::format("Failed to chown directory {}: {}",
                                         parent_path.string(),
                                         strerror(errno)));
    }

    parent_path = parent_path.parent_path();  // /mnt/crane
    fs::permissions(
        parent_path,
        fs::perms::owner_all | fs::perms::group_exec | fs::perms::others_exec,
        ec);
    if (ec) {
      return std::unexpected(std::format("Failed to chmod directory {}: {}",
                                         parent_path.string(), ec.message()));
    }
    if (chown(parent_path.c_str(), 0, 0) != 0) {
      return std::unexpected(std::format("Failed to chown directory {}: {}",
                                         parent_path.string(),
                                         strerror(errno)));
    }
  }

  // Try to obtain the file lock
  auto lk =
      util::FileLockGuard::Acquire(m_target_lock_, util::FileLockType::WRITE);
  if (!lk)
    return std::unexpected(std::format("Failed to acquire lock for {}: {}",
                                       m_target_lock_.string(), lk.error()));

  BindFsMetadata metadata;
  auto lk_meta_expt = lk->ReadMetadata();
  if (!lk_meta_expt) return std::unexpected(lk_meta_expt.error());

  if (lk_meta_expt->empty()) {
    metadata.source = m_source_;
    metadata.uid_offset = m_subuids_.At(0).start;
    metadata.gid_offset = m_subgids_.At(0).start;
    metadata.user = m_user_;
    metadata.group = m_group_;
  } else {
    auto meta_expt = BindFsMetadata::Unmarshal(lk_meta_expt.value());
    if (!meta_expt) return std::unexpected(meta_expt.error());
    metadata = std::move(meta_expt.value());
  }

  bool valid = CheckMountValid_(m_target_);

  if (metadata.counter > 0) {
    // Check if the mount is still valid
    if (!valid)
      return std::unexpected(
          std::format("Bindfs mount {} is corrupted.", m_target_.string()));

    // Already mounted, just increment counter
    metadata.counter += 1;
    auto write_result = lk->WriteMetadata(metadata.Marshal());
    if (!write_result) {
      return std::unexpected(write_result.error());
    }
    return {};
  }

  // If the counter is 0 but mount is valid (due to unmount failure),
  // treat it as valid to avoid remounting over existing mount
  if (!valid) {
    if (int rc = Mount_(); rc) {
      return std::unexpected(std::format("Failed to mount bindfs {} (rc: {})",
                                         m_target_.string(), rc));
    }
  }

  // Update the mount counter
  metadata.counter += 1;

  // Write back the updated metadata
  auto write_result = lk->WriteMetadata(metadata.Marshal());
  if (!write_result) {
    Unmount_();  // Try best to unmount when lock failed
    return std::unexpected(std::format("Failed to update bindfs metadata: {}",
                                       write_result.error()));
  }

  return {};
}

std::expected<void, std::string> IdMappedBindFs::ReleaseMountPoint_() noexcept {
  namespace fs = std::filesystem;

  // Acquire the lock to modify counter
  auto lk =
      util::FileLockGuard::Acquire(m_target_lock_, util::FileLockType::WRITE);
  if (!lk) {
    return std::unexpected(std::format("Failed to acquire lock for {}: {}",
                                       m_target_lock_.string(), lk.error()));
  }

  // Read current metadata
  auto lk_meta_expt = lk->ReadMetadata();
  if (!lk_meta_expt) {
    return std::unexpected(
        std::format("Failed to read metadata: {}", lk_meta_expt.error()));
  }

  auto meta_expt = BindFsMetadata::Unmarshal(lk_meta_expt.value());
  if (!meta_expt) {
    return std::unexpected(
        std::format("Failed to unmarshal metadata: {}", meta_expt.error()));
  }

  BindFsMetadata metadata = std::move(meta_expt.value());

  if (metadata.counter == 0) {
    CRANE_WARN("Counter already 0 for {}", m_target_lock_.string());
    return {};
  }

  // Decrement counter
  metadata.counter--;

  if (metadata.counter > 0) {
    // Write back the updated metadata (counter > 0, keep lock file)
    auto write_result = lk->WriteMetadata(metadata.Marshal());
    if (!write_result) {
      return std::unexpected(
          std::format("Failed to write metadata: {}", write_result.error()));
    }

    return {};
  }

  // Last reference, unmount the bindfs
  CRANE_TRACE("Unmounting bindfs {} (counter reached 0)", m_target_.string());
  int rc = Unmount_();
  if (rc != 0) {
    CRANE_ERROR("Failed to unmount bindfs {} (rc: {})", m_target_.string(), rc);
    // Continue cleanup even if unmount failed
  }

  // Remove the mount directory if empty (only if unmount succeeded)
  if (rc == 0) {
    std::error_code ec;
    bool exists = fs::exists(m_target_, ec);
    if (ec) {
      CRANE_WARN("Failed to stat mount directory {}: {}", m_target_.string(),
                 ec.message());
    } else if (exists) {
      bool empty = fs::is_empty(m_target_, ec);
      if (ec) {
        CRANE_WARN("Failed to check mount directory {}: {}", m_target_.string(),
                   ec.message());
      } else if (empty) {
        fs::remove(m_target_, ec);
        if (ec) {
          CRANE_WARN("Failed to remove mount directory {}: {}",
                     m_target_.string(), ec.message());
        }
      }
    }
  }

  // Write metadata with counter=0, then release lock and remove lock file
  // Do this even if unmount failed, because counter reflects process count
  auto write_result = lk->WriteMetadata(metadata.Marshal());
  if (!write_result) {
    return std::unexpected(
        std::format("Failed to write metadata: {}", write_result.error()));
  }

  // Explicitly release the lock before removing the lock file
  lk->Release();

  // Remove the lock file as counter reached 0 (no active processes)
  std::error_code ec;
  fs::remove(m_target_lock_, ec);
  if (ec) {
    CRANE_WARN("Failed to remove lock file {}: {}", m_target_lock_.string(),
               ec.message());
  }

  // Return error if unmount failed (after cleanup)
  if (rc != 0) {
    return std::unexpected("Failed to unmount. Mount point may still open.");
  }

  return {};
}

IdMappedBindFs::IdMappedBindFs(std::filesystem::path source,
                               const PasswordEntry& pwd, uid_t uid, gid_t gid,
                               std::filesystem::path bindfs_bin,
                               std::filesystem::path fusermount_bin)
    : m_uid_(uid),
      m_gid_(gid),
      m_user_(pwd.Username()),
      m_subuids_(pwd.SubUidRanges()),
      m_subgids_(pwd.SubGidRanges()),
      m_bindfs_bin_(std::move(bindfs_bin)),
      m_fusermount_bin_(std::move(fusermount_bin)),
      m_source_(std::move(source)) {
  // bindfs only support directory.
  if (!std::filesystem::exists(m_source_) ||
      !std::filesystem::is_directory(m_source_)) {
    throw std::runtime_error(
        "Source path does not exist or is not a directory");
  }
  m_source_ = std::filesystem::canonical(m_source_);

  if (!pwd.Valid()) {
    throw std::runtime_error("Invalid PasswordEntry");
  }

  if (!m_subuids_.Valid() || !m_subgids_.Valid()) {
    throw std::runtime_error("Invalid subuid/subgid ranges");
  }

  if (m_subuids_.Count() != 1 || m_subgids_.Count() != 1) {
    throw std::runtime_error(
        "Currently only support one continuous subuid/subgid range");
  }

  struct group* grp = getgrgid(gid);
  if (grp != nullptr) {
    m_group_ = std::string(grp->gr_name);
  } else {
    throw std::runtime_error(
        std::format("Failed to get group name for gid: {}", gid));
  }

  // e.g., /mnt/crane/1000/9f8b7c6d5e4f3a2b
  m_target_ = std::filesystem::path(kMountPrefix) / std::to_string(m_uid_) /
              GetHashedMountPoint_();
  m_target_lock_ = std::filesystem::path(m_target_.string() + ".lock");

  auto result = CreateMountPoint_();
  if (!result) {
    CRANE_ERROR("Failed to create bindfs {} for {}", m_target_, m_source_,
                result.error());
    throw std::runtime_error(result.error());
  }

  CRANE_INFO("Bindfs {} created for {}", m_target_, m_source_);
};

IdMappedBindFs::~IdMappedBindFs() {
  auto result = ReleaseMountPoint_();
  if (!result) {
    CRANE_ERROR("Failed to release bindfs {}: {}", m_target_, result.error());
  } else {
    CRANE_INFO("Bindfs {} released", m_target_);
  }
}
