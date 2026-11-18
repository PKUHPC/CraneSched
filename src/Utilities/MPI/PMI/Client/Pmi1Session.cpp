/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 */

#include "Pmi1Session.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>

#include "Utilities.h"

namespace crane::pmi1 {
namespace {
using Codec = ::pmi::pmi1::Codec;
using pmi::ParseInt;

int CopyString(std::string_view value, char* output, int length) {
  if (output == nullptr || length <= 0) return PMI_ERR_INVALID_ARG;
  if (value.size() >= static_cast<size_t>(length))
    return PMI_ERR_INVALID_LENGTH;
  std::memcpy(output, value.data(), value.size());
  output[value.size()] = '\0';
  return PMI_SUCCESS;
}
}  // namespace

Session& GetSession() {
  static Session session;
  return session;
}

bool Session::Active_() const {
  return m_state_ == State::kSingleton || m_state_ == State::kConnected;
}

int Session::Exchange_(std::string_view command, const Fields& fields,
                       std::string_view expected, Fields* response) {
  auto request = Codec::EncodeLine(command, fields);
  if (!request) return PMI_ERR_INVALID_ARG;
  if (!m_transport_->SendLine(*request)) return PMI_FAIL;
  auto line = m_transport_->ReceiveLine(Codec::kMaxLine);
  auto reply = line ? Codec::ParseLine(*line) : std::nullopt;
  if (!reply) return PMI_FAIL;
  auto rc = reply->find("rc");
  auto status = rc == reply->end() ? std::nullopt : ParseInt(rc->second);
  if (!status) return PMI_FAIL;
  if (*status != PMI_SUCCESS) return *status;
  if (reply->at("cmd") != expected) return PMI_FAIL;
  if (response) *response = std::move(*reply);
  return PMI_SUCCESS;
}

int Session::Initialize(int* spawned) {
  if (spawned == nullptr) return PMI_ERR_INVALID_ARG;
  std::lock_guard lock(m_mutex_);
  if (m_state_ == State::kConnected || m_state_ == State::kFinalized)
    return PMI_ERR_INIT;
  const char* fd_text = std::getenv("PMI_FD");
  if (fd_text == nullptr) {
    m_local_ranks_ = {0};
    m_state_ = State::kSingleton;
  } else {
    auto fd = ParseInt(fd_text);
    const char* rank = std::getenv("PMI_RANK");
    const char* size = std::getenv("PMI_SIZE");
    auto rank_value = rank ? ParseInt(rank) : std::nullopt;
    auto size_value = size ? ParseInt(size) : std::nullopt;
    if (!fd || *fd < 0 || !rank_value || !size_value || *rank_value < 0 ||
        *size_value <= *rank_value)
      return PMI_ERR_INVALID_ARG;
    m_transport_ =
        std::make_unique<pmi::Transport>(*fd, std::chrono::seconds(60));
    Fields reply;
    int result =
        Exchange_("init", {{"pmi_version", "1"}, {"pmi_subversion", "1"}},
                  "response_to_init", &reply);
    if (result != PMI_SUCCESS || reply["pmi_version"] != "1" ||
        reply["pmi_subversion"] != "1") {
      m_transport_.reset();
      return result == PMI_SUCCESS ? PMI_FAIL : result;
    }
    result = Exchange_("get_appnum", {}, "appnum", &reply);
    auto appnum =
        result == PMI_SUCCESS ? ParseInt(reply["appnum"]) : std::nullopt;
    if (!appnum) {
      m_transport_.reset();
      return result == PMI_SUCCESS ? PMI_FAIL : result;
    }
    m_rank_ = *rank_value;
    m_size_ = *size_value;
    m_appnum_ = *appnum;
    if (const char* jobid = std::getenv("PMI_JOBID")) m_jobid_ = jobid;
    m_local_ranks_.clear();
    // Slurm's public clique extensions describe actual local membership,
    // not a contiguous rank interval. Crane already supplies this task list.
    if (const char* text = std::getenv("SLURM_GTIDS")) {
      std::string_view remaining(text);
      while (!remaining.empty()) {
        auto end = remaining.find(',');
        auto local = ParseInt(remaining.substr(0, end));
        if (!local || *local < 0 || *local >= m_size_) {
          m_local_ranks_.clear();
          break;
        }
        m_local_ranks_.push_back(*local);
        if (end == std::string_view::npos) break;
        remaining.remove_prefix(end + 1);
      }
    }
    m_state_ = State::kConnected;
  }
  *spawned = PMI_FALSE;
  return PMI_SUCCESS;
}

int Session::Initialized(int* initialized) const {
  if (initialized == nullptr) return PMI_ERR_INVALID_ARG;
  std::lock_guard lock(m_mutex_);
  *initialized = Active_();
  return PMI_SUCCESS;
}

int Session::Finalize() {
  std::lock_guard lock(m_mutex_);
  if (!Active_()) return PMI_ERR_INIT;
  int result = PMI_SUCCESS;
  if (m_state_ == State::kConnected)
    result = Exchange_("finalize", {}, "finalize_ack");
  m_transport_.reset();
  m_state_ = State::kFinalized;
  return result;
}

int Session::Abort(int exit_code, const char* message) {
  {
    std::lock_guard lock(m_mutex_);
    if (!Active_()) return PMI_ERR_INIT;
    if (m_transport_) {
      auto line =
          Codec::EncodeLine("abort", {{"exitcode", std::to_string(exit_code)},
                                      {"error_msg", message ? message : ""}});
      if (line) (void)m_transport_->SendLine(*line);
      m_transport_.reset();
    }
    m_state_ = State::kFinalized;
  }
  // Application exit callbacks may reenter PMI, so release the lock first.
  std::exit(exit_code > 0 && exit_code <= 255 ? exit_code : EXIT_FAILURE);
}

int Session::GetInteger(Integer attribute, int* value) const {
  if (value == nullptr) return PMI_ERR_INVALID_ARG;
  std::lock_guard lock(m_mutex_);
  if (!Active_()) return PMI_ERR_INIT;
  switch (attribute) {
  case Integer::kRank:
    *value = m_rank_;
    break;
  case Integer::kSize:
  case Integer::kUniverse:
    *value = m_size_;
    break;
  case Integer::kAppnum:
    *value = m_appnum_;
    break;
  case Integer::kCliqueSize:
    if (m_local_ranks_.empty()) return PMI_FAIL;
    *value = static_cast<int>(m_local_ranks_.size());
    break;
  case Integer::kNameLength:
    *value = 256;
    break;
  case Integer::kKeyLength:
    *value = Codec::kMaxKey;
    break;
  case Integer::kValueLength:
    *value = Codec::kMaxValue;
    break;
  }
  return PMI_SUCCESS;
}

int Session::CliqueRanks(int* ranks, int length) const {
  if (ranks == nullptr || length < 0) return PMI_ERR_INVALID_ARG;
  std::lock_guard lock(m_mutex_);
  if (!Active_()) return PMI_ERR_INIT;
  if (m_local_ranks_.empty()) return PMI_FAIL;
  if (static_cast<size_t>(length) < m_local_ranks_.size())
    return PMI_ERR_INVALID_LENGTH;
  std::copy(m_local_ranks_.begin(), m_local_ranks_.end(), ranks);
  return PMI_SUCCESS;
}

int Session::KvsName(char* name, int length) const {
  std::lock_guard lock(m_mutex_);
  if (!Active_()) return PMI_ERR_INIT;
  return CopyString(m_jobid_, name, length);
}

int Session::CheckKvs_(const char* name) const {
  if (!Active_()) return PMI_ERR_INIT;
  return name != nullptr && m_jobid_ == name ? PMI_SUCCESS
                                             : PMI_ERR_INVALID_KVS;
}

int Session::Commit(const char* name) const {
  std::lock_guard lock(m_mutex_);
  // Put has already submitted data to the server; Barrier makes it visible.
  return CheckKvs_(name);
}

int Session::Put(const char* name, const char* key, const char* value) {
  if (key == nullptr || value == nullptr) return PMI_ERR_INVALID_ARG;
  if (*key == '\0' || std::strlen(key) >= Codec::kMaxKey)
    return PMI_ERR_INVALID_KEY_LENGTH;
  if (std::strlen(value) >= Codec::kMaxValue) return PMI_ERR_INVALID_VAL_LENGTH;
  std::lock_guard lock(m_mutex_);
  int result = CheckKvs_(name);
  if (result != PMI_SUCCESS) return result;
  if (m_state_ == State::kSingleton) {
    m_singleton_kvs_[key] = value;
    return PMI_SUCCESS;
  }
  return Exchange_("put", {{"kvsname", name}, {"key", key}, {"value", value}},
                   "put_result");
}

int Session::Get(const char* name, const char* key, char* value, int length) {
  if (key == nullptr || value == nullptr || length <= 0)
    return PMI_ERR_INVALID_ARG;
  std::lock_guard lock(m_mutex_);
  int result = CheckKvs_(name);
  if (result != PMI_SUCCESS) return result;
  Fields reply;
  if (m_state_ == State::kSingleton) {
    auto found = m_singleton_kvs_.find(key);
    return found == m_singleton_kvs_.end()
               ? PMI_ERR_INVALID_KEY
               : CopyString(found->second, value, length);
  }
  result =
      Exchange_("get", {{"kvsname", name}, {"key", key}}, "get_result", &reply);
  if (result != PMI_SUCCESS) return result;
  auto found = reply.find("value");
  return found == reply.end() ? PMI_FAIL
                              : CopyString(found->second, value, length);
}

int Session::Barrier() {
  std::lock_guard lock(m_mutex_);
  if (!Active_()) return PMI_ERR_INIT;
  return m_state_ == State::kSingleton
             ? PMI_SUCCESS
             : Exchange_("barrier_in", {}, "barrier_out");
}
}  // namespace crane::pmi1
