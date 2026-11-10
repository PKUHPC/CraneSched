/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 */

#include "Pmi2Session.h"

#include <sys/socket.h>

#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <cstring>
#include <string_view>

#include "Pmi2Transport.h"

namespace crane::pmi2 {
namespace {

std::optional<int> ParseInt(std::string_view value) {
  int result = 0;
  auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size())
    return std::nullopt;
  return result;
}

bool IsTrue(std::string_view value) {
  return value == "TRUE" || value == "true" || value == "1";
}

int ServerError(int rc) {
  if (rc == 0) return PMI2_SUCCESS;
  if (rc == PMI2_ERR_INVALID_ARG) return PMI2_ERR_INVALID_ARG;
  return PMI2_ERR_OTHER;
}

}  // namespace

Session& GetSession() {
  static Session session;
  return session;
}

int Session::CommandLocked(std::string_view command, const Fields& fields,
                           std::string_view expected, Fields* response) {
  if (state_ != State::kConnected || transport_ == nullptr)
    return PMI2_ERR_INIT;
  Fields request = fields;
  request["cmd"] = std::string(command);
  auto body = Codec::EncodeFrame(request);
  if (!body || !transport_->SendFrame(*body)) return PMI2_ERR_OTHER;
  auto response_body = transport_->ReceiveFrame();
  if (!response_body) return PMI2_ERR_OTHER;
  auto parsed = Codec::ParseFrame(*response_body);
  if (!parsed) return PMI2_ERR_OTHER;
  auto command_it = parsed->find("cmd");
  if (command_it == parsed->end() || command_it->second != expected)
    return PMI2_ERR_OTHER;
  int rc = 0;
  if (auto it = parsed->find("rc"); it != parsed->end()) {
    auto parsed_rc = ParseInt(it->second);
    if (!parsed_rc) return PMI2_ERR_OTHER;
    rc = *parsed_rc;
  }
  if (response != nullptr) *response = std::move(*parsed);
  return ServerError(rc);
}

int Session::Initialize(int* spawned, int* size, int* rank, int* appnum) {
  if (spawned == nullptr || size == nullptr || rank == nullptr ||
      appnum == nullptr)
    return PMI2_ERR_INVALID_ARG;
  std::lock_guard lock(mutex_);
  if (state_ != State::kUninitialized) return PMI2_SUCCESS;

  const char* fd_text = std::getenv("PMI_FD");
  if (fd_text == nullptr) {
    state_ = State::kSingleton;
    *spawned = 0;
    *size = size_ = 1;
    *rank = rank_ = 0;
    *appnum = appnum_ = -1;
    if (const char* jobid = std::getenv("PMI_JOBID")) jobid_ = jobid;
    return PMI2_SUCCESS;
  }
  const auto fd = ParseInt(fd_text);
  if (!fd || *fd < 0) return PMI2_ERR_INVALID_ARG;

  transport_ = std::make_unique<Transport>(*fd, std::chrono::seconds(60));
  auto fail = [&] {
    transport_.reset();
    state_ = State::kUninitialized;
    return PMI2_ERR_OTHER;
  };

  Fields init{{"cmd", "init"}, {"pmi_version", "2"}, {"pmi_subversion", "0"}};
  if (!transport_->SendInit(Codec::EncodeInit(init))) return fail();
  auto init_response = transport_->ReceiveInit();
  if (!init_response) return fail();
  auto init_fields = Codec::ParseInit(*init_response);
  if (!init_fields || init_fields->at("cmd") != "response_to_init" ||
      !init_fields->contains("rc") || init_fields->at("rc") != "0")
    return fail();

  int environment_rank = 0;
  if (const char* rank_text = std::getenv("PMI_RANK")) {
    auto parsed_rank = ParseInt(rank_text);
    if (!parsed_rank || *parsed_rank < 0) return fail();
    environment_rank = *parsed_rank;
  }
  Fields fullinit{{"cmd", "fullinit"},
                  {"pmirank", std::to_string(environment_rank)},
                  {"threaded", "FALSE"}};
  if (const char* pmi_id = std::getenv("PMI_ID"))
    fullinit.emplace("srcid", pmi_id);
  if (const char* pmi_jobid = std::getenv("PMI_JOBID")) {
    jobid_ = pmi_jobid;
    fullinit.emplace("pmijobid", pmi_jobid);
  }
  auto body = Codec::EncodeFrame(fullinit);
  if (!body || !transport_->SendFrame(*body)) return fail();
  auto full_response = transport_->ReceiveFrame();
  if (!full_response) return fail();
  auto fields = Codec::ParseFrame(*full_response);
  if (!fields) return fail();
  auto command_it = fields->find("cmd");
  if (command_it == fields->end() || command_it->second != "fullinit-response")
    return fail();
  auto rc = fields->find("rc");
  if (rc == fields->end() || rc->second != "0") return fail();

  auto read_required = [&](const char* key) -> std::optional<int> {
    auto it = fields->find(key);
    return it == fields->end() ? std::nullopt : ParseInt(it->second);
  };
  auto response_rank = read_required("rank");
  auto response_size = read_required("size");
  auto response_appnum = read_required("appnum");
  if (!response_rank || !response_size || !response_appnum ||
      *response_size < 1 || *response_rank < 0 ||
      *response_rank >= *response_size)
    return fail();

  rank_ = *response_rank;
  size_ = *response_size;
  appnum_ = *response_appnum;
  state_ = State::kConnected;
  *spawned = fields->contains("spawner-jobid") ? 1 : 0;
  *size = size_;
  *rank = rank_;
  *appnum = appnum_;
  return PMI2_SUCCESS;
}

int Session::Finalize() {
  std::lock_guard lock(mutex_);
  if (state_ == State::kUninitialized) return PMI2_ERR_INIT;
  if (state_ == State::kConnected) {
    Fields response;
    int result = CommandLocked("finalize", {}, "finalize-response", &response);
    transport_->Close();
    transport_.reset();
    state_ = State::kFinalized;
    return result;
  }
  state_ = State::kFinalized;
  return PMI2_SUCCESS;
}

int Session::Initialized() const {
  std::lock_guard lock(mutex_);
  return state_ == State::kSingleton || state_ == State::kConnected;
}

int Session::Abort(int flag, const char* message) {
  std::lock_guard lock(mutex_);
  if (state_ != State::kConnected || transport_ == nullptr)
    return state_ == State::kSingleton ? PMI2_SUCCESS : PMI2_ERR_INIT;
  Fields request{{"cmd", "abort"},
                 {"isworld", flag ? "TRUE" : "FALSE"},
                 {"msg", message == nullptr ? "" : message}};
  auto body = Codec::EncodeFrame(request);
  if (!body || !transport_->SendFrame(*body)) return PMI2_ERR_OTHER;
  // PMI-2 defines Abort as a terminal operation. Let the process manager
  // observe the request before terminating this rank.
  std::exit(flag);
}

int Session::CopyValue(std::string_view source, char* destination, int capacity,
                       int* value_length) const {
  if (destination == nullptr || value_length == nullptr || capacity <= 0)
    return PMI2_ERR_INVALID_ARG;
  const auto length = static_cast<int>(source.size());
  const auto copied = std::min<int>(length, capacity - 1);
  std::memcpy(destination, source.data(), copied);
  destination[copied] = '\0';
  *value_length = copied == length ? length : -length;
  return PMI2_SUCCESS;
}

int Session::JobGetId(char* jobid, int size) {
  if (jobid == nullptr || size <= 0) return PMI2_ERR_INVALID_ARG;
  std::lock_guard lock(mutex_);
  if (state_ == State::kSingleton) {
    std::string_view value = jobid_;
    const auto copied = std::min<int>(value.size(), size - 1);
    std::memcpy(jobid, value.data(), copied);
    jobid[copied] = '\0';
    return copied == static_cast<int>(value.size()) ? PMI2_SUCCESS
                                                    : PMI2_ERR_INVALID_LENGTH;
  }
  Fields response;
  int result = CommandLocked("job-getid", {}, "job-getid-response", &response);
  if (result != PMI2_SUCCESS) return result;
  auto it = response.find("jobid");
  if (it == response.end()) return PMI2_ERR_OTHER;
  const auto copied = std::min<int>(it->second.size(), size - 1);
  std::memcpy(jobid, it->second.data(), copied);
  jobid[copied] = '\0';
  return copied == static_cast<int>(it->second.size())
             ? PMI2_SUCCESS
             : PMI2_ERR_INVALID_LENGTH;
}

int Session::JobGetRank(int* rank) const {
  if (rank == nullptr) return PMI2_ERR_INVALID_ARG;
  std::lock_guard lock(mutex_);
  if (state_ == State::kUninitialized || state_ == State::kFinalized)
    return PMI2_ERR_INIT;
  *rank = rank_;
  return PMI2_SUCCESS;
}

int Session::InfoGetSize(int* size) const {
  if (size == nullptr) return PMI2_ERR_INVALID_ARG;
  std::lock_guard lock(mutex_);
  if (state_ == State::kUninitialized || state_ == State::kFinalized)
    return PMI2_ERR_INIT;
  *size = size_;
  return PMI2_SUCCESS;
}

int Session::ValidateKeyValue(const char* key, const char* value) const {
  if (key == nullptr || value == nullptr) return PMI2_ERR_INVALID_ARG;
  const auto key_length = std::strlen(key);
  const auto value_length = std::strlen(value);
  if (key_length == 0 || key_length >= Codec::kMaxKey)
    return PMI2_ERR_INVALID_KEY_LENGTH;
  if (value_length >= Codec::kMaxValue) return PMI2_ERR_INVALID_VAL_LENGTH;
  return PMI2_SUCCESS;
}

int Session::KvsPut(const char* key, const char* value) {
  int result = ValidateKeyValue(key, value);
  if (result != PMI2_SUCCESS) return result;
  std::lock_guard lock(mutex_);
  return CommandLocked("kvs-put", {{"key", key}, {"value", value}},
                       "kvs-put-response", nullptr);
}

int Session::KvsFence() {
  std::lock_guard lock(mutex_);
  return CommandLocked("kvs-fence", {}, "kvs-fence-response", nullptr);
}

int Session::KvsGet(const char* jobid, int source, const char* key, char* value,
                    int maxvalue, int* value_length) {
  if (key == nullptr || value == nullptr || value_length == nullptr ||
      maxvalue <= 0)
    return PMI2_ERR_INVALID_ARG;
  if (std::strlen(key) >= Codec::kMaxKey) return PMI2_ERR_INVALID_KEY_LENGTH;
  std::lock_guard lock(mutex_);
  if (state_ != State::kConnected) return PMI2_ERR_INIT;
  Fields response;
  int result = CommandLocked("kvs-get",
                             {{"jobid", jobid == nullptr ? jobid_ : jobid},
                              {"srcid", std::to_string(source)},
                              {"key", key}},
                             "kvs-get-response", &response);
  if (result != PMI2_SUCCESS) return result;
  auto found = response.find("found");
  auto found_value = response.find("value");
  if (found == response.end() || !IsTrue(found->second) ||
      found_value == response.end())
    return PMI2_ERR_OTHER;
  return CopyValue(found_value->second, value, maxvalue, value_length);
}

int Session::AttributeGetLocked(std::string_view command, const char* name,
                                char* value, int value_length, int* found,
                                bool wait) {
  if (name == nullptr || value == nullptr || found == nullptr ||
      value_length <= 0)
    return PMI2_ERR_INVALID_ARG;
  if (std::strlen(name) >= Codec::kMaxKey) return PMI2_ERR_INVALID_KEY_LENGTH;
  Fields response;
  int result =
      CommandLocked(command, {{"key", name}, {"wait", wait ? "TRUE" : "FALSE"}},
                    command == "info-getnodeattr" ? "info-getnodeattr-response"
                                                  : "info-getjobattr-response",
                    &response);
  if (result != PMI2_SUCCESS) return result;
  auto found_value = response.find("found");
  if (found_value == response.end()) return PMI2_ERR_OTHER;
  *found = IsTrue(found_value->second) ? 1 : 0;
  if (!*found) {
    value[0] = '\0';
    return PMI2_SUCCESS;
  }
  auto value_it = response.find("value");
  if (value_it == response.end()) return PMI2_ERR_OTHER;
  int ignored_length = 0;
  return CopyValue(value_it->second, value, value_length, &ignored_length);
}

int Session::NodeAttrGet(const char* name, char* value, int value_length,
                         int* found, int wait) {
  std::lock_guard lock(mutex_);
  return AttributeGetLocked("info-getnodeattr", name, value, value_length,
                            found, wait != 0);
}

int Session::NodeAttrPut(const char* name, const char* value) {
  int result = ValidateKeyValue(name, value);
  if (result != PMI2_SUCCESS) return result;
  std::lock_guard lock(mutex_);
  return CommandLocked("info-putnodeattr", {{"key", name}, {"value", value}},
                       "info-putnodeattr-response", nullptr);
}

int Session::JobAttrGet(const char* name, char* value, int value_length,
                        int* found) {
  std::lock_guard lock(mutex_);
  return AttributeGetLocked("info-getjobattr", name, value, value_length, found,
                            false);
}

int Session::CopyArray(std::string_view source, int* array, int array_length,
                       int* output_length) const {
  if (array == nullptr || output_length == nullptr || array_length < 0)
    return PMI2_ERR_INVALID_ARG;
  if (source.empty()) {
    *output_length = 0;
    return PMI2_SUCCESS;
  }
  size_t start = 0;
  int count = 0;
  while (start <= source.size()) {
    size_t end = source.find(',', start);
    if (end == std::string_view::npos) end = source.size();
    if (count >= array_length) return PMI2_ERR_INVALID_SIZE;
    auto parsed = ParseInt(source.substr(start, end - start));
    if (!parsed) return PMI2_ERR_OTHER;
    array[count++] = *parsed;
    if (end == source.size()) break;
    start = end + 1;
  }
  *output_length = count;
  return PMI2_SUCCESS;
}

int Session::NodeAttrGetIntArray(const char* name, int* array, int array_length,
                                 int* output_length, int* found) {
  if (name == nullptr || array == nullptr || output_length == nullptr ||
      found == nullptr)
    return PMI2_ERR_INVALID_ARG;
  std::lock_guard lock(mutex_);
  if (state_ != State::kConnected) return PMI2_ERR_INIT;
  Fields response;
  int result =
      CommandLocked("info-getnodeattr", {{"key", name}, {"wait", "FALSE"}},
                    "info-getnodeattr-response", &response);
  if (result != PMI2_SUCCESS) return result;
  auto found_value = response.find("found");
  if (found_value == response.end()) return PMI2_ERR_OTHER;
  *found = IsTrue(found_value->second) ? 1 : 0;
  if (!*found) {
    *output_length = 0;
    return PMI2_SUCCESS;
  }
  auto value = response.find("value");
  return value == response.end()
             ? PMI2_ERR_OTHER
             : CopyArray(value->second, array, array_length, output_length);
}

int Session::JobAttrGetIntArray(const char* name, int* array, int array_length,
                                int* output_length, int* found) {
  if (name == nullptr || array == nullptr || output_length == nullptr ||
      found == nullptr)
    return PMI2_ERR_INVALID_ARG;
  std::lock_guard lock(mutex_);
  if (state_ != State::kConnected) return PMI2_ERR_INIT;
  Fields response;
  int result = CommandLocked("info-getjobattr", {{"key", name}},
                             "info-getjobattr-response", &response);
  if (result != PMI2_SUCCESS) return result;
  auto found_value = response.find("found");
  if (found_value == response.end()) return PMI2_ERR_OTHER;
  *found = IsTrue(found_value->second) ? 1 : 0;
  if (!*found) {
    *output_length = 0;
    return PMI2_SUCCESS;
  }
  auto value = response.find("value");
  return value == response.end()
             ? PMI2_ERR_OTHER
             : CopyArray(value->second, array, array_length, output_length);
}

int Session::Ring(const char* value, int* rank, int* ranks, char* left,
                  char* right, int maxvalue) {
  if (value == nullptr || rank == nullptr || ranks == nullptr ||
      left == nullptr || right == nullptr || maxvalue <= 0)
    return PMI2_ERR_INVALID_ARG;
  std::lock_guard lock(mutex_);
  if (state_ == State::kSingleton) {
    *rank = 0;
    *ranks = 1;
    std::strncpy(left, value, maxvalue - 1);
    std::strncpy(right, value, maxvalue - 1);
    left[maxvalue - 1] = right[maxvalue - 1] = '\0';
    return PMI2_SUCCESS;
  }
  Fields response;
  int result =
      CommandLocked("ring", {{"value", value}}, "ring-response", &response);
  if (result != PMI2_SUCCESS) return result;
  auto read = [&](const char* key, std::string* destination) {
    auto it = response.find(key);
    if (it == response.end()) return false;
    *destination = it->second;
    return true;
  };
  auto rank_it = response.find("rank");
  auto ranks_it = response.find("ranks");
  std::string left_value, right_value;
  auto response_rank = rank_it == response.end() ? std::optional<int>{}
                                                 : ParseInt(rank_it->second);
  auto response_ranks = ranks_it == response.end() ? std::optional<int>{}
                                                   : ParseInt(ranks_it->second);
  if (!response_rank || !response_ranks || !read("ring-left", &left_value) ||
      !read("ring-right", &right_value))
    return PMI2_ERR_OTHER;
  *rank = *response_rank;
  *ranks = *response_ranks;
  std::strncpy(left, left_value.c_str(), maxvalue - 1);
  std::strncpy(right, right_value.c_str(), maxvalue - 1);
  left[maxvalue - 1] = right[maxvalue - 1] = '\0';
  return PMI2_SUCCESS;
}

}  // namespace crane::pmi2
