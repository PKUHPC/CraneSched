/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 */

#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "Pmi1Codec.h"
#include "Transport.h"
#include "crane/pmi1/pmi.h"

namespace crane::pmi1 {

class Session final {
 public:
  enum class Integer {
    kRank,
    kSize,
    kUniverse,
    kAppnum,
    kCliqueSize,
    kNameLength,
    kKeyLength,
    kValueLength
  };
  int Initialize(int* spawned);
  int Initialized(int* initialized) const;
  int Finalize();
  int Abort(int exit_code, const char* message);
  int GetInteger(Integer attribute, int* value) const;
  int CliqueRanks(int* ranks, int length) const;
  int KvsName(char* name, int length) const;
  int Put(const char* name, const char* key, const char* value);
  int Commit(const char* name) const;
  int Get(const char* name, const char* key, char* value, int length);
  int Barrier();

 private:
  using Fields = ::pmi::pmi1::Fields;
  enum class State { kUninitialized, kSingleton, kConnected, kFinalized };
  bool Active_() const;
  int CheckKvs_(const char* name) const;
  int Exchange_(std::string_view command, const Fields& fields,
                std::string_view expected, Fields* response = nullptr);

  mutable std::mutex m_mutex_;
  State m_state_{State::kUninitialized};
  std::unique_ptr<pmi::Transport> m_transport_;
  int m_rank_{0};
  int m_size_{1};
  int m_appnum_{0};
  std::string m_jobid_{"singleton"};
  std::vector<int> m_local_ranks_;
  Fields m_singleton_kvs_;
};

Session& GetSession();
}  // namespace crane::pmi1
