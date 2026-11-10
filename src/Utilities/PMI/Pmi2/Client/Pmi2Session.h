/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 */

#pragma once

#include <memory>
#include <mutex>
#include <string>

#include "Pmi2Codec.h"
#include "crane/pmi2/pmi2.h"

namespace crane::pmi2 {

class Transport;

class Session final {
 public:
  int Initialize(int* spawned, int* size, int* rank, int* appnum);
  int Finalize();
  int Initialized() const;
  int Abort(int flag, const char* message);
  int JobGetId(char* jobid, int size);
  int JobGetRank(int* rank) const;
  int InfoGetSize(int* size) const;
  int Ring(const char* value, int* rank, int* ranks, char* left, char* right,
           int maxvalue);
  int KvsPut(const char* key, const char* value);
  int KvsFence();
  int KvsGet(const char* jobid, int source, const char* key, char* value,
             int maxvalue, int* value_length);
  int NodeAttrGet(const char* name, char* value, int value_length, int* found,
                  int wait);
  int NodeAttrPut(const char* name, const char* value);
  int JobAttrGet(const char* name, char* value, int value_length, int* found);
  int JobAttrGetIntArray(const char* name, int* array, int array_length,
                         int* output_length, int* found);
  int NodeAttrGetIntArray(const char* name, int* array, int array_length,
                          int* output_length, int* found);

 private:
  enum class State { kUninitialized, kSingleton, kConnected, kFinalized };

  int CommandLocked(std::string_view command, const Fields& fields,
                    std::string_view expected, Fields* response);
  int AttributeGetLocked(std::string_view command, const char* name,
                         char* value, int value_length, int* found, bool wait);
  int CopyValue(std::string_view source, char* destination, int capacity,
                int* value_length) const;
  int CopyArray(std::string_view source, int* array, int array_length,
                int* output_length) const;
  int ValidateKeyValue(const char* key, const char* value) const;

  mutable std::mutex mutex_;
  State state_{State::kUninitialized};
  std::unique_ptr<Transport> transport_;
  std::string jobid_;
  int rank_{0};
  int size_{1};
  int appnum_{-1};
};

Session& GetSession();

}  // namespace crane::pmi2
