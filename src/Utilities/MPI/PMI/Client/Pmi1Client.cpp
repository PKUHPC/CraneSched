/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 */

#include "Pmi1Session.h"

using crane::pmi1::GetSession;
using Integer = crane::pmi1::Session::Integer;

extern "C" {
int PMI_Init(int* spawned) { return GetSession().Initialize(spawned); }
int PMI_Initialized(PMI_BOOL* initialized) {
  return GetSession().Initialized(initialized);
}
int PMI_Finalize(void) { return GetSession().Finalize(); }
int PMI_Abort(int exit_code, const char message[]) {
  return GetSession().Abort(exit_code, message);
}
int PMI_Get_rank(int* rank) {
  return GetSession().GetInteger(Integer::kRank, rank);
}
int PMI_Get_size(int* size) {
  return GetSession().GetInteger(Integer::kSize, size);
}
int PMI_Get_universe_size(int* size) {
  return GetSession().GetInteger(Integer::kUniverse, size);
}
int PMI_Get_appnum(int* appnum) {
  return GetSession().GetInteger(Integer::kAppnum, appnum);
}
int PMI_Get_clique_size(int* size) {
  return GetSession().GetInteger(Integer::kCliqueSize, size);
}
int PMI_Get_clique_ranks(int ranks[], int length) {
  return GetSession().CliqueRanks(ranks, length);
}
int PMI_KVS_Get_my_name(char name[], int length) {
  return GetSession().KvsName(name, length);
}
int PMI_Get_id(char id[], int length) {
  return GetSession().KvsName(id, length);
}
int PMI_Get_kvs_domain_id(char id[], int length) {
  return GetSession().KvsName(id, length);
}
int PMI_Get_id_length_max(int* length) {
  return GetSession().GetInteger(Integer::kNameLength, length);
}
int PMI_KVS_Get_name_length_max(int* length) {
  return GetSession().GetInteger(Integer::kNameLength, length);
}
int PMI_KVS_Get_key_length_max(int* length) {
  return GetSession().GetInteger(Integer::kKeyLength, length);
}
int PMI_KVS_Get_value_length_max(int* length) {
  return GetSession().GetInteger(Integer::kValueLength, length);
}
int PMI_KVS_Put(const char name[], const char key[], const char value[]) {
  return GetSession().Put(name, key, value);
}
int PMI_KVS_Commit(const char name[]) { return GetSession().Commit(name); }
int PMI_KVS_Get(const char name[], const char key[], char value[], int length) {
  return GetSession().Get(name, key, value, length);
}
int PMI_Barrier(void) { return GetSession().Barrier(); }
int PMI_KVS_Create(char[], int) { return PMI_ERR_NOT_SUPPORTED; }
int PMI_KVS_Destroy(const char[]) { return PMI_ERR_NOT_SUPPORTED; }
int PMI_Publish_name(const char[], const char[]) {
  return PMI_ERR_NOT_SUPPORTED;
}
int PMI_Unpublish_name(const char[]) { return PMI_ERR_NOT_SUPPORTED; }
int PMI_Lookup_name(const char[], char[]) { return PMI_ERR_NOT_SUPPORTED; }
int PMI_Spawn_multiple(int, const char*[], const char**[], const int[],
                       const int[], const PMI_keyval_t*[], int,
                       const PMI_keyval_t[], int[]) {
  return PMI_ERR_NOT_SUPPORTED;
}
}  // extern "C"
