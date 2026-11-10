/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 */

#include <algorithm>
#include <cstring>

#include "Pmi2Session.h"
#include "crane/pmi2/pmi2.h"

using crane::pmi2::GetSession;

extern "C" {

int PMI2_Init(int* spawned, int* size, int* rank, int* appnum) {
  return GetSession().Initialize(spawned, size, rank, appnum);
}

int PMI2_Finalize(void) { return GetSession().Finalize(); }

int PMI2_Initialized(void) { return GetSession().Initialized(); }

int PMI2_Abort(int flag, const char msg[]) {
  return GetSession().Abort(flag, msg);
}

int PMI2_Job_Spawn(int count, const char*[], int[], const char***, const int[],
                   const int[], const struct MPID_Info*[], int,
                   const struct MPID_Info*[], char jobId[], int jobIdSize,
                   int errors[]) {
  if (count < 0 || (jobId != nullptr && jobIdSize < 0))
    return PMI2_ERR_INVALID_ARG;
  if (errors != nullptr) std::fill_n(errors, count, PMI2_ERR_NOT_SUPPORTED);
  if (jobId != nullptr && jobIdSize > 0) jobId[0] = '\0';
  return PMI2_ERR_NOT_SUPPORTED;
}

int PMI2_Job_GetId(char jobid[], int jobid_size) {
  return GetSession().JobGetId(jobid, jobid_size);
}

int PMI2_Job_GetRank(int* rank) { return GetSession().JobGetRank(rank); }

int PMI2_Info_GetSize(int* size) { return GetSession().InfoGetSize(size); }

int PMI2_Job_Connect(const char[], PMI2_Connect_comm_t*) {
  return PMI2_ERR_NOT_SUPPORTED;
}

int PMI2_Job_Disconnect(const char[]) { return PMI2_ERR_NOT_SUPPORTED; }

int PMIX_Ring(const char value[], int* rank, int* ranks, char left[],
              char right[], int maxvalue) {
  return GetSession().Ring(value, rank, ranks, left, right, maxvalue);
}

int PMI2_KVS_Put(const char key[], const char value[]) {
  return GetSession().KvsPut(key, value);
}

int PMI2_KVS_Fence(void) { return GetSession().KvsFence(); }

int PMI2_KVS_Get(const char* jobid, int src_pmi_id, const char key[],
                 char value[], int maxvalue, int* vallen) {
  return GetSession().KvsGet(jobid, src_pmi_id, key, value, maxvalue, vallen);
}

int PMI2_Info_GetNodeAttr(const char name[], char value[], int valuelen,
                          int* found, int waitfor) {
  return GetSession().NodeAttrGet(name, value, valuelen, found, waitfor);
}

int PMI2_Info_GetNodeAttrIntArray(const char name[], int array[], int arraylen,
                                  int* outlen, int* found) {
  return GetSession().NodeAttrGetIntArray(name, array, arraylen, outlen, found);
}

int PMI2_Info_PutNodeAttr(const char name[], const char value[]) {
  return GetSession().NodeAttrPut(name, value);
}

int PMI2_Info_GetJobAttr(const char name[], char value[], int valuelen,
                         int* found) {
  return GetSession().JobAttrGet(name, value, valuelen, found);
}

int PMI2_Info_GetJobAttrIntArray(const char name[], int array[], int arraylen,
                                 int* outlen, int* found) {
  return GetSession().JobAttrGetIntArray(name, array, arraylen, outlen, found);
}

int PMI2_Nameserv_publish(const char[], const struct MPID_Info*, const char[]) {
  return PMI2_ERR_NOT_SUPPORTED;
}

int PMI2_Nameserv_lookup(const char[], const struct MPID_Info*, char port[],
                         int portLen) {
  if (port != nullptr && portLen > 0) port[0] = '\0';
  return PMI2_ERR_NOT_SUPPORTED;
}

int PMI2_Nameserv_unpublish(const char[], const struct MPID_Info*) {
  return PMI2_ERR_NOT_SUPPORTED;
}

}  // extern "C"
