/**
 * Copyright (c) 2026 Peking University and Peking University
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

#include <pmi2.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(call)                           \
  do {                                        \
    int rc = (call);                          \
    if (rc != PMI2_SUCCESS) {                 \
      fprintf(stderr, "%s: %d\n", #call, rc); \
      return 1;                               \
    }                                         \
  } while (0)

int main(int argc, char** argv) {
  int spawned, size, rank, appnum;
  CHECK(PMI2_Init(&spawned, &size, &rank, &appnum));
  if (size != atoi(getenv("PMI_SIZE")) || rank != atoi(getenv("PMI_RANK")))
    return 2;
  char jobid[128], key[64], value[1024];
  int found, length;
  CHECK(PMI2_Job_GetId(jobid, sizeof(jobid)));
  if (strcmp(jobid, getenv("PMI_JOBID"))) return 3;
  CHECK(PMI2_Info_GetJobAttr("PMI_process_mapping", value, sizeof(value),
                             &found));
  if (!found || strcmp(value, getenv("EXPECTED_MAPPING"))) return 4;
  if (argc > 1 && !strcmp(argv[1], "mapping")) {
    CHECK(PMI2_Finalize());
    return 0;
  }
  if (argc > 1 && !strcmp(argv[1], "wait-timeout")) {
    int rc = PMI2_Info_GetNodeAttr("never-published", value, sizeof(value),
                                   &found, 1);
    return rc == PMI2_SUCCESS ? 10 : 0;
  }
  for (int epoch = 0; epoch < 3; ++epoch) {
    snprintf(key, sizeof(key), "rank_%d", rank);
    snprintf(value, sizeof(value), "%d", rank + epoch * 100);
    CHECK(PMI2_KVS_Put(key, value));
    CHECK(PMI2_KVS_Fence());
    for (int peer = 0; peer < size; ++peer) {
      snprintf(key, sizeof(key), "rank_%d", peer);
      CHECK(PMI2_KVS_Get(jobid, peer, key, value, sizeof(value), &length));
      if (atoi(value) != peer + epoch * 100) return 5;
    }
  }
  CHECK(PMI2_Info_GetJobAttr("not-an-attribute", value, sizeof(value), &found));
  if (found) return 6;
  if (atoi(getenv("PMI_LOCAL_RANK")) == 0) {
    CHECK(PMI2_Info_PutNodeAttr("ready", "node-value"));
  }
  CHECK(PMI2_Info_GetNodeAttr("ready", value, sizeof(value), &found, 1));
  if (!found || strcmp(value, "node-value")) return 7;
  CHECK(PMI2_KVS_Put("escaped", "a;b=c;;d"));
  CHECK(PMI2_KVS_Fence());
  CHECK(PMI2_KVS_Get(jobid, PMI2_ID_NULL, "escaped", value, sizeof(value),
                     &length));
  if (strcmp(value, "a;b=c;;d")) return 8;
  if (PMI2_Job_Connect("other-job", NULL) == PMI2_SUCCESS) return 9;
  CHECK(PMI2_Finalize());
  printf("PMI2_OK rank=%d size=%d jobid=%s\n", rank, size, jobid);
  return 0;
}
