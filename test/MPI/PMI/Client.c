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

static int CheckIntegerAttributes(int size) {
  const struct {
    const char* value;
    int capacity;
    int result;
    int length;
    int values[3];
  } cases[] = {
      {"1,-2,3", 3, PMI2_SUCCESS, 3, {1, -2, 3}},
      {"", 0, PMI2_SUCCESS, 0, {0}},
      {"1,2,3", 2, PMI2_ERR_INVALID_SIZE, 0, {0}},
      {"1", -1, PMI2_ERR_INVALID_ARG, 0, {0}},
      {"1,no", 3, PMI2_ERR_OTHER, 0, {0}},
      {"99999999999999999999999", 3, PMI2_ERR_OTHER, 0, {0}},
      {"1,", 3, PMI2_ERR_OTHER, 0, {0}},
  };
  int values[3], length, found;
  for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
    CHECK(PMI2_Info_PutNodeAttr("integers", cases[i].value));
    int rc = PMI2_Info_GetNodeAttrIntArray("integers", values,
                                           cases[i].capacity, &length, &found);
    if (rc != cases[i].result || !found) return 11;
    if (rc == PMI2_SUCCESS &&
        (length != cases[i].length ||
         memcmp(values, cases[i].values, length * sizeof(int))))
      return 12;
  }

  // Both public entry points share decoding and validation, while requesting
  // different server attributes. A missing node attribute must not wait.
  int (*getters[])(const char*, int*, int, int*, int*) = {
      PMI2_Info_GetNodeAttrIntArray, PMI2_Info_GetJobAttrIntArray};
  const char* keys[] = {"integers", "universeSize"};
  CHECK(PMI2_Info_PutNodeAttr("integers", "1"));
  for (size_t i = 0; i < sizeof(getters) / sizeof(getters[0]); ++i) {
    CHECK(getters[i](keys[i], values, 3, &length, &found));
    if (!found || length != 1 || values[0] != (i == 0 ? 1 : size)) return 13;
    length = found = -1;
    CHECK(getters[i]("missing-integer-attribute", values, 3, &length, &found));
    if (found != 0 || length != 0) return 14;
    if (getters[i](NULL, values, 3, &length, &found) != PMI2_ERR_INVALID_ARG ||
        getters[i](keys[i], NULL, 3, &length, &found) != PMI2_ERR_INVALID_ARG ||
        getters[i](keys[i], values, 3, NULL, &found) != PMI2_ERR_INVALID_ARG ||
        getters[i](keys[i], values, 3, &length, NULL) != PMI2_ERR_INVALID_ARG ||
        getters[i](keys[i], values, 0, &length, &found) !=
            PMI2_ERR_INVALID_SIZE ||
        getters[i](keys[i], values, -1, &length, &found) !=
            PMI2_ERR_INVALID_ARG)
      return 15;
  }
  return 0;
}

int main(int argc, char** argv) {
  int spawned, size, rank, appnum;
  CHECK(PMI2_Init(&spawned, &size, &rank, &appnum));
  if (size != atoi(getenv("PMI_SIZE")) || rank != atoi(getenv("PMI_RANK")) ||
      appnum != 0)
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
  if (argc > 1 && !strcmp(argv[1], "int-attributes")) {
    int rc = CheckIntegerAttributes(size);
    CHECK(PMI2_Finalize());
    return rc;
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
