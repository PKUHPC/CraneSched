/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "crane/pmi1/pmi.h"

#define CHECK(call)                           \
  do {                                        \
    if ((call) != PMI_SUCCESS) {              \
      fprintf(stderr, "failed: %s\n", #call); \
      return 1;                               \
    }                                         \
  } while (0)

int main(int argc, char** argv) {
  int initialized, spawned, rank, size, local_size, appnum;
  CHECK(PMI_Initialized(&initialized));
  if (initialized) return 1;
  CHECK(PMI_Init(&spawned));
  CHECK(PMI_Get_rank(&rank));
  CHECK(PMI_Get_size(&size));
  CHECK(PMI_Get_appnum(&appnum));
  if (appnum != -1) return 6;
  CHECK(PMI_Get_clique_size(&local_size));
  int* local = malloc((size_t)local_size * sizeof(int));
  CHECK(PMI_Get_clique_ranks(local, local_size));
  if (local_size != 2 || local[0] != rank % 2 || local[1] != rank % 2 + 2)
    return 2;
  free(local);
  if (argc > 1 && strcmp(argv[1], "abort") == 0)
    return PMI_Abort(17, "native PMI-1 abort");
  char name[256], alias[256], key[64], value[1024], expected[128];
  CHECK(PMI_KVS_Get_my_name(name, sizeof(name)));
  CHECK(PMI_Get_kvs_domain_id(alias, sizeof(alias)));
  if (strcmp(name, alias) != 0 || PMI_Init(&spawned) != PMI_ERR_INIT) return 3;
  if (PMI_KVS_Get_my_name(alias, 1) != PMI_ERR_INVALID_LENGTH) return 4;
  for (int round = 0; round < 3; ++round) {
    snprintf(key, sizeof(key), "round_%d_rank_%d", round, rank);
    snprintf(value, sizeof(value), "%d  padding-\\%d", rank, round);
    CHECK(PMI_KVS_Put(name, key, value));
    CHECK(PMI_KVS_Commit(name));
    CHECK(PMI_Barrier());
    for (int peer = 0; peer < size; ++peer) {
      snprintf(key, sizeof(key), "round_%d_rank_%d", round, peer);
      snprintf(expected, sizeof(expected), "%d  padding-\\%d", peer, round);
      CHECK(PMI_KVS_Get(name, key, value, sizeof(value)));
      if (strcmp(value, expected) != 0) return 5;
    }
    CHECK(PMI_Barrier());
  }
  CHECK(PMI_Finalize());
  CHECK(PMI_Initialized(&initialized));
  return initialized || PMI_Init(&spawned) != PMI_ERR_INIT;
}
