/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute of Computing and Digital Economy
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static int WriteAll(int fd, const char* text) {
  size_t left = strlen(text);
  while (left > 0) {
    ssize_t written = send(fd, text, left, MSG_NOSIGNAL);
    if (written <= 0) return 0;
    text += written;
    left -= (size_t)written;
  }
  return 1;
}

static int ReadLine(int fd, char* line, size_t capacity) {
  size_t length = 0;
  while (length + 1 < capacity) {
    char ch;
    ssize_t received = recv(fd, &ch, 1, 0);
    if (received != 1) return 0;
    line[length++] = ch;
    if (ch == '\n') {
      line[length] = '\0';
      return 1;
    }
  }
  return 0;
}

static int Exchange(int fd, const char* request, char* response,
                    size_t capacity) {
  return WriteAll(fd, request) && ReadLine(fd, response, capacity);
}

static int Field(const char* line, const char* key, char* value,
                 size_t capacity) {
  size_t key_length = strlen(key);
  const char* current = line;
  while (*current != '\0' && *current != '\n') {
    while (*current == ' ') ++current;
    const char* end = current;
    while (*end != '\0' && *end != '\n' && *end != ' ') ++end;
    const char* equal = memchr(current, '=', (size_t)(end - current));
    if (equal != NULL && (size_t)(equal - current) == key_length &&
        memcmp(current, key, key_length) == 0) {
      size_t length = (size_t)(end - equal - 1);
      if (length + 1 > capacity) return 0;
      memcpy(value, equal + 1, length);
      value[length] = '\0';
      return 1;
    }
    current = end;
  }
  return 0;
}

static int Has(const char* line, const char* expected) {
  return strstr(line, expected) != NULL;
}

int main(void) {
  const char* fd_text = getenv("PMI_FD");
  const char* rank_text = getenv("PMI_RANK");
  const char* size_text = getenv("PMI_SIZE");
  const char* jobid = getenv("PMI_JOBID");
  if (fd_text == NULL || rank_text == NULL || size_text == NULL ||
      jobid == NULL)
    return 1;
  int fd = atoi(fd_text);
  int rank = atoi(rank_text);
  int size = atoi(size_text);
  char request[256], response[4096], value[128];

  if (!Exchange(fd, "cmd=init pmi_version=1 pmi_subversion=1\n", response,
                sizeof(response)) ||
      !Has(response, "cmd=response_to_init") || !Has(response, "rc=0") ||
      !Has(response, "pmi_version=1") || !Has(response, "pmi_subversion=1"))
    return 2;

  if (!Exchange(fd, "cmd=get_maxes\n", response, sizeof(response)) ||
      !Has(response, "cmd=maxes") || !Has(response, "rc=0") ||
      !Has(response, "keylen_max=64") || !Has(response, "vallen_max=1024"))
    return 3;
  if (!Exchange(fd, "cmd=get_universe_size\n", response, sizeof(response)) ||
      !Has(response, "cmd=universe_size") ||
      !Field(response, "size", value, sizeof(value)) || atoi(value) != size)
    return 4;
  if (!Exchange(fd, "cmd=get_appnum\n", response, sizeof(response)) ||
      !Has(response, "cmd=appnum") || !Has(response, "appnum=-1"))
    return 5;
  if (!Exchange(fd, "cmd=get_my_kvsname\n", response, sizeof(response)) ||
      !Has(response, "cmd=my_kvsname") ||
      !Field(response, "kvsname", value, sizeof(value)) ||
      strcmp(value, jobid) != 0)
    return 6;

  snprintf(request, sizeof(request),
           "cmd=put kvsname=%s key=rank_%d value=%d\n", jobid, rank, rank);
  if (!Exchange(fd, request, response, sizeof(response)) ||
      !Has(response, "cmd=put_result") || !Has(response, "rc=0"))
    return 7;
  if (!Exchange(fd, "cmd=barrier_in\n", response, sizeof(response)) ||
      !Has(response, "cmd=barrier_out") || !Has(response, "rc=0"))
    return 8;

  for (int peer = 0; peer < size; ++peer) {
    snprintf(request, sizeof(request), "cmd=get kvsname=%s key=rank_%d\n",
             jobid, peer);
    if (!Exchange(fd, request, response, sizeof(response)) ||
        !Has(response, "cmd=get_result") || !Has(response, "rc=0") ||
        !Field(response, "value", value, sizeof(value)) || atoi(value) != peer)
      return 9;
  }

  if (!Exchange(fd, "cmd=finalize\n", response, sizeof(response)) ||
      !Has(response, "cmd=finalize_ack") || !Has(response, "rc=0"))
    return 10;
  printf("PMI1_OK rank=%d size=%d jobid=%s\n", rank, size, jobid);
  return 0;
}
