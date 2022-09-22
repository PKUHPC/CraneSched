#include <gtest/gtest.h>
#include <libcgroup.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <iostream>
#include <numeric>
#include <thread>
#include <vector>

#include "AnonymousPipe.h"
#include "cgroup.linux.h"

// NOTICE: For non-RHEL system, swap account in cgroup is disabled by default.
//  Turn it on by adding
//  `GRUB_CMDLINE_LINUX="cgroup_enable=memory swapaccount=1`
//  to `/etc/default/grub` file.

// NOTICE: By default, Linux follows an optimistic memory allocation
//  strategy.  This means that when malloc() returns non-NULL there
//  is no guarantee that the memory really is available.
//  see https://man7.org/linux/man-pages/man3/malloc.3.html
//
//  The process exceeding the memory+swap limit will be killed by oom-kill
//  (Receiving SIGTERM and exiting with return value 9).
//
//  To make malloc return NULL, see the description of
//  /proc/sys/vm/overcommit_memory and /proc/sys/vm/oom_adj in proc(5), and the
//  kernel source file Documentation/vm/overcommit-accounting

struct pstat {
  long unsigned int utime_ticks;
  long int cutime_ticks;
  long unsigned int stime_ticks;
  long int cstime_ticks;
  long unsigned int vsize;  // virtual memory size in bytes
  long unsigned int rss;    // Resident  Set  Size in bytes

  long unsigned int cpu_total_time;
};

int get_usage(pid_t pid, struct pstat* result);

void calc_cpu_usage_pct(const struct pstat* cur_usage,
                        const struct pstat* last_usage, double* ucpu_usage,
                        double* scpu_usage);
constexpr size_t B = 1;
constexpr size_t KB = 1024 * B;
constexpr size_t MB = 1024 * KB;

std::atomic_bool sigchld_received;

void sigchld_handler(int sig) { sigchld_received = true; }

TEST(cgroup, cpu_core_limit) {
  signal(SIGCHLD, sigchld_handler);
  sigchld_received = false;

  AnonymousPipe anon_pipe_p_c;
  AnonymousPipe anon_pipe_c_p;
  u_char val;
  bool _;

  pid_t child_pid = fork();
  if (child_pid == 0) {
    _ = anon_pipe_p_c.ReadIntegerFromParent<u_char>(&val);

    auto cpu_burning = [] {
      clock_t timeStart;
      timeStart = clock();
      volatile int i1, i2, i3, i4, i5, i6, i7, i8;
      i1 = i2 = i3 = i4 = i5 = i6 = i7 = i8 = 0;
      while (true) {
        i1++;
        i2++;
        i3++;
        i4++;
        i5++;
        i6++;
        i7++;
        i8++;

        if ((clock() - timeStart) / CLOCKS_PER_SEC >= 60)  // time in seconds
          break;
      }
    };

    std::thread t1(cpu_burning);
    std::thread t2(cpu_burning);
    std::thread t3(cpu_burning);

    _ = anon_pipe_c_p.WriteIntegerToChild<u_char>(val);

    t1.join();
    t2.join();
    t3.join();

  } else {
    using namespace util;
    const std::string cg_path{"riley_cgroup"};

    CgroupManager& cm = CgroupManager::Instance();
    Cgroup* cg = cm.CreateOrOpen(cg_path, ALL_CONTROLLER_FLAG,
                                 NO_CONTROLLER_FLAG, false);

    cg->SetCpuCoreLimit(2);

    cm.MigrateProcTo(child_pid, cg_path);

    val = 1;
    _ = anon_pipe_p_c.WriteIntegerToChild<u_char>(val);

    std::atomic_bool running{true};
    pstat stat_1{};
    pstat stat_2{};
    double scpu, ucpu;
    double avg_cpu = 0;

    _ = anon_pipe_c_p.ReadIntegerFromParent<u_char>(&val);

    get_usage(child_pid, &stat_2);
    while (!sigchld_received) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      get_usage(child_pid, &stat_1);
      calc_cpu_usage_pct(&stat_1, &stat_2, &ucpu, &scpu);
      stat_2 = stat_1;

      avg_cpu =
          (avg_cpu > 0.005f) ? (avg_cpu + ucpu + scpu) / 2 : (ucpu + scpu);

      // spdlog::info("Pid:{}, CPU: {:.1f}%, Avg: {:.1f}%", child_pid, ucpu +
      // scpu, avg_cpu);
    }

    ASSERT_LE(avg_cpu, 240)
        << "Avg cpu usage " << avg_cpu << "% should be less equal than 240%!";

    int child_ret_val;
    wait(&child_ret_val);

    cm.Release(cg_path);
  }

  signal(SIGCHLD, SIG_DFL);
}

TEST(cgroup, memory_limit) {
  pid_t child_pid = fork();
  if (child_pid == 0) {
    sleep(2);

    uint8_t* space = nullptr;
    try {
      size_t size = 20 * MB;
      space = new uint8_t[size];

      std::memset(space, 0, size);
    } catch (std::bad_alloc& e) {
      fmt::print("[Child] std::bad_alloc {}\n", e.what());
      space = nullptr;
    }

    if (space) {
      delete[] space;
    }

    sleep(10);
  } else {
    SPDLOG_INFO("[Parent] Child pid: {}\n", child_pid);

    const std::string cg_path{"riley_cgroup"};

    using namespace util;
    CgroupManager& cm = CgroupManager::Instance();
    Cgroup* cg = cm.CreateOrOpen(cg_path, ALL_CONTROLLER_FLAG,
                                 NO_CONTROLLER_FLAG, false);

    cg->SetMemoryLimitBytes(10 * MB);
    cg->SetMemorySwLimitBytes(10 * MB);
    cg->SetMemorySoftLimitBytes(10 * MB);

    cm.MigrateProcTo(child_pid, cg_path);

    int child_status;
    wait(&child_status);

    ASSERT_TRUE(WIFSIGNALED(child_status)) << "Child should be killed.";
    ASSERT_EQ(WTERMSIG(child_status), 9)
        << "Child should exit with SIGTERM rather than "
        << strsignal(WTERMSIG(child_status)) << ".\n";

    cm.Release(cg_path);
  }
}

/*
 * read /proc data into the passed struct pstat
 * returns 0 on success, -1 on error
 */
int get_usage(pid_t pid, struct pstat* result) {
  // convert  pid to string
  char pid_s[20];
  snprintf(pid_s, sizeof(pid_s), "%d", pid);
  char stat_filepath[30] = "/proc/";
  strncat(stat_filepath, pid_s,
          sizeof(stat_filepath) - strlen(stat_filepath) - 1);
  strncat(stat_filepath, "/stat",
          sizeof(stat_filepath) - strlen(stat_filepath) - 1);

  FILE* fpstat = fopen(stat_filepath, "r");
  if (fpstat == NULL) {
    perror("FOPEN ERROR ");
    return -1;
  }

  FILE* fstat = fopen("/proc/stat", "r");
  if (fstat == NULL) {
    perror("FOPEN ERROR ");
    fclose(fstat);
    return -1;
  }

  // read values from /proc/pid/stat
  bzero(result, sizeof(struct pstat));
  long int rss;
  if (fscanf(fpstat,
             "%*d %*s %*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %lu"
             "%lu %ld %ld %*d %*d %*d %*d %*u %lu %ld",
             &result->utime_ticks, &result->stime_ticks, &result->cutime_ticks,
             &result->cstime_ticks, &result->vsize, &rss) == EOF) {
    fclose(fpstat);
    return -1;
  }
  fclose(fpstat);
  result->rss = rss * getpagesize();

  // read+calc cpu total time from /proc/stat
  unsigned long long cpu_time[4];
  bzero(cpu_time, sizeof(cpu_time));
  if (fscanf(fstat, "%*s %llu %llu %llu %llu", &cpu_time[0], &cpu_time[1],
             &cpu_time[2], &cpu_time[3]) == EOF) {
    fclose(fstat);
    return -1;
  }

  fclose(fstat);

  for (unsigned long long i : cpu_time) result->cpu_total_time += i;

  return 0;
}

/*
 * calculates the elapsed CPU usage between 2 measuring points. in percent
 */
void calc_cpu_usage_pct(const struct pstat* cur_usage,
                        const struct pstat* last_usage, double* ucpu_usage,
                        double* scpu_usage) {
  static long nb = sysconf(_SC_NPROCESSORS_ONLN);

  const long unsigned int total_time_diff =
      cur_usage->cpu_total_time - last_usage->cpu_total_time;

  *ucpu_usage = 100 * nb *
                ((cur_usage->utime_ticks - last_usage->utime_ticks) /
                 (double)total_time_diff);

  *scpu_usage = 100 * nb *
                (((cur_usage->stime_ticks - last_usage->stime_ticks)) /
                 (double)total_time_diff);
}

/*
 * calculates the elapsed CPU usage between 2 measuring points in ticks
 */
void calc_cpu_usage(const struct pstat* cur_usage,
                    const struct pstat* last_usage,
                    long unsigned int* ucpu_usage,
                    long unsigned int* scpu_usage) {
  *ucpu_usage = (cur_usage->utime_ticks + cur_usage->cutime_ticks) -
                (last_usage->utime_ticks + last_usage->cutime_ticks);

  *scpu_usage = (cur_usage->stime_ticks + cur_usage->cstime_ticks) -
                (last_usage->stime_ticks + last_usage->cstime_ticks);
}