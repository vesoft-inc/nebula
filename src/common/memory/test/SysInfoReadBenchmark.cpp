/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <errno.h>                // for errno
#include <folly/Benchmark.h>      // for addBenchmark, BENCHMARK, runBenchmarks
#include <folly/BenchmarkUtil.h>  // for doNotOptimizeAway
#include <gflags/gflags.h>        // for ParseCommandLineFlags
#include <linux/sysinfo.h>        // for sysinfo
#include <stdint.h>               // for uint64_t
#include <string.h>               // for strerror
#include <sys/sysinfo.h>          // for sysinfo

#include <cstdio>    // for fscanf, pclose, popen
#include <fstream>   // for operator<<, cerr, ostream, ifstream
#include <iostream>  // for operator<<, cerr, ostream, ifstream

BENCHMARK(Popen) {
  auto pipe = popen("cat /sys/fs/cgroup/memory/memory.limit_in_bytes", "r");
  if (!pipe) {
    std::cerr << ::strerror(errno);
  }
  uint64_t value = 0;
  if (fscanf(pipe, "%lu", &value) != 2) {
    std::cerr << ::strerror(errno);
  }
  if (pclose(pipe) < 0) {
    std::cerr << ::strerror(errno);
  }
  folly::doNotOptimizeAway(value);
}

BENCHMARK(Fstream) {
  std::ifstream ifs("/sys/fs/cgroup/memory/memory.limit_in_bytes");
  if (!ifs.is_open()) {
    std::cerr << "fail to open";
  }
  uint64_t value;
  ifs >> value;
  ifs.close();
  folly::doNotOptimizeAway(value);
}

BENCHMARK(SysInfo) {
  struct sysinfo info;
  if (sysinfo(&info) == -1) {
    std::cerr << "fail to sysinfo: " << strerror(errno);
  }
  uint64_t total = info.totalram * info.mem_unit;
  folly::doNotOptimizeAway(total);
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}

// ============================================================================
// src/common/memory/test/SysInfoReadBenchmark.cpp relative  time/iter  iters/s
// ============================================================================
// Popen                                                        1.34ms   744.82
// Fstream                                                      4.44us  225.29K
// SysInfo                                                    524.54ns    1.91M
// ============================================================================
