/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/Benchmark.h>

#include <cstdio>
#include <fstream>
#include <iostream>

BENCHMARK(Popen) {
  auto pipe = popen("cat /sys/fs/cgroup/memory/memory.limit_in_bytes", "r");
  if (!pipe) {
    std::cerr << ::strerror(errno);
  }
  uint64_t value = 0;
  fscanf(pipe, "%lu", &value);
  if (pclose(pipe) < 0) {
    std::cerr << ::strerror(errno);
  }
}

BENCHMARK(Fstream) {
  std::ifstream ifs("/sys/fs/cgroup/memory/memory.limit_in_bytes");
  if (!ifs.is_open()) {
    std::cerr << "fail to open";
  }
  uint64_t value;
  ifs >> value;
  ifs.close();
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}

// ============================================================================
// src/common/memory/test/SysInfoReadBenchmark.cpp relative  time/iter  iters/s
// ============================================================================
// Popen                                                        1.46ms   683.72
// Fstream                                                      6.83us  146.42K
// ============================================================================
