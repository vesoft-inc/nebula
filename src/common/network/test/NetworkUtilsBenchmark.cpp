/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Benchmark.h>      // for addBenchmark, runBenchmarks
#include <folly/BenchmarkUtil.h>  // for doNotOptimizeAway
#include <folly/Conv.h>           // for to
#include <folly/String.h>         // for join
#include <folly/init/Init.h>      // for init
#include <stdint.h>               // for uint32_t

#include <algorithm>  // for copy
#include <deque>      // for operator!=, deque, operator==
#include <exception>  // for exception
#include <ostream>    // for operator<<, basic_ostream
#include <string>     // for string, basic_string, char_...

#include "common/base/Logging.h"          // for LOG, LogMessageFatal, _LOG_...
#include "common/network/NetworkUtils.h"  // for NetworkUtils

using nebula::network::NetworkUtils;

std::string intToIp(uint32_t ip) {
  std::deque<std::string> parts;
  for (int i = 0; i < 4; i++) {
    try {
      parts.emplace_front(folly::to<std::string>(ip & 0x000000FF));
      ip >>= 8;
    } catch (const std::exception& ex) {
      LOG(FATAL) << "Something is wrong: " << ex.what();
    }
  }

  return folly::join(".", parts);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(shared_intToIp, iters) {
  uint32_t ipInt = 0x44332211;
  for (uint32_t i = 0; i < iters; i++) {
    std::string ipStr = intToIp(ipInt);
    folly::doNotOptimizeAway(&ipStr);
  }
}
BENCHMARK_RELATIVE(intToIPv4, iters) {
  uint32_t ipInt = 0x44332211;
  for (uint32_t i = 0; i < iters; i++) {
    std::string ipStr = NetworkUtils::intToIPv4(ipInt);
    folly::doNotOptimizeAway(&ipStr);
  }
}

BENCHMARK_DRAW_LINE();

int main(int argc, char** argv) {
  folly::init(&argc, &argv, true);

  folly::runBenchmarks();
  return 0;
}

/*
 Tested on Intel Core i5-4300U (4 cores) with 8GB RAM
*/
