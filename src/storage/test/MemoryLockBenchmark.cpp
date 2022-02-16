/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Benchmark.h>                       // for addBenchmark, runB...
#include <folly/Conv.h>                            // for to
#include <folly/executors/IOThreadPoolExecutor.h>  // for IOThreadPoolExecutor
#include <folly/init/Init.h>                       // for init
#include <gflags/gflags.h>                         // for DEFINE_int64, DEFI...
#include <stddef.h>                                // for size_t
#include <stdint.h>                                // for int32_t, int64_t

#include <algorithm>      // for max
#include <functional>     // for _Bind_helper<>::type
#include <memory>         // for make_unique, uniqu...
#include <string>         // for string, basic_string
#include <tuple>          // for make_tuple, tuple
#include <unordered_map>  // for unordered_map
#include <unordered_set>  // for unordered_set
#include <utility>        // for move
#include <vector>         // for vector

#include "common/utils/MemoryLockCore.h"     // for MemoryLockCore
#include "common/utils/MemoryLockWrapper.h"  // for MemoryLockGuard
#include "common/utils/NebulaKeyUtils.h"     // for NebulaKeyUtils

DEFINE_int64(total_spaces, 10000, "total spaces number");
DEFINE_int64(num_threads, 100, "threads number");
DEFINE_int32(num_batch, 10000, "batch write number");

namespace nebula {
namespace storage {

using ThreadPool = folly::IOThreadPoolExecutor;
using Tuple = std::tuple<int32_t, int32_t, int32_t, std::string>;
using TupleLock = MemoryLockCore<Tuple>;
using StringLock = MemoryLockCore<std::string>;

void forTuple(TupleLock* lock, int64_t spaceId) noexcept {
  std::vector<Tuple> toLock;
  for (int32_t j = 0; j < FLAGS_num_batch; j++) {
    toLock.emplace_back(std::make_tuple(spaceId, j, j, folly::to<std::string>(j)));
  }
  nebula::MemoryLockGuard<Tuple> lg(lock, std::move(toLock));
}

void forString(StringLock* lock, int64_t spaceId) noexcept {
  std::vector<std::string> toLock;
  size_t vIdLen = 32;
  for (int32_t j = 0; j < FLAGS_num_batch; j++) {
    toLock.emplace_back(folly::to<std::string>(spaceId) +
                        NebulaKeyUtils::tagKey(vIdLen, j, folly::to<std::string>(j), j));
  }
  nebula::MemoryLockGuard<std::string> lg(lock, std::move(toLock));
}

BENCHMARK(TupleKey) {
  std::unique_ptr<TupleLock> lock = std::make_unique<TupleLock>();
  auto pool = std::make_unique<ThreadPool>(FLAGS_num_threads);
  for (auto i = 0; i < FLAGS_total_spaces; ++i) {
    pool->add(std::bind(forTuple, lock.get(), i));
  }
  pool->join();
}

BENCHMARK_RELATIVE(StringKey) {
  std::unique_ptr<StringLock> lock = std::make_unique<StringLock>();
  auto pool = std::make_unique<ThreadPool>(FLAGS_num_threads);
  for (auto i = 0; i < FLAGS_total_spaces; ++i) {
    pool->add(std::bind(forString, lock.get(), i));
  }
  pool->join();
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  folly::init(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}

// ============================================================================
// /MemoryLockBenchmark.cpp                         relative  time/iter  iters/s
// ============================================================================
// TupleKey                                                      4.85s  206.07m
// StringKey                                         90.23%      5.38s  185.94m
// ============================================================================
