/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <rocksdb/db.h>
#include <folly/Benchmark.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include "utils/MemoryLockWrapper.h"
#include "utils/NebulaKeyUtils.h"

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
                            NebulaKeyUtils::vertexKey(vIdLen, j, folly::to<std::string>(j), j));
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

int main(int argc, char **argv) {
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
