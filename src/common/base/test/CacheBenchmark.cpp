/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <folly/init/Init.h>
#include <folly/Benchmark.h>
#include <algorithm>
#include <vector>
#include "base/ConcurrentLRUCache.h"

const int32_t kTotal = 1 << 12;
using Cache = nebula::ConcurrentLRUCache<std::pair<int64_t, int32_t>, std::string>;
using ConMapCache = nebula::ConcurrentLRUCache<std::pair<int64_t, int32_t>,
                                               std::string,
                                               nebula::MapTTLCache<std::pair<int64_t, int32_t>,
                                                                   std::string>>;

BENCHMARK(LruCache1024Buckets) {
    std::unique_ptr<Cache> cache;
    BENCHMARK_SUSPEND {
        cache = std::make_unique<Cache>(kTotal, 10);
        for (int32_t i = 0; i < kTotal; i++) {
            cache->insert(std::make_pair(i, 0), std::to_string(i));
        }
    }
    for (int32_t i = 0; i < kTotal; i++) {
        auto val = cache->get(std::make_pair(i, 0));
        folly::doNotOptimizeAway(val);
    }
    BENCHMARK_SUSPEND {
        cache.reset();
    }
}

BENCHMARK_RELATIVE(MapCache1024Buckets) {
    std::unique_ptr<ConMapCache> cache;
    BENCHMARK_SUSPEND {
        cache = std::make_unique<ConMapCache>(kTotal, 10);
        for (int32_t i = 0; i < kTotal; i++) {
            cache->insert(std::make_pair(i, 0), std::to_string(i));
        }
    }
    for (int32_t i = 0; i < kTotal; i++) {
        auto val = cache->get(std::make_pair(i, 0));
        folly::doNotOptimizeAway(val);
    }
    BENCHMARK_SUSPEND {
        cache.reset();
    }
}

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    folly::runBenchmarks();
    return 0;
}


/*
Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz*

-O2
============================================================================
CacheBenchmark.cpprelative                                  time/iter  iters/s
============================================================================
LruCache1024Buckets                                        446.91us    2.24K
MapCache1024Buckets                              260.43%   171.60us    5.83K
============================================================================
*/

