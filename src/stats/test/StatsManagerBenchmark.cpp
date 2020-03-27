/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <folly/Benchmark.h>
#include "stats/StatsManager.h"

using nebula::stats::StatsManager;

const int32_t kCounterStats = StatsManager::registerStats("stats");
const int32_t kCounterHisto = StatsManager::registerHisto("histogram", 10, 1, 100);


void statsBM(int32_t counterId, uint32_t numThreads, uint32_t iters) {
    std::vector<std::thread> threads;
    for (uint32_t i = 0; i < numThreads; i++) {
        auto itersInThread = i == 0 ? iters - (iters / numThreads) * (numThreads - 1)
                                    : iters / numThreads;
        threads.emplace_back([counterId, itersInThread]() {
            for (uint32_t k = 0; k < itersInThread; k++) {
                StatsManager::addValue(counterId, k);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
}


BENCHMARK_DRAW_LINE();

BENCHMARK(add_stats_value_1t, iters) {
    statsBM(kCounterStats, 1, iters);
}

BENCHMARK(add_stats_value_4t, iters) {
    statsBM(kCounterStats, 4, iters);
}

BENCHMARK(add_stats_value_8t, iters) {
    statsBM(kCounterStats, 8, iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(add_histogram_value_1t, iters) {
    statsBM(kCounterHisto, 1, iters);
}

BENCHMARK(add_histogram_value_4t, iters) {
    statsBM(kCounterHisto, 4, iters);
}

BENCHMARK(add_histogram_value_8t, iters) {
    statsBM(kCounterHisto, 8, iters);
}

BENCHMARK_DRAW_LINE();


int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);

    folly::runBenchmarks();
    return 0;
}

/*
Test on Intel i7-8650U CPU @ 1.90GHz, 8GB RAM

============================================================================
StatsManagerBenchmark.cpp                       relative  time/iter  iters/s
============================================================================
----------------------------------------------------------------------------
add_stats_value_1t                                         701.59ns    1.43M
add_stats_value_4t                                           1.14us  877.59K
add_stats_value_8t                                           1.45us  689.51K
----------------------------------------------------------------------------
add_histogram_value_1t                                     813.82ns    1.23M
add_histogram_value_4t                                       1.18us  846.58K
add_histogram_value_8t                                       1.44us  695.65K
----------------------------------------------------------------------------
============================================================================
*/

