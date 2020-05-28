/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <sys/types.h>
#include <folly/init/Init.h>
#include <folly/Benchmark.h>
#include <iostream>

#define LOG_SOMETHING(iters) \
    for (int64_t i = 0; i < iters; i++) { \
        LOG(INFO) << "Hello" << " " << "Wolrd" << "123"; \
    }

/***************************
 *
 * <glog/logging.h> native
 *
 **************************/
#include <glog/logging.h>
void  loggingUsingGlog(int64_t iters) {
    LOG_SOMETHING(iters);
}


/***************************
 *
 * Optimized version
 *
 **************************/
#include "common/base/Logging.h"
void  loggingOptimized(int64_t iters) {
    LOG_SOMETHING(iters);
}


/***************************
 *
 * Run benchmarks
 *
 **************************/
BENCHMARK(glog_output_logs, iters) {
    FLAGS_minloglevel = 0;
    loggingUsingGlog(iters);
}

BENCHMARK_RELATIVE(optimized_output_logs, iters) {
    FLAGS_minloglevel = 0;
    loggingOptimized(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(glog_skip_logs, iters) {
    FLAGS_minloglevel = 1;
    loggingUsingGlog(iters);
}

BENCHMARK_RELATIVE(optimized_skip_logs, iters) {
    FLAGS_minloglevel = 1;
    loggingOptimized(iters);
}


/***************************
 *
 * main()
 *
 **************************/
int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);

    folly::runBenchmarks();
    return 0;
}


/*
Benchmark number is taken from WSL running on i7-8650

============================================================================
LoggingBenchmark.cpp                            relative  time/iter  iters/s
============================================================================
glog_output_logs                                             3.13us  319.07K
optimized_output_logs                            100.10%     3.13us  319.39K
----------------------------------------------------------------------------
glog_skip_logs                                               1.76us  567.45K
optimized_skip_logs                                 inf%     0.00fs  Infinity
============================================================================
*/
