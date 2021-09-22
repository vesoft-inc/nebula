/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <folly/logging/FileHandlerFactory.h>
#include <folly/logging/Init.h>
#include <folly/logging/LoggerDB.h>
#include <sys/types.h>

#include <iostream>

#define LOG_SOMETHING(iters)            \
  for (int64_t i = 0; i < iters; i++) { \
    LOG(INFO) << "Hello"                \
              << " "                    \
              << "Wolrd"                \
              << "123";                 \
  }

#define XLOG_SOMETHING(iters)           \
  for (int64_t i = 0; i < iters; i++) { \
    XLOG(INFO) << "Hello"               \
               << " "                   \
               << "Wolrd"               \
               << "123";                \
  }

class XlogInit {
 public:
  explicit XlogInit(folly::StringPiece config) { folly::initLogging(config); }
};

static void xlogRegistFileHandler() {
  folly::LoggerDB::get().registerHandlerFactory(std::make_unique<folly::FileHandlerFactory>());
  // Since glog outputs the logs to /tmp by default, so we explicitly set a file handler for
  // xlog and output logs to /tmp.
  folly::initLogging(";default=file:path=/tmp/logging_bm.log");
}

/***************************
 *
 * <glog/logging.h> native
 *
 **************************/
#include <glog/logging.h>
void loggingUsingGlog(int64_t iters) { LOG_SOMETHING(iters); }

/***************************
 *
 * Optimized version
 *
 **************************/
#include "common/base/Logging.h"
void loggingOptimized(int64_t iters) { LOG_SOMETHING(iters); }

#include <folly/logging/xlog.h>
void loggingUsingXlog(int64_t iters) { XLOG_SOMETHING(iters); }

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

BENCHMARK_RELATIVE(xlog_output_logs, iters) {
  BENCHMARK_SUSPEND { static XlogInit init(".=INFO:default"); }
  loggingUsingXlog(iters);
}

BENCHMARK_RELATIVE(xlog_output_logs_async, iters) {
  BENCHMARK_SUSPEND {
    static XlogInit init(".=INFO:default;default:async=true,max_buffer_size=4096");
  }
  loggingUsingXlog(iters);
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

BENCHMARK_RELATIVE(xlog_skip_logs, iters) {
  BENCHMARK_SUSPEND { static XlogInit init(".=WARN:default"); }
  loggingUsingXlog(iters);
}
/***************************
 *
 * main()
 *
 **************************/
int main(int argc, char** argv) {
  folly::init(&argc, &argv, true);
  xlogRegistFileHandler();

  folly::runBenchmarks();
  return 0;
}

/*
Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz
============================================================================
src/common/base/test/LoggingBenchmark.cpp       relative  time/iter  iters/s
============================================================================
glog_output_logs                                             1.86us  536.82K
optimized_output_logs                            100.26%     1.86us  538.24K
xlog_output_logs                                  52.73%     3.53us  283.09K
xlog_output_logs_async                            53.40%     3.49us  286.68K
----------------------------------------------------------------------------
glog_skip_logs                                               1.27us  789.36K
optimized_skip_logs                             94753.03%     1.34ns  747.94M
xlog_skip_logs                                  5215.83%    24.29ns   41.17M
============================================================================
*/
