/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/stats/Metrics.h"
#include "folly/Benchmark.h"

using nebula::stats::Metrics;

void metricsBenchmark(const std::string& name, uint32_t numThreads, uint32_t iters) {
  std::vector<std::thread> threads;
  for (uint32_t i = 0; i < numThreads; i++) {
    auto itersInThread =
        i == 0 ? iters - (iters / numThreads) * (numThreads - 1) : iters / numThreads;
    threads.emplace_back([&name, itersInThread]() {
      for (uint32_t k = 0; k < itersInThread; k++) {
        Metrics::getInstance().counter(name);
      }
    });
  }
  for (size_t i = 0; i < numThreads; i++) {
    threads[i].join();
  }
}

BENCHMARK(METRICS_COUNTER_1T, iters) {
  metricsBenchmark("test_name", 1, iters);
}
BENCHMARK(METRICS_COUNTER_8T, iters) {
  metricsBenchmark("test_name", 1, iters);
}
BENCHMARK(METRICS_COUNTER_32T, iters) {
  metricsBenchmark("test_name", 1, iters);
}
BENCHMARK(METRICS_COUNTER_64T, iters) {
  metricsBenchmark("test_name", 1, iters);
}
int main(int argc, char** argv) {
  folly::init(&argc, &argv, true);
  Metrics::getInstance().registerCounter("test_name");
  folly::runBenchmarks();
  return 0;
}
