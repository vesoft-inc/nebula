/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Benchmark.h>

#include "MockMetaClient.h"

class MockMetaClient : public nebula::meta::BaseMetaClient {
 public:
  folly::Future<nebula::StatusOr<int64_t>> getSegmentId(int64_t length) override {
    std::lock_guard<std::mutex> guard(mutex_);
    auto future = folly::makeFuture(cur_.load());
    cur_.fetch_add(length);
    return future;
  }

 private:
  std::mutex mutex_;
  std::atomic_int64_t cur_{0};
};

size_t SegmentIdCurrencyTest(size_t iters, int threadNum) {
  constexpr size_t ops = 1000000UL;
  int step = 120000000;

  MockMetaClient metaClient = MockMetaClient();
  std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager(
      PriorityThreadManager::newPriorityThreadManager(32));
  threadManager->setNamePrefix("executor");
  threadManager->start();

  nebula::SegmentId generator = nebula::SegmentId(&metaClient, threadManager.get());
  nebula::Status status = generator.init(step);
  ASSERT(status.ok());

  auto proc = [&]() {
    auto n = iters * ops;
    for (auto i = 0UL; i < n; i++) {
      nebula::StatusOr<int64_t> id = generator.getId();
      folly::doNotOptimizeAway(id);
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(threadNum);

  for (int i = 0; i < threadNum; i++) {
    threads.emplace_back(std::thread(proc));
  }

  for (int i = 0; i < threadNum; i++) {
    threads[i].join();
  }

  return iters * ops * threadNum;
}

BENCHMARK_NAMED_PARAM_MULTI(SegmentIdCurrencyTest, 1_thread, 1)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SegmentIdCurrencyTest, 2_thread, 2)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SegmentIdCurrencyTest, 4_thread, 4)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SegmentIdCurrencyTest, 8_thread, 8)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SegmentIdCurrencyTest, 16_thread, 16)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SegmentIdCurrencyTest, 32_thread, 32)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SegmentIdCurrencyTest, 64_thread, 64)

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}

/*           Intel(R) Xeon(R) CPU E5-2673 v3 @ 2.40GHz
============================================================================
nebula/src/common/id/test/SegmentIdBenchmark.cpprelative  time/iter  iters/s
============================================================================
SegmentIdCurrencyTest(1_thread)                             64.23ns   15.57M
SegmentIdCurrencyTest(2_thread)                   55.35%   116.03ns    8.62M
SegmentIdCurrencyTest(4_thread)                   49.64%   129.39ns    7.73M
SegmentIdCurrencyTest(8_thread)                   37.61%   170.76ns    5.86M
SegmentIdCurrencyTest(16_thread)                  34.91%   183.98ns    5.44M
SegmentIdCurrencyTest(32_thread)                  27.57%   232.98ns    4.29M
SegmentIdCurrencyTest(64_thread)                  21.77%   295.00ns    3.39M
============================================================================
*/
