/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/concurrency/ConcurrentHashMap.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "common/id/SegmentId.h"

namespace nebula {
class MockMetaClient : public meta::BaseMetaClient {
 public:
  folly::Future<StatusOr<int64_t>> getSegmentId(int64_t length) override {
    std::lock_guard<std::mutex> guard(mutex_);
    auto future = folly::makeFuture(cur_.load());
    cur_.fetch_add(length);
    return future;
  }

 private:
  std::mutex mutex_;
  std::atomic_int64_t cur_{0};
};

// In this case, small step cause that id run out before asyncFetchSegment() finish.
// So one segment replace another one. (one is from asyncFetchSegment(), another if from
// fetchSegment())
TEST(SegmentIdTest, TestConcurrencySmallStep) {
  MockMetaClient metaClient = MockMetaClient();

  int threadNum = 32;
  int times = 100000;

  std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager(
      PriorityThreadManager::newPriorityThreadManager(32));
  threadManager->setNamePrefix("executor");
  threadManager->start();

  SegmentId::initClient(&metaClient);
  SegmentId::initRunner(threadManager.get());
  SegmentId& generator = SegmentId::getInstance();
  generator.step_ = 1000;

  folly::ConcurrentHashMap<int64_t, int> map;
  std::vector<std::thread> threads;
  threads.reserve(threadNum);

  auto proc = [&]() {
    for (int i = 0; i < times; i++) {
      StatusOr<int64_t> id = generator.getId();
      ASSERT_TRUE(id.ok());
      // check duplicated id
      ASSERT_TRUE(map.find(id.value()) == map.end()) << "id: " << id.value();
      map.insert(id.value(), 0);
    }
  };

  for (int i = 0; i < threadNum; i++) {
    threads.emplace_back(std::thread(proc));
  }

  for (int i = 0; i < threadNum; i++) {
    threads[i].join();
  }
}

// check the result (in the case of no fetchSegment() by useing big step)
TEST(SegmentIdTest, TestConcurrencyBigStep) {
  MockMetaClient metaClient = MockMetaClient();

  int threadNum = 32;
  int times = 100000;

  std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager(
      PriorityThreadManager::newPriorityThreadManager(32));
  threadManager->setNamePrefix("executor");
  threadManager->start();

  SegmentId::initClient(&metaClient);
  SegmentId::initRunner(threadManager.get());
  SegmentId& generator = SegmentId::getInstance();
  Status status = generator.init(120000000);

  folly::ConcurrentHashMap<int64_t, int> map;
  std::vector<std::thread> threads;
  threads.reserve(threadNum);

  auto proc = [&]() {
    for (int i = 0; i < times; i++) {
      StatusOr<int64_t> id = generator.getId();
      ASSERT_TRUE(id.ok());
      // check duplicated id
      ASSERT_TRUE(map.find(id.value()) == map.end()) << "id: " << id.value();
      map.insert(id.value(), 0);
    }
  };

  for (int i = 0; i < threadNum; i++) {
    threads.emplace_back(std::thread(proc));
  }

  for (int i = 0; i < threadNum; i++) {
    threads[i].join();
  }

  // check the result
  for (int i = 0; i < (times * threadNum); i++) {
    ASSERT_TRUE(map.find(i) != map.end()) << "id: " << i;
  }
}

}  // namespace nebula
