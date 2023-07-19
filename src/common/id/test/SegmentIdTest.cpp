/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/concurrency/ConcurrentHashMap.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "MockMetaClient.h"

namespace nebula {
class TestSegmentId : public testing::Test {
 protected:
  void SetUp() override {
    threads_.reserve(threadNum_);

    threadManager_->setNamePrefix("executor");
    threadManager_->start();
  }

  int threadNum_{32};
  int times_{100000};

  std::vector<std::thread> threads_;
  folly::ConcurrentHashMap<int64_t, int> map_;

  nebula::MockMetaClient metaClient_;
  std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager_{
      PriorityThreadManager::newPriorityThreadManager(32)};
};

TEST_F(TestSegmentId, TestConcurrencySmallStep) {
  SegmentId generator = SegmentId(&metaClient_, threadManager_.get());
  Status status = generator.init(1000);

  auto proc = [&]() {
    for (int i = 0; i < times_; i++) {
      StatusOr<int64_t> id = generator.getId();
      ASSERT_TRUE(id.ok());
      // check duplicated id
      ASSERT_TRUE(map_.find(id.value()) == map_.end()) << "id: " << id.value();
      map_.insert(id.value(), 0);
    }
  };

  for (int i = 0; i < threadNum_; i++) {
    threads_.emplace_back(std::thread(proc));
  }

  for (int i = 0; i < threadNum_; i++) {
    threads_[i].join();
  }
}

// check the result (in the case of no fetchSegment() by using big step)
TEST_F(TestSegmentId, TestConcurrencyBigStep) {
  SegmentId generator = SegmentId(&metaClient_, threadManager_.get());
  Status status = generator.init(120000000);

  auto proc = [&]() {
    for (int i = 0; i < times_; i++) {
      StatusOr<int64_t> id = generator.getId();
      ASSERT_TRUE(id.ok());
      // check duplicated id
      ASSERT_TRUE(map_.find(id.value()) == map_.end()) << "id: " << id.value();
      map_.insert(id.value(), 0);
    }
  };

  for (int i = 0; i < threadNum_; i++) {
    threads_.emplace_back(std::thread(proc));
  }

  for (int i = 0; i < threadNum_; i++) {
    threads_[i].join();
  }

  // check the result
  for (int i = 0; i < (times_ * threadNum_); i++) {
    ASSERT_TRUE(map_.find(i) != map_.end()) << "id: " << i;
  }
}

}  // namespace nebula
