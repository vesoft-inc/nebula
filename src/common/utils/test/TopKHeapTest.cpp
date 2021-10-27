/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/utils/TopKHeap.h"

namespace nebula {

std::vector<int> getVector(std::function<bool(int, int)> comparator) {
  TopKHeap<int> topKHeap(3, comparator);
  topKHeap.push(3);
  topKHeap.push(6);
  topKHeap.push(1);
  topKHeap.push(9);
  topKHeap.push(2);
  topKHeap.push(7);
  return topKHeap.moveTopK();
}

TEST(TopKHeapTest, minHeapTest) {
  auto topK = getVector([](const int& lhs, const int& rhs) { return lhs < rhs; });
  std::stringstream ss;
  for (auto& item : topK) {
    ss << item << " ";
  }
  ASSERT_EQ(ss.str(), "3 2 1 ");
}

TEST(TopKHeapTest, maxHeapTest) {
  auto topK = getVector([](const int& lhs, const int& rhs) { return lhs > rhs; });
  std::stringstream ss;
  for (auto& item : topK) {
    ss << item << " ";
  }
  ASSERT_EQ(ss.str(), "6 7 9 ");
}

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
