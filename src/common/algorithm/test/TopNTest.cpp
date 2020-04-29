/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "algorithm/TopN.h"
#include <gtest/gtest.h>

namespace nebula {
namespace algorithm {
TEST(TopNTest, TopN) {
    std::vector<int64_t> nums = {5, 8, 4, 0, 9, 7, 6, 3, 1, 2};
    {
        std::vector<int64_t> top3(nums.begin(), nums.begin() + 3);
        auto less = [] (const auto& lhs, const auto& rhs) { return lhs < rhs; };
        auto eq = [] (const auto& lhs, const auto& rhs) { return lhs == rhs; };
        TopN<int64_t> topN(std::move(top3), less, eq);
        for (size_t i = 3; i < nums.size(); ++i) {
            auto num = nums[i];
            topN.push(std::move(num));
        }

        std::vector<int64_t> result = {9, 8, 7};
        EXPECT_EQ(topN.topN(), result);
    }
}
}  // namespace algorithm
}  // namespace nebula
