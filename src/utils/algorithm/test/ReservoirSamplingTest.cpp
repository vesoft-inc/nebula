/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/algorithm/ReservoirSampling.h"
#include <gtest/gtest.h>

namespace nebula {
namespace algorithm {
TEST(ReservoirSamplingTest, Sample) {
    {
        ReservoirSampling<int64_t> sampler(5);
        std::vector<int64_t> sampleSpace = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        for (auto i : sampleSpace) {
            sampler.sampling(std::move(i));
        }

        auto result = std::move(sampler).samples();
        EXPECT_EQ(5, result.size());
        for (auto i : result) {
            EXPECT_LE(0, i);
            EXPECT_GE(9, i);
        }
    }
    {
        ReservoirSampling<int64_t> sampler(5);
        std::vector<int64_t> sampleSpace = {0, 1, 2};
        for (auto i : sampleSpace) {
            sampler.sampling(std::move(i));
        }

        auto result = std::move(sampler).samples();
        EXPECT_EQ(3, result.size());
        EXPECT_EQ(0, result[0]);
        EXPECT_EQ(1, result[1]);
        EXPECT_EQ(2, result[2]);
    }
}
}  // namespace algorithm
}  // namespace nebula
