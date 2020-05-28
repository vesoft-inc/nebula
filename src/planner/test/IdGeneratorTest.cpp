/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "planner/IdGenerator.h"
#include "common/concurrent/Barrier.h"

namespace nebula {
namespace graph {
TEST(IdGeneratorTest, gen) {
    nebula::concurrent::Barrier bar(3);
    std::vector<int64_t> ids1;
    auto t1 = std::thread([&ids1, &bar] () {
        for (auto i = 0; i < 5; ++i) {
            ids1.emplace_back(EPIdGenerator::instance().id());
        }
        bar.wait();
    });

    std::vector<int64_t> ids2;
    auto t2 = std::thread([&ids2, &bar] () {
        for (auto i = 0; i < 5; ++i) {
            ids2.emplace_back(EPIdGenerator::instance().id());
        }
        bar.wait();
    });

    bar.wait();
    t1.join();
    t2.join();

    ids1.insert(ids1.end(), ids2.begin(), ids2.end());
    EXPECT_EQ(10, ids1.size());
    std::sort(ids1.begin(), ids1.end());
    std::vector<int64_t> expected = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    EXPECT_EQ(expected, ids1);
}
}  // namespace graph
}  // namespace nebula
