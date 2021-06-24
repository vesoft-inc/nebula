/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/base/Memory.h"

namespace nebula {

TEST(MemInfoTest, TestMemInfo) {
    auto status = MemInfo::make();
    ASSERT(status.ok());
    auto mem = std::move(status).value();
    ASSERT_GE(mem->totalInKB(), mem->usedInKB());
    ASSERT_GE(mem->totalInKB(), mem->freeInKB());
    ASSERT_GE(mem->totalInKB(), mem->bufferInKB());
    ASSERT_EQ(mem->totalInKB(), mem->usedInKB() + mem->freeInKB() + mem->bufferInKB());

    float ratio = static_cast<double>(mem->usedInKB() - 100) / mem->totalInKB();
    ASSERT(mem->hitsHighWatermark(ratio));

    ratio = static_cast<double>(mem->usedInKB() + 100) / mem->totalInKB();
    ASSERT_FALSE(mem->hitsHighWatermark(ratio));
}

}   // namespace nebula
