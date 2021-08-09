/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "utils/OperationKeyUtils.h"

namespace nebula {

TEST(OperationKeyUtilsTest, ModifyKeyTest) {
    PartitionID part = 1;
    auto opKey = OperationKeyUtils::modifyOperationKey(part, "modify key");
    ASSERT_TRUE(OperationKeyUtils::isModifyOperation(opKey));
    ASSERT_EQ(OperationKeyUtils::getOperationKey(opKey), "modify key");
}

TEST(OperationKeyUtilsTest, DeleteKeyTest) {
    PartitionID part = 1;
    auto opKey = OperationKeyUtils::deleteOperationKey(part);
    ASSERT_TRUE(OperationKeyUtils::isDeleteOperation(opKey));
}

}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
