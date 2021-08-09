/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

using PK = nebula::graph::PlanNode::Kind;
class MaintainValidatorTest : public ValidatorTestBase {
};

TEST_F(MaintainValidatorTest, SpaceTest) {
    std::vector<PlanNode::Kind> expected = {PK::kDescSpace, PK::kCreateSpace, PK::kStart};
    ASSERT_TRUE(
        checkResult("CREATE SPACE TEST(vid_type = fixed_string(2)); DESC SPACE TEST;", expected));
}

TEST_F(MaintainValidatorTest, TagTest) {
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kDescTag, PK::kCreateTag, PK::kStart
        };
        ASSERT_TRUE(checkResult("CREATE TAG TEST(); DESC TAG TEST;", expected));
    }
    // the same name schema
    {
        ASSERT_FALSE(checkResult("CREATE TAG TEST();CREATE TAG TEST()", {}));
    }
}

TEST_F(MaintainValidatorTest, EdgeTest) {
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kDescEdge, PK::kCreateEdge, PK::kStart
        };
        ASSERT_TRUE(checkResult("CREATE EDGE TEST(); DESC EDGE TEST;", expected));
    }
    // the same name schema
    {
        ASSERT_FALSE(checkResult("CREATE EDGE TEST();CREATE EDGE TEST()", {}));
    }
}
}  // namespace graph
}  // namespace nebula
