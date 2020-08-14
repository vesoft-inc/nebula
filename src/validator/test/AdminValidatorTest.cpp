/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

using PK = nebula::graph::PlanNode::Kind;
class AdminValidatorTest : public ValidatorTestBase {
};

TEST_F(AdminValidatorTest, SpaceTest) {
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kDescSpace, PK::kCreateSpace, PK::kStart
        };
        ASSERT_TRUE(checkResult("CREATE SPACE TEST; DESC SPACE TEST;", expected));
    }
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kSwitchSpace, PK::kCreateSpace, PK::kStart
        };
        ASSERT_TRUE(checkResult("CREATE SPACE TEST; USE TEST;", expected));
    }
}

TEST_F(AdminValidatorTest, ShowHosts) {
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kShowHosts, PK::kStart
        };
        ASSERT_TRUE(checkResult("SHOW HOSTS;", expected));
    }
    // chain
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kShowHosts, PK::kDescSpace, PK::kStart
        };
        ASSERT_TRUE(checkResult("DESC SPACE TEST; SHOW HOSTS", expected));
    }
}

}  // namespace graph
}  // namespace nebula
