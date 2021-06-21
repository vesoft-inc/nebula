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
        std::vector<PlanNode::Kind> expected = {PK::kDescSpace, PK::kCreateSpace, PK::kStart};
        ASSERT_TRUE(checkResult("CREATE SPACE TEST(vid_type = fixed_string(2)); DESC SPACE TEST;",
                                expected));
    }
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kUpdateSession, PK::kSwitchSpace, PK::kCreateSpace, PK::kStart};
        ASSERT_TRUE(
            checkResult("CREATE SPACE TEST(vid_type = fixed_string(2)); USE TEST;", expected));
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

TEST_F(AdminValidatorTest, TestParts) {
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kShowParts, PK::kStart
        };
        ASSERT_TRUE(checkResult("SHOW PARTS;", expected));
    }
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kShowParts, PK::kShowParts, PK::kStart
        };
        ASSERT_TRUE(checkResult("SHOW PARTS; SHOW PART 3;", expected));
    }
}

TEST_F(AdminValidatorTest, TestSessions) {
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kShowSessions, PK::kStart
        };
        ASSERT_TRUE(checkResult("SHOW SESSIONS;", expected));
    }
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kShowSessions, PK::kStart
        };
        ASSERT_TRUE(checkResult("SHOW SESSION 1;", expected));
    }
}

}  // namespace graph
}  // namespace nebula
