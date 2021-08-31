/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/planner/plan/Admin.h"
#include "graph/validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class BalanceValidatorTest : public ValidatorTestBase {
 protected:
  const PlanNode *getPlanRoot(const std::string query) {
    auto qctxStatus = validate(query);
    EXPECT_TRUE(qctxStatus.ok()) << qctxStatus.status();
    auto qctx = std::move(qctxStatus).value();
    return qctx->plan()->root();
  }
};

TEST_F(BalanceValidatorTest, Simple) {
  {
    auto root = getPlanRoot(folly::stringPrintf("BALANCE Leader"));
    std::vector<PlanNode::Kind> expected{
        PlanNode::Kind::kBalanceLeaders,
        PlanNode::Kind::kStart,
    };
    ASSERT_TRUE(verifyPlan(root, expected));
  }
  {
    auto root = getPlanRoot(folly::stringPrintf("BALANCE DATA"));
    std::vector<PlanNode::Kind> expected{
        PlanNode::Kind::kBalance,
        PlanNode::Kind::kStart,
    };
    ASSERT_TRUE(verifyPlan(root, expected));
  }
  {
    auto root = getPlanRoot(folly::stringPrintf("BALANCE DATA 12345678"));
    std::vector<PlanNode::Kind> expected{
        PlanNode::Kind::kShowBalance,
        PlanNode::Kind::kStart,
    };
    ASSERT_TRUE(verifyPlan(root, expected));
  }
  {
    auto root = getPlanRoot(folly::stringPrintf("BALANCE DATA STOP"));
    std::vector<PlanNode::Kind> expected{
        PlanNode::Kind::kStopBalance,
        PlanNode::Kind::kStart,
    };
    ASSERT_TRUE(verifyPlan(root, expected));
  }
  {
    auto root = getPlanRoot(folly::stringPrintf("BALANCE DATA RESET PLAN"));
    std::vector<PlanNode::Kind> expected{
        PlanNode::Kind::kResetBalance,
        PlanNode::Kind::kStart,
    };
    ASSERT_TRUE(verifyPlan(root, expected));
  }
  {
    auto root = getPlanRoot(folly::stringPrintf("BALANCE DATA REMOVE 127.0.0.1:8989"));
    std::vector<PlanNode::Kind> expected{
        PlanNode::Kind::kBalance,
        PlanNode::Kind::kStart,
    };
    ASSERT_TRUE(verifyPlan(root, expected));
  }
  {
    auto root = getPlanRoot(folly::stringPrintf("BALANCE DISK ATTACH 127.0.0.1:8989 \"/data\""));
    std::vector<PlanNode::Kind> expected{
        PlanNode::Kind::kBalanceDiskAttach,
        PlanNode::Kind::kStart,
    };
    ASSERT_TRUE(verifyPlan(root, expected));
  }
  {
    auto root = getPlanRoot(
        folly::stringPrintf("BALANCE DISK ATTACH 127.0.0.1:8989 \"/data.0\",\"/data.1\""));
    std::vector<PlanNode::Kind> expected{
        PlanNode::Kind::kBalanceDiskAttach,
        PlanNode::Kind::kStart,
    };
    ASSERT_TRUE(verifyPlan(root, expected));
  }
  {
    auto root = getPlanRoot(folly::stringPrintf("BALANCE DISK REMOVE 127.0.0.1:8989 \"/data\""));
    std::vector<PlanNode::Kind> expected{
        PlanNode::Kind::kBalanceDiskRemove,
        PlanNode::Kind::kStart,
    };
    ASSERT_TRUE(verifyPlan(root, expected));
  }
  {
    auto root = getPlanRoot(
        folly::stringPrintf("BALANCE DISK REMOVE 127.0.0.1:8989 \"/data.0\",\"/data.1\""));
    std::vector<PlanNode::Kind> expected{
        PlanNode::Kind::kBalanceDiskRemove,
        PlanNode::Kind::kStart,
    };
    ASSERT_TRUE(verifyPlan(root, expected));
  }
}

}  // namespace graph
}  // namespace nebula
