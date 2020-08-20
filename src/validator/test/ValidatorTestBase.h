/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _VALIDATOR_TEST_VALIDATOR_TEST_BASE_H_
#define _VALIDATOR_TEST_VALIDATOR_TEST_BASE_H_

#include "common/base/Base.h"

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "context/QueryContext.h"
#include "context/ValidateContext.h"
#include "parser/GQLParser.h"
#include "planner/ExecutionPlan.h"
#include "planner/Logic.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"
#include "util/ObjectPool.h"
#include "validator/Validator.h"
#include "validator/test/MockSchemaManager.h"

namespace nebula {
namespace graph {

class ValidatorTestBase : public ::testing::Test {
protected:
    void SetUp() override {
        session_ = Session::create(0);
        session_->setSpace("test_space", 1);
        schemaMng_ = CHECK_NOTNULL(MockSchemaManager::makeUnique());
        pool_ = std::make_unique<ObjectPool>();
    }

    ExecutionPlan* toPlan(const std::string& query) {
        auto planStatus = validate(query);
        EXPECT_TRUE(planStatus);
        return std::move(planStatus).value();
    }

    StatusOr<ExecutionPlan*> validate(const std::string& query) {
        VLOG(1) << "query: " << query;
        auto result = GQLParser().parse(query);
        if (!result.ok()) {
            return std::move(result).status();
        }
        auto sentences = pool_->add(std::move(result).value().release());
        auto qctx = buildContext();
        NG_RETURN_IF_ERROR(Validator::validate(sentences, qctx));
        return qctx->plan();
    }

    QueryContext* buildContext() {
        auto rctx = std::make_unique<RequestContext<cpp2::ExecutionResponse>>();
        rctx->setSession(session_);
        auto qctx = pool_->add(new QueryContext());
        qctx->setRctx(std::move(rctx));
        qctx->setSchemaManager(schemaMng_.get());
        qctx->setCharsetInfo(CharsetInfo::instance());
        return qctx;
    }

    static Status EqSelf(const PlanNode* l, const PlanNode* r);

    static Status Eq(const PlanNode *l, const PlanNode *r);

    ::testing::AssertionResult checkResult(const std::string& query,
                                           const std::vector<PlanNode::Kind>& expected = {},
                                           const std::vector<std::string> &rootColumns = {}) {
        auto planStatus = validate(query);
        if (!planStatus) {
            return ::testing::AssertionFailure() << std::move(planStatus).status().toString();
        }
        auto plan = std::move(planStatus).value();
        if (plan == nullptr) {
            return ::testing::AssertionFailure() << "plan is nullptr";
        }
        if (expected.empty()) {
            return ::testing::AssertionSuccess();
        }
        auto assertResult = verifyPlan(plan->root(), expected);
        if (!assertResult) {
            return assertResult;
        }
        if (rootColumns.empty()) {
            return ::testing::AssertionSuccess();
        }
        auto outputColumns = plan->root()->colNames();
        if (outputColumns == rootColumns) {
            return ::testing::AssertionSuccess();
        }
        return ::testing::AssertionFailure()
               << "Columns of root plan node are different: " << folly::join(",", outputColumns)
               << " vs. " << folly::join(",", rootColumns);
    }

    static ::testing::AssertionResult verifyPlan(const PlanNode* root,
                                                 const std::vector<PlanNode::Kind>& expected) {
        if (root == nullptr) {
            return ::testing::AssertionFailure() << "Get nullptr plan.";
        }

        std::vector<PlanNode::Kind> result;
        bfsTraverse(root, result);
        if (result == expected) {
            return ::testing::AssertionSuccess();
        }
        return ::testing::AssertionFailure()
               << "\n\tResult: " << result << "\n\tExpected: " << expected;
    }

    static void bfsTraverse(const PlanNode* root, std::vector<PlanNode::Kind>& result);

protected:
    std::shared_ptr<Session>              session_;
    std::unique_ptr<MockSchemaManager>    schemaMng_;
    std::unique_ptr<Sentence>             sentences_;
    std::unique_ptr<ObjectPool>           pool_;
};

std::ostream& operator<<(std::ostream& os, const std::vector<PlanNode::Kind>& plan);

}   // namespace graph
}   // namespace nebula

#endif   // VALIDATOR_TEST_VALIDATORTESTBASE_H_
