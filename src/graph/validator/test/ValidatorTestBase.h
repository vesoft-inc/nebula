/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _VALIDATOR_TEST_VALIDATOR_TEST_BASE_H_
#define _VALIDATOR_TEST_VALIDATOR_TEST_BASE_H_

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "context/QueryContext.h"
#include "context/ValidateContext.h"
#include "parser/GQLParser.h"
#include "planner/plan/ExecutionPlan.h"
#include "planner/plan/Logic.h"
#include "planner/plan/PlanNode.h"
#include "planner/PlannersRegister.h"
#include "planner/plan/Query.h"
#include "common/base/ObjectPool.h"
#include "validator/Validator.h"
#include "validator/test/MockSchemaManager.h"
#include "validator/test/MockIndexManager.h"
#include "util/AstUtils.h"

namespace nebula {
namespace graph {

class ValidatorTestBase : public ::testing::Test {
protected:
    void SetUp() override {
        meta::cpp2::Session session;
        session.set_session_id(0);
        session.set_user_name("root");
        session_ = ClientSession::create(std::move(session), nullptr);
        SpaceInfo spaceInfo;
        spaceInfo.name = "test_space";
        spaceInfo.id = 1;
        spaceInfo.spaceDesc.set_space_name("test_space");
        session_->setSpace(std::move(spaceInfo));
        schemaMng_ = CHECK_NOTNULL(MockSchemaManager::makeUnique());
        indexMng_ = CHECK_NOTNULL(MockIndexManager::makeUnique());
        pool_ = std::make_unique<ObjectPool>();
        PlannersRegister::registPlanners();
    }

    StatusOr<QueryContext*> validate(const std::string& query) {
        VLOG(1) << "query: " << query;
        auto qctx = buildContext();
        auto result = GQLParser(qctx).parse(query);
        if (!result.ok()) {
            return std::move(result).status();
        }
        NG_RETURN_IF_ERROR(AstUtils::reprAstCheck(*result.value(), qctx));

        auto sentences = pool_->add(std::move(result).value().release());
        NG_RETURN_IF_ERROR(Validator::validate(sentences, qctx));
        return qctx;
    }

    QueryContext* buildContext() {
        auto rctx = std::make_unique<RequestContext<ExecutionResponse>>();
        rctx->setSession(session_);
        auto qctx = pool_->add(new QueryContext());
        qctx->setRCtx(std::move(rctx));
        qctx->setSchemaManager(schemaMng_.get());
        qctx->setIndexManager(indexMng_.get());
        qctx->setCharsetInfo(CharsetInfo::instance());
        return qctx;
    }

    static Status EqSelf(const PlanNode* l, const PlanNode* r);

    static Status Eq(const PlanNode *l, const PlanNode *r);

    ::testing::AssertionResult checkResult(const std::string& query,
                                           const std::vector<PlanNode::Kind>& expected = {},
                                           const std::vector<std::string> &rootColumns = {}) {
        auto status = validate(query);
        if (!status) {
            return ::testing::AssertionFailure() << std::move(status).status().toString();
        }
        auto qctx = std::move(status).value();
        if (qctx == nullptr) {
            return ::testing::AssertionFailure() << "qctx is nullptr";
        }
        if (expected.empty()) {
            return ::testing::AssertionSuccess();
        }
        auto plan = qctx->plan();
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
    std::shared_ptr<ClientSession>        session_;
    std::unique_ptr<MockSchemaManager>    schemaMng_;
    std::unique_ptr<MockIndexManager>     indexMng_{nullptr};
    std::unique_ptr<Sentence>             sentences_;
    std::unique_ptr<ObjectPool>           pool_;
};

std::ostream& operator<<(std::ostream& os, const std::vector<PlanNode::Kind>& plan);

}   // namespace graph
}   // namespace nebula

#endif   // VALIDATOR_TEST_VALIDATORTESTBASE_H_
