/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"

#include <gtest/gtest.h>

#include "parser/GQLParser.h"
#include "validator/ASTValidator.h"
#include "context/QueryContext.h"
#include "planner/ExecutionPlan.h"
#include "context/ValidateContext.h"

namespace nebula {
namespace graph {
class ValidatorTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        auto session = new ClientSession(0);
        session->setSpace("test", 0);
        session_.reset(session);
        // TODO: Need AdHocSchemaManager here.
    }

    void SetUp() override {
    }

    void TearDown() override {
    }

    std::unique_ptr<QueryContext> buildContext();

protected:
    static std::shared_ptr<ClientSession>      session_;
    static meta::SchemaManager*                schemaMng_;
};
std::shared_ptr<ClientSession> ValidatorTest::session_;
meta::SchemaManager* ValidatorTest::schemaMng_;

std::unique_ptr<QueryContext> ValidatorTest::buildContext() {
    auto rctx = std::make_unique<RequestContext<cpp2::ExecutionResponse>>();
    rctx->setSession(session_);
    auto qctx = std::make_unique<QueryContext>();
    qctx->setRctx(std::move(rctx));
    qctx->setSchemaManager(schemaMng_);
    qctx->setCharsetInfo(CharsetInfo::instance());
    return qctx;
}

TEST_F(ValidatorTest, Subgraph) {
    {
        std::string query = "GET SUBGRAPH 3 STEPS FROM \"1\"";
        auto result = GQLParser().parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto sentences = std::move(result).value();
        auto context = buildContext();
        ASTValidator validator(sentences.get(), context.get());
        auto validateResult = validator.validate();
        ASSERT_TRUE(validateResult.ok()) << validateResult;
        // TODO: Check the plan.
        auto plan = context->plan();
        ASSERT_NE(plan, nullptr);
    }
}
}  // namespace graph
}  // namespace nebula
