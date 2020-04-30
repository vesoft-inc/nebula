/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"

#include <gtest/gtest.h>

#include "parser/GQLParser.h"
#include "validator/ASTValidator.h"
#include "service/ExecutionContext.h"
#include "planner/ExecutionPlan.h"

namespace nebula {
namespace graph {
class ValidatorTest : public ::testing::Test {
public:
    void SetUp() override {
        session_ = new ClientSession(0);
        session_->setSpace("test", 0);
        ectx_ = std::make_unique<ExecutionContext>();
        plan_ = std::make_unique<ExecutionPlan>(ectx_.get());
        charsetInfo_ = CharsetInfo::instance();
        // TODO: Need AdHocSchemaManager here.
    }

    void TearDown() override {
        delete session_;
    }

protected:
    ClientSession                      *session_;
    meta::SchemaManager                *schemaMng_;
    std::unique_ptr<ExecutionContext>   ectx_;
    std::unique_ptr<ExecutionPlan>      plan_;
    CharsetInfo*                        charsetInfo_;
};

TEST_F(ValidatorTest, Subgraph) {
    {
        std::string query = "GET SUBGRAPH 3 STEPS FROM 1";
        auto result = GQLParser().parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), session_, schemaMng_, charsetInfo_);
        auto validateResult = validator.validate(plan_.get());
        ASSERT_TRUE(validateResult.ok()) << validateResult;
        // TODO: Check the plan.
    }
}
}  // namespace graph
}  // namespace nebula
