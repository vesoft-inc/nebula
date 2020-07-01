/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

using PK = nebula::graph::PlanNode::Kind;
class MutateValidatorTest : public ValidatorTestBase {
};

TEST_F(MutateValidatorTest, InsertVertexTest) {
    // wrong schema
    {
        auto cmd = "INSERT VERTEX person(name, age2) VALUES \"A\":(\"a\", 19);";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
}

TEST_F(MutateValidatorTest, InsertEdgeTest) {
    // wrong schema
    {
        auto cmd = "INSERT EDGE like(start, end2) VALUES \"A\"->\"B\":(11, 11);";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
}
}  // namespace graph
}  // namespace nebula
