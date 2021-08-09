/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/ObjectPool.h"
#include "planner/plan/Logic.h"
#include "planner/plan/Query.h"
#include "validator/LookupValidator.h"
#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class LookupValidatorTest : public ValidatorTestBase {};

TEST_F(LookupValidatorTest, InputOutput) {
    // pipe
    {
        const std::string query = "LOOKUP ON person where person.age == 35 | "
                                  "FETCH PROP ON person $-.VertexID";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kFilter,
                                    PlanNode::Kind::kTagIndexFullScan,
                                    PlanNode::Kind::kStart,
                                }));
    }
    // pipe with yield
    {
        const std::string query =
            "LOOKUP ON person where person.age == 35 YIELD person.name AS name | "
            "FETCH PROP ON person $-.name";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kFilter,
                                    PlanNode::Kind::kTagIndexFullScan,
                                    PlanNode::Kind::kStart,
                                }));
    }
    // variable
    {
        const std::string query = "$a = LOOKUP ON person where person.age == 35; "
                                  "FETCH PROP ON person $a.VertexID";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kFilter,
                                    PlanNode::Kind::kTagIndexFullScan,
                                    PlanNode::Kind::kStart,
                                }));
    }
    // var with yield
    {
        const std::string query =
            "$a = LOOKUP ON person where person.age == 35 YIELD person.name AS name;"
            "FETCH PROP ON person $a.name";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kFilter,
                                    PlanNode::Kind::kTagIndexFullScan,
                                    PlanNode::Kind::kStart,
                                }));
    }
}

TEST_F(LookupValidatorTest, InvalidYieldExpression) {
    // TODO(shylock)
    {
        const std::string query =
            "LOOKUP ON person where person.age == 35 YIELD person.age + 1 AS age;";
        EXPECT_FALSE(checkResult(query,
                                 {
                                     PlanNode::Kind::kProject,
                                     PlanNode::Kind::kFilter,
                                     PlanNode::Kind::kTagIndexFullScan,
                                     PlanNode::Kind::kStart,
                                 }));
    }
}

TEST_F(LookupValidatorTest, InvalidFilterExpression) {
    {
        const std::string query =
            "LOOKUP ON person where person.age == person.name;";
        EXPECT_FALSE(checkResult(query, {}));
    }
    {
        const std::string query =
            "LOOKUP ON person where person.age > person.name;";
        EXPECT_FALSE(checkResult(query, {}));
    }
    {
        const std::string query =
            "LOOKUP ON person where person.age != person.name;";
        EXPECT_FALSE(checkResult(query, {}));
    }
    {
        const std::string query =
            "LOOKUP ON person where person.age + 1 > 5;";
        EXPECT_FALSE(checkResult(query, {}));
    }
    {
        const std::string query =
            "LOOKUP ON person where person.age > person.name + 5;";
        EXPECT_FALSE(checkResult(query, {}));
    }
    {
        const std::string query =
            "LOOKUP ON person where  1 + 5 < person.age;";
        EXPECT_TRUE(checkResult(query, {}));
    }
    {
        const std::string query =
            "LOOKUP ON person where person.age > 1 + 5;";
        EXPECT_TRUE(checkResult(query, {}));
    }
    {
        const std::string query =
            "LOOKUP ON person where person.age > abs(-5);";
        EXPECT_TRUE(checkResult(query, {}));
    }
}
}   // namespace graph
}   // namespace nebula
