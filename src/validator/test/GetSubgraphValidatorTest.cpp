/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"

#include "validator/test/ValidatorTestBase.h"

DECLARE_uint32(max_allowed_statements);

namespace nebula {
namespace graph {

class GetSubgraphValidatorTest : public ValidatorTestBase {
public:
};

using PK = nebula::graph::PlanNode::Kind;

TEST_F(GetSubgraphValidatorTest, Base) {
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kLoop,
            PK::kStart,
            PK::kDedup,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult("GET SUBGRAPH FROM \"1\"", expected));
    }
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kLoop,
            PK::kStart,
            PK::kDedup,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult("GET SUBGRAPH FROM \"1\" BOTH like", expected));
    }
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kLoop,
            PK::kStart,
            PK::kDedup,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult("GET SUBGRAPH 3 STEPS FROM \"1\"", expected));
    }
}

TEST_F(GetSubgraphValidatorTest, Input) {
    {
        EXPECT_TRUE(checkResult("GO FROM \"1\" OVER like YIELD like._src AS src"
                                " | GET SUBGRAPH FROM $-.src",
                                {
                                    PK::kDataCollect,
                                    PK::kLoop,
                                    PK::kProject,
                                    PK::kDedup,
                                    PK::kGetNeighbors,
                                    PK::kProject,
                                    PK::kStart,
                                    PK::kGetNeighbors,
                                    PK::kStart,
                                }));
    }
    {
        EXPECT_TRUE(checkResult("$a = GO FROM \"1\" OVER like YIELD like._src AS src;"
                                "GET SUBGRAPH FROM $a.src",
                                {
                                    PK::kDataCollect,
                                    PK::kLoop,
                                    PK::kProject,
                                    PK::kDedup,
                                    PK::kGetNeighbors,
                                    PK::kProject,
                                    PK::kStart,
                                    PK::kGetNeighbors,
                                    PK::kStart,
                                }));
    }
}

TEST_F(GetSubgraphValidatorTest, RefNotExist) {
    {
        EXPECT_FALSE(checkResult("$a = GO FROM \"1\" OVER like YIELD like._src AS src;"
                                "GET SUBGRAPH FROM $b.src"));
    }
    {
        EXPECT_FALSE(checkResult("GET SUBGRAPH FROM $-.src"));
    }
}

}  // namespace graph
}  // namespace nebula
