/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GroupByValidator.h"

#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class GroupByValidatorTest : public ValidatorTestBase {
public:
};

using PK = nebula::graph::PlanNode::Kind;

TEST_F(GroupByValidatorTest, TestGroupBy) {
    {
        std::string query =
            "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
            "| GROUP BY $-.age YIELD $-.age, COUNT($-.id)";
        std::vector<PlanNode::Kind> expected = {
            PK::kAggregate,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query =
            "GO FROM \"NoExist\" OVER like YIELD like._dst AS id, $^.person.age AS age "
            "| GROUP BY $-.id YIELD $-.id AS id";
        std::vector<PlanNode::Kind> expected = {
            PK::kAggregate,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query =
            "GO FROM \"NoExist\" OVER like YIELD like._dst AS id, $^.person.age AS age "
            "| GROUP BY $-.id YIELD $-.id AS id, abs(avg($-.age)) AS age";
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kAggregate,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "GO FROM \"1\", \"2\" OVER like "
                            "YIELD $$.person.name as name, "
                            "$$.person.age AS dst_age, "
                            "$$.person.age AS src_age"
                            "| GROUP BY $-.name "
                            "YIELD $-.name AS name, "
                            "SUM($-.dst_age) AS sum_dst_age, "
                            "AVG($-.dst_age) AS avg_dst_age, "
                            "MAX($-.src_age) AS max_src_age, "
                            "MIN($-.src_age) AS min_src_age, "
                            "STD($-.src_age) AS std_src_age, "
                            "BIT_AND(1) AS bit_and, "
                            "BIT_OR(2) AS bit_or, "
                            "BIT_XOR(3) AS bit_xor";
        std::vector<PlanNode::Kind> expected = {
            PK::kAggregate,
            PK::kProject,
            PK::kLeftJoin,
            PK::kProject,
            PK::kGetVertices,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "GO FROM \"1\", \"2\" OVER like "
                            "YIELD $$.person.name as name, "
                            "$$.person.age AS dst_age, "
                            "$$.person.age AS src_age"
                            "| GROUP BY $-.name "
                            "YIELD $-.name AS name, "
                            "SUM($-.dst_age) AS sum_dst_age, "
                            "abs(AVG($-.dst_age)) AS avg_dst_age, "
                            "MAX($-.src_age) AS max_src_age, "
                            "MIN($-.src_age) AS min_src_age, "
                            "STD($-.src_age) AS std_src_age, "
                            "BIT_AND(1) AS bit_and, "
                            "BIT_OR(2) AS bit_or, "
                            "BIT_XOR(3) AS bit_xor";
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kAggregate,
            PK::kProject,
            PK::kLeftJoin,
            PK::kProject,
            PK::kGetVertices,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        // group one col
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $$.person.name as name, "
                            "like._dst AS id, "
                            "like.start AS start_year, "
                            "like.end AS end_year "
                            "| GROUP BY $-.start_year "
                            "YIELD COUNT($-.id), "
                            "$-.start_year AS start_year, "
                            "AVG($-.end_year) AS avg";
        std::vector<PlanNode::Kind> expected = {
            PK::kAggregate,
            PK::kProject,
            PK::kLeftJoin,
            PK::kProject,
            PK::kGetVertices,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        // group has fun col
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $$.person.name as name, "
                            "like._dst AS id, "
                            "like.start AS start_year, "
                            "like.end AS end_year"
                            "| GROUP BY $-.name, abs($-.end_year) "
                            "YIELD $-.name AS name, "
                            "SUM(1.5) AS sum, "
                            "COUNT(*) AS count, "
                            "1+1 AS cal";
        std::vector<PlanNode::Kind> expected = {
            PK::kAggregate,
            PK::kProject,
            PK::kLeftJoin,
            PK::kProject,
            PK::kGetVertices,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        // group has fun col
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $$.person.name as name, "
                            "like._dst AS id, "
                            "like.start AS start_year, "
                            "like.end AS end_year"
                            "| GROUP BY $-.name, $-.id "
                            "YIELD $-.name AS name, "
                            "SUM(1.5) AS sum, "
                            "COUNT(*) AS count, "
                            "1+1 AS cal";
        std::vector<PlanNode::Kind> expected = {
            PK::kAggregate,
            PK::kProject,
            PK::kLeftJoin,
            PK::kProject,
            PK::kGetVertices,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
}

TEST_F(GroupByValidatorTest, VariableTest) {
    {
        std::string query =
            "$a = GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age; "
            "GROUP BY $a.age YIELD $a.age, COUNT($a.id)";
        std::vector<PlanNode::Kind> expected = {
            PK::kAggregate, PK::kProject, PK::kGetNeighbors, PK::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        // group has fun col
        std::string query = "$a = GO FROM \"1\" OVER like "
                            "YIELD $$.person.name as name, "
                            "like._dst AS id, "
                            "like.start AS start_year, "
                            "like.end AS end_year;"
                            "GROUP BY $a.name, $a.id "
                            "YIELD $a.name AS name, "
                            "SUM(1.5) AS sum, "
                            "COUNT(*) AS count, "
                            "1+1 AS cal";
        std::vector<PlanNode::Kind> expected = {
            PK::kAggregate,
            PK::kProject,
            PK::kLeftJoin,
            PK::kProject,
            PK::kGetVertices,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        // group has fun col
        std::string query = "$a = GO FROM \"1\" OVER like "
                            "YIELD $$.person.name as name, "
                            "like._dst AS id, "
                            "like.start AS start_year, "
                            "like.end AS end_year;"
                            "GROUP BY $a.name, abs($a.end_year) "
                            "YIELD $a.name AS name, "
                            "SUM(1.5) AS sum, "
                            "COUNT(*) AS count, "
                            "1+1 AS cal";
        std::vector<PlanNode::Kind> expected = {
            PK::kAggregate,
            PK::kProject,
            PK::kLeftJoin,
            PK::kProject,
            PK::kGetVertices,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
}


TEST_F(GroupByValidatorTest, InvalidTest) {
    {
        // use groupby without input
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY 1+1 YIELD COUNT(1), 1+1";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SemanticError: Group `(1+1)' invalid");
    }
    {
        // use groupby without input
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY count(*)+1 YIELD COUNT(1), 1+1";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SemanticError: Group `(count(*)+1)' invalid");
    }
    {
        // use groupby without input
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY abs(1)+1 YIELD COUNT(1), 1+1";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SemanticError: Group `(abs(1)+1)' invalid");
    }
    {
        // use dst
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY $-.age YIELD COUNT($$.person.name)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Only support input and variable in GroupBy sentence.");
    }
    {
        // group input noexist
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY $-.start_year YIELD COUNT($-.age)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: `$-.start_year', not exist prop `start_year'");
    }
    {
        // group name noexist
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY noexist YIELD COUNT($-.age)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                   "SemanticError: Group `noexist' invalid");
    }
    {
        // TODO: move to parser UT
        // use sum(*)
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY $-.id YIELD SUM(*)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SyntaxError: Could not apply aggregation function on `*` near `SUM'");
    }
    {
        // TODO: move to parser UT
        // use agg fun has more than two inputs
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY $-.id YIELD COUNT($-.id, $-.age)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SyntaxError: Unknown function  near `COUNT'");
    }
    {
        // group col has agg fun
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age "
                            "| GROUP BY $-.id, SUM($-.age) YIELD $-.id, SUM($-.age)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SemanticError: Group `SUM($-.age)' invalid");
    }
    {
        // yield without group by
        std::string query = "GO FROM \"1\" OVER like YIELD $^.person.age AS age, "
                            "COUNT(like._dst) AS id ";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: `COUNT(like._dst) AS id', "
                  "not support aggregate function in go sentence.");
    }
    {
        // yield without group by
        std::string query = "GO FROM \"1\" OVER like YIELD $^.person.age AS age, "
                            "COUNT(like._dst)+1 AS id ";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: `(COUNT(like._dst)+1) AS id', "
                  "not support aggregate function in go sentence.");
    }
    {
        // yield without group by
        std::string query = "GO FROM \"1\" OVER like WHERE count(*) + 1 >3 "
                            "YIELD $^.person.age AS age, "
                            "COUNT(like._dst)+1 AS id ";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: `((count(*)+1)>3)', "
                  "not support aggregate function in where sentence.");
    }
    {
        // yield col not in group output
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $$.person.name as name, "
                            "like._dst AS id, "
                            "like.start AS start_year, "
                            "like.end AS end_year"
                            "| GROUP BY $-.start_year, abs($-.end_year) "
                            "YIELD $-.name AS name, "
                            "SUM(1.5) AS sum, "
                            "COUNT(*) AS count, "
                            "1+1 AS cal";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Yield non-agg expression `$-.name' "
                  "must be functionally dependent on items in GROUP BY clause");
    }
    {
        // duplicate col
        std::string query =
            "GO FROM \"1\" OVER like YIELD $$.person.age AS age, $^.person.age AS age"
            "| GROUP BY $-.age YIELD $-.age, 1+1";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Duplicate Column Name : `age'");
    }
    {
        // duplicate col
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $$.person.age AS age, $^.person.age AS age, like._dst AS id "
                            "| GROUP BY $-.id YIELD $-.id, COUNT($-.age)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Duplicate Column Name : `age'");
    }

    // {
    //     // todo(jmq) not support $-.*
    //     std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age
    //                         "
    //                         "| GROUP BY $-.id YIELD  COUNT($-.*)";
    //     auto result = checkResult(query);
    //     EXPECT_EQ(std::string(result.message()), "SemanticError: Use invalid group function
    //     `SUM`");
    // }
    // {
    //     //  todo(jmq) not support $-.*
    //     std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id, $^.person.age AS age
    //                         "
    //                         "| GROUP BY $-.* YIELD  $-.*";
    //     auto result = checkResult(query);
    //     EXPECT_EQ(std::string(result.message()), "SemanticError: Use invalid group function
    //     `SUM`");
    // }
}

}  // namespace graph
}  // namespace nebula
