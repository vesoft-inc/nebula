/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/MatchValidator.h"

#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class MatchValidatorTest : public ValidatorTestBase {};

TEST_F(MatchValidatorTest, SeekByTagIndex) {
    // empty properties index
    {
        std::string query = "MATCH (v:person) RETURN id(v) AS id;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kProject,
                                                // TODO this tag filter could remove in this case
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    // non empty properties index
    {
        std::string query = "MATCH (v:book) RETURN id(v) AS id;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kProject,
                                                // TODO this tag filter could remove in this case
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    // non empty properties index with extend
    {
        std::string query = "MATCH (p:person)-[:like]->(b:book) RETURN b.name AS book;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kInnerJoin,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetNeighbors,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    // non index
    {
        std::string query = "MATCH (v:room) RETURN id(v) AS id;";
        EXPECT_FALSE(validate(query));
    }
}

TEST_F(MatchValidatorTest, SeekByEdgeIndex) {
    // empty properties index
    {
        std::string query = "MATCH (v1)-[:like]->(v2) RETURN id(v1), id(v2);";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kInnerJoin,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kGetNeighbors,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
}

TEST_F(MatchValidatorTest, groupby) {
    {
        std::string query = "MATCH(n:person)"
                            "RETURN id(n) AS id,"
                                   "count(n) AS count,"
                                   "sum(floor(n.age)) AS sum,"
                                   "max(n.age) AS max,"
                                   "min(n.age) AS min,"
                                   "avg(distinct n.age) AS age,"
                                   "labels(n) AS lb;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kAggregate,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "MATCH(n:person)"
                            "RETURN id(n) AS id,"
                                   "count(*) AS count,"
                                   "sum(floor(n.age)) AS sum,"
                                   "max(n.age) AS max,"
                                   "min(n.age) AS min,"
                                   "(INT)avg(distinct n.age)+1 AS age,"
                                   "labels(n) AS lb;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kProject,
                                                PlanNode::Kind::kAggregate,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "MATCH(n:person)"
                            "RETURN id(n) AS id,"
                                   "count(n) AS count,"
                                   "sum(floor(n.age)) AS sum,"
                                   "max(n.age) AS max,"
                                   "min(n.age) AS min,"
                                   "avg(distinct n.age)+1 AS age,"
                                   "labels(n) AS lb "
                                   "ORDER BY id;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kDataCollect,
                                                PlanNode::Kind::kSort,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kAggregate,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "MATCH(n:person)"
                            "RETURN id(n) AS id,"
                                   "count(n) AS count,"
                                   "sum(floor(n.age)) AS sum,"
                                   "max(n.age) AS max,"
                                   "min(n.age) AS min,"
                                   "avg(distinct n.age)+1 AS age,"
                                   "labels(n) AS lb "
                                "ORDER BY id "
                                "SKIP 10 LIMIT 20;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kDataCollect,
                                                PlanNode::Kind::kLimit,
                                                PlanNode::Kind::kSort,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kAggregate,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "MATCH(n:person)-[:like]->(m) "
                            "RETURN id(n) AS id,"
                                   "count(n) AS count,"
                                   "sum(floor(n.age)) AS sum,"
                                   "max(m.age) AS max,"
                                   "min(n.age) AS min,"
                                   "avg(distinct n.age) AS age,"
                                   "labels(m) AS lb;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kAggregate,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kInnerJoin,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetNeighbors,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "MATCH(n:person)-[:like]->(m) "
                            "RETURN id(n) AS id,"
                                   "count(n) AS count,"
                                   "sum(floor(n.age)) AS sum,"
                                   "max(m.age) AS max,"
                                   "min(n.age) AS min,"
                                   "avg(distinct n.age)+1 AS age,"
                                   "labels(m) AS lb;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kProject,
                                                PlanNode::Kind::kAggregate,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kInnerJoin,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetNeighbors,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "MATCH(n:person)-[:like]->(m) "
                            "WHERE n.age > 35 "
                            "RETURN id(n) AS id,"
                                   "count(n) AS count,"
                                   "sum(floor(n.age)) AS sum,"
                                   "max(m.age) AS max,"
                                   "min(n.age) AS min,"
                                   "avg(distinct n.age) AS age,"
                                   "labels(m) AS lb ";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kAggregate,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kInnerJoin,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetNeighbors,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "MATCH(n:person)-[:like]->(m) "
                            "WHERE n.age > 35 "
                            "RETURN id(n) AS id,"
                                   "count(n) AS count,"
                                   "sum(floor(n.age)) AS sum,"
                                   "max(m.age) AS max,"
                                   "min(n.age) AS min,"
                                   "avg(distinct n.age) AS age,"
                                   "labels(m) AS lb "
                                "SKIP 10 LIMIT 20;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kDataCollect,
                                                PlanNode::Kind::kLimit,
                                                PlanNode::Kind::kAggregate,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kInnerJoin,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetNeighbors,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "MATCH(n:person)-[:like]->(m) "
                            "WHERE n.age > 35 "
                            "RETURN DISTINCT id(n) AS id,"
                                   "count(n) AS count,"
                                   "sum(floor(n.age)) AS sum,"
                                   "max(m.age) AS max,"
                                   "min(n.age) AS min,"
                                   "avg(distinct n.age) AS age,"
                                   "labels(m) AS lb;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kDataCollect,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kAggregate,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kInnerJoin,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetNeighbors,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "MATCH(n:person)-[:like]->(m) "
                            "WHERE n.age > 35 "
                            "RETURN id(n) AS id,"
                                   "count(n) AS count,"
                                   "sum(floor(n.age)) AS sum,"
                                   "max(m.age) AS max,"
                                   "min(n.age) AS min,"
                                   "avg(distinct n.age)+1 AS age,"
                                   "labels(m) AS lb "
                                "SKIP 10 LIMIT 20;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kDataCollect,
                                                PlanNode::Kind::kLimit,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kAggregate,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kInnerJoin,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetNeighbors,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "MATCH(n:person)-[:like]->(m) "
                            "WHERE n.age > 35 "
                            "RETURN DISTINCT id(n) AS id,"
                                   "count(n) AS count,"
                                   "sum(floor(n.age)) AS sum,"
                                   "max(m.age) AS max,"
                                   "min(n.age) AS min,"
                                   "avg(distinct n.age)+1 AS age,"
                                   "labels(m) AS lb "
                                "ORDER BY id "
                                "SKIP 10 LIMIT 20;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kDataCollect,
                                                PlanNode::Kind::kLimit,
                                                PlanNode::Kind::kSort,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kAggregate,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kInnerJoin,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetNeighbors,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "MATCH(n:person)-[:like]->(m) "
                            "WHERE n.age > 35 "
                            "RETURN DISTINCT id(n) AS id,"
                                   "count(n) AS count,"
                                   "sum(floor(n.age)) AS sum,"
                                   "max(m.age) AS max,"
                                   "min(n.age) AS min,"
                                   "avg(distinct n.age) AS age,"
                                   "labels(m) AS lb "
                                "ORDER BY id "
                                "SKIP 10 LIMIT 20;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kDataCollect,
                                                PlanNode::Kind::kLimit,
                                                PlanNode::Kind::kSort,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kAggregate,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kInnerJoin,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetNeighbors,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "MATCH(n:person)-[:like]->(m)-[:serve]-() "
                            "WHERE n.age > 35 "
                            "RETURN DISTINCT id(n) AS id,"
                                   "count(n) AS count,"
                                   "sum(floor(n.age)) AS sum,"
                                   "max(m.age) AS max,"
                                   "min(n.age) AS min,"
                                   "(INT)avg(distinct n.age)+1 AS age,"
                                   "labels(m) AS lb "
                                "ORDER BY id "
                                "SKIP 10 LIMIT 20;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kDataCollect,
                                                PlanNode::Kind::kLimit,
                                                PlanNode::Kind::kSort,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kAggregate,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kInnerJoin,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kInnerJoin,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kGetNeighbors,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetNeighbors,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
}

TEST_F(MatchValidatorTest, with) {
    {
        std::string query = "MATCH (v :person{name:\"Tim Duncan\"})-[]-(v2) "
                            "WITH abs(avg(v2.age)) as average_age "
                            "RETURN average_age";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kProject,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kAggregate,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kInnerJoin,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetNeighbors,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
}

TEST_F(MatchValidatorTest, validateAlias) {
    // validate undefined alias in filter
    {
        std::string query = "MATCH (v :person{name:\"Tim Duncan\"})-[e]-(v2) "
                            "WHERE abc._src>0"
                            "RETURN e";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Alias used but not defined: `abc'");
    }
    // validate undefined alias in return clause
    {
        std::string query = "MATCH (v :person{name:\"Tim Duncan\"})-[e]-(v2) "
                            "RETURN abc";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Alias used but not defined: `abc'");
    }
    // invalid prop attribute in filter
    // vertex
    {
        std::string query = "MATCH (v :person{name:\"Tim Duncan\"})-[e]-(v2) "
                            "WHERE v._src>0"
                            "RETURN e";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Vertex `v' does not have the src attribute");
    }
    {
        std::string query = "MATCH (v :person{name:\"Tim Duncan\"})-[e]-(v2) "
                            "WHERE v._dst>0"
                            "RETURN e";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Vertex `v' does not have the dst attribute");
    }
    {
        std::string query = "MATCH (v :person{name:\"Tim Duncan\"})-[e]-(v2) "
                            "WHERE v._rank>0"
                            "RETURN e";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Vertex `v' does not have the ranking attribute");
    }
    {
        std::string query = "MATCH (v :person{name:\"Tim Duncan\"})-[e]-(v2) "
                            "WHERE v._type>0"
                            "RETURN e";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Vertex `v' does not have the type attribute");
    }
    // edge
    {
        std::string query = "MATCH (v :person{name:\"Tim Duncan\"})-[e]-(v2) "
                            "WHERE e._src>0"
                            "RETURN e";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: To get the src vid of the edge, use src(e)");
    }
    {
        std::string query = "MATCH (v :person{name:\"Tim Duncan\"})-[e]-(v2) "
                            "WHERE e._dst>0"
                            "RETURN e";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: To get the dst vid of the edge, use dst(e)");
    }
    {
        std::string query = "MATCH (v :person{name:\"Tim Duncan\"})-[e]-(v2) "
                            "WHERE e._rank>0"
                            "RETURN e";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: To get the ranking of the edge, use rank(e)");
    }
    {
        std::string query = "MATCH (v :person{name:\"Tim Duncan\"})-[e]-(v2) "
                            "WHERE e._type>0"
                            "RETURN e";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: To get the type of the edge, use type(e)");
    }
    // path
    {
        std::string query = "MATCH p = (v :person{name:\"Tim Duncan\"})-[e]-(v2) "
                            "WHERE id(p._src) == \"Tim Duncan\""
                            "RETURN p._src";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: To get the start node of the path, use startNode(p)");
    }
    {
        std::string query = "MATCH p = (v :person{name:\"Tim Duncan\"})-[e]-(v2) "
                            "WHERE id(p._dst) == \"Tim Duncan\""
                            "RETURN p._dst";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: To get the end node of the path, use endNode(p)");
    }
    {
        std::string query = "MATCH p = (v :person{name:\"Tim Duncan\"})-[e]-(v2) "
                            "RETURN p._rank";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Path `p' does not have the ranking attribute");
    }
    {
        std::string query = "MATCH p = (v :person{name:\"Tim Duncan\"})-[e]-(v2) "
                            "RETURN p._type";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Path `p' does not have the type attribute");
    }
    // invalid prop attribute in return clause
    {
        std::string query = "MATCH p = (v :person{name:\"Tim Duncan\"})-[e]-(v2) "
                            "RETURN p._type";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Path `p' does not have the type attribute");
    }
}

}   // namespace graph
}   // namespace nebula
