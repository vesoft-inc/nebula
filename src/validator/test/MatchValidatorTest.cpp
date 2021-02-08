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
                                                PlanNode::Kind::kDataJoin,
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
                                                PlanNode::Kind::kDataJoin,
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
                                                PlanNode::Kind::kDataJoin,
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
                                                PlanNode::Kind::kDataJoin,
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
                                                PlanNode::Kind::kDataJoin,
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
                                                PlanNode::Kind::kDataJoin,
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
                                                PlanNode::Kind::kDataJoin,
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
                                                PlanNode::Kind::kDataJoin,
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
                                                PlanNode::Kind::kDataJoin,
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
                                                PlanNode::Kind::kDataJoin,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kDataJoin,
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

}   // namespace graph
}   // namespace nebula
