/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/base/Base.h"
#include "graph/validator/test/ValidatorTestBase.h"

DECLARE_uint32(max_allowed_statements);

namespace nebula {
namespace graph {

class FindPathValidatorTest : public ValidatorTestBase {
 public:
};

using PK = nebula::graph::PlanNode::Kind;

TEST_F(FindPathValidatorTest, invalidYield) {
  {
    std::string query = "FIND SHORTEST PATH  FROM \"Tim\" TO \"Tony\" OVER *";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()), "SemanticError: Missing yield clause.");
  }
  {
    std::string query = "FIND SHORTEST PATH  FROM \"Tim\" TO \"Tony\" OVER * YIELD vertex";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SyntaxError: please add alias when using `vertex'. near `vertex'");
  }
  {
    std::string query =
        "FIND ALL PATH WITH PROP FROM \"Tim\" TO \"Tony\" OVER like YIELD edge as e";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SemanticError: Illegal yield clauses `EDGE AS e'. only support yield path");
  }
  {
    std::string query =
        "FIND NOLOOP PATH WITH PROP FROM \"Tim\" TO \"Yao\" OVER teammate YIELD path";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SyntaxError: please add alias when using `path'. near `path'");
  }
  {
    std::string query =
        "FIND NOLOOP PATH WITH PROP FROM \"Tim\" TO \"Yao\" OVER * YIELD "
        "$$.player.name";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SemanticError: Illegal yield clauses `$$.player.name'. only support yield path");
  }
}

TEST_F(FindPathValidatorTest, SinglePairPath) {
  {
    std::string query =
        "FIND SHORTEST PATH FROM \"1\" TO \"2\" OVER like UPTO 5 STEPS YIELD path as p";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kStart,
        PK::kBFSShortest,
        PK::kGetNeighbors,
        PK::kGetNeighbors,
        PK::kPassThrough,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "FIND SHORTEST PATH FROM \"1\" TO \"2\" OVER like, serve UPTO 5 STEPS YIELD path as p";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kStart,
        PK::kBFSShortest,
        PK::kGetNeighbors,
        PK::kGetNeighbors,
        PK::kPassThrough,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(FindPathValidatorTest, MultiPairPath) {
  {
    std::string query =
        "FIND SHORTEST PATH FROM \"1\" TO \"2\",\"3\" OVER like UPTO 5 STEPS YIELD path as p";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kProject,
        PK::kMultiShortestPath,
        PK::kProject,
        PK::kGetNeighbors,
        PK::kGetNeighbors,
        PK::kStart,
        PK::kPassThrough,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "FIND SHORTEST PATH FROM \"1\",\"2\" TO \"3\",\"4\" OVER like UPTO 5 "
        "STEPS YIELD path as p";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kProject,
        PK::kMultiShortestPath,
        PK::kProject,
        PK::kGetNeighbors,
        PK::kGetNeighbors,
        PK::kStart,
        PK::kPassThrough,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(FindPathValidatorTest, ALLPath) {
  {
    std::string query = "FIND ALL PATH FROM \"1\" TO \"2\" OVER like UPTO 5 STEPS YIELD path as p";
    std::vector<PlanNode::Kind> expected = {
        PK::kProject,
        PK::kAllPaths,
        PK::kDedup,
        PK::kDedup,
        PK::kPassThrough,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "FIND ALL PATH FROM \"1\" TO \"2\",\"3\" OVER like UPTO 5 STEPS YIELD path as p";
    std::vector<PlanNode::Kind> expected = {
        PK::kProject,
        PK::kAllPaths,
        PK::kDedup,
        PK::kDedup,
        PK::kPassThrough,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(FindPathValidatorTest, RunTimePath) {
  {
    std::string query =
        "GO FROM \"1\" OVER like YIELD like._src AS src, like._dst AS dst "
        " | FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like, serve UPTO 5 "
        "STEPS YIELD path as p";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kProject,
        PK::kMultiShortestPath,
        PK::kProject,
        PK::kGetNeighbors,
        PK::kGetNeighbors,
        PK::kDedup,
        PK::kPassThrough,
        PK::kProject,
        PK::kStart,
        PK::kDedup,
        PK::kProject,
        PK::kProject,
        PK::kExpandAll,
        PK::kExpand,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO FROM \"1\" OVER like YIELD like._src AS src, like._dst AS dst "
        " | FIND ALL PATH FROM $-.src TO $-.dst OVER like, serve UPTO 5 STEPS YIELD path as p";
    std::vector<PlanNode::Kind> expected = {
        PK::kProject,
        PK::kAllPaths,
        PK::kDedup,
        PK::kDedup,
        PK::kProject,
        PK::kProject,
        PK::kPassThrough,
        PK::kProject,
        PK::kExpandAll,
        PK::kExpand,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO FROM \"1\" OVER like YIELD like._src AS src, like._dst AS dst "
        " | FIND SHORTEST PATH FROM \"2\" TO $-.dst OVER like, serve UPTO 5 "
        "STEPS YIELD path as p";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kProject,
        PK::kMultiShortestPath,
        PK::kProject,
        PK::kGetNeighbors,
        PK::kGetNeighbors,
        PK::kDedup,
        PK::kPassThrough,
        PK::kProject,
        PK::kStart,
        PK::kProject,
        PK::kExpandAll,
        PK::kExpand,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO FROM \"1\" OVER like YIELD like._src AS src, like._dst AS dst "
        " | FIND SHORTEST PATH FROM $-.src TO \"2\" OVER like, serve UPTO 5 "
        "STEPS YIELD path as p";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kProject,
        PK::kMultiShortestPath,
        PK::kProject,
        PK::kGetNeighbors,
        PK::kGetNeighbors,
        PK::kDedup,
        PK::kPassThrough,
        PK::kProject,
        PK::kStart,
        PK::kProject,
        PK::kExpandAll,
        PK::kExpand,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "$a = GO FROM \"1\" OVER like yield like._src AS src; "
        "GO FROM \"2\" OVER like yield like._src AS src, like._dst AS dst "
        " | FIND SHORTEST PATH FROM $a.src TO $-.dst OVER like UPTO 5 STEPS YIELD path as p";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect, PK::kLoop,         PK::kProject,      PK::kMultiShortestPath,
        PK::kProject,     PK::kGetNeighbors, PK::kGetNeighbors, PK::kDedup,
        PK::kPassThrough, PK::kProject,      PK::kStart,        PK::kDedup,
        PK::kProject,     PK::kProject,      PK::kExpandAll,    PK::kExpand,
        PK::kProject,     PK::kExpandAll,    PK::kExpand,       PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "YIELD \"1\" AS src, \"2\" AS dst"
        " | FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like, serve UPTO 5 "
        "STEPS YIELD path as p";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kProject,
        PK::kMultiShortestPath,
        PK::kProject,
        PK::kGetNeighbors,
        PK::kGetNeighbors,
        PK::kDedup,
        PK::kPassThrough,
        PK::kProject,
        PK::kStart,
        PK::kDedup,
        PK::kProject,
        PK::kProject,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "YIELD \"1\" AS src, \"2\" AS dst"
        " | FIND ALL PATH FROM $-.src TO $-.dst OVER like, serve UPTO 5 STEPS YIELD path as p";
    std::vector<PlanNode::Kind> expected = {
        PK::kProject,
        PK::kAllPaths,
        PK::kDedup,
        PK::kDedup,
        PK::kProject,
        PK::kProject,
        PK::kPassThrough,
        PK::kProject,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(FindPathValidatorTest, PathWithFilter) {
  {
    std::string query =
        "FIND ALL PATH FROM \"1\" TO \"2\" OVER like WHERE like.likeness > 30 "
        "UPTO 5 STEPS YIELD path as p";
    std::vector<PlanNode::Kind> expected = {
        PK::kProject,
        PK::kFilter,
        PK::kAllPaths,
        PK::kDedup,
        PK::kDedup,
        PK::kPassThrough,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "FIND SHORTEST PATH FROM \"1\" TO \"2\" OVER like WHERE like.likeness "
        "> 30 UPTO 5 STEPS YIELD path as p";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kStart,
        PK::kBFSShortest,
        PK::kFilter,
        PK::kFilter,
        PK::kGetNeighbors,
        PK::kGetNeighbors,
        PK::kPassThrough,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "FIND SHORTEST PATH FROM \"1\" TO \"2\", \"3\" OVER like WHERE "
        "like.likeness > 30 UPTO 5 STEPS YIELD path as p";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kProject,
        PK::kMultiShortestPath,
        PK::kProject,
        PK::kFilter,
        PK::kFilter,
        PK::kStart,
        PK::kGetNeighbors,
        PK::kGetNeighbors,
        PK::kPassThrough,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
}

}  // namespace graph
}  // namespace nebula
