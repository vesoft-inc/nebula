/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>               // for Message
#include <gtest/gtest.h>               // for TestPartResult
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <memory>       // for allocator, make_...
#include <set>          // for set, operator!=
#include <string>       // for string, basic_st...
#include <type_traits>  // for remove_reference...
#include <utility>      // for move
#include <vector>       // for vector

#include "common/base/Base.h"                        // for kDst, kRank, kSrc
#include "common/base/Status.h"                      // for operator<<, Status
#include "common/base/StatusOr.h"                    // for StatusOr
#include "common/expression/ArithmeticExpression.h"  // for ArithmeticExpres...
#include "common/expression/ColumnExpression.h"      // for ColumnExpression
#include "common/expression/ConstantExpression.h"    // for ConstantExpression
#include "common/expression/EdgeExpression.h"        // for EdgeExpression
#include "common/expression/PropertyExpression.h"    // for EdgePropertyExpr...
#include "common/expression/RelationalExpression.h"  // for RelationalExpres...
#include "graph/context/QueryContext.h"              // for QueryContext
#include "graph/planner/plan/ExecutionPlan.h"        // for ExecutionPlan
#include "graph/planner/plan/Logic.h"                // for StartNode
#include "graph/planner/plan/PlanNode.h"             // for PlanNode::Kind
#include "graph/planner/plan/Query.h"                // for Project, Filter
#include "graph/validator/test/MockSchemaManager.h"  // for MockSchemaManager
#include "graph/validator/test/ValidatorTestBase.h"  // for ValidatorTestBase
#include "interface/gen-cpp2/storage_types.h"        // for EdgeProp, Expr
#include "parser/Clauses.h"                          // for YieldColumns

namespace nebula {
namespace graph {

class FetchEdgesValidatorTest : public ValidatorTestBase {
 protected:
  QueryContext *getQCtx(const std::string &query) {
    auto status = validate(query);
    EXPECT_TRUE(status);
    return std::move(status).value();
  }
};

TEST_F(FetchEdgesValidatorTest, FetchEdgesProp) {
  auto src = ColumnExpression::make(pool_.get(), 0);
  auto type = VariablePropertyExpression::make(pool_.get(), "_VAR2_", kType);
  auto rank = ColumnExpression::make(pool_.get(), 1);
  auto dst = ColumnExpression::make(pool_.get(), 2);
  {
    auto qctx = getQCtx("FETCH PROP ON like \"1\"->\"2\" YIELD edge as e");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
    ASSERT_TRUE(edgeTypeResult.ok());
    auto edgeType = edgeTypeResult.value();
    storage::cpp2::EdgeProp prop;
    prop.type_ref() = edgeType;
    std::set<std::string> propNames{kSrc, kDst, kRank, kType, "start", "end", "likeness"};
    prop.props_ref() = std::vector<std::string>(propNames.begin(), propNames.end());
    auto props = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
    props->emplace_back(std::move(prop));
    auto *ge = GetEdges::make(qctx, start, 1, src, type, rank, dst, std::move(props));
    auto *filter = Filter::make(qctx, ge, nullptr /*TODO*/);

    // project
    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(EdgeExpression::make(pool), "e"));
    auto *project = Project::make(qctx, filter, yieldColumns.get());
    project->setColNames({"e"});
    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
  // With YIELD
  {
    auto qctx = getQCtx("FETCH PROP ON like \"1\"->\"2\" YIELD like.start, like.end");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
    ASSERT_TRUE(edgeTypeResult.ok());
    auto edgeType = edgeTypeResult.value();
    storage::cpp2::EdgeProp prop;
    prop.type_ref() = edgeType;
    std::set<std::string> propNames{kSrc, kDst, kRank, "start", "end"};
    prop.props_ref() = std::vector<std::string>(propNames.begin(), propNames.end());
    auto props = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
    props->emplace_back(std::move(prop));
    auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
    storage::cpp2::Expr expr1;
    expr1.expr_ref() = EdgePropertyExpression::make(pool, "like", "start")->encode();
    storage::cpp2::Expr expr2;
    expr2.expr_ref() = EdgePropertyExpression::make(pool, "like", "end")->encode();
    exprs->emplace_back(std::move(expr1));
    exprs->emplace_back(std::move(expr2));
    auto *ge =
        GetEdges::make(qctx, start, 1, src, type, rank, dst, std::move(props), std::move(exprs));
    auto *filter = Filter::make(qctx, ge, nullptr /*TODO*/);

    // Project
    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(EdgePropertyExpression::make(pool, "like", "start")));
    yieldColumns->addColumn(new YieldColumn(EdgePropertyExpression::make(pool, "like", "end")));
    auto *project = Project::make(qctx, filter, yieldColumns.get());
    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
  // With YIELD const expression
  {
    auto qctx = getQCtx("FETCH PROP ON like \"1\"->\"2\" YIELD like.start, 1 + 1, like.end");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    // GetEdges
    auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
    ASSERT_TRUE(edgeTypeResult.ok());
    auto edgeType = edgeTypeResult.value();
    storage::cpp2::EdgeProp prop;
    prop.type_ref() = edgeType;
    std::set<std::string> propNames{kSrc, kDst, kRank, "start", "end"};
    prop.props_ref() = std::vector<std::string>(propNames.begin(), propNames.end());
    auto props = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
    props->emplace_back(std::move(prop));
    auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
    storage::cpp2::Expr expr1;
    expr1.expr_ref() = EdgePropertyExpression::make(pool, "like", "start")->encode();
    storage::cpp2::Expr expr2;
    expr2.expr_ref() = EdgePropertyExpression::make(pool, "like", "end")->encode();
    exprs->emplace_back(std::move(expr1));
    exprs->emplace_back(std::move(expr2));
    auto *ge =
        GetEdges::make(qctx, start, 1, src, type, rank, dst, std::move(props), std::move(exprs));
    auto *filter = Filter::make(qctx, ge, nullptr /*TODO*/);

    // Project
    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(EdgePropertyExpression::make(pool, "like", "start")));
    yieldColumns->addColumn(new YieldColumn(ArithmeticExpression::makeAdd(
        pool, ConstantExpression::make(pool, 1), ConstantExpression::make(pool, 1))));
    yieldColumns->addColumn(new YieldColumn(EdgePropertyExpression::make(pool, "like", "end")));
    auto *project = Project::make(qctx, filter, yieldColumns.get());
    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
  // With YIELD combine properties
  {
    auto qctx = getQCtx("FETCH PROP ON like \"1\"->\"2\" YIELD like.start > like.end");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
    ASSERT_TRUE(edgeTypeResult.ok());
    auto edgeType = edgeTypeResult.value();
    storage::cpp2::EdgeProp prop;
    prop.type_ref() = edgeType;
    std::set<std::string> propNames{kSrc, kDst, kRank, "start", "end"};
    prop.props_ref() = std::vector<std::string>(propNames.begin(), propNames.end());
    auto props = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
    props->emplace_back(std::move(prop));
    auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
    storage::cpp2::Expr expr1;
    expr1.expr_ref() =
        RelationalExpression::makeGT(pool,
                                     EdgePropertyExpression::make(pool, "like", "start"),
                                     EdgePropertyExpression::make(pool, "like", "end"))
            ->encode();
    exprs->emplace_back(std::move(expr1));
    auto *ge =
        GetEdges::make(qctx, start, 1, src, type, rank, dst, std::move(props), std::move(exprs));
    auto *filter = Filter::make(qctx, ge, nullptr /*TODO*/);

    // project, TODO(shylock) it's could push-down to storage if it supported
    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(
        RelationalExpression::makeGT(pool,
                                     EdgePropertyExpression::make(pool, "like", "start"),
                                     EdgePropertyExpression::make(pool, "like", "end"))));
    auto *project = Project::make(qctx, filter, yieldColumns.get());
    auto result = Eq(qctx->plan()->root(), project);
    ASSERT_TRUE(result.ok()) << result;
  }
  // With YIELD distinct
  {
    auto qctx = getQCtx("FETCH PROP ON like \"1\"->\"2\" YIELD distinct like.start, like.end");
    auto *pool = qctx->objPool();
    auto *start = StartNode::make(qctx);

    auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
    ASSERT_TRUE(edgeTypeResult.ok());
    auto edgeType = edgeTypeResult.value();
    storage::cpp2::EdgeProp prop;
    prop.type_ref() = edgeType;
    std::set<std::string> propNames{kSrc, kDst, kRank, "start", "end"};
    prop.props_ref() = std::vector<std::string>(propNames.begin(), propNames.end());
    auto props = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
    props->emplace_back(std::move(prop));
    auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
    storage::cpp2::Expr expr1;
    expr1.expr_ref() = EdgePropertyExpression::make(pool, "like", "start")->encode();
    storage::cpp2::Expr expr2;
    expr2.expr_ref() = EdgePropertyExpression::make(pool, "like", "end")->encode();
    exprs->emplace_back(std::move(expr1));
    exprs->emplace_back(std::move(expr2));
    auto *ge =
        GetEdges::make(qctx, start, 1, src, type, rank, dst, std::move(props), std::move(exprs));
    auto *filter = Filter::make(qctx, ge, nullptr /*TODO*/);

    // project
    auto yieldColumns = std::make_unique<YieldColumns>();
    yieldColumns->addColumn(new YieldColumn(EdgePropertyExpression::make(pool, "like", "start")));
    yieldColumns->addColumn(new YieldColumn(EdgePropertyExpression::make(pool, "like", "end")));
    auto *project = Project::make(qctx, filter, yieldColumns.get());
    auto *dedup = Dedup::make(qctx, project);

    // data collect
    auto *dataCollect = DataCollect::make(qctx, DataCollect::DCKind::kRowBasedMove);
    dataCollect->addDep(dedup);
    dataCollect->setInputVars({dedup->outputVar()});
    dataCollect->setColNames({"like.start", "like.end"});

    auto result = Eq(qctx->plan()->root(), dataCollect);
    ASSERT_TRUE(result.ok()) << result;
  }
}

TEST_F(FetchEdgesValidatorTest, FetchEdgesInputOutput) {
  // var
  {
    const std::string query =
        "$a = FETCH PROP ON like \"1\"->\"2\" "
        "YIELD like._src AS src, like._dst AS dst, like._rank AS rank;"
        "FETCH PROP ON like $a.src->$a.dst YIELD edge as e";
    EXPECT_TRUE(checkResult(query,
                            {
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kFilter,
                                PlanNode::Kind::kGetEdges,
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kFilter,
                                PlanNode::Kind::kGetEdges,
                                PlanNode::Kind::kStart,
                            }));
  }
  // pipe
  {
    const std::string query =
        "FETCH PROP ON like \"1\"->\"2\" "
        "YIELD like._src AS src, like._dst AS dst, like._rank AS rank"
        " | FETCH PROP ON like $-.src->$-.dst@$-.rank YIELD edge as e";
    EXPECT_TRUE(checkResult(query,
                            {
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kFilter,
                                PlanNode::Kind::kGetEdges,
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kFilter,
                                PlanNode::Kind::kGetEdges,
                                PlanNode::Kind::kStart,
                            }));
  }

  // with project var
  {
    const std::string query =
        "$a = FETCH PROP ON like \"1\"->\"2\" "
        "YIELD like._src AS src, like._dst AS dst, like._rank + 1 AS rank;"
        "FETCH PROP ON like $a.src->$a.dst@$a.rank "
        "YIELD like._src + 1";
    EXPECT_TRUE(checkResult(query,
                            {
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kFilter,
                                PlanNode::Kind::kGetEdges,
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kFilter,
                                PlanNode::Kind::kGetEdges,
                                PlanNode::Kind::kStart,
                            }));
  }
  // pipe
  {
    const std::string query =
        "FETCH PROP ON like \"1\"->\"2\" "
        "YIELD like._src AS src, like._dst AS dst, like._rank + 1 AS rank"
        " | FETCH PROP ON like $-.src->$-.dst@$-.rank "
        "YIELD like._src + 1";
    EXPECT_TRUE(checkResult(query,
                            {
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kFilter,
                                PlanNode::Kind::kGetEdges,
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kFilter,
                                PlanNode::Kind::kGetEdges,
                                PlanNode::Kind::kStart,
                            }));
  }
}

TEST_F(FetchEdgesValidatorTest, FetchEdgesPropFailed) {
  // mismatched tag
  ASSERT_FALSE(validate("FETCH PROP ON edge1 \"1\"->\"2\" YIELD edge2.prop2"));

  ASSERT_FALSE(validate("FETCH PROP ON edge1 \"1\"->\"2\" YIELD edge"));
  ASSERT_FALSE(validate("FETCH PROP ON edge1 \"1\"->\"2\" YIELD vertex"));
  ASSERT_FALSE(validate("FETCH PROP ON edge1 \"1\"->\"2\" YIELD vertex as a, edge1.prop1"));
  ASSERT_FALSE(validate("FETCH PROP ON edge1 \"1\"->\"2\" YIELD vertex as b"));
  ASSERT_FALSE(validate("FETCH PROP ON edge1 \"1\"->\"2\" YIELD path as a"));
  ASSERT_FALSE(validate("FETCH PROP ON edge1 \"1\"->\"2\" YIELD $$.player.name"));
  ASSERT_FALSE(validate("FETCH PROP ON edge1 \"1\"->\"2\" YIELD $^.player.name"));

  // nonexistent edge
  ASSERT_FALSE(validate("FETCH PROP ON not_exist_edge \"1\"->\"2\" YIELD not_exist_edge.prop1"));

  // nonexistent edge property
  ASSERT_FALSE(validate("FETCH PROP ON like \"1\"->\"2\" YIELD like.not_exist_prop"));

  // invalid yield expression
  ASSERT_FALSE(
      validate("$a = FETCH PROP ON like \"1\"->\"2\" "
               "YIELD like._src AS src;"
               "FETCH PROP ON like \"1\"->\"2\" YIELD $a.src + 1"));

  ASSERT_FALSE(
      validate("FETCH PROP ON like \"1\"->\"2\" "
               "YIELD like._src AS src | "
               "FETCH PROP ON like \"1\"->\"2\" YIELD $-.src + 1"));

  ASSERT_FALSE(validate("FETCH PROP ON like \"1\"->\"2\" YIELD $^.like.start + 1"));
  ASSERT_FALSE(validate("FETCH PROP ON like \"1\"->\"2\" YIELD $$.like.start + 1"));

  // Fetch on multi-edges
  ASSERT_FALSE(validate("FETCH PROP ON like, serve \"1\"->\"2\" YIELD edge as e"));
}

TEST_F(FetchEdgesValidatorTest, FetchEdgesInputFailed) {
  // mismatched variable
  ASSERT_FALSE(
      validate("$a = FETCH PROP ON like \"1\"->\"2\" "
               "YIELD like._src AS src, like._dst AS dst, like._rank AS rank;"
               "FETCH PROP ON like $b.src->$b.dst@$b.rank YIELD edge as e"));

  // mismatched variable property
  ASSERT_FALSE(
      validate("$a = FETCH PROP ON like \"1\"->\"2\" "
               "YIELD like._src AS src, like._dst AS dst, like._rank AS rank;"
               "FETCH PROP ON like $b.src->$b.dst@$b.not_exist_property YIELD edge as e"));

  // mismatched input property
  ASSERT_FALSE(
      validate("FETCH PROP ON like \"1\"->\"2\" "
               "YIELD like._src AS src, like._dst AS dst, like._rank AS rank | "
               "FETCH PROP ON like $-.src->$-.dst@$-.not_exist_property YIELD edge as e"));

  // refer to different variables
  ASSERT_FALSE(
      validate("$a = FETCH PROP ON like \"1\"->\"2\" "
               "YIELD like._src AS src, like._dst AS dst, like._rank AS rank;"
               "$b = FETCH PROP ON like \"1\"->\"2\" "
               "YIELD like._src AS src, like._dst AS dst, like._rank AS rank;"
               "FETCH PROP ON like $a.src->$b.dst@$b.rank YIELD edge as e"));
}

}  // namespace graph
}  // namespace nebula
