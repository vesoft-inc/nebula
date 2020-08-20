/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Query.h"
#include "validator/FetchEdgesValidator.h"
#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class FetchEdgesValidatorTest : public ValidatorTestBase {};

TEST_F(FetchEdgesValidatorTest, FetchEdgesProp) {
    auto src = std::make_unique<VariablePropertyExpression>(new std::string("_VAR1_"),
                                                            new std::string(kSrc));
    auto type = std::make_unique<VariablePropertyExpression>(new std::string("_VAR2_"),
                                                             new std::string(kType));
    auto rank = std::make_unique<VariablePropertyExpression>(new std::string("_VAR3_"),
                                                             new std::string(kRank));
    auto dst = std::make_unique<VariablePropertyExpression>(new std::string("_VAR4_"),
                                                            new std::string(kDst));
    {
        auto plan = toPlan("FETCH PROP ON like \"1\"->\"2\"");

        ExecutionPlan expectedPlan(pool_.get());
        auto *start = StartNode::make(&expectedPlan);

        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        storage::cpp2::EdgeProp prop;
        prop.set_type(edgeType);
        prop.set_props({kSrc, kDst, kRank, "start", "end", "likeness"});
        std::vector<storage::cpp2::EdgeProp> props;
        props.emplace_back(std::move(prop));
        auto *ge = GetEdges::make(&expectedPlan,
                                  start,
                                  1,
                                  src.get(),
                                  type.get(),
                                  rank.get(),
                                  dst.get(),
                                  std::move(props),
                                  {});
        ge->setColNames({std::string("like.") + kSrc,
                         std::string("like.") + kDst,
                         std::string("like.") + kRank,
                         "like.start",
                         "like.end",
                         "like.likeness"});
        expectedPlan.setRoot(ge);
        auto result = Eq(plan->root(), ge);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD
    {
        auto plan = toPlan("FETCH PROP ON like \"1\"->\"2\" YIELD like.start, like.end");

        ExecutionPlan expectedPlan(pool_.get());
        auto *start = StartNode::make(&expectedPlan);

        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        storage::cpp2::EdgeProp prop;
        prop.set_type(edgeType);
        prop.set_props({kSrc, kDst, kRank, "start", "end"});
        std::vector<storage::cpp2::EdgeProp> props;
        props.emplace_back(std::move(prop));
        std::vector<storage::cpp2::Expr> exprs;
        storage::cpp2::Expr expr1;
        expr1.set_expr(
            EdgePropertyExpression(new std::string("like"), new std::string("start")).encode());
        storage::cpp2::Expr expr2;
        expr2.set_expr(
            EdgePropertyExpression(new std::string("like"), new std::string("end")).encode());
        exprs.emplace_back(std::move(expr1));
        exprs.emplace_back(std::move(expr2));
        auto *ge = GetEdges::make(&expectedPlan,
                                  start,
                                  1,
                                  src.get(),
                                  type.get(),
                                  rank.get(),
                                  dst.get(),
                                  std::move(props),
                                  std::move(exprs));
        ge->setColNames({std::string("like.") + kSrc,
                         std::string("like.") + kDst,
                         std::string("like.") + kRank,
                         "like.start",
                         "like.end"});

        // Project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(new YieldColumn(new EdgeSrcIdExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(new EdgeDstIdExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(new EdgeRankExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(
            new EdgePropertyExpression(new std::string("like"), new std::string("start"))));
        yieldColumns->addColumn(new YieldColumn(
            new EdgePropertyExpression(new std::string("like"), new std::string("end"))));
        auto *project = Project::make(&expectedPlan, ge, yieldColumns.get());
        project->setColNames({std::string("like.") + kSrc,
                              std::string("like.") + kDst,
                              std::string("like.") + kRank,
                              "like.start",
                              "like.end"});
        expectedPlan.setRoot(project);
        auto result = Eq(plan->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD const expression
    {
        auto plan = toPlan("FETCH PROP ON like \"1\"->\"2\" YIELD like.start, 1 + 1, like.end");

        ExecutionPlan expectedPlan(pool_.get());
        auto *start = StartNode::make(&expectedPlan);

        // GetEdges
        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        storage::cpp2::EdgeProp prop;
        prop.set_type(edgeType);
        prop.set_props({kSrc, kDst, kRank, "start", "end"});
        std::vector<storage::cpp2::EdgeProp> props;
        props.emplace_back(std::move(prop));
        std::vector<storage::cpp2::Expr> exprs;
        storage::cpp2::Expr expr1;
        expr1.set_expr(
            EdgePropertyExpression(new std::string("like"), new std::string("start")).encode());
        storage::cpp2::Expr expr2;
        expr2.set_expr(
            EdgePropertyExpression(new std::string("like"), new std::string("end")).encode());
        exprs.emplace_back(std::move(expr1));
        exprs.emplace_back(std::move(expr2));
        auto *ge = GetEdges::make(&expectedPlan,
                                  start,
                                  1,
                                  src.get(),
                                  type.get(),
                                  rank.get(),
                                  dst.get(),
                                  std::move(props),
                                  std::move(exprs));
        ge->setColNames({std::string("like.") + kSrc,
                         std::string("like.") + kDst,
                         std::string("like.") + kRank,
                         "like.start",
                         "(1+1)",
                         "like.end"});  // TODO(shylock) fix

        // Project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(new YieldColumn(new EdgeSrcIdExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(new EdgeDstIdExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(new EdgeRankExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(
            new EdgePropertyExpression(new std::string("like"), new std::string("start"))));
        yieldColumns->addColumn(new YieldColumn(new ArithmeticExpression(
            Expression::Kind::kAdd, new ConstantExpression(1), new ConstantExpression(1))));
        yieldColumns->addColumn(new YieldColumn(
            new EdgePropertyExpression(new std::string("like"), new std::string("end"))));
        auto *project = Project::make(&expectedPlan, ge, yieldColumns.get());
        project->setColNames({std::string("like.") + kSrc,
                              std::string("like.") + kDst,
                              std::string("like.") + kRank,
                              "like.start",
                              "(1+1)",
                              "like.end"});
        expectedPlan.setRoot(project);
        auto result = Eq(plan->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD combine properties
    {
        auto plan = toPlan("FETCH PROP ON like \"1\"->\"2\" YIELD like.start > like.end");

        ExecutionPlan expectedPlan(pool_.get());
        auto *start = StartNode::make(&expectedPlan);

        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        storage::cpp2::EdgeProp prop;
        prop.set_type(edgeType);
        std::vector<std::string> propsName;
        prop.set_props({kSrc, kDst, kRank, "start", "end"});
        std::vector<storage::cpp2::EdgeProp> props;
        props.emplace_back(std::move(prop));
        std::vector<storage::cpp2::Expr> exprs;
        storage::cpp2::Expr expr1;
        expr1.set_expr(
            RelationalExpression(
                Expression::Kind::kRelGT,
                new EdgePropertyExpression(new std::string("like"), new std::string("start")),
                new EdgePropertyExpression(new std::string("like"), new std::string("end")))
                .encode());
        exprs.emplace_back(std::move(expr1));
        auto *ge = GetEdges::make(&expectedPlan,
                                  start,
                                  1,
                                  src.get(),
                                  type.get(),
                                  rank.get(),
                                  dst.get(),
                                  std::move(props),
                                  std::move(exprs));
        ge->setColNames({std::string("like.") + kSrc,
                         std::string("like.") + kDst,
                         std::string("like.") + kRank,
                         "(like.start>like.end)"});  // TODO(shylock) fix

        // project, TODO(shylock) it's could push-down to storage if it supported
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(new YieldColumn(new EdgeSrcIdExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(new EdgeDstIdExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(new EdgeRankExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(new RelationalExpression(
            Expression::Kind::kRelGT,
            new EdgePropertyExpression(new std::string("like"), new std::string("start")),
            new EdgePropertyExpression(new std::string("like"), new std::string("end")))));
        auto *project = Project::make(&expectedPlan, ge, yieldColumns.get());
        project->setColNames({std::string("like.") + kSrc,
                              std::string("like.") + kDst,
                              std::string("like.") + kRank,
                              "(like.start>like.end)"});

        expectedPlan.setRoot(project);
        auto result = Eq(plan->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD distinct
    {
        auto plan = toPlan("FETCH PROP ON like \"1\"->\"2\" YIELD distinct like.start, like.end");

        ExecutionPlan expectedPlan(pool_.get());
        auto *start = StartNode::make(&expectedPlan);

        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        storage::cpp2::EdgeProp prop;
        prop.set_type(edgeType);
        prop.set_props({kSrc, kDst, kRank, "start", "end"});
        std::vector<storage::cpp2::EdgeProp> props;
        props.emplace_back(std::move(prop));
        std::vector<storage::cpp2::Expr> exprs;
        storage::cpp2::Expr expr1;
        expr1.set_expr(
            EdgePropertyExpression(new std::string("like"), new std::string("start")).encode());
        storage::cpp2::Expr expr2;
        expr2.set_expr(
            EdgePropertyExpression(new std::string("like"), new std::string("end")).encode());
        exprs.emplace_back(std::move(expr1));
        exprs.emplace_back(std::move(expr2));
        auto *ge = GetEdges::make(&expectedPlan,
                                  start,
                                  1,
                                  src.get(),
                                  type.get(),
                                  rank.get(),
                                  dst.get(),
                                  std::move(props),
                                  std::move(exprs));

        std::vector<std::string> colNames{std::string("like.") + kSrc,
                                          std::string("like.") + kDst,
                                          std::string("like.") + kRank,
                                          "like.start",
                                          "like.end"};
        ge->setColNames(colNames);

        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(new YieldColumn(new EdgeSrcIdExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(new EdgeDstIdExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(new EdgeRankExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(
            new EdgePropertyExpression(new std::string("like"), new std::string("start"))));
        yieldColumns->addColumn(new YieldColumn(
            new EdgePropertyExpression(new std::string("like"), new std::string("end"))));
        auto *project = Project::make(&expectedPlan, ge, yieldColumns.get());
        project->setColNames({std::string("like.") + kSrc,
                              std::string("like.") + kDst,
                              std::string("like.") + kRank,
                              "like.start",
                              "like.end"});
        // dedup
        auto *dedup = Dedup::make(&expectedPlan, project);
        dedup->setColNames(colNames);

        // data collect
        auto *dataCollect = DataCollect::make(
            &expectedPlan, dedup, DataCollect::CollectKind::kRowBasedMove, {dedup->varName()});
        dataCollect->setColNames(colNames);

        expectedPlan.setRoot(dataCollect);
        auto result = Eq(plan->root(), dataCollect);
        ASSERT_TRUE(result.ok()) << result;
    }
}

TEST_F(FetchEdgesValidatorTest, FetchEdgesInputOutput) {
    // var
    {
        const std::string query = "$a = FETCH PROP ON like \"1\"->\"2\" "
                                  "YIELD like._src AS src, like._dst AS dst, like._rank AS rank;"
                                  "FETCH PROP ON like $a.src->$a.dst";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kGetEdges,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetEdges,
                                    PlanNode::Kind::kStart,
                                }));
    }
    // pipe
    {
        const std::string query = "FETCH PROP ON like \"1\"->\"2\" "
                                  "YIELD like._src AS src, like._dst AS dst, like._rank AS rank"
                                  " | FETCH PROP ON like $-.src->$-.dst@$-.rank";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kGetEdges,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetEdges,
                                    PlanNode::Kind::kStart,
                                }));
    }

    // with project
    // var
    {
        const std::string query =
            "$a = FETCH PROP ON like \"1\"->\"2\" "
            "YIELD like._src AS src, like._dst AS dst, like._rank + 1 AS rank;"
            "FETCH PROP ON like $a.src->$a.dst@$a.rank "
            "YIELD like._src + 1";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetEdges,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetEdges,
                                    PlanNode::Kind::kStart,
                                }));
    }
    // pipe
    {
        const std::string query = "FETCH PROP ON like \"1\"->\"2\" "
                                  "YIELD like._src AS src, like._dst AS dst, like._rank + 1 AS rank"
                                  " | FETCH PROP ON like $-.src->$-.dst@$-.rank "
                                  "YIELD like._src + 1";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetEdges,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kGetEdges,
                                    PlanNode::Kind::kStart,
                                }));
    }
}

TEST_F(FetchEdgesValidatorTest, FetchEdgesPropFailed) {
    // mismatched tag
    ASSERT_FALSE(validate("FETCH PROP ON edge1 \"1\"->\"2\" YIELD edge2.prop2"));

    // notexist edge
    ASSERT_FALSE(validate("FETCH PROP ON not_exist_edge \"1\"->\"2\" YIELD not_exist_edge.prop1"));

    // notexist edge property
    ASSERT_FALSE(validate("FETCH PROP ON like \"1\"->\"2\" YIELD like.not_exist_prop"));

    // invalid yield expression
    ASSERT_FALSE(validate("$a = FETCH PROP ON like \"1\"->\"2\" "
                          "YIELD like._src AS src;"
                          "FETCH PROP ON like \"1\"->\"2\" YIELD $a.src + 1"));

    ASSERT_FALSE(validate("FETCH PROP ON like \"1\"->\"2\" "
                          "YIELD like._src AS src | "
                          "FETCH PROP ON like \"1\"->\"2\" YIELD $-.src + 1"));

    ASSERT_FALSE(validate("FETCH PROP ON like \"1\"->\"2\" YIELD $^.like.start + 1"));
    ASSERT_FALSE(validate("FETCH PROP ON like \"1\"->\"2\" YIELD $$.like.start + 1"));
}

TEST_F(FetchEdgesValidatorTest, FetchEdgesInputFailed) {
    // mismatched variable
    ASSERT_FALSE(validate("$a = FETCH PROP ON like \"1\"->\"2\" "
                          "YIELD like._src AS src, like._dst AS dst, like._rank AS rank;"
                          "FETCH PROP ON like $b.src->$b.dst@$b.rank"));

    // mismatched variable property
    ASSERT_FALSE(validate("$a = FETCH PROP ON like \"1\"->\"2\" "
                          "YIELD like._src AS src, like._dst AS dst, like._rank AS rank;"
                          "FETCH PROP ON like $b.src->$b.dst@$b.not_exist_property"));

    // mismatched input property
    ASSERT_FALSE(validate("FETCH PROP ON like \"1\"->\"2\" "
                          "YIELD like._src AS src, like._dst AS dst, like._rank AS rank | "
                          "FETCH PROP ON like $-.src->$-.dst@$-.not_exist_property"));

    // refer to different variables
    ASSERT_FALSE(validate("$a = FETCH PROP ON like \"1\"->\"2\" "
                          "YIELD like._src AS src, like._dst AS dst, like._rank AS rank;"
                          "$b = FETCH PROP ON like \"1\"->\"2\" "
                          "YIELD like._src AS src, like._dst AS dst, like._rank AS rank;"
                          "FETCH PROP ON like $a.src->$b.dst@$b.rank"));
}

}   // namespace graph
}   // namespace nebula
