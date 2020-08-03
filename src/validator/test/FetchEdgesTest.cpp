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
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON like \"1\"->\"2\""));

        auto *start = StartNode::make(expectedQueryCtx_->plan());

        auto *plan = qCtx_->plan();
        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        std::vector<nebula::Row> edges{nebula::Row({
            "1",
            edgeType,
            0,
            "2",
        })};
        storage::cpp2::EdgeProp prop;
        prop.set_type(edgeType);
        prop.set_props({kSrc, kDst, kRank, "start", "end", "likeness"});
        std::vector<storage::cpp2::EdgeProp> props;
        props.emplace_back(std::move(prop));
        auto *ge = GetEdges::make(expectedQueryCtx_->plan(),
                                  start,
                                  1,
                                  std::move(edges),
                                  nullptr,
                                  edgeType,
                                  nullptr,
                                  nullptr,
                                  std::move(props),
                                  {});
        expectedQueryCtx_->plan()->setRoot(ge);
        auto result = Eq(plan->root(), ge);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON like \"1\"->\"2\" YIELD like.start, like.end"));

        auto *start = StartNode::make(expectedQueryCtx_->plan());

        auto *plan = qCtx_->plan();
        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        std::vector<nebula::Row> edges{nebula::Row({
            "1",
            edgeType,
            0,
            "2",
        })};
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
        auto *ge = GetEdges::make(expectedQueryCtx_->plan(),
                                  start,
                                  1,
                                  std::move(edges),
                                  nullptr,
                                  edgeType,
                                  nullptr,
                                  nullptr,
                                  std::move(props),
                                  std::move(exprs));

        // Project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(new YieldColumn(new EdgeSrcIdExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(new EdgeDstIdExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(new EdgeRankExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(
            new EdgePropertyExpression(new std::string("like"), new std::string("start"))));
        yieldColumns->addColumn(new YieldColumn(
            new EdgePropertyExpression(new std::string("like"), new std::string("end"))));
        auto *project = Project::make(expectedQueryCtx_->plan(), ge, yieldColumns.get());
        project->setColNames({std::string("like.") + kSrc,
                              std::string("like.") + kDst,
                              std::string("like.") + kRank,
                              "like.start",
                              "like.end"});
        expectedQueryCtx_->plan()->setRoot(project);
        auto result = Eq(plan->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD const expression
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON like \"1\"->\"2\" YIELD like.start, 1 + 1, like.end"));

        auto *start = StartNode::make(expectedQueryCtx_->plan());

        auto *plan = qCtx_->plan();

        // GetEdges
        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        std::vector<nebula::Row> edges{nebula::Row({
            "1",
            edgeType,
            0,
            "2",
        })};
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
        auto *ge = GetEdges::make(expectedQueryCtx_->plan(),
                                  start,
                                  1,
                                  std::move(edges),
                                  nullptr,
                                  edgeType,
                                  nullptr,
                                  nullptr,
                                  std::move(props),
                                  std::move(exprs));

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
        auto *project = Project::make(expectedQueryCtx_->plan(), ge, yieldColumns.get());
        project->setColNames({std::string("like.") + kSrc,
                              std::string("like.") + kDst,
                              std::string("like.") + kRank,
                              "like.start",
                              "(1+1)",
                              "like.end"});
        expectedQueryCtx_->plan()->setRoot(project);
        auto result = Eq(plan->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD combine properties
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON like \"1\"->\"2\" YIELD like.start > like.end"));

        auto *start = StartNode::make(expectedQueryCtx_->plan());

        auto *plan = qCtx_->plan();
        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        std::vector<nebula::Row> edges{nebula::Row({
            "1",
            edgeType,
            0,
            "2",
        })};
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
        auto *ge = GetEdges::make(expectedQueryCtx_->plan(),
                                  start,
                                  1,
                                  std::move(edges),
                                  nullptr,
                                  edgeType,
                                  nullptr,
                                  nullptr,
                                  std::move(props),
                                  std::move(exprs));

        // project, TODO(shylock) it's could push-down to storage if it supported
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(new YieldColumn(new EdgeSrcIdExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(new EdgeDstIdExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(new EdgeRankExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(new RelationalExpression(
            Expression::Kind::kRelGT,
            new EdgePropertyExpression(new std::string("like"), new std::string("start")),
            new EdgePropertyExpression(new std::string("like"), new std::string("end")))));
        auto *project = Project::make(expectedQueryCtx_->plan(), ge, yieldColumns.get());
        project->setColNames({std::string("like.") + kSrc,
                              std::string("like.") + kDst,
                              std::string("like.") + kRank,
                              "(like.start>like.end)"});

        expectedQueryCtx_->plan()->setRoot(project);
        auto result = Eq(plan->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD distinct
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON like \"1\"->\"2\" YIELD distinct like.start, like.end"));

        auto *start = StartNode::make(expectedQueryCtx_->plan());

        auto *plan = qCtx_->plan();
        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        std::vector<nebula::Row> edges{nebula::Row({
            "1",
            edgeType,
            0,
            "2",
        })};
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
        auto *ge = GetEdges::make(expectedQueryCtx_->plan(),
                                  start,
                                  1,
                                  std::move(edges),
                                  nullptr,
                                  edgeType,
                                  nullptr,
                                  nullptr,
                                  std::move(props),
                                  std::move(exprs));

        std::vector<std::string> colNames{std::string("like.") + kSrc,
                                          std::string("like.") + kDst,
                                          std::string("like.") + kRank,
                                          "like.start",
                                          "like.end"};

        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(new YieldColumn(new EdgeSrcIdExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(new EdgeDstIdExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(new EdgeRankExpression(new std::string("like"))));
        yieldColumns->addColumn(new YieldColumn(
            new EdgePropertyExpression(new std::string("like"), new std::string("start"))));
        yieldColumns->addColumn(new YieldColumn(
            new EdgePropertyExpression(new std::string("like"), new std::string("end"))));
        auto *project = Project::make(expectedQueryCtx_->plan(), ge, yieldColumns.get());
        project->setColNames({std::string("like.") + kSrc,
                              std::string("like.") + kDst,
                              std::string("like.") + kRank,
                              "like.start",
                              "like.end"});
        // dedup
        auto *dedup = Dedup::make(expectedQueryCtx_->plan(), project);
        dedup->setColNames(colNames);

        // data collect
        auto *dataCollect = DataCollect::make(expectedQueryCtx_->plan(),
                                              dedup,
                                              DataCollect::CollectKind::kRowBasedMove,
                                              {dedup->varName()});
        dataCollect->setColNames(colNames);

        expectedQueryCtx_->plan()->setRoot(dataCollect);
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
    {
        auto result = GQLParser().parse("FETCH PROP ON edge1 \"1\"->\"2\" YIELD edge2.prop2");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }

    // notexist edge
    {
        auto result = GQLParser().parse("FETCH PROP ON not_exist_edge \"1\"->\"2\" "
                                        "YIELD not_exist_edge.prop1");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }

    // notexist edge property
    {
        auto result = GQLParser().parse("FETCH PROP ON like \"1\"->\"2\" "
                                        "YIELD like.not_exist_prop");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }

    // invalid yield expression
    {
        auto result = GQLParser().parse("$a = FETCH PROP ON like \"1\"->\"2\" "
                                        "YIELD like._src AS src;"
                                        "FETCH PROP ON like \"1\"->\"2\" YIELD $a.src + 1");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }
    {
        auto result = GQLParser().parse("FETCH PROP ON like \"1\"->\"2\" "
                                        "YIELD like._src AS src | "
                                        "FETCH PROP ON like \"1\"->\"2\" YIELD $-.src + 1");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }
    {
        auto result = GQLParser().parse("FETCH PROP ON like \"1\"->\"2\" "
                                        "YIELD $^.like.start + 1");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }
    {
        auto result = GQLParser().parse("FETCH PROP ON like \"1\"->\"2\" "
                                        "YIELD $$.like.start + 1");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }
}

TEST_F(FetchEdgesValidatorTest, FetchEdgesInputFailed) {
    // mismatched variable
    {
        auto result =
            GQLParser().parse("$a = FETCH PROP ON like \"1\"->\"2\" "
                              "YIELD like._src AS src, like._dst AS dst, like._rank AS rank;"
                              "FETCH PROP ON like $b.src->$b.dst@$b.rank");
        ASSERT_TRUE(result.ok()) << result.status();
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }

    // mismatched variable property
    {
        auto result =
            GQLParser().parse("$a = FETCH PROP ON like \"1\"->\"2\" "
                              "YIELD like._src AS src, like._dst AS dst, like._rank AS rank;"
                              "FETCH PROP ON like $b.src->$b.dst@$b.not_exist_property");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }

    // mismatched input property
    {
        auto result =
            GQLParser().parse("FETCH PROP ON like \"1\"->\"2\" "
                              "YIELD like._src AS src, like._dst AS dst, like._rank AS rank | "
                              "FETCH PROP ON like $-.src->$-.dst@$-.not_exist_property");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }

    // refer to different variables
    {
        auto result =
            GQLParser().parse("$a = FETCH PROP ON like \"1\"->\"2\" "
                              "YIELD like._src AS src, like._dst AS dst, like._rank AS rank;"
                              "$b = FETCH PROP ON like \"1\"->\"2\" "
                              "YIELD like._src AS src, like._dst AS dst, like._rank AS rank;"
                              "FETCH PROP ON like $a.src->$b.dst@$b.rank");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }
}

}   // namespace graph
}   // namespace nebula
