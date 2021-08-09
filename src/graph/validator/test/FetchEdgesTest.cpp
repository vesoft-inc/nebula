/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/plan/Query.h"
#include "validator/FetchEdgesValidator.h"
#include "validator/test/ValidatorTestBase.h"

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
    auto src = VariablePropertyExpression::make(pool_.get(), "_VAR1_", kSrc);
    auto type = VariablePropertyExpression::make(pool_.get(), "_VAR2_", kType);
    auto rank = VariablePropertyExpression::make(pool_.get(), "_VAR3_", kRank);
    auto dst = VariablePropertyExpression::make(pool_.get(), "_VAR4_", kDst);
    {
        auto qctx = getQCtx("FETCH PROP ON like \"1\"->\"2\"");
        auto *pool = qctx->objPool();
        auto *start = StartNode::make(qctx);

        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        storage::cpp2::EdgeProp prop;
        prop.set_type(edgeType);
        prop.set_props({kSrc, kDst, kRank, kType, "start", "end", "likeness"});
        auto props = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
        props->emplace_back(std::move(prop));
        auto *ge = GetEdges::make(qctx, start, 1, src, type, rank, dst, std::move(props));
        std::vector<std::string> colNames{std::string("like.") + kSrc,
                                          std::string("like.") + kDst,
                                          std::string("like.") + kRank,
                                          std::string("like.") + kType,
                                          "like.start",
                                          "like.end",
                                          "like.likeness"};
        ge->setColNames(colNames);

        // filter
        auto *filter = Filter::make(qctx, ge, nullptr /*TODO*/);
        filter->setColNames(colNames);

        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(new YieldColumn(EdgeExpression::make(pool), "edges_"));
        auto *project = Project::make(qctx, filter, yieldColumns.get());
        project->setColNames({"edges_"});
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
        prop.set_type(edgeType);
        prop.set_props({kSrc, kDst, kRank, "start", "end"});
        auto props = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
        props->emplace_back(std::move(prop));
        auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
        storage::cpp2::Expr expr1;
        expr1.set_expr(EdgePropertyExpression::make(pool, "like", "start")->encode());
        storage::cpp2::Expr expr2;
        expr2.set_expr(EdgePropertyExpression::make(pool, "like", "end")->encode());
        exprs->emplace_back(std::move(expr1));
        exprs->emplace_back(std::move(expr2));
        auto *ge = GetEdges::make(qctx,
                                  start,
                                  1,
                                  src,
                                  type,
                                  rank,
                                  dst,
                                  std::move(props),
                                  std::move(exprs));
        std::vector<std::string> colNames{std::string("like.") + kSrc,
                                          std::string("like.") + kDst,
                                          std::string("like.") + kRank,
                                          "like.start",
                                          "like.end"};
        ge->setColNames(colNames);

        // filter
        auto *filter = Filter::make(qctx, ge, nullptr /*TODO*/);
        filter->setColNames(colNames);

        // Project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(new YieldColumn(EdgeSrcIdExpression::make(pool, "like")));
        yieldColumns->addColumn(new YieldColumn(EdgeDstIdExpression::make(pool, "like")));
        yieldColumns->addColumn(new YieldColumn(EdgeRankExpression::make(pool, "like")));
        yieldColumns->addColumn(
            new YieldColumn(EdgePropertyExpression::make(pool, "like", "start")));
        yieldColumns->addColumn(new YieldColumn(EdgePropertyExpression::make(pool, "like", "end")));
        auto *project = Project::make(qctx, filter, yieldColumns.get());
        project->setColNames({std::string("like.") + kSrc,
                              std::string("like.") + kDst,
                              std::string("like.") + kRank,
                              "like.start",
                              "like.end"});
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
        prop.set_type(edgeType);
        prop.set_props({kSrc, kDst, kRank, "start", "end"});
        auto props = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
        props->emplace_back(std::move(prop));
        auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
        storage::cpp2::Expr expr1;
        expr1.set_expr(EdgePropertyExpression::make(pool, "like", "start")->encode());
        storage::cpp2::Expr expr2;
        expr2.set_expr(EdgePropertyExpression::make(pool, "like", "end")->encode());
        exprs->emplace_back(std::move(expr1));
        exprs->emplace_back(std::move(expr2));
        auto *ge = GetEdges::make(qctx,
                                  start,
                                  1,
                                  src,
                                  type,
                                  rank,
                                  dst,
                                  std::move(props),
                                  std::move(exprs));
        std::vector<std::string> colNames{std::string("like.") + kSrc,
                                          std::string("like.") + kDst,
                                          std::string("like.") + kRank,
                                          "like.start",
                                          "like.end"};
        ge->setColNames(colNames);

        // filter
        auto *filter = Filter::make(qctx, ge, nullptr /*TODO*/);
        filter->setColNames(colNames);

        // Project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(new YieldColumn(EdgeSrcIdExpression::make(pool, "like")));
        yieldColumns->addColumn(new YieldColumn(EdgeDstIdExpression::make(pool, "like")));
        yieldColumns->addColumn(new YieldColumn(EdgeRankExpression::make(pool, "like")));
        yieldColumns->addColumn(
            new YieldColumn(EdgePropertyExpression::make(pool, "like", "start")));
        yieldColumns->addColumn(new YieldColumn(ArithmeticExpression::makeAdd(
            pool, ConstantExpression::make(pool, 1), ConstantExpression::make(pool, 1))));
        yieldColumns->addColumn(new YieldColumn(EdgePropertyExpression::make(pool, "like", "end")));
        auto *project = Project::make(qctx, filter, yieldColumns.get());
        project->setColNames({std::string("like.") + kSrc,
                              std::string("like.") + kDst,
                              std::string("like.") + kRank,
                              "like.start",
                              "(1+1)",
                              "like.end"});
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
        prop.set_type(edgeType);
        std::vector<std::string> propsName;
        prop.set_props({kSrc, kDst, kRank, "start", "end"});
        auto props = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
        props->emplace_back(std::move(prop));
        auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
        storage::cpp2::Expr expr1;
        expr1.set_expr(
            RelationalExpression::makeGT(pool,
                                         EdgePropertyExpression::make(pool, "like", "start"),
                                         EdgePropertyExpression::make(pool, "like", "end"))
                ->encode());
        exprs->emplace_back(std::move(expr1));
        auto *ge = GetEdges::make(qctx,
                                  start,
                                  1,
                                  src,
                                  type,
                                  rank,
                                  dst,
                                  std::move(props),
                                  std::move(exprs));
        std::vector<std::string> colNames{std::string("like.") + kSrc,
                                          std::string("like.") + kDst,
                                          std::string("like.") + kRank,
                                          "like.start",
                                          "like.end"};
        ge->setColNames(colNames);

        // filter
        auto *filter = Filter::make(qctx, ge, nullptr /*TODO*/);
        filter->setColNames(colNames);

        // project, TODO(shylock) it's could push-down to storage if it supported
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(new YieldColumn(EdgeSrcIdExpression::make(pool, "like")));
        yieldColumns->addColumn(new YieldColumn(EdgeDstIdExpression::make(pool, "like")));
        yieldColumns->addColumn(new YieldColumn(EdgeRankExpression::make(pool, "like")));
        yieldColumns->addColumn(new YieldColumn(
            RelationalExpression::makeGT(pool,
                                         EdgePropertyExpression::make(pool, "like", "start"),
                                         EdgePropertyExpression::make(pool, "like", "end"))));
        auto *project = Project::make(qctx, filter, yieldColumns.get());
        project->setColNames({std::string("like.") + kSrc,
                              std::string("like.") + kDst,
                              std::string("like.") + kRank,
                              "(like.start>like.end)"});

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
        prop.set_type(edgeType);
        prop.set_props({kSrc, kDst, kRank, "start", "end"});
        auto props = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
        props->emplace_back(std::move(prop));
        auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
        storage::cpp2::Expr expr1;
        expr1.set_expr(EdgePropertyExpression::make(pool, "like", "start")->encode());
        storage::cpp2::Expr expr2;
        expr2.set_expr(EdgePropertyExpression::make(pool, "like", "end")->encode());
        exprs->emplace_back(std::move(expr1));
        exprs->emplace_back(std::move(expr2));
        auto *ge = GetEdges::make(qctx,
                                  start,
                                  1,
                                  src,
                                  type,
                                  rank,
                                  dst,
                                  std::move(props),
                                  std::move(exprs));

        std::vector<std::string> colNames{std::string("like.") + kSrc,
                                          std::string("like.") + kDst,
                                          std::string("like.") + kRank,
                                          "like.start",
                                          "like.end"};
        ge->setColNames(colNames);

        // filter
        auto *filter = Filter::make(qctx, ge, nullptr /*TODO*/);
        filter->setColNames(colNames);

        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(new YieldColumn(EdgeSrcIdExpression::make(pool, "like")));
        yieldColumns->addColumn(new YieldColumn(EdgeDstIdExpression::make(pool, "like")));
        yieldColumns->addColumn(new YieldColumn(EdgeRankExpression::make(pool, "like")));
        yieldColumns->addColumn(
            new YieldColumn(EdgePropertyExpression::make(pool, "like", "start")));
        yieldColumns->addColumn(new YieldColumn(EdgePropertyExpression::make(pool, "like", "end")));
        auto *project = Project::make(qctx, filter, yieldColumns.get());
        project->setColNames({std::string("like.") + kSrc,
                              std::string("like.") + kDst,
                              std::string("like.") + kRank,
                              "like.start",
                              "like.end"});
        // dedup
        auto *dedup = Dedup::make(qctx, project);
        dedup->setColNames(colNames);

        // data collect
        auto *dataCollect = DataCollect::make(qctx, DataCollect::DCKind::kRowBasedMove);
        dataCollect->addDep(dedup);
        dataCollect->setInputVars({dedup->outputVar()});
        dataCollect->setColNames(colNames);

        auto result = Eq(qctx->plan()->root(), dataCollect);
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
        const std::string query = "FETCH PROP ON like \"1\"->\"2\" "
                                  "YIELD like._src AS src, like._dst AS dst, like._rank AS rank"
                                  " | FETCH PROP ON like $-.src->$-.dst@$-.rank";
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
        const std::string query = "FETCH PROP ON like \"1\"->\"2\" "
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

    // Fetch on multi-edges
    ASSERT_FALSE(validate("FETCH PROP ON like, serve \"1\"->\"2\""));
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
