/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class PathBuildExpressionTest : public ExpressionTest {};

TEST_F(PathBuildExpressionTest, PathBuild) {
    {
        auto expr = (PathBuildExpression::make(&pool));
        expr->add(VariablePropertyExpression::make(&pool, "var1", "path_src"))
            .add(VariablePropertyExpression::make(&pool, "var1", "path_edge1"))
            .add(VariablePropertyExpression::make(&pool, "var1", "path_v1"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::PATH);
        Path expected;
        expected.src = Vertex("1", {});
        expected.steps.emplace_back(Step(Vertex("2", {}), 1, "edge", 0, {}));
        EXPECT_EQ(eval.getPath(), expected);
    }
    {
        auto expr = (PathBuildExpression::make(&pool));
        expr->add(VariablePropertyExpression::make(&pool, "var1", "path_src"))
            .add(VariablePropertyExpression::make(&pool, "var1", "path_edge1"))
            .add(VariablePropertyExpression::make(&pool, "var1", "path_v1"))
            .add(VariablePropertyExpression::make(&pool, "var1", "path_edge2"))
            .add(VariablePropertyExpression::make(&pool, "var1", "path_v2"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::PATH);
        Path expected;
        expected.src = Vertex("1", {});
        expected.steps.emplace_back(Step(Vertex("2", {}), 1, "edge", 0, {}));
        expected.steps.emplace_back(Step(Vertex("3", {}), 1, "edge", 0, {}));
        EXPECT_EQ(eval.getPath(), expected);
    }
    {
        auto expr = (PathBuildExpression::make(&pool));
        expr->add(VariablePropertyExpression::make(&pool, "var1", "path_src"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::PATH);
        Path expected;
        expected.src = Vertex("1", {});
        EXPECT_EQ(eval.getPath(), expected);
    }
    {
        auto expr = (PathBuildExpression::make(&pool));
        expr->add(VariablePropertyExpression::make(&pool, "var1", "path_src"))
            .add(VariablePropertyExpression::make(&pool, "var1", "path_edge1"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::PATH);
    }

    auto varPropExpr = [](const std::string &name) {
        return VariablePropertyExpression::make(&pool, "var1", name);
    };

    {
        auto expr0 = PathBuildExpression::make(&pool);
        expr0->add(varPropExpr("path_src"));
        auto expr = PathBuildExpression::make(&pool);
        expr->add(expr0);
        expr->add(varPropExpr("path_edge1"));

        {
            // Test: Path + Edge
            auto result = Expression::eval(expr, gExpCtxt);
            EXPECT_EQ(result.type(), Value::Type::PATH);
            const auto &path = result.getPath();
            EXPECT_EQ(path.steps.size(), 1);
            EXPECT_EQ(path.steps.back().dst.vid, "2");
        }

        auto expr1 = PathBuildExpression::make(&pool);
        expr1->add(varPropExpr("path_v1"));
        expr->add(expr1);

        {
            // Test: Path + Edge + Path
            auto result = Expression::eval(expr, gExpCtxt);
            EXPECT_EQ(result.type(), Value::Type::PATH);
            const auto &path = result.getPath();
            EXPECT_EQ(path.steps.size(), 1);
            EXPECT_EQ(path.steps.back().dst.vid, "2");
        }

        expr->add(varPropExpr("path_edge2"));

        {
            // Test: Path + Edge + Path + Edge
            auto result = Expression::eval(expr, gExpCtxt);
            EXPECT_EQ(result.type(), Value::Type::PATH);
            const auto &path = result.getPath();
            EXPECT_EQ(path.steps.size(), 2);
            EXPECT_EQ(path.steps.back().dst.vid, "3");
        }

        auto pathExpr2 = PathBuildExpression::make(&pool);
        pathExpr2->add(varPropExpr("path_v2"));
        pathExpr2->add(varPropExpr("path_edge3"));

        auto pathExpr3 = PathBuildExpression::make(&pool);
        pathExpr3->add(std::move(expr));
        pathExpr3->add(std::move(pathExpr2));

        {
            // Test: Path + Path
            auto result = Expression::eval(pathExpr3, gExpCtxt);
            EXPECT_EQ(result.type(), Value::Type::PATH);
            const auto &path = result.getPath();
            EXPECT_EQ(path.steps.size(), 3);
            EXPECT_EQ(path.steps.back().dst.vid, "4");
        }
    }
}

TEST_F(PathBuildExpressionTest, PathBuildToString) {
    {
        auto expr = (PathBuildExpression::make(&pool));
        expr->add(VariablePropertyExpression::make(&pool, "var1", "path_src"))
            .add(VariablePropertyExpression::make(&pool, "var1", "path_edge1"))
            .add(VariablePropertyExpression::make(&pool, "var1", "path_v1"));
        EXPECT_EQ(expr->toString(), "PathBuild[$var1.path_src,$var1.path_edge1,$var1.path_v1]");
    }
}
}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
