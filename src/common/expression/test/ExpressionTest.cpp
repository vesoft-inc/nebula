/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/test/TestBase.h"

namespace nebula {

TEST_F(ExpressionTest, toStringTest) {
    {
        auto ep = ConstantExpression::make(&pool, 1);
        EXPECT_EQ(ep->toString(), "1");
    }
    {
        auto ep = ConstantExpression::make(&pool, 1.123);
        EXPECT_EQ(ep->toString(), "1.123");
    }
    // FIXME: double/float to string conversion
    // {
    //     auto ep = ConstantExpression::make(&pool, 1.0);
    //     EXPECT_EQ(ep->toString(), "1.0");
    // }
    {
        auto ep = ConstantExpression::make(&pool, true);
        EXPECT_EQ(ep->toString(), "true");
    }
    {
        auto ep = ConstantExpression::make(&pool, List(std::vector<Value>{1, 2, 3, 4, 9, 0, -23}));
        EXPECT_EQ(ep->toString(), "[1,2,3,4,9,0,-23]");
    }
    {
        auto ep = ConstantExpression::make(&pool, Map({{"hello", "world"}, {"name", "zhang"}}));
        EXPECT_EQ(ep->toString(), "{name:\"zhang\",hello:\"world\"}");
    }
    {
        auto ep = ConstantExpression::make(&pool, Set({1, 2.3, "hello", true}));
        EXPECT_EQ(ep->toString(), "{\"hello\",2.3,true,1}");
    }
    {
        auto ep = ConstantExpression::make(&pool, Date(1234));
        EXPECT_EQ(ep->toString(), "-32765-05-19");
    }
    {
        auto ep =
            ConstantExpression::make(&pool, Edge("100", "102", 2, "like", 3, {{"likeness", 95}}));
        EXPECT_EQ(ep->toString(), "(\"100\")-[like(2)]->(\"102\")@3 likeness:95");
    }
    {
        auto ep =
            ConstantExpression::make(&pool, Vertex("100", {Tag("player", {{"name", "jame"}})}));
        EXPECT_EQ(ep->toString(), "(\"100\") Tag: player, name:\"jame\"");
    }
    {
        auto ep = TypeCastingExpression::make(
            &pool, Value::Type::FLOAT, ConstantExpression::make(&pool, 2));
        EXPECT_EQ(ep->toString(), "(FLOAT)2");
    }
    {
        auto plusEx = UnaryExpression::makePlus(&pool, ConstantExpression::make(&pool, 2));
        EXPECT_EQ(plusEx->toString(), "+(2)");

        auto negEx = UnaryExpression::makeNegate(&pool, ConstantExpression::make(&pool, 2));
        EXPECT_EQ(negEx->toString(), "-(2)");

        auto incrEx = UnaryExpression::makeIncr(&pool, ConstantExpression::make(&pool, 2));
        EXPECT_EQ(incrEx->toString(), "++(2)");

        auto decrEx = UnaryExpression::makeDecr(&pool, ConstantExpression::make(&pool, 2));
        EXPECT_EQ(decrEx->toString(), "--(2)");

        auto notEx = UnaryExpression::makeNot(&pool, ConstantExpression::make(&pool, 2));
        EXPECT_EQ(notEx->toString(), "!(2)");

        auto isNullEx = UnaryExpression::makeIsNull(&pool, ConstantExpression::make(&pool, 2));
        EXPECT_EQ(isNullEx->toString(), "2 IS NULL");

        auto isNotNullEx = UnaryExpression::makeIsNotNull(
            &pool, ConstantExpression::make(&pool, Value::kNullValue));
        EXPECT_EQ(isNotNullEx->toString(), "NULL IS NOT NULL");

        auto isEmptyEx = UnaryExpression::makeIsEmpty(&pool, ConstantExpression::make(&pool, 2));
        EXPECT_EQ(isEmptyEx->toString(), "2 IS EMPTY");

        auto isNotEmptyEx =
            UnaryExpression::makeIsNotEmpty(&pool, ConstantExpression::make(&pool, Value::kEmpty));
        EXPECT_EQ(isNotEmptyEx->toString(), "__EMPTY__ IS NOT EMPTY");
    }
    {
        auto var = VariableExpression::make(&pool, "name");
        EXPECT_EQ(var->toString(), "$name");

        auto versionVar =
            VersionedVariableExpression::make(&pool, "name", ConstantExpression::make(&pool, 1));
        EXPECT_EQ(versionVar->toString(), "$name{1}");
    }
    {
        TEST_TOSTRING(2 + 2 - 3, "((2+2)-3)");
        TEST_TOSTRING(true OR true, "(true OR true)");
        TEST_TOSTRING(true AND false OR false, "((true AND false) OR false)");
        TEST_TOSTRING(true == 2, "(true==2)");
        TEST_TOSTRING(2 > 1 AND 3 > 2, "((2>1) AND (3>2))");
        TEST_TOSTRING((3 + 5) * 3 / (6 - 2), "(((3+5)*3)/(6-2))");
        TEST_TOSTRING(76 - 100 / 20 * 4, "(76-((100/20)*4))");
        TEST_TOSTRING(8 % 2 + 1 == 1, "(((8%2)+1)==1)");
        TEST_TOSTRING(1 == 2, "(1==2)");
    }
}

TEST_F(ExpressionTest, TestExprClone) {
    auto expr = ConstantExpression::make(&pool, 1);
    auto clone = expr->clone();
    ASSERT_EQ(*clone, *expr);

    auto aexpr = ArithmeticExpression::makeAdd(
        &pool, ConstantExpression::make(&pool, 1), ConstantExpression::make(&pool, 1));
    ASSERT_EQ(*aexpr, *aexpr->clone());

    auto aggExpr =
        AggregateExpression::make(&pool, "COUNT", ConstantExpression::make(&pool, "$-.*"), true);
    ASSERT_EQ(*aggExpr, *aggExpr->clone());

    auto edgeExpr = EdgeExpression::make(&pool);
    ASSERT_EQ(*edgeExpr, *edgeExpr->clone());

    auto vertExpr = VertexExpression::make(&pool);
    ASSERT_EQ(*vertExpr, *vertExpr->clone());

    auto labelExpr = LabelExpression::make(&pool, "label");
    ASSERT_EQ(*labelExpr, *labelExpr->clone());

    auto attrExpr = AttributeExpression::make(
        &pool, LabelExpression::make(&pool, "label"), LabelExpression::make(&pool, "label"));
    ASSERT_EQ(*attrExpr, *attrExpr->clone());

    auto labelAttrExpr = LabelAttributeExpression::make(
        &pool, LabelExpression::make(&pool, "label"), ConstantExpression::make(&pool, "prop"));
    ASSERT_EQ(*labelAttrExpr, *labelAttrExpr->clone());

    auto typeCastExpr = TypeCastingExpression::make(
        &pool, Value::Type::STRING, ConstantExpression::make(&pool, 100));
    ASSERT_EQ(*typeCastExpr, *typeCastExpr->clone());

    auto fnCallExpr = FunctionCallExpression::make(&pool, "count", ArgumentList::make(&pool));
    ASSERT_EQ(*fnCallExpr, *fnCallExpr->clone());

    auto uuidExpr = UUIDExpression::make(&pool, "hello");
    ASSERT_EQ(*uuidExpr, *uuidExpr->clone());

    auto subExpr = SubscriptExpression::make(
        &pool, VariableExpression::make(&pool, "var"), ConstantExpression::make(&pool, 0));
    ASSERT_EQ(*subExpr, *subExpr->clone());

    auto *elist = ExpressionList::make(&pool);
    (*elist)
        .add(ConstantExpression::make(&pool, 12345))
        .add(ConstantExpression::make(&pool, "Hello"))
        .add(ConstantExpression::make(&pool, true));
    auto listExpr = ListExpression::make(&pool, elist);
    ASSERT_EQ(*listExpr, *listExpr->clone());

    auto setExpr = SetExpression::make(&pool, elist);
    ASSERT_EQ(*setExpr, *setExpr->clone());

    auto *mapItems = MapItemList::make(&pool);
    (*mapItems)
        .add("key1", ConstantExpression::make(&pool, 12345))
        .add("key2", ConstantExpression::make(&pool, 12345))
        .add("key3", ConstantExpression::make(&pool, "Hello"))
        .add("key4", ConstantExpression::make(&pool, true));
    auto mapExpr = MapExpression::make(&pool, mapItems);
    ASSERT_EQ(*mapExpr, *mapExpr->clone());

    auto edgePropExpr = EdgePropertyExpression::make(&pool, "edge", "prop");
    ASSERT_EQ(*edgePropExpr, *edgePropExpr->clone());

    auto tagPropExpr = TagPropertyExpression::make(&pool, "tag", "prop");
    ASSERT_EQ(*tagPropExpr, *tagPropExpr->clone());

    auto inputPropExpr = InputPropertyExpression::make(&pool, "input");
    ASSERT_EQ(*inputPropExpr, *inputPropExpr->clone());

    auto varPropExpr = VariablePropertyExpression::make(&pool, "var", "prop");
    ASSERT_EQ(*varPropExpr, *varPropExpr->clone());

    auto srcPropExpr = SourcePropertyExpression::make(&pool, "tag", "prop");
    ASSERT_EQ(*srcPropExpr, *srcPropExpr->clone());

    auto dstPropExpr = DestPropertyExpression::make(&pool, "tag", "prop");
    ASSERT_EQ(*dstPropExpr, *dstPropExpr->clone());

    auto edgeSrcIdExpr = EdgeSrcIdExpression::make(&pool, "edge");
    ASSERT_EQ(*edgeSrcIdExpr, *edgeSrcIdExpr->clone());

    auto edgeTypeExpr = EdgeTypeExpression::make(&pool, "edge");
    ASSERT_EQ(*edgeTypeExpr, *edgeTypeExpr->clone());

    auto edgeRankExpr = EdgeRankExpression::make(&pool, "edge");
    ASSERT_EQ(*edgeRankExpr, *edgeRankExpr->clone());

    auto edgeDstIdExpr = EdgeDstIdExpression::make(&pool, "edge");
    ASSERT_EQ(*edgeDstIdExpr, *edgeDstIdExpr->clone());

    auto varExpr = VariableExpression::make(&pool, "VARNAME");
    ASSERT_EQ(*varExpr, *varExpr->clone());

    auto verVarExpr =
        VersionedVariableExpression::make(&pool, "VARNAME", ConstantExpression::make(&pool, 0));
    ASSERT_EQ(*verVarExpr, *verVarExpr->clone());

    auto *cases = CaseList::make(&pool);
    cases->add(ConstantExpression::make(&pool, 3), ConstantExpression::make(&pool, 9));
    auto caseExpr = CaseExpression::make(&pool, cases);
    caseExpr->setCondition(ConstantExpression::make(&pool, 2));
    caseExpr->setDefault(ConstantExpression::make(&pool, 8));
    ASSERT_EQ(*caseExpr, *caseExpr->clone());

    auto pathBuild = PathBuildExpression::make(&pool);
    pathBuild->add(VariablePropertyExpression::make(&pool, "var1", "path_src"))
        .add(VariablePropertyExpression::make(&pool, "var1", "path_edge1"))
        .add(VariablePropertyExpression::make(&pool, "var1", "path_v1"));
    ASSERT_EQ(*pathBuild, *pathBuild->clone());

    auto argList = ArgumentList::make(&pool);
    argList->addArgument(ConstantExpression::make(&pool, 1));
    argList->addArgument(ConstantExpression::make(&pool, 5));
    auto lcExpr = ListComprehensionExpression::make(
        &pool,
        "n",
        FunctionCallExpression::make(&pool, "range", argList),
        RelationalExpression::makeGE(
            &pool, LabelExpression::make(&pool, "n"), ConstantExpression::make(&pool, 2)));
    ASSERT_EQ(*lcExpr, *lcExpr->clone());

    argList = ArgumentList::make(&pool);
    argList->addArgument(ConstantExpression::make(&pool, 1));
    argList->addArgument(ConstantExpression::make(&pool, 5));
    auto predExpr = PredicateExpression::make(
        &pool,
        "all",
        "n",
        FunctionCallExpression::make(&pool, "range", argList),
        RelationalExpression::makeGE(
            &pool, LabelExpression::make(&pool, "n"), ConstantExpression::make(&pool, 2)));
    ASSERT_EQ(*predExpr, *predExpr->clone());

    argList = ArgumentList::make(&pool);
    argList->addArgument(ConstantExpression::make(&pool, 1));
    argList->addArgument(ConstantExpression::make(&pool, 5));
    auto reduceExpr = ReduceExpression::make(
        &pool,
        "totalNum",
        ArithmeticExpression::makeMultiply(
            &pool, ConstantExpression::make(&pool, 2), ConstantExpression::make(&pool, 10)),
        "n",
        FunctionCallExpression::make(&pool, "range", argList),
        ArithmeticExpression::makeAdd(
            &pool,
            LabelExpression::make(&pool, "totalNum"),
            ArithmeticExpression::makeMultiply(
                &pool, LabelExpression::make(&pool, "n"), ConstantExpression::make(&pool, 2))));
    ASSERT_EQ(*reduceExpr, *reduceExpr->clone());
}
}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
