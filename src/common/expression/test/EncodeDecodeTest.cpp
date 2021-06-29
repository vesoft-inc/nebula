/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/test/TestBase.h"

namespace nebula {

TEST(ExpressionEncodeDecode, ConstantExpression) {
    const auto& val1 = *ConstantExpression::make(&pool, 123);
    const auto& val2 = *ConstantExpression::make(&pool, "Hello world");
    const auto& val3 = *ConstantExpression::make(&pool, true);
    const auto& val4 = *ConstantExpression::make(&pool, 3.14159);
    const auto& val5 = *ConstantExpression::make(&pool, Time{1, 2, 3, 4});
    const auto& val6 = *ConstantExpression::make(&pool, DateTime{1, 2, 3, 4, 5, 6, 7});

    std::string encoded = Expression::encode(val1);
    auto decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(val1, *decoded);

    encoded = Expression::encode(val2);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(val2, *decoded);

    encoded = Expression::encode(val3);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(val3, *decoded);

    encoded = Expression::encode(val4);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(val4, *decoded);

    encoded = Expression::encode(val5);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(val5, *decoded);

    encoded = Expression::encode(val6);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(val6, *decoded);
}

TEST(ExpressionEncodeDecode, SymbolPropertyExpression) {
    const auto& spEx = *SourcePropertyExpression::make(&pool, "tag", "prop");
    auto encoded = Expression::encode(spEx);
    auto decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(spEx, *decoded);

    const auto& dpEx = *DestPropertyExpression::make(&pool, "tag", "prop");
    encoded = Expression::encode(dpEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(dpEx, *decoded);

    const auto& srcIdEx = *EdgeSrcIdExpression::make(&pool, "alias");
    encoded = Expression::encode(srcIdEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(srcIdEx, *decoded);

    const auto& etEx = *EdgeTypeExpression::make(&pool, "alias");
    encoded = Expression::encode(etEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(etEx, *decoded);

    const auto& erEx = *EdgeRankExpression::make(&pool, "alias");
    encoded = Expression::encode(erEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(erEx, *decoded);

    const auto& dstIdEx = *EdgeDstIdExpression::make(&pool, "alias");
    encoded = Expression::encode(dstIdEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(dstIdEx, *decoded);

    const auto& edgeEx = *EdgePropertyExpression::make(&pool, "edge", "prop");
    encoded = Expression::encode(edgeEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(edgeEx, *decoded);
}

TEST(ExpressionEncodeDecode, ArithmeticExpression) {
    auto addEx = ArithmeticExpression::makeAdd(
        &pool, ConstantExpression::make(&pool, 123), ConstantExpression::make(&pool, "Hello"));
    std::string encoded = Expression::encode(*addEx);
    auto decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(*addEx, *decoded);

    auto minusEx = ArithmeticExpression::makeMinus(
        &pool, ConstantExpression::make(&pool, 3.14), ConstantExpression::make(&pool, "Hello"));
    encoded = Expression::encode(*minusEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(*minusEx, *decoded);

    auto multiEx = ArithmeticExpression::makeMinus(
        &pool, ConstantExpression::make(&pool, 3.14), ConstantExpression::make(&pool, 1234));
    encoded = Expression::encode(*multiEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(*multiEx, *decoded);

    auto divEx = ArithmeticExpression::makeDivision(
        &pool, ConstantExpression::make(&pool, 3.14), ConstantExpression::make(&pool, 1234));
    encoded = Expression::encode(*divEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(*divEx, *decoded);

    auto modEx = ArithmeticExpression::makeMod(
        &pool, ConstantExpression::make(&pool, 1234567), ConstantExpression::make(&pool, 123));
    encoded = Expression::encode(*modEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(*modEx, *decoded);
}

TEST(ExpressionEncodeDecode, FunctionCallExpression) {
    ArgumentList *args = ArgumentList::make(&pool);
    args->addArgument(ConstantExpression::make(&pool, 123));
    args->addArgument(ConstantExpression::make(&pool, 3.14));
    args->addArgument(ConstantExpression::make(&pool, "Hello world"));
    const auto& fcEx = *FunctionCallExpression::make(&pool, "func", args);
    std::string encoded = Expression::encode(fcEx);
    auto decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(fcEx, *decoded);
}

TEST(ExpressionEncodeDecode, AggregateExpression) {
    auto aggEx =
        AggregateExpression::make(&pool, "COUNT", ConstantExpression::make(&pool, 123), true);
    std::string encoded = Expression::encode(*aggEx);
    auto decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(*aggEx, *decoded);
}

TEST(ExpressionEncodeDecode, RelationalExpression) {
    const auto& eqEx = *RelationalExpression::makeEQ(
        &pool, ConstantExpression::make(&pool, 123), ConstantExpression::make(&pool, 123));
    std::string encoded = Expression::encode(eqEx);
    auto decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(eqEx, *decoded);

    const auto& neEx = *RelationalExpression::makeNE(
        &pool, ConstantExpression::make(&pool, 123), ConstantExpression::make(&pool, "Hello"));
    encoded = Expression::encode(neEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(neEx, *decoded);

    const auto& ltEx = *RelationalExpression::makeLE(
        &pool, ConstantExpression::make(&pool, 123), ConstantExpression::make(&pool, 12345));
    encoded = Expression::encode(ltEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(ltEx, *decoded);

    const auto& leEx = *RelationalExpression::makeLE(
        &pool, ConstantExpression::make(&pool, 123), ConstantExpression::make(&pool, 12345));
    encoded = Expression::encode(leEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(leEx, *decoded);

    const auto& gtEx = *RelationalExpression::makeGT(
        &pool, ConstantExpression::make(&pool, 12345), ConstantExpression::make(&pool, 123));
    encoded = Expression::encode(gtEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(gtEx, *decoded);

    const auto& geEx = *RelationalExpression::makeGE(
        &pool, ConstantExpression::make(&pool, 12345), ConstantExpression::make(&pool, 123));
    encoded = Expression::encode(geEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(geEx, *decoded);

    const auto& containEx = *RelationalExpression::makeContains(
        &pool, ConstantExpression::make(&pool, "12345"), ConstantExpression::make(&pool, "123"));
    encoded = Expression::encode(containEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(containEx, *decoded);
}

TEST(ExpressionEncodeDecode, LogicalExpression) {
    const auto& andEx = *LogicalExpression::makeAnd(
        &pool, ConstantExpression::make(&pool, true), ConstantExpression::make(&pool, false));
    auto decoded = Expression::decode(&pool, Expression::encode(andEx));
    EXPECT_EQ(andEx, *decoded);

    auto lhs = RelationalExpression::makeLT(
        &pool, ConstantExpression::make(&pool, 123), ConstantExpression::make(&pool, 12345));
    auto rhs = RelationalExpression::makeEQ(
        &pool, ConstantExpression::make(&pool, "Hello"), ConstantExpression::make(&pool, "World"));
    const auto& orEx = *LogicalExpression::makeOr(&pool, lhs, rhs);
    decoded = Expression::decode(&pool, Expression::encode(orEx));
    EXPECT_EQ(orEx, *decoded);

    auto arEx = ArithmeticExpression::makeAdd(
        &pool, ConstantExpression::make(&pool, 123), ConstantExpression::make(&pool, 456));
    lhs = RelationalExpression::makeLT(&pool, ConstantExpression::make(&pool, 12345), arEx);
    rhs = RelationalExpression::makeEQ(
        &pool, ConstantExpression::make(&pool, "Hello"), ConstantExpression::make(&pool, "World"));
    const auto& xorEx = *LogicalExpression::makeXor(&pool, lhs, rhs);
    decoded = Expression::decode(&pool, Expression::encode(xorEx));
    EXPECT_EQ(xorEx, *decoded);
}

TEST(ExpressionEncodeDecode, TypeCastingExpression) {
    const auto& tcEx = *TypeCastingExpression::make(
        &pool, Value::Type::INT, ConstantExpression::make(&pool, 3.14));
    std::string encoded = Expression::encode(tcEx);
    auto decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(tcEx, *decoded);
}

TEST(ExpressionEncodeDecode, UUIDExpression) {
    const auto& uuidEx = *UUIDExpression::make(&pool, "field");
    std::string encoded = Expression::encode(uuidEx);
    auto decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(uuidEx, *decoded);
}

TEST(ExpressionEncodeDecode, UnaryExpression) {
    const auto& plusEx = *UnaryExpression::makePlus(&pool, ConstantExpression::make(&pool, 12345));
    std::string encoded = Expression::encode(plusEx);
    auto decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(plusEx, *decoded);

    const auto& negEx = *UnaryExpression::makeNegate(&pool, ConstantExpression::make(&pool, 12345));
    encoded = Expression::encode(negEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(negEx, *decoded);

    const auto& notEx = *UnaryExpression::makeNot(&pool, ConstantExpression::make(&pool, false));
    encoded = Expression::encode(notEx);
    decoded = Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(notEx, *decoded);
}

TEST(ExpressionEncodeDecode, ListExpression) {
    auto list = ExpressionList::make(&pool);
    (*list)
        .add(ConstantExpression::make(&pool, 1))
        .add(ConstantExpression::make(&pool, "Hello"))
        .add(ConstantExpression::make(&pool, true));
    auto origin = ListExpression::make(&pool, list);
    auto decoded = Expression::decode(&pool, Expression::encode(*origin));
    ASSERT_EQ(*origin, *decoded);
}

TEST(ExpressionEncodeDecode, SetExpression) {
    auto list = ExpressionList::make(&pool);
    (*list)
        .add(ConstantExpression::make(&pool, 1))
        .add(ConstantExpression::make(&pool, "Hello"))
        .add(ConstantExpression::make(&pool, true));
    auto origin = SetExpression::make(&pool, list);
    auto decoded = Expression::decode(&pool, Expression::encode(*origin));
    ASSERT_EQ(*origin, *decoded);
}

TEST(ExpressionEncodeDecode, MapExpression) {
    auto list = MapItemList::make(&pool);
    (*list)
        .add("key1", ConstantExpression::make(&pool, 1))
        .add("key2", ConstantExpression::make(&pool, 2))
        .add("key3", ConstantExpression::make(&pool, 3));
    auto origin = MapExpression::make(&pool, list);
    auto decoded = Expression::decode(&pool, Expression::encode(*origin));
    ASSERT_EQ(*origin, *decoded);
}

TEST(ExpressionEncodeDecode, SubscriptExpression) {
    auto *left = ConstantExpression::make(&pool, 1);
    auto *right = ConstantExpression::make(&pool, 2);
    auto origin = SubscriptExpression::make(&pool, left, right);
    auto decoded = Expression::decode(&pool, Expression::encode(*origin));
    ASSERT_EQ(*origin, *decoded);
}

TEST(ExpressionEncodeDecode, SubscriptRangeExpression) {
    {
        auto *list = ListExpression::make(&pool);
        auto *left = ConstantExpression::make(&pool, 1);
        auto *right = ConstantExpression::make(&pool, 2);
        auto origin = SubscriptRangeExpression::make(&pool, list, left, right);
        auto decoded = Expression::decode(&pool, Expression::encode(*origin));
        ASSERT_EQ(*origin, *decoded);
    }
    {
        auto *list = ListExpression::make(&pool);
        Expression *left = nullptr;
        auto *right = ConstantExpression::make(&pool, 2);
        auto origin = SubscriptRangeExpression::make(&pool, list, left, right);
        auto decoded = Expression::decode(&pool, Expression::encode(*origin));
        ASSERT_EQ(*origin, *decoded);
    }
    {
        auto *list = ListExpression::make(&pool);
        auto *left = ConstantExpression::make(&pool, 1);
        Expression *right = nullptr;
        auto origin = SubscriptRangeExpression::make(&pool, list, left, right);
        auto decoded = Expression::decode(&pool, Expression::encode(*origin));
        ASSERT_EQ(*origin, *decoded);
    }
}

TEST(ExpressionEncodeDecode, LabelExpression) {
    auto origin = LabelExpression::make(&pool, "name");
    auto decoded = Expression::decode(&pool, Expression::encode(*origin));
    ASSERT_EQ(*origin, *decoded);
}

TEST(ExpressionEncodeDecode, CaseExpression) {
    {
        // CASE 23 WHEN 24 THEN 1 END
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, 24), ConstantExpression::make(&pool, 1));
        auto origin = CaseExpression::make(&pool, cases);
        origin->setCondition(ConstantExpression::make(&pool, 23));
        std::string encoded = Expression::encode(*origin);
        auto decoded =
            Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
    {
        // CASE 23 WHEN 24 THEN 1 ELSE false END
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, 24), ConstantExpression::make(&pool, 1));
        auto origin = CaseExpression::make(&pool, cases);
        origin->setCondition(ConstantExpression::make(&pool, 23));
        origin->setDefault(ConstantExpression::make(&pool, false));
        std::string encoded = Expression::encode(*origin);
        auto decoded =
            Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }

    {
        // CASE ("nebula" STARTS WITH "nebu") WHEN false THEN 1 WHEN true THEN 2 ELSE 3 END
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, false), ConstantExpression::make(&pool, 1));
        cases->add(ConstantExpression::make(&pool, true), ConstantExpression::make(&pool, 2));
        auto origin = CaseExpression::make(&pool, cases);
        origin->setCondition(
            RelationalExpression::makeStartsWith(&pool,
                                                 ConstantExpression::make(&pool, "nebula"),
                                                 ConstantExpression::make(&pool, "nebu")));
        origin->setDefault(ConstantExpression::make(&pool, 3));
        std::string encoded = Expression::encode(*origin);
        auto decoded =
            Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
    {
        // CASE (3+5) WHEN 7 THEN 1 WHEN 8 THEN 2 WHEN 8 THEN "jack" ELSE "no" END
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, 7), ConstantExpression::make(&pool, 1));
        cases->add(ConstantExpression::make(&pool, 8), ConstantExpression::make(&pool, 2));
        cases->add(ConstantExpression::make(&pool, 8), ConstantExpression::make(&pool, "jack"));
        auto origin = CaseExpression::make(&pool, cases);
        origin->setCondition(ArithmeticExpression::makeAdd(
            &pool, ConstantExpression::make(&pool, 3), ConstantExpression::make(&pool, 5)));
        origin->setDefault(ConstantExpression::make(&pool, "no"));
        std::string encoded = Expression::encode(*origin);
        auto decoded =
            Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
    {
        // CASE WHEN false THEN 18 END
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, false), ConstantExpression::make(&pool, 18));
        auto origin = CaseExpression::make(&pool, cases);
        std::string encoded = Expression::encode(*origin);
        auto decoded =
            Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
    {
        // CASE WHEN false THEN 18 ELSE ok END
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, false), ConstantExpression::make(&pool, 18));
        auto origin = CaseExpression::make(&pool, cases);
        origin->setDefault(ConstantExpression::make(&pool, "ok"));
        std::string encoded = Expression::encode(*origin);
        auto decoded =
            Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
    {
        // CASE WHEN "invalid when" THEN "no" ELSE 3 END
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, "invalid when"),
                   ConstantExpression::make(&pool, "no"));
        auto origin = CaseExpression::make(&pool, cases);
        origin->setDefault(ConstantExpression::make(&pool, 3));
        std::string encoded = Expression::encode(*origin);
        auto decoded =
            Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
    {
        // CASE WHEN (23<17) THEN 1 WHEN (37==37) THEN 2 WHEN (45!=99) THEN 3 ELSE 4 END
        auto *cases = CaseList::make(&pool);
        cases->add(
            RelationalExpression::makeLT(
                &pool, ConstantExpression::make(&pool, 23), ConstantExpression::make(&pool, 17)),
            ConstantExpression::make(&pool, 1));
        cases->add(
            RelationalExpression::makeEQ(
                &pool, ConstantExpression::make(&pool, 37), ConstantExpression::make(&pool, 37)),
            ConstantExpression::make(&pool, 2));
        cases->add(
            RelationalExpression::makeNE(
                &pool, ConstantExpression::make(&pool, 45), ConstantExpression::make(&pool, 99)),
            ConstantExpression::make(&pool, 3));
        auto origin = CaseExpression::make(&pool, cases);
        origin->setDefault(ConstantExpression::make(&pool, 4));
        std::string encoded = Expression::encode(*origin);
        auto decoded =
            Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
    {
        // ((23<17) ? 1 : 2)
        auto *cases = CaseList::make(&pool);
        cases->add(
            RelationalExpression::makeLT(
                &pool, ConstantExpression::make(&pool, 23), ConstantExpression::make(&pool, 17)),
            ConstantExpression::make(&pool, 1));
        auto origin = CaseExpression::make(&pool, cases, false);
        origin->setDefault(ConstantExpression::make(&pool, 2));
        std::string encoded = Expression::encode(*origin);
        auto decoded =
            Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
    {
        // (false ? 1 : "ok")
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, false), ConstantExpression::make(&pool, 1));
        auto origin = CaseExpression::make(&pool, cases, false);
        origin->setDefault(ConstantExpression::make(&pool, "ok"));
        std::string encoded = Expression::encode(*origin);
        auto decoded =
            Expression::decode(&pool, folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
}

TEST(ExpressionEncodeDecode, PredicateExpression) {
    {
        // all(n IN range(1, 5) WHERE n >= 2)
        ArgumentList *argList = ArgumentList::make(&pool);
        argList->addArgument(ConstantExpression::make(&pool, 1));
        argList->addArgument(ConstantExpression::make(&pool, 5));
        auto origin = PredicateExpression::make(
            &pool,
            "all",
            "n",
            FunctionCallExpression::make(&pool, "range", argList),
            RelationalExpression::makeGE(
                &pool, LabelExpression::make(&pool, "n"), ConstantExpression::make(&pool, 2)));
        auto decoded = Expression::decode(&pool, Expression::encode(*origin));
        ASSERT_EQ(*origin, *decoded);
    }
}

TEST(ExpressionEncodeDecode, ReduceExpression) {
    {
        // reduce(totalNum = 2 * 10, n IN range(1, 5) | totalNum + n * 2)
        ArgumentList *argList = ArgumentList::make(&pool);
        argList->addArgument(ConstantExpression::make(&pool, 1));
        argList->addArgument(ConstantExpression::make(&pool, 5));
        auto origin = ReduceExpression::make(
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
        auto decoded = Expression::decode(&pool, Expression::encode(*origin));
        ASSERT_EQ(*origin, *decoded);
    }
}

TEST(ExpressionEncodeDecode, PathBuildExpression) {
    auto origin = PathBuildExpression::make(&pool);
    (*origin)
        .add(LabelExpression::make(&pool, "path_src"))
        .add(LabelExpression::make(&pool, "path_edge1"))
        .add(LabelExpression::make(&pool, "path_v1"));
    auto decoded = Expression::decode(&pool, Expression::encode(*origin));
    ASSERT_EQ(*origin, *decoded);
}

TEST(ExpressionEncodeDecode, ListComprehensionExpression) {
    {
        ArgumentList *argList = ArgumentList::make(&pool);
        argList->addArgument(ConstantExpression::make(&pool, 1));
        argList->addArgument(ConstantExpression::make(&pool, 5));
        auto origin = ListComprehensionExpression::make(
            &pool,
            "n",
            FunctionCallExpression::make(&pool, "range", argList),
            RelationalExpression::makeGE(
                &pool, LabelExpression::make(&pool, "n"), ConstantExpression::make(&pool, 2)));
        auto decoded = Expression::decode(&pool, Expression::encode(*origin));
        ASSERT_EQ(*origin, *decoded);
    }
    {
        ArgumentList *argList = ArgumentList::make(&pool);
        argList->addArgument(LabelExpression::make(&pool, "p"));
        auto origin = ListComprehensionExpression::make(
            &pool,
            "n",
            FunctionCallExpression::make(&pool, "nodes", argList),
            nullptr,
            ArithmeticExpression::makeAdd(
                &pool, LabelExpression::make(&pool, "n"), ConstantExpression::make(&pool, 10)));
        auto decoded = Expression::decode(&pool, Expression::encode(*origin));
        ASSERT_EQ(*origin, *decoded);
    }
    {
        ArgumentList *argList = ArgumentList::make(&pool);
        argList->addArgument(ConstantExpression::make(&pool, 1));
        argList->addArgument(ConstantExpression::make(&pool, 5));
        auto origin = ListComprehensionExpression::make(
            &pool,
            "n",
            FunctionCallExpression::make(&pool, "range", argList),
            RelationalExpression::makeGE(
                &pool, LabelExpression::make(&pool, "n"), ConstantExpression::make(&pool, 2)),
            ArithmeticExpression::makeAdd(
                &pool, LabelExpression::make(&pool, "n"), ConstantExpression::make(&pool, 10)));

        auto decoded = Expression::decode(&pool, Expression::encode(*origin));
        ASSERT_EQ(*origin, *decoded);
    }
}

}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
