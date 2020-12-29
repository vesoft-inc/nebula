/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/base/Base.h"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LabelExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/PathBuildExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/expression/SubscriptExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UUIDExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/expression/CaseExpression.h"
#include "common/expression/ListComprehensionExpression.h"

namespace nebula {

TEST(ExpressionEncodeDecode, ConstantExpression) {
    ConstantExpression val1(123);
    ConstantExpression val2("Hello world");
    ConstantExpression val3(true);
    ConstantExpression val4(3.14159);
    ConstantExpression val5(Time{1, 2, 3, 4});
    ConstantExpression val6(DateTime{1, 2, 3, 4, 5, 6, 7});

    std::string encoded = Expression::encode(val1);
    auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(val1, *decoded);

    encoded = Expression::encode(val2);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(val2, *decoded);

    encoded = Expression::encode(val3);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(val3, *decoded);

    encoded = Expression::encode(val4);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(val4, *decoded);

    encoded = Expression::encode(val5);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(val5, *decoded);

    encoded = Expression::encode(val6);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(val6, *decoded);
}


TEST(ExpressionEncodeDecode, SymbolPropertyExpression) {
    SourcePropertyExpression spEx(new std::string("tag"), new std::string("prop"));
    auto encoded = Expression::encode(spEx);
    auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(spEx, *decoded);

    DestPropertyExpression dpEx(new std::string("tag"), new std::string("prop"));
    encoded = Expression::encode(dpEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(dpEx, *decoded);

    EdgeSrcIdExpression srcIdEx(new std::string("alias"));
    encoded = Expression::encode(srcIdEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(srcIdEx, *decoded);

    EdgeTypeExpression etEx(new std::string("alias"));
    encoded = Expression::encode(etEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(etEx, *decoded);

    EdgeRankExpression erEx(new std::string("alias"));
    encoded = Expression::encode(erEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(erEx, *decoded);

    EdgeDstIdExpression dstIdEx(new std::string("alias"));
    encoded = Expression::encode(dstIdEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(dstIdEx, *decoded);

    EdgePropertyExpression edgeEx(new std::string("edge"), new std::string("prop"));
    encoded = Expression::encode(edgeEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(edgeEx, *decoded);
}


TEST(ExpressionEncodeDecode, ArithmeticExpression) {
    ArithmeticExpression addEx(Expression::Kind::kAdd,
                               new ConstantExpression(123),
                               new ConstantExpression("Hello"));
    std::string encoded = Expression::encode(addEx);
    auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(addEx, *decoded);

    ArithmeticExpression minusEx(Expression::Kind::kMinus,
                                 new ConstantExpression(3.14),
                                 new ConstantExpression("Hello"));
    encoded = Expression::encode(minusEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(minusEx, *decoded);

    ArithmeticExpression multiEx(Expression::Kind::kMultiply,
                                 new ConstantExpression(3.14),
                                 new ConstantExpression(1234));
    encoded = Expression::encode(multiEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(multiEx, *decoded);

    ArithmeticExpression divEx(Expression::Kind::kDivision,
                               new ConstantExpression(3.14),
                               new ConstantExpression(1234));
    encoded = Expression::encode(divEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(divEx, *decoded);

    ArithmeticExpression modEx(Expression::Kind::kDivision,
                               new ConstantExpression(1234567),
                               new ConstantExpression(123));
    encoded = Expression::encode(modEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(modEx, *decoded);
}


TEST(ExpressionEncodeDecode, FunctionCallExpression) {
    ArgumentList* args = new ArgumentList();
    args->addArgument(std::make_unique<ConstantExpression>(123));
    args->addArgument(std::make_unique<ConstantExpression>(3.14));
    args->addArgument(std::make_unique<ConstantExpression>("Hello world"));
    FunctionCallExpression fcEx(new std::string("func"), args);
    std::string encoded = Expression::encode(fcEx);
    auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(fcEx, *decoded);
}


TEST(ExpressionEncodeDecode, RelationalExpression) {
    RelationalExpression eqEx(Expression::Kind::kRelEQ,
                              new ConstantExpression(123),
                              new ConstantExpression(123));
    std::string encoded = Expression::encode(eqEx);
    auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(eqEx, *decoded);

    RelationalExpression neEx(Expression::Kind::kRelEQ,
                              new ConstantExpression(123),
                              new ConstantExpression("Hello"));
    encoded = Expression::encode(neEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(neEx, *decoded);

    RelationalExpression ltEx(Expression::Kind::kRelEQ,
                              new ConstantExpression(123),
                              new ConstantExpression(12345));
    encoded = Expression::encode(ltEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(ltEx, *decoded);

    RelationalExpression leEx(Expression::Kind::kRelEQ,
                              new ConstantExpression(123),
                              new ConstantExpression(12345));
    encoded = Expression::encode(leEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(leEx, *decoded);

    RelationalExpression gtEx(Expression::Kind::kRelEQ,
                              new ConstantExpression(12345),
                              new ConstantExpression(123));
    encoded = Expression::encode(gtEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(gtEx, *decoded);

    RelationalExpression geEx(Expression::Kind::kRelEQ,
                              new ConstantExpression(12345),
                              new ConstantExpression(123));
    encoded = Expression::encode(geEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(geEx, *decoded);

    RelationalExpression containEx(Expression::Kind::kContains,
                              new ConstantExpression("12345"),
                              new ConstantExpression("123"));
    encoded = Expression::encode(containEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(containEx, *decoded);
}


TEST(ExpressionEncodeDecode, LogicalExpression) {
    LogicalExpression andEx(Expression::Kind::kLogicalAnd,
                            new ConstantExpression(true),
                            new ConstantExpression(false));
    auto decoded = Expression::decode(Expression::encode(andEx));
    EXPECT_EQ(andEx, *decoded);

    auto lhs = new RelationalExpression(
        Expression::Kind::kRelLT,
        new ConstantExpression(123),
        new ConstantExpression(12345));
    auto rhs = new RelationalExpression(
        Expression::Kind::kRelEQ,
        new ConstantExpression("Hello"),
        new ConstantExpression("World"));
    LogicalExpression orEx(Expression::Kind::kLogicalOr, lhs, rhs);
    decoded = Expression::decode(Expression::encode(orEx));
    EXPECT_EQ(orEx, *decoded);

    auto arEx = new ArithmeticExpression(
        Expression::Kind::kAdd,
        new ConstantExpression(123),
        new ConstantExpression(456));
    lhs = new RelationalExpression(
        Expression::Kind::kRelLT,
        new ConstantExpression(12345),
        arEx);
    rhs = new RelationalExpression(
        Expression::Kind::kRelEQ,
        new ConstantExpression("Hello"),
        new ConstantExpression("World"));
    LogicalExpression xorEx(Expression::Kind::kLogicalXor, lhs, rhs);
    decoded = Expression::decode(Expression::encode(xorEx));
    EXPECT_EQ(xorEx, *decoded);
}


TEST(ExpressionEncodeDecode, TypeCastingExpression) {
    TypeCastingExpression tcEx(Value::Type::INT,
                               new ConstantExpression(3.14));
    std::string encoded = Expression::encode(tcEx);
    auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(tcEx, *decoded);
}


TEST(ExpressionEncodeDecode, UUIDExpression) {
    UUIDExpression uuidEx(new std::string("field"));
    std::string encoded = Expression::encode(uuidEx);
    auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(uuidEx, *decoded);
}


TEST(ExpressionEncodeDecode, UnaryExpression) {
    UnaryExpression plusEx(Expression::Kind::kUnaryPlus,
                           new ConstantExpression(12345));
    std::string encoded = Expression::encode(plusEx);
    auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(plusEx, *decoded);

    UnaryExpression negEx(Expression::Kind::kUnaryNegate,
                          new ConstantExpression(12345));
    encoded = Expression::encode(negEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(negEx, *decoded);

    UnaryExpression notEx(Expression::Kind::kUnaryNot,
                          new ConstantExpression(false));
    encoded = Expression::encode(notEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(notEx, *decoded);
}


TEST(ExpressionEncodeDecode, ListExpression) {
    auto *list = new ExpressionList();
    (*list).add(new ConstantExpression(1))
           .add(new ConstantExpression("Hello"))
           .add(new ConstantExpression(true));
    auto origin = std::make_unique<ListExpression>(list);
    auto decoded = Expression::decode(Expression::encode(*origin));
    ASSERT_EQ(*origin, *decoded);
}


TEST(ExpressionEncodeDecode, SetExpression) {
    auto *list = new ExpressionList();
    (*list).add(new ConstantExpression(1))
           .add(new ConstantExpression("Hello"))
           .add(new ConstantExpression(true));
    auto origin = std::make_unique<SetExpression>(list);
    auto decoded = Expression::decode(Expression::encode(*origin));
    ASSERT_EQ(*origin, *decoded);
}


TEST(ExpressionEncodeDecode, MapExpression) {
    auto *list = new MapItemList();
    (*list).add(new std::string("key1"), new ConstantExpression(1))
           .add(new std::string("key2"), new ConstantExpression(2))
           .add(new std::string("key3"), new ConstantExpression(3));
    auto origin = std::make_unique<MapExpression>(list);
    auto decoded = Expression::decode(Expression::encode(*origin));
    ASSERT_EQ(*origin, *decoded);
}


TEST(ExpressionEncodeDecode, SubscriptExpression) {
    auto *left = new ConstantExpression(1);
    auto *right = new ConstantExpression(2);
    auto origin = std::make_unique<SubscriptExpression>(left, right);
    auto decoded = Expression::decode(Expression::encode(*origin));
    ASSERT_EQ(*origin, *decoded);
}

TEST(ExpressionEncodeDecode, LabelExpression) {
    auto origin = std::make_unique<LabelExpression>(new std::string("name"));
    auto decoded = Expression::decode(Expression::encode(*origin));
    ASSERT_EQ(*origin, *decoded);
}

TEST(ExpressionEncodeDecode, CaseExpression) {
    {
        // CASE 23 WHEN 24 THEN 1 END
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(24), new ConstantExpression(1));
        auto origin = std::make_unique<CaseExpression>(cases);
        origin->setCondition(new ConstantExpression(23));
        std::string encoded = Expression::encode(*origin);
        auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
    {
        // CASE 23 WHEN 24 THEN 1 ELSE false END
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(24), new ConstantExpression(1));
        auto origin = std::make_unique<CaseExpression>(cases);
        origin->setCondition(new ConstantExpression(23));
        origin->setDefault(new ConstantExpression(false));
        std::string encoded = Expression::encode(*origin);
        auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }

    {
        // CASE ("nebula" STARTS WITH "nebu") WHEN false THEN 1 WHEN true THEN 2 ELSE 3 END
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(false), new ConstantExpression(1));
        cases->add(new ConstantExpression(true), new ConstantExpression(2));
        auto origin = std::make_unique<CaseExpression>(cases);
        origin->setCondition(new RelationalExpression(Expression::Kind::kStartsWith,
                                                    new ConstantExpression("nebula"),
                                                    new ConstantExpression("nebu")));
        origin->setDefault(new ConstantExpression(3));
        std::string encoded = Expression::encode(*origin);
        auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
    {
        // CASE (3+5) WHEN 7 THEN 1 WHEN 8 THEN 2 WHEN 8 THEN "jack" ELSE "no" END
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(7), new ConstantExpression(1));
        cases->add(new ConstantExpression(8), new ConstantExpression(2));
        cases->add(new ConstantExpression(8), new ConstantExpression("jack"));
        auto origin = std::make_unique<CaseExpression>(cases);
        origin->setCondition(new ArithmeticExpression(
            Expression::Kind::kAdd, new ConstantExpression(3), new ConstantExpression(5)));
        origin->setDefault(new ConstantExpression("no"));
        std::string encoded = Expression::encode(*origin);
        auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
    {
        // CASE WHEN false THEN 18 END
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(false), new ConstantExpression(18));
        auto origin = std::make_unique<CaseExpression>(cases);
        std::string encoded = Expression::encode(*origin);
        auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
    {
        // CASE WHEN false THEN 18 ELSE ok END
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(false), new ConstantExpression(18));
        auto origin = std::make_unique<CaseExpression>(cases);
        origin->setDefault(new ConstantExpression("ok"));
        std::string encoded = Expression::encode(*origin);
        auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
    {
        // CASE WHEN "invalid when" THEN "no" ELSE 3 END
        auto *cases = new CaseList();
        cases->add(new ConstantExpression("invalid when"), new ConstantExpression("no"));
        auto origin = std::make_unique<CaseExpression>(cases);
        origin->setDefault(new ConstantExpression(3));
        std::string encoded = Expression::encode(*origin);
        auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
    {
        // CASE WHEN (23<17) THEN 1 WHEN (37==37) THEN 2 WHEN (45!=99) THEN 3 ELSE 4 END
        auto *cases = new CaseList();
        cases->add(
            new RelationalExpression(
                Expression::Kind::kRelLT, new ConstantExpression(23), new ConstantExpression(17)),
            new ConstantExpression(1));
        cases->add(
            new RelationalExpression(
                Expression::Kind::kRelEQ, new ConstantExpression(37), new ConstantExpression(37)),
            new ConstantExpression(2));
        cases->add(
            new RelationalExpression(
                Expression::Kind::kRelNE, new ConstantExpression(45), new ConstantExpression(99)),
            new ConstantExpression(3));
        auto origin = std::make_unique<CaseExpression>(cases);
        origin->setDefault(new ConstantExpression(4));
        std::string encoded = Expression::encode(*origin);
        auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
    {
        // ((23<17) ? 1 : 2)
        auto *cases = new CaseList();
        cases->add(
            new RelationalExpression(
                Expression::Kind::kRelLT, new ConstantExpression(23), new ConstantExpression(17)),
            new ConstantExpression(1));
        auto origin = std::make_unique<CaseExpression>(cases, false);
        origin->setDefault(new ConstantExpression(2));
        std::string encoded = Expression::encode(*origin);
        auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
    {
        // (false ? 1 : "ok")
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(false), new ConstantExpression(1));
        auto origin = std::make_unique<CaseExpression>(cases, false);
        origin->setDefault(new ConstantExpression("ok"));
        std::string encoded = Expression::encode(*origin);
        auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
        EXPECT_EQ(*origin, *decoded);
    }
}

TEST(ExpressionEncodeDecode, PathBuildExpression) {
    auto origin = std::make_unique<PathBuildExpression>();
    (*origin)
        .add(std::make_unique<LabelExpression>(new std::string("path_src")))
        .add(std::make_unique<LabelExpression>(new std::string("path_edge1")))
        .add(std::make_unique<LabelExpression>(new std::string("path_v1")));
    auto decoded = Expression::decode(Expression::encode(*origin));
    ASSERT_EQ(*origin, *decoded);
}

TEST(ExpressionEncodeDecode, ListComprehensionExpression) {
    {
        ArgumentList *argList = new ArgumentList();
        argList->addArgument(std::make_unique<ConstantExpression>(1));
        argList->addArgument(std::make_unique<ConstantExpression>(5));
        auto origin = std::make_unique<ListComprehensionExpression>(
            new std::string("n"),
            new FunctionCallExpression(new std::string("range"), argList),
            new RelationalExpression(
                Expression::Kind::kRelGE,
                new LabelExpression(new std::string("n")),
                new ConstantExpression(2)));
        auto decoded = Expression::decode(Expression::encode(*origin));
        ASSERT_EQ(*origin, *decoded);
    }
    {
        ArgumentList *argList = new ArgumentList();
        argList->addArgument(std::make_unique<LabelExpression>(new std::string("p")));
        auto origin = std::make_unique<ListComprehensionExpression>(
            new std::string("n"),
            new FunctionCallExpression(new std::string("nodes"), argList),
            nullptr,
            new ArithmeticExpression(
                Expression::Kind::kAdd,
                new LabelExpression(new std::string("n")),
                new ConstantExpression(10)));
        auto decoded = Expression::decode(Expression::encode(*origin));
        ASSERT_EQ(*origin, *decoded);
    }
    {
        ArgumentList *argList = new ArgumentList();
        argList->addArgument(std::make_unique<ConstantExpression>(1));
        argList->addArgument(std::make_unique<ConstantExpression>(5));
        auto origin = std::make_unique<ListComprehensionExpression>(
            new std::string("n"),
            new FunctionCallExpression(new std::string("range"), argList),
            new RelationalExpression(Expression::Kind::kRelGE,
                                    new LabelExpression(new std::string("n")),
                                    new ConstantExpression(2)),
            new ArithmeticExpression(Expression::Kind::kAdd,
                                    new LabelExpression(new std::string("n")),
                                    new ConstantExpression(10)));

        auto decoded = Expression::decode(Expression::encode(*origin));
        ASSERT_EQ(*origin, *decoded);
    }
}

}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
