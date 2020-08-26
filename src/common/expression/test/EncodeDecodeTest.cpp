/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/expression/PropertyExpression.h"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/expression/SubscriptExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UUIDExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/LabelExpression.h"

namespace nebula {

TEST(ExpressionEncodeDecode, ConstantExpression) {
    ConstantExpression val1(123);
    ConstantExpression val2("Hello world");
    ConstantExpression val3(true);
    ConstantExpression val4(3.14159);

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
}


TEST(ExpressionEncodeDecode, SymbolPropertyExpression) {
    InputPropertyExpression inputEx(new std::string("prop"));
    auto encoded = Expression::encode(inputEx);
    auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(inputEx, *decoded);

    VariablePropertyExpression varEx(new std::string("var"), new std::string("prop"));
    encoded = Expression::encode(varEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(varEx, *decoded);

    SourcePropertyExpression spEx(new std::string("tag"), new std::string("prop"));
    encoded = Expression::encode(spEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
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
    std::string encoded = Expression::encode(andEx);
    auto decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
    EXPECT_EQ(andEx, *decoded);

    auto lhs = new RelationalExpression(
        Expression::Kind::kRelLT,
        new ConstantExpression(123),
        new ConstantExpression(12345));
    auto rhs = new RelationalExpression(
        Expression::Kind::kRelEQ,
        new ConstantExpression("Hello"),
        new ConstantExpression("World"));
    RelationalExpression orEx(Expression::Kind::kLogicalOr, lhs, rhs);
    encoded = Expression::encode(orEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
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
    RelationalExpression xorEx(Expression::Kind::kLogicalXor, lhs, rhs);
    encoded = Expression::encode(xorEx);
    decoded = Expression::decode(folly::StringPiece(encoded.data(), encoded.size()));
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

}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

