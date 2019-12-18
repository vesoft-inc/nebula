/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "filter/FunctionManager.h"
#include "parser/GQLParser.h"
#include "parser/SequentialSentences.h"

namespace nebula {

class ExpressionTest : public testing::Test {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    Expression* getFilterExpr(SequentialSentences *sentences);
};


void ExpressionTest::SetUp() {
}


void ExpressionTest::TearDown() {
}


Expression* ExpressionTest::getFilterExpr(SequentialSentences *sentences) {
    auto *go = dynamic_cast<GoSentence*>(sentences->sentences().front());
    return go->whereClause()->filter();
}


TEST_F(ExpressionTest, LiteralConstants) {
    GQLParser parser;
#define TEST_EXPR(expr_arg, type)                                       \
    do {                                                                \
        std::string query = "GO FROM 1 OVER follow WHERE " #expr_arg;   \
        auto parsed = parser.parse(query);                              \
        ASSERT_TRUE(parsed.ok()) << parsed.status();                    \
        auto *expr = getFilterExpr(parsed.value().get());               \
        ASSERT_NE(nullptr, expr);                                       \
        auto value = expr->eval();                                      \
        ASSERT_TRUE(value.ok());                                        \
        auto v = value.value();                                         \
        ASSERT_TRUE(Expression::is##type(v));                           \
        if (#type == std::string("Double")) {                           \
            ASSERT_DOUBLE_EQ((expr_arg), Expression::as##type(v));      \
        } else {                                                        \
            ASSERT_EQ((expr_arg), Expression::as##type(v));             \
        }                                                               \
        auto decoded = Expression::decode(Expression::encode(expr));    \
        ASSERT_TRUE(decoded.ok()) << decoded.status();                  \
        value = decoded.value()->eval();                                \
        ASSERT_TRUE(value.ok());                                        \
        v = value.value();                                              \
        ASSERT_TRUE(Expression::is##type(v));                           \
        if (#type == std::string("Double")) {                           \
            ASSERT_DOUBLE_EQ((expr_arg), Expression::as##type(v));      \
        } else {                                                        \
            ASSERT_EQ((expr_arg), Expression::as##type(v));             \
        }                                                               \
    } while (false)

    TEST_EXPR(true, Bool);
    TEST_EXPR(!true, Bool);
    TEST_EXPR(false, Bool);
    TEST_EXPR(!false, Bool);
    TEST_EXPR(!123, Bool);
    TEST_EXPR(!0, Bool);

    TEST_EXPR(123, Int);
    TEST_EXPR(-123, Int);
    TEST_EXPR(0x123, Int);

    TEST_EXPR(3.14, Double);

#undef TEST_EXPR
    // string
    {
        std::string query = "GO FROM 1 OVER follow WHERE \"string_literal\"";
        auto parsed = parser.parse(query);
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto value = expr->eval();
        ASSERT_TRUE(value.ok());
        auto v = value.value();
        ASSERT_TRUE(Expression::isString(v));
        ASSERT_EQ("string_literal", Expression::asString(v));

        auto buffer = Expression::encode(expr);
        ASSERT_FALSE(buffer.empty());
        auto decoded = Expression::decode(buffer);
        ASSERT_TRUE(decoded.ok()) << decoded.status();
        ASSERT_NE(nullptr, decoded.value());
        value = decoded.value()->eval();
        ASSERT_TRUE(value.ok());
        v = value.value();
        ASSERT_TRUE(Expression::isString(v));
        ASSERT_EQ("string_literal", Expression::asString(v));
    }
}


TEST_F(ExpressionTest, LiteralContantsArithmetic) {
    GQLParser parser;
#define TEST_EXPR(expr_arg, expected, type)                             \
    do {                                                                \
        std::string query = "GO FROM 1 OVER follow WHERE " #expr_arg;   \
        auto parsed = parser.parse(query);                              \
        ASSERT_TRUE(parsed.ok()) << parsed.status();                    \
        auto *expr = getFilterExpr(parsed.value().get());               \
        ASSERT_NE(nullptr, expr);                                       \
        auto value = expr->eval();                                      \
        ASSERT_TRUE(value.ok());                                        \
        auto v = value.value();                                         \
        ASSERT_TRUE(Expression::is##type(v));                           \
        if (#type == std::string("Double")) {                           \
            ASSERT_DOUBLE_EQ((expr_arg), Expression::as##type(v));      \
            ASSERT_DOUBLE_EQ((expected), Expression::as##type(v));      \
        } else {                                                        \
            ASSERT_EQ((expr_arg), Expression::as##type(v));             \
            ASSERT_EQ((expected), Expression::as##type(v));             \
        }                                                               \
        auto decoded = Expression::decode(Expression::encode(expr));    \
        ASSERT_TRUE(decoded.ok()) << decoded.status();                  \
        value = decoded.value()->eval();                                \
        ASSERT_TRUE(value.ok());                                        \
        v = value.value();                                              \
        ASSERT_TRUE(Expression::is##type(v));                           \
        if (#type == std::string("Double")) {                           \
            ASSERT_DOUBLE_EQ((expr_arg), Expression::as##type(v));      \
            ASSERT_DOUBLE_EQ((expected), Expression::as##type(v));      \
        } else {                                                        \
            ASSERT_EQ((expr_arg), Expression::as##type(v));             \
            ASSERT_EQ((expected), Expression::as##type(v));             \
        }                                                               \
    } while (false)

    TEST_EXPR(8 + 16, 24, Int);
    TEST_EXPR(8 - 16, -8, Int);
    TEST_EXPR(8 * 16, 128, Int);
    TEST_EXPR(8 / 16, 0, Int);
    TEST_EXPR(16 / 8, 2, Int);
    TEST_EXPR(8 % 16, 8, Int);
    TEST_EXPR(16 % 8, 0, Int);


    TEST_EXPR(16 + 4 + 2, 22, Int);
    TEST_EXPR(16+4+2, 22, Int);
    TEST_EXPR(16 - 4 - 2, 10, Int);
    TEST_EXPR(16-4-2, 10, Int);
    TEST_EXPR(16 - (4 - 2), 14, Int);
    TEST_EXPR(16 * 4 * 2, 128, Int);
    TEST_EXPR(16 / 4 / 2, 2, Int);
    TEST_EXPR(16 / (4 / 2), 8, Int);
    TEST_EXPR(16 / 4 % 2, 0, Int);


    TEST_EXPR(16 * 8 + 4 - 2, 130, Int);
    TEST_EXPR(16 * (8 + 4) - 2, 190, Int);
    TEST_EXPR(16 + 8 * 4 - 2, 46, Int);
    TEST_EXPR(16 + 8 * (4 - 2), 32, Int);
    TEST_EXPR(16 + 8 + 4 * 2, 32, Int);

    TEST_EXPR(16 / 8 + 4 - 2, 4, Int);
    TEST_EXPR(16 / (8 + 4) - 2, -1, Int);
    TEST_EXPR(16 + 8 / 4 - 2, 16, Int);
    TEST_EXPR(16 + 8 / (4 - 2), 20, Int);
    TEST_EXPR(16 + 8 + 4 / 2, 26, Int);

    TEST_EXPR(17 % 7 + 4 - 2, 5, Int);
    TEST_EXPR(17 + 7 % 4 - 2, 18, Int);
    TEST_EXPR(17 + 7 + 4 % 2, 24, Int);


    TEST_EXPR(16. + 8, 24., Double);
    TEST_EXPR(16 + 8., 24., Double);
    TEST_EXPR(16. - 8, 8., Double);
    TEST_EXPR(16 - 8., 8., Double);
    TEST_EXPR(16. * 8, 128., Double);
    TEST_EXPR(16 * 8., 128., Double);
    TEST_EXPR(16. / 8, 2., Double);
    TEST_EXPR(16 / 8., 2., Double);

    TEST_EXPR(16. * 8 + 4 - 2, 130., Double);
    TEST_EXPR(16 + 8. * 4 - 2, 46., Double);
    TEST_EXPR(16 + 8 + 4. * 2, 32., Double);

    TEST_EXPR(16. / 8 + 4 - 2, 4., Double);
    TEST_EXPR(16 + 8 / 4. - 2, 16., Double);
    TEST_EXPR(16 + 8 + 4 / 2., 26., Double);

    TEST_EXPR(3.14 * 3 * 3 / 2, 14.13, Double);

#undef TEST_EXPR
    {
        std::string query = "GO FROM 1 OVER follow WHERE 16 + 8 / 4 - 2";
        auto parsed = parser.parse(query);
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto value = expr->eval();
        ASSERT_TRUE(value.ok());
        auto v = value.value();
        ASSERT_TRUE(Expression::isInt(v));
        ASSERT_EQ(16, Expression::asInt(v));

        auto buffer = Expression::encode(expr);
        ASSERT_FALSE(buffer.empty());
        auto decoded = Expression::decode(buffer);
        ASSERT_TRUE(decoded.ok()) << decoded.status();
        ASSERT_NE(nullptr, decoded.value());
        value = decoded.value()->eval();
        ASSERT_TRUE(value.ok());
        v = value.value();
        ASSERT_TRUE(Expression::isInt(v));
        ASSERT_EQ(16, Expression::asInt(v));
    }
}


TEST_F(ExpressionTest, LiteralConstantsRelational) {
    GQLParser parser;
#define TEST_EXPR(expr_arg, expected)                                   \
    do {                                                                \
        std::string query = "GO FROM 1 OVER follow WHERE " #expr_arg;   \
        auto parsed = parser.parse(query);                              \
        ASSERT_TRUE(parsed.ok()) << parsed.status();                    \
        auto *expr = getFilterExpr(parsed.value().get());               \
        ASSERT_NE(nullptr, expr);                                       \
        auto value = expr->eval();                                      \
        ASSERT_TRUE(value.ok());                                        \
        auto v = value.value();                                         \
        ASSERT_TRUE(Expression::isBool(v));                             \
        ASSERT_EQ((expr_arg), Expression::asBool(v));                   \
        ASSERT_EQ((expected), Expression::asBool(v));                   \
        auto decoded = Expression::decode(Expression::encode(expr));    \
        ASSERT_TRUE(decoded.ok()) << decoded.status();                  \
        value = decoded.value()->eval();                                \
        ASSERT_TRUE(value.ok());                                        \
        v = value.value();                                              \
        ASSERT_TRUE(Expression::isBool(v));                             \
        ASSERT_EQ((expr_arg), Expression::asBool(v));                   \
        ASSERT_EQ((expected), Expression::asBool(v));                   \
    } while (false)

    TEST_EXPR(!-1, false);
    TEST_EXPR(!!-1, true);
    TEST_EXPR(1 == 1, true);
    TEST_EXPR(1 != 1, false);
    TEST_EXPR(1 > 1, false);
    TEST_EXPR(1 >= 1, true);
    TEST_EXPR(1 < 1, false);
    TEST_EXPR(1 <= 1, true);

    TEST_EXPR(-1 == -1, true);
    TEST_EXPR(-1 != -1, false);
    TEST_EXPR(-1 > -1, false);
    TEST_EXPR(-1 >= -1, true);
    TEST_EXPR(-1 < -1, false);
    TEST_EXPR(-1 <= -1, true);

    TEST_EXPR(1 == 2, false);
    TEST_EXPR(2 == 1, false);
    TEST_EXPR(1 != 2, true);
    TEST_EXPR(2 != 1, true);
    TEST_EXPR(1 > 2, false);
    TEST_EXPR(2 > 1, true);
    TEST_EXPR(1 >= 2, false);
    TEST_EXPR(2 >= 1, true);
    TEST_EXPR(1 < 2, true);
    TEST_EXPR(2 < 1, false);
    TEST_EXPR(1 <= 2, true);
    TEST_EXPR(2 <= 1, false);

    TEST_EXPR(-1 == -2, false);
    TEST_EXPR(-2 == -1, false);
    TEST_EXPR(-1 != -2, true);
    TEST_EXPR(-2 != -1, true);
    TEST_EXPR(-1 > -2, true);
    TEST_EXPR(-2 > -1, false);
    TEST_EXPR(-1 >= -2, true);
    TEST_EXPR(-2 >= -1, false);
    TEST_EXPR(-1 < -2, false);
    TEST_EXPR(-2 < -1, true);
    TEST_EXPR(-1 <= -2, false);
    TEST_EXPR(-2 <= -1, true);

    TEST_EXPR(0.5 == 1, false);
    TEST_EXPR(1.0 == 1, true);
    TEST_EXPR(0.5 != 1, true);
    TEST_EXPR(1.0 != 1, false);
    TEST_EXPR(0.5 > 1, false);
    TEST_EXPR(0.5 >= 1, false);
    TEST_EXPR(0.5 < 1, true);
    TEST_EXPR(0.5 <= 1, true);

    TEST_EXPR(true == 1, true);
    TEST_EXPR(true == 2, false);
    TEST_EXPR(true != 1, false);
    TEST_EXPR(true != 2, true);
    TEST_EXPR(true > 1, false);
    TEST_EXPR(true >= 1, true);
    TEST_EXPR(true < 1, false);
    TEST_EXPR(true <= 1, true);
    TEST_EXPR(false == 0, true);
    TEST_EXPR(false == 1, false);
    TEST_EXPR(false != 0, false);
    TEST_EXPR(false != 1, true);
    TEST_EXPR(false > 0, false);
    TEST_EXPR(false >= 0, true);
    TEST_EXPR(false < 0, false);
    TEST_EXPR(false <= 0, true);

    TEST_EXPR(true == 1.0, true);
    TEST_EXPR(true == 2.0, false);
    TEST_EXPR(true != 1.0, false);
    TEST_EXPR(true != 2.0, true);
    TEST_EXPR(true > 1.0, false);
    TEST_EXPR(true >= 1.0, true);
    TEST_EXPR(true < 1.0, false);
    TEST_EXPR(true <= 1.0, true);
    TEST_EXPR(false == 0.0, true);
    TEST_EXPR(false == 1.0, false);
    TEST_EXPR(false != 0.0, false);
    TEST_EXPR(false != 1.0, true);
    TEST_EXPR(false > 0.0, false);
    TEST_EXPR(false >= 0.0, true);
    TEST_EXPR(false < 0.0, false);
    TEST_EXPR(false <= 0.0, true);

    TEST_EXPR(8 % 2 + 1 == 1, true);
    TEST_EXPR(8 % 2 + 1 != 1, false);
    TEST_EXPR(8 % 3 + 1 == 3, true);
    TEST_EXPR(8 % 3 + 1 != 3, false);
    TEST_EXPR(8 % 3 > 1, true);
    TEST_EXPR(8 % 3 >= 2, true);
    TEST_EXPR(8 % 3 <= 2, true);
    TEST_EXPR(8 % 3 < 2, false);

    TEST_EXPR(3.14 * 3 * 3 / 2 > 3.14 * 1.5 * 1.5 / 2, true);
    TEST_EXPR(3.14 * 3 * 3 / 2 < 3.14 * 1.5 * 1.5 / 2, false);

    {
        std::string query = "GO FROM 1 OVER follow WHERE 3.14 * 3 * 3 / 2 == 14.13";
        auto parsed = parser.parse(query);
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto value = expr->eval();
        ASSERT_TRUE(value.ok());
        auto v = value.value();
        ASSERT_TRUE(Expression::isBool(v));
        ASSERT_TRUE(Expression::asBool(v));
        auto decoded = Expression::decode(Expression::encode(expr));
        ASSERT_TRUE(decoded.ok()) << decoded.status();
        value = decoded.value()->eval();
        ASSERT_TRUE(value.ok());
        v = value.value();
        ASSERT_TRUE(Expression::isBool(v));
        ASSERT_TRUE(Expression::asBool(v));
    }
    {
        std::string query = "GO FROM 1 OVER follow WHERE 3.14 * 3 * 3 / 2 != 3.14 * 1.5 * 1.5 / 2";
        auto parsed = parser.parse(query);
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto value = expr->eval();
        ASSERT_TRUE(value.ok());
        auto v = value.value();
        ASSERT_TRUE(Expression::isBool(v));
        ASSERT_TRUE(Expression::asBool(v));
        auto decoded = Expression::decode(Expression::encode(expr));
        ASSERT_TRUE(decoded.ok()) << decoded.status();
        value = decoded.value()->eval();
        ASSERT_TRUE(value.ok());
        v = value.value();
        ASSERT_TRUE(Expression::isBool(v));
        ASSERT_TRUE(Expression::asBool(v));
    }

#undef TEST_EXPR
}


TEST_F(ExpressionTest, LiteralConstantsLogical) {
    GQLParser parser;
#define TEST_EXPR(expr_arg, expected)                                   \
    do {                                                                \
        std::string query = "GO FROM 1 OVER follow WHERE " #expr_arg;   \
        auto parsed = parser.parse(query);                              \
        ASSERT_TRUE(parsed.ok()) << parsed.status();                    \
        auto *expr = getFilterExpr(parsed.value().get());               \
        ASSERT_NE(nullptr, expr);                                       \
        auto value = expr->eval();                                      \
        ASSERT_TRUE(value.ok());                                        \
        auto v = value.value();                                         \
        ASSERT_TRUE(Expression::isBool(v));                             \
        ASSERT_EQ((expr_arg), Expression::asBool(v));                   \
        ASSERT_EQ((expected), Expression::asBool(v));                   \
        auto decoded = Expression::decode(Expression::encode(expr));    \
        ASSERT_TRUE(decoded.ok()) << decoded.status();                  \
        value = decoded.value()->eval();                                \
        ASSERT_TRUE(value.ok());                                        \
        v = value.value();                                              \
        ASSERT_TRUE(Expression::isBool(v));                             \
        ASSERT_EQ((expr_arg), Expression::asBool(v));                   \
        ASSERT_EQ((expected), Expression::asBool(v));                   \
    } while (false)

    // AND
    TEST_EXPR(true && true, true);
    TEST_EXPR(true && false, false);
    TEST_EXPR(false && true, false);
    TEST_EXPR(false && false, false);

    // AND AND  ===  (AND) AND
    TEST_EXPR(true && true && true, true);
    TEST_EXPR(true && true && false, false);
    TEST_EXPR(true && false && true, false);
    TEST_EXPR(true && false && false, false);
    TEST_EXPR(false && true && true, false);
    TEST_EXPR(false && true && false, false);
    TEST_EXPR(false && false && true, false);
    TEST_EXPR(false && false && false, false);

    // OR
    TEST_EXPR(true || true, true);
    TEST_EXPR(true || false, true);
    TEST_EXPR(false || true, true);
    TEST_EXPR(false || false, false);

    // OR OR  ===  (OR) OR
    TEST_EXPR(true || true || true, true);
    TEST_EXPR(true || true || false, true);
    TEST_EXPR(true || false || true, true);
    TEST_EXPR(true || false || false, true);
    TEST_EXPR(false || true || true, true);
    TEST_EXPR(false || true || false, true);
    TEST_EXPR(false || false || true, true);
    TEST_EXPR(false || false || false, false);

    // AND OR  ===  (AND) OR
    TEST_EXPR(true && true || true, true);
    TEST_EXPR(true && true || false, true);
    TEST_EXPR(true && false || true, true);
    TEST_EXPR(true && false || false, false);
    TEST_EXPR(false && true || true, true);
    TEST_EXPR(false && true || false, false);
    TEST_EXPR(false && false || true, true);
    TEST_EXPR(false && false || false, false);

    // OR AND  === OR (AND)
    TEST_EXPR(true || true && true, true);
    TEST_EXPR(true || true && false, true);
    TEST_EXPR(true || false && true, true);
    TEST_EXPR(true || false && false, true);
    TEST_EXPR(false || true && true, true);
    TEST_EXPR(false || true && false, false);
    TEST_EXPR(false || false && true, false);
    TEST_EXPR(false || false && false, false);

    TEST_EXPR(2 > 1 && 3 > 2, true);
    TEST_EXPR(2 <= 1 && 3 > 2, false);
    TEST_EXPR(2 > 1 && 3 < 2, false);
    TEST_EXPR(2 < 1 && 3 < 2, false);

#undef TEST_EXPR
}


TEST_F(ExpressionTest, InputReference) {
    GQLParser parser;
    {
        std::string query = "GO FROM 1 OVER follow WHERE $-.name";
        auto parsed = parser.parse(query);
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto ctx = std::make_unique<ExpressionContext>();
        ctx->getters().getInputProp = [] (auto &prop) -> VariantType {
            if (prop == "name") {
                return std::string("Freddie");
            } else {
                return std::string("nobody");
            }
        };
        expr->setContext(ctx.get());
        auto value = expr->eval();
        ASSERT_TRUE(value.ok());
        auto v = value.value();
        ASSERT_TRUE(Expression::isString(v));
        ASSERT_EQ("Freddie", Expression::asString(v));
    }
    {
        std::string query = "GO FROM 1 OVER follow WHERE $-.age >= 18";
        auto parsed = parser.parse(query);
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto ctx = std::make_unique<ExpressionContext>();
        ctx->getters().getInputProp = [] (auto &prop) -> VariantType {
            if (prop == "age") {
                return 18L;
            } else {
                return 14L;
            }
        };
        expr->setContext(ctx.get());
        auto value = expr->eval();
        ASSERT_TRUE(value.ok());
        auto v = value.value();
        ASSERT_TRUE(Expression::isBool(v));
        ASSERT_TRUE(Expression::asBool(v));
    }
}


TEST_F(ExpressionTest, SourceTagReference) {
    GQLParser parser;
    {
        std::string query = "GO FROM 1 OVER follow WHERE $^.person.name == \"dutor\"";
        auto parsed = parser.parse(query);
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto ctx = std::make_unique<ExpressionContext>();
        ctx->getters().getSrcTagProp = [] (auto &tag, auto &prop) -> VariantType {
            if (tag == "person" && prop == "name") {
                return std::string("dutor");
            }
            return std::string("nobody");
        };
        expr->setContext(ctx.get());
        auto value = expr->eval();
        ASSERT_TRUE(value.ok());
        auto v = value.value();
        ASSERT_TRUE(Expression::isBool(v));
        ASSERT_TRUE(Expression::asBool(v));
    }
}


TEST_F(ExpressionTest, EdgeReference) {
    GQLParser parser;
    {
        std::string query = "GO FROM 1 OVER follow WHERE follow._src == 1 "
                                                        "|| follow.cur_time < 1545798791"
                                                        "&& follow._dst == 2";
        auto parsed = parser.parse(query);
        ASSERT_TRUE(parsed.ok());
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto ctx = std::make_unique<ExpressionContext>();
        ctx->getters().getAliasProp = [] (auto &, auto &prop) -> VariantType {
            if (prop == "cur_time") {
                return static_cast<int64_t>(::time(NULL));
            }
            if (prop == "_src") {
                return 0L;
            }
            if (prop == "_dst") {
                return 2L;
            }
            return 1545798790L;
        };
        expr->setContext(ctx.get());
        auto value = expr->eval();
        ASSERT_TRUE(value.ok());
        auto v = value.value();
        ASSERT_TRUE(Expression::isBool(v));
        ASSERT_FALSE(Expression::asBool(v));
    }
}


TEST_F(ExpressionTest, FunctionCall) {
    GQLParser parser;
#define TEST_EXPR(expected, op, expr_arg, type)                         \
    do {                                                                \
        std::string query = "GO FROM 1 OVER follow WHERE " #expr_arg;   \
        auto parsed = parser.parse(query);                              \
        ASSERT_TRUE(parsed.ok()) << parsed.status();                    \
        auto *expr = getFilterExpr(parsed.value().get());               \
        ASSERT_NE(nullptr, expr);                                       \
        auto decoded = Expression::decode(Expression::encode(expr));    \
        ASSERT_TRUE(decoded.ok()) << decoded.status();                  \
        auto ctx = std::make_unique<ExpressionContext>();               \
        decoded.value()->setContext(ctx.get());                         \
        auto status = decoded.value()->prepare();                       \
        ASSERT_TRUE(status.ok()) << status;                             \
        auto value = decoded.value()->eval();                           \
        ASSERT_TRUE(value.ok());                                        \
        auto v = value.value();                                         \
        ASSERT_TRUE(Expression::is##type(v));                           \
        if (#type == std::string("Double")) {                           \
            if (#op != std::string("EQ")) {                             \
                ASSERT_##op(expected, Expression::as##type(v));         \
            } else {                                                    \
                ASSERT_DOUBLE_EQ(expected, Expression::as##type(v));    \
            }                                                           \
        } else {                                                        \
            ASSERT_##op(expected, Expression::as##type(v));             \
        }                                                               \
    } while (false)

    TEST_EXPR(5.0, EQ, abs(5), Double);
    TEST_EXPR(5.0, EQ, abs(-5), Double);

    TEST_EXPR(3.0, EQ, floor(3.14), Double);
    TEST_EXPR(-4.0, EQ, floor(-3.14), Double);

    TEST_EXPR(4.0, EQ, ceil(3.14), Double);
    TEST_EXPR(-3.0, EQ, ceil(-3.14), Double);

    TEST_EXPR(3.0, EQ, round(3.14), Double);
    TEST_EXPR(-3.0, EQ, round(-3.14), Double);
    TEST_EXPR(4.0, EQ, round(3.5), Double);
    TEST_EXPR(-4.0, EQ, round(-3.5), Double);

    TEST_EXPR(3.0, EQ, cbrt(27), Double);

    constexpr auto euler = 2.7182818284590451;
    TEST_EXPR(euler, EQ, exp(1), Double);
    TEST_EXPR(1024, EQ, exp2(10), Double);

    TEST_EXPR(2, EQ, log(pow(2.7182818284590451, 2)), Double);
    TEST_EXPR(10, EQ, log2(1024), Double);
    TEST_EXPR(3, EQ, log10(1000), Double);

    TEST_EXPR(5.0, EQ, hypot(3, 4), Double);
    TEST_EXPR(5.0, EQ, sqrt(pow(3, 2) + pow(4, 2)), Double);

    TEST_EXPR(1.0, EQ, hypot(sin(0.5), cos(0.5)), Double);
    TEST_EXPR(1.0, EQ, (sin(0.5) / cos(0.5)) / tan(0.5), Double);

    TEST_EXPR(0.3, EQ, sin(asin(0.3)), Double);
    TEST_EXPR(0.3, EQ, cos(acos(0.3)), Double);
    TEST_EXPR(0.3, EQ, tan(atan(0.3)), Double);

    TEST_EXPR(1024, GT, rand32(1024), Int);
    TEST_EXPR(0, LE, rand32(1024), Int);
    TEST_EXPR(4096, GT, rand64(1024, 4096), Int);
    TEST_EXPR(1024, LE, rand64(1024, 4096), Int);

    TEST_EXPR(1554716753, LT, now(), Int);
    TEST_EXPR(4773548753, GE, now(), Int);  // failed 102 years later

    TEST_EXPR(0, EQ, strcasecmp("HelLo", "hello"), Int);
    TEST_EXPR(0, LT, strcasecmp("HelLo", "hell"), Int);
    TEST_EXPR(0, GT, strcasecmp("HelLo", "World"), Int);

    TEST_EXPR(5, EQ, length("hello"), Int);
    TEST_EXPR(0, EQ, length(""), Int);

#undef TEST_EXPR
}

TEST_F(ExpressionTest, StringFunctionCall) {
    GQLParser parser;
#define TEST_EXPR(expected, op, expr_arg, type)                         \
    do {                                                                \
        std::string query = "GO FROM 1 OVER follow WHERE " #expr_arg;   \
        auto parsed = parser.parse(query);                              \
        ASSERT_TRUE(parsed.ok()) << parsed.status();                    \
        auto *expr = getFilterExpr(parsed.value().get());               \
        ASSERT_NE(nullptr, expr);                                       \
        auto decoded = Expression::decode(Expression::encode(expr));    \
        ASSERT_TRUE(decoded.ok()) << decoded.status();                  \
        auto ctx = std::make_unique<ExpressionContext>();               \
        decoded.value()->setContext(ctx.get());                         \
        auto status = decoded.value()->prepare();                       \
        ASSERT_TRUE(status.ok()) << status;                             \
        auto value = decoded.value()->eval();                           \
        ASSERT_TRUE(value.ok());                                        \
        auto v = value.value();                                         \
        ASSERT_TRUE(Expression::is##type(v));                           \
        if (#type == std::string("String")) {                           \
            if (#op != std::string("EQ")) {                             \
                ASSERT_##op(expected, Expression::as##type(v));         \
            } else {                                                    \
                ASSERT_EQ(expected, Expression::as##type(v));    \
            }                                                           \
        } else {                                                        \
            ASSERT_##op(expected, Expression::as##type(v));             \
        }                                                               \
    } while (false)

    TEST_EXPR("hello", EQ, lower("HelLo"), String);
    TEST_EXPR("hello", EQ, lower("HELLO"), String);
    TEST_EXPR("hello", EQ, lower("hello"), String);

    TEST_EXPR("HELLO", EQ, upper("HelLo"), String);
    TEST_EXPR("HELLO", EQ, upper("HELLO"), String);
    TEST_EXPR("HELLO", EQ, upper("hello"), String);

    TEST_EXPR("hello", EQ, trim(" hello "), String);
    TEST_EXPR("hello", EQ, trim(" hello"),  String);
    TEST_EXPR("hello", EQ, trim("hello "),  String);

    TEST_EXPR("hello ", EQ, ltrim(" hello "), String);
    TEST_EXPR("hello",  EQ, ltrim(" hello"),  String);
    TEST_EXPR("hello ", EQ, ltrim("hello "),  String);

    TEST_EXPR(" hello", EQ, rtrim(" hello "), String);
    TEST_EXPR(" hello", EQ, rtrim(" hello"),  String);
    TEST_EXPR("hello",  EQ, rtrim("hello "),  String);

    TEST_EXPR("hello", EQ, left("hello world", 5),  String);
    TEST_EXPR("",      EQ, left("hello world", 0),  String);
    TEST_EXPR("",      EQ, left("hello world", -1), String);

    TEST_EXPR("world", EQ, right("hello world", 5),  String);
    TEST_EXPR("",      EQ, right("hello world", 0),  String);
    TEST_EXPR("",      EQ, right("hello world", -1), String);

    TEST_EXPR("111Hello", EQ, lpad("Hello", 8, "1"),  String);
    TEST_EXPR("wewHello", EQ, lpad("Hello", 8, "we"), String);
    TEST_EXPR("Hell",     EQ, lpad("Hello", 4, "1"),  String);
    TEST_EXPR("",         EQ, lpad("Hello", 0, "1"),  String);

    TEST_EXPR("Hello111", EQ, rpad("Hello", 8, "1"),  String);
    TEST_EXPR("Hellowew", EQ, rpad("Hello", 8, "we"), String);
    TEST_EXPR("Hell",     EQ, rpad("Hello", 4, "1"),  String);
    TEST_EXPR("",         EQ, rpad("Hello", 0, "1"),  String);

    TEST_EXPR("1", EQ, substr("123", 1, 1),   String);
    TEST_EXPR("",  EQ, substr("123", 1, 0),   String);
    TEST_EXPR("",  EQ, substr("123", 1, -1),  String);
    TEST_EXPR("3", EQ, substr("123", -1, 1),  String);
    TEST_EXPR("",  EQ, substr("123", -1, 0),  String);
    TEST_EXPR("",  EQ, substr("123", -1, -1), String);
    TEST_EXPR("",  EQ, substr("123", 5, 1),   String);
    TEST_EXPR("",  EQ, substr("123", -5, 1),  String);

#undef TEST_EXPR
}

TEST_F(ExpressionTest, InvalidExpressionTest) {
    GQLParser parser;

#define TEST_EXPR(expr_arg)                                           \
    do {                                                              \
        std::string query = "GO FROM 1 OVER follow WHERE " #expr_arg; \
        auto parsed = parser.parse(query);                            \
        ASSERT_TRUE(parsed.ok()) << parsed.status();                  \
        auto *expr = getFilterExpr(parsed.value().get());             \
        ASSERT_NE(nullptr, expr);                                     \
        auto decoded = Expression::decode(Expression::encode(expr));  \
        ASSERT_TRUE(decoded.ok()) << decoded.status();                \
        auto ctx = std::make_unique<ExpressionContext>();             \
        decoded.value()->setContext(ctx.get());                       \
        auto status = decoded.value()->prepare();                     \
        ASSERT_TRUE(status.ok()) << status;                           \
        auto value = decoded.value()->eval();                         \
        ASSERT_TRUE(!value.ok());                                     \
    } while (false)

    TEST_EXPR("a" + 1);
    TEST_EXPR(3.14 + "a");
    TEST_EXPR("ab" - "c");
    TEST_EXPR(1 - "a");
    TEST_EXPR("a" * 1);
    TEST_EXPR(1.0 * "a");
    TEST_EXPR(1 / "a");
    TEST_EXPR("a" / "b");
    TEST_EXPR(1.0 % "a");
    TEST_EXPR(-"A");
    TEST_EXPR(TRUE + FALSE);
    TEST_EXPR("123" > 123);
    TEST_EXPR("123" < 123);
    TEST_EXPR("123" >= 123);
    TEST_EXPR("123" <= 123);
    TEST_EXPR("123" == 123);
    TEST_EXPR("123" != 123);
#undef TEST_EXPR
}


TEST_F(ExpressionTest, StringLengthLimitTest) {
    constexpr auto MAX = (1UL<<20);
    std::string str(MAX, 'X');

    // double quote
    {
        GQLParser parser;
        auto *fmt = "GO FROM 1 OVER follow WHERE \"%s\"";
        {
            auto query = folly::stringPrintf(fmt, str.c_str());
            auto parsed = parser.parse(query);
            ASSERT_TRUE(parsed.ok()) << parsed.status();
        }
    }
    // single quote
    {
        GQLParser parser;
        auto *fmt = "GO FROM 1 OVER follow WHERE '%s'";
        {
            auto query = folly::stringPrintf(fmt, str.c_str());
            auto parsed = parser.parse(query);
            ASSERT_TRUE(parsed.ok()) << parsed.status();
        }
    }
}

}   // namespace nebula
