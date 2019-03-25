/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
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
        ASSERT_TRUE(Expression::is##type(value));                       \
        if (#type == std::string("Double")) {                           \
            ASSERT_DOUBLE_EQ((expr_arg), Expression::as##type(value));  \
        } else {                                                        \
            ASSERT_EQ((expr_arg), Expression::as##type(value));         \
        }                                                               \
        auto decoded = Expression::decode(Expression::encode(expr));    \
        ASSERT_TRUE(decoded.ok()) << decoded.status();                  \
        value = decoded.value()->eval();                                \
        ASSERT_TRUE(Expression::is##type(value));                       \
        if (#type == std::string("Double")) {                           \
            ASSERT_DOUBLE_EQ((expr_arg), Expression::as##type(value));  \
        } else {                                                        \
            ASSERT_EQ((expr_arg), Expression::as##type(value));         \
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
        ASSERT_TRUE(Expression::isString(value));
        ASSERT_EQ("string_literal", Expression::asString(value));

        auto buffer = Expression::encode(expr);
        ASSERT_FALSE(buffer.empty());
        auto decoded = Expression::decode(buffer);
        ASSERT_TRUE(decoded.ok()) << decoded.status();
        ASSERT_NE(nullptr, decoded.value());
        value = decoded.value()->eval();
        ASSERT_TRUE(Expression::isString(value));
        ASSERT_EQ("string_literal", Expression::asString(value));
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
        ASSERT_TRUE(Expression::is##type(value));                       \
        if (#type == std::string("Double")) {                           \
            ASSERT_DOUBLE_EQ((expr_arg), Expression::as##type(value));  \
            ASSERT_DOUBLE_EQ((expected), Expression::as##type(value));  \
        } else {                                                        \
            ASSERT_EQ((expr_arg), Expression::as##type(value));         \
            ASSERT_EQ((expected), Expression::as##type(value));         \
        }                                                               \
        auto decoded = Expression::decode(Expression::encode(expr));    \
        ASSERT_TRUE(decoded.ok()) << decoded.status();                  \
        value = decoded.value()->eval();                                \
        ASSERT_TRUE(Expression::is##type(value));                       \
        if (#type == std::string("Double")) {                           \
            ASSERT_DOUBLE_EQ((expr_arg), Expression::as##type(value));  \
            ASSERT_DOUBLE_EQ((expected), Expression::as##type(value));  \
        } else {                                                        \
            ASSERT_EQ((expr_arg), Expression::as##type(value));         \
            ASSERT_EQ((expected), Expression::as##type(value));         \
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
    TEST_EXPR(16 - 4 - 2, 10, Int);
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
        ASSERT_TRUE(Expression::isInt(value));
        ASSERT_EQ(16, Expression::asInt(value));

        auto buffer = Expression::encode(expr);
        ASSERT_FALSE(buffer.empty());
        auto decoded = Expression::decode(buffer);
        ASSERT_TRUE(decoded.ok()) << decoded.status();
        ASSERT_NE(nullptr, decoded.value());
        value = decoded.value()->eval();
        ASSERT_TRUE(Expression::isInt(value));
        ASSERT_EQ(16, Expression::asInt(value));
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
        ASSERT_TRUE(Expression::isBool(value));                         \
        ASSERT_EQ((expr_arg), Expression::asBool(value));               \
        ASSERT_EQ((expected), Expression::asBool(value));               \
        auto decoded = Expression::decode(Expression::encode(expr));    \
        ASSERT_TRUE(decoded.ok()) << decoded.status();                  \
        value = decoded.value()->eval();                                \
        ASSERT_TRUE(Expression::isBool(value));                         \
        ASSERT_EQ((expr_arg), Expression::asBool(value));               \
        ASSERT_EQ((expected), Expression::asBool(value));               \
    } while (false)

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
        ASSERT_TRUE(Expression::isBool(value));
        ASSERT_TRUE(Expression::asBool(value));
        auto decoded = Expression::decode(Expression::encode(expr));
        ASSERT_TRUE(decoded.ok()) << decoded.status();
        value = decoded.value()->eval();
        ASSERT_TRUE(Expression::isBool(value));
        ASSERT_TRUE(Expression::asBool(value));
    }
    {
        std::string query = "GO FROM 1 OVER follow WHERE 3.14 * 3 * 3 / 2 != 3.14 * 1.5 * 1.5 / 2";
        auto parsed = parser.parse(query);
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto value = expr->eval();
        ASSERT_TRUE(Expression::isBool(value));
        ASSERT_TRUE(Expression::asBool(value));
        auto decoded = Expression::decode(Expression::encode(expr));
        ASSERT_TRUE(decoded.ok()) << decoded.status();
        value = decoded.value()->eval();
        ASSERT_TRUE(Expression::isBool(value));
        ASSERT_TRUE(Expression::asBool(value));
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
        ASSERT_TRUE(Expression::isBool(value));                         \
        ASSERT_EQ((expr_arg), Expression::asBool(value));               \
        ASSERT_EQ((expected), Expression::asBool(value));               \
        auto decoded = Expression::decode(Expression::encode(expr));    \
        ASSERT_TRUE(decoded.ok()) << decoded.status();                  \
        value = decoded.value()->eval();                                \
        ASSERT_TRUE(Expression::isBool(value));                         \
        ASSERT_EQ((expr_arg), Expression::asBool(value));               \
        ASSERT_EQ((expected), Expression::asBool(value));               \
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
        std::string query = "GO FROM 1 OVER follow WHERE $_.name";
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
        ASSERT_TRUE(Expression::isString(value));
        ASSERT_EQ("Freddie", Expression::asString(value));
    }
    {
        std::string query = "GO FROM 1 OVER follow WHERE $_.age >= 18";
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
        ASSERT_TRUE(Expression::isBool(value));
        ASSERT_TRUE(Expression::asBool(value));
    }
}


TEST_F(ExpressionTest, SourceTagReference) {
    GQLParser parser;
    {
        std::string query = "GO FROM 1 AS src OVER follow WHERE src[person].name == \"dutor\" "
                                                                "&& src[person]._id == 1";
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
        ctx->getters().getSrcTagId = [] () -> int64_t {
            return 1L;
        };
        expr->setContext(ctx.get());
        auto value = expr->eval();
        ASSERT_TRUE(Expression::isBool(value));
        ASSERT_TRUE(Expression::asBool(value));
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
        ctx->getters().getSrcTagId = [] () -> int64_t {
            return 0L;
        };
        ctx->getters().getDstTagId = [] () -> int64_t {
            return 2L;
        };
        ctx->getters().getEdgeProp = [] (auto &prop) -> VariantType {
            if (prop == "cur_time") {
                return static_cast<int64_t>(::time(NULL));
            }
            return 1545798790L;
        };
        expr->setContext(ctx.get());
        auto value = expr->eval();
        ASSERT_TRUE(Expression::isBool(value));
        ASSERT_FALSE(Expression::asBool(value));
    }
}

}   // namespace nebula
