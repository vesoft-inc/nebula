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

#define TEST_EXPR_IMPL(OP, exprSymbol, expected)                     \
    std::string query = "GO FROM 1 OVER follow WHERE " + exprSymbol; \
    auto parsed = parser_->parse(query);                             \
    ASSERT_TRUE(parsed.ok()) << parsed.status();                     \
    Getters getters;                                                 \
    auto *expr = getFilterExpr(parsed.value().get());                \
    ASSERT_NE(nullptr, expr);                                        \
    auto ctx = std::make_unique<ExpressionContext>();                \
    expr->setContext(ctx.get());                                     \
    auto status = expr->prepare();                                   \
    ASSERT_TRUE(status.ok()) << status;                              \
    auto value = expr->eval(getters);                                \
    ASSERT_TRUE(value.ok());                                         \
    auto v = value.value();                                          \
    ASSERT_##OP(Expression::as<decltype(expected)>(v), expected);    \
    auto decoded = Expression::decode(Expression::encode(expr));     \
    ASSERT_TRUE(decoded.ok()) << decoded.status();                   \
    decoded.value()->setContext(ctx.get());                          \
    status = decoded.value()->prepare();                             \
    ASSERT_TRUE(status.ok()) << status;                              \
    value = decoded.value()->eval(getters);                          \
    ASSERT_TRUE(value.ok());                                         \
    v = value.value();                                               \
    ASSERT_##OP(Expression::as<decltype(expected)>(v), expected);    \


    template <typename T>
    void testExpr(const std::string& exprSymbol, T expected) {
        TEST_EXPR_IMPL(EQ, exprSymbol, expected);
    }

    void testExpr(const std::string& exprSymbol, double expected) {
        TEST_EXPR_IMPL(DOUBLE_EQ, exprSymbol, expected);
    }

    void testExpr(const std::string& exprSymbol, int expected) {
        testExpr(exprSymbol, static_cast<int64_t>(expected));
    }

    void testExpr(const std::string& exprSymbol, const char* expected) {
        testExpr(exprSymbol, std::string(expected));
    }

    template <typename T>
    void testExprGT(const std::string& exprSymbol, T expected) {
        TEST_EXPR_IMPL(GT, exprSymbol, expected);
    }

    void testExprGT(const std::string& exprSymbol, int expected) {
        testExprGT(exprSymbol, static_cast<int64_t>(expected));
    }

    template <typename T>
    void testExprGE(const std::string& exprSymbol, T expected) {
        TEST_EXPR_IMPL(GE, exprSymbol, expected);
    }

    void testExprGE(const std::string& exprSymbol, int expected) {
        testExprGE(exprSymbol, static_cast<int64_t>(expected));
    }

    template <typename T>
    void testExprLT(const std::string& exprSymbol, T expected) {
        TEST_EXPR_IMPL(LT, exprSymbol, expected);
    }

    void testExprLT(const std::string& exprSymbol, int expected) {
        testExprLT(exprSymbol, static_cast<int64_t>(expected));
    }

    template <typename T>
    void testExprLE(const std::string& exprSymbol, T expected) {
        TEST_EXPR_IMPL(LE, exprSymbol, expected);
    }

    void testExprLE(const std::string& exprSymbol, int expected) {
        testExprLE(exprSymbol, static_cast<int64_t>(expected));
    }

    void testExprFailed(const std::string& exprSymbol) {
        std::string query = "GO FROM 1 OVER follow WHERE " + exprSymbol;
        auto parsed = parser_->parse(query);
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        Getters getters;
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto decoded = Expression::decode(Expression::encode(expr));
        ASSERT_TRUE(decoded.ok()) << decoded.status();
        auto ctx = std::make_unique<ExpressionContext>();
        decoded.value()->setContext(ctx.get());
        auto status = decoded.value()->prepare();
        ASSERT_TRUE(status.ok()) << status;
        auto value = decoded.value()->eval(getters);
        ASSERT_TRUE(!value.ok());
    }

    std::unique_ptr<GQLParser> parser_{nullptr};
};

#undef TEST_EXPR_IMPL


void ExpressionTest::SetUp() {
    parser_.reset(new(std::nothrow) GQLParser());
    ASSERT_NE(parser_, nullptr);
}


void ExpressionTest::TearDown() {
}


Expression* ExpressionTest::getFilterExpr(SequentialSentences *sentences) {
    auto *go = dynamic_cast<GoSentence*>(sentences->sentences().front());
    return go->whereClause()->filter();
}

// expr -- the expression can evaluate by nGQL parser may not evaluated by c++
// expected -- the expected value of expression must evaluated by c++
#define TEST_EXPR(expr, expected) do { testExpr(#expr, expected); } while (0);
#define TEST_EXPR_GT(expr, expected) do { testExprGT(#expr, expected); } while (0);
#define TEST_EXPR_GE(expr, expected) do { testExprGE(#expr, expected); } while (0);
#define TEST_EXPR_LT(expr, expected) do { testExprLT(#expr, expected); } while (0);
#define TEST_EXPR_LE(expr, expected) do { testExprLE(#expr, expected); } while (0);
#define TEST_EXPR_FAILED(expr) do { testExprFailed(#expr); } while (0);

TEST_F(ExpressionTest, LiteralConstants) {
    TEST_EXPR(true, true);
    TEST_EXPR(!true, false);
    TEST_EXPR(false, false);
    TEST_EXPR(!false, true);
    TEST_EXPR(!123, false);
    TEST_EXPR(!0, true);

    TEST_EXPR(123, 123);
    TEST_EXPR(-123, -123);
    TEST_EXPR(0x123, 0x123);

    TEST_EXPR(3.14, 3.14);

    // string
    {
        std::string query = "GO FROM 1 OVER follow WHERE \"string_literal\"";
        auto parsed = parser_->parse(query);
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        Getters getters;
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto value = expr->eval(getters);
        ASSERT_TRUE(value.ok());
        auto v = value.value();
        ASSERT_TRUE(Expression::isString(v));
        ASSERT_EQ("string_literal", Expression::asString(v));

        auto buffer = Expression::encode(expr);
        ASSERT_FALSE(buffer.empty());
        auto decoded = Expression::decode(buffer);
        ASSERT_TRUE(decoded.ok()) << decoded.status();
        ASSERT_NE(nullptr, decoded.value());
        value = decoded.value()->eval(getters);
        ASSERT_TRUE(value.ok());
        v = value.value();
        ASSERT_TRUE(Expression::isString(v));
        ASSERT_EQ("string_literal", Expression::asString(v));
    }
}


TEST_F(ExpressionTest, LiteralContantsArithmetic) {
    TEST_EXPR(8 + 16, 24);
    TEST_EXPR(8 - 16, -8);
    TEST_EXPR(8 * 16, 128);
    TEST_EXPR(8 / 16, 0);
    TEST_EXPR(16 / 8, 2);
    TEST_EXPR(8 % 16, 8);
    TEST_EXPR(16 % 8, 0);


    TEST_EXPR(16 + 4 + 2, 22);
    TEST_EXPR(16+4+2, 22);
    TEST_EXPR(16 - 4 - 2, 10);
    TEST_EXPR(16-4-2, 10);
    TEST_EXPR(16 - (4 - 2), 14);
    TEST_EXPR(16 * 4 * 2, 128);
    TEST_EXPR(16 / 4 / 2, 2);
    TEST_EXPR(16 / (4 / 2), 8);
    TEST_EXPR(16 / 4 % 2, 0);


    TEST_EXPR(16 * 8 + 4 - 2, 130);
    TEST_EXPR(16 * (8 + 4) - 2, 190);
    TEST_EXPR(16 + 8 * 4 - 2, 46);
    TEST_EXPR(16 + 8 * (4 - 2), 32);
    TEST_EXPR(16 + 8 + 4 * 2, 32);

    TEST_EXPR(16 / 8 + 4 - 2, 4);
    TEST_EXPR(16 / (8 + 4) - 2, -1);
    TEST_EXPR(16 + 8 / 4 - 2, 16);
    TEST_EXPR(16 + 8 / (4 - 2), 20);
    TEST_EXPR(16 + 8 + 4 / 2, 26);

    TEST_EXPR(17 % 7 + 4 - 2, 5);
    TEST_EXPR(17 + 7 % 4 - 2, 18);
    TEST_EXPR(17 + 7 + 4 % 2, 24);


    TEST_EXPR(16. + 8, 24.);
    TEST_EXPR(16 + 8., 24.);
    TEST_EXPR(16. - 8, 8.);
    TEST_EXPR(16 - 8., 8.);
    TEST_EXPR(16. * 8, 128.);
    TEST_EXPR(16 * 8., 128.);
    TEST_EXPR(16. / 8, 2.);
    TEST_EXPR(16 / 8., 2.);

    TEST_EXPR(16. * 8 + 4 - 2, 130.);
    TEST_EXPR(16 + 8. * 4 - 2, 46.);
    TEST_EXPR(16 + 8 + 4. * 2, 32.);

    TEST_EXPR(16. / 8 + 4 - 2, 4.);
    TEST_EXPR(16 + 8 / 4. - 2, 16.);
    TEST_EXPR(16 + 8 + 4 / 2., 26.);

    TEST_EXPR(3.14 * 3 * 3 / 2, 14.13);

    {
        std::string query = "GO FROM 1 OVER follow WHERE 16 + 8 / 4 - 2";
        auto parsed = parser_->parse(query);
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        Getters getters;
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto value = expr->eval(getters);
        ASSERT_TRUE(value.ok());
        auto v = value.value();
        ASSERT_TRUE(Expression::isInt(v));
        ASSERT_EQ(16, Expression::asInt(v));

        auto buffer = Expression::encode(expr);
        ASSERT_FALSE(buffer.empty());
        auto decoded = Expression::decode(buffer);
        ASSERT_TRUE(decoded.ok()) << decoded.status();
        ASSERT_NE(nullptr, decoded.value());
        value = decoded.value()->eval(getters);
        ASSERT_TRUE(value.ok());
        v = value.value();
        ASSERT_TRUE(Expression::isInt(v));
        ASSERT_EQ(16, Expression::asInt(v));
    }
}


TEST_F(ExpressionTest, LiteralConstantsRelational) {
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

    TEST_EXPR(abcd CONTAINS a, true)
    TEST_EXPR(abcd CONTAINS bc, true)
    TEST_EXPR(abcd CONTAINS cd, true)
    TEST_EXPR(bcd CONTAINS a, false)
    TEST_EXPR("abcd" CONTAINS "a", true)
    TEST_EXPR("abcd" CONTAINS "bc", true)
    TEST_EXPR("abcd" CONTAINS "cd", true)
    TEST_EXPR("bcd" CONTAINS "a", false)
    TEST_EXPR("Abcd" CONTAINS "A", true)
    TEST_EXPR("Abcd" CONTAINS "a", false)
    TEST_EXPR("abcd" CONTAINS "", true)
    TEST_EXPR("" CONTAINS "abcd", false)
    TEST_EXPR("" CONTAINS "", true)

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

    TEST_EXPR_FAILED(!(1/0));

    {
        std::string query = "GO FROM 1 OVER follow WHERE 3.14 * 3 * 3 / 2 == 14.13";
        auto parsed = parser_->parse(query);
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        Getters getters;
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto value = expr->eval(getters);
        ASSERT_TRUE(value.ok());
        auto v = value.value();
        ASSERT_TRUE(Expression::isBool(v));
        ASSERT_TRUE(Expression::asBool(v));
        auto decoded = Expression::decode(Expression::encode(expr));
        ASSERT_TRUE(decoded.ok()) << decoded.status();
        value = decoded.value()->eval(getters);
        ASSERT_TRUE(value.ok());
        v = value.value();
        ASSERT_TRUE(Expression::isBool(v));
        ASSERT_TRUE(Expression::asBool(v));
    }
    {
        std::string query = "GO FROM 1 OVER follow WHERE 3.14 * 3 * 3 / 2 != 3.14 * 1.5 * 1.5 / 2";
        auto parsed = parser_->parse(query);
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        Getters getters;
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto value = expr->eval(getters);
        ASSERT_TRUE(value.ok());
        auto v = value.value();
        ASSERT_TRUE(Expression::isBool(v));
        ASSERT_TRUE(Expression::asBool(v));
        auto decoded = Expression::decode(Expression::encode(expr));
        ASSERT_TRUE(decoded.ok()) << decoded.status();
        value = decoded.value()->eval(getters);
        ASSERT_TRUE(value.ok());
        v = value.value();
        ASSERT_TRUE(Expression::isBool(v));
        ASSERT_TRUE(Expression::asBool(v));
    }
}


TEST_F(ExpressionTest, LiteralConstantsLogical) {
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
}


TEST_F(ExpressionTest, InputReference) {
    {
        std::string query = "GO FROM 1 OVER follow WHERE $-.name";
        auto parsed = parser_->parse(query);
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto ctx = std::make_unique<ExpressionContext>();
        Getters getters;
        getters.getInputProp = [] (auto &prop) -> VariantType {
            if (prop == "name") {
                return std::string("Freddie");
            } else {
                return std::string("nobody");
            }
        };
        expr->setContext(ctx.get());
        auto value = expr->eval(getters);
        ASSERT_TRUE(value.ok());
        auto v = value.value();
        ASSERT_TRUE(Expression::isString(v));
        ASSERT_EQ("Freddie", Expression::asString(v));
    }
    {
        std::string query = "GO FROM 1 OVER follow WHERE $-.age >= 18";
        auto parsed = parser_->parse(query);
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto ctx = std::make_unique<ExpressionContext>();
        Getters getters;
        getters.getInputProp = [] (auto &prop) -> VariantType {
            if (prop == "age") {
                return 18L;
            } else {
                return 14L;
            }
        };
        expr->setContext(ctx.get());
        auto value = expr->eval(getters);
        ASSERT_TRUE(value.ok());
        auto v = value.value();
        ASSERT_TRUE(Expression::isBool(v));
        ASSERT_TRUE(Expression::asBool(v));
    }
}


TEST_F(ExpressionTest, SourceTagReference) {
    {
        std::string query = "GO FROM 1 OVER follow WHERE $^.person.name == \"dutor\"";
        auto parsed = parser_->parse(query);
        ASSERT_TRUE(parsed.ok()) << parsed.status();
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto ctx = std::make_unique<ExpressionContext>();
        Getters getters;
        getters.getSrcTagProp = [] (auto &tag, auto &prop) -> VariantType {
            if (tag == "person" && prop == "name") {
                return std::string("dutor");
            }
            return std::string("nobody");
        };
        expr->setContext(ctx.get());
        auto value = expr->eval(getters);
        ASSERT_TRUE(value.ok());
        auto v = value.value();
        ASSERT_TRUE(Expression::isBool(v));
        ASSERT_TRUE(Expression::asBool(v));
    }
}


TEST_F(ExpressionTest, EdgeReference) {
    {
        std::string query = "GO FROM 1 OVER follow WHERE follow._src == 1 "
                                                        "|| follow.cur_time < 1545798791"
                                                        "&& follow._dst == 2";
        auto parsed = parser_->parse(query);
        ASSERT_TRUE(parsed.ok());
        auto *expr = getFilterExpr(parsed.value().get());
        ASSERT_NE(nullptr, expr);
        auto ctx = std::make_unique<ExpressionContext>();
        Getters getters;
        getters.getAliasProp = [] (auto &, auto &prop) -> VariantType {
            if (prop == "cur_time") {
                return static_cast<int64_t>(::time(NULL));
            }
            if (prop == "_src") {
                return 0L;
            }
            return 1545798790L;
        };
        getters.getEdgeDstId = [] (auto&) -> VariantType {
            return 2L;
        };
        expr->setContext(ctx.get());
        auto value = expr->eval(getters);
        ASSERT_TRUE(value.ok());
        auto v = value.value();
        ASSERT_TRUE(Expression::isBool(v));
        ASSERT_FALSE(Expression::asBool(v));
    }
}


TEST_F(ExpressionTest, FunctionCall) {
    TEST_EXPR(abs(5), 5.0);
    TEST_EXPR(abs(-5), 5.0);

    TEST_EXPR(floor(3.14), 3.0);
    TEST_EXPR(floor(-3.14), -4.0);

    TEST_EXPR(ceil(3.14), 4.0);
    TEST_EXPR(ceil(-3.14), -3.0);

    TEST_EXPR(round(3.14), 3.0);
    TEST_EXPR(round(-3.14), -3.0);
    TEST_EXPR(round(3.5), 4.0);
    TEST_EXPR(round(-3.5), -4.0);

    TEST_EXPR(cbrt(27), 3.0);

    constexpr auto euler = 2.7182818284590451;
    TEST_EXPR(exp(1), euler);
    TEST_EXPR(exp2(10), 1024.);

    TEST_EXPR(log(pow(2.7182818284590451, 2)), 2.);
    TEST_EXPR(log2(1024), 10.);
    TEST_EXPR(log10(1000), 3.);

    TEST_EXPR(hypot(3, 4), 5.0);
    TEST_EXPR(sqrt(pow(3, 2) + pow(4, 2)), 5.0);

    TEST_EXPR(hypot(sin(0.5), cos(0.5)), 1.0);
    TEST_EXPR((sin(0.5) / cos(0.5)) / tan(0.5), 1.0);

    TEST_EXPR(sin(asin(0.3)), 0.3);
    TEST_EXPR(cos(acos(0.3)), 0.3);
    TEST_EXPR(tan(atan(0.3)), 0.3);

    TEST_EXPR_LT(rand32(1024), 1024);
    TEST_EXPR_GE(rand32(1024), 0);
    TEST_EXPR_FAILED(rand32(-1));
    TEST_EXPR_FAILED(rand32(-1, -2));
    TEST_EXPR_FAILED(rand32(3, 2));
    TEST_EXPR_FAILED(rand32(2, 2));
    TEST_EXPR_LT(rand64(1024, 4096), 4096);
    TEST_EXPR_GE(rand64(1024, 4096), 1024);
    TEST_EXPR_FAILED(rand64(-1));
    TEST_EXPR_FAILED(rand64(-1, -2));
    TEST_EXPR_FAILED(rand64(3, 2));
    TEST_EXPR_FAILED(rand64(2, 2));

    TEST_EXPR_GT(now(), 1554716753);
    TEST_EXPR_LE(now(), 4773548753);  // failed 102 years later

    TEST_EXPR(strcasecmp("HelLo", "hello"), 0);
    TEST_EXPR_GT(strcasecmp("HelLo", "hell"), 0);
    TEST_EXPR_LT(strcasecmp("HelLo", "World"), 0);

    TEST_EXPR(length("hello"), 5);
    TEST_EXPR(length(""), 0);
}

TEST_F(ExpressionTest, StringFunctionCall) {
    TEST_EXPR(lower("HelLo"), std::string("hello"));
    TEST_EXPR(lower("HELLO"), "hello");
    TEST_EXPR(lower("hello"), "hello");

    TEST_EXPR(upper("HelLo"), "HELLO");
    TEST_EXPR(upper("HELLO"), "HELLO");
    TEST_EXPR(upper("hello"), "HELLO");

    TEST_EXPR(trim(" hello "), "hello");
    TEST_EXPR(trim(" hello"), "hello");
    TEST_EXPR(trim("hello "), "hello");

    TEST_EXPR(ltrim(" hello "), "hello ");
    TEST_EXPR(ltrim(" hello"), "hello");
    TEST_EXPR(ltrim("hello "), "hello ");

    TEST_EXPR(rtrim(" hello "), " hello");
    TEST_EXPR(rtrim(" hello"), " hello");
    TEST_EXPR(rtrim("hello "), "hello");

    TEST_EXPR(left("hello world", 5), "hello");
    TEST_EXPR(left("hello world", 0), "");
    TEST_EXPR(left("hello world", -1), "");

    TEST_EXPR(right("hello world", 5), "world");
    TEST_EXPR(right("hello world", 0), "");
    TEST_EXPR(right("hello world", -1), "");

    TEST_EXPR(lpad("Hello", 8, "1"), "111Hello");
    TEST_EXPR(lpad("Hello", 8, "we"), "wewHello");
    TEST_EXPR(lpad("Hello", 4, "1"), "Hell");
    TEST_EXPR(lpad("Hello", 0, "1"), "");

    TEST_EXPR(rpad("Hello", 8, "1"), "Hello111");
    TEST_EXPR(rpad("Hello", 8, "we"), "Hellowew");
    TEST_EXPR(rpad("Hello", 4, "1"), "Hell");
    TEST_EXPR(rpad("Hello", 0, "1"), "");

    TEST_EXPR(substr("123", 1, 1), "1");
    TEST_EXPR(substr("123", 1, 0), "");
    TEST_EXPR(substr("123", 1, -1), "");
    TEST_EXPR(substr("123", -1, 1), "3");
    TEST_EXPR(substr("123", -1, 0), "");
    TEST_EXPR(substr("123", -1, -1), "");
    TEST_EXPR(substr("123", 5, 1), "");
    TEST_EXPR(substr("123", -5, 1), "");
}

TEST_F(ExpressionTest, InvalidExpressionTest) {
    TEST_EXPR_FAILED("a" + 1);
    TEST_EXPR_FAILED(3.14 + "a");
    TEST_EXPR_FAILED("ab" - "c");
    TEST_EXPR_FAILED(1 - "a");
    TEST_EXPR_FAILED("a" * 1);
    TEST_EXPR_FAILED(1.0 * "a");
    TEST_EXPR_FAILED(1 / "a");
    TEST_EXPR_FAILED("a" / "b");
    TEST_EXPR_FAILED(1.0 % "a");
    TEST_EXPR_FAILED(-"A");
    TEST_EXPR_FAILED(TRUE + FALSE);
    TEST_EXPR_FAILED("123" > 123);
    TEST_EXPR_FAILED("123" < 123);
    TEST_EXPR_FAILED("123" >= 123);
    TEST_EXPR_FAILED("123" <= 123);
    TEST_EXPR_FAILED("123" == 123);
    TEST_EXPR_FAILED("123" != 123);
    TEST_EXPR_FAILED("abc" CONTAINS 0);
    TEST_EXPR_FAILED("abc" CONTAINS 1.0);
    TEST_EXPR_FAILED("abc" CONTAINS true);
    TEST_EXPR_FAILED(0 CONTAINS 0);
}


TEST_F(ExpressionTest, StringLengthLimitTest) {
    constexpr auto MAX = (1UL<<20);
    std::string str(MAX, 'X');

    // double quote
    {
        auto *fmt = "GO FROM 1 OVER follow WHERE \"%s\"";
        {
            auto query = folly::stringPrintf(fmt, str.c_str());
            auto parsed = parser_->parse(query);
            ASSERT_TRUE(parsed.ok()) << parsed.status();
        }
    }
    // single quote
    {
        auto *fmt = "GO FROM 1 OVER follow WHERE '%s'";
        {
            auto query = folly::stringPrintf(fmt, str.c_str());
            auto parsed = parser_->parse(query);
            ASSERT_TRUE(parsed.ok()) << parsed.status();
        }
    }
}

}   // namespace nebula
