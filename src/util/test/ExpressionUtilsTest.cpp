/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "util/ExpressionUtils.h"
#include "parser/GQLParser.h"

namespace nebula {
namespace graph {

class ExpressionUtilsTest : public ::testing::Test {};

TEST_F(ExpressionUtilsTest, CheckComponent) {
    {
        // single node
        const auto root = std::make_unique<ConstantExpression>();

        ASSERT_TRUE(ExpressionUtils::isKindOf(root.get(), {Expression::Kind::kConstant}));
        ASSERT_TRUE(ExpressionUtils::hasAny(root.get(), {Expression::Kind::kConstant}));

        ASSERT_TRUE(ExpressionUtils::isKindOf(
            root.get(), {Expression::Kind::kConstant, Expression::Kind::kAdd}));
        ASSERT_TRUE(ExpressionUtils::hasAny(root.get(),
                                            {Expression::Kind::kConstant, Expression::Kind::kAdd}));

        ASSERT_FALSE(ExpressionUtils::isKindOf(root.get(), {Expression::Kind::kAdd}));
        ASSERT_FALSE(ExpressionUtils::hasAny(root.get(), {Expression::Kind::kAdd}));

        ASSERT_FALSE(ExpressionUtils::isKindOf(
            root.get(), {Expression::Kind::kDivision, Expression::Kind::kAdd}));
        ASSERT_FALSE(ExpressionUtils::hasAny(
            root.get(), {Expression::Kind::kDstProperty, Expression::Kind::kAdd}));

        // find
        const Expression *found =
            ExpressionUtils::findAny(root.get(), {Expression::Kind::kConstant});
        ASSERT_EQ(found, root.get());

        found = ExpressionUtils::findAny(
            root.get(),
            {Expression::Kind::kConstant, Expression::Kind::kAdd, Expression::Kind::kEdgeProperty});
        ASSERT_EQ(found, root.get());

        found = ExpressionUtils::findAny(root.get(), {Expression::Kind::kEdgeDst});
        ASSERT_EQ(found, nullptr);

        found = ExpressionUtils::findAny(
            root.get(), {Expression::Kind::kEdgeRank, Expression::Kind::kInputProperty});
        ASSERT_EQ(found, nullptr);

        // find all
        const auto willFoundAll = std::vector<const Expression *>{root.get()};
        std::vector<const Expression *> founds =
            ExpressionUtils::collectAll(root.get(), {Expression::Kind::kConstant});
        ASSERT_EQ(founds, willFoundAll);

        founds = ExpressionUtils::collectAll(
            root.get(),
            {Expression::Kind::kAdd, Expression::Kind::kConstant, Expression::Kind::kEdgeDst});
        ASSERT_EQ(founds, willFoundAll);

        founds = ExpressionUtils::collectAll(root.get(), {Expression::Kind::kSrcProperty});
        ASSERT_TRUE(founds.empty());

        founds = ExpressionUtils::collectAll(root.get(),
                                             {Expression::Kind::kUnaryNegate,
                                              Expression::Kind::kEdgeDst,
                                              Expression::Kind::kEdgeDst});
        ASSERT_TRUE(founds.empty());
    }

    {
        // list like
        const auto root = std::make_unique<TypeCastingExpression>(
            Value::Type::BOOL,
            new TypeCastingExpression(
                Value::Type::BOOL,
                new TypeCastingExpression(Value::Type::BOOL, new ConstantExpression())));

        ASSERT_TRUE(ExpressionUtils::isKindOf(root.get(), {Expression::Kind::kTypeCasting}));
        ASSERT_TRUE(ExpressionUtils::hasAny(root.get(), {Expression::Kind::kConstant}));

        ASSERT_TRUE(ExpressionUtils::isKindOf(
            root.get(), {Expression::Kind::kTypeCasting, Expression::Kind::kAdd}));
        ASSERT_TRUE(ExpressionUtils::hasAny(
            root.get(), {Expression::Kind::kTypeCasting, Expression::Kind::kAdd}));

        ASSERT_FALSE(ExpressionUtils::isKindOf(root.get(), {Expression::Kind::kAdd}));
        ASSERT_FALSE(ExpressionUtils::hasAny(root.get(), {Expression::Kind::kAdd}));

        ASSERT_FALSE(ExpressionUtils::isKindOf(
            root.get(), {Expression::Kind::kDivision, Expression::Kind::kAdd}));
        ASSERT_FALSE(ExpressionUtils::hasAny(
            root.get(), {Expression::Kind::kDstProperty, Expression::Kind::kAdd}));

        // found
        const Expression *found =
            ExpressionUtils::findAny(root.get(), {Expression::Kind::kTypeCasting});
        ASSERT_EQ(found, root.get());

        found = ExpressionUtils::findAny(root.get(),
                                         {Expression::Kind::kFunctionCall,
                                          Expression::Kind::kTypeCasting,
                                          Expression::Kind::kLogicalAnd});
        ASSERT_EQ(found, root.get());

        found = ExpressionUtils::findAny(root.get(), {Expression::Kind::kDivision});
        ASSERT_EQ(found, nullptr);

        found = ExpressionUtils::findAny(root.get(),
                                         {Expression::Kind::kLogicalXor,
                                          Expression::Kind::kRelGE,
                                          Expression::Kind::kEdgeProperty});
        ASSERT_EQ(found, nullptr);

        // found all
        std::vector<const Expression *> founds =
            ExpressionUtils::collectAll(root.get(), {Expression::Kind::kConstant});
        ASSERT_EQ(founds.size(), 1);

        founds = ExpressionUtils::collectAll(
            root.get(), {Expression::Kind::kFunctionCall, Expression::Kind::kTypeCasting});
        ASSERT_EQ(founds.size(), 3);

        founds = ExpressionUtils::collectAll(root.get(), {Expression::Kind::kAdd});
        ASSERT_TRUE(founds.empty());

        founds = ExpressionUtils::collectAll(
            root.get(), {Expression::Kind::kRelLE, Expression::Kind::kDstProperty});
        ASSERT_TRUE(founds.empty());
    }

    {
        // tree like
        const auto root = std::make_unique<ArithmeticExpression>(
            Expression::Kind::kAdd,
            new ArithmeticExpression(Expression::Kind::kDivision,
                                     new ConstantExpression(3),
                                     new ArithmeticExpression(Expression::Kind::kMinus,
                                                              new ConstantExpression(4),
                                                              new ConstantExpression(2))),
            new ArithmeticExpression(Expression::Kind::kMod,
                                     new ArithmeticExpression(Expression::Kind::kMultiply,
                                                              new ConstantExpression(3),
                                                              new ConstantExpression(10)),
                                     new ConstantExpression(2)));

        ASSERT_TRUE(ExpressionUtils::isKindOf(root.get(), {Expression::Kind::kAdd}));
        ASSERT_TRUE(ExpressionUtils::hasAny(root.get(), {Expression::Kind::kMinus}));

        ASSERT_TRUE(ExpressionUtils::isKindOf(
            root.get(), {Expression::Kind::kTypeCasting, Expression::Kind::kAdd}));
        ASSERT_TRUE(ExpressionUtils::hasAny(
            root.get(), {Expression::Kind::kLabelAttribute, Expression::Kind::kDivision}));

        ASSERT_FALSE(ExpressionUtils::isKindOf(root.get(), {Expression::Kind::kConstant}));
        ASSERT_FALSE(ExpressionUtils::hasAny(root.get(), {Expression::Kind::kFunctionCall}));

        ASSERT_FALSE(ExpressionUtils::isKindOf(
            root.get(), {Expression::Kind::kDivision, Expression::Kind::kEdgeProperty}));
        ASSERT_FALSE(ExpressionUtils::hasAny(
            root.get(), {Expression::Kind::kDstProperty, Expression::Kind::kLogicalAnd}));

        // found
        const Expression *found = ExpressionUtils::findAny(root.get(), {Expression::Kind::kAdd});
        ASSERT_EQ(found, root.get());

        found = ExpressionUtils::findAny(root.get(),
                                         {Expression::Kind::kFunctionCall,
                                          Expression::Kind::kRelLE,
                                          Expression::Kind::kMultiply});
        ASSERT_NE(found, nullptr);

        found = ExpressionUtils::findAny(root.get(), {Expression::Kind::kInputProperty});
        ASSERT_EQ(found, nullptr);

        found = ExpressionUtils::findAny(root.get(),
                                         {Expression::Kind::kLogicalXor,
                                          Expression::Kind::kEdgeRank,
                                          Expression::Kind::kUnaryNot});
        ASSERT_EQ(found, nullptr);

        // found all
        std::vector<const Expression *> founds =
            ExpressionUtils::collectAll(root.get(), {Expression::Kind::kConstant});
        ASSERT_EQ(founds.size(), 6);

        founds = ExpressionUtils::collectAll(
            root.get(), {Expression::Kind::kDivision, Expression::Kind::kMinus});
        ASSERT_EQ(founds.size(), 2);

        founds = ExpressionUtils::collectAll(root.get(), {Expression::Kind::kEdgeDst});
        ASSERT_TRUE(founds.empty());

        founds = ExpressionUtils::collectAll(
            root.get(), {Expression::Kind::kLogicalAnd, Expression::Kind::kUnaryNegate});
        ASSERT_TRUE(founds.empty());
    }
}

TEST_F(ExpressionUtilsTest, PullAnds) {
    using Kind = Expression::Kind;
    // true AND false
    {
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        LogicalExpression expr(Kind::kLogicalAnd, first, second);
        LogicalExpression expected(Kind::kLogicalAnd,
                                   first->clone().release(),
                                   second->clone().release());
        ExpressionUtils::pullAnds(&expr);
        ASSERT_EQ(expected, expr);
    }
    // true AND false AND true
    {
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        auto *third = new ConstantExpression(true);
        LogicalExpression expr(Kind::kLogicalAnd,
                new LogicalExpression(Kind::kLogicalAnd,
                    first,
                    second),
                third);
        LogicalExpression expected(Kind::kLogicalAnd);
        expected.addOperand(first->clone().release());
        expected.addOperand(second->clone().release());
        expected.addOperand(third->clone().release());
        ExpressionUtils::pullAnds(&expr);
        ASSERT_EQ(expected, expr);
    }
    // true AND (false AND true)
    {
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        auto *third = new ConstantExpression(true);
        LogicalExpression expr(Kind::kLogicalAnd,
                first,
                new LogicalExpression(Kind::kLogicalAnd,
                    second,
                    third));
        LogicalExpression expected(Kind::kLogicalAnd);
        expected.addOperand(first->clone().release());
        expected.addOperand(second->clone().release());
        expected.addOperand(third->clone().release());
        ExpressionUtils::pullAnds(&expr);
        ASSERT_EQ(expected, expr);
    }
    // (true OR false) AND (true OR false)
    {
        auto *first = new LogicalExpression(Kind::kLogicalOr,
                new ConstantExpression(true),
                new ConstantExpression(false));
        auto *second = new LogicalExpression(Kind::kLogicalOr,
                new ConstantExpression(true),
                new ConstantExpression(false));
        LogicalExpression expr(Kind::kLogicalAnd, first, second);
        LogicalExpression expected(Kind::kLogicalAnd);
        expected.addOperand(first->clone().release());
        expected.addOperand(second->clone().release());
        ExpressionUtils::pullAnds(&expr);
        ASSERT_EQ(expected, expr);
    }
    // true AND ((false AND true) OR false) AND true
    {
        auto *first = new ConstantExpression(true);
        auto *second = new LogicalExpression(Kind::kLogicalOr,
                new LogicalExpression(Kind::kLogicalAnd,
                    new ConstantExpression(false),
                    new ConstantExpression(true)),
                new ConstantExpression(false));
        auto *third = new ConstantExpression(true);
        LogicalExpression expr(Kind::kLogicalAnd,
                new LogicalExpression(Kind::kLogicalAnd, first, second), third);
        LogicalExpression expected(Kind::kLogicalAnd);
        expected.addOperand(first->clone().release());
        expected.addOperand(second->clone().release());
        expected.addOperand(third->clone().release());
        ExpressionUtils::pullAnds(&expr);
        ASSERT_EQ(expected, expr);
    }
}

TEST_F(ExpressionUtilsTest, PullOrs) {
    using Kind = Expression::Kind;
    // true OR false
    {
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        LogicalExpression expr(Kind::kLogicalOr, first, second);
        LogicalExpression expected(Kind::kLogicalOr,
                first->clone().release(),
                second->clone().release());
        ExpressionUtils::pullOrs(&expr);
        ASSERT_EQ(expected, expr);
    }
    // true OR false OR true
    {
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        auto *third = new ConstantExpression(true);
        LogicalExpression expr(Kind::kLogicalOr,
                new LogicalExpression(Kind::kLogicalOr, first, second), third);
        LogicalExpression expected(Kind::kLogicalOr);
        expected.addOperand(first->clone().release());
        expected.addOperand(second->clone().release());
        expected.addOperand(third->clone().release());
        ExpressionUtils::pullOrs(&expr);
        ASSERT_EQ(expected, expr);
    }
    // true OR (false OR true)
    {
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        auto *third = new ConstantExpression(true);
        LogicalExpression expr(Kind::kLogicalOr,
                first,
                new LogicalExpression(Kind::kLogicalOr, second, third));
        LogicalExpression expected(Kind::kLogicalOr);
        expected.addOperand(first->clone().release());
        expected.addOperand(second->clone().release());
        expected.addOperand(third->clone().release());
        ExpressionUtils::pullOrs(&expr);
        ASSERT_EQ(expected, expr);
    }
    // (true AND false) OR (true AND false)
    {
        auto *first = new LogicalExpression(Kind::kLogicalAnd,
                new ConstantExpression(true),
                new ConstantExpression(false));
        auto *second = new LogicalExpression(Kind::kLogicalAnd,
                new ConstantExpression(true),
                new ConstantExpression(false));
        LogicalExpression expr(Kind::kLogicalOr, first, second);
        LogicalExpression expected(Kind::kLogicalOr,
                first->clone().release(),
                second->clone().release());
        ExpressionUtils::pullOrs(&expr);
        ASSERT_EQ(expected, expr);
    }
    // true OR ((false OR true) AND false) OR true
    {
        auto *first = new ConstantExpression(true);
        auto *second = new LogicalExpression(Kind::kLogicalAnd,
                new LogicalExpression(Kind::kLogicalOr,
                    new ConstantExpression(false),
                    new ConstantExpression(true)),
                new ConstantExpression(false));
        auto *third = new ConstantExpression(true);
        LogicalExpression expr(Kind::kLogicalOr,
                new LogicalExpression(Kind::kLogicalOr, first, second), third);
        LogicalExpression expected(Kind::kLogicalOr);
        expected.addOperand(first->clone().release());
        expected.addOperand(second->clone().release());
        expected.addOperand(third->clone().release());
        ExpressionUtils::pullOrs(&expr);
        ASSERT_EQ(expected, expr);
    }
}

TEST_F(ExpressionUtilsTest, pushOrs) {
    std::vector<std::unique_ptr<Expression>> rels;
    for (int16_t i = 0; i < 5; i++) {
        auto r = std::make_unique<RelationalExpression>(
            Expression::Kind::kRelEQ,
            new LabelAttributeExpression(new LabelExpression(folly::stringPrintf("tag%d", i)),
                                         new ConstantExpression(folly::stringPrintf("col%d", i))),
            new ConstantExpression(Value(folly::stringPrintf("val%d", i))));
        rels.emplace_back(std::move(r));
    }
    auto t = ExpressionUtils::pushOrs(rels);
    auto expected = std::string("(((((tag0.col0==\"val0\") OR "
                                "(tag1.col1==\"val1\")) OR "
                                "(tag2.col2==\"val2\")) OR "
                                "(tag3.col3==\"val3\")) OR "
                                "(tag4.col4==\"val4\"))");
    ASSERT_EQ(expected, t->toString());
}

TEST_F(ExpressionUtilsTest, pushAnds) {
    std::vector<std::unique_ptr<Expression>> rels;
    for (int16_t i = 0; i < 5; i++) {
        auto r = std::make_unique<RelationalExpression>(
            Expression::Kind::kRelEQ,
            new LabelAttributeExpression(new LabelExpression(folly::stringPrintf("tag%d", i)),
                                         new ConstantExpression(folly::stringPrintf("col%d", i))),
            new ConstantExpression(Value(folly::stringPrintf("val%d", i))));
        rels.emplace_back(std::move(r));
    }
    auto t = ExpressionUtils::pushAnds(rels);
    auto expected = std::string("(((((tag0.col0==\"val0\") AND "
                                "(tag1.col1==\"val1\")) AND "
                                "(tag2.col2==\"val2\")) AND "
                                "(tag3.col3==\"val3\")) AND "
                                "(tag4.col4==\"val4\"))");
    ASSERT_EQ(expected, t->toString());
}

TEST_F(ExpressionUtilsTest, flattenInnerLogicalExpr) {
    using Kind = Expression::Kind;
    // true AND false AND true
    {
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        auto *third = new ConstantExpression(true);
        LogicalExpression expr(Kind::kLogicalAnd,
                new LogicalExpression(Kind::kLogicalAnd,
                    first,
                    second),
                third);
        LogicalExpression expected(Kind::kLogicalAnd);
        expected.addOperand(first->clone().release());
        expected.addOperand(second->clone().release());
        expected.addOperand(third->clone().release());
        auto newExpr = ExpressionUtils::flattenInnerLogicalExpr(&expr);
        ASSERT_EQ(expected, *newExpr);
    }
    // true OR false OR true
    {
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        auto *third = new ConstantExpression(true);
        LogicalExpression expr(Kind::kLogicalOr,
                new LogicalExpression(Kind::kLogicalOr,
                    first,
                    second),
                third);
        LogicalExpression expected(Kind::kLogicalOr);
        expected.addOperand(first->clone().release());
        expected.addOperand(second->clone().release());
        expected.addOperand(third->clone().release());
        auto newExpr = ExpressionUtils::flattenInnerLogicalExpr(&expr);
        ASSERT_EQ(expected, *newExpr);
    }
    // (true OR false OR true)==(true AND false AND true)
    {
        auto *or1 = new ConstantExpression(true);
        auto *or2 = new ConstantExpression(false);
        auto *or3 = new ConstantExpression(true);
        auto* logicOrExpr = new LogicalExpression(Kind::kLogicalOr,
                new LogicalExpression(Kind::kLogicalOr,
                    or1,
                    or2),
                or3);
        auto *and1 = new ConstantExpression(false);
        auto *and2 = new ConstantExpression(false);
        auto *and3 = new ConstantExpression(true);
        auto* logicAndExpr = new LogicalExpression(Kind::kLogicalAnd,
                new LogicalExpression(Kind::kLogicalAnd,
                    and1,
                    and2),
                and3);
        RelationalExpression expr(Kind::kRelEQ, logicOrExpr, logicAndExpr);

        auto* logicOrFlatten = new LogicalExpression(Kind::kLogicalOr);
        logicOrFlatten->addOperand(or1->clone().release());
        logicOrFlatten->addOperand(or2->clone().release());
        logicOrFlatten->addOperand(or3->clone().release());
        auto* logicAndFlatten = new LogicalExpression(Kind::kLogicalAnd);
        logicAndFlatten->addOperand(and1->clone().release());
        logicAndFlatten->addOperand(and2->clone().release());
        logicAndFlatten->addOperand(and3->clone().release());
        RelationalExpression expected(Kind::kRelEQ, logicOrFlatten, logicAndFlatten);

        auto newExpr = ExpressionUtils::flattenInnerLogicalExpr(&expr);
        ASSERT_EQ(expected, *newExpr);
    }
    // (true OR false OR true) AND (true AND false AND true)
    {
        auto *or1 = new ConstantExpression(true);
        auto *or2 = new ConstantExpression(false);
        auto *or3 = new ConstantExpression(true);
        auto* logicOrExpr = new LogicalExpression(Kind::kLogicalOr,
                new LogicalExpression(Kind::kLogicalOr,
                    or1,
                    or2),
                or3);
        auto *and1 = new ConstantExpression(false);
        auto *and2 = new ConstantExpression(false);
        auto *and3 = new ConstantExpression(true);
        auto* logicAndExpr = new LogicalExpression(Kind::kLogicalAnd,
                new LogicalExpression(Kind::kLogicalAnd,
                    and1,
                    and2),
                and3);
        LogicalExpression expr(Kind::kLogicalAnd, logicOrExpr, logicAndExpr);

        auto* logicOrFlatten = new LogicalExpression(Kind::kLogicalOr);
        logicOrFlatten->addOperand(or1->clone().release());
        logicOrFlatten->addOperand(or2->clone().release());
        logicOrFlatten->addOperand(or3->clone().release());
        LogicalExpression expected(Kind::kLogicalAnd);
        expected.addOperand(logicOrFlatten);
        expected.addOperand(and1->clone().release());
        expected.addOperand(and2->clone().release());
        expected.addOperand(and3->clone().release());

        auto newExpr = ExpressionUtils::flattenInnerLogicalExpr(&expr);
        ASSERT_EQ(expected, *newExpr);
    }
}

TEST_F(ExpressionUtilsTest, splitFilter) {
    using Kind = Expression::Kind;
    {
        // true AND false AND true
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        auto *third = new ConstantExpression(true);
        LogicalExpression expr(
            Kind::kLogicalAnd, new LogicalExpression(Kind::kLogicalAnd, first, second), third);
        LogicalExpression expected1(Kind::kLogicalAnd);
        expected1.addOperand(first->clone().release());
        expected1.addOperand(third->clone().release());
        auto picker = [](const Expression *e) {
            if (e->kind() != Kind::kConstant) return false;
            auto &v = static_cast<const ConstantExpression *>(e)->value();
            if (v.type() != Value::Type::BOOL) return false;
            return v.getBool();
        };
        std::unique_ptr<Expression> newExpr1;
        std::unique_ptr<Expression> newExpr2;
        ExpressionUtils::splitFilter(&expr, picker, &newExpr1, &newExpr2);
        ASSERT_EQ(expected1, *newExpr1);
        ASSERT_EQ(*second, *newExpr2);
    }
    {
        // true
        auto expr = std::make_unique<ConstantExpression>(true);
        auto picker = [](const Expression *e) {
            if (e->kind() != Kind::kConstant) return false;
            auto &v = static_cast<const ConstantExpression *>(e)->value();
            if (v.type() != Value::Type::BOOL) return false;
            return v.getBool();
        };
        std::unique_ptr<Expression> newExpr1;
        std::unique_ptr<Expression> newExpr2;
        ExpressionUtils::splitFilter(expr.get(), picker, &newExpr1, &newExpr2);
        ASSERT_EQ(*expr, *newExpr1);
        ASSERT_EQ(nullptr, newExpr2);
    }
}

std::unique_ptr<Expression> parse(const std::string& expr) {
    std::string query = "LOOKUP on t1 WHERE " + expr;
    GQLParser parser;
    auto result = parser.parse(std::move(query));
    CHECK(result.ok()) << result.status();
    auto stmt = std::move(result).value();
    auto *seq = static_cast<SequentialSentences *>(stmt.get());
    auto *lookup = static_cast<LookupSentence *>(seq->sentences()[0]);
    return lookup->whereClause()->filter()->clone();
}

TEST_F(ExpressionUtilsTest, expandExpression) {
    {
        auto filter = parse("t1.c1 == 1");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "(t1.c1==1)";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("t1.c1 == 1 and t1.c2 == 2");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "((t1.c1==1) AND (t1.c2==2))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("t1.c1 == 1 and t1.c2 == 2 and t1.c3 == 3");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "(((t1.c1==1) AND (t1.c2==2)) AND (t1.c3==3))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("t1.c1 == 1 or t1.c2 == 2");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "((t1.c1==1) OR (t1.c2==2))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("t1.c1 == 1 or t1.c2 == 2 or t1.c3 == 3");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "(((t1.c1==1) OR (t1.c2==2)) OR (t1.c3==3))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("t1.c1 == 1 and t1.c2 == 2 or t1.c1 == 3");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "(((t1.c1==1) AND (t1.c2==2)) OR (t1.c1==3))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("t1.c1 == 1 or t1.c2 == 2 and t1.c1 == 3");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "((t1.c1==1) OR ((t1.c2==2) AND (t1.c1==3)))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("(t1.c1 == 1 or t1.c2 == 2) and t1.c3 == 3");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "(((t1.c1==1) AND (t1.c3==3)) OR ((t1.c2==2) AND (t1.c3==3)))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("(t1.c1 == 1 or t1.c2 == 2) and t1.c3 == 3 or t1.c4 == 4");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "((((t1.c1==1) AND (t1.c3==3)) OR "
                        "((t1.c2==2) AND (t1.c3==3))) OR "
                        "(t1.c4==4))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("(t1.c1 == 1 or t1.c2 == 2) and (t1.c3 == 3 or t1.c4 == 4)");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "(((((t1.c1==1) AND (t1.c3==3)) OR "
                        "((t1.c1==1) AND (t1.c4==4))) OR "
                        "((t1.c2==2) AND (t1.c3==3))) OR "
                        "((t1.c2==2) AND (t1.c4==4)))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("(t1.c1 == 1 or t1.c2 == 2 or t1.c3 == 3 or t1.c4 == 4) "
                            "and t1.c5 == 5");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "(((((t1.c1==1) AND (t1.c5==5)) OR "
                        "((t1.c2==2) AND (t1.c5==5))) OR "
                        "((t1.c3==3) AND (t1.c5==5))) OR "
                        "((t1.c4==4) AND (t1.c5==5)))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("(t1.c1 == 1 or t1.c2 == 2) and t1.c4 == 4 and t1.c5 == 5");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "((((t1.c1==1) AND (t1.c4==4)) AND (t1.c5==5)) OR "
                        "(((t1.c2==2) AND (t1.c4==4)) AND (t1.c5==5)))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("t1.c1 == 1 and (t1.c2 == 2 or t1.c4 == 4) and t1.c5 == 5");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "((((t1.c1==1) AND (t1.c2==2)) AND (t1.c5==5)) OR "
                        "(((t1.c1==1) AND (t1.c4==4)) AND (t1.c5==5)))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("t1.c1 == 1 and t1.c2 == 2 and (t1.c4 == 4 or t1.c5 == 5)");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "((((t1.c1==1) AND (t1.c2==2)) AND (t1.c4==4)) OR "
                        "(((t1.c1==1) AND (t1.c2==2)) AND (t1.c5==5)))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("(t1.c1 == 1 or t1.c2 == 2) and "
                            "(t1.c3 == 3 or t1.c4 == 4) and "
                            "t1.c5 == 5");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "((((((t1.c1==1) AND (t1.c3==3)) AND (t1.c5==5)) OR "
                        "(((t1.c1==1) AND (t1.c4==4)) AND (t1.c5==5))) OR "
                        "(((t1.c2==2) AND (t1.c3==3)) AND (t1.c5==5))) OR "
                        "(((t1.c2==2) AND (t1.c4==4)) AND (t1.c5==5)))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("t1.c4 == 4 or (t1.c1 == 1 and (t1.c2 == 2 or t1.c3 == 3))");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "((t1.c4==4) OR "
                        "(((t1.c1==1) AND (t1.c2==2)) OR "
                        "((t1.c1==1) AND (t1.c3==3))))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("t1.c4 == 4 or "
                            "(t1.c1 == 1 and (t1.c2 == 2 or t1.c3 == 3)) or "
                            "t1.c5 == 5");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "(((t1.c4==4) OR "
                        "(((t1.c1==1) AND (t1.c2==2)) OR ((t1.c1==1) AND (t1.c3==3)))) OR "
                        "(t1.c5==5))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        auto filter = parse("t1.c1 == 1 and (t1.c2 == 2 or t1.c4) and t1.c5 == 5");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "((((t1.c1==1) AND (t1.c2==2)) AND (t1.c5==5)) OR "
                        "(((t1.c1==1) AND t1.c4) AND (t1.c5==5)))";
        ASSERT_EQ(expected, target->toString());
    }
    {
        // Invalid expression for index. don't need to expand.
        auto filter = parse("t1.c1 == 1 and (t1.c2 == 2 or t1.c4) == true and t1.c5 == 5");
        auto target = ExpressionUtils::expandExpr(filter.get());
        auto expected = "(((t1.c1==1) AND (((t1.c2==2) OR t1.c4)==true)) AND (t1.c5==5))";
        ASSERT_EQ(expected, target->toString());
    }
}
}   // namespace graph
}   // namespace nebula
