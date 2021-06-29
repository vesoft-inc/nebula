/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class RelationalExpressionTest : public ExpressionTest {};

TEST_F(ExpressionTest, LiteralConstantsRelational) {
    {
        TEST_EXPR(true == 1.0, false);
        TEST_EXPR(true == 2.0, false);
        TEST_EXPR(true != 1.0, true);
        TEST_EXPR(true != 2.0, true);
        TEST_EXPR(true > 1.0, Value::kNullBadType);
        TEST_EXPR(true >= 1.0, Value::kNullBadType);
        TEST_EXPR(true < 1.0, Value::kNullBadType);
        TEST_EXPR(true <= 1.0, Value::kNullBadType);
        TEST_EXPR(false == 0.0, false);
        TEST_EXPR(false == 1.0, false);
        TEST_EXPR(false != 0.0, true);
        TEST_EXPR(false != 1.0, true);
        TEST_EXPR(false > 0.0, Value::kNullBadType);
        TEST_EXPR(false >= 0.0, Value::kNullBadType);
        TEST_EXPR(false < 0.0, Value::kNullBadType);
        TEST_EXPR(false <= 0.0, Value::kNullBadType);

        TEST_EXPR(true == 1, false);
        TEST_EXPR(true == 2, false);
        TEST_EXPR(true != 1, true);
        TEST_EXPR(true != 2, true);
        TEST_EXPR(true > 1, Value::kNullBadType);
        TEST_EXPR(true >= 1, Value::kNullBadType);
        TEST_EXPR(true < 1, Value::kNullBadType);
        TEST_EXPR(true <= 1, Value::kNullBadType);
        TEST_EXPR(false == 0, false);
        TEST_EXPR(false == 1, false);
        TEST_EXPR(false != 0, true);
        TEST_EXPR(false != 1, true);
        TEST_EXPR(false > 0, Value::kNullBadType);
        TEST_EXPR(false >= 0, Value::kNullBadType);
        TEST_EXPR(false < 0, Value::kNullBadType);
        TEST_EXPR(false <= 0, Value::kNullBadType);
    }
    {
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

        TEST_EXPR(1 == 1, true);
        TEST_EXPR(1 != 1, false);
        TEST_EXPR(1 > 1, false);
        TEST_EXPR(1 >= 1, true);
        TEST_EXPR(1 < 1, false);
        TEST_EXPR(1 <= 1, true);
    }
    {
        TEST_EXPR(empty == empty, true);
        TEST_EXPR(empty == null, Value::kNullValue);
        TEST_EXPR(empty != null, Value::kNullValue);
        TEST_EXPR(empty != 1, true);
        TEST_EXPR(empty != true, true);
        TEST_EXPR(empty > "1", Value::kEmpty);
        TEST_EXPR(empty < 1, Value::kEmpty);
        TEST_EXPR(empty >= 1.11, Value::kEmpty);

        TEST_EXPR(null != 1, Value::kNullValue);
        TEST_EXPR(null != true, Value::kNullValue);
        TEST_EXPR(null > "1", Value::kNullValue);
        TEST_EXPR(null < 1, Value::kNullValue);
        TEST_EXPR(null >= 1.11, Value::kNullValue);
    }
    {
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
    }
}

// TODO: Add tests for all kinds of relExpr
TEST_F(RelationalExpressionTest, RelationEQ) {
    {
        // e1.list == NULL
        auto expr =
            RelationalExpression::makeEQ(&pool,
                                         EdgePropertyExpression::make(&pool, "e1", "list"),
                                         ConstantExpression::make(&pool, Value(NullType::NaN)));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        // e1.list_of_list == NULL
        auto expr =
            RelationalExpression::makeEQ(&pool,
                                         EdgePropertyExpression::make(&pool, "e1", "list_of_list"),
                                         ConstantExpression::make(&pool, Value(NullType::NaN)));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        // e1.list == e1.list
        auto expr = RelationalExpression::makeEQ(&pool,
                                                 EdgePropertyExpression::make(&pool, "e1", "list"),
                                                 EdgePropertyExpression::make(&pool, "e1", "list"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // e1.list_of_list == e1.list_of_list
        auto expr =
            RelationalExpression::makeEQ(&pool,
                                         EdgePropertyExpression::make(&pool, "e1", "list_of_list"),
                                         EdgePropertyExpression::make(&pool, "e1", "list_of_list"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // 1 == NULL
        auto expr =
            RelationalExpression::makeEQ(&pool,
                                         ConstantExpression::make(&pool, Value(1)),
                                         ConstantExpression::make(&pool, Value(NullType::NaN)));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        // NULL == NULL
        auto expr =
            RelationalExpression::makeEQ(&pool,
                                         ConstantExpression::make(&pool, Value(NullType::NaN)),
                                         ConstantExpression::make(&pool, Value(NullType::NaN)));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
}

TEST_F(RelationalExpressionTest, RelationNE) {
    {
        // 1 != NULL
        auto expr =
            RelationalExpression::makeNE(&pool,
                                         ConstantExpression::make(&pool, Value(1)),
                                         ConstantExpression::make(&pool, Value(NullType::NaN)));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        // NULL != NULL
        auto expr =
            RelationalExpression::makeNE(&pool,
                                         ConstantExpression::make(&pool, Value(NullType::NaN)),
                                         ConstantExpression::make(&pool, Value(NullType::NaN)));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
}

TEST_F(RelationalExpressionTest, RelationLT) {
    {
        // 1 < NULL
        auto expr =
            RelationalExpression::makeLT(&pool,
                                         ConstantExpression::make(&pool, Value(1)),
                                         ConstantExpression::make(&pool, Value(NullType::NaN)));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullValue);
    }
    {
        // NULL < NULL
        auto expr =
            RelationalExpression::makeLT(&pool,
                                         ConstantExpression::make(&pool, Value(NullType::NaN)),
                                         ConstantExpression::make(&pool, Value(NullType::NaN)));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullValue);
    }
}

TEST_F(RelationalExpressionTest, RelationIn) {
    {
        auto expr = RelationalExpression::makeIn(&pool,
                                                 ConstantExpression::make(&pool, 1),
                                                 ConstantExpression::make(&pool, List({1, 2})));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr = RelationalExpression::makeIn(&pool,
                                                 ConstantExpression::make(&pool, 3),
                                                 ConstantExpression::make(&pool, List({1, 2})));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto expr = RelationalExpression::makeIn(&pool,
                                                 ConstantExpression::make(&pool, Value::kNullValue),
                                                 ConstantExpression::make(&pool, List({1, 2})));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto list = List({1, 2});
        list.emplace_back(Value::kNullValue);
        auto expr = RelationalExpression::makeIn(
            &pool, ConstantExpression::make(&pool, 1), ConstantExpression::make(&pool, list));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto list = List({3, 2});
        list.emplace_back(Value::kNullValue);
        auto expr = RelationalExpression::makeIn(
            &pool, ConstantExpression::make(&pool, 1), ConstantExpression::make(&pool, list));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto list = List({3, 2});
        list.emplace_back(Value::kNullValue);
        auto expr = RelationalExpression::makeIn(
            &pool, ConstantExpression::make(&pool, 1), ConstantExpression::make(&pool, list));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto list = List({3, 2});
        list.emplace_back(Value::kNullValue);
        auto expr = RelationalExpression::makeIn(
            &pool, ConstantExpression::make(&pool, list), ConstantExpression::make(&pool, list));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto expr =
            RelationalExpression::makeIn(&pool,
                                         ConstantExpression::make(&pool, Value::kNullValue),
                                         ConstantExpression::make(&pool, Value::kNullValue));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
    {
        auto expr =
            RelationalExpression::makeIn(&pool,
                                         ConstantExpression::make(&pool, 2.3),
                                         ConstantExpression::make(&pool, Value::kNullValue));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
    {
        auto list1 = List({3, 2});
        auto list2 = list1;
        list1.emplace_back(Value::kNullValue);
        auto expr = RelationalExpression::makeIn(
            &pool, ConstantExpression::make(&pool, list1), ConstantExpression::make(&pool, list2));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto list1 = List({3, 2});
        auto list2 = List({1});
        list2.emplace_back(list1);
        list2.emplace_back(Value::kNullValue);
        auto expr = RelationalExpression::makeIn(
            &pool, ConstantExpression::make(&pool, list1), ConstantExpression::make(&pool, list2));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
}

TEST_F(RelationalExpressionTest, RelationNotIn) {
    {
        auto expr = RelationalExpression::makeNotIn(&pool,
                                                     ConstantExpression::make(&pool, 1),
                                                     ConstantExpression::make(&pool, List({1, 2})));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto expr = RelationalExpression::makeNotIn(&pool,
                                                     ConstantExpression::make(&pool, 3),
                                                     ConstantExpression::make(&pool, List({1, 2})));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr =
           RelationalExpression::makeNotIn(&pool,
                                             ConstantExpression::make(&pool, Value::kNullValue),
                                             ConstantExpression::make(&pool, List({1, 2})));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto list = List({1, 2});
        list.emplace_back(Value::kNullValue);
        auto expr = RelationalExpression::makeNotIn(
            &pool, ConstantExpression::make(&pool, 1), ConstantExpression::make(&pool, list));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto list = List({3, 2});
        list.emplace_back(Value::kNullValue);
        auto expr = RelationalExpression::makeNotIn(
            &pool, ConstantExpression::make(&pool, 1), ConstantExpression::make(&pool, list));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto list = List({3, 2});
        list.emplace_back(Value::kNullValue);
        auto expr = RelationalExpression::makeNotIn(
            &pool, ConstantExpression::make(&pool, 1), ConstantExpression::make(&pool, list));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto list = List({3, 2});
        list.emplace_back(Value::kNullValue);
        auto expr = RelationalExpression::makeNotIn(
            &pool, ConstantExpression::make(&pool, list), ConstantExpression::make(&pool, list));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto expr =
           RelationalExpression::makeNotIn(&pool,
                                             ConstantExpression::make(&pool, Value::kNullValue),
                                             ConstantExpression::make(&pool, Value::kNullValue));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
    {
        auto expr =
           RelationalExpression::makeNotIn(&pool,
                                             ConstantExpression::make(&pool, 2.3),
                                             ConstantExpression::make(&pool, Value::kNullValue));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
    {
        auto list1 = List({3, 2});
        auto list2 = list1;
        list1.emplace_back(Value::kNullValue);
        auto expr = RelationalExpression::makeNotIn(
            &pool, ConstantExpression::make(&pool, list1), ConstantExpression::make(&pool, list2));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto list1 = List({3, 2});
        auto list2 = List({1});
        list2.emplace_back(list1);
        list2.emplace_back(Value::kNullValue);
        auto expr = RelationalExpression::makeNotIn(
            &pool, ConstantExpression::make(&pool, list1), ConstantExpression::make(&pool, list2));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
}

TEST_F(RelationalExpressionTest, InList) {
    {
        auto *elist = ExpressionList::make(&pool);
        (*elist)
            .add(ConstantExpression::make(&pool, 12345))
            .add(ConstantExpression::make(&pool, "Hello"))
            .add(ConstantExpression::make(&pool, true));
        auto listExpr = ListExpression::make(&pool, elist);
        auto expr =
           RelationalExpression::makeIn(&pool, ConstantExpression::make(&pool, 12345), listExpr);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value);
    }
    {
        auto *elist = ExpressionList::make(&pool);
        (*elist)
            .add(ConstantExpression::make(&pool, 12345))
            .add(ConstantExpression::make(&pool, "Hello"))
            .add(ConstantExpression::make(&pool, true));
        auto listExpr = ListExpression::make(&pool, elist);
        auto expr =
           RelationalExpression::makeIn(&pool, ConstantExpression::make(&pool, false), listExpr);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value);
    }
}

TEST_F(RelationalExpressionTest, InSet) {
    {
        auto *elist = ExpressionList::make(&pool);
        (*elist)
            .add(ConstantExpression::make(&pool, 12345))
            .add(ConstantExpression::make(&pool, "Hello"))
            .add(ConstantExpression::make(&pool, true));
        auto setExpr = SetExpression::make(&pool, elist);
        auto expr =
           RelationalExpression::makeIn(&pool, ConstantExpression::make(&pool, 12345), setExpr);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value);
    }
    {
        auto *elist = ExpressionList::make(&pool);
        (*elist)
            .add(ConstantExpression::make(&pool, 12345))
            .add(ConstantExpression::make(&pool, "Hello"))
            .add(ConstantExpression::make(&pool, true));
        auto setExpr = ListExpression::make(&pool, elist);
        auto expr =
           RelationalExpression::makeIn(&pool, ConstantExpression::make(&pool, false), setExpr);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value);
    }
}

TEST_F(RelationalExpressionTest, InMap) {
    {
        auto *items = MapItemList::make(&pool);
        (*items)
            .add("key1", ConstantExpression::make(&pool, 12345))
            .add("key2", ConstantExpression::make(&pool, 12345))
            .add("key3", ConstantExpression::make(&pool, "Hello"))
            .add("key4", ConstantExpression::make(&pool, true));
        auto mapExpr = MapExpression::make(&pool, items);
        auto expr =
           RelationalExpression::makeIn(&pool, ConstantExpression::make(&pool, "key1"), mapExpr);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value);
    }
    {
        auto *items = MapItemList::make(&pool);
        (*items)
            .add("key1", ConstantExpression::make(&pool, 12345))
            .add("key2", ConstantExpression::make(&pool, 12345))
            .add("key3", ConstantExpression::make(&pool, "Hello"))
            .add("key4", ConstantExpression::make(&pool, true));
        auto mapExpr = MapExpression::make(&pool, items);
        auto expr =
           RelationalExpression::makeIn(&pool, ConstantExpression::make(&pool, "key5"), mapExpr);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value);
    }
    {
        auto *items = MapItemList::make(&pool);
        (*items)
            .add("key1", ConstantExpression::make(&pool, 12345))
            .add("key2", ConstantExpression::make(&pool, 12345))
            .add("key3", ConstantExpression::make(&pool, "Hello"))
            .add("key4", ConstantExpression::make(&pool, true));
        auto mapExpr = MapExpression::make(&pool, items);
        auto expr =
           RelationalExpression::makeIn(&pool, ConstantExpression::make(&pool, 12345), mapExpr);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value);
    }
}

TEST_F(RelationalExpressionTest, NotInList) {
    {
        auto *elist = ExpressionList::make(&pool);
        (*elist)
            .add(ConstantExpression::make(&pool, 12345))
            .add(ConstantExpression::make(&pool, "Hello"))
            .add(ConstantExpression::make(&pool, true));
        auto listExpr = ListExpression::make(&pool, elist);
        auto expr = RelationalExpression::makeNotIn(
            &pool, ConstantExpression::make(&pool, 12345), listExpr);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value);
    }
    {
        auto *elist = ExpressionList::make(&pool);
        (*elist)
            .add(ConstantExpression::make(&pool, 12345))
            .add(ConstantExpression::make(&pool, "Hello"))
            .add(ConstantExpression::make(&pool, true));
        auto listExpr = ListExpression::make(&pool, elist);
        auto expr = RelationalExpression::makeNotIn(
            &pool, ConstantExpression::make(&pool, false), listExpr);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value);
    }
}

TEST_F(RelationalExpressionTest, NotInSet) {
    {
        auto *elist = ExpressionList::make(&pool);
        (*elist)
            .add(ConstantExpression::make(&pool, 12345))
            .add(ConstantExpression::make(&pool, "Hello"))
            .add(ConstantExpression::make(&pool, true));
        auto setExpr = SetExpression::make(&pool, elist);
        auto expr = RelationalExpression::makeNotIn(
            &pool, ConstantExpression::make(&pool, 12345), setExpr);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value);
    }
    {
        auto *elist = ExpressionList::make(&pool);
        (*elist)
            .add(ConstantExpression::make(&pool, 12345))
            .add(ConstantExpression::make(&pool, "Hello"))
            .add(ConstantExpression::make(&pool, true));
        auto setExpr = ListExpression::make(&pool, elist);
        auto expr = RelationalExpression::makeNotIn(
            &pool, ConstantExpression::make(&pool, false), setExpr);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value);
    }
}

TEST_F(RelationalExpressionTest, NotInMap) {
    {
        auto *items = MapItemList::make(&pool);
        (*items)
            .add("key1", ConstantExpression::make(&pool, 12345))
            .add("key2", ConstantExpression::make(&pool, 12345))
            .add("key3", ConstantExpression::make(&pool, "Hello"))
            .add("key4", ConstantExpression::make(&pool, true));
        auto mapExpr = MapExpression::make(&pool, items);
        auto expr = RelationalExpression::makeNotIn(
            &pool, ConstantExpression::make(&pool, "key1"), mapExpr);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value);
    }
    {
        auto *items = MapItemList::make(&pool);
        (*items)
            .add("key1", ConstantExpression::make(&pool, 12345))
            .add("key2", ConstantExpression::make(&pool, 12345))
            .add("key3", ConstantExpression::make(&pool, "Hello"))
            .add("key4", ConstantExpression::make(&pool, true));
        auto mapExpr = MapExpression::make(&pool, items);
        auto expr = RelationalExpression::makeNotIn(
            &pool, ConstantExpression::make(&pool, "key5"), mapExpr);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value);
    }
    {
        auto *items = MapItemList::make(&pool);
        (*items)
            .add("key1", ConstantExpression::make(&pool, 12345))
            .add("key2", ConstantExpression::make(&pool, 12345))
            .add("key3", ConstantExpression::make(&pool, "Hello"))
            .add("key4", ConstantExpression::make(&pool, true));
        auto mapExpr = MapExpression::make(&pool, items);
        auto expr = RelationalExpression::makeNotIn(
            &pool, ConstantExpression::make(&pool, 12345), mapExpr);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value);
    }
}

TEST_F(RelationalExpressionTest, RelationRegexMatch) {
    {
        auto expr = RelationalExpression::makeREG(
            &pool,
            ConstantExpression::make(&pool, "abcd\xA3g1234efgh\x49ijkl"),
            ConstantExpression::make(&pool, "\\w{4}\xA3g12\\d*e\\w+\x49\\w+"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr = RelationalExpression::makeREG(&pool,
                                                   ConstantExpression::make(&pool, "Tony Parker"),
                                                   ConstantExpression::make(&pool, "T.*er"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr =
           RelationalExpression::makeREG(&pool,
                                           ConstantExpression::make(&pool, "010-12345"),
                                           ConstantExpression::make(&pool, "\\d{3}\\-\\d{3,8}"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr = RelationalExpression::makeREG(
            &pool,
            ConstantExpression::make(&pool, "test_space_128"),
            ConstantExpression::make(&pool, "[a-zA-Z_][0-9a-zA-Z_]{0,19}"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr = RelationalExpression::makeREG(
            &pool,
            ConstantExpression::make(&pool, "2001-09-01 08:00:00"),
            ConstantExpression::make(&pool, "\\d+\\-0\\d?\\-\\d+\\s\\d+:00:\\d+"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr = RelationalExpression::makeREG(
            &pool,
            ConstantExpression::make(&pool, "jack138tom发."),
            ConstantExpression::make(&pool, "j\\w*\\d+\\w+\u53d1\\."));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr = RelationalExpression::makeREG(
            &pool,
            ConstantExpression::make(&pool, "jack138tom\u53d1.34数数数"),
            ConstantExpression::make(&pool, "j\\w*\\d+\\w+发\\.34[\u4e00-\u9fa5]+"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr = RelationalExpression::makeREG(&pool,
                                                   ConstantExpression::make(&pool, "a good person"),
                                                   ConstantExpression::make(&pool, "a\\s\\w+"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto expr =
           RelationalExpression::makeREG(&pool,
                                           ConstantExpression::make(&pool, "Tony Parker"),
                                           ConstantExpression::make(&pool, "T\\w+\\s?\\P\\d+"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto expr =
           RelationalExpression::makeREG(&pool,
                                           ConstantExpression::make(&pool, "010-12345"),
                                           ConstantExpression::make(&pool, "\\d?\\-\\d{3,8}"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto expr = RelationalExpression::makeREG(
            &pool,
            ConstantExpression::make(&pool, "test_space_128牛"),
            ConstantExpression::make(&pool, "[a-zA-Z_][0-9a-zA-Z_]{0,19}"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto expr =
           RelationalExpression::makeREG(&pool,
                                           ConstantExpression::make(&pool, "2001-09-01 08:00:00"),
                                           ConstantExpression::make(&pool, "\\d+\\s\\d+:00:\\d+"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto expr =
           RelationalExpression::makeREG(&pool,
                                           ConstantExpression::make(&pool, Value::kNullBadData),
                                           ConstantExpression::make(&pool, Value::kNullBadType));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), true);
    }
    {
        auto expr =
           RelationalExpression::makeREG(&pool,
                                           ConstantExpression::make(&pool, Value::kNullValue),
                                           ConstantExpression::make(&pool, 3));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), true);
    }
    {
        auto expr = RelationalExpression::makeREG(
            &pool, ConstantExpression::make(&pool, 3), ConstantExpression::make(&pool, true));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), true);
    }
    {
        auto expr =
           RelationalExpression::makeREG(&pool,
                                           ConstantExpression::make(&pool, "abc"),
                                           ConstantExpression::make(&pool, Value::kNullValue));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
}

TEST_F(RelationalExpressionTest, RelationContains) {
    {
        // "abc" contains "a"
        auto expr = RelationalExpression::makeContains(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "a"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" contains "bc"
        auto expr = RelationalExpression::makeContains(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "bc"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" contains "d"
        auto expr = RelationalExpression::makeContains(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "d"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" contains 1
        auto expr = RelationalExpression::makeContains(
            &pool, ConstantExpression::make(&pool, "abc1"), ConstantExpression::make(&pool, 1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        // 1234 contains 1
        auto expr = RelationalExpression::makeContains(
            &pool, ConstantExpression::make(&pool, 1234), ConstantExpression::make(&pool, 1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
}

TEST_F(RelationalExpressionTest, RelationStartsWith) {
    {
        // "abc" starts with "a"
        auto expr = RelationalExpression::makeStartsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "a"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" starts with "ab"
        auto expr = RelationalExpression::makeStartsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "ab"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "1234"" starts with "12"
        auto expr = RelationalExpression::makeStartsWith(
            &pool, ConstantExpression::make(&pool, "1234"), ConstantExpression::make(&pool, "12"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "1234"" starts with "34"
        auto expr = RelationalExpression::makeStartsWith(
            &pool, ConstantExpression::make(&pool, "1234"), ConstantExpression::make(&pool, "34"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" starts with "bc"
        auto expr = RelationalExpression::makeStartsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "bc"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" starts with "ac"
        auto expr = RelationalExpression::makeStartsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "ac"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" starts with "AB"
        auto expr = RelationalExpression::makeStartsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "AB"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" starts with 1
        auto expr = RelationalExpression::makeStartsWith(
            &pool, ConstantExpression::make(&pool, "abc1"), ConstantExpression::make(&pool, 1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        // 1234 starts with 1
        auto expr = RelationalExpression::makeStartsWith(
            &pool, ConstantExpression::make(&pool, 1234), ConstantExpression::make(&pool, 1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        auto expr = RelationalExpression::makeStartsWith(
            &pool,
            ConstantExpression::make(&pool, 1234),
            ConstantExpression::make(&pool, Value::kNullBadData));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), true);
    }
    {
        auto expr = RelationalExpression::makeStartsWith(
            &pool, ConstantExpression::make(&pool, 1234), ConstantExpression::make(&pool, "1234"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), true);
    }
    {
        auto expr = RelationalExpression::makeStartsWith(
            &pool,
            ConstantExpression::make(&pool, Value::kNullValue),
            ConstantExpression::make(&pool, "NULL"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
    {
        auto expr = RelationalExpression::makeStartsWith(
            &pool,
            ConstantExpression::make(&pool, "Null"),
            ConstantExpression::make(&pool, Value::kNullValue));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
}

TEST_F(RelationalExpressionTest, RelationNotStartsWith) {
    {
        // "abc" not starts with "a"
        auto expr = RelationalExpression::makeNotStartsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "a"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" not starts with "ab"
        auto expr = RelationalExpression::makeNotStartsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "ab"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "1234"" not starts with "12"
        auto expr = RelationalExpression::makeNotStartsWith(
            &pool, ConstantExpression::make(&pool, "1234"), ConstantExpression::make(&pool, "12"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "1234"" not starts with "34"
        auto expr = RelationalExpression::makeNotStartsWith(
            &pool, ConstantExpression::make(&pool, "1234"), ConstantExpression::make(&pool, "34"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" not starts with "bc"
        auto expr = RelationalExpression::makeNotStartsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "bc"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" not starts with "ac"
        auto expr = RelationalExpression::makeNotStartsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "ac"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" not starts with "AB"
        auto expr = RelationalExpression::makeNotStartsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "AB"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" not starts with 1
        auto expr = RelationalExpression::makeNotStartsWith(
            &pool, ConstantExpression::make(&pool, "abc1"), ConstantExpression::make(&pool, 1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        // 1234 not starts with 1
        auto expr = RelationalExpression::makeNotStartsWith(
            &pool, ConstantExpression::make(&pool, 1234), ConstantExpression::make(&pool, 1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        auto expr = RelationalExpression::makeNotStartsWith(
            &pool,
            ConstantExpression::make(&pool, 1234),
            ConstantExpression::make(&pool, Value::kNullBadData));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), true);
    }
    {
        auto expr = RelationalExpression::makeNotStartsWith(
            &pool, ConstantExpression::make(&pool, 1234), ConstantExpression::make(&pool, "1234"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), true);
    }
    {
        auto expr = RelationalExpression::makeNotStartsWith(
            &pool,
            ConstantExpression::make(&pool, Value::kNullValue),
            ConstantExpression::make(&pool, "NULL"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
    {
        auto expr = RelationalExpression::makeNotStartsWith(
            &pool,
            ConstantExpression::make(&pool, "Null"),
            ConstantExpression::make(&pool, Value::kNullValue));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
}

TEST_F(RelationalExpressionTest, RelationEndsWith) {
    {
        // "abc" ends with "a"
        auto expr = RelationalExpression::makeEndsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "a"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" ends with "ab"
        auto expr = RelationalExpression::makeEndsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "ab"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "1234"" ends with "12"
        auto expr = RelationalExpression::makeEndsWith(
            &pool, ConstantExpression::make(&pool, "1234"), ConstantExpression::make(&pool, "12"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "1234"" ends with "34"
        auto expr = RelationalExpression::makeEndsWith(
            &pool, ConstantExpression::make(&pool, "1234"), ConstantExpression::make(&pool, "34"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" ends with "bc"
        auto expr = RelationalExpression::makeEndsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "bc"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" ends with "ac"
        auto expr = RelationalExpression::makeEndsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "ac"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" ends with "AB"
        auto expr = RelationalExpression::makeEndsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "AB"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" ends with "BC"
        auto expr = RelationalExpression::makeEndsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "BC"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" ends with 1
        auto expr = RelationalExpression::makeEndsWith(
            &pool, ConstantExpression::make(&pool, "abc1"), ConstantExpression::make(&pool, 1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        // 1234 ends with 1
        auto expr = RelationalExpression::makeEndsWith(
            &pool, ConstantExpression::make(&pool, 1234), ConstantExpression::make(&pool, 1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        // "steve jobs" ends with "jobs"
        auto expr =
           RelationalExpression::makeEndsWith(&pool,
                                                ConstantExpression::make(&pool, "steve jobs"),
                                                ConstantExpression::make(&pool, "jobs"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr = RelationalExpression::makeEndsWith(
            &pool,
            ConstantExpression::make(&pool, 1234),
            ConstantExpression::make(&pool, Value::kNullBadData));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), true);
    }
    {
        auto expr = RelationalExpression::makeEndsWith(
            &pool, ConstantExpression::make(&pool, 1234), ConstantExpression::make(&pool, "1234"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), true);
    }
    {
        auto expr =
           RelationalExpression::makeEndsWith(&pool,
                                                ConstantExpression::make(&pool, Value::kNullValue),
                                                ConstantExpression::make(&pool, "NULL"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
    {
        auto expr =
           RelationalExpression::makeEndsWith(&pool,
                                                ConstantExpression::make(&pool, "Null"),
                                                ConstantExpression::make(&pool, Value::kNullValue));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
}

TEST_F(RelationalExpressionTest, RelationNotEndsWith) {
    {
        // "abc" not ends with "a"
        auto expr = RelationalExpression::makeNotEndsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "a"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" not ends with "ab"
        auto expr = RelationalExpression::makeNotEndsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "ab"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "1234"" not ends with "12"
        auto expr = RelationalExpression::makeNotEndsWith(
            &pool, ConstantExpression::make(&pool, "1234"), ConstantExpression::make(&pool, "12"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "1234"" not ends with "34"
        auto expr = RelationalExpression::makeNotEndsWith(
            &pool, ConstantExpression::make(&pool, "1234"), ConstantExpression::make(&pool, "34"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" not ends with "bc"
        auto expr = RelationalExpression::makeNotEndsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "bc"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" not ends with "ac"
        auto expr = RelationalExpression::makeNotEndsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "ac"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" not ends with "AB"
        auto expr = RelationalExpression::makeNotEndsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "AB"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" not ends with "BC"
        auto expr = RelationalExpression::makeNotEndsWith(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "BC"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" not ends with 1
        auto expr = RelationalExpression::makeNotEndsWith(
            &pool, ConstantExpression::make(&pool, "abc1"), ConstantExpression::make(&pool, 1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        // 1234 not ends with 1
        auto expr = RelationalExpression::makeNotEndsWith(
            &pool, ConstantExpression::make(&pool, 1234), ConstantExpression::make(&pool, 1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        auto expr = RelationalExpression::makeNotEndsWith(
            &pool,
            ConstantExpression::make(&pool, 1234),
            ConstantExpression::make(&pool, Value::kNullBadData));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), true);
    }
    {
        auto expr = RelationalExpression::makeNotEndsWith(
            &pool, ConstantExpression::make(&pool, 1234), ConstantExpression::make(&pool, "1234"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), true);
    }
    {
        auto expr = RelationalExpression::makeNotEndsWith(
            &pool,
            ConstantExpression::make(&pool, Value::kNullValue),
            ConstantExpression::make(&pool, "NULL"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
    {
        auto expr = RelationalExpression::makeNotEndsWith(
            &pool,
            ConstantExpression::make(&pool, "Null"),
            ConstantExpression::make(&pool, Value::kNullValue));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
}

TEST_F(RelationalExpressionTest, ContainsToString) {
    {
        // "abc" contains "a"
        auto expr = RelationalExpression::makeContains(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "a"));
        ASSERT_EQ("(\"abc\" CONTAINS \"a\")", expr->toString());
    }
    {
        auto expr = RelationalExpression::makeContains(
            &pool,
            ConstantExpression::make(&pool, 1234),
            ConstantExpression::make(&pool, Value::kNullBadData));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), true);
    }
    {
        auto expr = RelationalExpression::makeContains(
            &pool, ConstantExpression::make(&pool, 1234), ConstantExpression::make(&pool, "1234"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), true);
    }
    {
        auto expr =
           RelationalExpression::makeContains(&pool,
                                                ConstantExpression::make(&pool, Value::kNullValue),
                                                ConstantExpression::make(&pool, "NULL"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
    {
        auto expr =
           RelationalExpression::makeContains(&pool,
                                                ConstantExpression::make(&pool, "Null"),
                                                ConstantExpression::make(&pool, Value::kNullValue));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
}

TEST_F(RelationalExpressionTest, NotContainsToString) {
    {
        // "abc" not contains "a"
        auto expr = RelationalExpression::makeNotContains(
            &pool, ConstantExpression::make(&pool, "abc"), ConstantExpression::make(&pool, "a"));
        ASSERT_EQ("(\"abc\" NOT CONTAINS \"a\")", expr->toString());
    }
    {
        auto expr = RelationalExpression::makeNotContains(
            &pool,
            ConstantExpression::make(&pool, 1234),
            ConstantExpression::make(&pool, Value::kNullBadData));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), true);
    }
    {
        auto expr = RelationalExpression::makeNotContains(
            &pool, ConstantExpression::make(&pool, 1234), ConstantExpression::make(&pool, "1234"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), true);
    }
    {
        auto expr = RelationalExpression::makeNotContains(
            &pool,
            ConstantExpression::make(&pool, Value::kNullValue),
            ConstantExpression::make(&pool, "NULL"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
    {
        auto expr = RelationalExpression::makeNotContains(
            &pool,
            ConstantExpression::make(&pool, "Null"),
            ConstantExpression::make(&pool, Value::kNullValue));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval.isBadNull(), false);
    }
}
}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
