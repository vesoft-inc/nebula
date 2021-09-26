/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/expression/ArithmeticExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "graph/util/ExpressionUtils.h"
#include "parser/GQLParser.h"

namespace nebula {
namespace graph {

using graph::QueryContext;

class ExpressionUtilsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    qctx_ = std::make_unique<QueryContext>();
    pool = qctx_->objPool();
  }

  void TearDown() override { qctx_.reset(); }

  Expression *parse(const std::string &expr) {
    std::string query = "LOOKUP on t1 WHERE " + expr;
    GQLParser parser(qctx_.get());
    auto result = parser.parse(std::move(query));
    CHECK(result.ok()) << result.status();
    auto stmt = std::move(result).value();
    auto *seq = static_cast<SequentialSentences *>(stmt.get());
    auto *lookup = static_cast<LookupSentence *>(seq->sentences()[0]);
    return lookup->whereClause()->filter()->clone();
  }

 protected:
  std::unique_ptr<QueryContext> qctx_;
  ObjectPool *pool;
};

TEST_F(ExpressionUtilsTest, CheckComponent) {
  {
    // single node
    const auto root = ConstantExpression::make(pool);

    ASSERT_TRUE(ExpressionUtils::isKindOf(root, {Expression::Kind::kConstant}));
    ASSERT_TRUE(ExpressionUtils::hasAny(root, {Expression::Kind::kConstant}));

    ASSERT_TRUE(
        ExpressionUtils::isKindOf(root, {Expression::Kind::kConstant, Expression::Kind::kAdd}));
    ASSERT_TRUE(
        ExpressionUtils::hasAny(root, {Expression::Kind::kConstant, Expression::Kind::kAdd}));

    ASSERT_FALSE(ExpressionUtils::isKindOf(root, {Expression::Kind::kAdd}));
    ASSERT_FALSE(ExpressionUtils::hasAny(root, {Expression::Kind::kAdd}));

    ASSERT_FALSE(
        ExpressionUtils::isKindOf(root, {Expression::Kind::kDivision, Expression::Kind::kAdd}));
    ASSERT_FALSE(
        ExpressionUtils::hasAny(root, {Expression::Kind::kDstProperty, Expression::Kind::kAdd}));

    // find
    const Expression *found = ExpressionUtils::findAny(root, {Expression::Kind::kConstant});
    ASSERT_EQ(found, root);

    found = ExpressionUtils::findAny(
        root,
        {Expression::Kind::kConstant, Expression::Kind::kAdd, Expression::Kind::kEdgeProperty});
    ASSERT_EQ(found, root);

    found = ExpressionUtils::findAny(root, {Expression::Kind::kEdgeDst});
    ASSERT_EQ(found, nullptr);

    found = ExpressionUtils::findAny(
        root, {Expression::Kind::kEdgeRank, Expression::Kind::kInputProperty});
    ASSERT_EQ(found, nullptr);

    // find all
    const auto willFoundAll = std::vector<const Expression *>{root};
    std::vector<const Expression *> founds =
        ExpressionUtils::collectAll(root, {Expression::Kind::kConstant});
    ASSERT_EQ(founds, willFoundAll);

    founds = ExpressionUtils::collectAll(
        root, {Expression::Kind::kAdd, Expression::Kind::kConstant, Expression::Kind::kEdgeDst});
    ASSERT_EQ(founds, willFoundAll);

    founds = ExpressionUtils::collectAll(root, {Expression::Kind::kSrcProperty});
    ASSERT_TRUE(founds.empty());

    founds = ExpressionUtils::collectAll(
        root,
        {Expression::Kind::kUnaryNegate, Expression::Kind::kEdgeDst, Expression::Kind::kEdgeDst});
    ASSERT_TRUE(founds.empty());
  }

  {
    // list like
    const auto root = TypeCastingExpression::make(
        pool,
        Value::Type::BOOL,
        TypeCastingExpression::make(
            pool,
            Value::Type::BOOL,
            TypeCastingExpression::make(pool, Value::Type::BOOL, ConstantExpression::make(pool))));

    ASSERT_TRUE(ExpressionUtils::isKindOf(root, {Expression::Kind::kTypeCasting}));
    ASSERT_TRUE(ExpressionUtils::hasAny(root, {Expression::Kind::kConstant}));

    ASSERT_TRUE(
        ExpressionUtils::isKindOf(root, {Expression::Kind::kTypeCasting, Expression::Kind::kAdd}));
    ASSERT_TRUE(
        ExpressionUtils::hasAny(root, {Expression::Kind::kTypeCasting, Expression::Kind::kAdd}));

    ASSERT_FALSE(ExpressionUtils::isKindOf(root, {Expression::Kind::kAdd}));
    ASSERT_FALSE(ExpressionUtils::hasAny(root, {Expression::Kind::kAdd}));

    ASSERT_FALSE(
        ExpressionUtils::isKindOf(root, {Expression::Kind::kDivision, Expression::Kind::kAdd}));
    ASSERT_FALSE(
        ExpressionUtils::hasAny(root, {Expression::Kind::kDstProperty, Expression::Kind::kAdd}));

    // found
    const Expression *found = ExpressionUtils::findAny(root, {Expression::Kind::kTypeCasting});
    ASSERT_EQ(found, root);

    found = ExpressionUtils::findAny(root,
                                     {Expression::Kind::kFunctionCall,
                                      Expression::Kind::kTypeCasting,
                                      Expression::Kind::kLogicalAnd});
    ASSERT_EQ(found, root);

    found = ExpressionUtils::findAny(root, {Expression::Kind::kDivision});
    ASSERT_EQ(found, nullptr);

    found = ExpressionUtils::findAny(
        root,
        {Expression::Kind::kLogicalXor, Expression::Kind::kRelGE, Expression::Kind::kEdgeProperty});
    ASSERT_EQ(found, nullptr);

    // found all
    std::vector<const Expression *> founds =
        ExpressionUtils::collectAll(root, {Expression::Kind::kConstant});
    ASSERT_EQ(founds.size(), 1);

    founds = ExpressionUtils::collectAll(
        root, {Expression::Kind::kFunctionCall, Expression::Kind::kTypeCasting});
    ASSERT_EQ(founds.size(), 3);

    founds = ExpressionUtils::collectAll(root, {Expression::Kind::kAdd});
    ASSERT_TRUE(founds.empty());

    founds = ExpressionUtils::collectAll(
        root, {Expression::Kind::kRelLE, Expression::Kind::kDstProperty});
    ASSERT_TRUE(founds.empty());
  }

  {
    // tree like
    const auto root = ArithmeticExpression::makeAdd(
        pool,
        ArithmeticExpression::makeDivision(
            pool,
            ConstantExpression::make(pool, 3),
            ArithmeticExpression::makeMinus(
                pool, ConstantExpression::make(pool, 4), ConstantExpression::make(pool, 2))),
        ArithmeticExpression::makeMod(
            pool,
            ArithmeticExpression::makeMultiply(
                pool, ConstantExpression::make(pool, 3), ConstantExpression::make(pool, 10)),
            ConstantExpression::make(pool, 2)));

    ASSERT_TRUE(ExpressionUtils::isKindOf(root, {Expression::Kind::kAdd}));
    ASSERT_TRUE(ExpressionUtils::hasAny(root, {Expression::Kind::kMinus}));

    ASSERT_TRUE(
        ExpressionUtils::isKindOf(root, {Expression::Kind::kTypeCasting, Expression::Kind::kAdd}));
    ASSERT_TRUE(ExpressionUtils::hasAny(
        root, {Expression::Kind::kLabelAttribute, Expression::Kind::kDivision}));

    ASSERT_FALSE(ExpressionUtils::isKindOf(root, {Expression::Kind::kConstant}));
    ASSERT_FALSE(ExpressionUtils::hasAny(root, {Expression::Kind::kFunctionCall}));

    ASSERT_FALSE(ExpressionUtils::isKindOf(
        root, {Expression::Kind::kDivision, Expression::Kind::kEdgeProperty}));
    ASSERT_FALSE(ExpressionUtils::hasAny(
        root, {Expression::Kind::kDstProperty, Expression::Kind::kLogicalAnd}));

    // found
    const Expression *found = ExpressionUtils::findAny(root, {Expression::Kind::kAdd});
    ASSERT_EQ(found, root);

    found = ExpressionUtils::findAny(
        root,
        {Expression::Kind::kFunctionCall, Expression::Kind::kRelLE, Expression::Kind::kMultiply});
    ASSERT_NE(found, nullptr);

    found = ExpressionUtils::findAny(root, {Expression::Kind::kInputProperty});
    ASSERT_EQ(found, nullptr);

    found = ExpressionUtils::findAny(
        root,
        {Expression::Kind::kLogicalXor, Expression::Kind::kEdgeRank, Expression::Kind::kUnaryNot});
    ASSERT_EQ(found, nullptr);

    // found all
    std::vector<const Expression *> founds =
        ExpressionUtils::collectAll(root, {Expression::Kind::kConstant});
    ASSERT_EQ(founds.size(), 6);

    founds =
        ExpressionUtils::collectAll(root, {Expression::Kind::kDivision, Expression::Kind::kMinus});
    ASSERT_EQ(founds.size(), 2);

    founds = ExpressionUtils::collectAll(root, {Expression::Kind::kEdgeDst});
    ASSERT_TRUE(founds.empty());

    founds = ExpressionUtils::collectAll(
        root, {Expression::Kind::kLogicalAnd, Expression::Kind::kUnaryNegate});
    ASSERT_TRUE(founds.empty());
  }
}

TEST_F(ExpressionUtilsTest, PullAnds) {
  // true AND false
  {
    auto *first = ConstantExpression::make(pool, true);
    auto *second = ConstantExpression::make(pool, false);
    auto expr = LogicalExpression::makeAnd(pool, first, second);
    auto expected = LogicalExpression::makeAnd(pool, first->clone(), second->clone());
    ExpressionUtils::pullAnds(expr);
    ASSERT_EQ(*expected, *expr);
  }
  // true AND false AND true
  {
    auto *first = ConstantExpression::make(pool, true);
    auto *second = ConstantExpression::make(pool, false);
    auto *third = ConstantExpression::make(pool, true);
    auto expr =
        LogicalExpression::makeAnd(pool, LogicalExpression::makeAnd(pool, first, second), third);
    auto expected = LogicalExpression::makeAnd(pool);
    expected->addOperand(first->clone());
    expected->addOperand(second->clone());
    expected->addOperand(third->clone());
    ExpressionUtils::pullAnds(expr);
    ASSERT_EQ(*expected, *expr);
  }
  // true AND (false AND true)
  {
    auto *first = ConstantExpression::make(pool, true);
    auto *second = ConstantExpression::make(pool, false);
    auto *third = ConstantExpression::make(pool, true);
    auto expr =
        LogicalExpression::makeAnd(pool, first, LogicalExpression::makeAnd(pool, second, third));
    auto expected = LogicalExpression::makeAnd(pool);
    expected->addOperand(first->clone());
    expected->addOperand(second->clone());
    expected->addOperand(third->clone());
    ExpressionUtils::pullAnds(expr);
    ASSERT_EQ(*expected, *expr);
  }
  // (true OR false) AND (true OR false)
  {
    auto *first = LogicalExpression::makeOr(
        pool, ConstantExpression::make(pool, true), ConstantExpression::make(pool, false));
    auto *second = LogicalExpression::makeOr(
        pool, ConstantExpression::make(pool, true), ConstantExpression::make(pool, false));
    auto expr = LogicalExpression::makeAnd(pool, first, second);
    auto expected = LogicalExpression::makeAnd(pool);
    expected->addOperand(first->clone());
    expected->addOperand(second->clone());
    ExpressionUtils::pullAnds(expr);
    ASSERT_EQ(*expected, *expr);
  }
  // true AND ((false AND true) OR false) AND true
  {
    auto *first = ConstantExpression::make(pool, true);
    auto *second = LogicalExpression::makeOr(
        pool,
        LogicalExpression::makeAnd(
            pool, ConstantExpression::make(pool, false), ConstantExpression::make(pool, true)),
        ConstantExpression::make(pool, false));
    auto *third = ConstantExpression::make(pool, true);
    auto expr =
        LogicalExpression::makeAnd(pool, LogicalExpression::makeAnd(pool, first, second), third);
    auto expected = LogicalExpression::makeAnd(pool);
    expected->addOperand(first->clone());
    expected->addOperand(second->clone());
    expected->addOperand(third->clone());
    ExpressionUtils::pullAnds(expr);
    ASSERT_EQ(*expected, *expr);
  }
}

TEST_F(ExpressionUtilsTest, PullOrs) {
  // true OR false
  {
    auto *first = ConstantExpression::make(pool, true);
    auto *second = ConstantExpression::make(pool, false);
    auto expr = LogicalExpression::makeOr(pool, first, second);
    auto expected = LogicalExpression::makeOr(pool, first->clone(), second->clone());
    ExpressionUtils::pullOrs(expr);
    ASSERT_EQ(*expected, *expr);
  }
  // true OR false OR true
  {
    auto *first = ConstantExpression::make(pool, true);
    auto *second = ConstantExpression::make(pool, false);
    auto *third = ConstantExpression::make(pool, true);
    auto expr =
        LogicalExpression::makeOr(pool, LogicalExpression::makeOr(pool, first, second), third);
    auto expected = LogicalExpression::makeOr(pool);
    expected->addOperand(first->clone());
    expected->addOperand(second->clone());
    expected->addOperand(third->clone());
    ExpressionUtils::pullOrs(expr);
    ASSERT_EQ(*expected, *expr);
  }
  // true OR (false OR true)
  {
    auto *first = ConstantExpression::make(pool, true);
    auto *second = ConstantExpression::make(pool, false);
    auto *third = ConstantExpression::make(pool, true);
    auto expr =
        LogicalExpression::makeOr(pool, first, LogicalExpression::makeOr(pool, second, third));
    auto expected = LogicalExpression::makeOr(pool);
    expected->addOperand(first->clone());
    expected->addOperand(second->clone());
    expected->addOperand(third->clone());
    ExpressionUtils::pullOrs(expr);
    ASSERT_EQ(*expected, *expr);
  }
  // (true AND false) OR (true AND false)
  {
    auto *first = LogicalExpression::makeAnd(
        pool, ConstantExpression::make(pool, true), ConstantExpression::make(pool, false));
    auto *second = LogicalExpression::makeAnd(
        pool, ConstantExpression::make(pool, true), ConstantExpression::make(pool, false));
    auto expr = LogicalExpression::makeOr(pool, first, second);
    auto expected = LogicalExpression::makeOr(pool, first->clone(), second->clone());
    ExpressionUtils::pullOrs(expr);
    ASSERT_EQ(*expected, *expr);
  }
  // true OR ((false OR true) AND false) OR true
  {
    auto *first = ConstantExpression::make(pool, true);
    auto *second = LogicalExpression::makeAnd(
        pool,
        LogicalExpression::makeOr(
            pool, ConstantExpression::make(pool, false), ConstantExpression::make(pool, true)),
        ConstantExpression::make(pool, false));
    auto *third = ConstantExpression::make(pool, true);
    auto expr =
        LogicalExpression::makeOr(pool, LogicalExpression::makeOr(pool, first, second), third);
    auto expected = LogicalExpression::makeOr(pool);
    expected->addOperand(first->clone());
    expected->addOperand(second->clone());
    expected->addOperand(third->clone());
    ExpressionUtils::pullOrs(expr);
    ASSERT_EQ(*expected, *expr);
  }
}

TEST_F(ExpressionUtilsTest, pushOrs) {
  std::vector<Expression *> rels;
  for (int16_t i = 0; i < 5; i++) {
    auto r = RelationalExpression::makeEQ(
        pool,
        LabelAttributeExpression::make(
            pool,
            LabelExpression::make(pool, folly::stringPrintf("tag%d", i)),
            ConstantExpression::make(pool, folly::stringPrintf("col%d", i))),
        ConstantExpression::make(pool, Value(folly::stringPrintf("val%d", i))));
    rels.emplace_back(std::move(r));
  }
  auto t = ExpressionUtils::pushOrs(pool, rels);
  auto expected = std::string(
      "(((((tag0.col0==\"val0\") OR "
      "(tag1.col1==\"val1\")) OR "
      "(tag2.col2==\"val2\")) OR "
      "(tag3.col3==\"val3\")) OR "
      "(tag4.col4==\"val4\"))");
  ASSERT_EQ(expected, t->toString());
}

TEST_F(ExpressionUtilsTest, pushAnds) {
  std::vector<Expression *> rels;
  for (int16_t i = 0; i < 5; i++) {
    auto r = RelationalExpression::makeEQ(
        pool,
        LabelAttributeExpression::make(
            pool,
            LabelExpression::make(pool, folly::stringPrintf("tag%d", i)),
            ConstantExpression::make(pool, folly::stringPrintf("col%d", i))),
        ConstantExpression::make(pool, Value(folly::stringPrintf("val%d", i))));
    rels.emplace_back(std::move(r));
  }
  auto t = ExpressionUtils::pushAnds(pool, rels);
  auto expected = std::string(
      "(((((tag0.col0==\"val0\") AND "
      "(tag1.col1==\"val1\")) AND "
      "(tag2.col2==\"val2\")) AND "
      "(tag3.col3==\"val3\")) AND "
      "(tag4.col4==\"val4\"))");
  ASSERT_EQ(expected, t->toString());
}

TEST_F(ExpressionUtilsTest, flattenInnerLogicalExpr) {
  // true AND false AND true
  {
    auto *first = ConstantExpression::make(pool, true);
    auto *second = ConstantExpression::make(pool, false);
    auto *third = ConstantExpression::make(pool, true);
    auto expr =
        LogicalExpression::makeAnd(pool, LogicalExpression::makeAnd(pool, first, second), third);
    auto expected = LogicalExpression::makeAnd(pool);
    expected->addOperand(first->clone());
    expected->addOperand(second->clone());
    expected->addOperand(third->clone());
    auto newExpr = ExpressionUtils::flattenInnerLogicalExpr(expr);
    ASSERT_EQ(*expected, *newExpr);
  }
  // true OR false OR true
  {
    auto *first = ConstantExpression::make(pool, true);
    auto *second = ConstantExpression::make(pool, false);
    auto *third = ConstantExpression::make(pool, true);
    auto expr =
        LogicalExpression::makeOr(pool, LogicalExpression::makeOr(pool, first, second), third);
    auto expected = LogicalExpression::makeOr(pool);
    expected->addOperand(first->clone());
    expected->addOperand(second->clone());
    expected->addOperand(third->clone());
    auto newExpr = ExpressionUtils::flattenInnerLogicalExpr(expr);
    ASSERT_EQ(*expected, *newExpr);
  }
  // (true OR false OR true)==(true AND false AND true)
  {
    auto *or1 = ConstantExpression::make(pool, true);
    auto *or2 = ConstantExpression::make(pool, false);
    auto *or3 = ConstantExpression::make(pool, true);
    auto *logicOrExpr =
        LogicalExpression::makeOr(pool, LogicalExpression::makeOr(pool, or1, or2), or3);
    auto *and1 = ConstantExpression::make(pool, false);
    auto *and2 = ConstantExpression::make(pool, false);
    auto *and3 = ConstantExpression::make(pool, true);
    auto *logicAndExpr =
        LogicalExpression::makeAnd(pool, LogicalExpression::makeAnd(pool, and1, and2), and3);
    auto expr = RelationalExpression ::makeEQ(pool, logicOrExpr, logicAndExpr);

    auto *logicOrFlatten = LogicalExpression::makeOr(pool);
    logicOrFlatten->addOperand(or1->clone());
    logicOrFlatten->addOperand(or2->clone());
    logicOrFlatten->addOperand(or3->clone());
    auto *logicAndFlatten = LogicalExpression::makeAnd(pool);
    logicAndFlatten->addOperand(and1->clone());
    logicAndFlatten->addOperand(and2->clone());
    logicAndFlatten->addOperand(and3->clone());
    auto expected = RelationalExpression ::makeEQ(pool, logicOrFlatten, logicAndFlatten);

    auto newExpr = ExpressionUtils::flattenInnerLogicalExpr(expr);
    ASSERT_EQ(*expected, *newExpr);
  }
  // (true OR false OR true) AND (true AND false AND true)
  {
    auto *or1 = ConstantExpression::make(pool, true);
    auto *or2 = ConstantExpression::make(pool, false);
    auto *or3 = ConstantExpression::make(pool, true);
    auto *logicOrExpr =
        LogicalExpression::makeOr(pool, LogicalExpression::makeOr(pool, or1, or2), or3);
    auto *and1 = ConstantExpression::make(pool, false);
    auto *and2 = ConstantExpression::make(pool, false);
    auto *and3 = ConstantExpression::make(pool, true);
    auto *logicAndExpr =
        LogicalExpression::makeAnd(pool, LogicalExpression::makeAnd(pool, and1, and2), and3);
    auto expr = LogicalExpression::makeAnd(pool, logicOrExpr, logicAndExpr);

    auto *logicOrFlatten = LogicalExpression::makeOr(pool);
    logicOrFlatten->addOperand(or1->clone());
    logicOrFlatten->addOperand(or2->clone());
    logicOrFlatten->addOperand(or3->clone());
    auto expected = LogicalExpression::makeAnd(pool);
    expected->addOperand(logicOrFlatten);
    expected->addOperand(and1->clone());
    expected->addOperand(and2->clone());
    expected->addOperand(and3->clone());

    auto newExpr = ExpressionUtils::flattenInnerLogicalExpr(expr);
    ASSERT_EQ(*expected, *newExpr);
  }
}

TEST_F(ExpressionUtilsTest, rewriteInExpr) {
  auto elist1 = ExpressionList::make(pool);
  (*elist1).add(ConstantExpression::make(pool, 10)).add(ConstantExpression::make(pool, 20));
  auto listExpr1 = ListExpression::make(pool, elist1);

  auto elist2 = ExpressionList::make(pool);
  (*elist2).add(ConstantExpression::make(pool, "a")).add(ConstantExpression::make(pool, "b"));
  auto listExpr2 = ListExpression::make(pool, elist2);

  auto elist3 = ExpressionList::make(pool);
  (*elist3).add(ConstantExpression::make(pool, 100));
  auto listExpr3 = ListExpression::make(pool, elist3);

  // a IN [b,c]  ->  a==b OR a==c
  {
    auto inExpr1 =
        RelationalExpression::makeIn(pool, ConstantExpression::make(pool, 10), listExpr1);
    auto orExpr1 = ExpressionUtils::rewriteInExpr(inExpr1);
    auto expected1 = LogicalExpression::makeOr(
        pool,
        RelationalExpression::makeEQ(
            pool, ConstantExpression::make(pool, 10), ConstantExpression::make(pool, 10)),
        RelationalExpression::makeEQ(
            pool, ConstantExpression::make(pool, 10), ConstantExpression::make(pool, 20)));
    ASSERT_EQ(*expected1, *orExpr1);

    auto inExpr2 =
        RelationalExpression::makeIn(pool, ConstantExpression::make(pool, "abc"), listExpr2);
    auto orExpr2 = ExpressionUtils::rewriteInExpr(inExpr2);
    auto expected2 = LogicalExpression::makeOr(
        pool,
        RelationalExpression::makeEQ(
            pool, ConstantExpression::make(pool, "abc"), ConstantExpression::make(pool, "a")),
        RelationalExpression::makeEQ(
            pool, ConstantExpression::make(pool, "abc"), ConstantExpression::make(pool, "b")));
    ASSERT_EQ(*expected2, *orExpr2);
  }

  // a IN [b]  ->  a == b
  {
    auto inExpr = RelationalExpression::makeIn(pool, ConstantExpression::make(pool, 10), listExpr3);
    auto expected = RelationalExpression::makeEQ(
        pool, ConstantExpression::make(pool, 10), ConstantExpression::make(pool, 100));
    ASSERT_EQ(*expected, *ExpressionUtils::rewriteInExpr(inExpr));
  }
}

TEST_F(ExpressionUtilsTest, rewriteLogicalAndToLogicalOr) {
  auto orExpr1 = LogicalExpression::makeOr(
      pool, ConstantExpression::make(pool, 10), ConstantExpression::make(pool, 20));
  auto orExpr2 = LogicalExpression::makeOr(
      pool, ConstantExpression::make(pool, "a"), ConstantExpression::make(pool, "b"));

  // (a OR b) AND (c OR d)  ->  (a AND c) OR (a AND d) OR (b AND c) OR (b AND d)
  {
    auto andExpr = LogicalExpression::makeAnd(pool, orExpr1, orExpr2);
    auto transformedExpr = ExpressionUtils::rewriteLogicalAndToLogicalOr(andExpr);

    std::vector<Expression *> orOperands = {
        LogicalExpression::makeAnd(
            pool, ConstantExpression::make(pool, 10), ConstantExpression::make(pool, "a")),
        LogicalExpression::makeAnd(
            pool, ConstantExpression::make(pool, 10), ConstantExpression::make(pool, "b")),
        LogicalExpression::makeAnd(
            pool, ConstantExpression::make(pool, 20), ConstantExpression::make(pool, "a")),
        LogicalExpression::makeAnd(
            pool, ConstantExpression::make(pool, 20), ConstantExpression::make(pool, "b"))};

    auto expected = LogicalExpression::makeOr(pool);
    expected->setOperands(orOperands);

    ASSERT_EQ(*expected, *transformedExpr);
  }
}

TEST_F(ExpressionUtilsTest, splitFilter) {
  using Kind = Expression::Kind;
  {
    // true AND false AND true
    auto *first = ConstantExpression::make(pool, true);
    auto *second = ConstantExpression::make(pool, false);
    auto *third = ConstantExpression::make(pool, true);
    auto expr =
        LogicalExpression::makeAnd(pool, LogicalExpression::makeAnd(pool, first, second), third);
    auto expected1 = LogicalExpression::makeAnd(pool);
    expected1->addOperand(first->clone());
    expected1->addOperand(third->clone());
    auto picker = [](const Expression *e) {
      if (e->kind() != Kind::kConstant) return false;
      auto &v = static_cast<const ConstantExpression *>(e)->value();
      if (v.type() != Value::Type::BOOL) return false;
      return v.getBool();
    };
    Expression *newExpr1 = nullptr;
    Expression *newExpr2 = nullptr;
    ExpressionUtils::splitFilter(expr, picker, &newExpr1, &newExpr2);
    ASSERT_EQ(*expected1, *newExpr1);
    ASSERT_EQ(*second, *newExpr2);
  }
  {
    // true
    auto expr = ConstantExpression::make(pool, true);
    auto picker = [](const Expression *e) {
      if (e->kind() != Kind::kConstant) return false;
      auto &v = static_cast<const ConstantExpression *>(e)->value();
      if (v.type() != Value::Type::BOOL) return false;
      return v.getBool();
    };
    Expression *newExpr1 = nullptr;
    Expression *newExpr2 = nullptr;
    ExpressionUtils::splitFilter(expr, picker, &newExpr1, &newExpr2);
    ASSERT_EQ(*expr, *newExpr1);
    ASSERT_EQ(nullptr, newExpr2);
  }
}

TEST_F(ExpressionUtilsTest, expandExpression) {
  {
    auto filter = parse("t1.c1 == 1");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected = "(t1.c1==1)";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse("t1.c1 == 1 and t1.c2 == 2");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected = "((t1.c1==1) AND (t1.c2==2))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse("t1.c1 == 1 and t1.c2 == 2 and t1.c3 == 3");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected = "(((t1.c1==1) AND (t1.c2==2)) AND (t1.c3==3))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse("t1.c1 == 1 or t1.c2 == 2");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected = "((t1.c1==1) OR (t1.c2==2))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse("t1.c1 == 1 or t1.c2 == 2 or t1.c3 == 3");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected = "(((t1.c1==1) OR (t1.c2==2)) OR (t1.c3==3))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse("t1.c1 == 1 and t1.c2 == 2 or t1.c1 == 3");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected = "(((t1.c1==1) AND (t1.c2==2)) OR (t1.c1==3))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse("t1.c1 == 1 or t1.c2 == 2 and t1.c1 == 3");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected = "((t1.c1==1) OR ((t1.c2==2) AND (t1.c1==3)))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse("(t1.c1 == 1 or t1.c2 == 2) and t1.c3 == 3");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected = "(((t1.c1==1) AND (t1.c3==3)) OR ((t1.c2==2) AND (t1.c3==3)))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse("(t1.c1 == 1 or t1.c2 == 2) and t1.c3 == 3 or t1.c4 == 4");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected =
        "((((t1.c1==1) AND (t1.c3==3)) OR "
        "((t1.c2==2) AND (t1.c3==3))) OR "
        "(t1.c4==4))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse("(t1.c1 == 1 or t1.c2 == 2) and (t1.c3 == 3 or t1.c4 == 4)");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected =
        "(((((t1.c1==1) AND (t1.c3==3)) OR "
        "((t1.c1==1) AND (t1.c4==4))) OR "
        "((t1.c2==2) AND (t1.c3==3))) OR "
        "((t1.c2==2) AND (t1.c4==4)))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse(
        "(t1.c1 == 1 or t1.c2 == 2 or t1.c3 == 3 or t1.c4 == 4) "
        "and t1.c5 == 5");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected =
        "(((((t1.c1==1) AND (t1.c5==5)) OR "
        "((t1.c2==2) AND (t1.c5==5))) OR "
        "((t1.c3==3) AND (t1.c5==5))) OR "
        "((t1.c4==4) AND (t1.c5==5)))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse("(t1.c1 == 1 or t1.c2 == 2) and t1.c4 == 4 and t1.c5 == 5");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected =
        "((((t1.c1==1) AND (t1.c4==4)) AND (t1.c5==5)) OR "
        "(((t1.c2==2) AND (t1.c4==4)) AND (t1.c5==5)))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse("t1.c1 == 1 and (t1.c2 == 2 or t1.c4 == 4) and t1.c5 == 5");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected =
        "((((t1.c1==1) AND (t1.c2==2)) AND (t1.c5==5)) OR "
        "(((t1.c1==1) AND (t1.c4==4)) AND (t1.c5==5)))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse("t1.c1 == 1 and t1.c2 == 2 and (t1.c4 == 4 or t1.c5 == 5)");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected =
        "((((t1.c1==1) AND (t1.c2==2)) AND (t1.c4==4)) OR "
        "(((t1.c1==1) AND (t1.c2==2)) AND (t1.c5==5)))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse(
        "(t1.c1 == 1 or t1.c2 == 2) and "
        "(t1.c3 == 3 or t1.c4 == 4) and "
        "t1.c5 == 5");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected =
        "((((((t1.c1==1) AND (t1.c3==3)) AND (t1.c5==5)) OR "
        "(((t1.c1==1) AND (t1.c4==4)) AND (t1.c5==5))) OR "
        "(((t1.c2==2) AND (t1.c3==3)) AND (t1.c5==5))) OR "
        "(((t1.c2==2) AND (t1.c4==4)) AND (t1.c5==5)))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse("t1.c4 == 4 or (t1.c1 == 1 and (t1.c2 == 2 or t1.c3 == 3))");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected =
        "((t1.c4==4) OR "
        "(((t1.c1==1) AND (t1.c2==2)) OR "
        "((t1.c1==1) AND (t1.c3==3))))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse(
        "t1.c4 == 4 or "
        "(t1.c1 == 1 and (t1.c2 == 2 or t1.c3 == 3)) or "
        "t1.c5 == 5");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected =
        "(((t1.c4==4) OR "
        "(((t1.c1==1) AND (t1.c2==2)) OR ((t1.c1==1) AND (t1.c3==3)))) OR "
        "(t1.c5==5))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    auto filter = parse("t1.c1 == 1 and (t1.c2 == 2 or t1.c4) and t1.c5 == 5");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected =
        "((((t1.c1==1) AND (t1.c2==2)) AND (t1.c5==5)) OR "
        "(((t1.c1==1) AND t1.c4) AND (t1.c5==5)))";
    ASSERT_EQ(expected, target->toString());
  }
  {
    // Invalid expression for index. don't need to expand.
    auto filter = parse("t1.c1 == 1 and (t1.c2 == 2 or t1.c4) == true and t1.c5 == 5");
    auto target = ExpressionUtils::expandExpr(pool, filter);
    auto expected = "(((t1.c1==1) AND (((t1.c2==2) OR t1.c4)==true)) AND (t1.c5==5))";
    ASSERT_EQ(expected, target->toString());
  }
}
}  // namespace graph
}  // namespace nebula
