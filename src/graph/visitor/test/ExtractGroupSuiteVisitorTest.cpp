/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "graph/context/ast/CypherAstContext.h"
#include "graph/visitor/ExtractGroupSuiteVisitor.h"
#include "graph/visitor/test/VisitorTestBase.h"

namespace nebula {
namespace graph {
class ExtractGroupSuiteVisitorTest : public VisitorTestBase {
 public:
  bool check(GroupSuite g1, GroupSuite g2) {
    if (g1.groupKeys.size() != g2.groupKeys.size() ||
        g1.groupItems.size() != g2.groupItems.size()) {
      std::cout << "g1.groupKeys.size()=" << g1.groupKeys.size() << std::endl;
      std::cout << "g2.groupKeys.size()=" << g2.groupKeys.size() << std::endl;
      std::cout << "g1.groupItems.size()=" << g1.groupItems.size() << std::endl;
      std::cout << "g2.groupItems.size()=" << g2.groupItems.size() << std::endl;
      return false;
    }
    for (unsigned int i = 0; i < g1.groupKeys.size(); i++) {
      if (g1.groupKeys[i]->toString() != g2.groupKeys[i]->toString()) {
        std::cout << "g1.groupKeys[i]=" << g1.groupKeys[i]->toString() << std::endl;
        std::cout << "g2.groupKeys[i]=" << g2.groupKeys[i]->toString() << std::endl;
        return false;
      }
    }
    for (unsigned int i = 0; i < g1.groupItems.size(); i++) {
      if (g1.groupItems[i]->toString() != g2.groupItems[i]->toString()) {
        std::cout << "g1.groupItems[i]=" << g1.groupItems[i]->toString() << std::endl;
        std::cout << "g2.groupItems[i]=" << g2.groupItems[i]->toString() << std::endl;
        return false;
      }
    }
    return true;
  }
};

TEST_F(ExtractGroupSuiteVisitorTest, TestConstantExpression1) {
  // "abc"
  auto* e = constantExpr("abc");
  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestConstantExpression2) {
  // "abc" + 1
  auto* e1 = constantExpr("abc");
  auto* e2 = constantExpr(1);
  auto* e = addExpr(e1, e2);
  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestConstantExpression3) {
  // "abc" + avg(a.edge)
  auto* e1 = constantExpr("abc");
  auto* e2 = aggExpr("avg", laExpr("a", "edge"), false);
  auto* e = addExpr(e1, e2);
  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupItems.push_back(e2);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestUnaryExpression1) {
  // not (1 - a) + count(a)
  auto* e1 = notExpr(minusExpr(constantExpr(1), labelExpr("a")));
  auto* e2 = aggExpr("count", labelExpr("a"), true);
  auto* e = addExpr(e1, e2);

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupKeys.push_back(e1);
  expect.groupItems.push_back(e1);
  expect.groupItems.push_back(e2);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestTypeCastingExpression) {
  // (bool)  (collect(a) / "abc") CASTING
  auto* e1 = aggExpr("collect", labelExpr("a"), false);
  auto* e = castExpr(Type::BOOL, divideExpr(e1, constantExpr("abc")));

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  // expect.groupKeys.push_back(e);
  expect.groupItems.push_back(e1);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestLabelExpression) {
  // aaaaa
  auto* e = labelExpr("aaaaa");

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupKeys.push_back(e);
  expect.groupItems.push_back(e);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestLabelAttributeExpression) {
  // m.age
  auto* e = laExpr("m", "age");

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupKeys.push_back(e);
  expect.groupItems.push_back(e);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestArithmeticExpression1) {
  // count(a) + avg(b)
  auto* left = aggExpr("count", labelExpr("a"), false);
  auto* right = aggExpr("avg", labelExpr("b"), true);
  auto* e = addExpr(left, right);

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupItems.push_back(left);
  expect.groupItems.push_back(right);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestArithmeticExpression2) {
  // 0 - count(1)
  auto* left = constantExpr(0);
  auto* right = aggExpr("count", constantExpr(1), false);
  auto* e = minusExpr(left, right);

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupItems.push_back(right);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestRelationalExpression) {
  // e.edge + count(a) + avg(b.name)
  auto* e1 = laExpr("e", "edge");
  auto* e2 = aggExpr("count", labelExpr("a"), false);
  auto* e3 = aggExpr("avg", laExpr("b", "name"), false);
  auto* r1 = addExpr(e1, e2);
  auto* e = addExpr(r1, e3);

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupKeys.push_back(e1);
  expect.groupItems.push_back(e1);
  expect.groupItems.push_back(e2);
  expect.groupItems.push_back(e3);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestSubscriptExpression) {
  // abc [ count(a) ]
  auto* left = labelExpr("abc");
  auto* right = aggExpr("count", labelExpr("a"), false);
  auto* e = subExpr(left, right);

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupKeys.push_back(left);
  expect.groupItems.push_back(left);
  expect.groupItems.push_back(right);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestLogicalExpression1) {
  // 1 and avg(m.edge)
  auto* left = constantExpr(1);
  auto* right = aggExpr("avg", laExpr("a", "edge"), false);
  auto* e = andExpr(left, right);

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupItems.push_back(right);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestFunctionCallExpression) {
  // fun( 1 , avg(a) )
  auto* e1 = aggExpr("avg", labelExpr("a"), false);
  auto* e = fnExpr("fun", {constantExpr(1), e1});

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupItems.push_back(e1);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestAggregateExpression) {
  // count(1)
  auto* e = aggExpr("count", constantExpr(1), false);

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupItems.push_back(e);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestVariableExpression) {
  // abc
  auto* e = varExpr("abc");

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupKeys.push_back(e);
  expect.groupItems.push_back(e);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestListExpression) {
  // [ "abc", count(1) ]
  auto* a1 = constantExpr("abc");
  auto* a2 = aggExpr("count", constantExpr(1), false);
  auto* e = listExpr({a1, a2});

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupItems.push_back(a2);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestSetExpression) {
  // {"abc" + count(1), avg(a)}
  auto* a1 = constantExpr("abc");
  auto* a2 = aggExpr("count", constantExpr(1), false);
  auto* a3 = aggExpr("avg", labelExpr("a"), false);
  auto* e = setExpr({addExpr(a1, a2), a3});

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupItems.push_back(a2);
  expect.groupItems.push_back(a3);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestMapExpression) {
  // {name1:m.name, name2:collect({name:e.age}))}
  auto* a1 = laExpr("m", "name");
  auto* a2 = laExpr("e", "age");
  auto* a3 = aggExpr("collect", mapExpr({{"name", a2}}), false);
  auto* e = mapExpr({{"name1", a1}, {"name2", a3}});

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupKeys.push_back(a1);
  expect.groupItems.push_back(a1);
  expect.groupItems.push_back(a3);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestVertexExpression) {
  // VERTEX
  auto* e = vertexExpr("VERTEX");

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupKeys.push_back(e);
  expect.groupItems.push_back(e);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestPredicateExpression) {
  // any(n in [1, "abc", m.age, count(a)] where n > 1)
  auto* e1 = laExpr("m", "age");
  auto* e2 = aggExpr("count", labelExpr("a"), false);
  auto* e3 = varExpr("n", true);
  auto* e = predExpr("any",
                     "n",
                     listExpr({constantExpr(1), constantExpr("abc"), e1, e2}),
                     gtExpr(e3, constantExpr(1)));

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupKeys.push_back(e1);
  expect.groupItems.push_back(e1);
  expect.groupItems.push_back(e2);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestReduceExpression) {
  // reduce(totalNum = 10, n IN [ avg(a), 1] | totalNum + n)
  auto* e1 = aggExpr("avg", labelExpr("a"), false);
  auto* v = varExpr("n", true);
  auto* e2 = addExpr(varExpr("totalNum", true), v);
  auto* e = reduceExpr("totalNum", constantExpr(10), "n", listExpr({e1, constantExpr(1)}), e2);

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupItems.push_back(e1);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestSubscriptRangeExpression) {
  // [ avg(a), count(b)+1, ]
  auto* e1 = aggExpr("avg", labelExpr("a"), false);
  auto* e2 = aggExpr("count", labelExpr("b"), true);
  auto* e3 = addExpr(e2, constantExpr(1));
  auto* e = srExpr(listExpr({}), e1, e3);

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupItems.push_back(e1);
  expect.groupItems.push_back(e2);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

TEST_F(ExtractGroupSuiteVisitorTest, TestInlineAgg) {
  // abs(v.age) + 1 + count(v.name)
  auto* la = laExpr("v", "age");
  auto* fun = fnExpr("abs", {la});
  auto* relationExpr = addExpr(fun, constantExpr(1));
  auto* agg = aggExpr("count", laExpr("v", "name"), false);
  auto* e = addExpr(relationExpr, agg);

  ExtractGroupSuiteVisitor visitor;
  e->accept(&visitor);

  GroupSuite expect;
  expect.groupKeys.push_back(relationExpr);
  expect.groupItems.push_back(relationExpr);
  expect.groupItems.push_back(agg);

  ASSERT_EQ(true, check(visitor.groupSuite(), expect));
}

}  // namespace graph
}  // namespace nebula
