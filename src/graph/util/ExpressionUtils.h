// Copyright (c) 2021 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef _UTIL_EXPRESSION_UTILS_H_
#define _UTIL_EXPRESSION_UTILS_H_

#include "common/base/Status.h"
#include "common/expression/AggregateExpression.h"
#include "common/expression/BinaryExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LabelExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UnaryExpression.h"
#include "graph/context/ast/CypherAstContext.h"
#include "graph/visitor/EvaluableExprVisitor.h"
#include "graph/visitor/FindVisitor.h"
#include "graph/visitor/RewriteVisitor.h"

DECLARE_int32(max_expression_depth);

namespace nebula {
class ObjectPool;
namespace graph {
class ExpressionUtils {
 public:
  explicit ExpressionUtils(...) = delete;

  // ** Expression type check **
  // Checks if the kind of the given expression is one of the expected kind
  static inline bool isKindOf(const Expression* expr,
                              const std::unordered_set<Expression::Kind>& expected) {
    return expected.find(DCHECK_NOTNULL(expr)->kind()) != expected.end();
  }

  // Checks if the expression is a property expression (TagProperty or LabelTagProperty or
  // EdgeProperty or InputProperty or VarProperty or DstProperty or SrcProperty)
  static bool isPropertyExpr(const Expression* expr);

  static bool isEvaluableExpr(const Expression* expr, const QueryContext* qctx = nullptr);

  // Iterate though all sub operands recursively of the given expression, and return the first
  // expression whose kind mataches the expected list
  static const Expression* findAny(const Expression* self,
                                   const std::unordered_set<Expression::Kind>& expected);

  // Checks if the given expression contains any expression whose kind mataches the expected list
  static bool hasAny(const Expression* expr, const std::unordered_set<Expression::Kind>& expected) {
    return findAny(expr, expected) != nullptr;
  }

  // Iterates though all sub operands recursively of the given expression, and return all
  // expressions whose kinds mataches the expected list
  static std::vector<const Expression*> collectAll(
      const Expression* self, const std::unordered_set<Expression::Kind>& expected);

  // Determines if the
  static bool checkVarExprIfExist(const Expression* expr, const QueryContext* qctx);

  // ** Expression rewrite **
  // rewrites Attribute to LabelTagProp
  static Expression* rewriteAttr2LabelTagProp(
      const Expression* expr, const std::unordered_map<std::string, AliasType>& aliasTypeMap);

  // rewrite rank(e) to e._rank
  static Expression* rewriteEdgePropFunc2LabelAttribute(
      const Expression* expr, const std::unordered_map<std::string, AliasType>& aliasTypeMap);

  // rewrite LabelAttr to tagProp
  static Expression* rewriteLabelAttr2TagProp(const Expression* expr);

  // rewrite LabelAttr to EdgeProp
  static Expression* rewriteLabelAttr2EdgeProp(const Expression* expr);

  // rewrite Agg to VarProp
  static Expression* rewriteAgg2VarProp(const Expression* expr);

  // rewrite subExprs to VariablePropertyExpression
  static Expression* rewriteSubExprs2VarProp(const Expression* expr,
                                             std::vector<Expression*>& subExprs);

  // rewrite var in VariablePropExpr to another var
  static Expression* rewriteInnerVar(const Expression* expr, std::string newVar);

  // Rewrites ParameterExpression to ConstantExpression
  static Expression* rewriteParameter(const Expression* expr, QueryContext* qctx);

  // Rewrite RelInExpr with only one operand in expression tree
  static Expression* rewriteInnerInExpr(const Expression* expr);

  // Rewrites relational expression, gather all evaluable expressions in the left operands and move
  // them to the right
  static Expression* rewriteRelExpr(const Expression* expr);
  static Expression* rewriteRelExprHelper(const Expression* expr, Expression*& relRightOperandExpr);

  // Rewrites IN expression into OR expression or relEQ expression
  static Expression* rewriteInExpr(const Expression* expr);

  // Rewrites STARTS WITH to logical AND expression to support range scan
  static Expression* rewriteStartsWithExpr(const Expression* expr);

  // Rewrite Logical AND expr that contains Logical OR expr to Logical OR expr using distributive
  // law
  // Examples:
  // A and (B or C)  => (A and B) or (A and C)
  // (A or B) and (C or D)  =>  (A and C) or (A and D) or (B and C) or (B or D)
  static Expression* rewriteLogicalAndToLogicalOr(const Expression* expr);

  static Expression* foldInnerLogicalExpr(const Expression* expr);

  // Returns the operands of container expressions
  // For list and set, return the operands
  // For map, return the keys
  static std::vector<Expression*> getContainerExprOperands(const Expression* expr);

  // Clones and folds constant expression
  // Returns an error status if an overflow occurs
  // Examples:
  // v.age > 40 + 1  =>  v.age > 41
  static StatusOr<Expression*> foldConstantExpr(const Expression* expr);

  // Simplify logical and/or expr.
  // e.g. A and true => A
  //      A or false => A
  //      A and false => false
  //      A or true => true
  static Expression* simplifyLogicalExpr(const LogicalExpression* logicalExpr);

  // Clones and reduces unaryNot expression
  // Examples:
  // !!(A and B)  =>  (A and B)
  static Expression* reduceUnaryNotExpr(const Expression* expr);

  // Transforms filter using multiple expression rewrite strategies:
  // 1. rewrite relational expressions containing arithmetic operands so that
  //    all constants are on the right side of relExpr.
  // 2. fold constant
  // 3. reduce unary expression e.g. !(A and B) => !A or !B
  static StatusOr<Expression*> filterTransform(const Expression* expr);

  // Negates the given logical expr: (A && B) -> (!A || !B)
  static LogicalExpression* reverseLogicalExpr(LogicalExpression* expr);

  // Negates the given relational expr: (A > B) -> (A <= B)
  static RelationalExpression* reverseRelExpr(RelationalExpression* expr);

  // Returns the negation of the given relational kind
  static Expression::Kind getNegatedRelExprKind(const Expression::Kind kind);

  // Returns the negation of the given logical kind:
  // for logicalAnd expression returns Expression::Kind::relOR
  static Expression::Kind getNegatedLogicalExprKind(const Expression::Kind kind);

  // Returns the negation of the given arithmetic kind: plus -> minus
  static Expression::Kind getNegatedArithmeticType(const Expression::Kind kind);

  // For a logical AND expression, extracts all non-logicalAndExpr from its operands and set them as
  // the new operands
  static void pullAnds(Expression* expr);
  static void pullAndsImpl(LogicalExpression* expr, std::vector<Expression*>& operands);

  // For a logical OR expression, extracts all non-logicalOrExpr from its operands and set them as
  // the new operands
  static void pullOrs(Expression* expr);
  static void pullOrsImpl(LogicalExpression* expr, std::vector<Expression*>& operands);

  // For a logical XOR expression, extracts all non-logicalXorExpr from its operands and set them as
  // the new operands
  static void pullXors(Expression* expr);
  static void pullXorsImpl(LogicalExpression* expr, std::vector<Expression*>& operands);

  // Constructs a nested logical OR expression
  // Example:
  // [expr1, expr2, expr3]  =>  ((expr1 OR expr2) OR expr3)
  static Expression* pushOrs(ObjectPool* pool, const std::vector<Expression*>& rels);

  // Constructs a nested logical AND expression
  // Example:
  // [expr1, expr2, expr3]  =>  ((expr1 AND expr2) AND expr3)
  static Expression* pushAnds(ObjectPool* pool, const std::vector<Expression*>& rels);

  static Expression* pushImpl(ObjectPool* pool,
                              Expression::Kind kind,
                              const std::vector<Expression*>& rels);

  // Iterates the expression recursively and pulls all out logical AND expressions
  static Expression* flattenInnerLogicalAndExpr(const Expression* expr);

  // Iterates the expression recursively and pulls all out logical OR expressions
  static Expression* flattenInnerLogicalOrExpr(const Expression* expr);

  // Pulls out logical expression recursively
  // calls flattenInnerLogicalAndExpr() first then executes flattenInnerLogicalOrExpr()
  static Expression* flattenInnerLogicalExpr(const Expression* expr);

  // Check whether there exists the property of variable expression in `columns'
  static bool checkVarPropIfExist(const std::vector<std::string>& columns, const Expression* e);

  // Uses the picker to split the given expression expr into two parts: filterPicked and
  // filterUnpicked If expr is a non-LogicalAnd expression, applies the picker to expr directly If
  // expr is a logicalAnd expression, applies the picker to all its operands
  static void splitFilter(const Expression* expr,
                          std::function<bool(const Expression*)> picker,
                          Expression** filterPicked,
                          Expression** filterUnpicked);

  // TODO(Aiee) These functions are unused. Remove when refactor.
  static Expression* expandExpr(ObjectPool* pool, const Expression* expr);
  static Expression* expandImplAnd(ObjectPool* pool, const Expression* expr);
  static std::vector<Expression*> expandImplOr(const Expression* expr);

  // Check if the aggExpr is valid in yield clause
  static Status checkAggExpr(const AggregateExpression* aggExpr);

  // Checks if expr contains function call expression that generate a random value
  static bool findInnerRandFunction(const Expression* expr);

  // Checks if expr contains function EdgeDst expr or id($$) expr
  static bool findEdgeDstExpr(const Expression* expr);

  // ** Loop node condition **
  // Generates an expression that is used as the condition of loop node:
  // ++loopSteps <= steps
  static Expression* stepCondition(ObjectPool* pool, const std::string& loopStep, uint32_t steps);
  // size(var) == 0
  static Expression* zeroCondition(ObjectPool* pool, const std::string& var);
  // size(var) != 0
  static Expression* neZeroCondition(ObjectPool* pool, const std::string& var);
  // var == value
  static Expression* equalCondition(ObjectPool* pool, const std::string& var, const Value& value);

  // TODO(jie) Move it to a better place
  static bool isGeoIndexAcceleratedPredicate(const Expression* expr);

  // Checks if the depth of the expression exceeds the maximum
  // This is used in the parser to prevent OOM due to highly nested expression
  static bool checkExprDepth(const Expression* expr);

  // Whether the whole expression is vertex id predication
  // e.g. id(v) == 1, id(v) IN [...]
  static bool isVidPredication(const Expression* expr, QueryContext* qctx);

  // Check if the expr looks like `$-.e[0].likeness`
  static bool isOneStepEdgeProp(const std::string& edgeAlias, const Expression* expr);

  static Expression* rewriteEdgePropertyFilter(ObjectPool* pool,
                                               const std::string& edgeAlias,
                                               Expression* expr);
};

}  // namespace graph
}  // namespace nebula

#endif  // _UTIL_EXPRESSION_UTILS_H_
