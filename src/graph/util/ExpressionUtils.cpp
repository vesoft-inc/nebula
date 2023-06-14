// Copyright (c) 2021 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/util/ExpressionUtils.h"

#include "ExpressionUtils.h"
#include "common/base/ObjectPool.h"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/PropertyExpression.h"
#include "common/function/AggFunctionManager.h"
#include "graph/context/QueryContext.h"
#include "graph/context/QueryExpressionContext.h"
#include "graph/visitor/FoldConstantExprVisitor.h"
#include "graph/visitor/PropertyTrackerVisitor.h"

DEFINE_int32(max_expression_depth, 512, "Max depth of expression tree.");

namespace nebula {
namespace graph {

bool ExpressionUtils::isPropertyExpr(const Expression *expr) {
  return isKindOf(expr,
                  {
                      Expression::Kind::kTagProperty,
                      Expression::Kind::kLabelTagProperty,
                      Expression::Kind::kEdgeProperty,
                      Expression::Kind::kEdgeSrc,
                      Expression::Kind::kEdgeType,
                      Expression::Kind::kEdgeRank,
                      Expression::Kind::kEdgeDst,
                      Expression::Kind::kInputProperty,
                      Expression::Kind::kVarProperty,
                      Expression::Kind::kDstProperty,
                      Expression::Kind::kSrcProperty,
                  });
}

const Expression *ExpressionUtils::findAny(const Expression *self,
                                           const std::unordered_set<Expression::Kind> &expected) {
  auto finder = [&expected](const Expression *expr) -> bool {
    if (expected.find(expr->kind()) != expected.end()) {
      return true;
    }
    return false;
  };
  FindVisitor visitor(finder);
  const_cast<Expression *>(self)->accept(&visitor);
  auto res = visitor.results();

  if (res.size() == 1) {
    // findAny only produce one result
    return res.front();
  }

  return nullptr;
}

bool ExpressionUtils::findEdgeDstExpr(const Expression *expr) {
  auto finder = [](const Expression *e) -> bool {
    if (e->kind() == Expression::Kind::kEdgeDst) {
      return true;
    } else {
      auto name = e->toString();
      std::transform(name.begin(), name.end(), name.begin(), ::tolower);
      if (name == "id($$)") {
        return true;
      }
    }
    return false;
  };
  if (finder(expr)) {
    return true;
  }
  FindVisitor visitor(finder);
  const_cast<Expression *>(expr)->accept(&visitor);
  if (!visitor.results().empty()) {
    return true;
  }
  return false;
}

// Finds all expressions fit the exprected list
// Returns an empty vector if no expression found
std::vector<const Expression *> ExpressionUtils::collectAll(
    const Expression *self, const std::unordered_set<Expression::Kind> &expected) {
  auto finder = [&expected](const Expression *expr) -> bool {
    if (expected.find(expr->kind()) != expected.end()) {
      return true;
    }
    return false;
  };
  FindVisitor visitor(finder, true);
  const_cast<Expression *>(self)->accept(&visitor);
  return std::move(visitor).results();
}

bool ExpressionUtils::checkVarExprIfExist(const Expression *expr, const QueryContext *qctx) {
  auto vars = ExpressionUtils::collectAll(expr, {Expression::Kind::kVar});
  for (auto *var : vars) {
    auto *varExpr = static_cast<const VariableExpression *>(var);
    if (!varExpr->isInner() && !qctx->existParameter(varExpr->var())) {
      return true;
    }
  }
  return false;
}

bool ExpressionUtils::isEvaluableExpr(const Expression *expr, const QueryContext *qctx) {
  EvaluableExprVisitor visitor(qctx);
  const_cast<Expression *>(expr)->accept(&visitor);
  return visitor.ok();
}

Expression *ExpressionUtils::rewriteAttr2LabelTagProp(
    const Expression *expr, const std::unordered_map<std::string, AliasType> &aliasTypeMap) {
  ObjectPool *pool = expr->getObjPool();

  auto matcher = [&aliasTypeMap](const Expression *e) -> bool {
    if (e->kind() != Expression::Kind::kAttribute) {
      return false;
    }
    auto attrExpr = static_cast<const AttributeExpression *>(e);
    if (attrExpr->left()->kind() != Expression::Kind::kLabelAttribute) {
      return false;
    }
    auto label = static_cast<const LabelAttributeExpression *>(attrExpr->left())->left()->name();
    auto iter = aliasTypeMap.find(label);
    if (iter == aliasTypeMap.end() || iter->second != AliasType::kNode) {
      return false;
    }
    return true;
  };

  auto rewriter = [pool](const Expression *e) -> Expression * {
    auto attrExpr = static_cast<const AttributeExpression *>(e);
    auto labelAttrExpr = static_cast<const LabelAttributeExpression *>(attrExpr->left());
    auto labelExpr = const_cast<LabelExpression *>(labelAttrExpr->left());
    auto tagExpr = const_cast<ConstantExpression *>(labelAttrExpr->right());
    auto propExpr = static_cast<const LabelExpression *>(attrExpr->right());
    QueryExpressionContext ctx(nullptr);
    const auto &labelVal = labelExpr->eval(ctx);
    auto label = VariablePropertyExpression::make(pool, "", labelVal.getStr());
    const auto &tag = tagExpr->eval(ctx);
    const auto &prop = const_cast<LabelExpression *>(propExpr)->eval(ctx);
    return LabelTagPropertyExpression::make(pool, label, tag.getStr(), prop.getStr());
  };

  return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

// rewrite rank(e) to e._rank, none_direct_src(e) to e._src, none_direct_dst(e) to e._dst
Expression *ExpressionUtils::rewriteEdgePropFunc2LabelAttribute(
    const Expression *expr, const std::unordered_map<std::string, AliasType> &aliasTypeMap) {
  ObjectPool *pool = expr->getObjPool();
  auto matcher = [&aliasTypeMap](const Expression *e) -> bool {
    if (e->kind() != Expression::Kind::kFunctionCall) return false;

    auto *funcExpr = static_cast<const FunctionCallExpression *>(e);
    auto funcName = funcExpr->name();
    std::transform(funcName.begin(), funcName.end(), funcName.begin(), ::tolower);
    if (funcName != "rank" && funcName != "none_direct_src" && funcName != "none_direct_dst") {
      return false;
    }
    auto args = funcExpr->args()->args();
    if (args.size() != 1) return false;
    if (args[0]->kind() != Expression::Kind::kLabel) return false;

    auto &label = static_cast<const LabelExpression *>(args[0])->name();
    auto iter = aliasTypeMap.find(label);
    if (iter == aliasTypeMap.end() || iter->second != AliasType::kEdge) {
      return false;
    }
    return true;
  };
  static const std::unordered_map<std::string, std::string> func2Attr = {
      {"rank", "_rank"}, {"none_direct_src", "_src"}, {"none_direct_dst", "_dst"}};
  auto rewriter = [pool](const Expression *e) -> Expression * {
    auto funcExpr = static_cast<const FunctionCallExpression *>(e);
    auto &funcName = funcExpr->name();
    auto &attrName = func2Attr.at(funcName);
    auto args = funcExpr->args()->args();
    return LabelAttributeExpression::make(
        pool, static_cast<LabelExpression *>(args[0]), ConstantExpression::make(pool, attrName));
  };

  return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

Expression *ExpressionUtils::rewriteLabelAttr2TagProp(const Expression *expr, bool toAttrExpr) {
  ObjectPool *pool = expr->getObjPool();
  auto matcher = [](const Expression *e) -> bool {
    return e->kind() == Expression::Kind::kLabelAttribute;
  };
  auto rewriter = [pool, toAttrExpr](const Expression *e) -> Expression * {
    auto labelAttrExpr = static_cast<const LabelAttributeExpression *>(e);
    auto leftName = labelAttrExpr->left()->name();
    auto rightName = labelAttrExpr->right()->value().getStr();
    if (toAttrExpr) {
      auto tagExpr = AttributeExpression::make(
          pool, VertexExpression::make(pool), ConstantExpression::make(pool, leftName));
      return AttributeExpression::make(pool, tagExpr, ConstantExpression::make(pool, rightName));
    }
    return TagPropertyExpression::make(pool, leftName, rightName);
  };

  return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

Expression *ExpressionUtils::rewriteLabelAttr2EdgeProp(const Expression *expr, bool toAttrExpr) {
  ObjectPool *pool = expr->getObjPool();
  auto matcher = [](const Expression *e) -> bool {
    return e->kind() == Expression::Kind::kLabelAttribute;
  };
  auto rewriter = [pool, toAttrExpr](const Expression *e) -> Expression * {
    auto labelAttrExpr = static_cast<const LabelAttributeExpression *>(e);
    auto leftName = labelAttrExpr->left()->name();
    auto rightName = labelAttrExpr->right()->value().getStr();
    if (toAttrExpr) {
      return AttributeExpression::make(
          pool, EdgeExpression::make(pool), ConstantExpression::make(pool, rightName));
    }
    return EdgePropertyExpression::make(pool, leftName, rightName);
  };

  return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

Expression *ExpressionUtils::rewriteInnerVar(const Expression *expr, std::string newVar) {
  ObjectPool *pool = expr->getObjPool();
  auto matcher = [](const Expression *e) -> bool {
    return e->kind() == Expression::Kind::kVarProperty;
  };
  auto rewriter = [pool, newVar](const Expression *e) -> Expression * {
    auto varPropExpr = static_cast<const VariablePropertyExpression *>(e);
    auto newProp = varPropExpr->prop();
    return VariablePropertyExpression::make(pool, newVar, newProp);
  };

  return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

Expression *ExpressionUtils::rewriteParameter(const Expression *expr, QueryContext *qctx) {
  auto matcher = [qctx](const Expression *e) -> bool {
    return e->kind() == Expression::Kind::kVar &&
           qctx->existParameter(static_cast<const VariableExpression *>(e)->var());
  };
  auto rewriter = [qctx](const Expression *e) -> Expression * {
    DCHECK_EQ(e->kind(), Expression::Kind::kVar);
    auto &v = const_cast<Expression *>(e)->eval(graph::QueryExpressionContext(qctx->ectx())());
    return ConstantExpression::make(qctx->objPool(), v);
  };

  return graph::RewriteVisitor::transform(expr, matcher, rewriter);
}

std::vector<const Expression *> ExpressionUtils::ExtractInnerVarExprs(const Expression *expr,
                                                                      QueryContext *qctx) {
  auto finder = [qctx](const Expression *e) -> bool {
    auto ve = static_cast<const VariableExpression *>(e);
    return e->kind() == Expression::Kind::kVar && !qctx->existParameter(ve->var()) &&
           !ve->isInner();
  };

  if (finder(expr)) {
    return {expr};
  }
  FindVisitor visitor(finder);
  const_cast<Expression *>(expr)->accept(&visitor);
  return visitor.results();
}

std::vector<std::string> ExpressionUtils::ExtractInnerVars(const Expression *expr,
                                                           QueryContext *qctx) {
  auto finder = [qctx](const Expression *e) -> bool {
    auto ve = static_cast<const VariableExpression *>(e);
    return e->kind() == Expression::Kind::kVar && !qctx->existParameter(ve->var()) &&
           !ve->isInner();
  };

  if (finder(expr)) {
    return {static_cast<const VariableExpression *>(expr)->var()};
  }
  FindVisitor visitor(finder, true);
  const_cast<Expression *>(expr)->accept(&visitor);
  auto varExprs = visitor.results();
  std::vector<std::string> vars;
  vars.reserve(varExprs.size());
  for (const auto *varExpr : varExprs) {
    vars.emplace_back(static_cast<const VariableExpression *>(varExpr)->var());
  }
  return vars;
}

Expression *ExpressionUtils::rewriteInnerInExpr(const Expression *expr) {
  auto matcher = [](const Expression *e) -> bool {
    if (e->kind() != Expression::Kind::kRelIn) {
      return false;
    }
    auto rhs = static_cast<const RelationalExpression *>(e)->right();
    auto kind = rhs->kind();
    if (kind == Expression::Kind::kConstant) {
      auto v = static_cast<const ConstantExpression *>(rhs)->value();
      return v.isList() || v.isSet();
    }
    if (kind != Expression::Kind::kList && kind != Expression::Kind::kSet) {
      return false;
    }
    auto items = static_cast<const ContainerExpression *>(rhs)->getKeys();
    for (const auto *item : items) {
      if (!ExpressionUtils::isEvaluableExpr(item)) {
        return false;
      }
    }
    return true;
  };
  auto rewriter = [](const Expression *e) -> Expression * {
    DCHECK_EQ(e->kind(), Expression::Kind::kRelIn);
    const auto re = static_cast<const RelationalExpression *>(e);
    auto lhs = re->left();
    auto rhs = re->right();
    auto kind = rhs->kind();
    auto pool = e->getObjPool();
    auto *rewrittenExpr = LogicalExpression::makeOr(pool);
    // Pointer to a single-level expression
    Expression *singleExpr = nullptr;
    if (kind == Expression::Kind::kConstant) {
      auto ce = static_cast<const ConstantExpression *>(rhs);
      auto v = ce->value();
      DCHECK(v.isList() || v.isSet());
      std::vector<Value> values;
      if (v.isList()) {
        values = v.getList().values;
      } else {
        auto setItems = v.getSet().values;
        values.insert(values.end(), setItems.begin(), setItems.end());
      }
      for (auto i = 0u; i < values.size(); ++i) {
        auto *ee = RelationalExpression::makeEQ(
            pool, lhs->clone(), ConstantExpression::make(pool, values[i]));
        rewrittenExpr->addOperand(ee);
        if (i == 0) {
          singleExpr = ee;
        } else {
          singleExpr = nullptr;
        }
      }
    } else {
      DCHECK(kind == Expression::Kind::kList || kind == Expression::Kind::kSet);
      auto ce = static_cast<const ContainerExpression *>(rhs);
      auto items = ce->getKeys();
      for (auto i = 0u; i < items.size(); ++i) {
        auto *ee = RelationalExpression::makeEQ(pool, lhs->clone(), items[i]->clone());
        rewrittenExpr->addOperand(ee);
        if (i == 0) {
          singleExpr = ee;
        } else {
          singleExpr = nullptr;
        }
      }
    }
    return singleExpr ? singleExpr : rewrittenExpr;
  };

  return graph::RewriteVisitor::transform(expr, matcher, rewriter);
}

Expression *ExpressionUtils::rewriteAgg2VarProp(const Expression *expr) {
  ObjectPool *pool = expr->getObjPool();
  auto matcher = [](const Expression *e) -> bool {
    return e->kind() == Expression::Kind::kAggregate;
  };
  auto rewriter = [pool](const Expression *e) -> Expression * {
    return VariablePropertyExpression::make(pool, "", e->toString());
  };

  return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

Expression *ExpressionUtils::rewriteSubExprs2VarProp(const Expression *expr,
                                                     std::vector<Expression *> &subExprs) {
  ObjectPool *pool = expr->getObjPool();
  auto matcher = [&subExprs](const Expression *e) -> bool {
    for (auto subExpr : subExprs) {
      if (e->toString() == subExpr->toString()) {
        return true;
      }
    }
    return false;
  };
  auto rewriter = [pool](const Expression *e) -> Expression * {
    return VariablePropertyExpression::make(pool, "", e->toString());
  };

  return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

// Rewrite the IN expr to a relEQ expr if the right operand has only 1 element.
// Rewrite the IN expr to an OR expr if the right operand has more than 1 element.
Expression *ExpressionUtils::rewriteInExpr(const Expression *expr) {
  DCHECK(expr->kind() == Expression::Kind::kRelIn);
  auto pool = expr->getObjPool();
  auto inExpr = static_cast<RelationalExpression *>(expr->clone());
  auto orExpr = LogicalExpression::makeOr(pool);
  if (inExpr->right()->isContainerExpr()) {
    auto containerOperands = getContainerExprOperands(inExpr->right());

    auto operandSize = containerOperands.size();
    // container has only 1 element, no need to transform to logical expression
    if (operandSize == 1) {
      return RelationalExpression::makeEQ(pool, inExpr->left(), containerOperands[0]);
    }

    std::vector<Expression *> orExprOperands;
    orExprOperands.reserve(operandSize);
    // A in [B, C, D]  =>  (A == B) or (A == C) or (A == D)
    for (auto *operand : containerOperands) {
      orExprOperands.emplace_back(RelationalExpression::makeEQ(pool, inExpr->left(), operand));
    }
    orExpr->setOperands(orExprOperands);
  } else if (inExpr->right()->kind() == Expression::Kind::kConstant) {
    auto constExprValue = static_cast<ConstantExpression *>(inExpr->right())->value();
    std::vector<Value> values;
    if (constExprValue.isList()) {
      values = constExprValue.getList().values;
    } else if (constExprValue.isSet()) {
      auto setValues = constExprValue.getSet().values;
      values = std::vector<Value>{std::make_move_iterator(setValues.begin()),
                                  std::make_move_iterator(setValues.end())};
    } else {
      return const_cast<Expression *>(expr);
    }
    std::vector<Expression *> operands;
    operands.reserve(values.size());
    for (const auto &v : values) {
      operands.emplace_back(
          RelationalExpression::makeEQ(pool, inExpr->left(), ConstantExpression::make(pool, v)));
    }
    if (operands.size() == 1) {
      return operands[0];
    }
    orExpr->setOperands(operands);
  }

  return orExpr;
}

// Rewrite starts with expr to an AND expr to trigger a range scan
// I.g. v.name starts with "abc" => (v.name >= "abc") and (v.name < "abd")
Expression *ExpressionUtils::rewriteStartsWithExpr(const Expression *expr) {
  DCHECK(expr->kind() == Expression::Kind::kStartsWith);
  auto pool = expr->getObjPool();
  auto startsWithExpr = static_cast<RelationalExpression *>(expr->clone());

  // rhs is a string value
  QueryExpressionContext ctx(nullptr);
  auto rightBoundary = startsWithExpr->right()->eval(ctx).getStr();

  // Increment the last char of the right boundary to get the range scan boundary
  // Do not increment the last char of the string if it could cause overflow
  if (*rightBoundary.end() < 127) {
    rightBoundary[rightBoundary.size() - 1] = ++rightBoundary[rightBoundary.size() - 1];
  } else {  // If the last char is already 127, append a char of minimum value to the string
    rightBoundary += static_cast<char>(0);
  }

  auto resultLeft =
      RelationalExpression::makeGE(pool, startsWithExpr->left(), startsWithExpr->right());
  auto resultRight = RelationalExpression::makeLT(
      pool, startsWithExpr->left(), ConstantExpression::make(pool, rightBoundary));

  return LogicalExpression::makeAnd(pool, resultLeft, resultRight);
}

Expression *ExpressionUtils::foldInnerLogicalExpr(const Expression *originExpr) {
  auto matcher = [](const Expression *e) -> bool {
    return e->kind() == Expression::Kind::kLogicalAnd || e->kind() == Expression::Kind::kLogicalOr;
  };
  auto rewriter = [](const Expression *e) -> Expression * {
    auto expr = e->clone();
    auto &operands = static_cast<LogicalExpression *>(expr)->operands();
    for (auto iter = operands.begin(); iter != operands.end();) {
      if (*iter == nullptr) {
        operands.erase(iter);
      } else {
        iter++;
      }
    }
    auto n = operands.size();
    if (n == 0) {
      return nullptr;
    } else if (n == 1) {
      return operands[0];
    }
    return expr;
  };

  return RewriteVisitor::transform(originExpr, std::move(matcher), std::move(rewriter));
}

Expression *ExpressionUtils::rewriteLogicalAndToLogicalOr(const Expression *expr) {
  DCHECK(expr->kind() == Expression::Kind::kLogicalAnd);

  // If the given expression does not contain any inner logical OR expression, no need to rewrite
  if (!findAny(expr, {Expression::Kind::kLogicalOr})) {
    return const_cast<Expression *>(expr);
  }

  auto pool = expr->getObjPool();
  auto logicalAndExpr = static_cast<LogicalExpression *>(expr->clone());
  auto logicalAndExprSize = (logicalAndExpr->operands()).size();

  // Extract all OR expr
  auto orExprList = collectAll(logicalAndExpr, {Expression::Kind::kLogicalOr});
  auto orExprListSize = orExprList.size();

  // Extract all non-OR expr
  std::vector<Expression *> nonOrExprList;
  bool isAllRelOr = logicalAndExprSize == orExprListSize;

  // If logical expression has operand that is not an OR expr, add into nonOrExprList
  if (!isAllRelOr) {
    nonOrExprList.reserve(logicalAndExprSize - orExprListSize);
    for (const auto &operand : logicalAndExpr->operands()) {
      if (operand->kind() != Expression::Kind::kLogicalOr) {
        nonOrExprList.emplace_back(std::move(operand));
      }
    }
  }

  DCHECK_GT(orExprListSize, 0);
  std::vector<std::vector<Expression *>> orExprOperands{{}};
  orExprOperands.reserve(orExprListSize);

  // Merge the elements of vec2 into each subVec of vec1
  // [[A], [B]] and [C, D]  =>  [[A, C], [A, D], [B, C], [B,D]]
  auto mergeVecs = [](std::vector<std::vector<Expression *>> &vec1,
                      const std::vector<Expression *> vec2) {
    std::vector<std::vector<Expression *>> res;
    for (auto &ele1 : vec1) {
      for (const auto &ele2 : vec2) {
        auto tempSubVec = ele1;
        tempSubVec.emplace_back(std::move(ele2));
        res.emplace_back(std::move(tempSubVec));
      }
    }
    return res;
  };

  // Iterate all OR exprs and construct the operand list
  for (auto curExpr : orExprList) {
    auto curLogicalOrExpr = static_cast<LogicalExpression *>(const_cast<Expression *>(curExpr));
    auto curOrOperands = curLogicalOrExpr->operands();

    orExprOperands = mergeVecs(orExprOperands, curOrOperands);
  }

  // orExprOperands is a 2D vector where each sub-vector is the operands of AND expression.
  // [[A, C], [A, D], [B, C], [B,D]]  =>  (A and C) or (A and D) or (B and C) or (B and D)
  std::vector<Expression *> andExprList;
  andExprList.reserve(orExprOperands.size());
  for (auto &operand : orExprOperands) {
    auto andExpr = LogicalExpression::makeAnd(pool);
    // if nonOrExprList is not empty, append it to operand
    if (!isAllRelOr) {
      operand.insert(operand.end(), nonOrExprList.begin(), nonOrExprList.end());
    }
    andExpr->setOperands(operand);
    andExprList.emplace_back(std::move(andExpr));
  }

  auto orExpr = LogicalExpression::makeOr(pool);
  orExpr->setOperands(andExprList);
  return orExpr;
}

std::vector<Expression *> ExpressionUtils::getContainerExprOperands(const Expression *expr) {
  DCHECK(expr->isContainerExpr());
  auto pool = expr->getObjPool();
  auto containerExpr = expr->clone();

  std::vector<Expression *> containerOperands;
  switch (containerExpr->kind()) {
    case Expression::Kind::kList:
      containerOperands = static_cast<ListExpression *>(containerExpr)->getKeys();
      break;
    case Expression::Kind::kSet: {
      containerOperands = static_cast<SetExpression *>(containerExpr)->getKeys();
      break;
    }
    case Expression::Kind::kMap: {
      auto mapItems = static_cast<MapExpression *>(containerExpr)->get();
      // iterate map and add key into containerOperands
      for (auto &item : mapItems) {
        containerOperands.emplace_back(ConstantExpression::make(pool, std::move(item.first)));
      }
      break;
    }
    default:
      LOG(FATAL) << "Invalid expression type " << containerExpr->kind();
  }
  return containerOperands;
}

StatusOr<Expression *> ExpressionUtils::foldConstantExpr(const Expression *expr) {
  ObjectPool *objPool = expr->getObjPool();
  auto newExpr = expr->clone();
  FoldConstantExprVisitor visitor(objPool);
  newExpr->accept(&visitor);
  if (!visitor.ok()) {
    return std::move(visitor).status();
  }
  if (visitor.canBeFolded()) {
    auto foldedExpr = visitor.fold(newExpr);
    if (!visitor.ok()) {
      return std::move(visitor).status();
    }
    return foldedExpr;
  }

  auto matcher = [](const Expression *e) {
    return e->kind() == Expression::Kind::kLogicalAnd || e->kind() == Expression::Kind::kLogicalOr;
  };
  auto rewriter = [](const Expression *e) {
    auto logicalExpr = static_cast<const LogicalExpression *>(e);
    return simplifyLogicalExpr(logicalExpr);
  };
  return RewriteVisitor::transform(newExpr, matcher, rewriter);
}

Expression *ExpressionUtils::simplifyLogicalExpr(const LogicalExpression *logicalExpr) {
  auto *expr = static_cast<LogicalExpression *>(logicalExpr->clone());
  if (expr->kind() == Expression::Kind::kLogicalXor) return expr;

  ObjectPool *objPool = logicalExpr->getObjPool();

  // Simplify logical and/or
  for (auto iter = expr->operands().begin(); iter != expr->operands().end();) {
    auto *operand = *iter;
    if (operand->kind() != Expression::Kind::kConstant) {
      ++iter;
      continue;
    }
    auto &val = static_cast<ConstantExpression *>(operand)->value();
    if (!val.isBool()) {
      ++iter;
      continue;
    }
    if (expr->kind() == Expression::Kind::kLogicalAnd) {
      if (val.getBool()) {
        // Remove the true operand
        iter = expr->operands().erase(iter);
        continue;
      }
      // The whole expression is false
      return ConstantExpression::make(objPool, false);
    }
    // expr->kind() == Expression::Kind::kLogicalOr
    if (val.getBool()) {
      // The whole expression is true
      return ConstantExpression::make(objPool, true);
    }
    // Remove the false operand
    iter = expr->operands().erase(iter);
  }

  if (expr->operands().empty()) {
    // true and true and true => true
    if (expr->kind() == Expression::Kind::kLogicalAnd) {
      return ConstantExpression::make(objPool, true);
    }
    // false or false or false => false
    return ConstantExpression::make(objPool, false);
  } else if (expr->operands().size() == 1) {
    return expr->operands()[0];
  } else {
    return expr;
  }
}

Expression *ExpressionUtils::reduceUnaryNotExpr(const Expression *expr) {
  // Match the operand
  auto operandMatcher = [](const Expression *operandExpr) -> bool {
    return (operandExpr->kind() == Expression::Kind::kUnaryNot ||
            (operandExpr->isRelExpr() && operandExpr->kind() != Expression::Kind::kRelREG) ||
            operandExpr->kind() == Expression::Kind::kLogicalAnd ||
            operandExpr->kind() == Expression::Kind::kLogicalOr);
  };

  // Match the root expression
  auto rootMatcher = [&operandMatcher](const Expression *e) -> bool {
    if (e->kind() == Expression::Kind::kUnaryNot) {
      auto operand = static_cast<const UnaryExpression *>(e)->operand();
      return (operandMatcher(operand));
    }
    return false;
  };

  std::function<Expression *(const Expression *)> rewriter =
      [&](const Expression *e) -> Expression * {
    auto operand = static_cast<const UnaryExpression *>(e)->operand();
    auto reducedExpr = operand->clone();

    if (reducedExpr->kind() == Expression::Kind::kUnaryNot) {
      auto castedExpr = static_cast<UnaryExpression *>(reducedExpr);
      reducedExpr = castedExpr->operand();
    } else if (reducedExpr->isRelExpr() && reducedExpr->kind() != Expression::Kind::kRelREG) {
      auto castedExpr = static_cast<RelationalExpression *>(reducedExpr);
      reducedExpr = reverseRelExpr(castedExpr);
    } else if (reducedExpr->isLogicalExpr()) {
      auto castedExpr = static_cast<LogicalExpression *>(reducedExpr);
      reducedExpr = reverseLogicalExpr(castedExpr);
    }
    // Rewrite the output of rewrite if possible
    return operandMatcher(reducedExpr)
               ? RewriteVisitor::transform(reducedExpr, rootMatcher, rewriter)
               : reducedExpr;
  };

  return RewriteVisitor::transform(expr, rootMatcher, rewriter);
}

Expression *ExpressionUtils::rewriteRelExpr(const Expression *expr) {
  ObjectPool *pool = expr->getObjPool();
  QueryExpressionContext ctx(nullptr);

  auto checkArithmExpr = [&ctx](const ArithmeticExpression *arithmExpr) -> bool {
    auto lExpr = const_cast<Expression *>(arithmExpr->left());
    auto rExpr = const_cast<Expression *>(arithmExpr->right());

    // If the arithExpr has constant expr as member that is a string, do not rewrite
    if (lExpr->kind() == Expression::Kind::kConstant) {
      if (lExpr->eval(ctx).isStr()) {
        return false;
      }
    }
    if (rExpr->kind() == Expression::Kind::kConstant) {
      if (rExpr->eval(ctx).isStr()) {
        return false;
      }
    }
    return isEvaluableExpr(arithmExpr->left()) || isEvaluableExpr(arithmExpr->right());
  };

  // Match relational expressions following these rules:
  // 1. the right operand of rel expr should be evaluable
  // 2. the left operand of rel expr should be:
  // - 2.a an arithmetic expr that does not contains string and has at least one operand that is
  // evaluable
  // OR
  // - 2.b an relational expr so that it might could be simplified:
  // ((v.age > 40 == true)  => (v.age > 40))
  auto matcher = [&checkArithmExpr](const Expression *e) -> bool {
    if (!e->isRelExpr()) {
      return false;
    }

    auto relExpr = static_cast<const RelationalExpression *>(e);
    // Check right operand
    bool checkRightOperand = isEvaluableExpr(relExpr->right());
    if (!checkRightOperand) {
      return false;
    }

    // Check left operand
    bool checkLeftOperand = false;
    auto lExpr = relExpr->left();
    // Left operand is arithmetical expr
    if (lExpr->isArithmeticExpr()) {
      auto arithmExpr = static_cast<const ArithmeticExpression *>(lExpr);
      checkLeftOperand = checkArithmExpr(arithmExpr);
    } else if (lExpr->isRelExpr() ||
               lExpr->kind() == Expression::Kind::kLabelAttribute) {  // for expressions that
                                                                      // contain boolean literals
                                                                      // such as (v.age <= null)
      checkLeftOperand = true;
    }
    return checkLeftOperand;
  };

  // Simplify relational expressions involving boolean literals
  auto simplifyBoolOperand = [pool, &ctx](RelationalExpression *relExpr,
                                          Expression *lExpr,
                                          Expression *rExpr) -> Expression * {
    if (rExpr->kind() == Expression::Kind::kConstant) {
      auto conExpr = static_cast<ConstantExpression *>(rExpr);
      auto val = conExpr->eval(ctx);
      auto valType = val.type();
      // Rewrite to null if the expression contains any operand that is null
      if (valType == Value::Type::NULLVALUE) {
        return rExpr->clone();
      }
      if (relExpr->kind() == Expression::Kind::kRelEQ) {
        if (valType == Value::Type::BOOL) {
          return val.getBool() ? lExpr->clone() : UnaryExpression::makeNot(pool, lExpr->clone());
        }
      } else if (relExpr->kind() == Expression::Kind::kRelNE) {
        if (valType == Value::Type::BOOL) {
          return val.getBool() ? UnaryExpression::makeNot(pool, lExpr->clone()) : lExpr->clone();
        }
      }
    }
    return nullptr;
  };

  auto rewriter = [&](const Expression *e) -> Expression * {
    auto exprCopy = e->clone();
    auto relExpr = static_cast<RelationalExpression *>(exprCopy);
    auto lExpr = relExpr->left();
    auto rExpr = relExpr->right();

    // Simplify relational expressions involving boolean literals
    auto simplifiedExpr = simplifyBoolOperand(relExpr, lExpr, rExpr);
    if (simplifiedExpr) {
      return simplifiedExpr;
    }
    // Move all evaluable expression to the right side
    auto relRightOperandExpr = relExpr->right()->clone();
    auto relLeftOperandExpr = rewriteRelExprHelper(relExpr->left(), relRightOperandExpr);
    return RelationalExpression::makeKind(
        pool, relExpr->kind(), relLeftOperandExpr->clone(), relRightOperandExpr->clone());
  };

  return RewriteVisitor::transform(expr, matcher, rewriter);
}

Expression *ExpressionUtils::rewriteRelExprHelper(const Expression *expr,
                                                  Expression *&relRightOperandExpr) {
  ObjectPool *pool = expr->getObjPool();
  // TODO: Support rewrite mul/div expression after fixing overflow
  auto matcher = [](const Expression *e) -> bool {
    if (!e->isArithmeticExpr() || e->kind() == Expression::Kind::kMultiply ||
        e->kind() == Expression::Kind::kDivision)
      return false;
    auto arithExpr = static_cast<const ArithmeticExpression *>(e);

    return ExpressionUtils::isEvaluableExpr(arithExpr->left()) ||
           ExpressionUtils::isEvaluableExpr(arithExpr->right());
  };

  if (!matcher(expr)) {
    return const_cast<Expression *>(expr);
  }

  auto arithExpr = static_cast<const ArithmeticExpression *>(expr);
  auto kind = getNegatedArithmeticType(arithExpr->kind());
  auto lexpr = relRightOperandExpr->clone();
  const Expression *root = nullptr;
  Expression *rexpr = nullptr;

  // Use left operand as root
  if (ExpressionUtils::isEvaluableExpr(arithExpr->right())) {
    rexpr = arithExpr->right()->clone();
    root = arithExpr->left();
  } else {
    rexpr = arithExpr->left()->clone();
    root = arithExpr->right();
  }
  switch (kind) {
    case Expression::Kind::kAdd:
      relRightOperandExpr = ArithmeticExpression::makeAdd(pool, lexpr, rexpr);
      break;
    case Expression::Kind::kMinus:
      relRightOperandExpr = ArithmeticExpression::makeMinus(pool, lexpr, rexpr);
      break;
    // Unsupported arithmetic kind
    // case Expression::Kind::kMultiply:
    // case Expression::Kind::kDivision:
    default:
      DLOG(ERROR) << "Unsupported expression kind: " << static_cast<uint8_t>(kind);
      break;
  }

  return rewriteRelExprHelper(root, relRightOperandExpr);
}

StatusOr<Expression *> ExpressionUtils::filterTransform(const Expression *filter) {
  // Check if any overflow happen before filter transform
  auto initialConstFold = foldConstantExpr(filter);
  NG_RETURN_IF_ERROR(initialConstFold);
  auto newFilter = initialConstFold.value();

  // If the filter contains more than one different Label expr, this filter cannot be
  // pushed down, such as where v1.player.name == 'xxx' or v2.player.age == 20
  auto propExprs = ExpressionUtils::collectAll(newFilter, {Expression::Kind::kLabel});
  // Deduplicate the list
  std::unordered_set<std::string> dedupPropExprSet;
  for (auto &iter : propExprs) {
    dedupPropExprSet.emplace(iter->toString());
  }
  if (dedupPropExprSet.size() > 1) {
    return const_cast<Expression *>(newFilter);
  }

  // Rewrite relational expression
  auto rewrittenExpr = rewriteRelExpr(newFilter->clone());

  // Fold constant expression
  auto constantFoldRes = foldConstantExpr(rewrittenExpr);
  // If errors like overflow happened during the constant fold, stop transferming and return the
  // original expression
  if (!constantFoldRes.ok()) {
    return const_cast<Expression *>(newFilter);
  }
  rewrittenExpr = constantFoldRes.value();

  // Reduce Unary expression
  rewrittenExpr = reduceUnaryNotExpr(rewrittenExpr);
  return rewrittenExpr;
}

void ExpressionUtils::pullAnds(Expression *expr) {
  DCHECK(expr->kind() == Expression::Kind::kLogicalAnd);
  auto *logic = static_cast<LogicalExpression *>(expr);
  std::vector<Expression *> operands;
  pullAndsImpl(logic, operands);
  logic->setOperands(std::move(operands));
}

void ExpressionUtils::pullOrs(Expression *expr) {
  DCHECK(expr->kind() == Expression::Kind::kLogicalOr);
  auto *logic = static_cast<LogicalExpression *>(expr);
  std::vector<Expression *> operands;
  pullOrsImpl(logic, operands);
  logic->setOperands(std::move(operands));
}

void ExpressionUtils::pullXors(Expression *expr) {
  DCHECK(expr->kind() == Expression::Kind::kLogicalXor);
  auto *logic = static_cast<LogicalExpression *>(expr);
  std::vector<Expression *> operands;
  pullXorsImpl(logic, operands);
  logic->setOperands(std::move(operands));
}

void ExpressionUtils::pullAndsImpl(LogicalExpression *expr, std::vector<Expression *> &operands) {
  for (auto &operand : expr->operands()) {
    if (operand->kind() != Expression::Kind::kLogicalAnd) {
      operands.emplace_back(std::move(operand));
      continue;
    }
    pullAndsImpl(static_cast<LogicalExpression *>(operand), operands);
  }
}

void ExpressionUtils::pullOrsImpl(LogicalExpression *expr, std::vector<Expression *> &operands) {
  for (auto &operand : expr->operands()) {
    if (operand->kind() != Expression::Kind::kLogicalOr) {
      operands.emplace_back(std::move(operand));
      continue;
    }
    pullOrsImpl(static_cast<LogicalExpression *>(operand), operands);
  }
}

void ExpressionUtils::pullXorsImpl(LogicalExpression *expr, std::vector<Expression *> &operands) {
  for (auto &operand : expr->operands()) {
    if (operand->kind() != Expression::Kind::kLogicalXor) {
      operands.emplace_back(std::move(operand));
      continue;
    }
    pullXorsImpl(static_cast<LogicalExpression *>(operand), operands);
  }
}

Expression *ExpressionUtils::flattenInnerLogicalAndExpr(const Expression *expr) {
  auto matcher = [](const Expression *e) -> bool {
    return e->kind() == Expression::Kind::kLogicalAnd;
  };
  auto rewriter = [](const Expression *e) -> Expression * {
    pullAnds(const_cast<Expression *>(e));
    return e->clone();
  };

  return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

Expression *ExpressionUtils::flattenInnerLogicalOrExpr(const Expression *expr) {
  auto matcher = [](const Expression *e) -> bool {
    return e->kind() == Expression::Kind::kLogicalOr;
  };
  auto rewriter = [](const Expression *e) -> Expression * {
    pullOrs(const_cast<Expression *>(e));
    return e->clone();
  };

  return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

Expression *ExpressionUtils::flattenInnerLogicalExpr(const Expression *expr) {
  auto andFlattenExpr = flattenInnerLogicalAndExpr(expr);
  auto allFlattenExpr = flattenInnerLogicalOrExpr(andFlattenExpr);

  return allFlattenExpr;
}

bool ExpressionUtils::checkColName(const std::vector<std::string> &columns, const Expression *e) {
  auto exprs = graph::ExpressionUtils::collectAll(
      e, {Expression::Kind::kVarProperty, Expression::Kind::kVertex, Expression::Kind::kEdge});
  if (exprs.empty()) {
    return false;
  }
  for (const auto *expr : exprs) {
    std::string colName;
    if (expr->kind() == Expression::Kind::kVarProperty) {
      colName = static_cast<const VariablePropertyExpression *>(expr)->prop();
    } else if (expr->kind() == Expression::Kind::kVertex) {
      colName = static_cast<const VertexExpression *>(expr)->name();
    } else {
      colName = static_cast<const EdgeExpression *>(expr)->toString();
    }
    auto iter = std::find_if(columns.begin(), columns.end(), [&colName](const std::string &item) {
      return !item.compare(colName);
    });
    if (iter == columns.end()) {
      return false;
    }
  }
  return true;
}

// pick the subparts of expression that meet picker's criteria
void ExpressionUtils::splitFilter(const Expression *expr,
                                  std::function<bool(const Expression *)> picker,
                                  Expression **filterPicked,
                                  Expression **filterUnpicked) {
  ObjectPool *pool = expr->getObjPool();
  // Pick the non-LogicalAndExpr directly
  if (expr->kind() != Expression::Kind::kLogicalAnd) {
    if (picker(expr)) {
      *filterPicked = expr->clone();
    } else {
      *filterUnpicked = expr->clone();
    }
    return;
  }

  auto flattenExpr = ExpressionUtils::flattenInnerLogicalExpr(expr);
  DCHECK(flattenExpr->kind() == Expression::Kind::kLogicalAnd);
  auto *logicExpr = static_cast<LogicalExpression *>(flattenExpr);
  auto filterPickedPtr = LogicalExpression::makeAnd(pool);
  auto filterUnpickedPtr = LogicalExpression::makeAnd(pool);

  std::vector<Expression *> &operands = logicExpr->operands();
  for (auto &operand : operands) {
    // TODO(czp): Sink all NOTs to second layer [[Refactor]]
    // TODO(czp): If find any not, dont pick this operand for now
    if (ExpressionUtils::findAny(operand, {Expression::Kind::kUnaryNot})) {
      filterUnpickedPtr->addOperand(operand->clone());
      continue;
    }
    if (picker(operand)) {
      filterPickedPtr->addOperand(operand->clone());
    } else {
      filterUnpickedPtr->addOperand(operand->clone());
    }
  }
  auto foldLogicalExpr = [](const LogicalExpression *e) -> Expression * {
    const auto &ops = e->operands();
    auto size = ops.size();
    if (size > 1) {
      return e->clone();
    } else if (size == 1) {
      return ops[0]->clone();
    } else {
      return nullptr;
    }
  };
  *filterPicked = foldLogicalExpr(filterPickedPtr);
  *filterUnpicked = foldLogicalExpr(filterUnpickedPtr);
}

Expression *ExpressionUtils::pushOrs(ObjectPool *pool, const std::vector<Expression *> &rels) {
  return pushImpl(pool, Expression::Kind::kLogicalOr, rels);
}

Expression *ExpressionUtils::pushAnds(ObjectPool *pool, const std::vector<Expression *> &rels) {
  return pushImpl(pool, Expression::Kind::kLogicalAnd, rels);
}

Expression *ExpressionUtils::pushImpl(ObjectPool *pool,
                                      Expression::Kind kind,
                                      const std::vector<Expression *> &rels) {
  DCHECK_GT(rels.size(), 1);
  DCHECK(kind == Expression::Kind::kLogicalOr || kind == Expression::Kind::kLogicalAnd);
  auto root = LogicalExpression::makeKind(pool, kind);
  root->addOperand(rels[0]->clone());
  root->addOperand(rels[1]->clone());
  for (size_t i = 2; i < rels.size(); i++) {
    auto l = LogicalExpression::makeKind(pool, kind);
    l->addOperand(root->clone());
    l->addOperand(rels[i]->clone());
    root = std::move(l);
  }
  return root;
}

Expression *ExpressionUtils::expandExpr(ObjectPool *pool, const Expression *expr) {
  auto kind = expr->kind();
  std::vector<Expression *> target;
  switch (kind) {
    case Expression::Kind::kLogicalOr: {
      const auto *logic = static_cast<const LogicalExpression *>(expr);
      for (const auto &e : logic->operands()) {
        if (e->kind() == Expression::Kind::kLogicalAnd) {
          target.emplace_back(expandImplAnd(pool, e));
        } else {
          target.emplace_back(expandExpr(pool, e));
        }
      }
      break;
    }
    case Expression::Kind::kLogicalAnd: {
      target.emplace_back(expandImplAnd(pool, expr));
      break;
    }
    default: {
      return expr->clone();
    }
  }
  DCHECK_GT(target.size(), 0);
  if (target.size() == 1) {
    if (target[0]->kind() == Expression::Kind::kLogicalAnd) {
      const auto *logic = static_cast<const LogicalExpression *>(target[0]);
      const auto &ops = logic->operands();
      DCHECK_EQ(ops.size(), 2);
      if (ops[0]->kind() == Expression::Kind::kLogicalOr ||
          ops[1]->kind() == Expression::Kind::kLogicalOr) {
        return expandExpr(pool, target[0]);
      }
    }
    return std::move(target[0]);
  }
  return pushImpl(pool, kind, target);
}

Expression *ExpressionUtils::expandImplAnd(ObjectPool *pool, const Expression *expr) {
  DCHECK(expr->kind() == Expression::Kind::kLogicalAnd);
  const auto *logic = static_cast<const LogicalExpression *>(expr);
  DCHECK_EQ(logic->operands().size(), 2);
  std::vector<Expression *> subL;
  auto &ops = logic->operands();
  if (ops[0]->kind() == Expression::Kind::kLogicalOr) {
    auto target = expandImplOr(ops[0]);
    for (const auto &e : target) {
      subL.emplace_back(e->clone());
    }
  } else {
    subL.emplace_back(expandExpr(pool, std::move(ops[0])));
  }
  std::vector<Expression *> subR;
  if (ops[1]->kind() == Expression::Kind::kLogicalOr) {
    auto target = expandImplOr(ops[1]);
    for (const auto &e : target) {
      subR.emplace_back(e->clone());
    }
  } else {
    subR.emplace_back(expandExpr(pool, std::move(ops[1])));
  }

  DCHECK_GT(subL.size(), 0);
  DCHECK_GT(subR.size(), 0);
  std::vector<Expression *> target;
  for (auto &le : subL) {
    for (auto &re : subR) {
      auto l = LogicalExpression::makeAnd(pool);
      l->addOperand(le->clone());
      l->addOperand(re->clone());
      target.emplace_back(std::move(l));
    }
  }
  DCHECK_GT(target.size(), 0);
  if (target.size() == 1) {
    return std::move(target[0]);
  }
  return pushImpl(pool, Expression::Kind::kLogicalOr, target);
}

std::vector<Expression *> ExpressionUtils::expandImplOr(const Expression *expr) {
  DCHECK(expr->kind() == Expression::Kind::kLogicalOr);
  const auto *logic = static_cast<const LogicalExpression *>(expr);
  std::vector<Expression *> exprs;
  auto &ops = logic->operands();
  for (const auto &op : ops) {
    if (op->kind() == Expression::Kind::kLogicalOr) {
      auto target = expandImplOr(op);
      for (const auto &e : target) {
        exprs.emplace_back(e->clone());
      }
    } else {
      exprs.emplace_back(op->clone());
    }
  }
  return exprs;
}

Status ExpressionUtils::checkAggExpr(const AggregateExpression *aggExpr) {
  const auto &func = aggExpr->name();
  NG_RETURN_IF_ERROR(AggFunctionManager::find(func));

  auto *aggArg = aggExpr->arg();
  if (graph::ExpressionUtils::findAny(aggArg, {Expression::Kind::kAggregate})) {
    return Status::SemanticError("Aggregate function nesting is not allowed: `%s'",
                                 aggExpr->toString().c_str());
  }

  // check : $-.* or $var.* can only be applied on `COUNT`
  if (func.compare("COUNT") && (aggArg->kind() == Expression::Kind::kInputProperty ||
                                aggArg->kind() == Expression::Kind::kVarProperty)) {
    auto propExpr = static_cast<const PropertyExpression *>(aggArg);
    if (propExpr->prop() == "*") {
      return Status::SemanticError("Could not apply aggregation function `%s' on `%s'",
                                   aggExpr->toString().c_str(),
                                   propExpr->toString().c_str());
    }
  }

  return Status::OK();
}

bool ExpressionUtils::findInnerRandFunction(const Expression *expr) {
  auto finder = [](const Expression *e) -> bool {
    if (e->kind() == Expression::Kind::kFunctionCall) {
      auto func = static_cast<const FunctionCallExpression *>(e)->name();
      std::transform(func.begin(), func.end(), func.begin(), ::tolower);
      return !func.compare("rand") || !func.compare("rand32") || !func.compare("rand64");
    }
    return false;
  };
  if (finder(expr)) {
    return true;
  }
  FindVisitor visitor(finder);
  const_cast<Expression *>(expr)->accept(&visitor);
  if (!visitor.results().empty()) {
    return true;
  }
  return false;
}

// Negate the given relational expr
RelationalExpression *ExpressionUtils::reverseRelExpr(RelationalExpression *expr) {
  ObjectPool *pool = expr->getObjPool();
  auto left = static_cast<RelationalExpression *>(expr)->left();
  auto right = static_cast<RelationalExpression *>(expr)->right();
  auto negatedKind = getNegatedRelExprKind(expr->kind());

  return RelationalExpression::makeKind(pool, negatedKind, left->clone(), right->clone());
}

// Return the negation of the given relational kind
Expression::Kind ExpressionUtils::getNegatedRelExprKind(const Expression::Kind kind) {
  switch (kind) {
    case Expression::Kind::kRelEQ:
      return Expression::Kind::kRelNE;
    case Expression::Kind::kRelNE:
      return Expression::Kind::kRelEQ;
    case Expression::Kind::kRelLT:
      return Expression::Kind::kRelGE;
    case Expression::Kind::kRelLE:
      return Expression::Kind::kRelGT;
    case Expression::Kind::kRelGT:
      return Expression::Kind::kRelLE;
    case Expression::Kind::kRelGE:
      return Expression::Kind::kRelLT;
    case Expression::Kind::kRelIn:
      return Expression::Kind::kRelNotIn;
    case Expression::Kind::kRelNotIn:
      return Expression::Kind::kRelIn;
    case Expression::Kind::kContains:
      return Expression::Kind::kNotContains;
    case Expression::Kind::kNotContains:
      return Expression::Kind::kContains;
    case Expression::Kind::kStartsWith:
      return Expression::Kind::kNotStartsWith;
    case Expression::Kind::kNotStartsWith:
      return Expression::Kind::kStartsWith;
    case Expression::Kind::kEndsWith:
      return Expression::Kind::kNotEndsWith;
    case Expression::Kind::kNotEndsWith:
      return Expression::Kind::kEndsWith;
    default:
      LOG(FATAL) << "Invalid relational expression kind: " << static_cast<uint8_t>(kind);
      break;
  }
}

Expression::Kind ExpressionUtils::getNegatedArithmeticType(const Expression::Kind kind) {
  switch (kind) {
    case Expression::Kind::kAdd:
      return Expression::Kind::kMinus;
    case Expression::Kind::kMinus:
      return Expression::Kind::kAdd;
    case Expression::Kind::kMultiply:
      return Expression::Kind::kDivision;
    case Expression::Kind::kDivision:
      return Expression::Kind::kMultiply;
    // There is no oppsite operation to Mod, return itself
    case Expression::Kind::kMod:
      return Expression::Kind::kMod;
      break;
    default:
      LOG(FATAL) << "Invalid arithmetic expression kind: " << static_cast<uint8_t>(kind);
      break;
  }
}

LogicalExpression *ExpressionUtils::reverseLogicalExpr(LogicalExpression *expr) {
  ObjectPool *pool = expr->getObjPool();
  std::vector<Expression *> operands;
  if (expr->kind() == Expression::Kind::kLogicalAnd) {
    pullAnds(expr);
  } else if (expr->kind() == Expression::Kind::kLogicalOr) {
    pullOrs(expr);
  } else if (expr->kind() == Expression::Kind::kLogicalXor) {
    pullXors(expr);
  } else {
    LOG(FATAL) << "Invalid logical expression kind: " << static_cast<uint8_t>(expr->kind());
  }

  auto &flattenOperands = static_cast<LogicalExpression *>(expr)->operands();
  auto negatedKind = getNegatedLogicalExprKind(expr->kind());
  auto logic = LogicalExpression::makeKind(pool, negatedKind);

  // negate each item in the operands list
  for (auto &operand : flattenOperands) {
    auto tempExpr = UnaryExpression::makeNot(pool, operand);
    operands.emplace_back(std::move(tempExpr));
  }
  logic->setOperands(std::move(operands));
  return logic;
}

Expression::Kind ExpressionUtils::getNegatedLogicalExprKind(const Expression::Kind kind) {
  switch (kind) {
    case Expression::Kind::kLogicalAnd:
      return Expression::Kind::kLogicalOr;
    case Expression::Kind::kLogicalOr:
      return Expression::Kind::kLogicalAnd;
    case Expression::Kind::kLogicalXor:
      return Expression::Kind::kLogicalXor;
    default:
      LOG(FATAL) << "Invalid logical expression kind: " << static_cast<uint8_t>(kind);
      break;
  }
}

// ++loopStep <= steps
Expression *ExpressionUtils::stepCondition(ObjectPool *pool,
                                           const std::string &loopStep,
                                           uint32_t steps) {
  return RelationalExpression::makeLE(
      pool,
      UnaryExpression::makeIncr(pool, VariableExpression::make(pool, loopStep)),
      ConstantExpression::make(pool, static_cast<int32_t>(steps)));
}

// size(var) != 0
Expression *ExpressionUtils::neZeroCondition(ObjectPool *pool, const std::string &var) {
  auto *args = ArgumentList::make(pool);
  args->addArgument(VariableExpression::make(pool, var));
  return RelationalExpression::makeNE(
      pool, FunctionCallExpression::make(pool, "size", args), ConstantExpression::make(pool, 0));
}

// size(var) == 0
Expression *ExpressionUtils::zeroCondition(ObjectPool *pool, const std::string &var) {
  auto *args = ArgumentList::make(pool);
  args->addArgument(VariableExpression::make(pool, var));
  return RelationalExpression::makeEQ(
      pool, FunctionCallExpression::make(pool, "size", args), ConstantExpression::make(pool, 0));
}

// var == value
Expression *ExpressionUtils::equalCondition(ObjectPool *pool,
                                            const std::string &var,
                                            const Value &value) {
  return RelationalExpression::makeEQ(
      pool, VariableExpression::make(pool, var), ConstantExpression::make(pool, value));
}

bool ExpressionUtils::isGeoIndexAcceleratedPredicate(const Expression *expr) {
  static std::unordered_set<std::string> geoIndexAcceleratedPredicates{
      "st_intersects", "st_covers", "st_coveredby", "st_dwithin"};

  if (expr->isRelExpr()) {
    auto *relExpr = static_cast<const RelationalExpression *>(expr);
    auto isSt_Distance = [](const Expression *e) {
      if (e->kind() == Expression::Kind::kFunctionCall) {
        auto *funcExpr = static_cast<const FunctionCallExpression *>(e);
        return boost::algorithm::iequals(funcExpr->name(), "st_distance");
      }
      return false;
    };
    return isSt_Distance(relExpr->left()) || isSt_Distance(relExpr->right());
  } else if (expr->kind() == Expression::Kind::kFunctionCall) {
    auto *funcExpr = static_cast<const FunctionCallExpression *>(expr);
    std::string funcName = funcExpr->name();
    folly::toLowerAscii(funcName);
    if (geoIndexAcceleratedPredicates.count(funcName) != 0) {
      return true;
    }
  }

  return false;
}

bool ExpressionUtils::checkExprDepth(const Expression *expr) {
  std::queue<const Expression *> exprQueue;
  exprQueue.emplace(expr);
  int depth = 0;
  while (!exprQueue.empty()) {
    int size = exprQueue.size();
    while (size > 0) {
      const Expression *cur = exprQueue.front();
      exprQueue.pop();
      switch (cur->kind()) {
        case Expression::Kind::kConstant:
        case Expression::Kind::kVar: {
          break;
        }
        case Expression::Kind::kAdd:
        case Expression::Kind::kMinus:
        case Expression::Kind::kMultiply:
        case Expression::Kind::kDivision:
        case Expression::Kind::kMod: {
          auto *ariExpr = static_cast<const ArithmeticExpression *>(cur);
          exprQueue.emplace(ariExpr->left());
          exprQueue.emplace(ariExpr->right());
          break;
        }
        case Expression::Kind::kIsNull:
        case Expression::Kind::kIsNotNull:
        case Expression::Kind::kIsEmpty:
        case Expression::Kind::kIsNotEmpty:
        case Expression::Kind::kUnaryPlus:
        case Expression::Kind::kUnaryNegate:
        case Expression::Kind::kUnaryNot:
        case Expression::Kind::kUnaryIncr:
        case Expression::Kind::kUnaryDecr: {
          auto *unaExpr = static_cast<const UnaryExpression *>(cur);
          exprQueue.emplace(unaExpr->operand());
          break;
        }
        case Expression::Kind::kRelEQ:
        case Expression::Kind::kRelNE:
        case Expression::Kind::kRelLT:
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelGT:
        case Expression::Kind::kRelGE:
        case Expression::Kind::kRelREG:
        case Expression::Kind::kContains:
        case Expression::Kind::kNotContains:
        case Expression::Kind::kStartsWith:
        case Expression::Kind::kNotStartsWith:
        case Expression::Kind::kEndsWith:
        case Expression::Kind::kNotEndsWith:
        case Expression::Kind::kRelNotIn:
        case Expression::Kind::kRelIn: {
          auto *relExpr = static_cast<const RelationalExpression *>(cur);
          exprQueue.emplace(relExpr->left());
          exprQueue.emplace(relExpr->right());
          break;
        }
        case Expression::Kind::kList: {
          auto *listExpr = static_cast<const ListExpression *>(cur);
          for (auto &item : listExpr->items()) {
            exprQueue.emplace(item);
          }
          break;
        }
        case Expression::Kind::kSet: {
          auto *setExpr = static_cast<const SetExpression *>(cur);
          for (auto &item : setExpr->items()) {
            exprQueue.emplace(item);
          }
          break;
        }
        case Expression::Kind::kMap: {
          auto *mapExpr = static_cast<const MapExpression *>(cur);
          for (auto &item : mapExpr->items()) {
            exprQueue.emplace(item.second);
          }
          break;
        }
        case Expression::Kind::kCase: {
          auto *caseExpr = static_cast<const CaseExpression *>(cur);
          if (caseExpr->hasCondition()) {
            exprQueue.emplace(caseExpr->condition());
          }
          if (caseExpr->hasDefault()) {
            exprQueue.emplace(caseExpr->defaultResult());
          }
          for (auto &whenThen : caseExpr->cases()) {
            exprQueue.emplace(whenThen.when);
            exprQueue.emplace(whenThen.then);
          }
          break;
        }
        case Expression::Kind::kListComprehension: {
          auto *lcExpr = static_cast<const ListComprehensionExpression *>(cur);
          exprQueue.emplace(lcExpr->collection());
          if (lcExpr->hasFilter()) {
            exprQueue.emplace(lcExpr->filter());
          }
          if (lcExpr->hasMapping()) {
            exprQueue.emplace(lcExpr->mapping());
          }
          break;
        }
        case Expression::Kind::kPredicate: {
          auto *predExpr = static_cast<const PredicateExpression *>(cur);
          exprQueue.emplace(predExpr->collection());
          if (predExpr->hasFilter()) {
            exprQueue.emplace(predExpr->filter());
          }
          break;
        }
        case Expression::Kind::kReduce: {
          auto *reduceExpr = static_cast<const ReduceExpression *>(cur);
          exprQueue.emplace(reduceExpr->collection());
          exprQueue.emplace(reduceExpr->mapping());
          break;
        }
        case Expression::Kind::kLogicalAnd:
        case Expression::Kind::kLogicalOr:
        case Expression::Kind::kLogicalXor: {
          auto *logExpr = static_cast<const LogicalExpression *>(cur);
          for (auto &op : logExpr->operands()) {
            exprQueue.emplace(op);
          }
          break;
        }
        case Expression::Kind::kTypeCasting: {
          auto *typExpr = static_cast<const TypeCastingExpression *>(cur);
          exprQueue.emplace(typExpr->operand());
          break;
        }
        case Expression::Kind::kFunctionCall: {
          auto *funExpr = static_cast<const FunctionCallExpression *>(cur);
          auto &args = funExpr->args()->args();
          for (auto iter = args.begin(); iter < args.end(); ++iter) {
            exprQueue.emplace(*iter);
          }
          break;
        }
        case Expression::Kind::kTagProperty:
        case Expression::Kind::kSrcProperty:
        case Expression::Kind::kEdgeRank:
        case Expression::Kind::kEdgeDst:
        case Expression::Kind::kEdgeSrc:
        case Expression::Kind::kEdgeType:
        case Expression::Kind::kEdgeProperty:
        case Expression::Kind::kInputProperty:
        case Expression::Kind::kSubscript:
        case Expression::Kind::kAttribute:
        case Expression::Kind::kLabelAttribute:
        case Expression::Kind::kVertex:
        case Expression::Kind::kEdge:
        case Expression::Kind::kLabel:
        case Expression::Kind::kVarProperty:
        case Expression::Kind::kDstProperty:
        case Expression::Kind::kUUID:
        case Expression::Kind::kPathBuild:
        case Expression::Kind::kColumn:
        case Expression::Kind::kESQUERY:
        case Expression::Kind::kAggregate:
        case Expression::Kind::kSubscriptRange:
        case Expression::Kind::kVersionedVar:
        default:
          break;
      }
      size -= 1;
    }
    depth += 1;
    if (depth > FLAGS_max_expression_depth) {
      return false;
    }
  }
  return true;
}

/*static*/ bool ExpressionUtils::isVidPredication(const Expression *expr, QueryContext *qctx) {
  if (DCHECK_NOTNULL(expr)->kind() != Expression::Kind::kRelIn &&
      expr->kind() != Expression::Kind::kRelEQ) {
    return false;
  }
  const auto *relExpr = static_cast<const RelationalExpression *>(expr);
  if (relExpr->left()->kind() != Expression::Kind::kFunctionCall) {
    return false;
  }
  const auto *fCallExpr = static_cast<const FunctionCallExpression *>(relExpr->left());
  if (fCallExpr->name() != "id" || fCallExpr->args()->numArgs() != 1 ||
      fCallExpr->args()->args().front()->kind() != Expression::Kind::kLabel) {
    return false;
  }
  if (expr->kind() == Expression::Kind::kRelIn) {
    // id(V) IN [List]
    if (!ExpressionUtils::isEvaluableExpr(relExpr->right(), qctx)) {
      return false;
    }

    auto rightListValue = const_cast<Expression *>(relExpr->right())
                              ->eval(graph::QueryExpressionContext(qctx->ectx())());
    if (!rightListValue.isList()) {
      return false;
    }
  } else if (expr->kind() == Expression::Kind::kRelEQ) {
    // id(V) = Value
    if (relExpr->right()->kind() != Expression::Kind::kConstant) {
      return false;
    }
  }
  return true;
}

/*static*/
bool ExpressionUtils::isOneStepEdgeProp(const std::string &edgeAlias, const Expression *expr) {
  if (expr->kind() != Expression::Kind::kAttribute) {
    return false;
  }
  auto attributeExpr = static_cast<const AttributeExpression *>(expr);
  auto *left = attributeExpr->left();
  auto *right = attributeExpr->right();

  if (left->kind() != Expression::Kind::kSubscript) return false;
  if (right->kind() != Expression::Kind::kConstant ||
      !static_cast<const ConstantExpression *>(right)->value().isStr())
    return false;

  auto subscriptExpr = static_cast<const SubscriptExpression *>(left);
  auto *listExpr = subscriptExpr->left();
  auto *idxExpr = subscriptExpr->right();
  if (listExpr->kind() != Expression::Kind::kInputProperty &&
      listExpr->kind() != Expression::Kind::kVarProperty) {
    return false;
  }
  if (static_cast<const PropertyExpression *>(listExpr)->prop() != edgeAlias) {
    return false;
  }

  // NOTE(jie): Just handled `$-.e[0].likeness` for now, whileas the traverse is single length
  // expand.
  // TODO(jie): Handle `ALL(i IN e WHERE i.likeness > 78)`, whileas the traverse is var len
  // expand.
  if (idxExpr->kind() != Expression::Kind::kConstant ||
      static_cast<const ConstantExpression *>(idxExpr)->value() != 0) {
    return false;
  }
  return true;
}

// Transform expression `$-.e[0].likeness` to EdgePropertyExpression `like.likeness`
// for more friendly to push down
// \param pool object pool to hold ownership of objects alloacted
// \param edgeAlias the name of edge. e.g. e in pattern -[e]->
// \param expr the filter expression
/*static*/ Expression *ExpressionUtils::rewriteEdgePropertyFilter(ObjectPool *pool,
                                                                  const std::string &edgeAlias,
                                                                  Expression *expr) {
  graph::RewriteVisitor::Matcher matcher = [&edgeAlias](const Expression *e) -> bool {
    return isOneStepEdgeProp(edgeAlias, e);
  };
  graph::RewriteVisitor::Rewriter rewriter = [pool](const Expression *e) -> Expression * {
    DCHECK_EQ(e->kind(), Expression::Kind::kAttribute);
    auto attributeExpr = static_cast<const AttributeExpression *>(e);
    auto *right = attributeExpr->right();
    DCHECK_EQ(right->kind(), Expression::Kind::kConstant);

    auto &prop = static_cast<const ConstantExpression *>(right)->value().getStr();

    auto *edgePropExpr = EdgePropertyExpression::make(pool, "*", prop);
    return edgePropExpr;
  };

  return graph::RewriteVisitor::transform(expr, matcher, rewriter);
}

// Transform Label Tag property expression like $-.v.player.name to Tag property like player.name
// for more friendly to push down
// \param pool object pool to hold ownership of objects alloacted
// \param node the name of node, i.e. v in pattern (v)
// \param expr the filter expression
/*static*/ Expression *ExpressionUtils::rewriteVertexPropertyFilter(ObjectPool *pool,
                                                                    const std::string &node,
                                                                    Expression *expr) {
  graph::RewriteVisitor::Matcher matcher = [&node](const Expression *e) -> bool {
    if (e->kind() != Expression::Kind::kLabelTagProperty) {
      return false;
    }
    auto *ltpExpr = static_cast<const LabelTagPropertyExpression *>(e);
    auto *labelExpr = ltpExpr->label();
    DCHECK(labelExpr->kind() == Expression::Kind::kInputProperty ||
           labelExpr->kind() == Expression::Kind::kVarProperty);
    if (labelExpr->kind() != Expression::Kind::kInputProperty &&
        labelExpr->kind() != Expression::Kind::kVarProperty) {
      return false;
    }
    auto *inputExpr = static_cast<const PropertyExpression *>(labelExpr);
    if (inputExpr->prop() != node) {
      return false;
    }
    return true;
  };
  graph::RewriteVisitor::Rewriter rewriter = [pool](const Expression *e) -> Expression * {
    DCHECK_EQ(e->kind(), Expression::Kind::kLabelTagProperty);
    auto *ltpExpr = static_cast<const LabelTagPropertyExpression *>(e);
    auto *tagPropExpr = TagPropertyExpression::make(pool, ltpExpr->sym(), ltpExpr->prop());
    return tagPropExpr;
  };
  return graph::RewriteVisitor::transform(expr, matcher, rewriter);
}

}  // namespace graph
}  // namespace nebula
