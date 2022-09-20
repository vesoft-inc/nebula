/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/LookupValidator.h"

#include "common/base/Status.h"
#include "common/meta/NebulaSchemaProvider.h"
#include "graph/context/ast/QueryAstContext.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/FTIndexUtils.h"
#include "graph/util/SchemaUtil.h"
#include "graph/util/ValidateUtil.h"
#include "parser/TraverseSentences.h"

using nebula::meta::NebulaSchemaProvider;
using std::shared_ptr;
using std::unique_ptr;

using ExprKind = nebula::Expression::Kind;

namespace nebula {
namespace graph {

LookupValidator::LookupValidator(Sentence* sentence, QueryContext* context)
    : Validator(sentence, context) {}

const LookupSentence* LookupValidator::sentence() const {
  return static_cast<LookupSentence*>(sentence_);
}

int32_t LookupValidator::schemaId() const {
  return DCHECK_NOTNULL(lookupCtx_)->schemaId;
}

GraphSpaceID LookupValidator::spaceId() const {
  return DCHECK_NOTNULL(lookupCtx_)->space.id;
}

AstContext* LookupValidator::getAstContext() {
  return lookupCtx_.get();
}

Status LookupValidator::validateImpl() {
  lookupCtx_ = getContext<LookupContext>();

  NG_RETURN_IF_ERROR(validateFrom());
  NG_RETURN_IF_ERROR(validateWhere());
  NG_RETURN_IF_ERROR(validateYield());
  return Status::OK();
}

// Validate specified schema(tag or edge) from sentence
Status LookupValidator::validateFrom() {
  auto spaceId = lookupCtx_->space.id;
  auto from = sentence()->from();
  auto ret = qctx_->schemaMng()->getSchemaIDByName(spaceId, from);
  NG_RETURN_IF_ERROR(ret);
  lookupCtx_->isEdge = ret.value().first;
  lookupCtx_->schemaId = ret.value().second;
  schemaIds_.emplace_back(ret.value().second);
  return Status::OK();
}

// Build the return properties and the final output column names
void LookupValidator::extractExprProps() {
  auto addProps = [this](const std::set<folly::StringPiece>& propNames) {
    for (const auto& propName : propNames) {
      auto iter = std::find(idxReturnCols_.begin(), idxReturnCols_.end(), propName);
      if (iter == idxReturnCols_.end()) {
        idxReturnCols_.emplace_back(propName);
      }
    }
  };
  auto from = sentence()->from();
  auto buildColNames = [&from](const std::string& colName) { return from + '.' + colName; };
  if (lookupCtx_->isEdge) {
    for (const auto& edgeProp : exprProps_.edgeProps()) {
      addProps(edgeProp.second);
    }
    std::vector<std::string> idxColNames = idxReturnCols_;
    std::transform(idxColNames.begin(), idxColNames.end(), idxColNames.begin(), buildColNames);
    lookupCtx_->idxColNames = std::move(idxColNames);
  } else {
    for (const auto& tagProp : exprProps_.tagProps()) {
      addProps(tagProp.second);
    }
    std::vector<std::string> idxColNames = idxReturnCols_;
    std::transform(idxColNames.begin(), idxColNames.end(), idxColNames.begin(), buildColNames);
    idxColNames[0] = nebula::kVid;
    lookupCtx_->idxColNames = std::move(idxColNames);
  }
  lookupCtx_->idxReturnCols = std::move(idxReturnCols_);
}

// Validate yield clause when lookup on edge.
// Disable invalid expressions, check schema name, rewrites expression to fit semantic,
// check type and collect properties.
Status LookupValidator::validateYieldEdge() {
  auto yield = sentence()->yieldClause();
  auto yieldExpr = lookupCtx_->yieldExpr;
  for (auto col : yield->columns()) {
    if (ExpressionUtils::hasAny(col->expr(), {Expression::Kind::kVertex})) {
      return Status::SemanticError("illegal yield clauses `%s'", col->toString().c_str());
    }
    if (col->expr()->kind() == Expression::Kind::kLabelAttribute) {
      const auto& schemaName = static_cast<LabelAttributeExpression*>(col->expr())->left()->name();
      if (schemaName != sentence()->from()) {
        return Status::SemanticError("Schema name error: %s", schemaName.c_str());
      }
    }
    col->setExpr(ExpressionUtils::rewriteLabelAttr2EdgeProp(col->expr()));
    NG_RETURN_IF_ERROR(ValidateUtil::invalidLabelIdentifiers(col->expr()));

    auto colExpr = col->expr();
    auto typeStatus = deduceExprType(colExpr);
    NG_RETURN_IF_ERROR(typeStatus);
    outputs_.emplace_back(col->name(), typeStatus.value());
    yieldExpr->addColumn(col->clone().release());
    NG_RETURN_IF_ERROR(deduceProps(colExpr, exprProps_, nullptr, &schemaIds_));
  }
  return Status::OK();
}

// Validate yield clause when lookup on tag.
// Disable invalid expressions, check schema name, rewrites expression to fit semantic,
// check type and collect properties.
Status LookupValidator::validateYieldTag() {
  auto yield = sentence()->yieldClause();
  auto yieldExpr = lookupCtx_->yieldExpr;
  for (auto col : yield->columns()) {
    if (ExpressionUtils::hasAny(col->expr(), {Expression::Kind::kEdge})) {
      return Status::SemanticError("illegal yield clauses `%s'", col->toString().c_str());
    }
    if (col->expr()->kind() == Expression::Kind::kLabelAttribute) {
      const auto& schemaName = static_cast<LabelAttributeExpression*>(col->expr())->left()->name();
      if (schemaName != sentence()->from()) {
        return Status::SemanticError("Schema name error: %s", schemaName.c_str());
      }
    }
    col->setExpr(ExpressionUtils::rewriteLabelAttr2TagProp(col->expr()));
    NG_RETURN_IF_ERROR(ValidateUtil::invalidLabelIdentifiers(col->expr()));

    auto colExpr = col->expr();
    auto typeStatus = deduceExprType(colExpr);
    NG_RETURN_IF_ERROR(typeStatus);
    outputs_.emplace_back(col->name(), typeStatus.value());
    yieldExpr->addColumn(col->clone().release());
    NG_RETURN_IF_ERROR(deduceProps(colExpr, exprProps_, &schemaIds_));
  }
  return Status::OK();
}

// Validate yield clause.
Status LookupValidator::validateYield() {
  auto yieldClause = sentence()->yieldClause();
  if (yieldClause == nullptr) {
    return Status::SemanticError("Missing yield clause.");
  }
  lookupCtx_->dedup = yieldClause->isDistinct();
  lookupCtx_->yieldExpr = qctx_->objPool()->makeAndAdd<YieldColumns>();

  if (lookupCtx_->isEdge) {
    idxReturnCols_.emplace_back(nebula::kSrc);
    idxReturnCols_.emplace_back(nebula::kDst);
    idxReturnCols_.emplace_back(nebula::kRank);
    idxReturnCols_.emplace_back(nebula::kType);
    NG_RETURN_IF_ERROR(validateYieldEdge());
  } else {
    idxReturnCols_.emplace_back(nebula::kVid);
    NG_RETURN_IF_ERROR(validateYieldTag());
  }
  if (exprProps_.hasInputVarProperty()) {
    return Status::SemanticError("unsupport input/variable property expression in yield.");
  }
  if (exprProps_.hasSrcDstTagProperty()) {
    return Status::SemanticError("unsupport src/dst property expression in yield.");
  }
  extractExprProps();
  return Status::OK();
}

// Validate filter expression.
// Check text search filter or normal filter, collect properties in filter.
Status LookupValidator::validateWhere() {
  auto whereClause = sentence()->whereClause();
  if (whereClause == nullptr) {
    return Status::OK();
  }

  auto* filter = whereClause->filter();
  if (FTIndexUtils::needTextSearch(filter)) {
    auto retFilter = genTsFilter(filter);
    NG_RETURN_IF_ERROR(retFilter);
    auto filterExpr = std::move(retFilter).value();
    if (filterExpr == nullptr) {
      // return empty result direct.
      lookupCtx_->isEmptyResultSet = true;
      return Status::OK();
    }
    lookupCtx_->filter = filterExpr;
  } else {
    auto ret = checkFilter(filter);
    NG_RETURN_IF_ERROR(ret);
    lookupCtx_->filter = std::move(ret).value();
    // Make sure the type of the rewritten filter expr is right
    NG_RETURN_IF_ERROR(deduceExprType(lookupCtx_->filter));
  }
  if (lookupCtx_->isEdge) {
    NG_RETURN_IF_ERROR(deduceProps(lookupCtx_->filter, exprProps_, nullptr, &schemaIds_));
  } else {
    NG_RETURN_IF_ERROR(deduceProps(lookupCtx_->filter, exprProps_, &schemaIds_));
  }
  return Status::OK();
}

StatusOr<Expression*> LookupValidator::handleLogicalExprOperands(LogicalExpression* lExpr) {
  auto& operands = lExpr->operands();
  for (auto i = 0u; i < operands.size(); i++) {
    auto operand = lExpr->operand(i);
    auto ret = checkFilter(operand);
    NG_RETURN_IF_ERROR(ret);

    auto newOperand = ret.value();
    if (operand != newOperand) {
      lExpr->setOperand(i, newOperand);
    }
  }
  return lExpr;
}

// Check could filter expression convert to Geo/Index Search.
StatusOr<Expression*> LookupValidator::checkFilter(Expression* expr) {
  // TODO: Support IN expression push down
  if (ExpressionUtils::isGeoIndexAcceleratedPredicate(expr)) {
    NG_RETURN_IF_ERROR(checkGeoPredicate(expr));
    return rewriteGeoPredicate(expr);
  } else if (expr->isRelExpr()) {
    // Only starts with can be pushed down as a range scan, so forbid other string-related relExpr
    if (expr->kind() == ExprKind::kRelREG || expr->kind() == ExprKind::kContains ||
        expr->kind() == ExprKind::kNotContains || expr->kind() == ExprKind::kEndsWith ||
        expr->kind() == ExprKind::kNotStartsWith || expr->kind() == ExprKind::kNotEndsWith) {
      return Status::SemanticError(
          "Expression %s is not supported, please use full-text index as an optimal solution",
          expr->toString().c_str());
    }

    auto relExpr = static_cast<RelationalExpression*>(expr);
    NG_RETURN_IF_ERROR(checkRelExpr(relExpr));
    return rewriteRelExpr(relExpr);
  }
  switch (expr->kind()) {
    case ExprKind::kLogicalOr: {
      ExpressionUtils::pullOrs(expr);
      return handleLogicalExprOperands(static_cast<LogicalExpression*>(expr));
    }
    case ExprKind::kLogicalAnd: {
      ExpressionUtils::pullAnds(expr);
      return handleLogicalExprOperands(static_cast<LogicalExpression*>(expr));
    }
    default: {
      return Status::SemanticError("Expression %s not supported yet", expr->toString().c_str());
    }
  }
}

// Check whether relational expression could convert to Index Search.
Status LookupValidator::checkRelExpr(RelationalExpression* expr) {
  auto* left = expr->left();
  auto* right = expr->right();
  // Does not support filter : schema.col1 > schema.col2
  if (left->kind() == ExprKind::kLabelAttribute && right->kind() == ExprKind::kLabelAttribute) {
    return Status::SemanticError("Expression %s not supported yet", expr->toString().c_str());
  }
  if (left->kind() == ExprKind::kLabelAttribute || right->kind() == ExprKind::kLabelAttribute) {
    return Status::OK();
  }

  return Status::SemanticError("Expression %s not supported yet", expr->toString().c_str());
}
// Check whether geo predicate expression could convert to Geo Index Search.
Status LookupValidator::checkGeoPredicate(const Expression* expr) const {
  auto checkFunc = [](const FunctionCallExpression* funcExpr) -> Status {
    if (funcExpr->args()->numArgs() < 2) {
      return Status::SemanticError("Expression %s has not enough arguments",
                                   funcExpr->toString().c_str());
    }
    auto* first = funcExpr->args()->args()[0];
    auto* second = funcExpr->args()->args()[1];

    if (first->kind() == ExprKind::kLabelAttribute && second->kind() == ExprKind::kLabelAttribute) {
      return Status::SemanticError("Expression %s not supported yet", funcExpr->toString().c_str());
    }
    if (first->kind() == ExprKind::kLabelAttribute || second->kind() == ExprKind::kLabelAttribute) {
      return Status::OK();
    }

    return Status::SemanticError("Expression %s not supported yet", funcExpr->toString().c_str());
  };
  if (expr->isRelExpr()) {
    auto* relExpr = static_cast<const RelationalExpression*>(expr);
    auto* left = relExpr->left();
    auto* right = relExpr->right();
    if (left->kind() == Expression::Kind::kFunctionCall) {
      NG_RETURN_IF_ERROR(checkFunc(static_cast<const FunctionCallExpression*>(left)));
    }
    if (right->kind() == Expression::Kind::kFunctionCall) {
      NG_RETURN_IF_ERROR(checkFunc(static_cast<const FunctionCallExpression*>(right)));
    }
  } else if (expr->kind() == Expression::Kind::kFunctionCall) {
    NG_RETURN_IF_ERROR(checkFunc(static_cast<const FunctionCallExpression*>(expr)));
  } else {
    return Status::SemanticError("Expression %s not supported yet", expr->toString().c_str());
  }

  return Status::OK();
}

// Rewrite relational expression.
// Put property expression to left, check schema validity, fold expression,
// rewrite attribute expression to fit semantic.
StatusOr<Expression*> LookupValidator::rewriteRelExpr(RelationalExpression* expr) {
  // swap LHS and RHS of relExpr if LabelAttributeExpr in on the right,
  // so that LabelAttributeExpr is always on the left
  auto rightOperand = expr->right();
  if (rightOperand->kind() == ExprKind::kLabelAttribute) {
    expr = static_cast<RelationalExpression*>(reverseRelKind(expr));
  }

  auto leftOperand = expr->left();
  auto* la = static_cast<LabelAttributeExpression*>(leftOperand);
  if (la->left()->name() != sentence()->from()) {
    return Status::SemanticError("Schema name error: %s", la->left()->name().c_str());
  }

  // fold constant expression
  auto foldRes = ExpressionUtils::foldConstantExpr(expr);
  NG_RETURN_IF_ERROR(foldRes);
  expr = static_cast<RelationalExpression*>(foldRes.value());
  DCHECK_EQ(expr->left()->kind(), ExprKind::kLabelAttribute);

  // Check schema and value type
  std::string prop = la->right()->value().getStr();
  auto relExprType = expr->kind();
  auto c = checkConstExpr(expr->right(), prop, relExprType);
  NG_RETURN_IF_ERROR(c);
  expr->setRight(std::move(c).value());

  // rewrite PropertyExpression
  auto propExpr = lookupCtx_->isEdge ? ExpressionUtils::rewriteLabelAttr2EdgeProp(la)
                                     : ExpressionUtils::rewriteLabelAttr2TagProp(la);
  expr->setLeft(propExpr);
  return expr;
}

// Rewrite expression of geo search.
// Put geo expression to left, check validity of geo search, check schema validity, fold expression,
// rewrite attribute expression to fit semantic.
StatusOr<Expression*> LookupValidator::rewriteGeoPredicate(Expression* expr) {
  // swap LHS and RHS of relExpr if LabelAttributeExpr in on the right,
  // so that LabelAttributeExpr is always on the left
  if (expr->isRelExpr()) {
    if (static_cast<RelationalExpression*>(expr)->right()->kind() ==
        Expression::Kind::kFunctionCall) {
      expr = reverseGeoPredicate(expr);
    }
    auto* relExpr = static_cast<RelationalExpression*>(expr);
    auto* left = relExpr->left();
    auto* right = relExpr->right();
    DCHECK(left->kind() == Expression::Kind::kFunctionCall);
    auto* funcExpr = static_cast<FunctionCallExpression*>(left);
    DCHECK_EQ(boost::to_lower_copy(funcExpr->name()), "st_distance");
    // `ST_Distance(g1, g1) < distanceInMeters` is equivalent to `ST_DWithin(g1, g2,
    // distanceInMeters, true), `ST_Distance(g1, g1) <= distanceInMeters` is equivalent to
    // `ST_DWithin(g1, g2, distanceInMeters, false)
    if (relExpr->kind() == Expression::Kind::kRelLT ||
        relExpr->kind() == Expression::Kind::kRelLE) {
      auto* newArgList = ArgumentList::make(qctx_->objPool(), 3);
      if (funcExpr->args()->numArgs() != 2) {
        return Status::SemanticError("Expression %s has wrong number arguments",
                                     funcExpr->toString().c_str());
      }
      newArgList->addArgument(funcExpr->args()->args()[0]->clone());
      newArgList->addArgument(funcExpr->args()->args()[1]->clone());
      newArgList->addArgument(right->clone());  // distanceInMeters
      bool exclusive = relExpr->kind() == Expression::Kind::kRelLT;
      newArgList->addArgument(ConstantExpression::make(qctx_->objPool(), exclusive));
      expr = FunctionCallExpression::make(qctx_->objPool(), "st_dwithin", newArgList);
    } else {
      return Status::SemanticError("Expression %s not supported yet", expr->toString().c_str());
    }
  }

  DCHECK(expr->kind() == Expression::Kind::kFunctionCall);
  if (static_cast<FunctionCallExpression*>(expr)->args()->args()[1]->kind() ==
      ExprKind::kLabelAttribute) {
    expr = reverseGeoPredicate(expr);
  }

  auto* geoFuncExpr = static_cast<FunctionCallExpression*>(expr);
  auto* first = geoFuncExpr->args()->args()[0];
  auto* la = static_cast<LabelAttributeExpression*>(first);
  if (la->left()->name() != sentence()->from()) {
    return Status::SemanticError("Schema name error: %s", la->left()->name().c_str());
  }

  // fold constant expression
  auto foldRes = ExpressionUtils::foldConstantExpr(expr);
  NG_RETURN_IF_ERROR(foldRes);
  geoFuncExpr = static_cast<FunctionCallExpression*>(foldRes.value());

  // Check schema and value type
  std::string prop = la->right()->value().getStr();
  auto c = checkConstExpr(geoFuncExpr->args()->args()[1], prop, Expression::Kind::kFunctionCall);
  NG_RETURN_IF_ERROR(c);
  geoFuncExpr->args()->setArg(1, std::move(c).value());

  // rewrite PropertyExpression
  auto propExpr = lookupCtx_->isEdge ? ExpressionUtils::rewriteLabelAttr2EdgeProp(la)
                                     : ExpressionUtils::rewriteLabelAttr2TagProp(la);
  geoFuncExpr->args()->setArg(0, propExpr);
  return geoFuncExpr;
}

// Check does constant expression could compare to given property.
// \param expr constant expression
// \param prop property name
// \param kind relational expression kind
StatusOr<Expression*> LookupValidator::checkConstExpr(Expression* expr,
                                                      const std::string& prop,
                                                      const ExprKind kind) {
  auto* pool = expr->getObjPool();
  if (!ExpressionUtils::isEvaluableExpr(expr, qctx_)) {
    return Status::SemanticError("'%s' is not an evaluable expression.", expr->toString().c_str());
  }
  auto schemaMgr = qctx_->schemaMng();
  auto schema = lookupCtx_->isEdge ? schemaMgr->getEdgeSchema(spaceId(), schemaId())
                                   : schemaMgr->getTagSchema(spaceId(), schemaId());
  auto type = schema->getFieldType(prop);
  if (type == nebula::cpp2::PropertyType::UNKNOWN) {
    return Status::SemanticError("Invalid column: %s", prop.c_str());
  }
  auto v = Expression::eval(expr, QueryExpressionContext(qctx_->ectx())());
  // TODO(Aiee) extract the type cast logic as a method if we decide to support
  // more cross-type comparisons.

  // Allow different numeric type to compare
  if (graph::SchemaUtil::propTypeToValueType(type) == Value::Type::FLOAT && v.isInt()) {
    return ConstantExpression::make(pool, v.toFloat());
  } else if (graph::SchemaUtil::propTypeToValueType(type) == Value::Type::INT && v.isFloat()) {
    // col1 < 10.5 range: [min, 11), col1 < 10 range: [min, 10)
    double f = v.getFloat();
    int iCeil = ceil(f);
    int iFloor = floor(f);
    if (kind == ExprKind::kRelGE || kind == ExprKind::kRelLT) {
      // edge case col1 >= 40.0, no need to round up
      if (std::abs(f - iCeil) < kEpsilon) {
        return ConstantExpression::make(pool, iFloor);
      }
      return ConstantExpression::make(pool, iCeil);
    }
    return ConstantExpression::make(pool, iFloor);
  }

  // Check prop type
  if (v.type() != SchemaUtil::propTypeToValueType(type)) {
    // allow different types in the IN expression, such as "abc" IN ["abc"]
    if (!expr->isContainerExpr()) {
      return Status::SemanticError("Column type error : %s", prop.c_str());
    }
  }
  return expr;
}

// Check does test search contains properties search in test search expression
StatusOr<std::string> LookupValidator::checkTSExpr(Expression* expr) {
  auto metaClient = qctx_->getMetaClient();
  auto tsi = metaClient->getFTIndexBySpaceSchemaFromCache(spaceId(), schemaId());
  NG_RETURN_IF_ERROR(tsi);
  auto tsName = tsi.value().first;
  auto ret = FTIndexUtils::checkTSIndex(tsClients_, tsName);
  NG_RETURN_IF_ERROR(ret);
  if (!ret.value()) {
    return Status::SemanticError("text search index not found : %s", tsName.c_str());
  }
  auto ftFields = tsi.value().second.get_fields();
  auto tsExpr = static_cast<TextSearchExpression*>(expr);
  auto prop = tsExpr->arg()->prop();

  auto iter = std::find(ftFields.begin(), ftFields.end(), prop);
  if (iter == ftFields.end()) {
    return Status::SemanticError("Column %s not found in %s", prop.c_str(), tsName.c_str());
  }
  return tsName;
}

// Reverse position of operands in relational expression and keep the origin semantic.
// Transform (A > B) to (B < A)
Expression* LookupValidator::reverseRelKind(RelationalExpression* expr) {
  auto kind = expr->kind();
  auto reversedKind = kind;

  switch (kind) {
    case ExprKind::kRelEQ:
      break;
    case ExprKind::kRelNE:
      break;
    case ExprKind::kRelLT:
      reversedKind = ExprKind::kRelGT;
      break;
    case ExprKind::kRelLE:
      reversedKind = ExprKind::kRelGE;
      break;
    case ExprKind::kRelGT:
      reversedKind = ExprKind::kRelLT;
      break;
    case ExprKind::kRelGE:
      reversedKind = ExprKind::kRelLE;
      break;
    default:
      LOG(FATAL) << "Invalid relational expression kind: " << static_cast<uint8_t>(kind);
      break;
  }

  auto left = expr->left();
  auto right = expr->right();
  auto* pool = qctx_->objPool();
  return RelationalExpression::makeKind(pool, reversedKind, right->clone(), left->clone());
}

// reverse geo predicates operands and keep the origin semantic
Expression* LookupValidator::reverseGeoPredicate(Expression* expr) {
  if (expr->isRelExpr()) {
    auto* relExpr = static_cast<RelationalExpression*>(expr);
    return reverseRelKind(relExpr);
  } else {
    DCHECK(expr->kind() == Expression::Kind::kFunctionCall);
    auto* funcExpr = static_cast<const FunctionCallExpression*>(expr);
    auto name = funcExpr->name();
    folly::toLowerAscii(name);
    auto newName = name;

    if (name == "st_covers") {
      newName = "st_coveredby";
    } else if (name == "st_coveredby") {
      newName = "st_covers";
    } else if (name == "st_intersects") {
    } else if (name == "st_dwithin") {
    }

    auto* newArgList = ArgumentList::make(qctx_->objPool(), funcExpr->args()->numArgs());
    for (auto& arg : funcExpr->args()->args()) {
      newArgList->addArgument(arg->clone());
    }
    newArgList->setArg(0, funcExpr->args()->args()[1]);
    newArgList->setArg(1, funcExpr->args()->args()[0]);
    return FunctionCallExpression::make(qctx_->objPool(), newName, newArgList);
  }
}

// Get schema info by schema name in sentence
// \param provider output schema info
Status LookupValidator::getSchemaProvider(shared_ptr<const NebulaSchemaProvider>* provider) const {
  auto from = sentence()->from();
  auto schemaMgr = qctx_->schemaMng();
  if (lookupCtx_->isEdge) {
    *provider = schemaMgr->getEdgeSchema(spaceId(), schemaId());
    if (*provider == nullptr) {
      return Status::EdgeNotFound("Edge schema not found : %s", from.c_str());
    }
  } else {
    *provider = schemaMgr->getTagSchema(spaceId(), schemaId());
    if (*provider == nullptr) {
      return Status::TagNotFound("Tag schema not found : %s", from.c_str());
    }
  }
  return Status::OK();
}

// Generate text search filter, check validity and rewrite
StatusOr<Expression*> LookupValidator::genTsFilter(Expression* filter) {
  auto tsRet = FTIndexUtils::getTSClients(qctx_->getMetaClient());
  NG_RETURN_IF_ERROR(tsRet);
  tsClients_ = std::move(tsRet).value();
  auto tsIndex = checkTSExpr(filter);
  NG_RETURN_IF_ERROR(tsIndex);
  return FTIndexUtils::rewriteTSFilter(
      qctx_->objPool(), lookupCtx_->isEdge, filter, tsIndex.value(), tsClients_);
}

}  // namespace graph
}  // namespace nebula
