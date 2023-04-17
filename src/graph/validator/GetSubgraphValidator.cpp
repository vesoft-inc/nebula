/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/GetSubgraphValidator.h"

#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/ValidateUtil.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {

Status GetSubgraphValidator::validateImpl() {
  auto* gsSentence = static_cast<GetSubgraphSentence*>(sentence_);
  subgraphCtx_ = getContext<SubgraphContext>();
  subgraphCtx_->withProp = gsSentence->withProp();

  NG_RETURN_IF_ERROR(ValidateUtil::validateStep(gsSentence->step(), subgraphCtx_->steps));
  NG_RETURN_IF_ERROR(validateStarts(gsSentence->from(), subgraphCtx_->from));
  NG_RETURN_IF_ERROR(validateInBound(gsSentence->in()));
  NG_RETURN_IF_ERROR(validateOutBound(gsSentence->out()));
  NG_RETURN_IF_ERROR(validateBothInOutBound(gsSentence->both()));
  NG_RETURN_IF_ERROR(validateWhere(gsSentence->where()));
  NG_RETURN_IF_ERROR(validateYield(gsSentence->yield()));
  return Status::OK();
}

// Validate in-bound edge types
Status GetSubgraphValidator::validateInBound(InBoundClause* in) {
  auto& edgeTypes = subgraphCtx_->edgeTypes;
  auto& edgeNames = subgraphCtx_->edgeNames;
  if (in != nullptr) {
    auto space = vctx_->whichSpace();
    auto edges = in->edges();
    edgeTypes.reserve(edgeTypes.size() + edges.size());
    for (auto* e : edges) {
      if (e->alias() != nullptr) {
        return Status::SemanticError("Get Subgraph not support rename edge name.");
      }

      std::string edgeName = *e->edge();
      auto et = qctx_->schemaMng()->toEdgeType(space.id, edgeName);
      NG_RETURN_IF_ERROR(et);
      edgeNames.emplace(edgeName);

      auto v = -et.value();
      edgeTypes.emplace(v);
    }
  }

  return Status::OK();
}

// Validate out-bound edge types
Status GetSubgraphValidator::validateOutBound(OutBoundClause* out) {
  auto& edgeTypes = subgraphCtx_->edgeTypes;
  auto& edgeNames = subgraphCtx_->edgeNames;
  if (out != nullptr) {
    auto space = vctx_->whichSpace();
    auto edges = out->edges();
    edgeTypes.reserve(edgeTypes.size() + edges.size());
    for (auto* e : out->edges()) {
      if (e->alias() != nullptr) {
        return Status::SemanticError("Get Subgraph not support rename edge name.");
      }
      std::string edgeName = *e->edge();
      auto et = qctx_->schemaMng()->toEdgeType(space.id, edgeName);
      NG_RETURN_IF_ERROR(et);
      edgeNames.emplace(edgeName);
      edgeTypes.emplace(et.value());
    }
  }

  return Status::OK();
}

// Validate bidirectional(in-bound and out-bound) edge types
Status GetSubgraphValidator::validateBothInOutBound(BothInOutClause* out) {
  auto& edgeTypes = subgraphCtx_->edgeTypes;
  auto& biEdgeTypes = subgraphCtx_->biDirectEdgeTypes;
  auto& edgeNames = subgraphCtx_->edgeNames;
  if (out != nullptr) {
    auto& space = vctx_->whichSpace();
    auto edges = out->edges();
    edgeTypes.reserve(edgeTypes.size() + edges.size() * 2);
    biEdgeTypes.reserve(edges.size() * 2);
    for (auto* e : out->edges()) {
      if (e->alias() != nullptr) {
        return Status::SemanticError("Get Subgraph not support rename edge name.");
      }
      std::string edgeName = *e->edge();
      auto et = qctx_->schemaMng()->toEdgeType(space.id, edgeName);
      NG_RETURN_IF_ERROR(et);
      edgeNames.emplace(edgeName);
      auto v = et.value();
      edgeTypes.emplace(v);
      edgeTypes.emplace(-v);
      biEdgeTypes.emplace(v);
      biEdgeTypes.emplace(-v);
    }
  }
  return Status::OK();
}

static Expression* rewriteDstProp2SrcProp(const Expression* expr) {
  ObjectPool* pool = expr->getObjPool();
  auto matcher = [](const Expression* e) -> bool {
    return e->kind() == Expression::Kind::kDstProperty;
  };
  auto rewriter = [pool](const Expression* e) -> Expression* {
    auto dstExpr = static_cast<const DestPropertyExpression*>(e);
    return SourcePropertyExpression::make(pool, dstExpr->sym(), dstExpr->prop());
  };

  return RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

// Check validity of filter expression, rewrites expression to fit its sementic,
// disable some invalid expression types, collect properties used in filter.
Status GetSubgraphValidator::validateWhere(WhereClause* where) {
  if (where == nullptr) {
    return Status::OK();
  }
  auto* filterExpr = where->filter();
  if (ExpressionUtils::findAny(filterExpr,
                               {Expression::Kind::kAggregate,
                                Expression::Kind::kSrcProperty,
                                Expression::Kind::kVarProperty,
                                Expression::Kind::kInputProperty,
                                Expression::Kind::kLogicalOr})) {
    return Status::SemanticError("Not support `%s' in where sentence.",
                                 filterExpr->toString().c_str());
  }

  auto undefinedParams = graph::ExpressionUtils::ExtractInnerVars(filterExpr, qctx_);
  if (!undefinedParams.empty()) {
    return Status::SemanticError(
        "Undefined parameters: " +
        std::accumulate(++undefinedParams.begin(),
                        undefinedParams.end(),
                        *undefinedParams.begin(),
                        [](auto& lhs, auto& rhs) { return lhs + ", " + rhs; }));
  }
  auto* newFilter = graph::ExpressionUtils::rewriteParameter(filterExpr, qctx_);

  where->setFilter(ExpressionUtils::rewriteLabelAttr2EdgeProp(newFilter));
  auto filter = where->filter();
  auto typeStatus = deduceExprType(filter);
  NG_RETURN_IF_ERROR(typeStatus);
  auto type = typeStatus.value();
  if (type != Value::Type::BOOL && type != Value::Type::NULLVALUE &&
      type != Value::Type::__EMPTY__) {
    std::stringstream ss;
    ss << "`" << filter->toString() << "', expected Boolean, "
       << "but was `" << type << "'";
    return Status::SemanticError(ss.str());
  }

  NG_RETURN_IF_ERROR(deduceProps(filter, subgraphCtx_->exprProps));

  // check EdgeFilter's edge type is in the edge type list
  // e.g. "like" is not in the edge list ["serve"]
  // GET SUBGRAPH FROM 'xxx' both serve WHERE like.likeness < 90 YIELD vertices as v, edges as e
  if (!subgraphCtx_->edgeNames.empty()) {
    for (auto edgeProp : subgraphCtx_->exprProps.edgeProps()) {
      auto filterEdgeName = qctx_->schemaMng()->toEdgeName(vctx_->whichSpace().id, edgeProp.first);
      NG_RETURN_IF_ERROR(filterEdgeName);
      if (subgraphCtx_->edgeNames.find(filterEdgeName.value()) == subgraphCtx_->edgeNames.end()) {
        return Status::SemanticError(
            fmt::format("Edge type \"{}\" in filter \"{}\" is not in the edge types [{}]",
                        filterEdgeName.value(),
                        filter->toString(),
                        folly::join(",", subgraphCtx_->edgeNames)));
      }
    }
  }

  auto condition = filter->clone();
  if (ExpressionUtils::findAny(filter, {Expression::Kind::kDstProperty})) {
    auto visitor = ExtractFilterExprVisitor::makePushGetVertices(qctx_->objPool());
    filter->accept(&visitor);
    if (!visitor.ok()) {
      return Status::SemanticError("Push target vertices filter error: " + filter->toString());
    }
    subgraphCtx_->edgeFilter = visitor.remainedExpr();
    auto tagFilter = visitor.extractedExpr() ? visitor.extractedExpr() : filter;
    subgraphCtx_->tagFilter = rewriteDstProp2SrcProp(tagFilter);
  } else {
    subgraphCtx_->edgeFilter = condition;
  }
  subgraphCtx_->filter = rewriteDstProp2SrcProp(condition);
  return Status::OK();
}

// Validate yield clause, which only supports YIELD vertices or edges
Status GetSubgraphValidator::validateYield(YieldClause* yield) {
  if (yield == nullptr) {
    return Status::SemanticError("Missing yield clause.");
  }
  auto size = yield->columns().size();
  outputs_.reserve(size);
  std::vector<Value::Type> colType;
  for (const auto& col : yield->columns()) {
    const std::string& colStr = col->expr()->toString();
    if (colStr == "VERTICES") {
      subgraphCtx_->getVertexProp = true;
      colType.emplace_back(Value::Type::VERTEX);
    } else if (colStr == "EDGES") {
      if (subgraphCtx_->steps.steps() == 0) {
        return Status::SemanticError("Get Subgraph 0 STEPS only support YIELD vertices");
      }
      subgraphCtx_->getEdgeProp = true;
      colType.emplace_back(Value::Type::EDGE);
    } else {
      return Status::SemanticError("Get Subgraph only support YIELD vertices OR edges");
    }
    outputs_.emplace_back(col->name(), Value::Type::LIST);
  }
  subgraphCtx_->colNames = getOutColNames();
  subgraphCtx_->colType = std::move(colType);
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
