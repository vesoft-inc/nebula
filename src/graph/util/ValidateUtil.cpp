/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/util/ValidateUtil.h"

#include <ostream>      // for operator<<, stringstream
#include <string>       // for string, basic_string
#include <type_traits>  // for remove_reference<>::type
#include <utility>      // for move
#include <vector>       // for vector

#include "common/base/StatusOr.h"               // for StatusOr
#include "common/expression/Expression.h"       // for Expression::Kind, Exp...
#include "common/meta/SchemaManager.h"          // for SchemaManager
#include "common/thrift/ThriftTypes.h"          // for EdgeType
#include "graph/context/QueryContext.h"         // for QueryContext
#include "graph/context/ValidateContext.h"      // for ValidateContext
#include "graph/context/ast/QueryAstContext.h"  // for Over
#include "graph/session/ClientSession.h"        // for SpaceInfo
#include "graph/util/ExpressionUtils.h"         // for ExpressionUtils
#include "parser/Clauses.h"                     // for StepClause, OverClause

namespace nebula {
namespace graph {

Status ValidateUtil::validateStep(const StepClause* clause, StepClause& step) {
  if (clause == nullptr) {
    return Status::SemanticError("Step clause nullptr.");
  }
  step = *clause;
  if (clause->isMToN()) {
    if (step.mSteps() == 0) {
      step.setMSteps(1);
    }
    if (step.nSteps() < step.mSteps()) {
      return Status::SemanticError("`%s', upper bound steps should be greater than lower bound.",
                                   step.toString().c_str());
    }
  }
  return Status::OK();
}

Status ValidateUtil::validateOver(QueryContext* qctx, const OverClause* clause, Over& over) {
  if (clause == nullptr) {
    return Status::SemanticError("Over clause nullptr.");
  }
  auto space = qctx->vctx()->whichSpace();

  over.direction = clause->direction();
  auto* schemaMng = qctx->schemaMng();
  if (clause->isOverAll()) {
    auto edgeStatus = schemaMng->getAllEdge(space.id);
    NG_RETURN_IF_ERROR(edgeStatus);
    auto edges = std::move(edgeStatus).value();
    if (edges.empty()) {
      return Status::SemanticError("No edge type found in space `%s'", space.name.c_str());
    }
    for (auto edge : edges) {
      auto edgeType = schemaMng->toEdgeType(space.id, edge);
      if (!edgeType.ok()) {
        return Status::SemanticError(
            "`%s' not found in space [`%s'].", edge.c_str(), space.name.c_str());
      }
      over.edgeTypes.emplace_back(edgeType.value());
    }
    over.allEdges = std::move(edges);
    over.isOverAll = true;
  } else {
    auto edges = clause->edges();
    for (auto* edge : edges) {
      auto edgeName = *edge->edge();
      auto edgeType = schemaMng->toEdgeType(space.id, edgeName);
      if (!edgeType.ok()) {
        return Status::SemanticError(
            "%s not found in space [%s].", edgeName.c_str(), space.name.c_str());
      }
      over.edgeTypes.emplace_back(edgeType.value());
    }
  }
  return Status::OK();
}

Status ValidateUtil::invalidLabelIdentifiers(const Expression* expr) {
  auto labelExprs = ExpressionUtils::collectAll(expr, {Expression::Kind::kLabel});
  if (!labelExprs.empty()) {
    std::stringstream ss;
    ss << "Invalid label identifiers: ";
    for (auto* label : labelExprs) {
      ss << label->toString() << ",";
    }
    auto errMsg = ss.str();
    errMsg.pop_back();
    return Status::SemanticError(std::move(errMsg));
  }
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
