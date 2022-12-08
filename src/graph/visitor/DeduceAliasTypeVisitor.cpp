/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/visitor/DeduceAliasTypeVisitor.h"

#include <sstream>
#include <unordered_map>

#include "common/datatypes/DataSet.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Path.h"
#include "common/datatypes/Set.h"
#include "common/function/FunctionManager.h"
#include "graph/context/QueryContext.h"
#include "graph/context/QueryExpressionContext.h"
#include "graph/context/ValidateContext.h"
#include "graph/visitor/EvaluableExprVisitor.h"

namespace nebula {
namespace graph {

DeduceAliasTypeVisitor::DeduceAliasTypeVisitor(QueryContext *qctx,
                                               ValidateContext *vctx,
                                               GraphSpaceID space,
                                               AliasType inputType)
    : qctx_(qctx), vctx_(vctx), space_(space), inputType_(inputType) {}

void DeduceAliasTypeVisitor::visit(VertexExpression *expr) {
  UNUSED(expr);
  outputType_ = AliasType::kNode;
}

void DeduceAliasTypeVisitor::visit(EdgeExpression *expr) {
  UNUSED(expr);
  outputType_ = AliasType::kEdge;
}

void DeduceAliasTypeVisitor::visit(PathBuildExpression *expr) {
  UNUSED(expr);
  outputType_ = AliasType::kPath;
}

void DeduceAliasTypeVisitor::visit(FunctionCallExpression *expr) {
  std::string funName = expr->name();
  std::transform(funName.begin(), funName.end(), funName.begin(), ::tolower);
  if (funName == "nodes") {
    outputType_ = AliasType::kNodeList;
  } else if (funName == "relationships") {
    outputType_ = AliasType::kEdgeList;
  } else if (funName == "reversepath") {
    outputType_ = AliasType::kPath;
  } else if (funName == "startnode" || funName == "endnode") {
    outputType_ = AliasType::kNode;
  }
}

void DeduceAliasTypeVisitor::visit(SubscriptExpression *expr) {
  Expression *leftExpr = expr->left();
  DeduceAliasTypeVisitor childVisitor(qctx_, vctx_, space_, inputType_);
  leftExpr->accept(&childVisitor);
  if (!childVisitor.ok()) {
    status_ = std::move(childVisitor).status();
    return;
  }
  inputType_ = childVisitor.outputType();
  // This is not accurate, since there exist List of List...Edges/Nodes,
  // may have opportunities when analyze more detail of the expr.
  if (inputType_ == AliasType::kEdgeList) {
    outputType_ = AliasType::kEdge;
  } else if (inputType_ == AliasType::kNodeList) {
    outputType_ = AliasType::kNode;
  } else {
    outputType_ = inputType_;
  }
}

void DeduceAliasTypeVisitor::visit(SubscriptRangeExpression *expr) {
  Expression *leftExpr = expr->list();
  DeduceAliasTypeVisitor childVisitor(qctx_, vctx_, space_, inputType_);
  leftExpr->accept(&childVisitor);
  if (!childVisitor.ok()) {
    status_ = std::move(childVisitor).status();
    return;
  }
  inputType_ = childVisitor.outputType();
  // This is not accurate, since there exist List of List...Edges/Nodes,
  // may have opportunities when analyze more detail of the expr.
  if (inputType_ == AliasType::kEdgeList) {
    outputType_ = AliasType::kEdgeList;
  } else if (inputType_ == AliasType::kNodeList) {
    outputType_ = AliasType::kNodeList;
  } else {
    outputType_ = inputType_;
  }
}

}  // namespace graph
}  // namespace nebula
