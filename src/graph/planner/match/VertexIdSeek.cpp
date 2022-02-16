/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/VertexIdSeek.h"

#include <unordered_map>
#include <utility>
#include <vector>

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Value.h"
#include "common/expression/Expression.h"
#include "common/expression/PropertyExpression.h"
#include "graph/context/ExecutionContext.h"
#include "graph/context/QueryContext.h"
#include "graph/context/Result.h"
#include "graph/context/ValidateContext.h"
#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/plan/ExecutionPlan.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/AnonVarGenerator.h"
#include "graph/visitor/VidExtractVisitor.h"

namespace nebula {
namespace graph {
bool VertexIdSeek::matchEdge(EdgeContext *edgeCtx) {
  UNUSED(edgeCtx);
  return false;
}

StatusOr<SubPlan> VertexIdSeek::transformEdge(EdgeContext *edgeCtx) {
  UNUSED(edgeCtx);
  return Status::Error("Unimplemented for edge pattern.");
}

bool VertexIdSeek::matchNode(NodeContext *nodeCtx) {
  auto &node = *nodeCtx->info;
  auto *matchClauseCtx = nodeCtx->matchClauseCtx;
  if (matchClauseCtx->where == nullptr || matchClauseCtx->where->filter == nullptr) {
    return false;
  }

  if (node.alias.empty() || node.anonymous) {
    // require one named node
    return false;
  }

  VidExtractVisitor vidExtractVisitor;
  matchClauseCtx->where->filter->accept(&vidExtractVisitor);
  auto vidResult = vidExtractVisitor.moveVidPattern();
  if (vidResult.spec != VidExtractVisitor::VidPattern::Special::kInUsed) {
    return false;
  }
  if (vidResult.nodes.size() > 1) {
    // where id(v) == 'xxx' or id(t) == 'yyy'
    return false;
  }
  for (auto &nodeVid : vidResult.nodes) {
    if (nodeVid.second.kind == VidExtractVisitor::VidPattern::Vids::Kind::kIn) {
      if (nodeVid.first == node.alias) {
        nodeCtx->ids = std::move(nodeVid.second.vids);
        return true;
      }
    }
  }

  return false;
}

std::string VertexIdSeek::listToAnnoVarVid(QueryContext *qctx, const List &list) {
  auto input = qctx->vctx()->anonVarGen()->getVar();
  DataSet vids({kVid});
  for (auto &v : list.values) {
    vids.emplace_back(Row({std::move(v)}));
  }

  qctx->ectx()->setResult(input, ResultBuilder().value(Value(std::move(vids))).build());

  return input;
}

StatusOr<SubPlan> VertexIdSeek::transformNode(NodeContext *nodeCtx) {
  SubPlan plan;
  auto *matchClauseCtx = nodeCtx->matchClauseCtx;
  auto *qctx = matchClauseCtx->qctx;

  std::string inputVar = listToAnnoVarVid(qctx, nodeCtx->ids);

  auto *passThrough = PassThroughNode::make(qctx, nullptr);
  passThrough->setOutputVar(inputVar);
  passThrough->setColNames({kVid});

  auto *dedup = Dedup::make(qctx, passThrough);
  dedup->setColNames({kVid});

  plan.root = dedup;
  plan.tail = passThrough;

  nodeCtx->initialExpr = InputPropertyExpression::make(qctx->objPool(), kVid);
  return plan;
}

}  // namespace graph
}  // namespace nebula
