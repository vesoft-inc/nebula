/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "graph/planner/ngql/SubgraphPlanner.h"

#include "graph/planner/plan/Algo.h"
#include "graph/planner/plan/Logic.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/PlannerUtil.h"
#include "graph/util/SchemaUtil.h"
#include "graph/validator/Validator.h"

namespace nebula {
namespace graph {

StatusOr<std::unique_ptr<std::vector<EdgeProp>>> SubgraphPlanner::buildEdgeProps() {
  auto* qctx = subgraphCtx_->qctx;
  bool getEdgeProp = subgraphCtx_->withProp && subgraphCtx_->getEdgeProp;
  const auto& space = subgraphCtx_->space;
  auto& edgeTypes = subgraphCtx_->edgeTypes;
  auto& biDirectEdgeTypes = subgraphCtx_->biDirectEdgeTypes;

  if (edgeTypes.empty()) {
    const auto allEdgesSchema = qctx->schemaMng()->getAllLatestVerEdgeSchema(space.id);
    NG_RETURN_IF_ERROR(allEdgesSchema);
    const auto allEdges = std::move(allEdgesSchema).value();
    for (const auto& edge : allEdges) {
      edgeTypes.emplace(edge.first);
      edgeTypes.emplace(-edge.first);
      biDirectEdgeTypes.emplace(edge.first);
      biDirectEdgeTypes.emplace(-edge.first);
    }
  }
  std::vector<EdgeType> vEdgeTypes(edgeTypes.begin(), edgeTypes.end());
  auto edgeProps = SchemaUtil::getEdgeProps(qctx, space, std::move(vEdgeTypes), getEdgeProp);
  NG_RETURN_IF_ERROR(edgeProps);
  return edgeProps;
}

// ++loopSteps{0} < steps  && (var is Empty OR size(var) != 0)
Expression* SubgraphPlanner::loopCondition(uint32_t steps, const std::string& var) {
  auto* qctx = subgraphCtx_->qctx;
  auto* pool = qctx->objPool();
  auto& loopSteps = subgraphCtx_->loopSteps;
  qctx->ectx()->setValue(loopSteps, 0);
  auto step = ExpressionUtils::stepCondition(pool, loopSteps, steps);
  auto empty = ExpressionUtils::equalCondition(pool, var, Value::kEmpty);
  auto neZero = ExpressionUtils::neZeroCondition(pool, var);
  auto* earlyEnd = LogicalExpression::makeOr(pool, empty, neZero);
  return LogicalExpression::makeAnd(pool, step, earlyEnd);
}

StatusOr<SubPlan> SubgraphPlanner::nSteps(SubPlan& startVidPlan, const std::string& input) {
  auto* qctx = subgraphCtx_->qctx;
  const auto& space = subgraphCtx_->space;
  const auto& steps = subgraphCtx_->steps;

  auto* startNode = StartNode::make(qctx);
  bool getVertexProp = subgraphCtx_->withProp && subgraphCtx_->getVertexProp;
  auto vertexProps = SchemaUtil::getAllVertexProp(qctx, space.id, getVertexProp);
  NG_RETURN_IF_ERROR(vertexProps);
  auto edgeProps = buildEdgeProps();
  NG_RETURN_IF_ERROR(edgeProps);
  auto* gn = GetNeighbors::make(qctx, startNode, space.id);
  gn->setSrc(subgraphCtx_->from.src);
  gn->setVertexProps(std::move(vertexProps).value());
  gn->setEdgeProps(std::move(edgeProps).value());
  gn->setInputVar(input);

  auto resultVar = qctx->vctx()->anonVarGen()->getVar();
  auto loopSteps = qctx->vctx()->anonVarGen()->getVar();
  subgraphCtx_->loopSteps = loopSteps;
  auto* subgraph = Subgraph::make(qctx, gn, resultVar, loopSteps, steps.steps() + 1);
  subgraph->setOutputVar(input);
  subgraph->setBiDirectEdgeTypes(subgraphCtx_->biDirectEdgeTypes);
  subgraph->setColNames({nebula::kVid});

  auto* condition = loopCondition(steps.steps() + 1, gn->outputVar());
  auto* loop = Loop::make(qctx, startVidPlan.root, subgraph, condition);

  auto* dc = DataCollect::make(qctx, DataCollect::DCKind::kSubgraph);
  dc->addDep(loop);
  dc->setInputVars({resultVar});
  dc->setColType(std::move(subgraphCtx_->colType));
  dc->setColNames(subgraphCtx_->colNames);

  SubPlan subPlan;
  subPlan.root = dc;
  subPlan.tail = startVidPlan.tail != nullptr ? startVidPlan.tail : loop;
  return subPlan;
}

StatusOr<SubPlan> SubgraphPlanner::zeroStep(SubPlan& startVidPlan, const std::string& input) {
  auto qctx = subgraphCtx_->qctx;
  const auto& space = subgraphCtx_->space;
  auto* pool = qctx->objPool();
  // get all vertexProp
  auto vertexProp = SchemaUtil::getAllVertexProp(qctx, space.id, subgraphCtx_->withProp);
  NG_RETURN_IF_ERROR(vertexProp);
  auto* getVertex = GetVertices::make(qctx,
                                      startVidPlan.root,
                                      space.id,
                                      subgraphCtx_->from.src,
                                      std::move(vertexProp).value(),
                                      {},
                                      true);
  getVertex->setInputVar(input);

  auto* vertexExpr = VertexExpression::make(pool);
  auto* func = AggregateExpression::make(pool, "COLLECT", vertexExpr, false);

  auto* collect = Aggregate::make(qctx, getVertex, {}, {func});
  collect->setColNames(std::move(subgraphCtx_->colNames));

  SubPlan subPlan;
  subPlan.root = collect;
  subPlan.tail = startVidPlan.tail == nullptr ? getVertex : startVidPlan.tail;
  return subPlan;
}

StatusOr<SubPlan> SubgraphPlanner::transform(AstContext* astCtx) {
  subgraphCtx_ = static_cast<SubgraphContext*>(astCtx);
  auto qctx = subgraphCtx_->qctx;
  std::string vidsVar;

  SubPlan startPlan = PlannerUtil::buildStart(qctx, subgraphCtx_->from, vidsVar);
  if (subgraphCtx_->steps.steps() == 0) {
    return zeroStep(startPlan, vidsVar);
  }
  return nSteps(startPlan, vidsVar);
}

}  //  namespace graph
}  //  namespace nebula
