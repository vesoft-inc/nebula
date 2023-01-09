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
StatusOr<std::unique_ptr<std::vector<VertexProp>>> SubgraphPlanner::buildVertexProps() {
  auto* qctx = subgraphCtx_->qctx;
  const auto& space = subgraphCtx_->space;
  bool getVertexProp = subgraphCtx_->withProp && subgraphCtx_->getVertexProp;
  auto vertexProps = SchemaUtil::getAllVertexProp(qctx, space.id, getVertexProp);
  return vertexProps;
}

StatusOr<std::unique_ptr<std::vector<EdgeProp>>> SubgraphPlanner::buildEdgeProps() {
  auto* qctx = subgraphCtx_->qctx;
  const auto& space = subgraphCtx_->space;
  auto& edgeTypes = subgraphCtx_->edgeTypes;
  auto& exprProps = subgraphCtx_->exprProps;
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
  const auto& edgeProps = exprProps.edgeProps();
  if (!subgraphCtx_->withProp && !edgeProps.empty()) {
    auto edgePropsPtr = std::make_unique<std::vector<EdgeProp>>();
    edgePropsPtr->reserve(edgeTypes.size());
    for (const auto& edgeType : edgeTypes) {
      EdgeProp ep;
      ep.type_ref() = edgeType;
      const auto& found = edgeProps.find(std::abs(edgeType));
      if (found != edgeProps.end()) {
        std::set<std::string> props(found->second.begin(), found->second.end());
        props.emplace(kType);
        props.emplace(kRank);
        props.emplace(kDst);
        ep.props_ref() = std::vector<std::string>(props.begin(), props.end());
      } else {
        ep.props_ref() = std::vector<std::string>({kType, kRank, kDst});
      }
      edgePropsPtr->emplace_back(std::move(ep));
    }
    return edgePropsPtr;
  } else {
    bool getEdgeProp = subgraphCtx_->withProp && subgraphCtx_->getEdgeProp;
    std::vector<EdgeType> vEdgeTypes(edgeTypes.begin(), edgeTypes.end());
    return SchemaUtil::getEdgeProps(qctx, space, std::move(vEdgeTypes), getEdgeProp);
  }
}

StatusOr<SubPlan> SubgraphPlanner::nSteps(SubPlan& startVidPlan, const std::string& input) {
  auto* qctx = subgraphCtx_->qctx;
  const auto& space = subgraphCtx_->space;
  const auto& steps = subgraphCtx_->steps;

  auto vertexProps = buildVertexProps();
  NG_RETURN_IF_ERROR(vertexProps);
  auto edgeProps = buildEdgeProps();
  NG_RETURN_IF_ERROR(edgeProps);

  auto* subgraph = Subgraph::make(qctx,
                                  startVidPlan.root,
                                  space.id,
                                  subgraphCtx_->from.src,
                                  subgraphCtx_->tagFilter,
                                  subgraphCtx_->edgeFilter,
                                  subgraphCtx_->filter,
                                  steps.steps() + 1);
  subgraph->setVertexProps(std::move(vertexProps).value());
  subgraph->setEdgeProps(std::move(edgeProps).value());
  subgraph->setInputVar(input);
  subgraph->setBiDirectEdgeTypes(subgraphCtx_->biDirectEdgeTypes);

  auto* dc = DataCollect::make(qctx, DataCollect::DCKind::kSubgraph);
  dc->addDep(subgraph);
  dc->setInputVars({subgraph->outputVar()});
  dc->setColType(std::move(subgraphCtx_->colType));
  dc->setColNames(subgraphCtx_->colNames);

  SubPlan subPlan;
  subPlan.root = dc;
  subPlan.tail = startVidPlan.tail == nullptr ? subgraph : startVidPlan.tail;
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
