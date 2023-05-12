/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/ngql/GoPlanner.h"

#include "graph/planner/plan/Logic.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/PlannerUtil.h"

namespace nebula {
namespace graph {

std::unique_ptr<GoPlanner::EdgeProps> GoPlanner::buildEdgeProps(bool onlyDst) {
  auto eProps = std::make_unique<EdgeProps>();
  switch (goCtx_->over.direction) {
    case storage::cpp2::EdgeDirection::IN_EDGE: {
      doBuildEdgeProps(eProps, onlyDst, true);
      break;
    }
    case storage::cpp2::EdgeDirection::OUT_EDGE: {
      doBuildEdgeProps(eProps, onlyDst, false);
      break;
    }
    case storage::cpp2::EdgeDirection::BOTH: {
      doBuildEdgeProps(eProps, onlyDst, true);
      doBuildEdgeProps(eProps, onlyDst, false);
      break;
    }
  }
  return eProps;
}

void GoPlanner::doBuildEdgeProps(std::unique_ptr<EdgeProps>& eProps, bool onlyDst, bool isInEdge) {
  const auto& exprProps = goCtx_->exprProps;
  for (const auto& e : goCtx_->over.edgeTypes) {
    EdgeProp ep;
    if (isInEdge) {
      ep.type_ref() = -e;
    } else {
      ep.type_ref() = e;
    }

    if (onlyDst) {
      ep.props_ref() = {kDst};
      eProps->emplace_back(std::move(ep));
      continue;
    }

    auto found = exprProps.edgeProps().find(e);
    if (found == exprProps.edgeProps().end()) {
      ep.props_ref() = {kDst};
    } else {
      std::set<folly::StringPiece> props(found->second.begin(), found->second.end());
      props.emplace(kDst);
      ep.props_ref() = std::vector<std::string>(props.begin(), props.end());
    }
    eProps->emplace_back(std::move(ep));
  }
}

std::vector<EdgeType> GoPlanner::buildEdgeTypes() {
  switch (goCtx_->over.direction) {
    case storage::cpp2::EdgeDirection::IN_EDGE: {
      std::vector<EdgeType> edgeTypes;
      edgeTypes.reserve(goCtx_->over.edgeTypes.size());
      for (auto edgeType : goCtx_->over.edgeTypes) {
        edgeTypes.emplace_back(-edgeType);
      }
      return edgeTypes;
    }
    case storage::cpp2::EdgeDirection::OUT_EDGE: {
      return goCtx_->over.edgeTypes;
    }
    case storage::cpp2::EdgeDirection::BOTH: {
      std::vector<EdgeType> edgeTypes;
      for (auto edgeType : goCtx_->over.edgeTypes) {
        edgeTypes.emplace_back(edgeType);
        edgeTypes.emplace_back(-edgeType);
      }
      return edgeTypes;
    }
  }
  return {};
}

std::unique_ptr<GoPlanner::VertexProps> GoPlanner::buildVertexProps(
    const ExpressionProps::TagIDPropsMap& propsMap) {
  if (propsMap.empty()) {
    return nullptr;
  }
  auto vertexProps = std::make_unique<VertexProps>(propsMap.size());
  auto fun = [](auto& tag) {
    VertexProp vp;
    vp.tag_ref() = tag.first;
    std::vector<std::string> props(tag.second.begin(), tag.second.end());
    vp.props_ref() = std::move(props);
    return vp;
  };
  std::transform(propsMap.begin(), propsMap.end(), vertexProps->begin(), fun);
  return vertexProps;
}

// The root plan node will output ColNames of
// {srcProps, edgeProps, kVid, "JOIN_DST_VID", "DST_VID", dstProps}
PlanNode* GoPlanner::buildJoinDstPlan(PlanNode* dep) {
  auto qctx = goCtx_->qctx;
  auto* pool = qctx->objPool();

  auto& colName = dep->colNames().back();
  auto argNode = Argument::make(qctx, colName);
  argNode->setColNames({colName});
  argNode->setInputVertexRequired(false);

  // dst is the first column, columnName is "JOIN_DST_VID"
  auto* dstExpr = ColumnExpression::make(pool, LAST_COL_INDEX);
  auto* getVertex = GetVertices::make(qctx,
                                      argNode,
                                      goCtx_->space.id,
                                      dstExpr,
                                      buildVertexProps(goCtx_->exprProps.dstTagProps()),
                                      {},
                                      true);

  auto& dstPropsExpr = goCtx_->dstPropsExpr;
  // extract dst's prop
  auto* vidExpr = new YieldColumn(ColumnExpression::make(pool, VID_INDEX), "_getVertex_vid");
  dstPropsExpr->addColumn(vidExpr);

  // extract dst's prop, vid is the last column
  auto* project = Project::make(qctx, getVertex, dstPropsExpr);

  // dep's colName "JOIN_DST_VID"  join getVertex's colName "DST_VID"
  auto* hashKey = ColumnExpression::make(pool, LAST_COL_INDEX);
  auto* probeKey = ColumnExpression::make(pool, LAST_COL_INDEX);

  auto* join = HashLeftJoin::make(qctx, dep, project, {hashKey}, {probeKey});
  std::vector<std::string> colNames = dep->colNames();
  colNames.insert(colNames.end(), project->colNames().begin(), project->colNames().end());
  join->setColNames(std::move(colNames));
  return join;
}

SubPlan GoPlanner::doSimplePlan() {
  auto qctx = goCtx_->qctx;
  size_t minStep = goCtx_->steps.mSteps();
  size_t maxStep = goCtx_->steps.nSteps();
  size_t steps = minStep;
  if (minStep != maxStep) {
    steps = minStep == 0 ? minStep : minStep - 1;
  }

  auto* expand = Expand::make(qctx,
                              startNode_,
                              goCtx_->space.id,
                              false,  // random
                              steps,
                              buildEdgeProps(true));
  expand->setEdgeTypes(buildEdgeTypes());
  expand->setColNames({"_expand_vid"});
  expand->setInputVar(goCtx_->vidsVar);

  auto dep = expand;
  if (minStep != maxStep) {
    //  simple m to n case
    //  go m to n steps from 'xxx' over edge yield distinct edge._dst
    dep = ExpandAll::make(qctx,
                          dep,
                          goCtx_->space.id,
                          false,  // random
                          minStep,
                          maxStep,
                          buildEdgeProps(true),
                          nullptr,
                          nullptr,
                          nullptr);
    dep->setEdgeTypes(buildEdgeTypes());
    dep->setColNames({"_expandall_vid"});
  }

  auto* dedup = Dedup::make(qctx, dep);

  auto pool = qctx->objPool();
  auto* newYieldExpr = pool->makeAndAdd<YieldColumns>();
  newYieldExpr->addColumn(new YieldColumn(ColumnExpression::make(pool, 0)));
  auto* project = Project::make(qctx, dedup, newYieldExpr);
  project->setColNames(std::move(goCtx_->colNames));

  SubPlan subPlan;
  subPlan.root = project;
  subPlan.tail = expand;
  return subPlan;
}

SubPlan GoPlanner::doPlan() {
  auto qctx = goCtx_->qctx;
  auto& from = goCtx_->from;
  auto* pool = qctx->objPool();

  size_t minStep = goCtx_->steps.mSteps();
  size_t maxStep = goCtx_->steps.nSteps();

  auto* expand = Expand::make(qctx,
                              startNode_,
                              goCtx_->space.id,
                              goCtx_->random,
                              minStep == 0 ? minStep : minStep - 1,
                              buildEdgeProps(true));
  if (goCtx_->joinInput) {
    expand->setJoinInput(true);
    expand->setColNames({"_expand_vid", "_expand_dst"});
  } else {
    expand->setEdgeTypes(buildEdgeTypes());
    expand->setColNames({"_expand_vid"});
  }
  expand->setInputVar(goCtx_->vidsVar);
  expand->setStepLimits(goCtx_->limits);

  if (goCtx_->joinDst) {
    auto* dstExpr =
        new YieldColumn(EdgePropertyExpression::make(pool, "*", kDst), "_expandall_dst");
    goCtx_->edgePropsExpr->addColumn(dstExpr);
  }
  auto* expandAll = ExpandAll::make(qctx,
                                    expand,
                                    goCtx_->space.id,
                                    goCtx_->random,
                                    minStep,
                                    maxStep,
                                    buildEdgeProps(false),
                                    buildVertexProps(goCtx_->exprProps.srcTagProps()),
                                    goCtx_->srcPropsExpr,
                                    goCtx_->edgePropsExpr);
  if (goCtx_->joinInput) {
    expandAll->setJoinInput(true);
    // Insert _expand_vid in the first column in colNames
    auto colNames = expandAll->colNames();
    colNames.insert(colNames.begin(), "_expand_vid");
    expandAll->setColNames(colNames);
  }
  expandAll->setStepLimits(goCtx_->limits);
  PlanNode* dep = expandAll;
  if (goCtx_->joinDst) {
    dep = buildJoinDstPlan(expandAll);
  }

  if (goCtx_->joinInput) {
    // the result of the previous statement InnerJoin the result of the current statement
    auto* hashKey = from.originalSrc;
    auto* probeKey = VariablePropertyExpression::make(pool, dep->outputVar(), "_expand_vid");
    auto* join = HashInnerJoin::make(qctx, preRootNode_, dep, {hashKey}, {probeKey});

    auto& varName = from.fromType == kPipe ? goCtx_->inputVarName : from.userDefinedVarName;
    auto* varPtr = qctx->symTable()->getVar(varName);
    DCHECK(varPtr != nullptr);
    std::vector<std::string> colNames = varPtr->colNames;
    colNames.insert(colNames.end(), dep->colNames().begin(), dep->colNames().end());
    join->setColNames(std::move(colNames));
    dep = join;
  }

  if (goCtx_->filter != nullptr) {
    dep = Filter::make(qctx, dep, goCtx_->filter);
  }

  dep = Project::make(qctx, dep, goCtx_->yieldExpr);
  dep->setColNames(std::move(goCtx_->colNames));

  if (goCtx_->distinct) {
    dep = Dedup::make(qctx, dep);
  }

  SubPlan subPlan;
  subPlan.root = dep;
  subPlan.tail = startNode_;
  return subPlan;
}

StatusOr<SubPlan> GoPlanner::transform(AstContext* astCtx) {
  goCtx_ = static_cast<GoContext*>(astCtx);
  auto qctx = goCtx_->qctx;
  goCtx_->joinInput = goCtx_->from.fromType != FromType::kInstantExpr;

  startNode_ = StartNode::make(qctx);
  auto& from = goCtx_->from;
  if (!from.vids.empty() && from.originalSrc == nullptr) {
    PlannerUtil::buildConstantInput(qctx, from, goCtx_->vidsVar);
  } else {
    // get root node of the previous statement
    auto& varName = from.fromType == kVariable ? from.userDefinedVarName : goCtx_->inputVarName;
    auto* varPtr = qctx->symTable()->getVar(varName);
    DCHECK(varPtr != nullptr);
    DCHECK_EQ(varPtr->writtenBy.size(), 1);
    for (auto node : varPtr->writtenBy) {
      preRootNode_ = node;
    }

    auto argNode = Argument::make(qctx, from.runtimeVidName);
    argNode->setColNames({from.runtimeVidName});
    argNode->setInputVertexRequired(false);
    goCtx_->vidsVar = argNode->outputVar();
    startNode_ = argNode;
  }
  auto& steps = goCtx_->steps;
  if (!steps.isMToN() && steps.steps() == 0) {
    auto* pt = PassThroughNode::make(qctx, nullptr);
    pt->setColNames(std::move(goCtx_->colNames));
    SubPlan subPlan;
    subPlan.root = subPlan.tail = pt;
    return subPlan;
  }
  if (goCtx_->isSimple) {
    return doSimplePlan();
  }
  return doPlan();
}

}  // namespace graph
}  // namespace nebula
