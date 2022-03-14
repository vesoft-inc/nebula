// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/planner/ngql/PathPlanner.h"

#include "graph/planner/plan/Algo.h"
#include "graph/planner/plan/Logic.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/PlannerUtil.h"
#include "graph/util/SchemaUtil.h"
#include "graph/validator/Validator.h"

namespace nebula {
namespace graph {
std::unique_ptr<std::vector<EdgeProp>> PathPlanner::buildEdgeProps(bool reverse) {
  auto edgeProps = std::make_unique<std::vector<EdgeProp>>();
  switch (pathCtx_->over.direction) {
    case storage::cpp2::EdgeDirection::IN_EDGE: {
      doBuildEdgeProps(edgeProps, reverse, true);
      break;
    }
    case storage::cpp2::EdgeDirection::OUT_EDGE: {
      doBuildEdgeProps(edgeProps, reverse, false);
      break;
    }
    case storage::cpp2::EdgeDirection::BOTH: {
      doBuildEdgeProps(edgeProps, reverse, true);
      doBuildEdgeProps(edgeProps, reverse, false);
      break;
    }
  }
  return edgeProps;
}

void PathPlanner::doBuildEdgeProps(std::unique_ptr<std::vector<EdgeProp>>& edgeProps,
                                   bool reverse,
                                   bool isInEdge) {
  const auto& exprProps = pathCtx_->exprProps;
  for (const auto& e : pathCtx_->over.edgeTypes) {
    storage::cpp2::EdgeProp ep;
    if (reverse == isInEdge) {
      ep.type_ref() = e;
    } else {
      ep.type_ref() = -e;
    }
    const auto& found = exprProps.edgeProps().find(e);
    if (found == exprProps.edgeProps().end()) {
      ep.props_ref() = {kDst, kType, kRank};
    } else {
      std::set<folly::StringPiece> props(found->second.begin(), found->second.end());
      props.emplace(kDst);
      props.emplace(kType);
      props.emplace(kRank);
      ep.props_ref() = std::vector<std::string>(props.begin(), props.end());
    }
    edgeProps->emplace_back(std::move(ep));
  }
}

void PathPlanner::buildStart(Starts& starts, std::string& vidsVar, bool reverse) {
  auto qctx = pathCtx_->qctx;
  if (!starts.vids.empty() && starts.originalSrc == nullptr) {
    PlannerUtil::buildConstantInput(qctx, starts, vidsVar);
  } else {
    if (reverse) {
      auto subPlan = PlannerUtil::buildRuntimeInput(qctx, starts);
      pathCtx_->runtimeToProject = subPlan.tail;
      pathCtx_->runtimeToDedup = subPlan.root;
      vidsVar = pathCtx_->runtimeToDedup->outputVar();
    } else {
      auto subPlan = PlannerUtil::buildRuntimeInput(qctx, starts);
      pathCtx_->runtimeFromProject = subPlan.tail;
      pathCtx_->runtimeFromDedup = subPlan.root;
      vidsVar = pathCtx_->runtimeFromDedup->outputVar();
    }
  }
}

// loopSteps{0} <= (steps + 1) / 2
Expression* PathPlanner::allPathLoopCondition(uint32_t steps) {
  auto loopSteps = pathCtx_->qctx->vctx()->anonVarGen()->getVar();
  pathCtx_->qctx->ectx()->setValue(loopSteps, 0);
  auto* pool = pathCtx_->qctx->objPool();
  return ExpressionUtils::stepCondition(pool, loopSteps, ((steps + 1) / 2));
}

// loopSteps{0} <= ((steps + 1) / 2) && (termination == false)
Expression* PathPlanner::shortestPathLoopCondition(uint32_t steps, const std::string& termination) {
  auto loopSteps = pathCtx_->qctx->vctx()->anonVarGen()->getVar();
  pathCtx_->qctx->ectx()->setValue(loopSteps, 0);
  auto* pool = pathCtx_->qctx->objPool();
  auto step = ExpressionUtils::stepCondition(pool, loopSteps, ((steps + 1) / 2));
  auto earlyEnd = ExpressionUtils::equalCondition(pool, termination, false);
  return LogicalExpression::makeAnd(pool, step, earlyEnd);
}

SubPlan PathPlanner::buildRuntimeVidPlan() {
  SubPlan subPlan;
  const auto& from = pathCtx_->from;
  const auto& to = pathCtx_->to;
  if (!from.vids.empty() && from.originalSrc == nullptr) {
    if (!to.vids.empty() && to.originalSrc == nullptr) {
      return subPlan;
    }
    subPlan.tail = pathCtx_->runtimeToProject;
    subPlan.root = pathCtx_->runtimeToDedup;
  } else {
    if (!to.vids.empty() && to.originalSrc == nullptr) {
      subPlan.tail = pathCtx_->runtimeFromProject;
      subPlan.root = pathCtx_->runtimeFromDedup;
    } else {
      auto* toProject = static_cast<SingleInputNode*>(pathCtx_->runtimeToProject);
      toProject->dependsOn(pathCtx_->runtimeFromDedup);
      // set <to>'s input
      auto& inputName = to.fromType == kPipe ? pathCtx_->inputVarName : to.userDefinedVarName;
      toProject->setInputVar(inputName);
      subPlan.tail = pathCtx_->runtimeFromProject;
      subPlan.root = pathCtx_->runtimeToDedup;
    }
  }
  return subPlan;
}

SubPlan PathPlanner::loopDepPlan() {
  auto* qctx = pathCtx_->qctx;
  auto* pool = qctx->objPool();
  SubPlan subPlan = buildRuntimeVidPlan();
  {
    auto* columns = pool->add(new YieldColumns());
    auto* column = new YieldColumn(ColumnExpression::make(pool, 0), kVid);
    columns->addColumn(column);
    auto* project = Project::make(qctx, subPlan.root, columns);
    project->setInputVar(pathCtx_->fromVidsVar);
    project->setOutputVar(pathCtx_->fromVidsVar);
    project->setColNames({nebula::kVid});
    subPlan.root = project;
  }
  subPlan.tail = subPlan.tail == nullptr ? subPlan.root : subPlan.tail;
  {
    auto* columns = pool->add(new YieldColumns());
    auto* column = new YieldColumn(ColumnExpression::make(pool, 0), kVid);
    columns->addColumn(column);
    auto* project = Project::make(qctx, subPlan.root, columns);
    project->setInputVar(pathCtx_->toVidsVar);
    project->setOutputVar(pathCtx_->toVidsVar);
    project->setColNames({nebula::kVid});
    subPlan.root = project;
  }
  return subPlan;
}

//
//              The plan looks like this:
//                 +--------+---------+
//             +-->+   PassThrough    +<----+
//             |   +------------------+     |
//             |                            |
//    +--------+---------+        +---------+--------+
//    |   GetNeighbors   |        |    GetNeighbors  |
//    +--------+---------+        +---------+--------+
//             |                            |
//             +------------+---------------+
//                          |
//                 +--------+---------+     +--------+--------+
//                 |     FindPath     |     |   LoopDepend    |
//                 +--------+---------+     +--------+--------+
//                          |                        |
//                 +--------+---------+              |
//                 |      Loop        |--------------+
//                 +--------+---------+
//                          |
//                 +--------+---------+
//                 |   DataCollect    |
//                 +--------+---------+

SubPlan PathPlanner::doPlan(PlanNode* dep) {
  auto qctx = pathCtx_->qctx;
  auto* pool = qctx->objPool();
  bool isShortest = pathCtx_->isShortest;
  PlanNode* left = nullptr;
  PlanNode* right = nullptr;
  {
    auto* gn = GetNeighbors::make(qctx, dep, pathCtx_->space.id);
    gn->setSrc(ColumnExpression::make(pool, 0));
    gn->setEdgeProps(buildEdgeProps(false));
    gn->setInputVar(pathCtx_->fromVidsVar);
    gn->setDedup();
    left = gn;
    if (pathCtx_->filter != nullptr) {
      auto* filterExpr = pathCtx_->filter->clone();
      auto* filter = Filter::make(qctx, gn, filterExpr);
      left = filter;
    }
  }
  {
    auto* gn = GetNeighbors::make(qctx, dep, pathCtx_->space.id);
    gn->setSrc(ColumnExpression::make(pool, 0));
    gn->setEdgeProps(buildEdgeProps(true));
    gn->setInputVar(pathCtx_->toVidsVar);
    gn->setDedup();
    right = gn;
    if (pathCtx_->filter != nullptr) {
      auto* filterExpr = pathCtx_->filter->clone();
      auto* filter = Filter::make(qctx, gn, filterExpr);
      right = filter;
    }
  }
  auto steps = pathCtx_->steps.steps();
  auto* path = FindPath::make(qctx, left, right, isShortest, pathCtx_->noLoop, steps);
  path->setLeftVidVar(pathCtx_->fromVidsVar);
  path->setRightVidVar(pathCtx_->toVidsVar);
  path->setColNames({kPathStr});

  SubPlan loopDep = loopDepPlan();
  Expression* loopCondition = nullptr;
  if (isShortest) {
    auto terminationVar = qctx->vctx()->anonVarGen()->getVar();
    qctx->ectx()->setValue(terminationVar, false);
    path->setTerminationVar(terminationVar);
    loopCondition = shortestPathLoopCondition(steps, terminationVar);
  } else {
    loopCondition = allPathLoopCondition(steps);
  }
  auto* loop = Loop::make(qctx, loopDep.root, path, loopCondition);

  auto kind = DataCollect::DCKind::kFindPath;
  if (isShortest && pathCtx_->from.vids.size() == 1 && pathCtx_->to.vids.size() == 1) {
    kind = DataCollect::DCKind::kRowBasedMove;
  }

  auto* dc = DataCollect::make(qctx, kind);
  dc->addDep(loop);
  dc->setInputVars({path->outputVar()});
  dc->setColNames(pathCtx_->colNames);

  SubPlan subPlan;
  subPlan.root = dc;
  subPlan.tail = loopDep.tail;
  return subPlan;
}

PlanNode* PathPlanner::buildVertexPlan(PlanNode* dep, const std::string& input) {
  auto qctx = pathCtx_->qctx;
  auto* pool = qctx->objPool();

  // col 0 of the input is path
  auto args = ArgumentList::make(pool);
  args->addArgument(ColumnExpression::make(pool, 0));
  auto funNodes = FunctionCallExpression::make(pool, "nodes", args);

  auto* column = new YieldColumn(funNodes, "nodes");
  auto* columns = pool->add(new YieldColumns());
  columns->addColumn(column);

  auto* project = Project::make(qctx, dep, columns);
  project->setColNames({"nodes"});
  project->setInputVar(input);

  // col 0 of the project->output is [node...]
  auto* unwindExpr = ColumnExpression::make(pool, 0);
  auto* unwind = Unwind::make(qctx, project, unwindExpr);
  unwind->setColNames({"nodes"});

  // extract vid from vertex, col 0 is vertex
  auto idArgs = ArgumentList::make(pool);
  idArgs->addArgument(ColumnExpression::make(pool, 1));
  auto* src = FunctionCallExpression::make(pool, "id", idArgs);
  // get all vertexprop
  auto vertexProp = SchemaUtil::getAllVertexProp(qctx, pathCtx_->space.id, true);
  auto* getVertices = GetVertices::make(
      qctx, unwind, pathCtx_->space.id, src, std::move(vertexProp).value(), {}, true);

  return getVertices;
}

PlanNode* PathPlanner::buildEdgePlan(PlanNode* dep, const std::string& input) {
  auto qctx = pathCtx_->qctx;
  auto* pool = qctx->objPool();
  // col 0 of the input is path
  auto args = ArgumentList::make(pool);
  args->addArgument(ColumnExpression::make(pool, 0));
  auto funEdges = FunctionCallExpression::make(pool, "relationships", args);

  auto* column = new YieldColumn(funEdges, "edges");
  auto* columns = pool->add(new YieldColumns());
  columns->addColumn(column);

  auto* project = Project::make(qctx, dep, columns);
  project->setColNames({"edges"});
  project->setInputVar(input);

  // col 0 of the project->output() is [edge...]
  auto* unwindExpr = ColumnExpression::make(pool, 0);
  auto* unwind = Unwind::make(qctx, project, unwindExpr);
  unwind->setColNames({"edges"});

  // extract src from edge
  auto srcArgs = ArgumentList::make(pool);
  srcArgs->addArgument(ColumnExpression::make(pool, 1));
  auto* src = FunctionCallExpression::make(pool, "src", srcArgs);
  // extract dst from edge
  auto dstArgs = ArgumentList::make(pool);
  dstArgs->addArgument(ColumnExpression::make(pool, 1));
  auto* dst = FunctionCallExpression::make(pool, "dst", dstArgs);
  // extract rank from edge
  auto rankArgs = ArgumentList::make(pool);
  rankArgs->addArgument(ColumnExpression::make(pool, 1));
  auto* rank = FunctionCallExpression::make(pool, "rank", rankArgs);
  // type
  auto typeArgs = ArgumentList::make(pool);
  typeArgs->addArgument(ColumnExpression::make(pool, 1));
  auto* type = FunctionCallExpression::make(pool, "typeid", typeArgs);
  // prepare edgetype
  auto edgeProp = SchemaUtil::getEdgeProps(qctx, pathCtx_->space, pathCtx_->over.edgeTypes, true);
  auto* getEdge = GetEdges::make(qctx,
                                 unwind,
                                 pathCtx_->space.id,
                                 src,
                                 type,
                                 rank,
                                 dst,
                                 std::move(edgeProp).value(),
                                 {},
                                 true);

  return getEdge;
}

//        The Plan looks like this:
//               +--------+---------+
//           +-->+   PassThrough    +<----+
//           |   +------------------+     |
//  +--------+---------+        +---------+------------+
//  |  Project(Nodes)  |        |Project(RelationShips)|
//  +--------+---------+        +---------+------------+
//           |                            |
//  +--------+---------+        +---------+--------+
//  |      Unwind      |        |      Unwind      |
//  +--------+---------+        +---------+--------+
//           |                            |
//  +--------+---------+        +---------+--------+
//  |   GetVertices    |        |    GetEdges      |
//  +--------+---------+        +---------+--------+
//           |                            |
//           +------------+---------------+
//                        |
//               +--------+---------+
//               |   DataCollect    |
//               +--------+---------+
//
PlanNode* PathPlanner::buildPathProp(PlanNode* dep) {
  auto qctx = pathCtx_->qctx;
  auto* pt = PassThroughNode::make(qctx, dep);

  auto* vertexPlan = buildVertexPlan(pt, dep->outputVar());
  auto* edgePlan = buildEdgePlan(pt, dep->outputVar());

  auto* dc = DataCollect::make(qctx, DataCollect::DCKind::kPathProp);
  dc->addDep(vertexPlan);
  dc->addDep(edgePlan);
  dc->setInputVars({vertexPlan->outputVar(), edgePlan->outputVar(), dep->outputVar()});
  dc->setColNames(std::move(pathCtx_->colNames));
  return dc;
}

StatusOr<SubPlan> PathPlanner::transform(AstContext* astCtx) {
  pathCtx_ = static_cast<PathContext*>(astCtx);
  auto qctx = pathCtx_->qctx;
  buildStart(pathCtx_->from, pathCtx_->fromVidsVar, false);
  buildStart(pathCtx_->to, pathCtx_->toVidsVar, true);

  auto* startNode = StartNode::make(qctx);
  auto* pt = PassThroughNode::make(qctx, startNode);

  SubPlan subPlan = doPlan(pt);
  if (pathCtx_->withProp) {
    subPlan.root = buildPathProp(subPlan.root);
  }
  return subPlan;
}

}  // namespace graph
}  // namespace nebula
