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
//                 |   BFS/Multi/ALL  |     |   LoopDepend    |
//                 +--------+---------+     +--------+--------+
//                          |                        |
//                 +--------+---------+              |
//                 |      Loop        |--------------+
//                 +--------+---------+
//                          |
//                 +--------+---------+
//                 |   DataCollect    |
//                 +--------+---------+
//                          |
//                 +--------+---------+
//                 |   GetPathProp    |
//                 +--------+---------+
//

StatusOr<SubPlan> PathPlanner::transform(AstContext* astCtx) {
  pathCtx_ = static_cast<PathContext*>(astCtx);
  auto qctx = pathCtx_->qctx;
  auto& from = pathCtx_->from;
  auto& to = pathCtx_->to;
  buildStart(from, pathCtx_->fromVidsVar, false);
  buildStart(to, pathCtx_->toVidsVar, true);

  auto* startNode = StartNode::make(qctx);
  auto* pt = PassThroughNode::make(qctx, startNode);

  PlanNode* left = getNeighbors(pt, false);
  PlanNode* right = getNeighbors(pt, true);

  SubPlan subPlan;
  if (!pathCtx_->isShortest || pathCtx_->noLoop) {
    subPlan = allPairPlan(left, right);
  } else if (from.vids.size() == 1 && to.vids.size() == 1) {
    subPlan = singlePairPlan(left, right);
  } else {
    subPlan = multiPairPlan(left, right);
  }

  // get path's property
  if (pathCtx_->withProp) {
    subPlan.root = buildPathProp(subPlan.root);
  }
  return subPlan;
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

PlanNode* PathPlanner::getNeighbors(PlanNode* dep, bool reverse) {
  auto qctx = pathCtx_->qctx;
  const auto& gnInputVar = reverse ? pathCtx_->toVidsVar : pathCtx_->fromVidsVar;
  auto* gn = GetNeighbors::make(qctx, dep, pathCtx_->space.id);
  gn->setSrc(ColumnExpression::make(qctx->objPool(), 0));
  gn->setEdgeProps(buildEdgeProps(reverse));
  gn->setInputVar(gnInputVar);
  gn->setDedup();
  gn->setEdgeDirection(pathCtx_->over.direction);

  PlanNode* result = gn;
  if (pathCtx_->filter != nullptr) {
    auto* filterExpr = pathCtx_->filter->clone();
    result = Filter::make(qctx, gn, filterExpr);
  }
  return result;
}

SubPlan PathPlanner::singlePairPlan(PlanNode* left, PlanNode* right) {
  auto qctx = pathCtx_->qctx;
  auto steps = pathCtx_->steps.steps();
  auto terminateEarlyVar = qctx->vctx()->anonVarGen()->getVar();
  qctx->ectx()->setValue(terminateEarlyVar, false);
  auto* path = BFSShortestPath::make(qctx, left, right, steps);
  path->setLeftVidVar(pathCtx_->fromVidsVar);
  path->setRightVidVar(pathCtx_->toVidsVar);
  path->setColNames({kPathStr});
  path->setTerminateEarlyVar(terminateEarlyVar);

  auto* loopCondition = singlePairLoopCondition(steps, path->outputVar(), terminateEarlyVar);
  auto* loop = Loop::make(qctx, nullptr, path, loopCondition);

  auto* dc = DataCollect::make(qctx, DataCollect::DCKind::kBFSShortest);
  dc->addDep(loop);
  dc->setInputVars({path->outputVar()});
  dc->setColNames(pathCtx_->colNames);

  SubPlan subPlan;
  subPlan.root = dc;
  subPlan.tail = loop;
  return subPlan;
}

SubPlan PathPlanner::allPairPlan(PlanNode* left, PlanNode* right) {
  auto qctx = pathCtx_->qctx;
  auto steps = pathCtx_->steps.steps();
  auto* path = ProduceAllPaths::make(qctx, left, right, steps, pathCtx_->noLoop);
  path->setLeftVidVar(pathCtx_->fromVidsVar);
  path->setRightVidVar(pathCtx_->toVidsVar);
  path->setColNames({kPathStr});

  SubPlan loopDep = loopDepPlan();
  auto* loopCondition = allPairLoopCondition(steps);
  auto* loop = Loop::make(qctx, loopDep.root, path, loopCondition);

  auto* dc = DataCollect::make(qctx, DataCollect::DCKind::kAllPaths);
  dc->addDep(loop);
  dc->setInputVars({path->outputVar()});
  dc->setColNames(pathCtx_->colNames);

  SubPlan subPlan;
  subPlan.root = dc;
  subPlan.tail = loopDep.tail;
  return subPlan;
}

SubPlan PathPlanner::multiPairPlan(PlanNode* left, PlanNode* right) {
  auto qctx = pathCtx_->qctx;
  auto steps = pathCtx_->steps.steps();
  auto terminationVar = qctx->vctx()->anonVarGen()->getVar();
  qctx->ectx()->setValue(terminationVar, false);
  auto* path = MultiShortestPath::make(qctx, left, right, steps);
  path->setLeftVidVar(pathCtx_->fromVidsVar);
  path->setRightVidVar(pathCtx_->toVidsVar);
  path->setTerminationVar(terminationVar);
  path->setColNames({kPathStr});

  SubPlan loopDep = loopDepPlan();
  auto* loopCondition = multiPairLoopCondition(steps, terminationVar);
  auto* loop = Loop::make(qctx, loopDep.root, path, loopCondition);

  auto* dc = DataCollect::make(qctx, DataCollect::DCKind::kAllPaths);
  dc->addDep(loop);
  dc->setInputVars({path->outputVar()});
  dc->setColNames(pathCtx_->colNames);

  SubPlan subPlan;
  subPlan.root = dc;
  subPlan.tail = loopDep.tail;
  return subPlan;
}

SubPlan PathPlanner::loopDepPlan() {
  auto* qctx = pathCtx_->qctx;
  auto* pool = qctx->objPool();
  SubPlan subPlan = buildRuntimeVidPlan();
  {
    auto* columns = pool->makeAndAdd<YieldColumns>();
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
    auto* columns = pool->makeAndAdd<YieldColumns>();
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

// loopSteps{0} <= ((steps + 1) / 2)  && (pathVar is Empty || size(pathVar) ==
// 0)
Expression* PathPlanner::singlePairLoopCondition(uint32_t steps,
                                                 const std::string& pathVar,
                                                 const std::string& terminateEarlyVar) {
  auto loopSteps = pathCtx_->qctx->vctx()->anonVarGen()->getVar();
  pathCtx_->qctx->ectx()->setValue(loopSteps, 0);
  auto* pool = pathCtx_->qctx->objPool();

  auto step = ExpressionUtils::stepCondition(pool, loopSteps, ((steps + 1) / 2));
  auto empty = ExpressionUtils::equalCondition(pool, pathVar, Value::kEmpty);
  auto zero = ExpressionUtils::zeroCondition(pool, pathVar);
  auto loopTerminateEarly = ExpressionUtils::equalCondition(pool, terminateEarlyVar, false);
  auto* noFound = LogicalExpression::makeOr(pool, empty, zero);
  auto* earlyStop = LogicalExpression::makeAnd(pool, step, loopTerminateEarly);
  return LogicalExpression::makeAnd(pool, earlyStop, noFound);
}

// loopSteps{0} <= (steps + 1) / 2
Expression* PathPlanner::allPairLoopCondition(uint32_t steps) {
  auto loopSteps = pathCtx_->qctx->vctx()->anonVarGen()->getVar();
  pathCtx_->qctx->ectx()->setValue(loopSteps, 0);
  auto* pool = pathCtx_->qctx->objPool();
  return ExpressionUtils::stepCondition(pool, loopSteps, ((steps + 1) / 2));
}

// loopSteps{0} <= ((steps + 1) / 2) && (terminationVar) == false)
Expression* PathPlanner::multiPairLoopCondition(uint32_t steps, const std::string& terminationVar) {
  auto loopSteps = pathCtx_->qctx->vctx()->anonVarGen()->getVar();
  pathCtx_->qctx->ectx()->setValue(loopSteps, 0);
  auto* pool = pathCtx_->qctx->objPool();
  auto step = ExpressionUtils::stepCondition(pool, loopSteps, ((steps + 1) / 2));
  auto terminate = ExpressionUtils::equalCondition(pool, terminationVar, false);
  return LogicalExpression::makeAnd(pool, step, terminate);
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

PlanNode* PathPlanner::buildVertexPlan(PlanNode* dep, const std::string& input) {
  auto qctx = pathCtx_->qctx;
  auto* pool = qctx->objPool();

  // col 0 of the input is path
  auto args = ArgumentList::make(pool);
  args->addArgument(ColumnExpression::make(pool, 0));
  auto funNodes = FunctionCallExpression::make(pool, "nodes", args);

  auto* column = new YieldColumn(funNodes, "nodes");
  auto* columns = pool->makeAndAdd<YieldColumns>();
  columns->addColumn(column);

  auto* project = Project::make(qctx, dep, columns);
  project->setColNames({"nodes"});
  project->setInputVar(input);

  // col 0 of the project->output is [node...]
  auto* unwindExpr = ColumnExpression::make(pool, 0);
  auto* unwind = Unwind::make(qctx, project, unwindExpr);
  unwind->setFromPipe(true);
  unwind->setColNames({"nodes"});

  // extract vid from vertex, col 0 is vertex
  auto idArgs = ArgumentList::make(pool);
  idArgs->addArgument(ColumnExpression::make(pool, 0));
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
  auto* columns = pool->makeAndAdd<YieldColumns>();
  columns->addColumn(column);

  auto* project = Project::make(qctx, dep, columns);
  project->setColNames({"edges"});
  project->setInputVar(input);

  // col 0 of the project->output() is [edge...]
  auto* unwindExpr = ColumnExpression::make(pool, 0);
  auto* unwind = Unwind::make(qctx, project, unwindExpr);
  unwind->setFromPipe(true);
  unwind->setColNames({"edges"});

  // extract src from edge
  auto srcArgs = ArgumentList::make(pool);
  srcArgs->addArgument(ColumnExpression::make(pool, 0));
  auto* src = FunctionCallExpression::make(pool, "src", srcArgs);
  // extract dst from edge
  auto dstArgs = ArgumentList::make(pool);
  dstArgs->addArgument(ColumnExpression::make(pool, 0));
  auto* dst = FunctionCallExpression::make(pool, "dst", dstArgs);
  // extract rank from edge
  auto rankArgs = ArgumentList::make(pool);
  rankArgs->addArgument(ColumnExpression::make(pool, 0));
  auto* rank = FunctionCallExpression::make(pool, "rank", rankArgs);
  // type
  auto typeArgs = ArgumentList::make(pool);
  typeArgs->addArgument(ColumnExpression::make(pool, 0));
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

}  // namespace graph
}  // namespace nebula
