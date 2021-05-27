/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "planner/ngql/PathPlanner.h"
#include "validator/Validator.h"
#include "planner/plan/Logic.h"
#include "planner/plan/Algo.h"
#include "util/SchemaUtil.h"
#include "util/QueryUtil.h"
#include "util/ExpressionUtils.h"

namespace nebula {
namespace graph {
GetNeighbors::EdgeProps PathPlanner::buildEdgeProps(bool reverse) {
    auto edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
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

void PathPlanner::doBuildEdgeProps(GetNeighbors::EdgeProps& edgeProps,
                                   bool reverse,
                                   bool isInEdge) {
    for (const auto& e : pathCtx_->over.edgeTypes) {
        storage::cpp2::EdgeProp ep;
        if (reverse == isInEdge) {
            ep.set_type(e);
        } else {
            ep.set_type(-e);
        }
        ep.set_props({kDst, kType, kRank});
        edgeProps->emplace_back(std::move(ep));
    }
}

void PathPlanner::buildStart(Starts& starts, std::string& vidsVar, bool reverse) {
    auto qctx = pathCtx_->qctx;
    if (!starts.vids.empty() && starts.originalSrc == nullptr) {
        QueryUtil::buildConstantInput(qctx, starts, vidsVar);
    } else {
        if (reverse) {
            auto subPlan = QueryUtil::buildRuntimeInput(qctx, starts);
            pathCtx_->runtimeToProject = subPlan.tail;
            pathCtx_->runtimeToDedup = subPlan.root;
            vidsVar = pathCtx_->runtimeToDedup->outputVar();
        } else {
            auto subPlan = QueryUtil::buildRuntimeInput(qctx, starts);
            pathCtx_->runtimeFromProject = subPlan.tail;
            pathCtx_->runtimeFromDedup = subPlan.root;
            vidsVar = pathCtx_->runtimeFromDedup->outputVar();
        }
    }
}

// loopSteps{0} <= ((steps + 1) / 2)  && (pathVar is Empty || size(pathVar) == 0)
Expression* PathPlanner::singlePairLoopCondition(uint32_t steps, const std::string& pathVar) {
    auto loopSteps = pathCtx_->qctx->vctx()->anonVarGen()->getVar();
    pathCtx_->qctx->ectx()->setValue(loopSteps, 0);
    auto step = ExpressionUtils::stepCondition(loopSteps, ((steps + 1) / 2));
    auto empty = ExpressionUtils::equalCondition(pathVar, Value::kEmpty);
    auto zero = ExpressionUtils::zeroCondition(pathVar);
    auto* noFound = ExpressionUtils::Or(empty.release(), zero.release());
    return ExpressionUtils::And(step.release(), noFound);
}

// loopSteps{0} <= (steps + 1) / 2
Expression* PathPlanner::allPairLoopCondition(uint32_t steps) {
    auto loopSteps = pathCtx_->qctx->vctx()->anonVarGen()->getVar();
    pathCtx_->qctx->ectx()->setValue(loopSteps, 0);
    return ExpressionUtils::stepCondition(loopSteps, ((steps + 1) / 2)).release();
}

// loopSteps{0} <= ((steps + 1) / 2) && (size(pathVar) != 0)
Expression* PathPlanner::multiPairLoopCondition(uint32_t steps, const std::string& pathVar) {
    auto loopSteps = pathCtx_->qctx->vctx()->anonVarGen()->getVar();
    pathCtx_->qctx->ectx()->setValue(loopSteps, 0);
    auto step = ExpressionUtils::stepCondition(loopSteps, ((steps + 1) / 2));
    auto neZero = ExpressionUtils::neZeroCondition(pathVar);
    return ExpressionUtils::And(step.release(), neZero.release());
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

PlanNode* PathPlanner::allPairStartVidDataSet(PlanNode* dep, const std::string& inputVar) {
    // col 0 is vid
    auto* vid = new YieldColumn(new ColumnExpression(0), new std::string(kVid));
    // col 1 is list<path(only contain src)>
    auto* pathExpr = new PathBuildExpression();
    pathExpr->add(std::make_unique<ColumnExpression>(0));

    auto* exprList = new ExpressionList();
    exprList->add(pathExpr);
    auto* listExpr = new ListExpression(exprList);
    auto* path = new YieldColumn(listExpr, new std::string(kPathStr));

    auto* columns = pathCtx_->qctx->objPool()->add(new YieldColumns());
    columns->addColumn(vid);
    columns->addColumn(path);

    auto* project = Project::make(pathCtx_->qctx, dep, columns);
    project->setColNames({kVid, kPathStr});
    project->setInputVar(inputVar);
    project->setOutputVar(inputVar);

    return project;
}

PlanNode* PathPlanner::multiPairStartVidDataSet(PlanNode* dep, const std::string& inputVar) {
    // col 0 is dst
    auto* dst = new YieldColumn(new ColumnExpression(0), new std::string(kDst));
    // col 1 is src
    auto* src = new YieldColumn(new ColumnExpression(1), new std::string(kSrc));
    // col 2 is cost
    auto* cost = new YieldColumn(new ConstantExpression(0), new std::string(kCostStr));
    // col 3 is list<path(only contain dst)>
    auto* pathExpr = new PathBuildExpression();
    pathExpr->add(std::make_unique<ColumnExpression>(0));

    auto* exprList = new ExpressionList();
    exprList->add(pathExpr);
    auto* listExpr = new ListExpression(exprList);
    auto* path = new YieldColumn(listExpr, new std::string(kPathStr));

    auto* columns = pathCtx_->qctx->objPool()->add(new YieldColumns());
    columns->addColumn(dst);
    columns->addColumn(src);
    columns->addColumn(cost);
    columns->addColumn(path);

    auto* project = Project::make(pathCtx_->qctx, dep, columns);
    project->setColNames({kDst, kSrc, kCostStr, kPathStr});
    project->setInputVar(inputVar);
    project->setOutputVar(inputVar);

    return project;
}

SubPlan PathPlanner::allPairLoopDepPlan() {
    SubPlan subPlan = buildRuntimeVidPlan();
    subPlan.root = allPairStartVidDataSet(subPlan.root, pathCtx_->fromVidsVar);
    subPlan.tail = subPlan.tail == nullptr ? subPlan.root : subPlan.tail;
    subPlan.root = allPairStartVidDataSet(subPlan.root, pathCtx_->toVidsVar);
    return subPlan;
}

SubPlan PathPlanner::multiPairLoopDepPlan() {
    SubPlan subPlan = buildRuntimeVidPlan();
    subPlan.root = multiPairStartVidDataSet(subPlan.root, pathCtx_->fromVidsVar);
    subPlan.tail = subPlan.tail == nullptr ? subPlan.root : subPlan.tail;
    subPlan.root = multiPairStartVidDataSet(subPlan.root, pathCtx_->toVidsVar);

    /*
    *  Create the Cartesian product of the fromVid set and the toVid set.
    *  When a pair of paths (<from>-[]-><to>) is found,
    *  delete it from the DataSet of cartesianProduct->outputVar().
    *  When the DataSet of cartesianProduct->outputVar() is empty
    *  terminate the execution early
    */
    auto* cartesianProduct = CartesianProduct::make(pathCtx_->qctx, subPlan.root);
    cartesianProduct->addVar(pathCtx_->fromVidsVar);
    cartesianProduct->addVar(pathCtx_->toVidsVar);
    subPlan.root = cartesianProduct;
    return subPlan;
}

PlanNode* PathPlanner::singlePairPath(PlanNode* dep, bool reverse) {
    const auto& vidsVar = reverse ? pathCtx_->toVidsVar : pathCtx_->fromVidsVar;
    auto qctx = pathCtx_->qctx;
    auto* src = qctx->objPool()->add(new ColumnExpression(0));

    auto* gn = GetNeighbors::make(qctx, dep, pathCtx_->space.id);
    gn->setSrc(src);
    gn->setEdgeProps(buildEdgeProps(reverse));
    gn->setInputVar(vidsVar);
    gn->setDedup();

    auto* path = BFSShortestPath::make(qctx, gn);
    path->setOutputVar(vidsVar);
    path->setColNames({kVid, kEdgeStr});

    // singlePair startVid dataset
    const auto& starts = reverse ? pathCtx_->to : pathCtx_->from;
    DataSet ds;
    ds.colNames = {kVid, kEdgeStr};
    Row row;
    row.values.emplace_back(starts.vids.front());
    row.values.emplace_back(Value::kEmpty);
    ds.rows.emplace_back(std::move(row));
    qctx->ectx()->setResult(vidsVar, ResultBuilder().value(Value(std::move(ds))).finish());
    return path;
}

SubPlan PathPlanner::singlePairPlan(PlanNode* dep) {
    auto* forwardPath = singlePairPath(dep, false);
    auto* backwardPath = singlePairPath(dep, true);
    auto qctx = pathCtx_->qctx;

    auto* conjunct = ConjunctPath::make(
        qctx, forwardPath, backwardPath, ConjunctPath::PathKind::kBiBFS, pathCtx_->steps.steps());
    conjunct->setColNames({kPathStr});

    auto* loopCondition = qctx->objPool()->add(
        singlePairLoopCondition(pathCtx_->steps.steps(), conjunct->outputVar()));
    auto* loop = Loop::make(qctx, nullptr, conjunct, loopCondition);
    auto* dc = DataCollect::make(qctx, DataCollect::DCKind::kBFSShortest);
    dc->setInputVars({conjunct->outputVar()});
    dc->addDep(loop);
    dc->setColNames({"path"});

    SubPlan subPlan;
    subPlan.root = dc;
    subPlan.tail = loop;
    return subPlan;
}

PlanNode* PathPlanner::allPairPath(PlanNode* dep, bool reverse) {
    const auto& vidsVar = reverse ? pathCtx_->toVidsVar : pathCtx_->fromVidsVar;
    auto qctx = pathCtx_->qctx;
    auto* src = qctx->objPool()->add(new ColumnExpression(0));

    auto* gn = GetNeighbors::make(qctx, dep, pathCtx_->space.id);
    gn->setSrc(src);
    gn->setEdgeProps(buildEdgeProps(reverse));
    gn->setInputVar(vidsVar);
    gn->setDedup();

    auto* path = ProduceAllPaths::make(qctx, gn);
    path->setOutputVar(vidsVar);
    path->setColNames({kVid, kPathStr});
    return path;
}

SubPlan PathPlanner::allPairPlan(PlanNode* dep) {
    auto* forwardPath = allPairPath(dep, false);
    auto* backwardPath = allPairPath(dep, true);
    auto qctx = pathCtx_->qctx;

    auto* conjunct = ConjunctPath::make(qctx,
                                        forwardPath,
                                        backwardPath,
                                        ConjunctPath::PathKind::kAllPaths,
                                        pathCtx_->steps.steps());
    conjunct->setNoLoop(pathCtx_->noLoop);
    conjunct->setColNames({kPathStr});

    SubPlan loopDepPlan = allPairLoopDepPlan();
    auto* loopCondition = qctx->objPool()->add(allPairLoopCondition(pathCtx_->steps.steps()));
    auto* loop = Loop::make(qctx, loopDepPlan.root, conjunct, loopCondition);

    auto* dc = DataCollect::make(qctx, DataCollect::DCKind::kAllPaths);
    dc->addDep(loop);
    dc->setInputVars({conjunct->outputVar()});
    dc->setColNames({"path"});

    SubPlan subPlan;
    subPlan.root = dc;
    subPlan.tail = loopDepPlan.tail;
    return subPlan;
}

PlanNode* PathPlanner::multiPairPath(PlanNode* dep, bool reverse) {
    const auto& vidsVar = reverse ? pathCtx_->toVidsVar : pathCtx_->fromVidsVar;
    auto qctx = pathCtx_->qctx;
    auto* src = qctx->objPool()->add(new ColumnExpression(0));

    auto* gn = GetNeighbors::make(qctx, dep, pathCtx_->space.id);
    gn->setSrc(src);
    gn->setEdgeProps(buildEdgeProps(reverse));
    gn->setInputVar(vidsVar);
    gn->setDedup();

    auto* path = ProduceSemiShortestPath::make(qctx, gn);
    path->setOutputVar(vidsVar);
    path->setColNames({kDst, kSrc, kCostStr, kPathStr});

    return path;
}

SubPlan PathPlanner::multiPairPlan(PlanNode* dep) {
    auto* forwardPath = multiPairPath(dep, false);
    auto* backwardPath = multiPairPath(dep, true);
    auto qctx = pathCtx_->qctx;

    auto* conjunct = ConjunctPath::make(
        qctx, forwardPath, backwardPath, ConjunctPath::PathKind::kFloyd, pathCtx_->steps.steps());
    conjunct->setColNames({kPathStr, kCostStr});

    SubPlan loopDepPlan = multiPairLoopDepPlan();
    // loopDepPlan.root is cartesianProduct
    const auto& endConditionVar = loopDepPlan.root->outputVar();
    conjunct->setConditionalVar(endConditionVar);
    auto* loopCondition =
        qctx->objPool()->add(multiPairLoopCondition(pathCtx_->steps.steps(), endConditionVar));
    auto* loop = Loop::make(qctx, loopDepPlan.root, conjunct, loopCondition);

    auto* dc = DataCollect::make(qctx, DataCollect::DCKind::kMultiplePairShortest);
    dc->addDep(loop);
    dc->setInputVars({conjunct->outputVar()});
    dc->setColNames({"path"});

    SubPlan subPlan;
    subPlan.root = dc;
    subPlan.tail = loopDepPlan.tail;
    return subPlan;
}

PlanNode* PathPlanner::buildVertexPlan(PlanNode* dep, const std::string& input) {
    auto qctx = pathCtx_->qctx;

    // col 0 of the input is path
    auto args = new ArgumentList();
    args->addArgument(std::make_unique<ColumnExpression>(0));
    auto funNodes = new FunctionCallExpression(new std::string("nodes"), args);

    auto* column = new YieldColumn(funNodes, new std::string("nodes"));
    auto* columns = qctx->objPool()->add(new YieldColumns());
    columns->addColumn(column);

    auto* project = Project::make(qctx, dep, columns);
    project->setColNames({"nodes"});
    project->setInputVar(input);

    // col 0 of the project->output is [node...]
    auto* unwindExpr = qctx->objPool()->add(new ColumnExpression(0));
    auto* unwind = Unwind::make(qctx, project, unwindExpr);
    unwind->setColNames({"nodes"});

    // extract vid from vertex, col 0 is vertex
    auto idArgs = new ArgumentList();
    idArgs->addArgument(std::make_unique<ColumnExpression>(1));
    auto* src = qctx->objPool()->add(new FunctionCallExpression(new std::string("id"), idArgs));
    // get all vertexprop
    auto vertexProp = SchemaUtil::getAllVertexProp(qctx, pathCtx_->space);
    auto* getVertices = GetVertices::make(
        qctx, unwind, pathCtx_->space.id, src, std::move(vertexProp).value(), {}, true);

    return getVertices;
}

PlanNode* PathPlanner::buildEdgePlan(PlanNode* dep, const std::string& input) {
    auto qctx = pathCtx_->qctx;

    // col 0 of the input is path
    auto args = new ArgumentList();
    args->addArgument(std::make_unique<ColumnExpression>(0));
    auto funEdges = new FunctionCallExpression(new std::string("relationships"), args);

    auto* column = new YieldColumn(funEdges, new std::string("edges"));
    auto* columns = qctx->objPool()->add(new YieldColumns());
    columns->addColumn(column);

    auto* project = Project::make(qctx, dep, columns);
    project->setColNames({"edges"});
    project->setInputVar(input);

    // col 0 of the project->output() is [edge...]
    auto* unwindExpr = qctx->objPool()->add(new ColumnExpression(0));
    auto* unwind = Unwind::make(qctx, project, unwindExpr);
    unwind->setColNames({"edges"});

    // extract src from edge
    auto srcArgs = new ArgumentList();
    srcArgs->addArgument(std::make_unique<ColumnExpression>(1));
    auto* src = qctx->objPool()->add(new FunctionCallExpression(new std::string("src"), srcArgs));
    // extract dst from edge
    auto dstArgs = new ArgumentList();
    dstArgs->addArgument(std::make_unique<ColumnExpression>(1));
    auto* dst = qctx->objPool()->add(new FunctionCallExpression(new std::string("dst"), dstArgs));
    // extract rank from edge
    auto rankArgs = new ArgumentList();
    rankArgs->addArgument(std::make_unique<ColumnExpression>(1));
    auto* rank =
        qctx->objPool()->add(new FunctionCallExpression(new std::string("rank"), rankArgs));
    // type
    auto typeArgs = new ArgumentList();
    typeArgs->addArgument(std::make_unique<ColumnExpression>(1));
    auto* type =
        qctx->objPool()->add(new FunctionCallExpression(new std::string("typeid"), typeArgs));
    // prepare edgetype
    auto edgeProp = SchemaUtil::getEdgeProp(qctx, pathCtx_->space, pathCtx_->over.edgeTypes);
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

/*
          The Plan looks like this:
                 +--------+---------+
             +-->+   PassThrough    +<----+
             |   +------------------+     |
    +--------+---------+        +---------+------------+
    |  Project(Nodes)  |        |Project(RelationShips)|
    +--------+---------+        +---------+------------+
             |                            |
    +--------+---------+        +---------+--------+
    |      Unwind      |        |      Unwind      |
    +--------+---------+        +---------+--------+
             |                            |
    +--------+---------+        +---------+--------+
    |   GetVertices    |        |    GetEdges      |
    +--------+---------+        +---------+--------+
             |                            |
             +------------+---------------+
                          |
                 +--------+---------+
                 |   DataCollect    |
                 +--------+---------+
*/
PlanNode* PathPlanner::buildPathProp(PlanNode* dep) {
    auto qctx = pathCtx_->qctx;
    auto* pt = PassThroughNode::make(qctx, dep);

    auto* vertexPlan = buildVertexPlan(pt, dep->outputVar());
    auto* edgePlan = buildEdgePlan(pt, dep->outputVar());

    auto* dc = DataCollect::make(qctx, DataCollect::DCKind::kPathProp);
    dc->addDep(vertexPlan);
    dc->addDep(edgePlan);
    dc->setInputVars({vertexPlan->outputVar(), edgePlan->outputVar(), dep->outputVar()});
    dc->setColNames({"path"});
    return dc;
}

StatusOr<SubPlan> PathPlanner::transform(AstContext* astCtx) {
    pathCtx_ = static_cast<PathContext *>(astCtx);

    buildStart(pathCtx_->from, pathCtx_->fromVidsVar, false);
    buildStart(pathCtx_->to, pathCtx_->toVidsVar, true);

    auto* startNode = StartNode::make(pathCtx_->qctx);
    auto* pt = PassThroughNode::make(pathCtx_->qctx, startNode);

    SubPlan subPlan;
    do {
        if (!pathCtx_->isShortest || pathCtx_->noLoop) {
            subPlan = allPairPlan(pt);
            break;
        }
        if (pathCtx_->from.vids.size() == 1 && pathCtx_->to.vids.size() == 1) {
            subPlan = singlePairPlan(pt);
            break;
        }
        subPlan = multiPairPlan(pt);
    } while (0);
    // get path's property
    if (pathCtx_->withProp) {
        subPlan.root = buildPathProp(subPlan.root);
    }

    return subPlan;
}

}  // namespace graph
}  // namespace nebula
