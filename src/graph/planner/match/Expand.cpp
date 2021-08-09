/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/Expand.h"

#include "planner/plan/Logic.h"
#include "planner/plan/Query.h"
#include "planner/match/MatchSolver.h"
#include "planner/match/SegmentsConnector.h"
#include "util/AnonColGenerator.h"
#include "util/ExpressionUtils.h"
#include "visitor/RewriteVisitor.h"

using nebula::storage::cpp2::EdgeProp;
using nebula::storage::cpp2::VertexProp;
using PNKind = nebula::graph::PlanNode::Kind;

namespace nebula {
namespace graph {

static std::unique_ptr<std::vector<VertexProp>> genVertexProps() {
    return std::make_unique<std::vector<VertexProp>>();
}

std::unique_ptr<std::vector<storage::cpp2::EdgeProp>> Expand::genEdgeProps(const EdgeInfo& edge) {
    auto edgeProps = std::make_unique<std::vector<EdgeProp>>();
    for (auto edgeType : edge.edgeTypes) {
        auto edgeSchema =
            matchCtx_->qctx->schemaMng()->getEdgeSchema(matchCtx_->space.id, edgeType);

        switch (edge.direction) {
            case Direction::OUT_EDGE: {
                if (reversely_) {
                    edgeType = -edgeType;
                }
                break;
            }
            case Direction::IN_EDGE: {
                if (!reversely_) {
                    edgeType = -edgeType;
                }
                break;
            }
            case Direction::BOTH: {
                EdgeProp edgeProp;
                edgeProp.set_type(-edgeType);
                std::vector<std::string> props{kSrc, kType, kRank, kDst};
                for (std::size_t i = 0; i < edgeSchema->getNumFields(); ++i) {
                    props.emplace_back(edgeSchema->getFieldName(i));
                }
                edgeProp.set_props(std::move(props));
                edgeProps->emplace_back(std::move(edgeProp));
                break;
            }
        }
        EdgeProp edgeProp;
        edgeProp.set_type(edgeType);
        std::vector<std::string> props{kSrc, kType, kRank, kDst};
        for (std::size_t i = 0; i < edgeSchema->getNumFields(); ++i) {
            props.emplace_back(edgeSchema->getFieldName(i));
        }
        edgeProp.set_props(std::move(props));
        edgeProps->emplace_back(std::move(edgeProp));
    }
    return edgeProps;
}

static Expression* mergePathColumnsExpr(ObjectPool* pool,
                                        const std::string& lcol,
                                        const std::string& rcol) {
    auto expr = PathBuildExpression::make(pool);
    expr->add(InputPropertyExpression::make(pool, lcol));
    expr->add(InputPropertyExpression::make(pool, rcol));
    return expr;
}

static Expression* buildPathExpr(ObjectPool* pool) {
    auto expr = PathBuildExpression::make(pool);
    expr->add(VertexExpression::make(pool));
    expr->add(EdgeExpression::make(pool));
    return expr;
}

Status Expand::doExpand(const NodeInfo& node, const EdgeInfo& edge, SubPlan* plan) {
    NG_RETURN_IF_ERROR(expandSteps(node, edge, plan));
    NG_RETURN_IF_ERROR(filterDatasetByPathLength(edge, plan->root, plan));
    return Status::OK();
}

// Build subplan: Project->Dedup->GetNeighbors->[Filter]->Project2->
// DataJoin->Project3->[Filter]->Passthrough->Loop->UnionAllVer
Status Expand::expandSteps(const NodeInfo& node, const EdgeInfo& edge, SubPlan* plan) {
    SubPlan subplan;
    int64_t startIndex = 0;
    auto minHop = edge.range ? edge.range->min() : 1;
    auto maxHop = edge.range ? edge.range->max() : 1;

    // Build first step
    // In the case of 0 step, src node is the dst node, return the vertex directly
    if (minHop == 0) {
        subplan = *plan;
        startIndex = 0;
        // Get vertex
        NG_RETURN_IF_ERROR(MatchSolver::appendFetchVertexPlan(node.filter,
                                                              matchCtx_->space,
                                                              matchCtx_->qctx,
                                                              &initialExpr_,
                                                              inputVar_,
                                                              subplan));
    } else {   // Case 1 to n steps
        startIndex = 1;
        // Expand first step from src
        NG_RETURN_IF_ERROR(expandStep(edge, dependency_, inputVar_, node.filter, &subplan));
    }
    // No need to further expand if maxHop is the start Index
    if (maxHop == startIndex) {
        plan->root = subplan.root;
        return Status::OK();
    }
    // Result of first step expansion
    PlanNode* firstStep = subplan.root;

    // Build Start node from first step
    SubPlan loopBodyPlan;
    PlanNode* startNode = StartNode::make(matchCtx_->qctx);
    startNode->setOutputVar(firstStep->outputVar());
    startNode->setColNames(firstStep->colNames());
    loopBodyPlan.tail = startNode;
    loopBodyPlan.root = startNode;

    // Construct loop body
    NG_RETURN_IF_ERROR(expandStep(edge,
                                  startNode,                // dep
                                  startNode->outputVar(),   // inputVar
                                  nullptr,
                                  &loopBodyPlan));

    NG_RETURN_IF_ERROR(collectData(startNode,           // left join node
                                   loopBodyPlan.root,   // right join node
                                   &firstStep,          // passThrough
                                   &subplan));
    // Union node
    auto body = subplan.root;

    // Loop condition
    auto condition = buildExpandCondition(body->outputVar(), startIndex, maxHop);

    // Create loop
    auto* loop = Loop::make(matchCtx_->qctx, firstStep, body, condition);

    // Unionize the results of each expansion which are stored in the firstStep node
    auto uResNode = UnionAllVersionVar::make(matchCtx_->qctx, loop);
    uResNode->setInputVar(firstStep->outputVar());
    uResNode->setColNames({kPathStr});

    subplan.root = uResNode;
    plan->root = subplan.root;
    return Status::OK();
}

// Build subplan: Project->Dedup->GetNeighbors->[Filter]->Project
Status Expand::expandStep(const EdgeInfo& edge,
                          PlanNode* dep,
                          const std::string& inputVar,
                          const Expression* nodeFilter,
                          SubPlan* plan) {
    auto qctx = matchCtx_->qctx;
    auto* pool = qctx->objPool();
    // Extract dst vid from input project node which output dataset format is: [v1,e1,...,vn,en]
    SubPlan curr;
    curr.root = dep;
    MatchSolver::extractAndDedupVidColumn(qctx, &initialExpr_, dep, inputVar, curr);
    // [GetNeighbors]
    auto gn = GetNeighbors::make(qctx, curr.root, matchCtx_->space.id);
    auto srcExpr = InputPropertyExpression::make(pool, kVid);
    gn->setSrc(srcExpr);
    gn->setVertexProps(genVertexProps());
    gn->setEdgeProps(genEdgeProps(edge));
    gn->setEdgeDirection(edge.direction);

    PlanNode* root = gn;
    if (nodeFilter != nullptr) {
        auto* newFilter = MatchSolver::rewriteLabel2Vertex(qctx, nodeFilter);
        auto filterNode = Filter::make(matchCtx_->qctx, root, newFilter);
        filterNode->setColNames(root->colNames());
        root = filterNode;
    }

    if (edge.filter != nullptr) {
        auto* newFilter = MatchSolver::rewriteLabel2Edge(qctx, edge.filter);
        auto filterNode = Filter::make(qctx, root, newFilter);
        filterNode->setColNames(root->colNames());
        root = filterNode;
    }

    auto listColumns = saveObject(new YieldColumns);
    listColumns->addColumn(new YieldColumn(buildPathExpr(pool), kPathStr));
    // [Project]
    root = Project::make(qctx, root, listColumns);
    root->setColNames({kPathStr});

    plan->root = root;
    plan->tail = curr.tail;
    return Status::OK();
}

// Build subplan: DataJoin->Project->Filter
Status Expand::collectData(const PlanNode* joinLeft,
                           const PlanNode* joinRight,
                           PlanNode** passThrough,
                           SubPlan* plan) {
    auto qctx = matchCtx_->qctx;
    // [dataJoin] read start node (joinLeft)
    auto join = SegmentsConnector::innerJoinSegments(qctx, joinLeft, joinRight);
    auto lpath = folly::stringPrintf("%s_%d", kPathStr, 0);
    auto rpath = folly::stringPrintf("%s_%d", kPathStr, 1);
    join->setColNames({lpath, rpath});
    plan->tail = join;

    auto columns = saveObject(new YieldColumns);
    auto listExpr = mergePathColumnsExpr(qctx->objPool(), lpath, rpath);
    columns->addColumn(new YieldColumn(listExpr));
    // [Project]
    auto project = Project::make(qctx, join, columns);
    project->setColNames({kPathStr});
    // [Filter]
    auto filter = MatchSolver::filtPathHasSameEdge(project, kPathStr, qctx);
    // Update start node
    filter->setOutputVar((*passThrough)->outputVar());
    plan->root = filter;
    return Status::OK();
}

Status Expand::filterDatasetByPathLength(const EdgeInfo& edge, PlanNode* input, SubPlan* plan) {
    auto qctx = matchCtx_->qctx;
    auto* pool = qctx->objPool();

    // Filter rows whose edges number less than min hop
    auto args = ArgumentList::make(pool);
    // Expr: length(relationships(p)) >= minHop
    auto pathExpr = InputPropertyExpression::make(pool, kPathStr);
    args->addArgument(pathExpr);
    auto edgeExpr = FunctionCallExpression::make(pool, "length", args);
    auto minHop = edge.range == nullptr ? 1 : edge.range->min();
    auto minHopExpr = ConstantExpression::make(pool, minHop);
    auto expr = RelationalExpression::makeGE(pool, edgeExpr, minHopExpr);

    auto filter = Filter::make(qctx, input, expr);
    filter->setColNames(input->colNames());
    plan->root = filter;
    return Status::OK();
}

// loopSteps{startIndex} <= maxHop &&  ($lastStepResult == empty || size($lastStepResult) != 0)
Expression* Expand::buildExpandCondition(const std::string& lastStepResult,
                                         int64_t startIndex,
                                         int64_t maxHop) const {
    VLOG(1) << "match expand maxHop: " << maxHop;
    auto pool = matchCtx_->qctx->objPool();
    auto loopSteps = matchCtx_->qctx->vctx()->anonVarGen()->getVar();
    matchCtx_->qctx->ectx()->setValue(loopSteps, startIndex);
    // ++loopSteps{startIndex} << maxHop
    auto stepCondition = ExpressionUtils::stepCondition(pool, loopSteps, maxHop);
    // lastStepResult == empty || size(lastStepReult) != 0
    auto* eqEmpty = RelationalExpression::makeEQ(pool,
                                                 VariableExpression::make(pool, lastStepResult),
                                                 ConstantExpression::make(pool, Value()));
    auto neZero = ExpressionUtils::neZeroCondition(pool, lastStepResult);
    auto* existValCondition = LogicalExpression::makeOr(pool, eqEmpty, neZero);
    return LogicalExpression::makeAnd(pool, stepCondition, existValCondition);
}

}   // namespace graph
}   // namespace nebula
