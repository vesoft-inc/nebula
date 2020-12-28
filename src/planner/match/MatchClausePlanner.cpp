/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/MatchClausePlanner.h"

#include "context/ast/QueryAstContext.h"
#include "planner/Query.h"
#include "planner/match/Expand.h"
#include "planner/match/MatchSolver.h"
#include "planner/match/SegmentsConnector.h"
#include "planner/match/StartVidFinder.h"
#include "planner/match/WhereClausePlanner.h"
#include "util/ExpressionUtils.h"
#include "visitor/RewriteMatchLabelVisitor.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> MatchClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
    if (clauseCtx->kind != CypherClauseKind::kMatch) {
        return Status::Error("Not a valid context for MatchClausePlanner.");
    }

    auto* matchClauseCtx = static_cast<MatchClauseContext*>(clauseCtx);
    auto& nodeInfos = matchClauseCtx->nodeInfos;
    auto& edgeInfos = matchClauseCtx->edgeInfos;
    SubPlan matchClausePlan;
    int64_t startIndex = -1;

    NG_RETURN_IF_ERROR(findStarts(matchClauseCtx, startIndex, matchClausePlan));
    NG_RETURN_IF_ERROR(expand(nodeInfos, edgeInfos, matchClauseCtx, startIndex, matchClausePlan));
    NG_RETURN_IF_ERROR(projectColumnsBySymbols(matchClauseCtx, &matchClausePlan));
    NG_RETURN_IF_ERROR(appendFilterPlan(matchClauseCtx, matchClausePlan));
    return matchClausePlan;
}

Status MatchClausePlanner::findStarts(MatchClauseContext* matchClauseCtx,
                                      int64_t& startIndex,
                                      SubPlan& matchClausePlan) {
    auto& nodeInfos = matchClauseCtx->nodeInfos;
    auto& edgeInfos = matchClauseCtx->edgeInfos;
    auto& startVidFinders = StartVidFinder::finders();
    // Find the start plan node
    for (size_t i = 0; i < nodeInfos.size(); ++i) {
        for (auto& finder : startVidFinders) {
            auto nodeCtx = NodeContext(matchClauseCtx, &nodeInfos[i]);
            auto finderObj = finder();
            if (finderObj->match(&nodeCtx)) {
                auto plan = finderObj->transform(&nodeCtx);
                if (!plan.ok()) {
                    return plan.status();
                }
                matchClausePlan = std::move(plan).value();
                startIndex = i;
                initialExpr_ = nodeCtx.initialExpr;
                VLOG(1) << "Find starts: " << startIndex
                    << " node: " << matchClausePlan.root->outputVar()
                    << " colNames: " << folly::join(",", matchClausePlan.root->colNames());
                break;
            }

            if (i != nodeInfos.size() - 1) {
                auto edgeCtx = EdgeContext(matchClauseCtx, &edgeInfos[i]);
                if (finderObj->match(&edgeCtx)) {
                    auto plan = finderObj->transform(&edgeCtx);
                    if (!plan.ok()) {
                        return plan.status();
                    }
                    matchClausePlan = std::move(plan).value();
                    startIndex = i;
                    break;
                }
            }
        }
        if (startIndex != -1) {
            break;
        }
    }
    if (startIndex < 0) {
        return Status::Error("Can't solve the start vids from the sentence: %s",
                             matchClauseCtx->sentence->toString().c_str());
    }

    return Status::OK();
}

Status MatchClausePlanner::expand(const std::vector<NodeInfo>& nodeInfos,
                                  const std::vector<EdgeInfo>& edgeInfos,
                                  MatchClauseContext* matchClauseCtx,
                                  int64_t startIndex,
                                  SubPlan& subplan) {
    // Do expand from startIndex and connect the the subplans.
    // TODO: Only support start from the head node now.
    if (startIndex != 0) {
        return Status::Error("Only support start from the head node parttern.");
    }

    std::vector<std::string> joinColNames = {folly::stringPrintf("%s_%d", kPathStr, 0)};
    for (size_t i = 0; i < edgeInfos.size(); ++i) {
        auto left = subplan.root;
        auto status = std::make_unique<Expand>(matchClauseCtx, &initialExpr_)
                          ->doExpand(nodeInfos[i], edgeInfos[i], subplan.root, &subplan);
        if (!status.ok()) {
            return status;
        }
        if (i > 0) {
            auto right = subplan.root;
            VLOG(1) << "left: " << folly::join(",", left->colNames())
                << " right: " << folly::join(",", right->colNames());
            subplan.root = SegmentsConnector::innerJoinSegments(matchClauseCtx->qctx, left, right);
            joinColNames.emplace_back(folly::stringPrintf("%s_%lu", kPathStr, i));
            subplan.root->setColNames(joinColNames);
        }
    }

    VLOG(1) << "root: " << subplan.root->outputVar() << " tail: " << subplan.tail->outputVar();
    auto left = subplan.root;
    NG_RETURN_IF_ERROR(appendFetchVertexPlan(
        nodeInfos.back().filter, matchClauseCtx->qctx, matchClauseCtx->space, &subplan));
    if (!edgeInfos.empty()) {
        auto right = subplan.root;
        VLOG(1) << "left: " << folly::join(",", left->colNames())
            << " right: " << folly::join(",", right->colNames());
        subplan.root = SegmentsConnector::innerJoinSegments(matchClauseCtx->qctx, left, right);
        joinColNames.emplace_back(folly::stringPrintf("%s_%lu", kPathStr, edgeInfos.size()));
        subplan.root->setColNames(joinColNames);
    }

    VLOG(1) << "root: " << subplan.root->outputVar() << " tail: " << subplan.tail->outputVar();
    return Status::OK();
}

Status MatchClausePlanner::appendFetchVertexPlan(const Expression* nodeFilter,
                                                 QueryContext* qctx,
                                                 SpaceInfo& space,
                                                 SubPlan* plan) {
    MatchSolver::extractAndDedupVidColumn(qctx, &initialExpr_, plan);
    auto srcExpr = ExpressionUtils::inputPropExpr(kVid);
    auto gv = GetVertices::make(
        qctx, plan->root, space.id, qctx->objPool()->add(srcExpr.release()), {}, {});

    PlanNode* root = gv;
    if (nodeFilter != nullptr) {
        auto filter = nodeFilter->clone().release();
        RewriteMatchLabelVisitor visitor(
            [](const Expression* expr) -> Expression *{
            DCHECK(expr->kind() == Expression::Kind::kLabelAttribute ||
                expr->kind() == Expression::Kind::kLabel);
            // filter prop
            if (expr->kind() == Expression::Kind::kLabelAttribute) {
                auto la = static_cast<const LabelAttributeExpression*>(expr);
                return new AttributeExpression(
                    new VertexExpression(), la->right()->clone().release());
            }
            // filter tag
            return new VertexExpression();
        });
        filter->accept(&visitor);
        root = Filter::make(qctx, root, filter);
    }

    // normalize all columns to one
    auto columns = qctx->objPool()->add(new YieldColumns);
    auto pathExpr = std::make_unique<PathBuildExpression>();
    pathExpr->add(std::make_unique<VertexExpression>());
    columns->addColumn(new YieldColumn(pathExpr.release()));
    plan->root = Project::make(qctx, root, columns);
    plan->root->setColNames({kPathStr});
    return Status::OK();
}

Status MatchClausePlanner::projectColumnsBySymbols(MatchClauseContext* matchClauseCtx,
                                                   SubPlan* plan) {
    auto qctx = matchClauseCtx->qctx;
    auto& nodeInfos = matchClauseCtx->nodeInfos;
    auto& edgeInfos = matchClauseCtx->edgeInfos;
    auto columns = qctx->objPool()->add(new YieldColumns);
    auto input = plan->root;
    const auto& inColNames = input->colNamesRef();
    DCHECK_EQ(inColNames.size(), nodeInfos.size());
    std::vector<std::string> colNames;

    auto addNode = [&, this](size_t i) {
        auto& nodeInfo = nodeInfos[i];
        if (nodeInfo.alias != nullptr && !nodeInfo.anonymous) {
            columns->addColumn(buildVertexColumn(inColNames[i], *nodeInfo.alias));
            colNames.emplace_back(*nodeInfo.alias);
        }
    };

    for (size_t i = 0; i < edgeInfos.size(); i++) {
        addNode(i);
        auto& edgeInfo = edgeInfos[i];
        if (edgeInfo.alias != nullptr && !edgeInfo.anonymous) {
            columns->addColumn(buildEdgeColumn(inColNames[i], edgeInfo));
            colNames.emplace_back(*edgeInfo.alias);
        }
    }

    // last vertex
    DCHECK(!nodeInfos.empty());
    addNode(nodeInfos.size() - 1);

    const auto& aliases = matchClauseCtx->aliases;
    auto iter = std::find_if(aliases.begin(), aliases.end(), [](const auto& alias) {
        return alias.second == AliasType::kPath;
    });
    std::string alias = iter != aliases.end() ? iter->first : qctx->vctx()->anonColGen()->getCol();
    columns->addColumn(buildPathColumn(alias, input));
    colNames.emplace_back(alias);

    auto project = Project::make(qctx, input, columns);
    project->setColNames(std::move(colNames));

    plan->root = MatchSolver::filtPathHasSameEdge(project, alias, qctx);
    VLOG(1) << "root: " << plan->root->outputVar() << " tail: " << plan->tail->outputVar();
    return Status::OK();
}

YieldColumn* MatchClausePlanner::buildVertexColumn(const std::string& colName,
                                                   const std::string& alias) const {
    auto colExpr = ExpressionUtils::inputPropExpr(colName);
    // startNode(path) => head node of path
    auto args = std::make_unique<ArgumentList>();
    args->addArgument(std::move(colExpr));
    auto fn = std::make_unique<std::string>("startNode");
    auto firstVertexExpr = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    return new YieldColumn(firstVertexExpr.release(), new std::string(alias));
}

YieldColumn* MatchClausePlanner::buildEdgeColumn(const std::string& colName, EdgeInfo& edge) const {
    auto colExpr = ExpressionUtils::inputPropExpr(colName);
    // relationships(p)
    auto args = std::make_unique<ArgumentList>();
    args->addArgument(std::move(colExpr));
    auto fn = std::make_unique<std::string>("relationships");
    auto relExpr = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    Expression* expr = nullptr;
    if (edge.range != nullptr) {
        expr = relExpr.release();
    } else {
        // Get first edge in path list [e1, e2, ...]
        auto idxExpr = std::make_unique<ConstantExpression>(0);
        auto subExpr = std::make_unique<SubscriptExpression>(relExpr.release(), idxExpr.release());
        expr = subExpr.release();
    }
    return new YieldColumn(expr, new std::string(*edge.alias));
}

YieldColumn* MatchClausePlanner::buildPathColumn(const std::string& alias,
                                                 const PlanNode* input) const {
    auto pathExpr = std::make_unique<PathBuildExpression>();
    for (const auto& colName : input->colNamesRef()) {
        pathExpr->add(ExpressionUtils::inputPropExpr(colName));
    }
    return new YieldColumn(pathExpr.release(), new std::string(alias));
}

Status MatchClausePlanner::appendFilterPlan(MatchClauseContext* matchClauseCtx, SubPlan& subplan) {
    if (matchClauseCtx->where == nullptr) {
        return Status::OK();
    }

    auto wherePlan = std::make_unique<WhereClausePlanner>()->transform(matchClauseCtx->where.get());
    NG_RETURN_IF_ERROR(wherePlan);
    auto plan = std::move(wherePlan).value();
    SegmentsConnector::addInput(plan.tail, subplan.root, true);
    subplan.root = plan.root;
    VLOG(1) << "root: " << subplan.root->outputVar() << " tail: " << subplan.tail->outputVar();
    return Status::OK();
}
}   // namespace graph
}   // namespace nebula
