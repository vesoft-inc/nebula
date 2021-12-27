/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/MatchClausePlanner.h"

#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/match/MatchSolver.h"
#include "graph/planner/match/SegmentsConnector.h"
#include "graph/planner/match/StartVidFinder.h"
#include "graph/planner/match/WhereClausePlanner.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/SchemaUtil.h"
#include "graph/visitor/RewriteVisitor.h"

using JoinStrategyPos = nebula::graph::InnerJoinStrategy::JoinPos;

namespace nebula {
namespace graph {
static std::vector<std::string> genTraverseColNames(const std::vector<std::string>& inputCols,
                                                    const NodeInfo& node,
                                                    const EdgeInfo& edge) {
  auto cols = inputCols;
  cols.emplace_back(node.alias);
  cols.emplace_back(edge.alias);
  return cols;
}

static std::vector<std::string> genAppendVColNames(const std::vector<std::string>& inputCols,
                                                   const NodeInfo& node) {
  auto cols = inputCols;
  cols.emplace_back(node.alias);
  return cols;
}

static Expression* genNextTraverseStart(ObjectPool* pool, const EdgeInfo& edge) {
  auto args = ArgumentList::make(pool);
  args->addArgument(InputPropertyExpression::make(pool, edge.alias));
  return FunctionCallExpression::make(pool, "none_direct_dst", args);
}

static Expression* genVertexFilter(const NodeInfo& node) {
  return node.filter;
}

static Expression* genEdgeFilter(const EdgeInfo& edge) {
  return edge.filter;
}

static StatusOr<std::unique_ptr<std::vector<VertexProp>>> genVertexProps(const NodeInfo& node,
                                                                         QueryContext* qctx,
                                                                         GraphSpaceID spaceId) {
  // TODO
  UNUSED(node);
  return SchemaUtil::getAllVertexProp(qctx, spaceId, true);
}

static std::unique_ptr<std::vector<storage::cpp2::EdgeProp>> genEdgeProps(const EdgeInfo& edge,
                                                                          bool reversely,
                                                                          QueryContext* qctx,
                                                                          GraphSpaceID spaceId) {
  auto edgeProps = std::make_unique<std::vector<EdgeProp>>();
  for (auto edgeType : edge.edgeTypes) {
    auto edgeSchema = qctx->schemaMng()->getEdgeSchema(spaceId, edgeType);

    switch (edge.direction) {
      case Direction::OUT_EDGE: {
        if (reversely) {
          edgeType = -edgeType;
        }
        break;
      }
      case Direction::IN_EDGE: {
        if (!reversely) {
          edgeType = -edgeType;
        }
        break;
      }
      case Direction::BOTH: {
        EdgeProp edgeProp;
        edgeProp.type_ref() = -edgeType;
        std::vector<std::string> props{kSrc, kType, kRank, kDst};
        for (std::size_t i = 0; i < edgeSchema->getNumFields(); ++i) {
          props.emplace_back(edgeSchema->getFieldName(i));
        }
        edgeProp.props_ref() = std::move(props);
        edgeProps->emplace_back(std::move(edgeProp));
        break;
      }
    }
    EdgeProp edgeProp;
    edgeProp.type_ref() = edgeType;
    std::vector<std::string> props{kSrc, kType, kRank, kDst};
    for (std::size_t i = 0; i < edgeSchema->getNumFields(); ++i) {
      props.emplace_back(edgeSchema->getFieldName(i));
    }
    edgeProp.props_ref() = std::move(props);
    edgeProps->emplace_back(std::move(edgeProp));
  }
  return edgeProps;
}

StatusOr<SubPlan> MatchClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
  if (clauseCtx->kind != CypherClauseKind::kMatch) {
    return Status::Error("Not a valid context for MatchClausePlanner.");
  }

  auto* matchClauseCtx = static_cast<MatchClauseContext*>(clauseCtx);
  auto& nodeInfos = matchClauseCtx->nodeInfos;
  auto& edgeInfos = matchClauseCtx->edgeInfos;
  SubPlan matchClausePlan;
  size_t startIndex = 0;
  bool startFromEdge = false;

  NG_RETURN_IF_ERROR(findStarts(matchClauseCtx, startFromEdge, startIndex, matchClausePlan));
  NG_RETURN_IF_ERROR(
      expand(nodeInfos, edgeInfos, matchClauseCtx, startFromEdge, startIndex, matchClausePlan));
  NG_RETURN_IF_ERROR(projectColumnsBySymbols(matchClauseCtx, matchClausePlan));
  NG_RETURN_IF_ERROR(appendFilterPlan(matchClauseCtx, matchClausePlan));
  return matchClausePlan;
}

Status MatchClausePlanner::findStarts(MatchClauseContext* matchClauseCtx,
                                      bool& startFromEdge,
                                      size_t& startIndex,
                                      SubPlan& matchClausePlan) {
  auto& nodeInfos = matchClauseCtx->nodeInfos;
  auto& edgeInfos = matchClauseCtx->edgeInfos;
  auto& startVidFinders = StartVidFinder::finders();
  bool foundStart = false;
  // Find the start plan node
  for (auto& finder : startVidFinders) {
    for (size_t i = 0; i < nodeInfos.size() && !foundStart; ++i) {
      auto nodeCtx = NodeContext(matchClauseCtx, &nodeInfos[i]);
      auto nodeFinder = finder();
      if (nodeFinder->match(&nodeCtx)) {
        auto plan = nodeFinder->transform(&nodeCtx);
        if (!plan.ok()) {
          return plan.status();
        }
        matchClausePlan = std::move(plan).value();
        startIndex = i;
        foundStart = true;
        initialExpr_ = nodeCtx.initialExpr->clone();
        VLOG(1) << "Find starts: " << startIndex << ", Pattern has " << edgeInfos.size()
                << " edges, root: " << matchClausePlan.root->outputVar()
                << ", colNames: " << folly::join(",", matchClausePlan.root->colNames());
        break;
      }

      if (i != nodeInfos.size() - 1) {
        auto edgeCtx = EdgeContext(matchClauseCtx, &edgeInfos[i]);
        auto edgeFinder = finder();
        if (edgeFinder->match(&edgeCtx)) {
          auto plan = edgeFinder->transform(&edgeCtx);
          if (!plan.ok()) {
            return plan.status();
          }
          matchClausePlan = std::move(plan).value();
          startFromEdge = true;
          startIndex = i;
          foundStart = true;
          initialExpr_ = edgeCtx.initialExpr->clone();
          break;
        }
      }
    }
    if (foundStart) {
      break;
    }
  }
  if (!foundStart) {
    return Status::SemanticError("Can't solve the start vids from the sentence: %s",
                                 matchClauseCtx->sentence->toString().c_str());
  }

  return Status::OK();
}

Status MatchClausePlanner::expand(const std::vector<NodeInfo>& nodeInfos,
                                  const std::vector<EdgeInfo>& edgeInfos,
                                  MatchClauseContext* matchClauseCtx,
                                  bool startFromEdge,
                                  size_t startIndex,
                                  SubPlan& subplan) {
  if (startFromEdge) {
    return expandFromEdge(nodeInfos, edgeInfos, matchClauseCtx, startIndex, subplan);
  } else {
    return expandFromNode(nodeInfos, edgeInfos, matchClauseCtx, startIndex, subplan);
  }
}

Status MatchClausePlanner::expandFromNode(const std::vector<NodeInfo>& nodeInfos,
                                          const std::vector<EdgeInfo>& edgeInfos,
                                          MatchClauseContext* matchClauseCtx,
                                          size_t startIndex,
                                          SubPlan& subplan) {
  DCHECK(!nodeInfos.empty() && startIndex < nodeInfos.size());
  if (startIndex == 0) {
    // Pattern: (start)-[]-...-()
    return rightExpandFromNode(nodeInfos, edgeInfos, matchClauseCtx, startIndex, subplan);
  }

  const auto& var = subplan.root->outputVar();
  if (startIndex == nodeInfos.size() - 1) {
    // Pattern: ()-[]-...-(start)
    return leftExpandFromNode(nodeInfos, edgeInfos, matchClauseCtx, startIndex, var, subplan);
  }

  // Pattern: ()-[]-...-(start)-...-[]-()
  NG_RETURN_IF_ERROR(
      rightExpandFromNode(nodeInfos, edgeInfos, matchClauseCtx, startIndex, subplan));
  NG_RETURN_IF_ERROR(leftExpandFromNode(
      nodeInfos, edgeInfos, matchClauseCtx, startIndex, subplan.root->outputVar(), subplan));

  return Status::OK();
}

Status MatchClausePlanner::leftExpandFromNode(const std::vector<NodeInfo>& nodeInfos,
                                              const std::vector<EdgeInfo>& edgeInfos,
                                              MatchClauseContext* matchClauseCtx,
                                              size_t startIndex,
                                              std::string inputVar,
                                              SubPlan& subplan) {
  Expression* nextTraverseStart = nullptr;
  auto qctx = matchClauseCtx->qctx;
  if (startIndex == nodeInfos.size() - 1) {
    nextTraverseStart = initialExpr_;
  } else {
    auto* pool = qctx->objPool();
    auto args = ArgumentList::make(pool);
    args->addArgument(InputPropertyExpression::make(pool, nodeInfos[startIndex].alias));
    nextTraverseStart = FunctionCallExpression::make(pool, "id", args);
  }
  auto spaceId = matchClauseCtx->space.id;
  bool reversely = true;
  for (size_t i = startIndex; i > 0; --i) {
    auto& node = nodeInfos[i];
    auto& edge = edgeInfos[i - 1];
    auto traverse = Traverse::make(qctx, subplan.root, spaceId);
    traverse->setSrc(nextTraverseStart);
    auto vertexProps = genVertexProps(node, qctx, spaceId);
    NG_RETURN_IF_ERROR(vertexProps);
    traverse->setVertexProps(std::move(vertexProps).value());
    traverse->setEdgeProps(genEdgeProps(edge, reversely, qctx, spaceId));
    traverse->setVertexFilter(genVertexFilter(node));
    traverse->setEdgeFilter(genEdgeFilter(edge));
    traverse->setEdgeDirection(edge.direction);
    traverse->setColNames(genTraverseColNames(subplan.root->colNames(), node, edge));
    traverse->setStepRange(edge.range);
    traverse->setDedup();
    subplan.root = traverse;
    nextTraverseStart = genNextTraverseStart(qctx->objPool(), edge);
    inputVar = traverse->outputVar();
  }

  VLOG(1) << subplan;
  auto& node = nodeInfos.front();
  auto appendV = AppendVertices::make(qctx, subplan.root, spaceId);
  auto vertexProps = SchemaUtil::getAllVertexProp(qctx, spaceId, true);
  NG_RETURN_IF_ERROR(vertexProps);
  appendV->setVertexProps(std::move(vertexProps).value());
  appendV->setSrc(nextTraverseStart);
  appendV->setVertexFilter(genVertexFilter(node));
  appendV->setColNames(genAppendVColNames(subplan.root->colNames(), node));
  appendV->setDedup();
  subplan.root = appendV;

  VLOG(1) << subplan;
  return Status::OK();
}

Status MatchClausePlanner::rightExpandFromNode(const std::vector<NodeInfo>& nodeInfos,
                                               const std::vector<EdgeInfo>& edgeInfos,
                                               MatchClauseContext* matchClauseCtx,
                                               size_t startIndex,
                                               SubPlan& subplan) {
  auto inputVar = subplan.root->outputVar();
  auto qctx = matchClauseCtx->qctx;
  auto spaceId = matchClauseCtx->space.id;
  Expression* nextTraverseStart = initialExpr_;
  bool reversely = false;
  for (size_t i = startIndex; i < edgeInfos.size(); ++i) {
    auto& node = nodeInfos[i];
    auto& edge = edgeInfos[i];
    auto traverse = Traverse::make(qctx, subplan.root, spaceId);
    traverse->setSrc(nextTraverseStart);
    auto vertexProps = genVertexProps(node, qctx, spaceId);
    NG_RETURN_IF_ERROR(vertexProps);
    traverse->setVertexProps(std::move(vertexProps).value());
    traverse->setEdgeProps(genEdgeProps(edge, reversely, qctx, spaceId));
    traverse->setVertexFilter(genVertexFilter(node));
    traverse->setEdgeFilter(genEdgeFilter(edge));
    traverse->setEdgeDirection(edge.direction);
    traverse->setColNames(genTraverseColNames(subplan.root->colNames(), node, edge));
    traverse->setStepRange(edge.range);
    traverse->setDedup();
    subplan.root = traverse;
    nextTraverseStart = genNextTraverseStart(qctx->objPool(), edge);
    inputVar = traverse->outputVar();
  }

  VLOG(1) << subplan;
  auto& node = nodeInfos.back();
  auto appendV = AppendVertices::make(qctx, subplan.root, spaceId);
  auto vertexProps = SchemaUtil::getAllVertexProp(qctx, spaceId, true);
  NG_RETURN_IF_ERROR(vertexProps);
  appendV->setVertexProps(std::move(vertexProps).value());
  appendV->setSrc(nextTraverseStart);
  appendV->setVertexFilter(genVertexFilter(node));
  appendV->setColNames(genAppendVColNames(subplan.root->colNames(), node));
  appendV->setDedup();
  subplan.root = appendV;

  VLOG(1) << subplan;
  return Status::OK();
}

Status MatchClausePlanner::expandFromEdge(const std::vector<NodeInfo>& nodeInfos,
                                          const std::vector<EdgeInfo>& edgeInfos,
                                          MatchClauseContext* matchClauseCtx,
                                          size_t startIndex,
                                          SubPlan& subplan) {
  return expandFromNode(nodeInfos, edgeInfos, matchClauseCtx, startIndex, subplan);
}

Status MatchClausePlanner::projectColumnsBySymbols(MatchClauseContext* matchClauseCtx,
                                                   SubPlan& plan) {
  auto qctx = matchClauseCtx->qctx;
  auto& nodeInfos = matchClauseCtx->nodeInfos;
  auto& edgeInfos = matchClauseCtx->edgeInfos;
  auto columns = qctx->objPool()->add(new YieldColumns);
  std::vector<std::string> colNames;

  auto addNode = [this, columns, &colNames, matchClauseCtx](auto& nodeInfo) {
    if (!nodeInfo.alias.empty() && !nodeInfo.anonymous) {
      columns->addColumn(buildVertexColumn(matchClauseCtx, nodeInfo.alias));
      colNames.emplace_back(nodeInfo.alias);
    }
  };

  auto addEdge = [this, columns, &colNames, matchClauseCtx](auto& edgeInfo) {
    if (!edgeInfo.alias.empty() && !edgeInfo.anonymous) {
      columns->addColumn(buildEdgeColumn(matchClauseCtx, edgeInfo));
      colNames.emplace_back(edgeInfo.alias);
    }
  };

  for (size_t i = 0; i < edgeInfos.size(); i++) {
    addNode(nodeInfos[i]);
    addEdge(edgeInfos[i]);
  }

  // last vertex
  DCHECK(!nodeInfos.empty());
  addNode(nodeInfos.back());

  const auto& aliases = matchClauseCtx->aliasesGenerated;
  auto iter = std::find_if(aliases.begin(), aliases.end(), [](const auto& alias) {
    return alias.second == AliasType::kPath;
  });
  if (iter != aliases.end()) {
    auto& alias = iter->first;
    columns->addColumn(buildPathColumn(matchClauseCtx, alias));
    colNames.emplace_back(alias);
  }

  auto project = Project::make(qctx, plan.root, columns);
  project->setColNames(std::move(colNames));

  plan.root = project;
  VLOG(1) << plan;
  return Status::OK();
}

YieldColumn* MatchClausePlanner::buildVertexColumn(MatchClauseContext* matchClauseCtx,
                                                   const std::string& alias) const {
  return new YieldColumn(InputPropertyExpression::make(matchClauseCtx->qctx->objPool(), alias),
                         alias);
}

YieldColumn* MatchClausePlanner::buildEdgeColumn(MatchClauseContext* matchClauseCtx,
                                                 EdgeInfo& edge) const {
  auto* pool = matchClauseCtx->qctx->objPool();
  Expression* expr = nullptr;
  if (edge.range == nullptr) {
    expr = SubscriptExpression::make(
        pool, InputPropertyExpression::make(pool, edge.alias), ConstantExpression::make(pool, 0));
  } else {
    auto* args = ArgumentList::make(pool);
    args->addArgument(VariableExpression::make(pool, "e"));
    auto* filter = FunctionCallExpression::make(pool, "is_edge", args);
    expr = ListComprehensionExpression::make(
        pool, "e", InputPropertyExpression::make(pool, edge.alias), filter);
  }
  return new YieldColumn(expr, edge.alias);
}

YieldColumn* MatchClausePlanner::buildPathColumn(MatchClauseContext* matchClauseCtx,
                                                 const std::string& alias) const {
  return new YieldColumn(matchClauseCtx->pathBuild, alias);
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
  VLOG(1) << subplan;
  return Status::OK();
}
}  // namespace graph
}  // namespace nebula
