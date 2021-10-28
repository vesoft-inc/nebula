/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/MatchClausePlanner.h"

#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/match/Expand.h"
#include "graph/planner/match/MatchSolver.h"
#include "graph/planner/match/SegmentsConnector.h"
#include "graph/planner/match/StartVidFinder.h"
#include "graph/planner/match/WhereClausePlanner.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
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
  if (edge.range != nullptr) {
    if (edge.range->max() != edge.range->min() || edge.range->max() > 1) {
      // dst(last(list))
      auto args = ArgumentList::make(pool);
      args->addArgument(InputPropertyExpression::make(pool, edge.alias));
      auto last = FunctionCallExpression::make(pool, "last", args);
      auto args1 = ArgumentList::make(pool);
      args1->addArgument(last);
      return FunctionCallExpression::make(pool, "dst", args1);
    } else {
      // dst(edge)
      auto args1 = ArgumentList::make(pool);
      args1->addArgument(InputPropertyExpression::make(pool, edge.alias));
      return FunctionCallExpression::make(pool, "dst", args1);
    }
  } else {
    return nullptr;
  }
}

static Expression* genVertexFilter(const NodeInfo& node) { return node.filter; }

static Expression* genEdgeFilter(const EdgeInfo& edge) { return edge.filter; }

static std::unique_ptr<std::vector<VertexProp>> genVertexProps(const NodeInfo& node,
                                                               QueryContext* qctx,
                                                               GraphSpaceID spaceId) {
  // TODO
  UNUSED(node);
  UNUSED(qctx);
  UNUSED(spaceId);
  return std::make_unique<std::vector<VertexProp>>();
}

static std::unique_ptr<std::vector<storage::cpp2::EdgeProp>> genEdgeDst(const EdgeInfo& edge,
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
        edgeProp.set_type(-edgeType);
        std::vector<std::string> props{kSrc, kType, kRank, kDst};
        edgeProp.set_props(std::move(props));
        edgeProps->emplace_back(std::move(edgeProp));
        break;
      }
    }
    EdgeProp edgeProp;
    edgeProp.set_type(edgeType);
    std::vector<std::string> props{kSrc, kType, kRank, kDst};
    edgeProp.set_props(std::move(props));
    edgeProps->emplace_back(std::move(edgeProp));
  }
  return edgeProps;
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
  // NG_RETURN_IF_ERROR(projectColumnsBySymbols(matchClauseCtx, startIndex, matchClausePlan));
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
  auto left = subplan.root;
  NG_RETURN_IF_ERROR(
      leftExpandFromNode(nodeInfos, edgeInfos, matchClauseCtx, startIndex, var, subplan));

  // Connect the left expand and right expand part.
  auto right = subplan.root;
  subplan.root = SegmentsConnector::innerJoinSegments(
      matchClauseCtx->qctx, left, right, JoinStrategyPos::kStart, JoinStrategyPos::kStart);
  return Status::OK();
}

Status MatchClausePlanner::leftExpandFromNode(const std::vector<NodeInfo>& nodeInfos,
                                              const std::vector<EdgeInfo>& edgeInfos,
                                              MatchClauseContext* matchClauseCtx,
                                              size_t startIndex,
                                              std::string inputVar,
                                              SubPlan& subplan) {
  auto qctx = matchClauseCtx->qctx;
  auto spaceId = matchClauseCtx->space.id;
  Expression* nextTraverseStart = nullptr;
  bool reversely = true;
  for (size_t i = startIndex; i > 0; --i) {
    auto& node = nodeInfos[i];
    auto& edge = edgeInfos[i - 1];
    auto traverse = Traverse::make(qctx, subplan.root, spaceId);
    traverse->setSrc(startIndex == i ? initialExpr_ : nextTraverseStart);
    traverse->setVertexProps(genVertexProps(node, qctx, spaceId));
    traverse->setEdgeProps(genEdgeProps(edge, reversely, qctx, spaceId));
    traverse->setEdgeProps(genEdgeDst(edge, reversely, qctx, spaceId));
    traverse->setVertexFilter(genVertexFilter(node));
    traverse->setEdgeFilter(genEdgeFilter(edge));
    traverse->setEdgeDirection(edge.direction);  // TODO: reverse the direction
    traverse->setColNames(genTraverseColNames(subplan.root->colNames(), node, edge));
    if (edge.range != nullptr) {
      traverse->setSteps(StepClause(edge.range->min(), edge.range->max()));
    }
    subplan.root = traverse;
    nextTraverseStart = genNextTraverseStart(qctx->objPool(), edge);
    inputVar = traverse->outputVar();
  }

  VLOG(1) << subplan;
  auto& node = nodeInfos.front();
  auto appendV = AppendVertices::make(qctx, subplan.root, spaceId);
  appendV->setVertexProps(genVertexProps(node, qctx, spaceId));
  appendV->setSrc(genNextTraverseStart(qctx->objPool(), edgeInfos.back()));
  appendV->setVertexFilter(genVertexFilter(node));
  appendV->setColNames(genAppendVColNames(subplan.root->colNames(), node));
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
  Expression* nextTraverseStart = nullptr;
  bool reversely = false;
  for (size_t i = startIndex; i < edgeInfos.size(); ++i) {
    auto& node = nodeInfos[i];
    auto& edge = edgeInfos[i];
    auto traverse = Traverse::make(qctx, subplan.root, spaceId);
    traverse->setSrc(startIndex == i ? initialExpr_ : nextTraverseStart);
    traverse->setVertexProps(genVertexProps(node, qctx, spaceId));
    traverse->setEdgeProps(genEdgeProps(edge, reversely, qctx, spaceId));
    traverse->setEdgeDst(genEdgeDst(edge, reversely, qctx, spaceId));
    traverse->setVertexFilter(genVertexFilter(node));
    traverse->setEdgeFilter(genEdgeFilter(edge));
    traverse->setEdgeDirection(edge.direction);
    traverse->setColNames(genTraverseColNames(subplan.root->colNames(), node, edge));
    if (edge.range != nullptr) {
      traverse->setSteps(StepClause(edge.range->min(), edge.range->max()));
    }
    subplan.root = traverse;
    nextTraverseStart = genNextTraverseStart(qctx->objPool(), edge);
    inputVar = traverse->outputVar();
  }

  VLOG(1) << subplan;
  auto& node = nodeInfos.back();
  auto appendV = AppendVertices::make(qctx, subplan.root, spaceId);
  appendV->setSrc(genNextTraverseStart(qctx->objPool(), edgeInfos.back()));
  appendV->setVertexProps(genVertexProps(node, qctx, spaceId));
  appendV->setVertexFilter(genVertexFilter(node));
  appendV->setColNames(genAppendVColNames(subplan.root->colNames(), node));
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
                                                   size_t startIndex,
                                                   SubPlan& plan) {
  auto qctx = matchClauseCtx->qctx;
  auto& nodeInfos = matchClauseCtx->nodeInfos;
  auto& edgeInfos = matchClauseCtx->edgeInfos;
  auto input = plan.root;
  const auto& inColNames = input->colNames();
  auto columns = qctx->objPool()->add(new YieldColumns);
  std::vector<std::string> colNames;

  auto addNode = [&, this](size_t i) {
    auto& nodeInfo = nodeInfos[i];
    if (!nodeInfo.alias.empty() && !nodeInfo.anonymous) {
      if (i >= startIndex) {
        columns->addColumn(
            buildVertexColumn(matchClauseCtx, inColNames[i - startIndex], nodeInfo.alias));
      } else if (startIndex == (nodeInfos.size() - 1)) {
        columns->addColumn(
            buildVertexColumn(matchClauseCtx, inColNames[startIndex - i], nodeInfo.alias));
      } else {
        columns->addColumn(
            buildVertexColumn(matchClauseCtx, inColNames[nodeInfos.size() - i], nodeInfo.alias));
      }
      colNames.emplace_back(nodeInfo.alias);
    }
  };

  for (size_t i = 0; i < edgeInfos.size(); i++) {
    VLOG(1) << "colSize: " << inColNames.size() << "i: " << i << " nodesize: " << nodeInfos.size()
            << " start: " << startIndex;
    addNode(i);
    auto& edgeInfo = edgeInfos[i];
    if (!edgeInfo.alias.empty() && !edgeInfo.anonymous) {
      if (i >= startIndex) {
        columns->addColumn(buildEdgeColumn(matchClauseCtx, inColNames[i - startIndex], edgeInfo));
      } else if (startIndex == (nodeInfos.size() - 1)) {
        columns->addColumn(
            buildEdgeColumn(matchClauseCtx, inColNames[edgeInfos.size() - 1 - i], edgeInfo));
      } else {
        columns->addColumn(
            buildEdgeColumn(matchClauseCtx, inColNames[edgeInfos.size() - i], edgeInfo));
      }
      colNames.emplace_back(edgeInfo.alias);
    }
  }

  // last vertex
  DCHECK(!nodeInfos.empty());
  addNode(nodeInfos.size() - 1);

  const auto& aliases = matchClauseCtx->aliasesGenerated;
  auto iter = std::find_if(aliases.begin(), aliases.end(), [](const auto& alias) {
    return alias.second == AliasType::kPath;
  });
  std::string alias = iter != aliases.end() ? iter->first : qctx->vctx()->anonColGen()->getCol();
  columns->addColumn(
      buildPathColumn(matchClauseCtx, alias, startIndex, inColNames, nodeInfos.size()));
  colNames.emplace_back(alias);

  auto project = Project::make(qctx, input, columns);
  project->setColNames(std::move(colNames));

  plan.root = MatchSolver::filtPathHasSameEdge(project, alias, qctx);
  VLOG(1) << plan;
  return Status::OK();
}

YieldColumn* MatchClausePlanner::buildVertexColumn(MatchClauseContext* matchClauseCtx,
                                                   const std::string& colName,
                                                   const std::string& alias) const {
  auto* pool = matchClauseCtx->qctx->objPool();
  auto colExpr = InputPropertyExpression::make(pool, colName);
  // startNode(path) => head node of path
  auto args = ArgumentList::make(pool);
  args->addArgument(colExpr);
  auto firstVertexExpr = FunctionCallExpression::make(pool, "startNode", args);
  return new YieldColumn(firstVertexExpr, alias);
}

YieldColumn* MatchClausePlanner::buildEdgeColumn(MatchClauseContext* matchClauseCtx,
                                                 const std::string& colName,
                                                 EdgeInfo& edge) const {
  auto* pool = matchClauseCtx->qctx->objPool();
  auto colExpr = InputPropertyExpression::make(pool, colName);
  // relationships(p)
  auto args = ArgumentList::make(pool);
  args->addArgument(colExpr);
  auto relExpr = FunctionCallExpression::make(pool, "relationships", args);
  Expression* expr = nullptr;
  if (edge.range != nullptr) {
    expr = relExpr;
  } else {
    // Get first edge in path list [e1, e2, ...]
    auto idxExpr = ConstantExpression::make(pool, 0);
    auto subExpr = SubscriptExpression::make(pool, relExpr, idxExpr);
    expr = subExpr;
  }
  return new YieldColumn(expr, edge.alias);
}

YieldColumn* MatchClausePlanner::buildPathColumn(MatchClauseContext* matchClauseCtx,
                                                 const std::string& alias,
                                                 size_t startIndex,
                                                 const std::vector<std::string> colNames,
                                                 size_t nodeInfoSize) const {
  auto colSize = colNames.size();
  DCHECK((nodeInfoSize == colSize) || (nodeInfoSize + 1 == colSize));
  size_t bound = 0;
  if (colSize > nodeInfoSize) {
    bound = colSize - startIndex - 1;
  } else if (startIndex == (nodeInfoSize - 1)) {
    bound = 0;
  } else {
    bound = colSize - startIndex;
  }
  auto* pool = matchClauseCtx->qctx->objPool();
  auto rightExpandPath = PathBuildExpression::make(pool);
  for (size_t i = 0; i < bound; ++i) {
    rightExpandPath->add(InputPropertyExpression::make(pool, colNames[i]));
  }

  auto leftExpandPath = PathBuildExpression::make(pool);
  for (size_t i = bound; i < colNames.size(); ++i) {
    leftExpandPath->add(InputPropertyExpression::make(pool, colNames[i]));
  }

  auto finalPath = PathBuildExpression::make(pool);
  if (leftExpandPath->size() != 0) {
    auto args = ArgumentList::make(pool);
    args->addArgument(leftExpandPath);
    auto reversePath = FunctionCallExpression::make(pool, "reversePath", args);
    if (rightExpandPath->size() == 0) {
      return new YieldColumn(reversePath, alias);
    }
    finalPath->add(reversePath);
  }
  if (rightExpandPath->size() != 0) {
    finalPath->add(rightExpandPath);
  }
  return new YieldColumn(finalPath, alias);
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
