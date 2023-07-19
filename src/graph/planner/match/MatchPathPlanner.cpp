// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/planner/match/MatchPathPlanner.h"

#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/match/MatchSolver.h"
#include "graph/planner/match/SegmentsConnector.h"
#include "graph/planner/match/StartVidFinder.h"
#include "graph/planner/match/WhereClausePlanner.h"
#include "graph/planner/plan/Algo.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/SchemaUtil.h"
#include "graph/visitor/RewriteVisitor.h"

namespace nebula {
namespace graph {

MatchPathPlanner::MatchPathPlanner(CypherClauseContextBase* ctx, const Path& path)
    : ctx_(DCHECK_NOTNULL(ctx)), path_(path) {}

static std::vector<std::string> genTraverseColNames(const std::vector<std::string>& inputCols,
                                                    const NodeInfo& node,
                                                    const EdgeInfo& edge,
                                                    bool trackPrev,
                                                    bool genPath = false) {
  std::vector<std::string> cols;
  if (trackPrev) {
    cols = inputCols;
  }
  cols.emplace_back(node.alias);
  cols.emplace_back(edge.alias);
  if (genPath) {
    cols.emplace_back(edge.innerAlias);
  }
  return cols;
}

static std::vector<std::string> genAppendVColNames(const std::vector<std::string>& inputCols,
                                                   const NodeInfo& node,
                                                   bool trackPrev) {
  std::vector<std::string> cols;
  if (trackPrev) {
    cols = inputCols;
  }
  cols.emplace_back(node.alias);
  return cols;
}

static Expression* genNextTraverseStart(ObjectPool* pool,
                                        const EdgeInfo& edge,
                                        const NodeInfo& node) {
  auto args = ArgumentList::make(pool);
  args->addArgument(InputPropertyExpression::make(pool, edge.alias));
  args->addArgument(InputPropertyExpression::make(pool, node.alias));
  return FunctionCallExpression::make(pool, "none_direct_dst", args);
}

static Expression* genVertexFilter(const NodeInfo& node) {
  return node.filter;
}

static Expression* genEdgeFilter(const EdgeInfo& edge) {
  return edge.filter;
}

static Expression* nodeId(ObjectPool* pool, const NodeInfo& node) {
  return AttributeExpression::make(
      pool, InputPropertyExpression::make(pool, node.alias), ConstantExpression::make(pool, kVid));
}

StatusOr<SubPlan> MatchPathPlanner::transform(WhereClauseContext* bindWhere,
                                              std::unordered_set<std::string> nodeAliasesSeen) {
  // All nodes ever seen in current match clause
  // TODO: Maybe it is better to rebuild the graph and find all connected components.
  SubPlan subplan;
  size_t startIndex = 0;
  bool startFromEdge = false;

  NG_RETURN_IF_ERROR(findStarts(bindWhere, nodeAliasesSeen, startFromEdge, startIndex, subplan));
  NG_RETURN_IF_ERROR(expand(startFromEdge, startIndex, subplan));

  // No need to actually build path if the path is just a predicate
  if (!path_.isPred) {
    MatchSolver::buildProjectColumns(ctx_->qctx, path_, subplan);
  }

  return subplan;
}

Status MatchPathPlanner::findStarts(WhereClauseContext* bindWhereClause,
                                    std::unordered_set<std::string> nodeAliasesSeen,
                                    bool& startFromEdge,
                                    size_t& startIndex,
                                    SubPlan& matchClausePlan) {
  auto& startVidFinders = StartVidFinder::finders();
  bool foundStart = false;
  std::for_each(
      ctx_->aliasesAvailable.begin(), ctx_->aliasesAvailable.end(), [&nodeAliasesSeen](auto& kv) {
        // if (kv.second == AliasType::kNode) {
        nodeAliasesSeen.emplace(kv.first);
        // }
      });

  auto spaceId = ctx_->space.id;
  auto* qctx = ctx_->qctx;
  const auto& nodeInfos = path_.nodeInfos;
  const auto& edgeInfos = path_.edgeInfos;
  // Find the start plan node
  for (auto& finder : startVidFinders) {
    for (size_t i = 0; i < nodeInfos.size() && !foundStart; ++i) {
      NodeContext nodeCtx(qctx, bindWhereClause, spaceId, &nodeInfos[i]);
      nodeCtx.aliasesAvailable = &nodeAliasesSeen;
      auto nodeFinder = finder();
      if (nodeFinder->match(&nodeCtx)) {
        auto plan = nodeFinder->transform(&nodeCtx);
        NG_RETURN_IF_ERROR(plan);
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
        EdgeContext edgeCtx(qctx, bindWhereClause, spaceId, &edgeInfos[i]);
        auto edgeFinder = finder();
        if (edgeFinder->match(&edgeCtx)) {
          auto plan = edgeFinder->transform(&edgeCtx);
          NG_RETURN_IF_ERROR(plan);
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
    return Status::SemanticError("Can't solve the start vids from the sentence.");
  }

  // Both StartNode and Argument are leaf plan nodes
  matchClausePlan.appendStartNode(qctx);

  return Status::OK();
}

Status MatchPathPlanner::expand(bool startFromEdge, size_t startIndex, SubPlan& subplan) {
  if (startFromEdge) {
    return expandFromEdge(startIndex, subplan);
  } else {
    return expandFromNode(startIndex, subplan);
  }
}

Status MatchPathPlanner::expandFromNode(size_t startIndex, SubPlan& subplan) {
  const auto& nodeInfos = path_.nodeInfos;
  DCHECK_LT(startIndex, nodeInfos.size());
  // Vid of the start node is known already
  nodeAliasesSeenInPattern_.emplace(nodeInfos[startIndex].alias);
  if (startIndex == 0) {
    // Pattern: (start)-[]-...-()
    return rightExpandFromNode(startIndex, subplan);
  }

  if (startIndex == nodeInfos.size() - 1) {
    // Pattern: ()-[]-...-(start)
    return leftExpandFromNode(startIndex, subplan);
  }

  // Pattern: ()-[]-...-(start)-...-[]-()
  NG_RETURN_IF_ERROR(rightExpandFromNode(startIndex, subplan));
  NG_RETURN_IF_ERROR(leftExpandFromNode(startIndex, subplan));

  return Status::OK();
}

Status MatchPathPlanner::leftExpandFromNode(size_t startIndex, SubPlan& subplan) {
  const auto& nodeInfos = path_.nodeInfos;
  const auto& edgeInfos = path_.edgeInfos;
  Expression* nextTraverseStart = nullptr;
  if (startIndex == nodeInfos.size() - 1) {
    nextTraverseStart = initialExpr_;
  } else {
    auto* pool = ctx_->qctx->objPool();
    auto args = ArgumentList::make(pool);
    args->addArgument(InputPropertyExpression::make(pool, nodeInfos[startIndex].alias));
    nextTraverseStart = FunctionCallExpression::make(pool, "_joinkey", args);
  }
  bool reversely = true;
  auto qctx = ctx_->qctx;
  auto spaceId = ctx_->space.id;
  for (size_t i = startIndex; i > 0; --i) {
    auto& node = nodeInfos[i];
    auto& dst = nodeInfos[i - 1];
    addNodeAlias(node);
    bool expandInto = isExpandInto(dst.alias);
    auto& edge = edgeInfos[i - 1];
    MatchStepRange stepRange(1, 1);
    if (edge.range != nullptr) {
      stepRange = *edge.range;
    }
    auto traverse = Traverse::make(qctx, subplan.root, spaceId);
    traverse->setSrc(nextTraverseStart);
    auto vertexProps = SchemaUtil::getAllVertexProp(qctx, spaceId, true);
    NG_RETURN_IF_ERROR(vertexProps);
    traverse->setVertexProps(std::move(vertexProps).value());
    traverse->setEdgeProps(SchemaUtil::getEdgeProps(edge, reversely, qctx, spaceId));
    traverse->setVertexFilter(genVertexFilter(node));
    traverse->setTagFilter(genVertexFilter(node));
    traverse->setEdgeFilter(genEdgeFilter(edge));
    traverse->setEdgeDirection(edge.direction);
    traverse->setStepRange(stepRange);
    traverse->setDedup();
    traverse->setGenPath(path_.genPath);
    // If start from end of the path pattern, the first traverse would not
    // track the previous path, otherwise, it should.
    bool trackPrevPath = (startIndex + 1 == nodeInfos.size() ? i != startIndex : true);
    traverse->setTrackPrevPath(trackPrevPath);
    traverse->setColNames(
        genTraverseColNames(subplan.root->colNames(), node, edge, trackPrevPath, path_.genPath));
    subplan.root = traverse;
    nextTraverseStart = genNextTraverseStart(qctx->objPool(), edge, node);
    if (expandInto) {
      // TODO(shylock) optimize to embed filter to Traverse
      auto* startVid = nodeId(qctx->objPool(), dst);
      auto* endVid = nextTraverseStart;
      auto* filterExpr = RelationalExpression::makeEQ(qctx->objPool(), startVid, endVid);
      auto* filter = Filter::make(qctx, traverse, filterExpr, false);
      subplan.root = filter;
    }
  }

  auto& lastNode = nodeInfos.front();

  bool duppedLastAlias = isExpandInto(lastNode.alias) && nodeAliasesSeenInPattern_.size() > 1;
  // If the the last alias has been presented in the pattern, we could emit the AppendVertices node
  // because the same alias always presents in the same entity.
  if (duppedLastAlias) {
    return Status::OK();
  }

  auto appendV = AppendVertices::make(qctx, subplan.root, spaceId);
  auto vertexProps = SchemaUtil::getAllVertexProp(qctx, spaceId, true);
  NG_RETURN_IF_ERROR(vertexProps);
  appendV->setVertexProps(std::move(vertexProps).value());
  appendV->setSrc(nextTraverseStart);
  appendV->setVertexFilter(genVertexFilter(lastNode));
  appendV->setDedup();
  appendV->setTrackPrevPath(!edgeInfos.empty());
  appendV->setColNames(genAppendVColNames(subplan.root->colNames(), lastNode, !edgeInfos.empty()));
  subplan.root = appendV;

  return Status::OK();
}

Status MatchPathPlanner::rightExpandFromNode(size_t startIndex, SubPlan& subplan) {
  const auto& nodeInfos = path_.nodeInfos;
  const auto& edgeInfos = path_.edgeInfos;
  Expression* nextTraverseStart = initialExpr_;
  bool reversely = false;
  auto qctx = ctx_->qctx;
  auto spaceId = ctx_->space.id;
  for (size_t i = startIndex; i < edgeInfos.size(); ++i) {
    auto& node = nodeInfos[i];
    auto& dst = nodeInfos[i + 1];

    addNodeAlias(node);
    bool expandInto = isExpandInto(dst.alias);

    auto& edge = edgeInfos[i];
    MatchStepRange stepRange(1, 1);
    if (edge.range != nullptr) {
      stepRange = *edge.range;
    }
    auto traverse = Traverse::make(qctx, subplan.root, spaceId);
    traverse->setSrc(nextTraverseStart);
    auto vertexProps = SchemaUtil::getAllVertexProp(qctx, spaceId, true);
    NG_RETURN_IF_ERROR(vertexProps);
    traverse->setVertexProps(std::move(vertexProps).value());
    traverse->setEdgeProps(SchemaUtil::getEdgeProps(edge, reversely, qctx, spaceId));
    traverse->setVertexFilter(genVertexFilter(node));
    traverse->setTagFilter(genVertexFilter(node));
    traverse->setEdgeFilter(genEdgeFilter(edge));
    traverse->setEdgeDirection(edge.direction);
    traverse->setStepRange(stepRange);
    traverse->setDedup();
    traverse->setTrackPrevPath(i != startIndex);
    traverse->setGenPath(path_.genPath);
    traverse->setColNames(
        genTraverseColNames(subplan.root->colNames(), node, edge, i != startIndex, path_.genPath));
    subplan.root = traverse;
    nextTraverseStart = genNextTraverseStart(qctx->objPool(), edge, node);
    if (expandInto) {
      auto* startVid = nodeId(qctx->objPool(), dst);
      auto* endVid = nextTraverseStart;
      auto* filterExpr = RelationalExpression::makeEQ(qctx->objPool(), startVid, endVid);
      auto* filter = Filter::make(qctx, traverse, filterExpr, false);
      subplan.root = filter;
    }
  }

  auto& lastNode = nodeInfos.back();

  bool duppedLastAlias = isExpandInto(lastNode.alias) && nodeAliasesSeenInPattern_.size() > 1;

  addNodeAlias(lastNode);

  // If the the last alias has been presented in the pattern, we could emit the AppendVertices node
  // because the same alias always presents in the same entity.
  if (duppedLastAlias) {
    return Status::OK();
  }

  auto appendV = AppendVertices::make(qctx, subplan.root, spaceId);
  auto vertexProps = SchemaUtil::getAllVertexProp(qctx, spaceId, true);
  NG_RETURN_IF_ERROR(vertexProps);
  appendV->setVertexProps(std::move(vertexProps).value());
  appendV->setSrc(nextTraverseStart);
  appendV->setVertexFilter(genVertexFilter(lastNode));
  appendV->setDedup();
  appendV->setTrackPrevPath(!edgeInfos.empty());
  appendV->setColNames(genAppendVColNames(subplan.root->colNames(), lastNode, !edgeInfos.empty()));
  subplan.root = appendV;

  return Status::OK();
}

Status MatchPathPlanner::expandFromEdge(size_t startIndex, SubPlan& subplan) {
  return expandFromNode(startIndex, subplan);
}

}  // namespace graph
}  // namespace nebula
