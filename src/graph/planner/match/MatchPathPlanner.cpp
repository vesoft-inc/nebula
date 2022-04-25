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
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/SchemaUtil.h"
#include "graph/visitor/RewriteVisitor.h"

namespace nebula {
namespace graph {
static std::vector<std::string> genTraverseColNames(const std::vector<std::string>& inputCols,
                                                    const NodeInfo& node,
                                                    const EdgeInfo& edge,
                                                    bool trackPrev) {
  std::vector<std::string> cols;
  if (trackPrev) {
    cols = inputCols;
  }
  cols.emplace_back(node.alias);
  cols.emplace_back(edge.alias);
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

static Expression* nodeId(ObjectPool* pool, const NodeInfo& node) {
  return AttributeExpression::make(
      pool, InputPropertyExpression::make(pool, node.alias), ConstantExpression::make(pool, kVid));
}

StatusOr<SubPlan> MatchPathPlanner::transform(
    QueryContext* qctx,
    GraphSpaceID spaceId,
    Expression* bindFilter,
    const std::unordered_map<std::string, AliasType>& aliasesAvailable,
    std::unordered_set<std::string> nodeAliasesSeen,
    Path& path) {
  // All nodes ever seen in current match clause
  // TODO: Maybe it is better to rebuild the graph and find all connected components.
  auto& nodeInfos = path.nodeInfos;
  auto& edgeInfos = path.edgeInfos;
  SubPlan subplan;
  size_t startIndex = 0;
  bool startFromEdge = false;
  // The node alias seen in current pattern only
  std::unordered_set<std::string> nodeAliasesSeenInPattern;

  NG_RETURN_IF_ERROR(findStarts(nodeInfos,
                                edgeInfos,
                                qctx,
                                spaceId,
                                bindFilter,
                                aliasesAvailable,
                                nodeAliasesSeen,
                                startFromEdge,
                                startIndex,
                                subplan));
  NG_RETURN_IF_ERROR(expand(nodeInfos,
                            edgeInfos,
                            qctx,
                            spaceId,
                            startFromEdge,
                            startIndex,
                            subplan,
                            nodeAliasesSeenInPattern));
  NG_RETURN_IF_ERROR(projectColumnsBySymbols(qctx, path, subplan));
  return subplan;
}

Status MatchPathPlanner::findStarts(
    std::vector<NodeInfo>& nodeInfos,
    std::vector<EdgeInfo>& edgeInfos,
    QueryContext* qctx,
    GraphSpaceID spaceId,
    Expression* bindFilter,
    const std::unordered_map<std::string, AliasType>& aliasesAvailable,
    std::unordered_set<std::string> nodeAliasesSeen,
    bool& startFromEdge,
    size_t& startIndex,
    SubPlan& matchClausePlan) {
  auto& startVidFinders = StartVidFinder::finders();
  bool foundStart = false;
  std::unordered_set<std::string> allNodeAliasesAvailable;
  allNodeAliasesAvailable.merge(nodeAliasesSeen);
  std::for_each(
      aliasesAvailable.begin(), aliasesAvailable.end(), [&allNodeAliasesAvailable](auto& kv) {
        if (kv.second == AliasType::kNode) {
          allNodeAliasesAvailable.emplace(kv.first);
        }
      });

  // Find the start plan node
  for (auto& finder : startVidFinders) {
    for (size_t i = 0; i < nodeInfos.size() && !foundStart; ++i) {
      auto nodeCtx = NodeContext(qctx, bindFilter, spaceId, &nodeInfos[i]);
      nodeCtx.nodeAliasesAvailable = &allNodeAliasesAvailable;
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
        auto edgeCtx = EdgeContext(qctx, bindFilter, spaceId, &edgeInfos[i]);
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
    return Status::SemanticError("Can't solve the start vids from the sentence.");
  }

  if (matchClausePlan.tail->isSingleInput()) {
    auto start = StartNode::make(qctx);
    matchClausePlan.tail->setDep(0, start);
    matchClausePlan.tail = start;
  }

  return Status::OK();
}

Status MatchPathPlanner::expand(const std::vector<NodeInfo>& nodeInfos,
                                const std::vector<EdgeInfo>& edgeInfos,
                                QueryContext* qctx,
                                GraphSpaceID spaceId,
                                bool startFromEdge,
                                size_t startIndex,
                                SubPlan& subplan,
                                std::unordered_set<std::string>& nodeAliasesSeenInPattern) {
  if (startFromEdge) {
    return expandFromEdge(
        nodeInfos, edgeInfos, qctx, spaceId, startIndex, subplan, nodeAliasesSeenInPattern);
  } else {
    return expandFromNode(
        nodeInfos, edgeInfos, qctx, spaceId, startIndex, subplan, nodeAliasesSeenInPattern);
  }
}

Status MatchPathPlanner::expandFromNode(const std::vector<NodeInfo>& nodeInfos,
                                        const std::vector<EdgeInfo>& edgeInfos,
                                        QueryContext* qctx,
                                        GraphSpaceID spaceId,
                                        size_t startIndex,
                                        SubPlan& subplan,
                                        std::unordered_set<std::string>& nodeAliasesSeenInPattern) {
  // Vid of the start node is known already
  nodeAliasesSeenInPattern.emplace(nodeInfos[startIndex].alias);
  DCHECK(!nodeInfos.empty() && startIndex < nodeInfos.size());
  if (startIndex == 0) {
    // Pattern: (start)-[]-...-()
    return rightExpandFromNode(
        nodeInfos, edgeInfos, qctx, spaceId, startIndex, subplan, nodeAliasesSeenInPattern);
  }

  const auto& var = subplan.root->outputVar();
  if (startIndex == nodeInfos.size() - 1) {
    // Pattern: ()-[]-...-(start)
    return leftExpandFromNode(
        nodeInfos, edgeInfos, qctx, spaceId, startIndex, var, subplan, nodeAliasesSeenInPattern);
  }

  // Pattern: ()-[]-...-(start)-...-[]-()
  NG_RETURN_IF_ERROR(rightExpandFromNode(
      nodeInfos, edgeInfos, qctx, spaceId, startIndex, subplan, nodeAliasesSeenInPattern));
  NG_RETURN_IF_ERROR(leftExpandFromNode(nodeInfos,
                                        edgeInfos,
                                        qctx,
                                        spaceId,
                                        startIndex,
                                        subplan.root->outputVar(),
                                        subplan,
                                        nodeAliasesSeenInPattern));

  return Status::OK();
}

Status MatchPathPlanner::leftExpandFromNode(
    const std::vector<NodeInfo>& nodeInfos,
    const std::vector<EdgeInfo>& edgeInfos,
    QueryContext* qctx,
    GraphSpaceID spaceId,
    size_t startIndex,
    std::string inputVar,
    SubPlan& subplan,
    std::unordered_set<std::string>& nodeAliasesSeenInPattern) {
  Expression* nextTraverseStart = nullptr;
  if (startIndex == nodeInfos.size() - 1) {
    nextTraverseStart = initialExpr_;
  } else {
    auto* pool = qctx->objPool();
    auto args = ArgumentList::make(pool);
    args->addArgument(InputPropertyExpression::make(pool, nodeInfos[startIndex].alias));
    nextTraverseStart = FunctionCallExpression::make(pool, "id", args);
  }
  bool reversely = true;
  for (size_t i = startIndex; i > 0; --i) {
    auto& node = nodeInfos[i];
    auto& dst = nodeInfos[i - 1];
    bool expandInto = nodeAliasesSeenInPattern.find(dst.alias) != nodeAliasesSeenInPattern.end();
    if (!node.anonymous) {
      nodeAliasesSeenInPattern.emplace(node.alias);
    }
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
    traverse->setStepRange(edge.range);
    traverse->setDedup();
    // If start from end of the path pattern, the first traverse would not
    // track the previos path, otherwise, it should.
    traverse->setTrackPrevPath(startIndex + 1 == nodeInfos.size() ? i != startIndex : true);
    traverse->setColNames(
        genTraverseColNames(subplan.root->colNames(),
                            node,
                            edge,
                            startIndex + 1 == nodeInfos.size() ? i != startIndex : true));
    subplan.root = traverse;
    nextTraverseStart = genNextTraverseStart(qctx->objPool(), edge);
    inputVar = traverse->outputVar();
    if (expandInto) {
      // TODO(shylock) optimize to embed filter to Traverse
      auto* startVid = nodeId(qctx->objPool(), dst);
      auto* endVid = nextTraverseStart;
      auto* filterExpr = RelationalExpression::makeEQ(qctx->objPool(), startVid, endVid);
      auto* filter = Filter::make(qctx, traverse, filterExpr, false);
      subplan.root = filter;
      inputVar = filter->outputVar();
    }
  }

  VLOG(1) << subplan;
  auto& node = nodeInfos.front();
  if (!node.anonymous) {
    nodeAliasesSeenInPattern.emplace(node.alias);
  }
  auto appendV = AppendVertices::make(qctx, subplan.root, spaceId);
  auto vertexProps = SchemaUtil::getAllVertexProp(qctx, spaceId, true);
  NG_RETURN_IF_ERROR(vertexProps);
  appendV->setVertexProps(std::move(vertexProps).value());
  appendV->setSrc(nextTraverseStart);
  appendV->setVertexFilter(genVertexFilter(node));
  appendV->setDedup();
  appendV->setTrackPrevPath(!edgeInfos.empty());
  appendV->setColNames(genAppendVColNames(subplan.root->colNames(), node, !edgeInfos.empty()));
  subplan.root = appendV;

  VLOG(1) << subplan;
  return Status::OK();
}

Status MatchPathPlanner::rightExpandFromNode(
    const std::vector<NodeInfo>& nodeInfos,
    const std::vector<EdgeInfo>& edgeInfos,
    QueryContext* qctx,
    GraphSpaceID spaceId,
    size_t startIndex,
    SubPlan& subplan,
    std::unordered_set<std::string>& nodeAliasesSeenInPattern) {
  auto inputVar = subplan.root->outputVar();
  Expression* nextTraverseStart = initialExpr_;
  bool reversely = false;
  for (size_t i = startIndex; i < edgeInfos.size(); ++i) {
    auto& node = nodeInfos[i];
    auto& dst = nodeInfos[i + 1];
    bool expandInto = nodeAliasesSeenInPattern.find(dst.alias) != nodeAliasesSeenInPattern.end();
    if (!node.anonymous) {
      nodeAliasesSeenInPattern.emplace(node.alias);
    }
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
    traverse->setStepRange(edge.range);
    traverse->setDedup();
    traverse->setTrackPrevPath(i != startIndex);
    traverse->setColNames(
        genTraverseColNames(subplan.root->colNames(), node, edge, i != startIndex));
    subplan.root = traverse;
    nextTraverseStart = genNextTraverseStart(qctx->objPool(), edge);
    if (expandInto) {
      auto* startVid = nodeId(qctx->objPool(), dst);
      auto* endVid = nextTraverseStart;
      auto* filterExpr = RelationalExpression::makeEQ(qctx->objPool(), startVid, endVid);
      auto* filter = Filter::make(qctx, traverse, filterExpr, false);
      subplan.root = filter;
      inputVar = filter->outputVar();
    }
  }

  VLOG(1) << subplan;
  auto& node = nodeInfos.back();
  if (!node.anonymous) {
    nodeAliasesSeenInPattern.emplace(node.alias);
  }
  auto appendV = AppendVertices::make(qctx, subplan.root, spaceId);
  auto vertexProps = SchemaUtil::getAllVertexProp(qctx, spaceId, true);
  NG_RETURN_IF_ERROR(vertexProps);
  appendV->setVertexProps(std::move(vertexProps).value());
  appendV->setSrc(nextTraverseStart);
  appendV->setVertexFilter(genVertexFilter(node));
  appendV->setDedup();
  appendV->setTrackPrevPath(!edgeInfos.empty());
  appendV->setColNames(genAppendVColNames(subplan.root->colNames(), node, !edgeInfos.empty()));
  subplan.root = appendV;

  VLOG(1) << subplan;
  return Status::OK();
}

Status MatchPathPlanner::expandFromEdge(const std::vector<NodeInfo>& nodeInfos,
                                        const std::vector<EdgeInfo>& edgeInfos,
                                        QueryContext* qctx,
                                        GraphSpaceID spaceId,
                                        size_t startIndex,
                                        SubPlan& subplan,
                                        std::unordered_set<std::string>& nodeAliasesSeenInPattern) {
  return expandFromNode(
      nodeInfos, edgeInfos, qctx, spaceId, startIndex, subplan, nodeAliasesSeenInPattern);
}

Status MatchPathPlanner::projectColumnsBySymbols(QueryContext* qctx, Path& path, SubPlan& plan) {
  auto columns = qctx->objPool()->makeAndAdd<YieldColumns>();
  std::vector<std::string> colNames;
  auto& nodeInfos = path.nodeInfos;
  auto& edgeInfos = path.edgeInfos;

  auto addNode = [this, columns, &colNames, qctx](auto& nodeInfo) {
    if (!nodeInfo.alias.empty() && !nodeInfo.anonymous) {
      columns->addColumn(buildVertexColumn(qctx->objPool(), nodeInfo.alias));
      colNames.emplace_back(nodeInfo.alias);
    }
  };

  auto addEdge = [this, columns, &colNames, qctx](auto& edgeInfo) {
    if (!edgeInfo.alias.empty() && !edgeInfo.anonymous) {
      columns->addColumn(buildEdgeColumn(qctx->objPool(), edgeInfo));
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

  if (!path.anonymous) {
    DCHECK(!path.alias.empty());
    columns->addColumn(buildPathColumn(DCHECK_NOTNULL(path.pathBuild), path.alias));
    colNames.emplace_back(path.alias);
  }

  auto project = Project::make(qctx, plan.root, columns);
  project->setColNames(std::move(colNames));

  plan.root = project;
  VLOG(1) << plan;
  return Status::OK();
}

YieldColumn* MatchPathPlanner::buildVertexColumn(ObjectPool* pool, const std::string& alias) const {
  return new YieldColumn(InputPropertyExpression::make(pool, alias), alias);
}

YieldColumn* MatchPathPlanner::buildEdgeColumn(ObjectPool* pool, EdgeInfo& edge) const {
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

YieldColumn* MatchPathPlanner::buildPathColumn(Expression* pathBuild,
                                               const std::string& alias) const {
  return new YieldColumn(pathBuild, alias);
}

}  // namespace graph
}  // namespace nebula
