/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/MatchClausePlanner.h"

#include "common/expression/ColumnExpression.h"
#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/match/MatchSolver.h"
#include "graph/planner/match/PropIndexSeek.h"
#include "graph/planner/match/SegmentsConnector.h"
#include "graph/planner/match/StartVidFinder.h"
#include "graph/planner/match/WhereClausePlanner.h"
#include "graph/planner/plan/Algo.h"
#include "graph/planner/plan/ExecutionPlan.h"
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

StatusOr<SubPlan> MatchClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
  if (clauseCtx->kind != CypherClauseKind::kMatch) {
    return Status::Error("Not a valid context for MatchClausePlanner.");
  }

  auto* matchClauseCtx = static_cast<MatchClauseContext*>(clauseCtx);
  SubPlan matchClausePlan;
  // All nodes ever seen in current match clause
  std::unordered_set<std::string> nodeAliasesSeen;
  // TODO: Maybe it is better to rebuild the graph and find all connected components.
  auto& pathInfos = matchClauseCtx->paths;
  for (auto& pathInfo : pathInfos) {
    auto& nodeInfos = pathInfo.nodeInfos;
    auto& edgeInfos = pathInfo.edgeInfos;
    SubPlan subplan;
    size_t startIndex = 0;
    bool startFromEdge = false;

    if (pathInfo.shortestPath.first) {
      NG_RETURN_IF_ERROR(buildShortestPath(
          nodeInfos, edgeInfos, matchClauseCtx, subplan, pathInfo.shortestPath.second));
    } else {
      NG_RETURN_IF_ERROR(findStarts(nodeInfos,
                                    edgeInfos,
                                    matchClauseCtx,
                                    nodeAliasesSeen,
                                    startFromEdge,
                                    startIndex,
                                    subplan));
      NG_RETURN_IF_ERROR(
          expand(nodeInfos, edgeInfos, matchClauseCtx, startFromEdge, startIndex, subplan));
    }
    NG_RETURN_IF_ERROR(
        connectPathPlan(nodeInfos, matchClauseCtx, subplan, nodeAliasesSeen, matchClausePlan));
  }
  NG_RETURN_IF_ERROR(projectColumnsBySymbols(matchClauseCtx, matchClausePlan));
  NG_RETURN_IF_ERROR(appendFilterPlan(matchClauseCtx, matchClausePlan));
  return matchClausePlan;
}

Status MatchClausePlanner::findStarts(std::vector<NodeInfo>& nodeInfos,
                                      std::vector<EdgeInfo>& edgeInfos,
                                      MatchClauseContext* matchClauseCtx,
                                      std::unordered_set<std::string> nodeAliasesSeen,
                                      bool& startFromEdge,
                                      size_t& startIndex,
                                      SubPlan& matchClausePlan) {
  auto& startVidFinders = StartVidFinder::finders();
  bool foundStart = false;
  std::unordered_set<std::string> allNodeAliasesAvailable;
  allNodeAliasesAvailable.merge(nodeAliasesSeen);
  for (auto& alias : matchClauseCtx->aliasesAvailable) {
    if (alias.second == AliasType::kNode) {
      allNodeAliasesAvailable.emplace(alias.first);
    }
  }
  // Find the start plan node
  for (auto& finder : startVidFinders) {
    for (size_t i = 0; i < nodeInfos.size() && !foundStart; ++i) {
      auto nodeCtx = NodeContext(matchClauseCtx, &nodeInfos[i]);
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

  if (matchClausePlan.tail->isSingleInput()) {
    auto start = StartNode::make(matchClauseCtx->qctx);
    matchClausePlan.tail->setDep(0, start);
    matchClausePlan.tail = start;
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
  }

  VLOG(1) << subplan;
  auto& node = nodeInfos.front();
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
    traverse->setStepRange(edge.range);
    traverse->setDedup();
    traverse->setTrackPrevPath(i != startIndex);
    traverse->setColNames(
        genTraverseColNames(subplan.root->colNames(), node, edge, i != startIndex));
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
  appendV->setDedup();
  appendV->setTrackPrevPath(!edgeInfos.empty());
  appendV->setColNames(genAppendVColNames(subplan.root->colNames(), node, !edgeInfos.empty()));
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
  auto columns = qctx->objPool()->add(new YieldColumns);
  std::vector<std::string> colNames;
  for (auto& path : matchClauseCtx->paths) {
    auto& nodeInfos = path.nodeInfos;
    auto& edgeInfos = path.edgeInfos;

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
    if (iter != aliases.end() && path.pathBuild != nullptr) {
      auto& alias = iter->first;
      columns->addColumn(buildPathColumn(path.pathBuild, alias));
      colNames.emplace_back(alias);
    }
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

YieldColumn* MatchClausePlanner::buildPathColumn(Expression* pathBuild,
                                                 const std::string& alias) const {
  return new YieldColumn(pathBuild, alias);
}

Status MatchClausePlanner::appendFilterPlan(MatchClauseContext* matchClauseCtx, SubPlan& subplan) {
  if (matchClauseCtx->where == nullptr) {
    return Status::OK();
  }

  auto wherePlan = std::make_unique<WhereClausePlanner>()->transform(matchClauseCtx->where.get());
  NG_RETURN_IF_ERROR(wherePlan);
  auto plan = std::move(wherePlan).value();
  subplan = SegmentsConnector::addInput(plan, subplan, true);
  VLOG(1) << subplan;
  return Status::OK();
}

Status MatchClausePlanner::connectPathPlan(const std::vector<NodeInfo>& nodeInfos,
                                           MatchClauseContext* matchClauseCtx,
                                           const SubPlan& subplan,
                                           std::unordered_set<std::string>& nodeAliasesSeen,
                                           SubPlan& matchClausePlan) {
  std::unordered_set<std::string> intersectedAliases;
  for (auto& nodeInfo : nodeInfos) {
    if (intersectedAliases.find(nodeInfo.alias) == intersectedAliases.end() ||
        !nodeInfo.anonymous) {
      nodeAliasesSeen.emplace(nodeInfo.alias);
    }
  }
  if (matchClausePlan.root == nullptr) {
    matchClausePlan = subplan;
  } else {
    if (intersectedAliases.empty()) {
      matchClausePlan =
          SegmentsConnector::cartesianProduct(matchClauseCtx->qctx, matchClausePlan, subplan);
    } else {
      // TODO: Actually a natural join would be much easy use.
      matchClausePlan = SegmentsConnector::innerJoin(
          matchClauseCtx->qctx, matchClausePlan, subplan, intersectedAliases);
    }
  }
  return Status::OK();
}

Status MatchClausePlanner::buildShortestPath(const std::vector<NodeInfo>& nodeInfos,
                                             std::vector<EdgeInfo>& edgeInfos,
                                             MatchClauseContext* matchClauseCtx,
                                             SubPlan& subplan,
                                             bool single) {
  DCHECK_EQ(nodeInfos.size(), 2UL) << "Shortest path can only be used for two nodes.";
  DCHECK_EQ(nodeInfos[0].tids.size(), 1UL) << "Not supported multiple edge indices seek.";
  DCHECK_EQ(nodeInfos[1].tids.size(), 1UL) << "Not supported multiple edge indices seek.";

  using IQC = nebula::storage::cpp2::IndexQueryContext;
  auto spaceId = matchClauseCtx->space.id;
  auto qtx = matchClauseCtx->qctx;

  std::vector<IndexScan*> scanNodes;
  scanNodes.reserve(2);

  for (int i = 0; i < 2; i++) {
    IQC iqctx;
    auto nodeInfo = nodeInfos[i];

    auto filterOpt = PropIndexSeek::buildFilter(matchClauseCtx, &nodeInfo);
    if (filterOpt.has_value()) {
      iqctx.filter_ref() = Expression::encode(*filterOpt.value());
    }

    auto scanNode =
        IndexScan::make(qtx, nullptr, spaceId, {iqctx}, {kVid}, false, nodeInfo.tids[0]);
    scanNode->setColNames({kVid});

    auto startNode = StartNode::make(qtx);
    scanNode->setDep(0, startNode);
    scanNodes.emplace_back(scanNode);

    subplan.tail = startNode;
  }

  BiCartesianProduct* cartesianProduct = BiCartesianProduct::make(qtx, scanNodes[0], scanNodes[1]);

  std::vector<Expression*> srcs = {ColumnExpression::make(matchClauseCtx->qctx->objPool(), 0),
                                   ColumnExpression::make(matchClauseCtx->qctx->objPool(), 1)};

  auto edge = edgeInfos.back();
  bool reversely = false;
  auto shortestPath = ShortestPath::make(qtx, cartesianProduct, spaceId);
  shortestPath->setSingle(single);
  shortestPath->setSrcs(std::move(srcs));
  shortestPath->setInputVar(cartesianProduct->outputVar());
  shortestPath->setEdgeProps(genEdgeProps(edge, reversely, qtx, spaceId));
  shortestPath->setReverseEdgeProps(genEdgeProps(edge, !reversely, qtx, spaceId));
  shortestPath->setEdgeDirection(edge.direction);
  shortestPath->setEdgeFilter(genEdgeFilter(edge));
  shortestPath->setStepRange(edge.range);

  auto vertexProps = SchemaUtil::getAllVertexProp(qtx, spaceId, true);
  NG_RETURN_IF_ERROR(vertexProps);
  shortestPath->setVertexProps(std::move(vertexProps).value());

  std::vector<std::string> cols = {nodeInfos[0].alias, edgeInfos[0].alias, nodeInfos[1].alias};
  shortestPath->setColNames(cols);

  subplan.root = shortestPath;

  return Status::OK();
}

StatusOr<std::vector<IndexID>> MatchClausePlanner::pickTagIndex(MatchClauseContext* matchClauseCtx,
                                                                NodeInfo nodeInfo) {
  std::vector<IndexID> indexIds;
  const auto* qctx = matchClauseCtx->qctx;
  auto tagIndexesResult = qctx->indexMng()->getTagIndexes(matchClauseCtx->space.id);
  NG_RETURN_IF_ERROR(tagIndexesResult);
  auto tagIndexes = std::move(tagIndexesResult).value();
  indexIds.reserve(1);

  auto tagId = nodeInfo.tids[0];
  std::shared_ptr<meta::cpp2::IndexItem> candidateIndex{nullptr};
  for (const auto& index : tagIndexes) {
    if (index->get_schema_id().get_tag_id() == tagId) {
      if (candidateIndex == nullptr) {
        candidateIndex = index;
      } else {
        candidateIndex = candidateIndex->get_fields().size() > index->get_fields().size()
                             ? index
                             : candidateIndex;
      }
    }
  }
  if (candidateIndex == nullptr) {
    return Status::SemanticError("No valid index for label `%s'.", nodeInfo.labels[0].c_str());
  }
  indexIds.emplace_back(candidateIndex->get_index_id());

  return indexIds;
}

}  // namespace graph
}  // namespace nebula
