// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/planner/match/ShortestPathPlanner.h"

#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/match/MatchSolver.h"
#include "graph/planner/match/SegmentsConnector.h"
#include "graph/planner/match/StartVidFinder.h"
#include "graph/planner/plan/Algo.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/SchemaUtil.h"

namespace nebula {
namespace graph {
// Match (start:tagName{propName:xxx}), (end:tagName{propName:yyy})
// Match p = shortestPath( (start) - [edge*..maxHop] - (end) )
// Return p
// OR
// Match p = shortestPath((start:tagName{propName:x})-[:edge*..maxHop]-(end:tagName{propName:y}))
// Return p

// Match p = shortestPath( (start) - [edge*..maxHop] - (end) )
// Where id(start) IN [xxx,zzz] AND id(end) IN [yyy, kkk]
// return p
// OR
// Match p = shortestPath( (start) - [edge*..maxHop] - (end) )
// Where start.tagName.propName == 'xxx' AND end.tagName.propName == 'yyy'
// return p

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

static YieldColumn* buildVertexColumn(ObjectPool* pool, const std::string& alias) {
  return new YieldColumn(InputPropertyExpression::make(pool, alias), alias);
}

static YieldColumn* buildEdgeColumn(ObjectPool* pool, EdgeInfo& edge) {
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


static void buildProjectColumns(QueryContext* qctx, Path& path, SubPlan& plan) {
  auto& objPool = qctx->objPool();

  auto columns = objPool->makeAndAdd<YieldColumns>();
  std::vector<std::string> colNames;
  auto& nodeInfos = path.nodeInfos;
  auto& edgeInfos = path.edgeInfos;

  auto addNode = [columns, &colNames, qctx](auto& nodeInfo) {
    if (!nodeInfo.alias.empty() && !nodeInfo.anonymous) {
      columns->addColumn(buildVertexColumn(qctx->objPool(), nodeInfo.alias));
      colNames.emplace_back(nodeInfo.alias);
    }
  };

  auto addEdge = [columns, &colNames, qctx](auto& edgeInfo) {
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
    columns->addColumn(YieldColumn(DCHECK_NOTNULL(path.pathBuild), path.alias));
    colNames.emplace_back(path.alias);
  }

  auto project = Project::make(qctx, plan.root, columns);
  project->setColNames(std::move(colNames));

  plan.root = project;
}

StatusOr<SubPlan> ShortestPathPlanner::transform(
    QueryContext* qctx,
    GraphSpaceID spaceId,
    Expression* bindFilter,
    const std::unordered_map<std::string, AliasType>& aliasesAvailable,
    std::unordered_set<std::string> nodeAliasesSeen,
    Path& path) {
  std::unordered_set<std::string> allNodeAliasesAvailable;
  allNodeAliasesAvailable.merge(nodeAliasesSeen);
  std::for_each(
      aliasesAvailable.begin(), aliasesAvailable.end(), [&allNodeAliasesAvailable](auto& kv) {
        if (kv.second == AliasType::kNode) {
          allNodeAliasesAvailable.emplace(kv.first);
        }
      });

  SubPlan subplan;
  bool singleShortest = path.pathType == Path::PathType::kSingleShortest;
  auto& nodeInfos = path.nodeInfos;
  auto& edge = path.edgeInfos.front();
  std::vector<std::string> colNames;
  colNames.emplace_back(nodeInfos.front().alias);
  colNames.emplace_back(edge.alias);
  colNames.emplace_back(nodeInfos.back().alias);

  auto& startVidFinders = StartVidFinder::finders();
  std::vector<SubPlan> plans;

  for (auto& nodeInfo : nodeInfos) {
    bool foundIndex = false;
    for (auto& finder : startVidFinders) {
      auto nodeCtx = NodeContext(qctx, bindFilter, spaceId, &nodeInfo);
      nodeCtx.nodeAliasesAvailable = &allNodeAliasesAvailable;
      auto nodeFinder = finder();
      if (nodeFinder->match(&nodeCtx)) {
        auto status = nodeFinder->transform(&nodeCtx);
        NG_RETURN_IF_ERROR(status);
        auto plan = status.value();
        auto start = StartNode::make(qctx);
        plan.tail->setDep(0, start);
        plan.tail = start;

        auto initExpr = nodeCtx.initialExpr->clone();
        auto columns = qctx->objPool()->makeAndAdd<YieldColumns>();
        columns->addColumn(new YieldColumn(initExpr, nodeInfo.alias));
        plan.root = Project::make(qctx, plan.root, columns);

        plans.emplace_back(std::move(plan));
        foundIndex = true;
        break;
      }
    }
    if (!foundIndex) {
      return Status::SemanticError("Can't find index from shortestPath pattern");
    }
  }
  auto& leftPlan = plans.front();
  auto& rightPlan = plans.back();

  auto cp = BiCartesianProduct::make(qctx, leftPlan.root, rightPlan.root);

  auto shortestPath = ShortestPath::make(qctx, cp, spaceId, singleShortest);
  auto vertexProp = genVertexProps(nodeInfos.front(), qctx, spaceId);
  NG_RETURN_IF_ERROR(vertexProp);
  shortestPath->setVertexProps(std::move(vertexProp).value());
  shortestPath->setEdgeProps(genEdgeProps(edge, false, qctx, spaceId));
  shortestPath->setReverseEdgeProps(genEdgeProps(edge, true, qctx, spaceId));
  shortestPath->setEdgeDirection(edge.direction);
  shortestPath->setStepRange(edge.range);
  shortestPath->setColNames(std::move(colNames));

  subplan.root = shortestPath;
  subplan.tail = leftPlan.tail;

  buildProjectColumns(qctx, path, subplan);
  return subplan;
}

}  // namespace graph
}  // namespace nebula
