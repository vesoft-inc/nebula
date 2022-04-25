// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/planner/match/ShortestPathPlanner.h"

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

static YieldColumn* buildVertexColumn(ObjectPool* pool, const std::string& alias) const {
  return new YieldColumn(InputPropertyExpression::make(pool, alias), alias);
}

static YieldColumn* buildEdgeColumn(ObjectPool* pool, EdgeInfo& edge) const {
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

static YieldColumn* buildPathColumn(Expression* pathBuild, const std::string& alias) const {
  return new YieldColumn(pathBuild, alias);
}

static void buildProjectColumns(QueryContext* qctx, Path& path, SubPlan& plan) {
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
}

StatusOr<SubPlan> ShortestPathPlanner::transform(
    QueryContext* qctx,
    GraphSpaceID spaceId,
    Expression* bindFilter,
    const std::unordered_map<std::string, AliasType>& aliasesAvailable,
    std::unordered_set<std::string> nodeAliasesSeen,
    Path& path) {
  UNUSED(aliasesAvailable);
  UNUSED(nodeAliasesSeen);

  SubPlan subplan;
  bool singleShortest = path.pathType == MatchPath::PathType::kSingleShortest;
  auto& nodeInfos = path.nodeInfos;
  auto& edge = path.edgeInfos.front();

  auto& startVidFinders = StartVidFinder::finders();
  std::vector<SubPlan> plans;

  for (size_t i = 0; i < nodeInfos.size(); ++i) {
    bool foundIndex = false;
    for (auto& finder : startVidFinders) {
      auto nodeCtx = NodeContext(qctx, bindFilter, spaceId, &nodeInfos[i]);
      auto nodeFinder = finder();
      if (nodeFinder->match(&nodeCtx)) {
        auto plan = nodeFinder->transform(&nodeCtx);
        NG_RETURN_IF_ERROR(plan);
        plans.emplace_back(std::move(plan).value());
        foundIndex = true;
        break;
      }
    }
    if (!foundIndex) {
      return Status::SemanticError("Can't find index from path pattern");
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
  shortestPath->setReverseEdgeProps(getEdgeProps(edge, true, qctx, spaceId));
  shortestPath->setEdgeDirection(edge.direction);
  shortestPath->setStepRange(edge.range);

  subplan.root = shortestPath;
  subplan.tail = leftPlan.tail;

  bulildProjectColumns(qctx, path, subplan);

  return subplan;
}

}  // namespace graph
}  // namespace nebula
