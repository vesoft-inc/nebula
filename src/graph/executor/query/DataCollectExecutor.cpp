/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/DataCollectExecutor.h"

#include "common/time/ScopedTimer.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {
folly::Future<Status> DataCollectExecutor::execute() {
  return doCollect().ensure([this]() {
    result_ = Value::kEmpty;
    colNames_.clear();
  });
}

folly::Future<Status> DataCollectExecutor::doCollect() {
  SCOPED_TIMER(&execTime_);

  auto* dc = asNode<DataCollect>(node());
  colNames_ = dc->colNames();
  auto vars = dc->vars();
  switch (dc->kind()) {
    case DataCollect::DCKind::kSubgraph: {
      NG_RETURN_IF_ERROR(collectSubgraph(vars));
      break;
    }
    case DataCollect::DCKind::kRowBasedMove: {
      NG_RETURN_IF_ERROR(rowBasedMove(vars));
      break;
    }
    case DataCollect::DCKind::kMToN: {
      NG_RETURN_IF_ERROR(collectMToN(vars, dc->step(), dc->distinct()));
      break;
    }
    case DataCollect::DCKind::kBFSShortest: {
      NG_RETURN_IF_ERROR(collectBFSShortest(vars));
      break;
    }
    case DataCollect::DCKind::kAllPaths: {
      NG_RETURN_IF_ERROR(collectAllPaths(vars));
      break;
    }
    case DataCollect::DCKind::kMultiplePairShortest: {
      NG_RETURN_IF_ERROR(collectMultiplePairShortestPath(vars));
      break;
    }
    case DataCollect::DCKind::kPathProp: {
      NG_RETURN_IF_ERROR(collectPathProp(vars));
      break;
    }
    default:
      LOG(FATAL) << "Unknown data collect type: " << static_cast<int64_t>(dc->kind());
  }
  ResultBuilder builder;
  builder.value(Value(std::move(result_))).iter(Iterator::Kind::kSequential);
  return finish(builder.build());
}

Status DataCollectExecutor::collectSubgraph(const std::vector<std::string>& vars) {
  const auto* dc = asNode<DataCollect>(node());
  const auto& colType = dc->colType();
  DataSet ds;
  ds.colNames = std::move(colNames_);
  const auto& hist = ectx_->getHistory(vars[0]);
  for (const auto& result : hist) {
    auto iter = result.iter();
    auto* gnIter = static_cast<GetNeighborsIter*>(iter.get());
    List vertices;
    List edges;
    Row row;
    bool notEmpty = false;
    for (const auto& type : colType) {
      if (type == Value::Type::VERTEX) {
        auto originVertices = gnIter->getVertices();
        vertices.reserve(originVertices.size());
        for (auto& v : originVertices.values) {
          if (UNLIKELY(!v.isVertex())) {
            continue;
          }
          vertices.emplace_back(std::move(v));
        }
        if (!vertices.empty()) {
          notEmpty = true;
          row.emplace_back(std::move(vertices));
        }
      } else {
        auto originEdges = gnIter->getEdges();
        edges.reserve(originEdges.size());
        for (auto& edge : originEdges.values) {
          if (UNLIKELY(!edge.isEdge())) {
            continue;
          }
          edges.emplace_back(std::move(edge));
        }
        if (!edges.empty()) {
          notEmpty = true;
        }
        row.emplace_back(std::move(edges));
      }
    }
    if (notEmpty) {
      ds.rows.emplace_back(std::move(row));
    }
  }
  result_.setDataSet(std::move(ds));
  return Status::OK();
}

Status DataCollectExecutor::rowBasedMove(const std::vector<std::string>& vars) {
  DataSet ds;
  ds.colNames = std::move(colNames_);
  DCHECK(!ds.colNames.empty());
  size_t cap = 0;
  for (auto& var : vars) {
    auto& result = ectx_->getResult(var);
    auto iter = result.iter();
    cap += iter->size();
  }
  ds.rows.reserve(cap);
  for (auto& var : vars) {
    auto& result = ectx_->getResult(var);
    auto iter = result.iter();
    if (iter->isSequentialIter() || iter->isPropIter()) {
      auto* seqIter = static_cast<SequentialIter*>(iter.get());
      for (; seqIter->valid(); seqIter->next()) {
        ds.rows.emplace_back(seqIter->moveRow());
      }
    } else {
      return Status::Error("Iterator should be kind of SequentialIter.");
    }
  }
  result_.setDataSet(std::move(ds));
  return Status::OK();
}

Status DataCollectExecutor::collectMToN(const std::vector<std::string>& vars,
                                        const StepClause& mToN,
                                        bool distinct) {
  DataSet ds;
  ds.colNames = std::move(colNames_);
  DCHECK(!ds.colNames.empty());
  std::unordered_set<const Row*> unique;
  // itersHolder keep life cycle of iters util this method return.
  std::vector<std::unique_ptr<Iterator>> itersHolder;
  for (auto& var : vars) {
    auto& hist = ectx_->getHistory(var);
    std::size_t histSize = hist.size();
    DCHECK_GE(mToN.mSteps(), 1);
    std::size_t n = mToN.nSteps() > histSize ? histSize : mToN.nSteps();
    for (auto i = mToN.mSteps() - 1; i < n; ++i) {
      auto iter = hist[i].iter();
      if (iter->isSequentialIter()) {
        auto* seqIter = static_cast<SequentialIter*>(iter.get());
        while (seqIter->valid()) {
          if (distinct && !unique.emplace(seqIter->row()).second) {
            seqIter->unstableErase();
          } else {
            seqIter->next();
          }
        }
      } else {
        std::stringstream msg;
        msg << "Iterator should be kind of SequentialIter, but was: " << iter->kind();
        return Status::Error(msg.str());
      }
      itersHolder.emplace_back(std::move(iter));
    }
  }

  for (auto& iter : itersHolder) {
    if (iter->isSequentialIter()) {
      auto* seqIter = static_cast<SequentialIter*>(iter.get());
      for (seqIter->reset(); seqIter->valid(); seqIter->next()) {
        ds.rows.emplace_back(seqIter->moveRow());
      }
    }
  }
  result_.setDataSet(std::move(ds));
  return Status::OK();
}

Status DataCollectExecutor::collectBFSShortest(const std::vector<std::string>& vars) {
  // Will rewrite this method once we implement returning the props for the
  // path.
  return rowBasedMove(vars);
}

Status DataCollectExecutor::collectAllPaths(const std::vector<std::string>& vars) {
  DataSet ds;
  ds.colNames = std::move(colNames_);
  DCHECK(!ds.colNames.empty());

  for (auto& var : vars) {
    auto& hist = ectx_->getHistory(var);
    for (auto& result : hist) {
      auto iter = result.iter();
      if (iter->isSequentialIter()) {
        auto* seqIter = static_cast<SequentialIter*>(iter.get());
        for (; seqIter->valid(); seqIter->next()) {
          ds.rows.emplace_back(seqIter->moveRow());
        }
      } else {
        std::stringstream msg;
        msg << "Iterator should be kind of SequentialIter, but was: " << iter->kind();
        return Status::Error(msg.str());
      }
    }
  }
  result_.setDataSet(std::move(ds));
  return Status::OK();
}

Status DataCollectExecutor::collectMultiplePairShortestPath(const std::vector<std::string>& vars) {
  DataSet ds;
  ds.colNames = std::move(colNames_);
  DCHECK(!ds.colNames.empty());

  // src : {dst : <cost, {path}>}
  std::unordered_map<Value, std::unordered_map<Value, std::pair<Value, std::vector<Path>>>>
      shortestPath;

  for (auto& var : vars) {
    auto& hist = ectx_->getHistory(var);
    for (auto& result : hist) {
      auto iter = result.iter();
      if (!iter->isSequentialIter()) {
        std::stringstream msg;
        msg << "Iterator should be kind of SequentialIter, but was: " << iter->kind();
        return Status::Error(msg.str());
      }
      auto* seqIter = static_cast<SequentialIter*>(iter.get());
      for (; seqIter->valid(); seqIter->next()) {
        auto& pathVal = seqIter->getColumn(kPathStr);
        auto cost = seqIter->getColumn(kCostStr);
        if (!pathVal.isPath()) {
          return Status::Error("Type error `%s', should be PATH", pathVal.typeName().c_str());
        }
        auto& path = pathVal.getPath();
        auto& src = path.src.vid;
        auto& dst = path.steps.back().dst.vid;
        if (shortestPath.find(src) == shortestPath.end() ||
            shortestPath[src].find(dst) == shortestPath[src].end()) {
          auto& dstHist = shortestPath[src];
          std::vector<Path> tempPaths = {std::move(path)};
          dstHist.emplace(dst, std::make_pair(cost, std::move(tempPaths)));
        } else {
          auto oldCost = shortestPath[src][dst].first;
          if (cost < oldCost) {
            std::vector<Path> tempPaths = {std::move(path)};
            shortestPath[src][dst].second.swap(tempPaths);
          } else if (cost == oldCost) {
            shortestPath[src][dst].second.emplace_back(std::move(path));
          } else {
            continue;
          }
        }
      }
    }
  }

  // collect result
  for (auto& srcPath : shortestPath) {
    for (auto& dstPath : srcPath.second) {
      for (auto& path : dstPath.second.second) {
        Row row;
        row.values.emplace_back(std::move(path));
        ds.rows.emplace_back(std::move(row));
      }
    }
  }
  result_.setDataSet(std::move(ds));
  return Status::OK();
}

Status DataCollectExecutor::collectPathProp(const std::vector<std::string>& vars) {
  DataSet ds;
  ds.colNames = colNames_;
  // 0: vertices's props, 1: Edges's props 2: paths without prop
  DCHECK_EQ(vars.size(), 3);

  auto vIter = ectx_->getResult(vars[0]).iter();
  std::unordered_map<Value, Vertex> vertexMap;
  vertexMap.reserve(vIter->size());
  DCHECK(vIter->isPropIter());
  for (; vIter->valid(); vIter->next()) {
    const auto& vertexVal = vIter->getVertex();
    if (UNLIKELY(!vertexVal.isVertex())) {
      continue;
    }
    const auto& vertex = vertexVal.getVertex();
    vertexMap.insert(std::make_pair(vertex.vid, std::move(vertex)));
  }

  auto eIter = ectx_->getResult(vars[1]).iter();
  std::unordered_map<std::tuple<Value, EdgeType, EdgeRanking, Value>, Edge> edgeMap;
  edgeMap.reserve(eIter->size());
  DCHECK(eIter->isPropIter());
  for (; eIter->valid(); eIter->next()) {
    const auto& edgeVal = eIter->getEdge();
    if (UNLIKELY(!edgeVal.isEdge())) {
      continue;
    }
    auto& edge = edgeVal.getEdge();
    auto edgeKey = std::make_tuple(edge.src, edge.type, edge.ranking, edge.dst);
    edgeMap.insert(std::make_pair(std::move(edgeKey), std::move(edge)));
  }

  auto pIter = ectx_->getResult(vars[2]).iter();
  DCHECK(pIter->isSequentialIter());
  for (; pIter->valid(); pIter->next()) {
    const auto& pathVal = pIter->getColumn(0);
    if (UNLIKELY(!pathVal.isPath())) {
      continue;
    }
    auto path = pathVal.getPath();
    auto src = path.src.vid;
    auto found = vertexMap.find(src);
    if (found != vertexMap.end()) {
      path.src = found->second;
    }
    for (auto& step : path.steps) {
      auto dst = step.dst.vid;
      found = vertexMap.find(dst);
      if (found != vertexMap.end()) {
        step.dst = found->second;
      }

      auto type = step.type;
      auto ranking = step.ranking;
      if (type < 0) {
        std::swap(src, dst);
        type = -type;
      }
      auto edgeKey = std::make_tuple(src, type, ranking, dst);
      auto edge = edgeMap[edgeKey];
      step.props = edge.props;
      src = step.dst.vid;
    }
    ds.rows.emplace_back(Row({std::move(path)}));
  }
  VLOG(2) << "Path with props : \n" << ds;
  result_.setDataSet(std::move(ds));
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
