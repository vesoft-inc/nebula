// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#include "graph/executor/algo/ProduceAllPathsExecutor.h"
#include "graph/planner/plan/Algo.h"

namespace nebula {
namespace graph {
folly::Future<Status> ProduceAllPathsExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* path = asNode<ProduceAllPaths>(node());
  noLoop_ = path->noLoop();
  auto leftIter = ectx_->getResult(path->leftInputVar()).iter();
  auto rightIter = ectx_->getResult(path->rightInputVar()).iter();
  DCHECK(!!leftIter && !!rightIter);
  DataSet ds;
  ds.colNames = node()->colNames();

  if (step_ == 1) {
    auto rIter = ectx_->getResult(path->rightVidVar()).iter();
    std::unordered_set<Value> rightVids;
    for (; rIter->valid(); rIter->next()) {
      auto& vid = rIter->getColumn(0);
      if (rightVids.emplace(vid).second) {
        preRightPaths_[vid].push_back({Path(Vertex(vid, {}), {})});
      }
    }
  }

  Interims leftPaths, rightPaths;
  buildPath(leftIter.get(), leftPaths, false);

  conjunctPath(leftPaths, preRightPaths_, ds);

  if (step_ * 2 <= path->steps()) {
    buildPath(rightIter.get(), rightPaths, true);
    conjunctPath(leftPaths, rightPaths, ds);
  }

  setNextStepVid(leftPaths, path->leftVidVar());
  setNextStepVid(rightPaths, path->rightVidVar());
  // update history
  preLeftPaths_.swap(leftPaths);
  preRightPaths_.swap(rightPaths);
  step_++;
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

void ProduceAllPathsExecutor::buildPath(Iterator* iter, Interims& currentPaths, bool reverse) {
  if (step_ == 1) {
    for (; iter->valid(); iter->next()) {
      auto edgeVal = iter->getEdge();
      if (UNLIKELY(!edgeVal.isEdge())) {
        continue;
      }
      auto& edge = edgeVal.getEdge();
      auto& src = edge.src;
      auto& dst = edge.dst;
      if (noLoop_ && src == dst) {
        continue;
      }
      Path path;
      path.src = Vertex(src, {});
      path.steps.emplace_back(Step(Vertex(dst, {}), edge.type, edge.name, edge.ranking, {}));
      currentPaths[dst].emplace_back(std::move(path));
    }
    return;
  }
  auto& historyPaths = reverse ? preRightPaths_ : preLeftPaths_;
  for (; iter->valid(); iter->next()) {
    auto edgeVal = iter->getEdge();
    if (UNLIKELY(!edgeVal.isEdge())) {
      continue;
    }
    auto& edge = edgeVal.getEdge();
    auto& src = edge.src;
    auto& dst = edge.dst;
    for (const auto& histPath : historyPaths[src]) {
      Path path = histPath;
      path.steps.emplace_back(Step(Vertex(dst, {}), edge.type, edge.name, edge.ranking, {}));
      if (path.hasDuplicateEdges()) {
        continue;
      }
      if (noLoop_ && path.hasDuplicateVertices()) {
        continue;
      }
      currentPaths[dst].emplace_back(std::move(path));
    }
  }
}

void ProduceAllPathsExecutor::conjunctPath(Interims& leftPaths, Interims& rightPaths, DataSet& ds) {
  for (auto& leftPath : leftPaths) {
    auto find = rightPaths.find(leftPath.first);
    if (find == rightPaths.end()) {
      continue;
    }
    for (const auto& lPath : leftPath.second) {
      for (const auto& rPath : find->second) {
        auto forwardPath = lPath;
        auto backwardPath = rPath;
        backwardPath.reverse();
        forwardPath.append(std::move(backwardPath));
        if (forwardPath.hasDuplicateEdges()) {
          continue;
        }
        if (noLoop_ && forwardPath.hasDuplicateVertices()) {
          continue;
        }
        Row row;
        row.values.emplace_back(std::move(forwardPath));
        ds.rows.emplace_back(std::move(row));
      }
    }
  }
}

void ProduceAllPathsExecutor::setNextStepVid(Interims& paths, const string& var) {
  DataSet ds;
  ds.colNames = {nebula::kVid};
  for (const auto& path : paths) {
    Row row;
    row.values.emplace_back(path.first);
    ds.rows.emplace_back(std::move(row));
  }
  ectx_->setResult(var, ResultBuilder().value(std::move(ds)).build());
}

}  // namespace graph
}  // namespace nebula
