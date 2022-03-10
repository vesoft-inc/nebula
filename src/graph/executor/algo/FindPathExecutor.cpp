// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/algo/FindPathExecutor.h"
#include "graph/planner/plan/Algo.h"

namespace nebula {
namespace graph {
void FindPathExecutor::init() {
  auto* path = asNode<FindPath>(node());
  auto lIter = ectx_->getResult(path->leftVidVar()).iter();
  auto rIter = ectx_->getResult(path->rightVidVar()).iter();
  std::set<Value> rightVids;
  for (; rIter->valid(); rIter->next()) {
    auto& vid = rIter->getColumn(0);
    rightVids.emplace(vid);
  }
  for (auto& vid : rightVids) {
    preRightPaths_[vid].push_back({Path(Vertex(vid, {}), {})});
  }

  if (shortest_) {
    std::set<Value> leftVids;
    for (; lIter->valid(); lIter->next()) {
      auto& vid = lIter->getColumn(0);
      leftVids.emplace(vid);
    }
    for (const auto& leftVid : leftVids) {
      for (const auto& rightVid : rightVids) {
        if (leftVid != rightVid) {
          termination_.insert({leftVid, {rightVid, true}});
        }
      }
    }
  }
}

void FindPathExecutor::setNextStepVidFromPath(Interims& paths, const string& var) {
  DataSet Ds;
  for (const auto& path : paths) {
    Row row;
    row.values.emplace_back(path.first);
    Ds.rows.emplace_back(std::move(row));
  }
  ectx_->setResult(var, ResultBuilder().value(std::move(Ds)).build());
}

bool FindPathExecutor::conjunctPath(Interims& leftPaths, Interims& rightPaths, DataSet& ds) {
  for (auto& leftPath : leftPaths) {
    auto find = rightPaths.find(leftPath.first);
    if (find == rightPaths.end()) {
      continue;
    }
    for (const auto& lPath : leftPath.second) {
      const auto& srcVid = lPath.src.vid;
      for (const auto& rPath : find->second) {
        const auto& dstVid = rPath.src.vid;
        if (shortest_) {
          auto range = termination_.equal_range(srcVid);
          for (auto iter = range.first; iter != range.second; ++iter) {
            if (iter->second.first == dstVid) {
              VLOG(1) << "Find Common Vid : " << find->first;
              auto forwardPath = lPath;
              auto backwardPath = rPath;
              backwardPath.reverse();
              forwardPath.append(std::move(backwardPath));
              Row row;
              row.values.emplace_back(std::move(forwardPath));
              ds.rows.emplace_back(std::move(row));
              iter->second.second = false;
            }
          }
        } else {
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
  if (shortest_) {
    for (auto iter = termination_.begin(); iter != termination_.end();) {
      if (!iter->second.second) {
        iter = termination_.erase(iter);
      } else {
        ++iter;
      }
    }
    if (termination_.empty()) {
      ectx_->setValue(terminationVar_, true);
      return true;
    }
  }
  return false;
}

void FindPathExecutor::buildPath(Iterator* iter, Interims& currentPaths, bool reverse) {
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
      if ((noLoop_ || shortest_) && path.hasDuplicateVertices()) {
        continue;
      }
      currentPaths[dst].emplace_back(std::move(path));
    }
  }
}

folly::Future<Status> FindPathExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* findPath = asNode<FindPath>(node());
  steps_ = findPath->steps();
  noLoop_ = findPath->noLoop();
  shortest_ = findPath->isShortest();
  terminationVar_ = findPath->terminationVar();
  auto leftIter = ectx_->getResult(findPath->leftInputVar()).iter();
  auto rightIter = ectx_->getResult(findPath->rightInputVar()).iter();
  DCHECK(!!leftIter && !!rightIter);
  DataSet ds;
  ds.colNames = node()->colNames();

  if (step_ == 1) {
    init();
  }

  Interims leftPaths, rightPaths;
  buildPath(leftIter.get(), leftPaths, false);

  if (conjunctPath(leftPaths, preRightPaths_, ds)) {
    return finish(ResultBuilder().value(Value(std::move(ds))).build());
  }

  if (step_ * 2 <= steps_) {
    buildPath(rightIter.get(), rightPaths, true);
    conjunctPath(leftPaths, rightPaths, ds);
  }

  auto leftVidVar = findPath->leftVidVar();
  auto rightVidVar = findPath->rightVidVar();
  setNextStepVidFromPath(leftPaths, leftVidVar);
  setNextStepVidFromPath(rightPaths, rightVidVar);
  // update history
  preLeftPaths_.swap(leftPaths);
  preRightPaths_.swap(rightPaths);
  step_++;
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}
}  // namespace graph
}  // namespace nebula
