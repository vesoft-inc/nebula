// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#include "graph/executor/algo/MultiShortestPathExecutor.h"

#include "graph/planner/plan/Algo.h"

namespace nebula {
namespace graph {
folly::Future<Status> MultiShortestPathExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  pathNode_ = asNode<MultiShortestPath>(node());
  terminationVar_ = pathNode_->terminationVar();

  if (step_ == 1) {
    init();
  }

  std::vector<folly::Future<Status>> futures;
  auto leftFuture = folly::via(runner(), [this]() { return buildPath(false); });
  auto rightFuture = folly::via(runner(), [this]() { return buildPath(true); });
  futures.emplace_back(std::move(leftFuture));
  futures.emplace_back(std::move(rightFuture));

  return folly::collect(futures)
      .via(runner())
      .thenValue([this](auto&& status) {
        UNUSED(status);
        return conjunctPath(true);
      })
      .thenValue([this](auto&& termination) {
        if (termination || step_ * 2 > pathNode_->steps()) {
          return folly::makeFuture<bool>(true);
        }
        return conjunctPath(false);
      })
      .thenValue([this](auto&& resp) {
        UNUSED(resp);
        step_++;
        DataSet ds;
        ds.colNames = pathNode_->colNames();
        ds.rows.swap(currentDs_.rows);
        return finish(ResultBuilder().value(Value(std::move(ds))).build());
      });
}

void MultiShortestPathExecutor::init() {
  auto lIter = ectx_->getResult(pathNode_->leftVidVar()).iter();
  auto rIter = ectx_->getResult(pathNode_->rightVidVar()).iter();
  std::set<Value> rightVids;
  for (; rIter->valid(); rIter->next()) {
    auto& vid = rIter->getColumn(0);
    if (rightVids.emplace(vid).second) {
      preRightPaths_[vid].push_back({Path(Vertex(vid, {}), {})});
    }
  }

  std::set<Value> leftVids;
  for (; lIter->valid(); lIter->next()) {
    auto& vid = lIter->getColumn(0);
    leftVids.emplace(vid);
  }
  for (const auto& leftVid : leftVids) {
    for (const auto& rightVid : rightVids) {
      if (leftVid != rightVid) {
        terminationMap_.insert({leftVid, {rightVid, true}});
      }
    }
  }
}

Status MultiShortestPathExecutor::buildPath(bool reverse) {
  auto iter = reverse ? ectx_->getResult(pathNode_->rightInputVar()).iter()
                      : ectx_->getResult(pathNode_->leftInputVar()).iter();
  DCHECK(!!iter);
  auto& currentPaths = reverse ? rightPaths_ : leftPaths_;
  if (step_ == 1) {
    for (; iter->valid(); iter->next()) {
      auto edgeVal = iter->getEdge();
      if (UNLIKELY(!edgeVal.isEdge())) {
        continue;
      }
      auto& edge = edgeVal.getEdge();
      auto& src = edge.src;
      auto& dst = edge.dst;
      if (src == dst) {
        continue;
      }
      Path path;
      path.src = Vertex(src, {});
      path.steps.emplace_back(Step(Vertex(dst, {}), edge.type, edge.name, edge.ranking, {}));
      currentPaths[dst].emplace_back(std::move(path));
    }
  } else {
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
        if (path.hasDuplicateVertices()) {
          continue;
        }
        currentPaths[dst].emplace_back(std::move(path));
      }
    }
  }
  // set nextVid
  const auto& nextVidVar = reverse ? pathNode_->rightVidVar() : pathNode_->leftVidVar();
  setNextStepVid(currentPaths, nextVidVar);
  return Status::OK();
}

DataSet MultiShortestPathExecutor::doConjunct(Interims::iterator startIter,
                                              Interims::iterator endIter,
                                              bool oddStep) {
  auto& rightPaths = oddStep ? preRightPaths_ : rightPaths_;
  DataSet ds;
  for (; startIter != endIter; ++startIter) {
    auto found = rightPaths.find(startIter->first);
    if (found == rightPaths.end()) {
      continue;
    }
    for (const auto& lPath : startIter->second) {
      const auto& srcVid = lPath.src.vid;
      auto range = terminationMap_.equal_range(srcVid);
      for (const auto& rPath : found->second) {
        const auto& dstVid = rPath.src.vid;
        for (auto iter = range.first; iter != range.second; ++iter) {
          if (iter->second.first == dstVid) {
            auto forwardPath = lPath;
            auto backwardPath = rPath;
            backwardPath.reverse();
            forwardPath.append(std::move(backwardPath));
            if (forwardPath.hasDuplicateVertices()) {
              continue;
            }
            Row row;
            row.values.emplace_back(std::move(forwardPath));
            ds.rows.emplace_back(std::move(row));
          }
        }
      }
    }
  }
  return ds;
}

folly::Future<bool> MultiShortestPathExecutor::conjunctPath(bool oddStep) {
  static size_t NUM_PROC = 5;
  size_t batchSize = leftPaths_.size() / NUM_PROC;
  std::vector<folly::Future<DataSet>> futures;
  size_t i = 0;

  auto startIter = leftPaths_.begin();
  for (auto leftIter = leftPaths_.begin(); leftIter != leftPaths_.end(); ++leftIter) {
    if (i++ == batchSize) {
      auto endIter = leftIter;
      endIter++;
      auto future = folly::via(runner(), [this, startIter, endIter, oddStep]() {
        return doConjunct(startIter, endIter, oddStep);
      });
      futures.emplace_back(std::move(future));
      i = 0;
      startIter = endIter;
    }
  }
  if (i != 0) {
    auto endIter = leftPaths_.end();
    auto future = folly::via(runner(), [this, startIter, endIter, oddStep]() {
      return doConjunct(startIter, endIter, oddStep);
    });
    futures.emplace_back(std::move(future));
  }

  return folly::collect(futures).via(runner()).thenValue([this](auto&& resps) {
    for (auto& resp : resps) {
      currentDs_.append(std::move(resp));
    }

    for (auto iter = terminationMap_.begin(); iter != terminationMap_.end();) {
      if (!iter->second.second) {
        iter = terminationMap_.erase(iter);
      } else {
        ++iter;
      }
    }
    if (terminationMap_.empty()) {
      ectx_->setValue(terminationVar_, true);
      return true;
    }
    return false;
  });
}

void MultiShortestPathExecutor::setNextStepVid(const Interims& paths, const string& var) {
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
