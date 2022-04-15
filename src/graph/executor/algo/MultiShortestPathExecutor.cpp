// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#include "graph/executor/algo/MultiShortestPathExecutor.h"

#include "graph/planner/plan/Algo.h"
DECLARE_int32(num_operator_threads);
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
        // oddStep
        UNUSED(status);
        return conjunctPath(true);
      })
      .thenValue([this](auto&& termination) {
        // termination is ture, all paths has found
        if (termination || step_ * 2 > pathNode_->steps()) {
          return folly::makeFuture<bool>(true);
        }
        // evenStep
        return conjunctPath(false);
      })
      .thenValue([this](auto&& resp) {
        UNUSED(resp);
        preLeftPaths_.swap(leftPaths_);
        preRightPaths_.swap(rightPaths_);
        leftPaths_.clear();
        rightPaths_.clear();
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
        terminationMap_.emplace(std::make_pair(leftVid, rightVid), true);
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

DataSet MultiShortestPathExecutor::doConjunct(
    const std::vector<std::pair<Interims::iterator, Interims::iterator>>& iters) {
  DataSet ds;
  for (const auto& iter : iters) {
    const auto& leftPaths = iter.first->second;
    const auto& rightPaths = iter.second->second;
    if (leftPaths.size() < rightPaths.size()) {
      for (const auto& leftPath : leftPaths) {
        const auto& srcVid = leftPath.src.vid;
        for (const auto& rightPath : rightPaths) {
          const auto& dstVid = rightPath.src.vid;
          auto found = terminationMap_.find({srcVid, dstVid});
          if (found == terminationMap_.end()) {
            continue;
          }
          auto forwardPath = leftPath;
          auto backwardPath = rightPath;
          backwardPath.reverse();
          forwardPath.append(std::move(backwardPath));
          Row row;
          row.values.emplace_back(std::move(forwardPath));
          ds.rows.emplace_back(std::move(row));
          found->second = false;
        }
      }
    } else {
      for (const auto& rightPath : rightPaths) {
        const auto& dstVid = rightPath.src.vid;
        for (const auto& leftPath : leftPaths) {
          const auto& srcVid = leftPath.src.vid;
          auto found = terminationMap_.find({srcVid, dstVid});
          if (found == terminationMap_.end()) {
            continue;
          }
          auto forwardPath = leftPath;
          auto backwardPath = rightPath;
          backwardPath.reverse();
          forwardPath.append(std::move(backwardPath));
          Row row;
          row.values.emplace_back(std::move(forwardPath));
          ds.rows.emplace_back(std::move(row));
          found->second = false;
        }
      }
    }
  }
  return ds;
}

folly::Future<bool> MultiShortestPathExecutor::conjunctPath(bool oddStep) {
  auto& rightPaths = oddStep ? preRightPaths_ : rightPaths_;
  size_t leftPathSize = leftPaths_.size();
  size_t rightPathSize = rightPaths.size();
  std::vector<folly::Future<DataSet>> futures;
  std::vector<std::pair<Interims::iterator, Interims::iterator>> pathIters;

  size_t i = 0;
  if (leftPathSize > rightPathSize) {
    size_t batchSize = leftPathSize / static_cast<size_t>(FLAGS_num_operator_threads);
    pathIters.reserve(batchSize);
    for (auto leftIter = leftPaths_.begin(); leftIter != leftPaths_.end(); ++leftIter) {
      auto rightIter = rightPaths.find(leftIter->first);
      if (rightIter == rightPaths.end()) {
        continue;
      }
      pathIters.emplace_back(leftIter, rightIter);
      if (++i == batchSize) {
        auto future = folly::via(
            runner(), [this, iters = std::move(pathIters)]() { return doConjunct(iters); });
        futures.emplace_back(std::move(future));
        pathIters.reserve(batchSize);
        i = 0;
      }
    }
  } else {
    size_t batchSize = rightPathSize / static_cast<size_t>(FLAGS_num_operator_threads);
    pathIters.reserve(batchSize);
    for (auto rightIter = rightPaths.begin(); rightIter != rightPaths.end(); ++rightIter) {
      auto leftIter = leftPaths_.find(rightIter->first);
      if (leftIter == leftPaths_.end()) {
        continue;
      }
      pathIters.emplace_back(leftIter, rightIter);
      if (++i == batchSize) {
        auto future = folly::via(
            runner(), [this, iters = std::move(pathIters)]() { return doConjunct(iters); });
        futures.emplace_back(std::move(future));
        pathIters.reserve(batchSize);
        i = 0;
      }
    }
  }
  if (i != 0) {
    auto future =
        folly::via(runner(), [this, iters = std::move(pathIters)]() { return doConjunct(iters); });
    futures.emplace_back(std::move(future));
  }

  return folly::collect(futures).via(runner()).thenValue([this](auto&& resps) {
    for (auto& resp : resps) {
      currentDs_.append(std::move(resp));
    }

    for (auto iter = terminationMap_.begin(); iter != terminationMap_.end();) {
      if (!iter->second) {
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
