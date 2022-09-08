// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#include "graph/executor/algo/ProduceAllPathsExecutor.h"

#include <robin_hood.h>

#include "graph/planner/plan/Algo.h"

DECLARE_int32(num_operator_threads);
namespace nebula {
namespace graph {
folly::Future<Status> ProduceAllPathsExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  pathNode_ = asNode<ProduceAllPaths>(node());
  noLoop_ = pathNode_->noLoop();

  if (step_ == 1) {
    auto rIter = ectx_->getResult(pathNode_->rightVidVar()).iter();
    using HashSet = robin_hood::unordered_flat_set<Value, std::hash<Value>>;
    HashSet rightVids;
    for (; rIter->valid(); rIter->next()) {
      auto& vid = rIter->getColumn(0);
      if (rightVids.emplace(vid).second) {
        preRightPaths_[vid].push_back({Path(Vertex(vid, {}), {})});
      }
    }
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
        return conjunctPath();
      })
      .thenValue([this](auto&& status) {
        UNUSED(status);
        step_++;
        DataSet ds;
        ds.colNames = pathNode_->colNames();
        ds.rows.swap(currentDs_.rows);
        return finish(ResultBuilder().value(Value(std::move(ds))).build());
      });
}

Status ProduceAllPathsExecutor::buildPath(bool reverse) {
  auto iter = reverse ? ectx_->getResult(pathNode_->rightInputVar()).iter()
                      : ectx_->getResult(pathNode_->leftInputVar()).iter();
  DCHECK(iter);
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
      if (noLoop_ && src == dst) {
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
  // set nextVid
  const auto& nextVidVar = reverse ? pathNode_->rightVidVar() : pathNode_->leftVidVar();
  setNextStepVid(currentPaths, nextVidVar);
  return Status::OK();
}

DataSet ProduceAllPathsExecutor::doConjunct(Interims::iterator startIter,
                                            Interims::iterator endIter,
                                            bool oddStep) const {
  auto& rightPaths = oddStep ? preRightPaths_ : rightPaths_;
  DataSet ds;
  for (; startIter != endIter; ++startIter) {
    auto found = rightPaths.find(startIter->first);
    if (found == rightPaths.end()) {
      continue;
    }
    for (const auto& lPath : startIter->second) {
      for (const auto& rPath : found->second) {
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
  return ds;
}

folly::Future<Status> ProduceAllPathsExecutor::conjunctPath() {
  auto batchSize = leftPaths_.size() / static_cast<size_t>(FLAGS_num_operator_threads);
  std::vector<folly::Future<DataSet>> futures;
  size_t i = 0;

  auto startIter = leftPaths_.begin();
  for (auto leftIter = leftPaths_.begin(); leftIter != leftPaths_.end(); ++leftIter) {
    if (++i == batchSize) {
      auto endIter = leftIter;
      endIter++;
      auto oddStepFuture = folly::via(
          runner(), [this, startIter, endIter]() { return doConjunct(startIter, endIter, true); });
      futures.emplace_back(std::move(oddStepFuture));
      if (step_ * 2 <= pathNode_->steps()) {
        auto evenStepFuture = folly::via(runner(), [this, startIter, endIter]() {
          return doConjunct(startIter, endIter, false);
        });
        futures.emplace_back(std::move(evenStepFuture));
      }

      i = 0;
      startIter = endIter;
    }
  }
  if (i != 0) {
    auto endIter = leftPaths_.end();
    auto oddStepFuture = folly::via(
        runner(), [this, startIter, endIter]() { return doConjunct(startIter, endIter, true); });
    futures.emplace_back(std::move(oddStepFuture));
    if (step_ * 2 <= pathNode_->steps()) {
      auto evenStepFuture = folly::via(
          runner(), [this, startIter, endIter]() { return doConjunct(startIter, endIter, false); });
      futures.emplace_back(std::move(evenStepFuture));
    }
  }

  return folly::collect(futures).via(runner()).thenValue([this](auto&& resps) {
    for (auto& resp : resps) {
      currentDs_.append(std::move(resp));
    }
    preLeftPaths_.swap(leftPaths_);
    preRightPaths_.swap(rightPaths_);
    leftPaths_.clear();
    rightPaths_.clear();
    return Status::OK();
  });
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
