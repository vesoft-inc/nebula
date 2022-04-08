// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/algo/BFSShortestPathExecutor.h"

#include "graph/planner/plan/Algo.h"

namespace nebula {
namespace graph {
folly::Future<Status> BFSShortestPathExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  pathNode_ = asNode<BFSShortestPath>(node());

  if (step_ == 1) {
    allRightEdges_.emplace_back();
    auto& currentEdges = allRightEdges_.back();
    auto rIter = ectx_->getResult(pathNode_->rightVidVar()).iter();
    std::unordered_set<Value> rightVids;
    for (; rIter->valid(); rIter->next()) {
      auto& vid = rIter->getColumn(0);
      if (rightVids.emplace(vid).second) {
        Edge dummy;
        currentEdges.emplace(vid, std::move(dummy));
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

Status BFSShortestPathExecutor::buildPath(bool reverse) {
  auto iter = reverse ? ectx_->getResult(pathNode_->rightInputVar()).iter()
                      : ectx_->getResult(pathNode_->leftInputVar()).iter();
  DCHECK(!!iter);
  auto& visitedVids = reverse ? rightVisitedVids_ : leftVisitedVids_;
  auto& allEdges = reverse ? allRightEdges_ : allLeftEdges_;
  allEdges.emplace_back();
  auto& currentEdges = allEdges.back();

  auto iterSize = iter->size();
  visitedVids.reserve(visitedVids.size() + iterSize);

  std::unordered_set<Value> uniqueDst;
  uniqueDst.reserve(iterSize);
  DataSet nextStepVids;
  nextStepVids.colNames = {nebula::kVid};
  if (step_ == 1) {
    for (; iter->valid(); iter->next()) {
      auto edgeVal = iter->getEdge();
      if (UNLIKELY(!edgeVal.isEdge())) {
        continue;
      }
      auto& edge = edgeVal.getEdge();
      auto dst = edge.dst;
      visitedVids.emplace(edge.src);
      if (uniqueDst.emplace(dst).second) {
        nextStepVids.rows.emplace_back(Row({dst}));
      }
      currentEdges.emplace(std::move(dst), std::move(edge));
    }
  } else {
    for (; iter->valid(); iter->next()) {
      auto edgeVal = iter->getEdge();
      if (UNLIKELY(!edgeVal.isEdge())) {
        continue;
      }
      auto& edge = edgeVal.getEdge();
      auto dst = edge.dst;
      if (visitedVids.find(dst) != visitedVids.end()) {
        continue;
      }
      if (uniqueDst.emplace(dst).second) {
        nextStepVids.rows.emplace_back(Row({dst}));
      }
      currentEdges.emplace(std::move(dst), std::move(edge));
    }
  }
  // set nextVid
  const auto& nextVidVar = reverse ? pathNode_->rightVidVar() : pathNode_->leftVidVar();
  ectx_->setResult(nextVidVar, ResultBuilder().value(std::move(nextStepVids)).build());

  visitedVids.insert(std::make_move_iterator(uniqueDst.begin()),
                     std::make_move_iterator(uniqueDst.end()));
  return Status::OK();
}

folly::Future<Status> BFSShortestPathExecutor::conjunctPath() {
  const auto& leftEdges = allLeftEdges_.back();
  const auto& preRightEdges = allRightEdges_[step_ - 1];
  std::vector<Value> meetVids;
  bool oddStep = true;
  for (const auto& edge : leftEdges) {
    if (preRightEdges.find(edge.first) != preRightEdges.end()) {
      meetVids.push_back(edge.first);
    }
  }
  if (meetVids.empty() && step_ * 2 <= pathNode_->steps()) {
    const auto& rightEdges = allRightEdges_.back();
    for (const auto& edge : leftEdges) {
      if (rightEdges.find(edge.first) != rightEdges.end()) {
        meetVids.push_back(edge.first);
        oddStep = false;
      }
    }
  }
  if (meetVids.empty()) {
    return Status::OK();
  }
  static size_t NUM_PROC = 5;
  size_t totalSize = meetVids.size();
  size_t batchSize = totalSize / NUM_PROC;
  std::vector<Value> batchVids;
  batchVids.reserve(batchSize);
  std::vector<folly::Future<DataSet>> futures;
  for (size_t i = 0; i < totalSize; ++i) {
    batchVids.push_back(meetVids[i]);
    if (i == totalSize - 1 || batchVids.size() == batchSize) {
      auto future = folly::via(runner(), [this, vids = std::move(batchVids), oddStep]() {
        return doConjunct(vids, oddStep);
      });
      futures.emplace_back(std::move(future));
    }
  }

  return folly::collect(futures).via(runner()).thenValue([this](auto&& resps) {
    for (auto& resp : resps) {
      currentDs_.append(std::move(resp));
    }
    return Status::OK();
  });
}

DataSet BFSShortestPathExecutor::doConjunct(const std::vector<Value> meetVids, bool oddStep) {
  DataSet ds;
  auto leftPaths = createPath(meetVids, false, oddStep);
  auto rightPaths = createPath(meetVids, true, oddStep);
  for (auto& leftPath : leftPaths) {
    auto range = rightPaths.equal_range(leftPath.first);
    for (auto& rightPath = range.first; rightPath != range.second; ++rightPath) {
      Path result = leftPath.second;
      result.reverse();
      result.append(rightPath->second);
      Row row;
      row.emplace_back(std::move(result));
      ds.rows.emplace_back(std::move(row));
    }
  }
  return ds;
}

std::unordered_multimap<Value, Path> BFSShortestPathExecutor::createPath(
    std::vector<Value> meetVids, bool reverse, bool oddStep) {
  std::unordered_multimap<Value, Path> result;
  auto& allEdges = reverse ? allRightEdges_ : allLeftEdges_;
  for (auto& meetVid : meetVids) {
    Path start;
    start.src = Vertex(meetVid, {});
    std::vector<Path> interimPaths = {std::move(start)};
    auto iter = (reverse && oddStep) ? allEdges.rbegin() + 1 : allEdges.rbegin();
    auto end = reverse ? allEdges.rend() - 1 : allEdges.rend();
    if (iter == end) {
      result.emplace(meetVid, std::move(start));
      return result;
    }
    for (; iter != end; ++iter) {
      std::vector<Path> temp;
      for (auto& interimPath : interimPaths) {
        Value id;
        if (interimPath.steps.empty()) {
          id = interimPath.src.vid;
        } else {
          id = interimPath.steps.back().dst.vid;
        }
        auto range = iter->equal_range(id);
        for (auto edgeIter = range.first; edgeIter != range.second; ++edgeIter) {
          Path p = interimPath;
          auto& edge = edgeIter->second;
          p.steps.emplace_back(Step(Vertex(edge.src, {}), -edge.type, edge.name, edge.ranking, {}));

          if (iter == end - 1) {
            result.emplace(p.src.vid, std::move(p));
          } else {
            temp.emplace_back(std::move(p));
          }
        }  //  edgeIter
      }    // interimPath
      interimPaths = std::move(temp);
    }
  }
  return result;
}

}  // namespace graph
}  // namespace nebula
