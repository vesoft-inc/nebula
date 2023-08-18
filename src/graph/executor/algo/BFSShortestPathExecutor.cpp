// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/algo/BFSShortestPathExecutor.h"

#include "graph/planner/plan/Algo.h"

DECLARE_int32(num_operator_threads);
namespace nebula {
namespace graph {
folly::Future<Status> BFSShortestPathExecutor::execute() {
  // MemoryTrackerVerified
  SCOPED_TIMER(&execTime_);
  pathNode_ = asNode<BFSShortestPath>(node());
  terminateEarlyVar_ = pathNode_->terminateEarlyVar();

  if (step_ == 1) {
    allRightEdges_.emplace_back();
    auto& currentEdges = allRightEdges_.back();
    auto rIter = ectx_->getResult(pathNode_->rightVidVar()).iter();
    HashSet rightVids;
    for (; rIter->valid(); rIter->next()) {
      auto& vid = rIter->getColumn(0);
      if (rightVids.emplace(vid).second) {
        Edge dummy;
        currentEdges.emplace(vid, std::move(dummy));
      }
    }
  }

  std::vector<folly::Future<Status>> futures;
  auto leftFuture = folly::via(runner(), [this]() {
    // MemoryTrackerVerified
    memory::MemoryCheckGuard guard;
    return buildPath(false);
  });
  auto rightFuture = folly::via(runner(), [this]() {
    // MemoryTrackerVerified
    memory::MemoryCheckGuard guard;
    return buildPath(true);
  });
  futures.emplace_back(std::move(leftFuture));
  futures.emplace_back(std::move(rightFuture));

  return folly::collectAll(futures)
      .via(runner())
      .thenValue([this](std::vector<folly::Try<Status>>&& resps) {
        for (auto& respVal : resps) {
          if (respVal.hasException()) {
            auto ex = respVal.exception().get_exception<std::bad_alloc>();
            if (ex) {
              throw std::bad_alloc();
            } else {
              throw std::runtime_error(respVal.exception().what().c_str());
            }
          }
        }
        memory::MemoryCheckGuard guard;
        return conjunctPath();
      })
      .thenValue([this](auto&& status) {
        NG_RETURN_IF_ERROR(status);
        memory::MemoryCheckGuard guard;
        step_++;
        DataSet ds;
        ds.colNames = pathNode_->colNames();
        ds.rows.swap(currentDs_.rows);
        return finish(ResultBuilder().value(Value(std::move(ds))).build());
      })
      .thenError(
          folly::tag_t<std::bad_alloc>{},
          [](const std::bad_alloc&) { return folly::makeFuture<Status>(memoryExceededStatus()); })
      .thenError(folly::tag_t<std::exception>{}, [](const std::exception& e) {
        return folly::makeFuture<Status>(std::runtime_error(e.what()));
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

  HashSet uniqueDst;
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
  if (uniqueDst.size() == 0) {
    ectx_->setValue(terminateEarlyVar_, true);
    return Status::OK();
  }
  visitedVids.insert(std::make_move_iterator(uniqueDst.begin()),
                     std::make_move_iterator(uniqueDst.end()));
  return Status::OK();
}

folly::Future<Status> BFSShortestPathExecutor::conjunctPath() {
  const auto& leftEdges = allLeftEdges_.back();
  const auto& preRightEdges = allRightEdges_[step_ - 1];
  HashSet meetVids;
  bool oddStep = true;
  for (const auto& edge : leftEdges) {
    if (preRightEdges.find(edge.first) != preRightEdges.end()) {
      meetVids.emplace(edge.first);
    }
  }
  if (meetVids.empty() && step_ * 2 <= pathNode_->steps()) {
    const auto& rightEdges = allRightEdges_.back();
    for (const auto& edge : leftEdges) {
      if (rightEdges.find(edge.first) != rightEdges.end()) {
        meetVids.emplace(edge.first);
        oddStep = false;
      }
    }
  }
  if (meetVids.empty()) {
    return Status::OK();
  }
  size_t i = 0;
  size_t totalSize = meetVids.size();
  size_t batchSize = totalSize / static_cast<size_t>(FLAGS_num_operator_threads);
  std::vector<Value> batchVids;
  batchVids.reserve(batchSize);
  std::vector<folly::Future<DataSet>> futures;
  for (auto& vid : meetVids) {
    batchVids.push_back(vid);
    if (++i == totalSize || batchVids.size() == batchSize) {
      auto future = folly::via(runner(), [this, vids = std::move(batchVids), oddStep]() {
        memory::MemoryCheckGuard guard;
        return doConjunct(vids, oddStep);
      });
      futures.emplace_back(std::move(future));
    }
  }

  return folly::collectAll(futures).via(runner()).thenValue(
      [this](std::vector<folly::Try<DataSet>>&& resps) {
        memory::MemoryCheckGuard guard;
        for (auto& respVal : resps) {
          if (respVal.hasException()) {
            auto ex = respVal.exception().get_exception<std::bad_alloc>();
            if (ex) {
              throw std::bad_alloc();
            } else {
              throw std::runtime_error(respVal.exception().what().c_str());
            }
          }
          auto resp = std::move(respVal).value();
          currentDs_.append(std::move(resp));
        }
        return Status::OK();
      });
}

DataSet BFSShortestPathExecutor::doConjunct(const std::vector<Value>& meetVids,
                                            bool oddStep) const {
  DataSet ds;
  auto leftPaths = createPath(meetVids, false, oddStep);
  auto rightPaths = createPath(meetVids, true, oddStep);
  for (auto& leftPath : leftPaths) {
    auto range = rightPaths.equal_range(leftPath.first);
    for (auto& rightPath = range.first; rightPath != range.second; ++rightPath) {
      Path result = leftPath.second;
      result.reverse();
      result.append(rightPath->second);
      if (result.hasDuplicateEdges()) {
        continue;
      }
      Row row;
      row.emplace_back(std::move(result));
      ds.rows.emplace_back(std::move(row));
    }
  }
  return ds;
}

std::unordered_multimap<Value, Path> BFSShortestPathExecutor::createPath(
    std::vector<Value> meetVids, bool reverse, bool oddStep) const {
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
