/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/algo/FindPathExecutor.h"
#include "graph/planner/plan/Algo.h"

namespace nebula {
namespace graph {
void FindPathExecutor::shortestPathInit() {
  auto* path = asNode<FindPath>(node());
  auto leftIter = ectx_->getVersionedResult(path->leftInputVar(), -1).iter();
  auto rightIter = ectx_->getVersionedResult(path->rightInputVar(), -1).iter();
  for (; leftIter->valid(); leftIter->next()) {
    const auto& leftVal = leftIter->getColumn(0);
    for (rightIter->reset(); rightIter->valid(); rightIter->next()) {
      const auto& rightVal = rightIter->getColumn(0);
      terminationMap_.insert({leftVal, rightVal});
    }
  }
  for (rightIter->reset(); rightIter->valid(); rightIter->next()) {
    auto& dst = rightIter->getColumn(0);
    std::vector<Path> temp({Path(Vertex(dst, {}), {})});
    prePaths_[dst].emplace(dst, std::move(temp));
  }
}

std::vector<Path> FindPathExecutor::createPaths(const std::vector<const Path*>& paths,
                                                const Edge& edge) {
  std::vector<Path> newPaths;
  newPaths.reserve(paths.size());
  for (const auto& p : paths) {
    Path path = *p;
    path.steps.emplace_back(Step(Vertex(edge.dst, {}), edge.type, edge.name, edge.ranking, {}));
    newPaths.emplace_back(std::move(path));
  }
  return newPaths;
}

void FindPathExecutor::doShortestPath(Iterator* iter, CurrentPath& currentPaths, bool reverse) {
  const auto& historyPath = reverse ? historyLeftPaths_ : historyRightPaths_;
  for (; iter->valid(); iter->next()) {
    auto edgeVal = iter->getEdge();
    if (UNLIKELY(!edgeVal.isEdge())) {
      continue;
    }
    auto& edge = edgeVal.getEdge();
    auto& src = edge.src;
    auto& dst = edge.dst;

    auto findHistorySrc = historyPath.find(src);
    if (findHistorySrc == historyPath.end()) {
      // src must be startVid
      Path path;
      path.src = Vertex(src, {});
      path.steps.emplace_back(Step(Vertex(dst, {}), edge.type, edge.name, edge.ranking, {}));
      if (currentPaths.find(dst) != currentPaths.end()) {
        if (currentPaths.find(src) == currentPaths.end()) {
          std::vector<Path> temp({std::move(path)});
          currentPaths[dst].emplace(src, std::move(temp));
        } else {
          // same <src, dst>, different edge type or rank
          currentPaths[dst][src].emplace_back(std::move(path));
        }
      } else {
        std::vector<Path> temp({std::move(path)});
        currentPaths[dst].emplace(src, std::move(temp));
      }
    } else {
      // not the first step
      auto& srcPaths = findHistorySrc->second;
      if (historyPath.find(dst) == historyPath.end()) {
        // dst not in history
        if (currentPaths.find(dst) == currentPaths.end()) {
          // new edge
          for (const auto& srcPath : srcPaths) {
            currentPaths[dst].emplace(srcPath.first, createPaths(srcPath.second, edge));
          }
        } else {
          // dst not in history, dst in current
          for (const auto& srcPath : srcPaths) {
            auto newPaths = createPaths(srcPath.second, edge);
            if (currentPaths[dst].find(srcPath.first) != currentPaths[dst].end()) {
              currentPaths[dst][srcPath.first].insert(
                  currentPaths[dst][srcPath.first].end(), newPaths.begin(), newPaths.end());
            } else {
              currentPaths[dst].emplace(srcPath.first, std::move(newPaths));
            }
          }
        }
      } else {
        // dst in history
        continue;
      }
    }
  }
}

void FindPathExecutor::buildPath(std::vector<Path>& leftPaths,
                                 std::vector<Path>& rightPaths,
                                 DataSet& ds) {
  for (auto leftPath : leftPaths) {
    for (auto rightPath : rightPaths) {
      rightPath.reverse();
      leftPath.append(std::move(rightPath));
      Row row;
      row.values.emplace_back(std::move(leftPath));
      ds.rows.emplace_back(std::move(row));
    }
  }
}

bool FindPathExecutor::conjunctPath(CurrentPath& leftPaths, CurrentPath& rightPaths, DataSet& ds) {
  for (auto& leftPath : leftPaths) {
    auto find = rightPaths.find(leftPath.first);
    if (find == rightPaths.end()) {
      continue;
    }
    // find common vid
    for (auto& lPaths : leftPath.second) {
      const auto& srcVid = lPaths.first;
      for (auto& rPaths : find->second) {
        const auto& dstVid = rPaths.first;
        auto range = terminationMap_.equal_range(srcVid);
        auto iter = range.first;
        while (iter != range.second) {
          if (iter->second == dstVid) {
            iter = terminationMap_.erase(iter);
          } else {
            ++iter;
          }
        }
        buildPath(lPaths.second, rPaths.second, ds);
      }
    }
  }
  if (terminationMap_.empty()) {
    ectx_->setValue(terminationVar_, true);
    return true;
  }
  return false;
}

void FindPathExecutor::setNextStepVidFromPath(CurrentPath& leftPaths, CurrentPath& rightPaths) {
  auto* findPath = asNode<FindPath>(node());
  auto leftVidVar = findPath->leftVidVar();
  auto rightVidVar = findPath->rightVidVar();
  DataSet leftDs, rightDs;
  for (const auto& path : leftPaths) {
    Row row;
    row.values.emplace_back(path.first);
    leftDs.rows.emplace_back(std::move(row));
  }
  ectx_->setResult(leftVidVar, ResultBuilder().value(std::move(leftDs)).build());
  for (const auto& path : rightPaths) {
    Row row;
    row.values.emplace_back(path.first);
    rightDs.rows.emplace_back(std::move(row));
  }
  ectx_->setResult(rightVidVar, ResultBuilder().value(std::move(rightDs)).build());
}

void FindPathExecutor::updateHistory(CurrentPath& paths, HistoryPath& historyPaths) {
  for (const auto& dstIter : paths) {
    auto& dstPaths = historyPaths[dstIter.first];
    for (const auto& srcIter : dstIter.second) {
      auto& srcPaths = dstPaths[srcIter.first];
      for (const auto& path : srcIter.second) {
        srcPaths.emplace_back(&path);
      }
    }
  }
}

void FindPathExecutor::shortestPath(Iterator* leftIter, Iterator* rightIter, DataSet& ds) {
  CurrentPath leftPaths, rightPaths;
  doShortestPath(leftIter, leftPaths, false);

  if (conjunctPath(leftPaths, prePaths_, ds)) {
    return;
  }
  if (step_ * 2 < steps_) {
    doShortestPath(rightIter, rightPaths, true);
    conjunctPath(leftPaths, rightPaths, ds);
  }

  setNextStepVidFromPath(leftPaths, rightPaths);
  // update history
  updateHistory(leftPaths, historyLeftPaths_);
  updateHistory(rightPaths, historyRightPaths_);
  prePaths_.swap(rightPaths);
}

void FindPathExecutor::updateAllHistory(CurrentAllPath& paths, HistoryAllPath& historyPaths) {
  UNUSED(paths);
  UNUSED(historyPaths);
  return;
}

void FindPathExecutor::doAllPath(Iterator* iter, CurrentAllPath& currentPath, bool reverse) {
  const auto& historyPath = reverse ? historyAllLeftPaths_ : historyAllRightPaths_;
  UNUSED(historyPath);
  UNUSED(iter);
  UNUSED(currentPath);
  return;
}

void FindPathExecutor::allPath(Iterator* leftIter, Iterator* rightIter, DataSet& ds) {
  UNUSED(ds);
  CurrentAllPath leftPaths, rightPaths;
  doAllPath(leftIter, leftPaths, false);
  // conjunctPath(leftPaths, preAllPaths_, ds);

  if (step_ * 2 < steps_) {
    doAllPath(rightIter, rightPaths, true);
    // conjunctPath(leftPaths, rightPaths, ds);
  }

  // setNextStepVidFromPath(leftPaths, rightPaths);
  // update history
  updateAllHistory(leftPaths, historyAllLeftPaths_);
  updateAllHistory(rightPaths, historyAllRightPaths_);
  preAllPaths_.swap(rightPaths);
}

folly::Future<Status> FindPathExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* findPath = asNode<FindPath>(node());
  steps_ = findPath->steps();
  noLoop_ = findPath->noLoop();
  terminationVar_ = findPath->terminationVar();
  auto leftIter = ectx_->getResult(findPath->leftInputVar()).iter();
  auto rightIter = ectx_->getResult(findPath->rightInputVar()).iter();
  VLOG(1) << "current: " << node()->outputVar();
  DCHECK(!!leftIter && !!rightIter);
  DataSet ds;
  ds.colNames = node()->colNames();
  if (findPath->isShortest()) {
    if (step_ == 0) {
      shortestPathInit();
    }
    shortestPath(leftIter.get(), rightIter.get(), ds);
  } else {
    allPath(leftIter.get(), rightIter.get(), ds);
  }
  step_++;
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}
}  // namespace graph
}  // namespace nebula
