// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/algo/FindPathExecutor.h"
#include "graph/planner/plan/Algo.h"

namespace nebula {
namespace graph {

void FindPathExecutor::shortestPathInit() {
  auto* path = asNode<FindPath>(node());
  auto leftIter = ectx_->getResult(path->leftVidVar()).iter();
  auto rightIter = ectx_->getResult(path->rightVidVar()).iter();
  std::set<Value> leftArray, rightArray;
  for (; leftIter->valid(); leftIter->next()) {
    auto& value = leftIter->getColumn(0);
    if (value.isStr() || value.isInt()) {
      leftArray.emplace(value);
    }
  }
  for (; rightIter->valid(); rightIter->next()) {
    auto& value = rightIter->getColumn(0);
    if (value.isStr() || value.isInt()) {
      rightArray.emplace(value);
    }
  }
  for (const auto& leftVal : leftArray) {
    for (const auto& rightVal : rightArray) {
      if (leftVal != rightVal) {
        terminationMap_.insert({leftVal, rightVal});
      }
    }
  }
  for (const auto& rightVal : rightArray) {
    std::vector<Path> temp({Path(Vertex(rightVal, {}), {})});
    prePaths_[rightVal].emplace(rightVal, std::move(temp));
  }
}

void FindPathExecutor::allPathInit() {
  auto* path = asNode<FindPath>(node());
  auto iter = ectx_->getResult(path->rightVidVar()).iter();
  for (; iter->valid(); iter->next()) {
    const auto& vid = iter->getColumn(0);
    preRightPaths_[vid].push_back({Path(Vertex(vid, {}), {})});
  }
}

std::vector<Path> FindPathExecutor::createPaths(const std::vector<Path>& paths, const Edge& edge) {
  std::vector<Path> newPaths;
  newPaths.reserve(paths.size());
  for (auto path : paths) {
    path.steps.emplace_back(Step(Vertex(edge.dst, {}), edge.type, edge.name, edge.ranking, {}));
    newPaths.emplace_back(std::move(path));
  }
  return newPaths;
}

void FindPathExecutor::buildPath(std::vector<Path>& leftPaths,
                                 std::vector<Path>& rightPaths,
                                 DataSet& ds) {
  for (const auto& leftPath : leftPaths) {
    for (const auto& rightPath : rightPaths) {
      auto forwardPath = leftPath;
      auto backwardPath = rightPath;
      backwardPath.reverse();
      forwardPath.append(std::move(backwardPath));
      Row row;
      row.values.emplace_back(std::move(forwardPath));
      ds.rows.emplace_back(std::move(row));
    }
  }
  VLOG(1) << "Build Path is : " << ds;
}

void FindPathExecutor::setNextStepVidFromPath(SPInterimsMap& leftPaths, SPInterimsMap& rightPaths) {
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

void FindPathExecutor::setNextStepVidFromPath(Interims& leftPaths, Interims& rightPaths) {
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

bool FindPathExecutor::conjunctPath(SPInterimsMap& leftPaths,
                                    SPInterimsMap& rightPaths,
                                    DataSet& ds) {
  // key : srcVid, value : dstVid
  std::unordered_multimap<Value, Value> vidMaps;
  for (auto& leftPath : leftPaths) {
    auto find = rightPaths.find(leftPath.first);
    if (find == rightPaths.end()) {
      continue;
    }
    for (auto& lPaths : leftPath.second) {
      const auto& srcVid = lPaths.first;
      for (auto& rPaths : find->second) {
        const auto& dstVid = rPaths.first;
        auto range = terminationMap_.equal_range(srcVid);
        for (auto iter = range.first; iter != range.second; ++iter) {
          if (iter->second == dstVid) {
            VLOG(1) << "Find Common Vid : " << find->first;
            vidMaps.insert({srcVid, dstVid});
            buildPath(lPaths.second, rPaths.second, ds);
          }
        }
      }
    }
  }
  // update terminationMap_
  for (auto iter : vidMaps) {
    const auto& srcVid = iter.first;
    const auto& dstVid = iter.second;
    auto range = terminationMap_.equal_range(srcVid);
    auto dstIter = range.first;
    while (dstIter != range.second) {
      if (dstIter->second == dstVid) {
        dstIter = terminationMap_.erase(dstIter);
      } else {
        ++dstIter;
      }
    }
  }
  if (terminationMap_.empty()) {
    ectx_->setValue(terminationVar_, true);
    return true;
  }
  return false;
}

void FindPathExecutor::doShortestPath(Iterator* iter, SPInterimsMap& currentPaths, bool reverse) {
  auto& historyPath = reverse ? historyRightPaths_ : historyLeftPaths_;
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
      // first step
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
      std::vector<Path> start({Path(Vertex(src, {}), {})});
      historyPath[src].emplace(src, std::move(start));
    } else {
      auto& srcPaths = findHistorySrc->second;
      auto findHistoryDst = historyPath.find(dst);
      if (findHistoryDst == historyPath.end()) {
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
        auto& historyDstPaths = findHistoryDst->second;
        for (auto& srcPath : srcPaths) {
          if (historyDstPaths.find(srcPath.first) != historyDstPaths.end()) {
            // loop: a->b->c->a or a->b->c->b
            continue;
          }
          if (currentPaths.find(dst) == currentPaths.end()) {
            currentPaths[dst].emplace(srcPath.first, createPaths(srcPath.second, edge));
          } else {
            auto newPaths = createPaths(srcPath.second, edge);
            if (currentPaths[dst].find(srcPath.first) != currentPaths[dst].end()) {
              currentPaths[dst][srcPath.first].insert(
                  currentPaths[dst][srcPath.first].end(), newPaths.begin(), newPaths.end());
            } else {
              currentPaths[dst].emplace(srcPath.first, std::move(newPaths));
            }
          }
        }
      }
    }
  }
}

void FindPathExecutor::shortestPath(Iterator* leftIter, Iterator* rightIter, DataSet& ds) {
  SPInterimsMap leftPaths, rightPaths;
  doShortestPath(leftIter, leftPaths, false);

  if (conjunctPath(leftPaths, prePaths_, ds)) {
    return;
  }
  if (step_ * 2 <= steps_) {
    doShortestPath(rightIter, rightPaths, true);
    conjunctPath(leftPaths, rightPaths, ds);
  }

  setNextStepVidFromPath(leftPaths, rightPaths);
  prePaths_ = rightPaths;
  // update history
  for (auto iter : leftPaths) {
    historyLeftPaths_[iter.first].insert(make_move_iterator(iter.second.begin()),
                                         make_move_iterator(iter.second.end()));
  }
  for (auto iter : rightPaths) {
    historyRightPaths_[iter.first].insert(make_move_iterator(iter.second.begin()),
                                          make_move_iterator(iter.second.end()));
  }
}

void FindPathExecutor::doAllPath(Iterator* iter, Interims& currentPaths, bool reverse) {
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
      if (noDuplicateVid_ && path.hasDuplicateVertices()) {
        continue;
      }
      currentPaths[dst].emplace_back(std::move(path));
    }
  }
}

void FindPathExecutor::allPath(Iterator* leftIter, Iterator* rightIter, DataSet& ds) {
  Interims leftPaths, rightPaths;
  doAllPath(leftIter, leftPaths, false);

  if (conjunctPath(leftPaths, preRightPaths_, ds)) {
    return;
  }

  if (step_ * 2 <= steps_) {
    doAllPath(rightIter, rightPaths, true);
    conjunctPath(leftPaths, rightPaths, ds);
  }

  setNextStepVidFromPath(leftPaths, rightPaths);
  // update history
  preLeftPaths_.swap(leftPaths);
  preRightPaths_.swap(rightPaths);
}

folly::Future<Status> FindPathExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* findPath = asNode<FindPath>(node());
  steps_ = findPath->steps();
  noLoop_ = findPath->noLoop();
  shortest_ = findPath->isShortest();
  noDuplicateVid_ = noLoop_ || shortest_;
  terminationVar_ = findPath->terminationVar();
  auto leftIter = ectx_->getResult(findPath->leftInputVar()).iter();
  auto rightIter = ectx_->getResult(findPath->rightInputVar()).iter();
  DCHECK(!!leftIter && !!rightIter);
  DataSet ds;
  ds.colNames = node()->colNames();
  // if (findPath->isShortest()) {
  //   if (step_ == 1) {
  //     shortestPathInit();
  //   }
  //   shortestPath(leftIter.get(), rightIter.get(), ds);
  // } else {
  //   if (step_ == 1) {
  //     allPathInit();
  //   }
  //   allPath(leftIter.get(), rightIter.get(), ds);
  // }
  if (step_ == 1) {
    init();
  }
  allPath(leftIter.get(), rightIter.get(), ds);
  step_++;
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

void FindPathExecutor::printPath(SPInterimsMap& paths) {
  for (auto& dstPath : paths) {
    auto& dst = dstPath.first;
    for (auto& srcPath : dstPath.second) {
      auto& src = srcPath.first;
      for (auto& path : srcPath.second) {
        VLOG(1) << "Path is src: " << src << " dst: " << dst << " path: " << path;
      }
    }
  }
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
          if (noDuplicateVid_ && forwardPath.hasDuplicateVertices()) {
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

}  // namespace graph
}  // namespace nebula
