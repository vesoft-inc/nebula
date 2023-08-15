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
  auto leftFuture = folly::via(runner(), [this]() {
    memory::MemoryCheckGuard guard;
    return buildPath(false);
  });
  auto rightFuture = folly::via(runner(), [this]() {
    memory::MemoryCheckGuard guard;
    return buildPath(true);
  });
  futures.emplace_back(std::move(leftFuture));
  futures.emplace_back(std::move(rightFuture));

  return folly::collectAll(futures)
      .via(runner())
      .thenValue([this](std::vector<folly::Try<Status>>&& resps) {
        // oddStep
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
        return conjunctPath(true);
      })
      .thenValue([this](auto&& termination) {
        memory::MemoryCheckGuard guard;
        // termination is true, all paths has found
        if (termination || step_ * 2 > pathNode_->steps()) {
          return folly::makeFuture<bool>(true);
        }
        // evenStep
        return conjunctPath(false);
      })
      .thenValue([this](auto&& resp) {
        memory::MemoryCheckGuard guard;
        UNUSED(resp);
        preRightPaths_ = rightPaths_;
        // update history
        for (auto& iter : leftPaths_) {
          historyLeftPaths_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                               std::make_move_iterator(iter.second.end()));
        }
        for (auto& iter : rightPaths_) {
          historyRightPaths_[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                                std::make_move_iterator(iter.second.end()));
        }
        leftPaths_.clear();
        rightPaths_.clear();

        step_++;
        DataSet ds;
        ds.colNames = pathNode_->colNames();
        ds.rows.swap(currentDs_.rows);
        return finish(ResultBuilder().value(Value(std::move(ds))).build());
      })
      .thenError(folly::tag_t<std::bad_alloc>{},
                 [](const std::bad_alloc&) {
                   return folly::makeFuture<Status>(Executor::memoryExceededStatus());
                 })
      .thenError(folly::tag_t<std::exception>{}, [](const std::exception& e) {
        return folly::makeFuture<Status>(std::runtime_error(e.what()));
      });
}

void MultiShortestPathExecutor::init() {
  auto lIter = ectx_->getResult(pathNode_->leftVidVar()).iter();
  auto rIter = ectx_->getResult(pathNode_->rightVidVar()).iter();
  std::set<Value> rightVids;
  for (; rIter->valid(); rIter->next()) {
    auto& vid = rIter->getColumn(0);
    if (rightVids.emplace(vid).second) {
      std::vector<Path> tmp({Path(Vertex(vid, {}), {})});
      historyRightPaths_[vid].emplace(vid, tmp);
      preRightPaths_[vid].emplace(vid, std::move(tmp));
    }
  }

  std::set<Value> leftVids;
  for (; lIter->valid(); lIter->next()) {
    auto& vid = lIter->getColumn(0);
    std::vector<Path> tmp({Path(Vertex(vid, {}), {})});
    historyLeftPaths_[vid].emplace(vid, std::move(tmp));
    leftVids.emplace(vid);
  }
  for (const auto& leftVid : leftVids) {
    for (const auto& rightVid : rightVids) {
      if (leftVid != rightVid) {
        terminationMap_.emplace(leftVid, std::make_pair(rightVid, true));
      }
    }
  }
}

std::vector<Path> MultiShortestPathExecutor::createPaths(const std::vector<Path>& paths,
                                                         const Edge& edge) {
  std::vector<Path> newPaths;
  newPaths.reserve(paths.size());
  for (const auto& p : paths) {
    Path path = p;
    path.steps.emplace_back(Step(Vertex(edge.dst, {}), edge.type, edge.name, edge.ranking, {}));
    newPaths.emplace_back(std::move(path));
  }
  return newPaths;
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
      auto foundDst = currentPaths.find(dst);
      if (foundDst != currentPaths.end()) {
        auto foundSrc = foundDst->second.find(src);
        if (foundSrc != foundDst->second.end()) {
          // same <src, dst>, different edge type or rank
          foundSrc->second.emplace_back(std::move(path));
        } else {
          std::vector<Path> tmp({std::move(path)});
          foundDst->second.emplace(src, std::move(tmp));
        }
      } else {
        std::vector<Path> tmp({std::move(path)});
        currentPaths[dst].emplace(src, std::move(tmp));
      }
    }
  } else {
    auto& historyPaths = reverse ? historyRightPaths_ : historyLeftPaths_;
    for (; iter->valid(); iter->next()) {
      auto edgeVal = iter->getEdge();
      if (UNLIKELY(!edgeVal.isEdge())) {
        continue;
      }
      auto& edge = edgeVal.getEdge();
      auto& src = edge.src;
      auto& dst = edge.dst;
      auto& prePaths = historyPaths[src];

      auto foundHistDst = historyPaths.find(dst);
      if (foundHistDst == historyPaths.end()) {
        // dst not in history
        auto foundDst = currentPaths.find(dst);
        if (foundDst == currentPaths.end()) {
          // dst not in current, new edge
          for (const auto& prePath : prePaths) {
            currentPaths[dst].emplace(prePath.first, createPaths(prePath.second, edge));
          }
        } else {
          // dst in current
          for (const auto& prePath : prePaths) {
            auto newPaths = createPaths(prePath.second, edge);
            auto foundSrc = foundDst->second.find(prePath.first);
            if (foundSrc == foundDst->second.end()) {
              foundDst->second.emplace(prePath.first, std::move(newPaths));
            } else {
              foundSrc->second.insert(foundSrc->second.begin(),
                                      std::make_move_iterator(newPaths.begin()),
                                      std::make_move_iterator(newPaths.end()));
            }
          }
        }
      } else {
        // dst in history
        auto& historyDstPaths = foundHistDst->second;
        for (const auto& prePath : prePaths) {
          if (historyDstPaths.find(prePath.first) != historyDstPaths.end()) {
            // loop: a->b->c->a or a->b->c->b,
            // filter out path that with duplicate vertex or have already been found before
            continue;
          }
          auto foundDst = currentPaths.find(dst);
          if (foundDst == currentPaths.end()) {
            currentPaths[dst].emplace(prePath.first, createPaths(prePath.second, edge));
          } else {
            auto newPaths = createPaths(prePath.second, edge);
            auto foundSrc = foundDst->second.find(prePath.first);
            if (foundSrc == foundDst->second.end()) {
              foundDst->second.emplace(prePath.first, std::move(newPaths));
            } else {
              foundSrc->second.insert(foundSrc->second.begin(),
                                      std::make_move_iterator(newPaths.begin()),
                                      std::make_move_iterator(newPaths.end()));
            }
          }
        }
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
  auto buildPaths =
      [](const std::vector<Path>& leftPaths, const std::vector<Path>& rightPaths, DataSet& ds) {
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
      };

  DataSet ds;
  for (const auto& iter : iters) {
    const auto& leftPaths = iter.first->second;
    const auto& rightPaths = iter.second->second;
    for (const auto& leftPath : leftPaths) {
      auto range = terminationMap_.equal_range(leftPath.first);
      for (const auto& rightPath : rightPaths) {
        for (auto found = range.first; found != range.second; ++found) {
          if (found->second.first == rightPath.first) {
            buildPaths(leftPath.second, rightPath.second, ds);
            found->second.second = false;
          }
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
        auto future = folly::via(runner(), [this, iters = std::move(pathIters)]() {
          memory::MemoryCheckGuard guard;
          return doConjunct(iters);
        });
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
        auto future = folly::via(runner(), [this, iters = std::move(pathIters)]() {
          memory::MemoryCheckGuard guard;
          return doConjunct(iters);
        });
        futures.emplace_back(std::move(future));
        pathIters.reserve(batchSize);
        i = 0;
      }
    }
  }
  if (i != 0) {
    auto future = folly::via(runner(), [this, iters = std::move(pathIters)]() {
      memory::MemoryCheckGuard guard;
      return doConjunct(iters);
    });
    futures.emplace_back(std::move(future));
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
