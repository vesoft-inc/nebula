// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#include "graph/executor/algo/AllPathsExecutor.h"

#include "common/thread/GenericThreadPool.h"
#include "graph/planner/plan/Algo.h"
#include "graph/service/GraphFlags.h"

DEFINE_uint32(
    path_threshold_size,
    100,
    "the number of vids to expand, when this threshold is exceeded, use heuristic expansion");
DEFINE_uint32(path_threshold_ratio, 2, "threshold for heuristics expansion");
DEFINE_uint32(path_batch_size, 50000, "number of paths constructed by each thread");

namespace nebula {
namespace graph {
folly::Future<Status> AllPathsExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  pathNode_ = asNode<AllPaths>(node());
  noLoop_ = pathNode_->noLoop();
  maxStep_ = pathNode_->steps();
  withProp_ = pathNode_->withProp();
  if (pathNode_->limit() != -1) {
    limit_ = pathNode_->limit();
  }
  buildRequestVids(true);
  buildRequestVids(false);
  result_.colNames = pathNode_->colNames();
  if (maxStep_ == 0 || leftNextStepVids_.empty() || rightNextStepVids_.empty()) {
    return finish(ResultBuilder().value(Value(std::move(result_))).build());
  }
  return doAllPaths();
}

void AllPathsExecutor::buildRequestVids(bool reverse) {
  auto inputVar = reverse ? pathNode_->rightInputVar() : pathNode_->leftInputVar();
  auto& initVids = reverse ? rightInitVids_ : leftInitVids_;
  auto& nextStepVids = reverse ? rightNextStepVids_ : leftNextStepVids_;

  auto iter = ectx_->getResult(inputVar).iter();
  size_t size = iter->size();
  initVids.reserve(size);
  nextStepVids.reserve(size);
  for (; iter->valid(); iter->next()) {
    auto& vid = iter->getColumn(0);
    if (vid.empty()) {
      continue;
    }
    if (initVids.emplace(vid).second) {
      nextStepVids.emplace_back(vid);
    }
  }
}

AllPathsExecutor::Direction AllPathsExecutor::direction() {
  auto leftSize = leftNextStepVids_.size();
  auto rightSize = rightNextStepVids_.size();
  if (leftSteps_ + rightSteps_ + 1 == maxStep_) {
    if (leftSize > rightSize) {
      ++rightSteps_;
      return Direction::kRight;
    } else {
      ++leftSteps_;
      return Direction::kLeft;
    }
  }
  if (leftSize > FLAGS_path_threshold_size && rightSize > FLAGS_path_threshold_size) {
    if (leftSize > rightSize && leftSize / rightSize > FLAGS_path_threshold_ratio) {
      ++rightSteps_;
      return Direction::kRight;
    }
    if (rightSize > leftSize && rightSize / leftSize > FLAGS_path_threshold_ratio) {
      ++leftSteps_;
      return Direction::kLeft;
    }
  }
  ++leftSteps_;
  ++rightSteps_;
  return Direction::kBoth;
}

folly::Future<Status> AllPathsExecutor::doAllPaths() {
  std::vector<folly::Future<Status>> futures;
  switch (direction()) {
    case Direction::kRight: {
      futures.emplace_back(getNeighbors(true));
      break;
    }
    case Direction::kLeft: {
      futures.emplace_back(getNeighbors(false));
      break;
    }
    default: {
      futures.emplace_back(getNeighbors(true));
      futures.emplace_back(getNeighbors(false));
      break;
    }
  }
  return folly::collect(futures).via(runner()).thenValue([this](auto&& resps) {
    memory::MemoryCheckGuard guard;
    for (auto& resp : resps) {
      if (!resp.ok()) {
        return folly::makeFuture<Status>(std::move(resp));
      }
    }
    if (leftSteps_ + rightSteps_ >= maxStep_ || leftNextStepVids_.empty() ||
        rightNextStepVids_.empty()) {
      return buildResult();
    }
    return doAllPaths();
  });
}

folly::Future<Status> AllPathsExecutor::getNeighbors(bool reverse) {
  StorageClient* storageClient = qctx_->getStorageClient();
  storage::StorageClient::CommonRequestParam param(pathNode_->space(),
                                                   qctx_->rctx()->session()->id(),
                                                   qctx_->plan()->id(),
                                                   qctx_->plan()->isProfileEnabled());
  auto& vids = reverse ? rightNextStepVids_ : leftNextStepVids_;
  auto filter = pathNode_->filter() ? pathNode_->filter()->clone() : nullptr;
  return storageClient
      ->getNeighbors(param,
                     {nebula::kVid},
                     std::move(vids),
                     {},
                     storage::cpp2::EdgeDirection::OUT_EDGE,
                     nullptr,
                     pathNode_->vertexProps(),
                     reverse ? pathNode_->reverseEdgeProps() : pathNode_->edgeProps(),
                     nullptr,
                     false,
                     false,
                     {},
                     -1,
                     filter,
                     nullptr)
      .via(runner())
      .thenValue([this, reverse](auto&& resps) {
        memory::MemoryCheckGuard guard;
        auto step = reverse ? rightSteps_ : leftSteps_;
        addGetNeighborStats(resps, step, reverse);
        auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
        NG_RETURN_IF_ERROR(result);
        auto& responses = std::move(resps).responses();
        List list;
        for (auto& resp : responses) {
          auto dataset = resp.get_vertices();
          if (dataset == nullptr) {
            LOG(INFO) << "Empty dataset in response";
            continue;
          }
          list.values.emplace_back(std::move(*dataset));
        }
        auto listVal = std::make_shared<Value>(std::move(list));
        auto iter = std::make_unique<GetNeighborsIter>(listVal);
        time::Duration buildAdjTime;
        auto key = folly::sformat("buildAdjTime {}step[{}]", reverse ? "reverse " : "", step);
        if (reverse) {
          rightNextStepVids_.clear();
          expandFromRight(iter.get());
        } else {
          leftNextStepVids_.clear();
          expandFromLeft(iter.get());
        }
        otherStats_.emplace(key, folly::sformat("{}(us)", buildAdjTime.elapsedInUSec()));
        return Status::OK();
      });
}

void AllPathsExecutor::expandFromRight(GetNeighborsIter* iter) {
  if (iter->numRows() == 0) {
    return;
  }
  auto* stepFilter = pathNode_->stepFilter();
  QueryExpressionContext ctx(ectx_);

  std::unordered_set<Value> uniqueVids;
  Value curVertex;
  for (; iter->valid(); iter->next()) {
    if (stepFilter != nullptr) {
      const auto& stepFilterVal = stepFilter->eval(ctx(iter));
      if (!stepFilterVal.isBool() || !stepFilterVal.getBool()) {
        continue;
      }
    }
    const auto& edgeVal = iter->getEdge();
    if (edgeVal.empty()) {
      continue;
    }
    auto edge = edgeVal.getEdge();
    edge.reverse();
    const auto& src = edge.src;
    auto srcIter = rightAdjList_.find(src);
    if (srcIter == rightAdjList_.end()) {
      if (uniqueVids.emplace(src).second && rightInitVids_.find(src) == rightInitVids_.end()) {
        rightNextStepVids_.emplace_back(src);
      }
      std::vector<Value> adjEdges({edge});
      rightAdjList_.emplace(src, std::move(adjEdges));
    } else {
      srcIter->second.emplace_back(edge);
    }
    const auto& vertex = iter->getVertex();
    if (curVertex != vertex) {
      curVertex = vertex;
      if (rightSteps_ == 1) {
        // delete item equal to vertex.vid
        rightInitVids_.erase(vertex);
        // add vertex to table
        rightInitVids_.emplace(vertex);
      }
      auto dstIter = rightAdjList_.find(vertex);
      if (dstIter != rightAdjList_.end()) {
        rightAdjList_[vertex] = dstIter->second;
      }
    }
  }
}

void AllPathsExecutor::expandFromLeft(GetNeighborsIter* iter) {
  if (iter->numRows() == 0) {
    return;
  }
  auto* stepFilter = pathNode_->stepFilter();
  QueryExpressionContext ctx(ectx_);

  std::unordered_set<Value> uniqueVids;
  Value curVertex;
  std::vector<Value> adjEdges;
  for (; iter->valid(); iter->next()) {
    if (stepFilter != nullptr) {
      const auto& stepFilterVal = stepFilter->eval(ctx(iter));
      if (!stepFilterVal.isBool() || !stepFilterVal.getBool()) {
        continue;
      }
    }
    const auto& edge = iter->getEdge();
    if (edge.empty()) {
      continue;
    }
    const auto& dst = edge.getEdge().dst;
    if (leftAdjList_.find(dst) == leftAdjList_.end() && uniqueVids.emplace(dst).second) {
      leftNextStepVids_.emplace_back(dst);
    }
    const auto& vertex = iter->getVertex();
    curVertex = curVertex.empty() ? vertex : curVertex;
    if (curVertex != vertex) {
      leftAdjList_.emplace(curVertex, std::move(adjEdges));
      curVertex = vertex;
    }
    adjEdges.emplace_back(edge);
  }
  if (!curVertex.empty()) {
    leftAdjList_.emplace(curVertex, std::move(adjEdges));
  }
}

folly::Future<Status> AllPathsExecutor::buildResult() {
  // when the key in the right adjacency list does not exist in the left adjacency list
  // add key & values to the left adjacency list,
  // if key exists, discard the right adjacency's key & values
  // because the right adjacency list may have fewer edges
  //  a->c->o, a->b, c->f, f->o
  time::Duration mergeAdjTime;
  for (auto& rAdj : rightAdjList_) {
    auto& src = rAdj.first;
    auto iter = leftAdjList_.find(src);
    if (iter == leftAdjList_.end()) {
      if (!src.isVertex()) {
        Value val(Vertex(src, {}));
        leftAdjList_.emplace(val, std::move(rAdj.second));
        emptyPropVids_.emplace_back(src);
      } else {
        leftAdjList_.emplace(src, std::move(rAdj.second));
      }
    }
  }
  if (rightSteps_ == 0) {
    std::unordered_set<Value, VertexHash, VertexEqual> rightVids;
    rightVids.reserve(rightInitVids_.size());
    for (auto& vid : rightInitVids_) {
      Value val = Vertex(vid, {});
      rightVids.emplace(val);
      emptyPropVids_.emplace_back(vid);
    }
    rightInitVids_.swap(rightVids);
  }
  if (leftSteps_ == 0) {
    for (auto& vid : leftInitVids_) {
      auto iter = leftAdjList_.find(vid);
      if (iter != leftAdjList_.end()) {
        emptyPropVids_.emplace_back(vid);
      }
    }
  }
  otherStats_.emplace("merge_adj_time", folly::sformat("{}(us)", mergeAdjTime.elapsedInUSec()));
  time::Duration buildPathTime;
  auto future = buildPathMultiJobs();
  return future.via(runner())
      .ensure([this, buildPathTime]() {
        otherStats_.emplace("build_path_time",
                            folly::sformat("{}(us)", buildPathTime.elapsedInUSec()));
      })
      .thenValue([this](auto&& resp) {
        UNUSED(resp);
        if (!withProp_ || emptyPropVids_.empty()) {
          finish(ResultBuilder().value(Value(std::move(result_))).build());
          return folly::makeFuture<Status>(Status::OK());
        }
        return getPathProps();
      });
}

folly::Future<Status> AllPathsExecutor::buildPathMultiJobs() {
  auto pathsPtr = std::make_shared<std::vector<NPath*>>();
  if (threadLocalPtr_.get() == nullptr) {
    threadLocalPtr_.reset(new std::deque<NPath>());
  }
  for (auto& vid : leftInitVids_) {
    auto vidIter = leftAdjList_.find(vid);
    if (vidIter == leftAdjList_.end()) {
      continue;
    }
    auto& src = vidIter->first;
    auto& adjEdges = vidIter->second;
    if (adjEdges.empty()) {
      continue;
    }
    pathsPtr->reserve(adjEdges.size() + pathsPtr->size());
    for (auto& edge : adjEdges) {
      threadLocalPtr_->emplace_back(NPath(src, edge));
      pathsPtr->emplace_back(&threadLocalPtr_->back());
    }
  }
  size_t step = 2;
  auto future = doBuildPath(step, 0, pathsPtr->size(), pathsPtr);
  return future.via(runner()).thenValue([this](std::vector<std::pair<NPath*, Value>>&& paths) {
    memory::MemoryCheckGuard guard;

    if (!paths.empty()) {
      time::Duration convertPathTime;
      if (paths.size() > limit_) {
        paths.resize(limit_);
      }
      for (auto& path : paths) {
        result_.rows.emplace_back(convertNPath2Row(path.first, path.second));
      }
      otherStats_.emplace("convert_path_time",
                          folly::sformat("{}(us)", convertPathTime.elapsedInUSec()));
    }
    return Status::OK();
  });
}

// construct ROW[src1, [e1, v2, e2], v3]
Row AllPathsExecutor::convertNPath2Row(NPath* path, Value dst) {
  std::vector<Value> list;
  NPath* head = path;
  while (head != nullptr) {
    list.emplace_back(head->edge);
    list.emplace_back(head->vertex);
    head = head->p;
  }
  Row row;
  // add src;
  row.values.emplace_back(list.back());
  list.pop_back();
  std::reverse(list.begin(), list.end());
  List edgeList(std::move(list));
  row.values.emplace_back(std::move(edgeList));
  row.values.emplace_back(std::move(dst));
  return row;
}

folly::Future<std::vector<std::pair<AllPathsExecutor::NPath*, Value>>>
AllPathsExecutor::doBuildPath(size_t step,
                              size_t start,
                              size_t end,
                              std::shared_ptr<std::vector<NPath*>> pathsPtr) {
  if (cnt_.load(std::memory_order_relaxed) >= limit_) {
    return folly::makeFuture<std::vector<std::pair<NPath*, Value>>>(
        std::vector<std::pair<NPath*, Value>>());
  }
  if (threadLocalPtr_.get() == nullptr) {
    threadLocalPtr_.reset(new std::deque<NPath>());
  }
  auto& adjList = leftAdjList_;
  auto currentStepResult = std::make_unique<std::vector<std::pair<NPath*, Value>>>();
  auto newPathsPtr = std::make_shared<std::vector<NPath*>>();

  for (auto i = start; i < end; ++i) {
    auto path = (*pathsPtr)[i];
    auto& edgeValue = path->edge;
    DCHECK(edgeValue.isEdge());
    auto& dst = edgeValue.getEdge().dst;
    auto dstIter = rightInitVids_.find(dst);
    if (dstIter != rightInitVids_.end()) {
      currentStepResult->emplace_back(std::make_pair(path, *dstIter));
      ++cnt_;
      if (cnt_.load(std::memory_order_relaxed) >= limit_) {
        break;
      }
    }
    if (step <= maxStep_) {
      auto adjIter = adjList.find(dst);
      if (adjIter == adjList.end()) {
        continue;
      }

      auto& adjedges = adjIter->second;
      for (auto& edge : adjedges) {
        if (noLoop_) {
          if (hasSameVertices(path, edge.getEdge())) {
            continue;
          }
        } else {
          if (hasSameEdge(path, edge.getEdge())) {
            continue;
          }
        }
        threadLocalPtr_->emplace_back(NPath(path, adjIter->first, edge));
        newPathsPtr->emplace_back(&threadLocalPtr_->back());
      }
    }
  }

  auto newPathsSize = newPathsPtr->size();
  if (step > maxStep_ || newPathsSize == 0) {
    return folly::makeFuture<std::vector<std::pair<NPath*, Value>>>(std::move(*currentStepResult));
  }
  std::vector<folly::Future<std::vector<std::pair<NPath*, Value>>>> futures;
  if (newPathsSize < FLAGS_path_batch_size) {
    futures.emplace_back(folly::via(runner(), [this, step, newPathsSize, newPathsPtr]() {
      return doBuildPath(step + 1, 0, newPathsSize, newPathsPtr);
    }));
  } else {
    for (size_t _start = 0; _start < newPathsSize; _start += FLAGS_path_batch_size) {
      auto tmp = _start + FLAGS_path_batch_size;
      auto _end = tmp > newPathsSize ? newPathsSize : tmp;
      futures.emplace_back(folly::via(runner(), [this, step, _start, _end, newPathsPtr]() {
        return doBuildPath(step + 1, _start, _end, newPathsPtr);
      }));
    }
  }
  return folly::collect(futures).via(runner()).thenValue(
      [pathPtr = std::move(currentStepResult)](
          std::vector<std::vector<std::pair<NPath*, Value>>>&& paths) {
        memory::MemoryCheckGuard guard;
        std::vector<std::pair<NPath*, Value>> result = std::move(*pathPtr);
        for (auto& path : paths) {
          if (path.empty()) {
            continue;
          }
          result.insert(result.end(),
                        std::make_move_iterator(path.begin()),
                        std::make_move_iterator(path.end()));
        }
        return result;
      });
}

folly::Future<Status> AllPathsExecutor::getPathProps() {
  auto future = getProps(emptyPropVids_, pathNode_->vertexProps());
  return future.via(runner()).thenValue([this](std::vector<Value>&& vertices) {
    memory::MemoryCheckGuard guard;
    for (auto& vertex : vertices) {
      if (vertex.empty()) {
        continue;
      }
      auto iter = leftAdjList_.find(vertex);
      if (iter != leftAdjList_.end()) {
        auto val = iter->first;
        auto& mutableVertex = val.mutableVertex();
        mutableVertex.tags.swap(vertex.mutableVertex().tags);
      }
    }
    return finish(ResultBuilder().value(Value(std::move(result_))).build());
  });
}

bool AllPathsExecutor::hasSameEdge(NPath* path, const Edge& edge) {
  NPath* head = path;
  while (head != nullptr) {
    if (edge == head->edge) {
      return true;
    }
    head = head->p;
  }
  return false;
}

bool AllPathsExecutor::hasSameVertices(NPath* path, const Edge& edge) {
  if (edge.src == edge.dst) {
    return true;
  }
  auto& vid = edge.dst;
  NPath* head = path;
  while (head != nullptr) {
    auto& vertex = head->vertex;
    if (vertex.getVertex().vid == vid) {
      return true;
    }
    head = head->p;
  }
  return false;
}

}  // namespace graph
}  // namespace nebula
