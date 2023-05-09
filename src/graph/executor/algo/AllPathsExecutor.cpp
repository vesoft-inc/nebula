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
        expand(iter.get(), reverse);
        otherStats_.emplace(key, folly::sformat("{}(us)", buildAdjTime.elapsedInUSec()));
        return Status::OK();
      });
}

void AllPathsExecutor::expand(GetNeighborsIter* iter, bool reverse) {
  if (iter->numRows() == 0) {
    return;
  }
  auto& adjList = reverse ? rightAdjList_ : leftAdjList_;
  auto& nextStepVids = reverse ? rightNextStepVids_ : leftNextStepVids_;
  nextStepVids.clear();
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
    if (adjList.find(dst) == adjList.end() && uniqueVids.emplace(dst).second) {
      nextStepVids.emplace_back(dst);
    }
    const auto& vertex = iter->getVertex();
    curVertex = curVertex.empty() ? vertex : curVertex;
    if (curVertex != vertex) {
      adjList.emplace(curVertex, std::move(adjEdges));
      curVertex = vertex;
    }
    adjEdges.emplace_back(edge);
  }
  if (!curVertex.empty()) {
    adjList.emplace(curVertex, std::move(adjEdges));
  }
}

folly::Future<Status> AllPathsExecutor::buildResult() {
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
  std::vector<folly::Future<std::vector<NPath*>>> futures;
  futures.emplace_back(
      folly::via(runner(), [this]() { return doBuildPath(1, 0, 0, nullptr, false); }));
  futures.emplace_back(
      folly::via(runner(), [this]() { return doBuildPath(1, 0, 0, nullptr, true); }));

  time::Duration conjunctPathTime;
  return folly::collect(futures)
      .via(runner())
      .thenValue([this](std::vector<std::vector<NPath*>>&& paths) {
        memory::MemoryCheckGuard guard;
        auto& leftPaths = paths.front();
        auto& rightPaths = paths.back();

        if (leftSteps_ == 0) {
          // todo
          buildOneWayPath(rightPaths, false);
          return folly::makeFuture<Status>(Status::OK());
        }
        if (rightSteps_ == 0) {
          buildOneWayPath(leftPaths, true);
          return folly::makeFuture<Status>(Status::OK());
        }

        return conjunctPath(leftPaths, rightPaths);
      })
      .thenValue([this, conjunctPathTime](auto&& resps) {
        NG_RETURN_IF_ERROR(resps);
        otherStats_.emplace("conjunct_path_time",
                            folly::sformat("{}(us)", conjunctPathTime.elapsedInUSec()));
        return Status::OK();
      });
}

folly::Future<std::vector<AllPathsExecutor::NPath*>> AllPathsExecutor::doBuildPath(
    size_t step,
    size_t start,
    size_t end,
    std::shared_ptr<std::vector<NPath*>> pathsPtr,
    bool reverse) {
  auto maxStep = reverse ? rightSteps_ : leftSteps_;
  if (step > maxStep) {
    return folly::makeFuture<std::vector<NPath*>>(std::vector<NPath*>());
  }
  if (threadLocalPtr_.get() == nullptr) {
    threadLocalPtr_.reset(new std::deque<NPath>());
  }
  auto& adjList = reverse ? rightAdjList_ : leftAdjList_;
  auto newPathsPtr = std::make_shared<std::vector<NPath*>>();

  if (step == 1) {
    auto& initVids = reverse ? rightInitVids_ : leftInitVids_;
    for (auto& vid : initVids) {
      auto vidIter = adjList.find(vid);
      if (vidIter == adjList.end()) {
        continue;
      }
      auto& src = vidIter->first;
      auto& adjEdges = vidIter->second;
      if (adjEdges.empty()) {
        continue;
      }
      for (auto& edge : adjEdges) {
        threadLocalPtr_->emplace_back(NPath(src, edge));
        newPathsPtr->emplace_back(&threadLocalPtr_->back());
      }
    }
  } else {
    for (auto i = start; i < end; ++i) {
      auto path = (*pathsPtr)[i];
      auto& edgeValue = path->edge;
      DCHECK(edgeValue.isEdge());
      auto& dst = edgeValue.getEdge().dst;
      auto adjIter = adjList.find(dst);
      if (adjIter == adjList.end()) {
        continue;
      }
      auto& adjEdges = adjIter->second;
      for (auto& edge : adjEdges) {
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
  if (newPathsSize == 0) {
    return folly::makeFuture<std::vector<NPath*>>(std::vector<NPath*>());
  }
  std::vector<folly::Future<std::vector<NPath*>>> futures;
  if (newPathsSize < FLAGS_path_batch_size) {
    futures.emplace_back(folly::via(runner(), [this, step, newPathsSize, newPathsPtr, reverse]() {
      return doBuildPath(step + 1, 0, newPathsSize, newPathsPtr, reverse);
    }));
  } else {
    for (size_t _start = 0; _start < newPathsSize; _start += FLAGS_path_batch_size) {
      auto tmp = _start + FLAGS_path_batch_size;
      auto _end = tmp > newPathsSize ? newPathsSize : tmp;
      futures.emplace_back(folly::via(runner(), [this, step, _start, _end, newPathsPtr, reverse]() {
        return doBuildPath(step + 1, _start, _end, newPathsPtr, reverse);
      }));
    }
  }
  return folly::collect(futures).via(runner()).thenValue(
      [currentStepResult = newPathsPtr](std::vector<std::vector<NPath*>>&& paths) {
        memory::MemoryCheckGuard guard;
        std::vector<NPath*> result = std::move(*currentStepResult);
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

void printHashTable(std::unordered_map<Value, std::vector<std::vector<Value>>> table) {
  for (auto& pair : table) {
    LOG(ERROR) << "key - " << pair.first.toString();
    for (auto& p : pair.second) {
      Row row(p);
      LOG(ERROR) << "values - " << row.toString();
    }
  }
}

std::vector<Value> AllPathsExecutor::convertNPath2List(NPath* path, bool reverse) {
  std::vector<Value> list;
  NPath* head = path;
  while (head != nullptr) {
    list.emplace_back(head->edge);
    list.emplace_back(head->vertex);
    head = head->p;
  }
  if (!reverse) {
    std::reverse(list.begin(), list.end());
  }
  return list;
}

void AllPathsExecutor::buildOneWayPath(std::vector<NPath*>& paths, bool reverse) {
  auto& initVids = reverse ? rightInitVids_ : leftInitVids_;
  for (auto& path : paths) {
    auto& edgeVal = path->edge;
    auto& dst = edgeVal.getEdge().dst;
    auto findDst = initVids.find(dst);
    if (findDst == initVids.end()) {
      continue;
    }
    ++cnt_;
    if (cnt_.load(std::memory_order_relaxed) > limit_) {
      break;
    }
    auto valueList = convertNPath2List(path, !reverse);
    Row row;
    Value emptyPropVertex(Vertex(dst, {}));
    if (!reverse) {
      auto dstVertex = valueList.back();
      valueList.pop_back();
      // add src
      row.values.emplace_back(emptyPropVertex);
      List edgeList(std::move(valueList));
      // add edgeList
      row.values.emplace_back(std::move(edgeList));
      // add dst
      row.values.emplace_back(dstVertex);
    } else {
      // add src
      row.values.emplace_back(valueList.front());
      std::vector<Value> tmp(std::make_move_iterator(valueList.begin() + 1),
                             std::make_move_iterator(valueList.end()));
      List edgeList(std::move(tmp));
      // add edgeList
      row.values.emplace_back(std::move(edgeList));
      // add dst
      row.values.emplace_back(emptyPropVertex);
    }
    if (emptyPropVertices_.emplace(emptyPropVertex).second) {
      emptyPropVids_.emplace_back(dst);
    }
    result_.rows.emplace_back(std::move(row));
    LOG(ERROR) << "one way --- " << result_.toString();
  }
}

std::vector<Row> AllPathsExecutor::buildOneWayPathFromHashTable(bool reverse) {
  auto& initVids = reverse ? rightInitVids_ : leftInitVids_;
  std::vector<Row> result;
  for (auto& vid : initVids) {
    auto findVid = hashTable_.find(vid);
    if (findVid == hashTable_.end()) {
      continue;
    }
    auto& paths = findVid->second;
    Value emptyPropVertex(Vertex(vid, {}));
    if (!reverse) {
      for (auto& path : paths) {
        ++cnt_;
        if (cnt_.load(std::memory_order_relaxed) > limit_) {
          break;
        }
        Row row;
        row.values.emplace_back(emptyPropVertex);
        auto& dstVertex = path.back();
        std::vector<Value> tmp(path.begin(), path.end() - 1);
        List edgeList(std::move(tmp));
        row.values.emplace_back(std::move(edgeList));
        row.values.emplace_back(dstVertex);
        LOG(ERROR) << "one way " << row.toString();
        result.emplace_back(std::move(row));
      }
    } else {
      for (auto& path : paths) {
        ++cnt_;
        if (cnt_.load(std::memory_order_relaxed) > limit_) {
          break;
        }
        Row row;
        row.values.emplace_back(path.front());
        std::vector<Value> tmp(path.begin() + 1, path.end());
        List edgeList(std::move(tmp));
        row.values.emplace_back(std::move(edgeList));
        row.values.emplace_back(emptyPropVertex);
        LOG(ERROR) << "one way " << row.toString();
        result.emplace_back(std::move(row));
      }
    }
    if (emptyPropVertices_.emplace(emptyPropVertex).second) {
      emptyPropVids_.emplace_back(vid);
    }
  }
  return result;
}

folly::Future<Status> AllPathsExecutor::conjunctPath(std::vector<NPath*>& leftPaths,
                                                     std::vector<NPath*>& rightPaths) {
  if (leftPaths.empty() || rightPaths.empty()) {
    return folly::makeFuture<Status>(Status::OK()); 
  }
  bool reverse = false;
  if (leftPaths.size() < rightPaths.size()) {
    buildHashTable(leftPaths, reverse);
    printHashTable(hashTable_);
    probePaths_ = std::move(rightPaths);
  } else {
    reverse = true;
    buildHashTable(rightPaths, reverse);
    printHashTable(hashTable_);
    probePaths_ = std::move(leftPaths);
  }
  auto oneWayPath = buildOneWayPathFromHashTable(!reverse);
  size_t probeSize = probePaths_.size();
  std::vector<folly::Future<std::vector<Row>>> futures;
  if (probeSize < FLAGS_path_batch_size) {
    futures.emplace_back(folly::via(
        runner(), [this, probeSize, reverse]() { return probe(0, probeSize, reverse); }));
  } else {
    for (size_t start = 0; start < probeSize; start += FLAGS_path_batch_size) {
      auto tmp = start + FLAGS_path_batch_size;
      auto end = tmp > probeSize ? probeSize : tmp;
      futures.emplace_back(folly::via(
          runner(), [this, start, end, reverse]() { return probe(start, end, reverse); }));
    }
  }
  return folly::collect(futures).via(runner()).thenValue(
      [this, path = std::move(oneWayPath)](std::vector<std::vector<Row>>&& resps) {
        memory::MemoryCheckGuard guard;
        result_.rows = std::move(path);
        LOG(ERROR) << "result is " << result_.toString();
        for (auto& rows : resps) {
          if (rows.empty()) {
            continue;
          }
          auto& emptyPropVerticesRow = rows.back();
          for (auto& emptyPropVertex : emptyPropVerticesRow.values) {
            if (emptyPropVertices_.emplace(emptyPropVertex).second) {
              emptyPropVids_.emplace_back(emptyPropVertex.getVertex().vid);
            }
          }
          rows.pop_back();
          result_.rows.insert(result_.rows.end(),
                              std::make_move_iterator(rows.begin()),
                              std::make_move_iterator(rows.end()));
        }
        if (result_.rows.size() > limit_) {
          result_.rows.resize(limit_);
        }
        LOG(ERROR) << "sdf " << result_.toString();
        return Status::OK();
      });
}

void AllPathsExecutor::buildHashTable(std::vector<NPath*>& paths, bool reverse) {
  for (auto& path : paths) {
    auto& edgeVal = path->edge;
    const auto& edge = edgeVal.getEdge();
    auto findDst = hashTable_.find(edge.dst);
    if (findDst == hashTable_.end()) {
      std::vector<std::vector<Value>> tmp({convertNPath2List(path, reverse)});
      hashTable_.emplace(edge.dst, std::move(tmp));
    } else {
      findDst->second.emplace_back(convertNPath2List(path, reverse));
    }
  }
}

void AllPathsExecutor::printNPath(std::vector<NPath*> paths, bool reverse) {
  for (auto path : paths) {
    Row row(convertNPath2List(path, reverse));
    LOG(ERROR) << reverse << " NPath " << row.toString();
  }
}

std::vector<Row> AllPathsExecutor::probe(size_t start, size_t end, bool reverse) {
  auto buildPath = [](std::vector<Value>& leftPath,
                      const Value& intersectVertex,
                      std::vector<Value>& rightPath) {
    Row row;
    row.values.emplace_back(leftPath.front());
    std::vector<Value> edgeList(leftPath.begin() + 1, leftPath.end());
    edgeList.emplace_back(intersectVertex);
    edgeList.insert(edgeList.end(), rightPath.begin(), rightPath.end() - 1);
    row.values.emplace_back(List(std::move(edgeList)));
    row.values.emplace_back(rightPath.back());
    LOG(ERROR) << "build Path " << row.toString();
    return row;
  };
  printHashTable(hashTable_);
  printNPath(probePaths_, !reverse);

  size_t minLength = reverse ? rightSteps_ * 2 : leftSteps_ * 2;
  std::vector<Row> result;
  Row emptyPropVerticesRow;
  for (size_t i = start; i < end; ++i) {
    auto& probePath = probePaths_[i];
    auto& edgeVal = probePath->edge;
    const auto& dst = edgeVal.getEdge().dst;
    Value intersectVertex(Vertex(dst, {}));
    auto findDst = hashTable_.find(dst);
    if (findDst == hashTable_.end()) {
      continue;
    }
    auto valueList = convertNPath2List(probePath, !reverse);

    for (auto& path : findDst->second) {
      if (path.size() != minLength) {
        continue;
      }
      auto& leftPath = reverse ? valueList : path;
      auto& rightPath = reverse ? path : valueList;
      if (noLoop_) {
        if (hasSameVertices(leftPath, dst, rightPath)) {
          continue;
        }
      } else {
        if (hasSameEdge(leftPath, rightPath)) {
          continue;
        }
      }
      ++cnt_;
      if (cnt_.load(std::memory_order_relaxed) > limit_) {
        break;
      }
      result.emplace_back(buildPath(leftPath, intersectVertex, rightPath));
    }
    emptyPropVerticesRow.values.emplace_back(intersectVertex);
  }
  result.emplace_back(emptyPropVerticesRow);
  return result;
}

folly::Future<Status> AllPathsExecutor::getPathProps() {
  auto future = getProps(emptyPropVids_, pathNode_->vertexProps());
  return future.via(runner()).thenValue([this](std::vector<Value>&& vertices) {
    memory::MemoryCheckGuard guard;
    for (auto& vertex : vertices) {
      if (vertex.empty()) {
        continue;
      }
      auto iter = emptyPropVertices_.find(vertex);
      if (iter != emptyPropVertices_.end()) {
        auto val = *iter;
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

bool AllPathsExecutor::hasSameEdge(std::vector<Value>& leftPaths, std::vector<Value>& rightPaths) {
  for (auto& leftEdge : leftPaths) {
    if (!leftEdge.isEdge()) {
      continue;
    }
    for (auto& rightEdge : rightPaths) {
      if (rightEdge.isEdge() && rightEdge.getEdge().keyEqual(leftEdge.getEdge())) {
        return true;
      }
    }
  }
  return false;
}

bool AllPathsExecutor::hasSameVertices(const std::vector<Value>& leftPaths,
                                       const Value& intersectVertex,
                                       const std::vector<Value>& rightPaths) {
  bool flag = leftPaths.size() > rightPaths.size();
  const auto& hashPath = flag ? rightPaths : leftPaths;
  const auto& probePath = flag ? leftPaths : rightPaths;
  VidHashSet hashTable;
  for (const auto& v : hashPath) {
    if (v.isVertex()) {
      hashTable.emplace(v);
    }
  }
  hashTable.emplace(intersectVertex);
  for (const auto& v : probePath) {
    if (v.isVertex()) {
      if (hashTable.find(v) != hashTable.end()) {
        return true;
      }
    }
  }
  return false;
}

}  // namespace graph
}  // namespace nebula
