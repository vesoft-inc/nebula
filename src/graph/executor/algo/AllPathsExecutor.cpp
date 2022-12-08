// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#include "graph/executor/algo/AllPathsExecutor.h"

#include "graph/planner/plan/Algo.h"
#include "graph/service/GraphFlags.h"

DEFINE_uint32(path_threshold_size, 100, "");
DEFINE_uint32(path_threshold_ratio, 2, "");

namespace nebula {
namespace graph {
folly::Future<Status> AllPathsExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  pathNode_ = asNode<AllPaths>(node());
  noLoop_ = pathNode_->noLoop();
  steps_ = pathNode_->steps();
  withProp_ = pathNode_->withProp();
  result_.colNames = pathNode_->colNames();
  init();
  if (leftVids_.empty() || rightVids_.empty()) {
    return finish(ResultBuilder().value(Value(std::move(result_))).build());
  }
  return doAllPaths();
}

void AllPathsExecutor::init() {
  {
    auto iter = ectx_->getResult(pathNode_->leftInputVar()).iter();
    size_t size = iter->size();
    leftInitVids_.reserve(size);
    leftVids_.reserve(size);
    for (; iter->valid(); iter->next()) {
      auto& vid = iter->getColumn(0);
      if (vid.empty()) {
        continue;
      }
      if (leftInitVids_.emplace(vid).second) {
        leftVids_.emplace_back(vid);
      }
    }
  }
  {
    auto iter = ectx_->getResult(pathNode_->rightInputVar()).iter();
    size_t size = iter->size();
    rightInitVids_.reserve(size);
    rightVids_.reserve(size);
    for (; iter->valid(); iter->next()) {
      auto& vid = iter->getColumn(0);
      if (vid.empty()) {
        continue;
      }
      if (rightInitVids_.emplace(vid).second) {
        rightVids_.emplace_back(vid);
      }
    }
  }
}

AllPathsExecutor::Direction AllPathsExecutor::direction() {
  auto leftSize = leftVids_.size();
  auto rightSize = rightVids_.size();
  if (leftSteps_ + rightSteps_ + 1 == steps_) {
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
    for (auto& resp : resps) {
      if (!resp.ok()) {
        return folly::makeFuture<Status>(std::move(resp));
      }
    }
    if (leftSteps_ + rightSteps_ < steps_) {
      if (leftVids_.empty() || rightVids_.empty()) {
        return buildResult();
      } else {
        return doAllPaths();
      }
    } else {
      return buildResult();
    }
  });
}

folly::Future<Status> AllPathsExecutor::getNeighbors(bool reverse) {
  StorageClient* storageClient = qctx_->getStorageClient();
  time::Duration getNbrTime;
  storage::StorageClient::CommonRequestParam param(qctx_->spaceId(),
                                                   qctx_->rctx()->session()->id(),
                                                   qctx_->plan()->id(),
                                                   qctx_->plan()->isProfileEnabled());
  auto& vids = reverse ? rightVids_ : leftVids_;
  return storageClient
      ->getNeighbors(param,
                     {nebula::kVid},
                     std::move(vids),
                     {},
                     pathNode_->edgeDirection(),
                     nullptr,
                     pathNode_->vertexProps(),
                     reverse ? pathNode_->reverseEdgeProps() : pathNode_->edgeProps(),
                     nullptr,
                     false,
                     false,
                     {},
                     -1,       //  (TODO jmq)limit
                     nullptr,  //  (TODO jmq)filter
                     nullptr)
      .via(runner())
      .thenValue([this, getNbrTime, reverse](auto&& resps) {
        UNUSED(getNbrTime);
        // addStats(resp, getNbrTime.elapsedInUSec(), reverse);
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
        if (reverse) {
          rightVids_.clear();
          expandFromRight(iter.get());
        } else {
          leftVids_.clear();
          expandFromLeft(iter.get());
        }
        return Status::OK();
      });
}

void AllPathsExecutor::expandFromRight(GetNeighborsIter* iter) {
  if (iter->numRows() == 0) {
    return;
  }
  std::unordered_set<Value> uniqueVids;
  Value curVertex;
  for (; iter->valid(); iter->next()) {
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
        rightVids_.emplace_back(src);
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
        rightInitVids_.erase(vertex);
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
  std::unordered_set<Value> uniqueVids;
  Value curVertex;
  std::vector<Value> adjEdges;
  for (; iter->valid(); iter->next()) {
    const auto& edge = iter->getEdge();
    if (edge.empty()) {
      continue;
    }
    const auto& dst = edge.getEdge().dst;
    if (leftAdjList_.find(dst) == leftAdjList_.end() && uniqueVids.emplace(dst).second) {
      leftVids_.emplace_back(dst);
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

  buildPath();
  if (!withProp_ || emptyPropVids_.empty()) {
    return finish(ResultBuilder().value(Value(std::move(result_))).build());
  }

  auto future = getProps(emptyPropVids_, pathNode_->vertexProps());
  return future.via(runner()).thenValue([this](auto&& vertices) {
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

void AllPathsExecutor::buildPath() {
  for (const auto& vid : leftInitVids_) {
    auto paths = doBuildPath(vid);
    if (paths.empty()) {
      continue;
    }
    result_.rows.insert(result_.rows.end(),
                        std::make_move_iterator(paths.begin()),
                        std::make_move_iterator(paths.end()));
  }
}

std::vector<Row> AllPathsExecutor::doBuildPath(const Value& vid) {
  auto& adjList = leftAdjList_;
  auto vidIter = adjList.find(vid);
  if (vidIter == adjList.end()) {
    return std::vector<Row>();
  }
  auto& src = vidIter->first;
  auto& adjEdges = vidIter->second;
  if (adjEdges.empty()) {
    return std::vector<Row>();
  }

  std::vector<Row> result;
  result.reserve(adjEdges.size());

  std::queue<std::vector<Value>*> queue;
  std::list<std::unique_ptr<std::vector<Value>>> holder;
  for (auto& edge : adjEdges) {
    auto ptr = std::make_unique<std::vector<Value>>(std::vector<Value>({edge}));
    queue.emplace(ptr.get());
    holder.emplace_back(std::move(ptr));
  }

  size_t step = 1;
  size_t adjSize = queue.size();
  while (!queue.empty()) {
    auto edgeListPtr = queue.front();
    queue.pop();
    --adjSize;

    auto& dst = edgeListPtr->back().getEdge().dst;
    auto dstIter = rightInitVids_.find(dst);
    if (dstIter != rightInitVids_.end()) {
      Row row;
      row.values.emplace_back(src);
      row.values.emplace_back(List(*edgeListPtr));
      row.values.emplace_back(*dstIter);
      result.emplace_back(std::move(row));
    }

    auto adjIter = adjList.find(dst);
    if (adjIter == adjList.end()) {
      if (adjSize == 0) {
        if (++step > steps_) {
          break;
        }
        adjSize = queue.size();
      }
      continue;
    }

    auto& adjedges = adjIter->second;
    for (auto& edge : adjedges) {
      if (hasSameEdge(*edgeListPtr, edge.getEdge())) {
        continue;
      }
      auto newEdgeListPtr = std::make_unique<std::vector<Value>>(*edgeListPtr);
      newEdgeListPtr->emplace_back(adjIter->first);
      newEdgeListPtr->emplace_back(edge);
      queue.emplace(newEdgeListPtr.get());
      holder.emplace_back(std::move(newEdgeListPtr));
    }
    if (adjSize == 0) {
      if (++step > steps_) {
        break;
      }
      adjSize = queue.size();
    }
  }
  return result;
}

}  // namespace graph
}  // namespace nebula
