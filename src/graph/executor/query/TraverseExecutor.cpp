// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/TraverseExecutor.h"

#include "clients/storage/StorageClient.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"
#include "graph/util/Utils.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;

namespace nebula {
namespace graph {

folly::Future<Status> TraverseExecutor::execute() {
  range_ = traverse_->stepRange();
  auto status = buildRequestVids();
  if (!status.ok()) {
    return error(std::move(status));
  }
  if (vids_.empty()) {
    DataSet emptyDs;
    return finish(ResultBuilder().value(Value(std::move(emptyDs))).build());
  }
  return getNeighbors();
}

Status TraverseExecutor::buildRequestVids() {
  SCOPED_TIMER(&execTime_);
  const auto& inputVar = traverse_->inputVar();
  auto inputIter = ectx_->getResult(inputVar).iterRef();
  auto iter = static_cast<SequentialIter*>(inputIter);
  size_t iterSize = iter->size();
  vids_.reserve(iterSize);
  initVids_.reserve(iterSize);
  auto* src = traverse_->src();
  QueryExpressionContext ctx(ectx_);

  bool mv = movable(traverse_->inputVars().front());
  if (traverse_->trackPrevPath()) {
    std::unordered_set<Value> uniqueVid;
    uniqueVid.reserve(iterSize);
    for (; iter->valid(); iter->next()) {
      const auto& vid = src->eval(ctx(iter));
      auto prevPath = mv ? iter->moveRow() : *iter->row();
      auto vidIter = dst2PathsMap_.find(vid);
      if (vidIter == dst2PathsMap_.end()) {
        std::vector<Row> tmp({std::move(prevPath)});
        dst2PathsMap_.emplace(vid, std::move(tmp));
      } else {
        vidIter->second.emplace_back(std::move(prevPath));
      }
      if (uniqueVid.emplace(vid).second) {
        vids_.emplace_back(vid);
        initVids_.emplace_back(vid);
      }
    }
  } else {
    initVids_.reserve(iterSize);
    const auto& spaceInfo = qctx()->rctx()->session()->space();
    const auto& metaVidType = *(spaceInfo.spaceDesc.vid_type_ref());
    auto vidType = SchemaUtil::propTypeToValueType(metaVidType.get_type());
    for (; iter->valid(); iter->next()) {
      const auto& vid = src->eval(ctx(iter));
      if (vid.type() != vidType) {
        LOG(ERROR) << "Mismatched vid type: " << vid.type() << ", space vid type: " << vidType;
        continue;
      }
      vids_.emplace_back(vid);
      initVids_.emplace_back(vid);
    }
  }
  return Status::OK();
}

folly::Future<Status> TraverseExecutor::getNeighbors() {
  currentStep_++;
  time::Duration getNbrTime;
  StorageClient* storageClient = qctx_->getStorageClient();
  bool finalStep = isFinalStep();
  StorageClient::CommonRequestParam param(traverse_->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  return storageClient
      ->getNeighbors(param,
                     {nebula::kVid},
                     std::move(vids_),
                     traverse_->edgeTypes(),
                     traverse_->edgeDirection(),
                     finalStep ? traverse_->statProps() : nullptr,
                     traverse_->vertexProps(),
                     traverse_->edgeProps(),
                     finalStep ? traverse_->exprs() : nullptr,
                     finalStep ? traverse_->dedup() : false,
                     finalStep ? traverse_->random() : false,
                     finalStep ? traverse_->orderBy() : std::vector<storage::cpp2::OrderBy>(),
                     finalStep ? traverse_->limit(qctx()) : -1,
                     selectFilter(),
                     currentStep_ == 1 ? traverse_->tagFilter() : nullptr)
      .via(runner())
      .thenValue([this, getNbrTime](StorageRpcResponse<GetNeighborsResponse>&& resp) mutable {
        vids_.clear();
        SCOPED_TIMER(&execTime_);
        addStats(resp, getNbrTime.elapsedInUSec());
        return handleResponse(std::move(resp));
      })
      .thenError(
          folly::tag_t<std::bad_alloc>{},
          [](const std::bad_alloc&) { return folly::makeFuture<Status>(memoryExceededStatus()); })
      .thenError(folly::tag_t<std::exception>{}, [](const std::exception& e) {
        return folly::makeFuture<Status>(std::runtime_error(e.what()));
      });
}

Expression* TraverseExecutor::selectFilter() {
  Expression* filter = nullptr;
  if (!(currentStep_ == 1 && traverse_->zeroStep())) {
    filter = const_cast<Expression*>(traverse_->filter());
  }
  if (currentStep_ == 1) {
    if (filter == nullptr) {
      filter = traverse_->firstStepFilter();
    } else if (traverse_->firstStepFilter() != nullptr) {
      filter = LogicalExpression::makeAnd(&objPool_, filter, traverse_->firstStepFilter());
    }
  }
  return filter;
}

void TraverseExecutor::addStats(RpcResponse& resp, int64_t getNbrTimeInUSec) {
  folly::dynamic stepInfo = folly::dynamic::array();
  auto& hostLatency = resp.hostLatency();
  for (size_t i = 0; i < hostLatency.size(); ++i) {
    size_t size = 0u;
    auto& result = resp.responses()[i];
    if (result.vertices_ref().has_value()) {
      size = (*result.vertices_ref()).size();
    }
    auto info = util::collectRespProfileData(result.result, hostLatency[i], size, getNbrTimeInUSec);
    stepInfo.push_back(std::move(info));
  }
  otherStats_.emplace(folly::sformat("step[{}]", currentStep_), folly::toPrettyJson(stepInfo));
}

folly::Future<Status> TraverseExecutor::handleResponse(RpcResponse&& resps) {
  auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
  if (!result.ok()) {
    return folly::makeFuture<Status>(std::move(result).status());
  }
  List list;
  for (auto& resp : resps.responses()) {
    auto dataset = resp.get_vertices();
    if (dataset == nullptr) {
      continue;
    }
    list.values.emplace_back(std::move(*dataset));
  }
  auto listVal = std::make_shared<Value>(std::move(list));
  auto iter = std::make_unique<GetNeighborsIter>(listVal);
  if (currentStep_ == 1) {
    if (range_ && range_->min() == 0) {
      result_.rows = buildZeroStepPath(iter.get());
    }
    // match (v)-[e:Rel]-(v1:Label1)-[e1*2]->() where id(v0) in [6, 23] return v1
    // the attributes of v1 will be obtained in the second traverse operator
    // If the conditions are not met, the path in the previous step needs to be filtered out
    if (!dst2PathsMap_.empty()) {
      std::unordered_set<Value, VertexHash, VertexEqual> existVids;
      existVids.reserve(iter->numRows());
      auto vertices = iter->getVertices();
      for (auto& vertex : vertices.values) {
        if (vertex.isVertex()) {
          existVids.emplace(vertex);
        }
      }
      auto dst2PathIter = dst2PathsMap_.begin();
      while (dst2PathIter != dst2PathsMap_.end()) {
        if (existVids.find(dst2PathIter->first) == existVids.end()) {
          dst2PathIter = dst2PathsMap_.erase(dst2PathIter);
        } else {
          dst2PathIter++;
        }
      }
      auto initVidIter = initVids_.begin();
      while (initVidIter != initVids_.end()) {
        if (existVids.find(*initVidIter) == existVids.end()) {
          initVidIter = initVids_.erase(initVidIter);
        } else {
          initVidIter++;
        }
      }
    }
  }
  expand(iter.get());
  if (!isFinalStep()) {
    if (vids_.empty()) {
      if (range_ != nullptr) {
        return buildResult();
      } else {
        return folly::makeFuture<Status>(Status::OK());
      }
    } else {
      return getNeighbors();
    }
  } else {
    return buildResult();
  }
}

void TraverseExecutor::expand(GetNeighborsIter* iter) {
  if (iter->numRows() == 0) {
    return;
  }
  auto* vFilter = traverse_->vFilter();
  auto* eFilter = traverse_->eFilter();
  QueryExpressionContext ctx(ectx_);

  std::unordered_set<Value> uniqueVids;
  Value curVertex;
  std::vector<Value> adjEdges;
  for (; iter->valid(); iter->next()) {
    if (vFilter != nullptr && currentStep_ == 1) {
      const auto& vFilterVal = vFilter->eval(ctx(iter));
      if (!vFilterVal.isBool() || !vFilterVal.getBool()) {
        continue;
      }
    }
    if (eFilter != nullptr) {
      const auto& eFilterVal = eFilter->eval(ctx(iter));
      if (!eFilterVal.isBool() || !eFilterVal.getBool()) {
        continue;
      }
    }
    const auto& edge = iter->getEdge();
    if (edge.empty()) {
      continue;
    }
    const auto& dst = edge.getEdge().dst;
    if (adjList_.find(dst) == adjList_.end() && uniqueVids.emplace(dst).second) {
      vids_.emplace_back(dst);
    }
    const auto& vertex = iter->getVertex();
    curVertex = curVertex.empty() ? vertex : curVertex;
    if (curVertex != vertex) {
      adjList_.emplace(curVertex, std::move(adjEdges));
      curVertex = vertex;
    }
    adjEdges.emplace_back(edge);
  }
  if (!curVertex.empty()) {
    adjList_.emplace(curVertex, std::move(adjEdges));
  }
}

std::vector<Row> TraverseExecutor::buildZeroStepPath(GetNeighborsIter* iter) {
  if (!iter || iter->numRows() == 0) {
    return std::vector<Row>();
  }
  std::vector<Row> result;
  result.reserve(iter->size());
  auto vertices = iter->getVertices();
  if (traverse_->trackPrevPath()) {
    for (auto& vertex : vertices.values) {
      auto dstIter = dst2PathsMap_.find(vertex);
      if (dstIter == dst2PathsMap_.end()) {
        continue;
      }
      auto& prevPaths = dstIter->second;
      for (auto& p : prevPaths) {
        Row row = p;
        List edgeList;
        edgeList.values.emplace_back(vertex);
        row.values.emplace_back(vertex);
        row.values.emplace_back(std::move(edgeList));
        result.emplace_back(std::move(row));
      }
    }
  } else {
    for (auto& vertex : vertices.values) {
      Row row;
      List edgeList;
      edgeList.values.emplace_back(vertex);
      row.values.emplace_back(vertex);
      row.values.emplace_back(std::move(edgeList));
      result.emplace_back(std::move(row));
    }
  }
  return result;
}

folly::Future<Status> TraverseExecutor::buildResult() {
  size_t minStep = 1;
  size_t maxStep = 1;
  if (range_ != nullptr) {
    minStep = range_->min();
    maxStep = range_->max();
  }

  result_.colNames = traverse_->colNames();
  if (maxStep == 0) {
    return finish(ResultBuilder().value(Value(std::move(result_))).build());
  }
  if (FLAGS_max_job_size <= 1) {
    for (const auto& vid : initVids_) {
      auto paths = buildPath(vid, minStep, maxStep);
      if (paths.empty()) {
        continue;
      }
      result_.rows.insert(result_.rows.end(),
                          std::make_move_iterator(paths.begin()),
                          std::make_move_iterator(paths.end()));
    }
    return finish(ResultBuilder().value(Value(std::move(result_))).build());
  }
  return buildPathMultiJobs(minStep, maxStep);
}

folly::Future<Status> TraverseExecutor::buildPathMultiJobs(size_t minStep, size_t maxStep) {
  DataSet vids;
  vids.rows.reserve(initVids_.size());
  for (auto& vid : initVids_) {
    Row row;
    row.values.emplace_back(std::move(vid));
    vids.rows.emplace_back(std::move(row));
  }
  auto val = std::make_shared<Value>(std::move(vids));
  auto iter = std::make_unique<SequentialIter>(val);

  auto scatter = [this, minStep, maxStep](
                     size_t begin, size_t end, Iterator* tmpIter) mutable -> std::vector<Row> {
    std::vector<Row> rows;
    for (; tmpIter->valid() && begin++ < end; tmpIter->next()) {
      auto& vid = tmpIter->getColumn(0);
      auto paths = buildPath(vid, minStep, maxStep);
      if (paths.empty()) {
        continue;
      }
      rows.insert(
          rows.end(), std::make_move_iterator(paths.begin()), std::make_move_iterator(paths.end()));
    }
    return rows;
  };

  auto gather = [this](std::vector<std::vector<Row>> resp) mutable -> Status {
    for (auto& rows : resp) {
      if (rows.empty()) {
        continue;
      }
      result_.rows.insert(result_.rows.end(),
                          std::make_move_iterator(rows.begin()),
                          std::make_move_iterator(rows.end()));
    }
    finish(ResultBuilder().value(Value(std::move(result_))).build());
    return Status::OK();
  };

  return runMultiJobs(std::move(scatter), std::move(gather), iter.get());
}

// build path based on BFS through adjancency list
std::vector<Row> TraverseExecutor::buildPath(const Value& vid, size_t minStep, size_t maxStep) {
  auto vidIter = adjList_.find(vid);
  if (vidIter == adjList_.end()) {
    return std::vector<Row>();
  }
  auto& src = vidIter->first;
  auto& adjEdges = vidIter->second;
  if (adjEdges.empty()) {
    return std::vector<Row>();
  }

  std::vector<Row> result;
  result.reserve(adjEdges.size());
  for (auto& edge : adjEdges) {
    List edgeList;
    edgeList.values.emplace_back(edge);
    Row row;
    row.values.emplace_back(src);
    row.values.emplace_back(std::move(edgeList));
    result.emplace_back(std::move(row));
  }

  if (maxStep == 1) {
    if (traverse_->trackPrevPath()) {
      std::vector<Row> newResult;
      auto dstIter = dst2PathsMap_.find(vid);
      if (dstIter == dst2PathsMap_.end()) {
        return std::vector<Row>();
      }
      auto& prevPaths = dstIter->second;
      for (auto& prevPath : prevPaths) {
        for (auto& p : result) {
          if (filterSameEdge(prevPath, p)) {
            continue;
          }
          // copy
          Row row = prevPath;
          row.values.emplace_back(p.values.front());
          row.values.emplace_back(p.values.back());
          newResult.emplace_back(std::move(row));
        }
      }
      return newResult;
    } else {
      return result;
    }
  }

  size_t step = 2;
  std::vector<Row> newResult;
  std::queue<std::vector<Value>*> queue;
  std::list<std::unique_ptr<std::vector<Value>>> holder;
  for (auto& edge : adjEdges) {
    auto ptr = std::make_unique<std::vector<Value>>(std::vector<Value>({edge}));
    queue.emplace(ptr.get());
    holder.emplace_back(std::move(ptr));
  }
  size_t adjSize = queue.size();
  while (!queue.empty()) {
    auto edgeListPtr = queue.front();
    auto& dst = edgeListPtr->back().getEdge().dst;
    queue.pop();
    --adjSize;
    auto dstIter = adjList_.find(dst);
    if (dstIter == adjList_.end()) {
      if (adjSize == 0) {
        if (++step > maxStep) {
          break;
        }
        adjSize = queue.size();
      }
      continue;
    }

    auto& adjedges = dstIter->second;
    for (auto& edge : adjedges) {
      if (hasSameEdge(*edgeListPtr, edge.getEdge())) {
        continue;
      }
      auto newEdgeListPtr = std::make_unique<std::vector<Value>>(*edgeListPtr);
      newEdgeListPtr->emplace_back(dstIter->first);
      newEdgeListPtr->emplace_back(edge);

      if (step >= minStep) {
        Row row;
        row.values.emplace_back(src);
        row.values.emplace_back(List(*newEdgeListPtr));
        newResult.emplace_back(std::move(row));
      }
      queue.emplace(newEdgeListPtr.get());
      holder.emplace_back(std::move(newEdgeListPtr));
    }
    if (adjSize == 0) {
      if (++step > maxStep) {
        break;
      }
      adjSize = queue.size();
    }
  }
  if (minStep <= 1) {
    newResult.insert(newResult.begin(),
                     std::make_move_iterator(result.begin()),
                     std::make_move_iterator(result.end()));
  }
  if (traverse_->trackPrevPath()) {
    std::vector<Row> newPaths;
    auto dstIter = dst2PathsMap_.find(vid);
    if (dstIter != dst2PathsMap_.end()) {
      auto& prevPaths = dstIter->second;
      for (auto& prevPath : prevPaths) {
        for (auto& p : newResult) {
          if (filterSameEdge(prevPath, p)) {
            continue;
          }
          // copy
          Row row = prevPath;
          row.values.emplace_back(p.values.front());
          row.values.emplace_back(p.values.back());
          newPaths.emplace_back(std::move(row));
        }
      }
      return newPaths;
    } else {
      return std::vector<Row>();
    }
  } else {
    return newResult;
  }
}

bool TraverseExecutor::hasSameEdge(const std::vector<Value>& edgeList, const Edge& edge) {
  for (auto& leftEdge : edgeList) {
    if (!leftEdge.isEdge()) {
      continue;
    }
    if (edge.keyEqual(leftEdge.getEdge())) {
      return true;
    }
  }
  return false;
}

bool TraverseExecutor::filterSameEdge(const Row& lhs,
                                      const Row& rhs,
                                      std::unordered_set<Value>* uniqueEdge) {
  if (uniqueEdge) {
    for (auto& rightListVal : rhs.values) {
      if (!rightListVal.isList()) {
        continue;
      }
      auto& rightList = rightListVal.getList().values;
      for (auto& edgeVal : rightList) {
        if (!edgeVal.isEdge()) {
          continue;
        }
        if (uniqueEdge->find(edgeVal) != uniqueEdge->end()) {
          return true;
        }
      }
    }
    return false;
  } else {
    for (auto& leftListVal : lhs.values) {
      if (!leftListVal.isList()) {
        continue;
      }
      auto& leftList = leftListVal.getList().values;
      for (auto& rightListVal : rhs.values) {
        if (!rightListVal.isList()) {
          continue;
        }
        auto& rightList = rightListVal.getList().values;
        for (auto& edgeVal : rightList) {
          if (!edgeVal.isEdge()) {
            continue;
          }
          if (hasSameEdge(leftList, edgeVal.getEdge())) {
            return true;
          }
        }
      }
    }
    return false;
  }
}

}  // namespace graph
}  // namespace nebula
