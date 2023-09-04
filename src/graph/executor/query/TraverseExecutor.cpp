// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/TraverseExecutor.h"

#include "clients/storage/StorageClient.h"
#include "common/memory/MemoryTracker.h"
#include "graph/context/iterator/GetNbrsRespDataSetIter.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"
#include "graph/util/Utils.h"

DEFINE_uint64(traverse_parallel_threshold_rows,
              150000,
              "threshold row number of traverse executor in parallel");

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;

namespace nebula {
namespace graph {

folly::Future<Status> TraverseExecutor::execute() {
  range_ = traverse_->stepRange();
  genPath_ = traverse_->genPath();
  NG_RETURN_IF_ERROR(buildRequestVids());
  if (vids_.empty()) {
    DataSet emptyDs;
    return finish(ResultBuilder().value(Value(std::move(emptyDs))).build());
  }
  return getNeighbors();
}

Status TraverseExecutor::buildRequestVids() {
  SCOPED_TIMER(&execTime_);
  const auto& inputVar = traverse_->inputVar();
  auto inputIter = ectx_->getResult(inputVar).iter();
  auto iter = static_cast<SequentialIter*>(inputIter.get());
  size_t iterSize = iter->size();
  vids_.reserve(iterSize);
  auto* src = traverse_->src();
  QueryExpressionContext ctx(ectx_);

  bool mv = movable(traverse_->inputVars().front());
  if (traverse_->trackPrevPath()) {
    for (; iter->valid(); iter->next()) {
      const auto& vid = src->eval(ctx(iter));
      auto prevPath = mv ? iter->moveRow() : *iter->row();
      auto vidIter = dst2PathsMap_.find(vid);
      if (vidIter == dst2PathsMap_.end()) {
        dst2PathsMap_.emplace(vid, std::vector<Row>{std::move(prevPath)});
      } else {
        vidIter->second.emplace_back(std::move(prevPath));
      }
      vids_.emplace(vid);
    }
  } else {
    const auto& spaceInfo = qctx()->rctx()->session()->space();
    const auto& metaVidType = *(spaceInfo.spaceDesc.vid_type_ref());
    auto vidType = SchemaUtil::propTypeToValueType(metaVidType.get_type());
    for (; iter->valid(); iter->next()) {
      const auto& vid = src->eval(ctx(iter));
      // FIXME(czp): Remove this DCHECK for now, we should check vid type at compile-time
      if (vid.type() != vidType) {
        return Status::Error("Vid type mismatched.");
      }
      // DCHECK_EQ(vid.type(), vidType)
      //     << "Mismatched vid type: " << vid.type() << ", space vid type: " << vidType;
      if (vid.type() == vidType) {
        vids_.emplace(vid);
      }
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
  std::vector<Value> vids(vids_.size());
  std::move(vids_.begin(), vids_.end(), vids.begin());
  return storageClient
      ->getNeighbors(param,
                     {nebula::kVid},
                     std::move(vids),
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
        // MemoryTrackerVerified
        memory::MemoryCheckGuard guard;
        vids_.clear();
        SCOPED_TIMER(&execTime_);
        addStats(resp, getNbrTime.elapsedInUSec());
        time::Duration expandTime;
        return handleResponse(std::move(resp)).ensure([this, expandTime]() {
          addState("expandTime", expandTime);
        });
      })
      .thenValue([this](Status s) -> folly::Future<Status> {
        NG_RETURN_IF_ERROR(s);
        if (!isFinalStep() && !vids_.empty()) {
          return getNeighbors();
        }
        return buildResult();
      });
}

Expression* TraverseExecutor::selectFilter() {
  Expression* filter = nullptr;
  if (!(currentStep_ == 1 && range_.min() == 0)) {
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
    auto info = util::collectRespProfileData(result.result, hostLatency[i], size);
    stepInfo.push_back(std::move(info));
  }
  folly::dynamic stepObj = folly::dynamic::object();
  stepObj.insert("storage", stepInfo);
  stepObj.insert("total_rpc_time", folly::sformat("{}(us)", getNbrTimeInUSec));
  addState(folly::sformat("step[{}]", currentStep_), std::move(stepObj));
}

size_t TraverseExecutor::numRowsOfRpcResp(const RpcResponse& resps) const {
  size_t numRows = 0;
  for (const auto& resp : resps.responses()) {
    auto dataset = resp.get_vertices();
    if (dataset) {
      numRows += dataset->rowSize();
    }
  }
  return numRows;
}

template <typename T>
size_t sizeOf(const std::vector<T>& v) {
  size_t sz = 0u;
  for (auto& e : v) {
    sz += e.size();
  }
  return sz;
}

void TraverseExecutor::buildAdjList(DataSet& dataset,
                                    std::vector<Value>& initVertices,
                                    VidHashSet& vids,
                                    VertexMap<Value>& adjList) const {
  for (GetNbrsRespDataSetIter iter(&dataset); iter.valid(); iter.next()) {
    Value v = iter.getVertex();
    initVertices.emplace_back(v);
    VidHashSet dstSet;
    auto adjEdges = iter.getAdjEdges(&dstSet);
    for (const Value& dst : dstSet) {
      if (adjList_.find(dst) == adjList_.end()) {
        vids.emplace(dst);
      }
    }
    DCHECK(adjList_.find(v) == adjList_.end())
        << "The adjacency list should not contain the source vertex: " << v;
    adjList.emplace(v, std::move(adjEdges));
  }
}

folly::Future<Status> TraverseExecutor::asyncExpandOneStep(RpcResponse&& resps) {
  size_t numResps = resps.responses().size();
  auto initVerticesList = std::make_shared<std::vector<std::vector<Value>>>(numResps);
  auto vidsList = std::make_shared<std::vector<VidHashSet>>(numResps);
  auto adjLists = std::make_shared<std::vector<VertexMap<Value>>>(numResps);
  auto taskRunTime = std::make_shared<std::vector<size_t>>(numResps, 0u);

  std::vector<folly::Future<folly::Unit>> futures;
  futures.reserve(numResps);

  for (size_t i = 0; i < numResps; i++) {
    auto dataset = resps.responses()[i].get_vertices();
    if (!dataset) continue;
    auto func = [this,
                 dataset = std::move(*dataset),
                 i,
                 initVerticesList,
                 vidsList,
                 adjLists,
                 taskRunTime]() mutable {
      SCOPED_TIMER(&((*taskRunTime)[i]));
      buildAdjList(dataset, (*initVerticesList)[i], (*vidsList)[i], (*adjLists)[i]);
    };
    futures.emplace_back(folly::via(runner(), std::move(func)));
  }

  return folly::collect(futures).via(runner()).thenValue(
      [this, initVerticesList, vidsList, adjLists, taskRunTime](std::vector<folly::Unit>&&) {
        time::Duration postTaskTime;
        initVertices_.reserve(sizeOf(*initVerticesList));
        for (auto& initVertices : *initVerticesList) {
          std::move(initVertices.begin(), initVertices.end(), std::back_inserter(initVertices_));
        }

        vids_.reserve(sizeOf(*vidsList));
        for (auto& vids : *vidsList) {
          for (auto& v : vids) {
            vids_.emplace(std::move(v));
          }
        }

        adjList_.reserve(adjList_.size() + sizeOf(*adjLists));
        for (auto& adjList : *adjLists) {
          for (auto& p : adjList) {
            adjList_.emplace(std::move(p.first), std::move(p.second));
          }
        }

        addState("expandPostTaskTime", postTaskTime);
        folly::dynamic taskRunTimeArray = folly::dynamic::array();
        for (auto time : *taskRunTime) {
          taskRunTimeArray.push_back(time);
        }
        addState("expandTaskRunTime", std::move(taskRunTimeArray));

        return Status::OK();
      });
}

folly::Future<Status> TraverseExecutor::expandOneStep(RpcResponse&& resps) {
  auto numRows = numRowsOfRpcResp(resps);
  if (numRows < FLAGS_traverse_parallel_threshold_rows) {
    return asyncExpandOneStep(std::move(resps));
  }

  initVertices_.reserve(numRows);
  for (auto& resp : resps.responses()) {
    auto dataset = resp.get_vertices();
    if (dataset) {
      buildAdjList(*dataset, initVertices_, vids_, adjList_);
    }
  }

  return Status::OK();
}

folly::Future<Status> TraverseExecutor::handleResponse(RpcResponse&& resps) {
  NG_RETURN_IF_ERROR(handleCompleteness(resps, FLAGS_accept_partial_success));

  if (currentStep_ == 1 && !traverse_->eFilter() && !traverse_->vFilter()) {
    return expandOneStep(std::move(resps)).thenValue([this](Status s) {
      NG_RETURN_IF_ERROR(s);
      if (range_.min() == 0) {
        result_.rows = buildZeroStepPath();
      }
      return Status::OK();
    });
  }

  List list;
  for (auto& resp : resps.responses()) {
    auto dataset = resp.get_vertices();
    if (dataset) {
      list.values.emplace_back(std::move(*dataset));
    }
  }
  auto listVal = std::make_shared<Value>(std::move(list));
  auto iter = std::make_unique<GetNeighborsIter>(listVal);
  if (currentStep_ == 1) {
    initVertices_.reserve(iter->numRows());
    auto vertices = iter->getVertices();
    // match (v)-[e:Rel]-(v1:Label1)-[e1*2]->() where id(v0) in [6, 23] return v1
    // save the vertex that meets the filter conditions as the starting vertex of the current
    // traverse
    for (auto& vertex : vertices.values) {
      if (vertex.isVertex()) {
        initVertices_.emplace_back(vertex);
      }
    }
    if (range_.min() == 0) {
      result_.rows = buildZeroStepPath();
    }
  }

  expand(iter.get());
  return Status::OK();
}

void TraverseExecutor::expand(GetNeighborsIter* iter) {
  if (iter->numRows() == 0) {
    return;
  }
  auto* vFilter = traverse_->vFilter();
  auto* eFilter = traverse_->eFilter();
  QueryExpressionContext ctx(ectx_);

  Value curVertex;
  std::vector<Value> adjEdges;
  auto sz = iter->size();
  adjEdges.reserve(sz);
  vids_.reserve(vids_.size() + sz);
  adjList_.reserve(adjList_.size() + iter->numRows() + 1u);
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
    if (adjList_.find(dst) == adjList_.end()) {
      vids_.emplace(dst);
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

std::vector<Row> TraverseExecutor::buildZeroStepPath() {
  if (initVertices_.empty()) {
    return std::vector<Row>();
  }
  std::vector<Row> result;
  result.reserve(initVertices_.size());
  if (traverse_->trackPrevPath()) {
    for (auto& vertex : initVertices_) {
      auto dstIter = dst2PathsMap_.find(vertex);
      if (dstIter == dst2PathsMap_.end()) {
        continue;
      }
      auto& prevPaths = dstIter->second;
      for (auto& p : prevPaths) {
        Row row = p;
        List edgeList;
        row.values.emplace_back(vertex);
        row.values.emplace_back(edgeList);
        if (genPath_) {
          edgeList.values.emplace_back(vertex);
          row.values.emplace_back(std::move(edgeList));
        }
        result.emplace_back(std::move(row));
      }
    }
  } else {
    for (auto& vertex : initVertices_) {
      Row row;
      List edgeList;
      row.values.emplace_back(vertex);
      row.values.emplace_back(edgeList);
      if (genPath_) {
        edgeList.values.emplace_back(vertex);
        row.values.emplace_back(std::move(edgeList));
      }
      result.emplace_back(std::move(row));
    }
  }
  return result;
}

folly::Future<Status> TraverseExecutor::buildResult() {
  size_t minStep = range_.min();
  size_t maxStep = range_.max();

  result_.colNames = traverse_->colNames();
  if (maxStep == 0) {
    return finish(ResultBuilder().value(Value(std::move(result_))).build());
  }
  if (FLAGS_max_job_size <= 1) {
    for (const auto& initVertex : initVertices_) {
      auto paths = buildPath(initVertex, minStep, maxStep);
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
  DataSet vertices;
  vertices.rows.reserve(initVertices_.size());
  for (auto& initVertex : initVertices_) {
    Row row;
    row.values.emplace_back(std::move(initVertex));
    vertices.rows.emplace_back(std::move(row));
  }
  auto val = std::make_shared<Value>(std::move(vertices));
  auto iter = std::make_unique<SequentialIter>(val);

  auto scatter = [this, minStep, maxStep](
                     size_t begin, size_t end, Iterator* tmpIter) mutable -> std::vector<Row> {
    // outside caller should already turn on throwOnMemoryExceeded
    DCHECK(memory::MemoryTracker::isOn()) << "MemoryTracker is off";
    // MemoryTrackerVerified
    std::vector<Row> rows;
    for (; tmpIter->valid() && begin++ < end; tmpIter->next()) {
      auto& initVertex = tmpIter->getColumn(0);
      auto paths = buildPath(initVertex, minStep, maxStep);
      if (paths.empty()) {
        continue;
      }
      rows.insert(
          rows.end(), std::make_move_iterator(paths.begin()), std::make_move_iterator(paths.end()));
    }
    return rows;
  };

  auto gather = [this](std::vector<folly::Try<std::vector<Row>>>&& resps) mutable -> Status {
    // MemoryTrackerVerified
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
      auto rows = std::move(respVal).value();
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

// build path based on BFS through adjacency list
std::vector<Row> TraverseExecutor::buildPath(const Value& initVertex,
                                             size_t minStep,
                                             size_t maxStep) {
  memory::MemoryCheckGuard guard;
  auto vidIter = adjList_.find(initVertex);
  if (vidIter == adjList_.end()) {
    return std::vector<Row>();
  }
  auto& src = vidIter->first;
  auto& adjEdges = vidIter->second;
  if (adjEdges.empty()) {
    return std::vector<Row>();
  }

  std::vector<Row> oneStepPath;
  oneStepPath.reserve(adjEdges.size());
  for (auto& edge : adjEdges) {
    List edgeList;
    edgeList.values.emplace_back(edge);
    Row row;
    row.values.emplace_back(src);
    // only contain edges
    row.values.emplace_back(edgeList);
    if (genPath_) {
      // contain nodes & edges
      row.values.emplace_back(std::move(edgeList));
    }
    oneStepPath.emplace_back(std::move(row));
  }

  if (maxStep == 1) {
    if (traverse_->trackPrevPath()) {
      return joinPrevPath(initVertex, oneStepPath);
    }
    return oneStepPath;
  }

  size_t step = 2;
  std::vector<Row> newResult;
  std::queue<std::vector<Value>*> queue;
  std::queue<std::vector<Value>*> edgeListQueue;
  std::list<std::unique_ptr<std::vector<Value>>> holder;
  for (auto& edge : adjEdges) {
    auto ptr = std::make_unique<std::vector<Value>>(std::vector<Value>({edge}));
    queue.emplace(ptr.get());
    edgeListQueue.emplace(ptr.get());
    holder.emplace_back(std::move(ptr));
  }

  size_t adjSize = edgeListQueue.size();
  while (!edgeListQueue.empty()) {
    auto edgeListPtr = edgeListQueue.front();
    auto& dst = edgeListPtr->back().getEdge().dst;
    edgeListQueue.pop();

    std::vector<Value>* vertexEdgeListPtr = nullptr;
    if (genPath_) {
      vertexEdgeListPtr = queue.front();
      queue.pop();
    }

    --adjSize;
    auto dstIter = adjList_.find(dst);
    if (dstIter == adjList_.end()) {
      if (adjSize == 0) {
        if (++step > maxStep) {
          break;
        }
        adjSize = edgeListQueue.size();
      }
      continue;
    }

    auto& adjedges = dstIter->second;
    for (auto& edge : adjedges) {
      if (hasSameEdge(*edgeListPtr, edge.getEdge())) {
        continue;
      }
      auto newEdgeListPtr = std::make_unique<std::vector<Value>>(*edgeListPtr);
      newEdgeListPtr->emplace_back(edge);

      std::unique_ptr<std::vector<Value>> newVertexEdgeListPtr = nullptr;
      if (genPath_) {
        newVertexEdgeListPtr = std::make_unique<std::vector<Value>>(*vertexEdgeListPtr);
        newVertexEdgeListPtr->emplace_back(dstIter->first);
        newVertexEdgeListPtr->emplace_back(edge);
      }

      if (step >= minStep) {
        Row row;
        row.values.emplace_back(src);
        // only contain edges
        row.values.emplace_back(List(*newEdgeListPtr));
        if (genPath_) {
          // contain nodes & edges
          row.values.emplace_back(List(*newVertexEdgeListPtr));
        }
        newResult.emplace_back(std::move(row));
      }
      edgeListQueue.emplace(newEdgeListPtr.get());
      holder.emplace_back(std::move(newEdgeListPtr));
      if (genPath_ && newVertexEdgeListPtr != nullptr) {
        queue.emplace(newVertexEdgeListPtr.get());
        holder.emplace_back(std::move(newVertexEdgeListPtr));
      }
    }
    if (adjSize == 0) {
      if (++step > maxStep) {
        break;
      }
      adjSize = edgeListQueue.size();
    }
  }
  if (minStep <= 1) {
    newResult.insert(newResult.begin(),
                     std::make_move_iterator(oneStepPath.begin()),
                     std::make_move_iterator(oneStepPath.end()));
  }
  if (traverse_->trackPrevPath()) {
    return joinPrevPath(initVertex, newResult);
  }
  return newResult;
}

std::vector<Row> TraverseExecutor::joinPrevPath(const Value& initVertex,
                                                const std::vector<Row>& newResult) const {
  auto dstIter = dst2PathsMap_.find(initVertex);
  if (dstIter == dst2PathsMap_.end()) {
    return std::vector<Row>();
  }

  std::vector<Row> newPaths;
  for (auto& prevPath : dstIter->second) {
    for (auto& p : newResult) {
      if (!hasSameEdgeInPath(prevPath, p)) {
        // copy
        Row row = prevPath;
        row.values.insert(row.values.end(), p.values.begin(), p.values.end());
        newPaths.emplace_back(std::move(row));
      }
    }
  }
  return newPaths;
}

bool TraverseExecutor::hasSameEdge(const std::vector<Value>& edgeList, const Edge& edge) const {
  for (const auto& leftEdge : edgeList) {
    if (leftEdge.isEdge() && leftEdge.getEdge().keyEqual(edge)) {
      return true;
    }
  }
  return false;
}

bool TraverseExecutor::hasSameEdgeInPath(const Row& lhs, const Row& rhs) const {
  for (const auto& leftListVal : lhs.values) {
    if (leftListVal.isList()) {
      auto& leftList = leftListVal.getList().values;
      for (auto& rightListVal : rhs.values) {
        if (rightListVal.isList()) {
          auto& rightList = rightListVal.getList().values;
          for (auto& edgeVal : rightList) {
            if (edgeVal.isEdge() && hasSameEdge(leftList, edgeVal.getEdge())) {
              return true;
            }
          }
        }
      }
    }
  }
  return false;
}

bool TraverseExecutor::hasSameEdgeInSet(const Row& rhs,
                                        const std::unordered_set<Value>& uniqueEdge) const {
  for (const auto& rightListVal : rhs.values) {
    if (rightListVal.isList()) {
      auto& rightList = rightListVal.getList().values;
      for (auto& edgeVal : rightList) {
        if (edgeVal.isEdge() && uniqueEdge.find(edgeVal) != uniqueEdge.end()) {
          return true;
        }
      }
    }
  }
  return false;
}

}  // namespace graph
}  // namespace nebula
