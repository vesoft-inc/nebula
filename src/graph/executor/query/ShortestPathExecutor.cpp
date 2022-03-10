/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "graph/executor/query/ShortestPathExecutor.h"

#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;

namespace nebula {
namespace graph {

// ShortestPath
folly::Future<Status> ShortestPathExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto status = buildRequestDataSet();
  if (!status.ok()) {
    return error(std::move(status));
  }
  if (cartesian_.empty()) {
    VLOG(1) << "Empty input.";
    DataSet emptyResult;
    return finish(ResultBuilder().value(Value(std::move(emptyResult))).build());
  }

  for (auto& pair : cartesian_) {
    dss_[0].rows = std::vector<Row>{Row(std::vector<Value>{std::move(pair.first)})};
    dss_[1].rows = std::vector<Row>{Row(std::vector<Value>{std::move(pair.second)})};

    resetPairState();
    step_ = 0;
    do {
      // Set left(dss_[0]) or right(dss_[1]) dataset as req dataset, and get neighbors
      for (int i = 0; i < 2; i++) {
        if (step_ > shortestPathNode_->stepRange()->max()) {
          dss_[0].clear();
          dss_[1].clear();
          break;
        }
        status = getNeighbors().get();
        if (!status.ok()) {
          return error(std::move(status));
        }
        setInterimState(1 - i);
      }
    } while (!dss_.empty() && !dss_[direction_].rows.empty());
  }

  return folly::makeFuture<Status>(buildResult());
}

void ShortestPathExecutor::setInterimState(int direction) {
  direction_ = direction;
  visiteds_[direction].clear();
  step_ += 1;
}

void ShortestPathExecutor::resetPairState() {
  step_ = 0;
  rightPaths_.clear();
  leftPaths_.clear();
  vidToVertex_.clear();
  visiteds_[0].clear();
  visiteds_[1].clear();
}

Status ShortestPathExecutor::close() {
  // clear the members
  dss_[0].clear();
  dss_[1].clear();
  visiteds_[0].clear();
  visiteds_[1].clear();
  vidToVertex_.clear();
  leftPaths_.clear();
  rightPaths_.clear();

  return Executor::close();
}

Status ShortestPathExecutor::buildRequestDataSet() {
  auto inputVar = shortestPathNode_->inputVar();
  const Result& inputResult = ectx_->getResult(inputVar);

  auto inputIter = inputResult.iter();
  auto iter = static_cast<SequentialIter*>(inputIter.get());

  cartesian_.reserve(iter->size());

  std::unordered_set<std::pair<Value, Value>> uniqueSet;
  uniqueSet.reserve(iter->size());

  const auto& vidType = *(qctx()->rctx()->session()->space().spaceDesc.vid_type_ref());
  std::vector<Expression*> srcs = shortestPathNode_->srcs();
  QueryExpressionContext ctx(ectx_);

  for (; iter->valid(); iter->next()) {
    auto srcVid = srcs[0]->eval(ctx(iter));
    auto dstVid = srcs[1]->eval(ctx(iter));
    if (!SchemaUtil::isValidVid(srcVid, vidType) || !SchemaUtil::isValidVid(dstVid, vidType)) {
      LOG(WARNING) << "Mismatched vid type, space vid type: " << SchemaUtil::typeToString(vidType);
      continue;
    }
    if (!uniqueSet.emplace(std::pair<Value, Value>{srcVid, dstVid}).second) {
      continue;
    }

    cartesian_.emplace_back(std::move(srcVid), std::move(dstVid));
  }

  return Status::OK();
}

folly::Future<Status> ShortestPathExecutor::getNeighbors() {
  StorageClient* storageClient = qctx_->getStorageClient();
  storage::StorageClient::CommonRequestParam param(shortestPathNode_->space(),
                                                   qctx()->rctx()->session()->id(),
                                                   qctx()->plan()->id(),
                                                   qctx()->plan()->isProfileEnabled());

  return storageClient
      ->getNeighbors(param,
                     dss_[direction_].colNames,
                     std::move(dss_[direction_].rows),
                     shortestPathNode_->edgeTypes(),
                     shortestPathNode_->edgeDirection(),
                     nullptr,
                     nullptr,
                     shortestPathNode_->edgeProps(),
                     nullptr,
                     false,
                     false,
                     std::vector<storage::cpp2::OrderBy>(),
                     -1,
                     nullptr)
      .via(runner())
      .thenValue([this](StorageRpcResponse<GetNeighborsResponse>&& resp) mutable {
        return handleResponse(std::move(resp));
      });
}

folly::Future<Status> ShortestPathExecutor::handleResponse(RpcResponse&& resps) {
  auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
  if (!result.ok()) {
    return folly::makeFuture<Status>(std::move(result).status());
  }

  auto& responses = resps.responses();
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

  return folly::makeFuture<Status>(judge(iter.get()));
}

// find the dst in the neighbors (the intersection of dst and neighbors)
Status ShortestPathExecutor::judge(GetNeighborsIter* iter) {
  const auto& spaceInfo = qctx()->rctx()->session()->space();
  QueryExpressionContext ctx(ectx_);
  std::unordered_set<Value> uniqueDst;
  std::unordered_map<Value, Paths>& prevPaths = direction_ == 0 ? leftPaths_ : rightPaths_;

  // std::unordered_set<Value> result;
  Value result;

  DataSet reqDs;
  reqDs.colNames = dss_[direction_].colNames;

  auto* vFilter = shortestPathNode_->vFilter();
  auto* eFilter = shortestPathNode_->eFilter();

  for (; iter->valid(); iter->next()) {
    if (vFilter != nullptr) {
      auto& vFilterVal = vFilter->eval(ctx(iter));
      if (!vFilterVal.isBool() || !vFilterVal.getBool()) {
        continue;
      }
    }
    if (eFilter != nullptr) {
      auto& eFilterVal = eFilter->eval(ctx(iter));
      if (!eFilterVal.isBool() || !eFilterVal.getBool()) {
        continue;
      }
    }
    Value srcV = iter->getVertex();
    // Due to src can have multiple edge, so src will be repeated, this make it work once
    if (vidToVertex_.find(srcV.getVertex().vid) == vidToVertex_.end()) {
      vidToVertex_.emplace(srcV.getVertex().vid, srcV);
      visiteds_[direction_].emplace(srcV.getVertex().vid);
    }

    Value edge = iter->getEdge();
    const Value& dst = iter->getEdgeProp("*", kDst);
    if (!SchemaUtil::isValidVid(dst, *(spaceInfo.spaceDesc.vid_type_ref()))) {
      continue;
    }
    // Filter the same dst
    // Filter same edge to avoid loop
    if (!uniqueDst.emplace(dst).second || vidToVertex_.find(dst) == vidToVertex_.end()) {
      continue;
    }

    List path = List(std::vector<Value>{srcV, edge});
    AddPrevPath(prevPaths, dst, std::move(path));

    // Join the left and right;
    if (visiteds_[1 - direction_].find(dst) != visiteds_[1 - direction_].end()) {
      // result.emplace(dst);
      findPaths(dst);
      return Status::OK();
    }
    reqDs.rows.emplace_back(Row({std::move(dst)}));
  }

  dss_[direction_] = std::move(reqDs);

  return Status::OK();
}

Status ShortestPathExecutor::buildResult() {
  resultDs_.colNames = shortestPathNode_->colNames();

  return finish(ResultBuilder().value(Value(std::move(resultDs_))).build());
}

// The shortest path from src to dst is like [src]..[node]..[dst].
// When we find the node, we need to find the path to src and dst.
// So build the backward paths to src and dst for finding path [node]-> [src] and [dst].
void ShortestPathExecutor::AddPrevPath(std::unordered_map<Value, Paths>& prevPaths,
                                       const Value& vid,
                                       Row&& prevPath) {
  auto pathFound = prevPaths.find(vid);
  if (pathFound == prevPaths.end()) {
    Paths interimPaths;
    interimPaths.emplace_back(std::move(prevPath));
    prevPaths.emplace(vid, std::move(interimPaths));
  } else {
    auto& interimPaths = pathFound->second;
    interimPaths.emplace_back(std::move(prevPath));
  }
}

// Find the shortest path by vid
// Todo: move()
void ShortestPathExecutor::findPaths(Value nodeVid) {
  std::deque<Value> deque;

  auto iter = vidToVertex_.find(nodeVid);
  if (iter == vidToVertex_.end()) {
    LOG(ERROR) << "Can not find the node {" << iter->first << "} in vidToVertex_";
    return;
  }
  deque.push_front(iter->second);

  Value curVid = nodeVid;
  while (leftPaths_.find(curVid) != leftPaths_.end()) {
    Paths& prevPaths = leftPaths_.find(curVid)->second;
    List prevPath = prevPaths[0];
    deque.push_front(prevPath[1]);  // edge
    deque.push_front(prevPath[0]);  // vertex

    curVid = prevPath[0].getVertex().vid;
  }

  curVid = nodeVid;
  while (rightPaths_.find(curVid) != rightPaths_.end()) {
    Paths& prevPaths = rightPaths_.find(curVid)->second;
    List prevPath = prevPaths[0];
    deque.push_back(prevPath[1]);  // edge
    deque.push_back(prevPath[0]);  // vertex

    curVid = prevPath[0].getVertex().vid;
  }

  Value src = std::move(deque.front());
  deque.pop_front();
  Value dst = std::move(deque.back());
  deque.pop_back();

  List list;
  list.reserve(deque.size());
  std::move(begin(deque), end(deque), back_inserter(list.values));

  resultDs_.rows.emplace_back(Row({src, std::move(list), dst}));
}

}  // namespace graph
}  // namespace nebula
