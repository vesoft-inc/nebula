/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "graph/executor/query/ShortestPathExecutor.h"

#include "common/datatypes/Value.h"
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

  for (int i = 0; i < 2; i++) {
    const string& inputVar =
        i == 0 ? shortestPathNode_->leftInputVar() : shortestPathNode_->rightInputVar();
    auto status = buildRequestDataSet(inputVar, dss_[i]);
    if (!status.ok()) {
      return error(std::move(status));
    }
  }

  while (reqDs_.size() != 0) {
    // Set left(dss_[0]) or right(dss_[1]) dataset as req dataset, and get neighbors
    for (int i = 0; i < 2; i++) {
      step_ += 1;
      reqDs_ = std::move(dss_[i]);
      direction_ = i;
      visiteds_[i].clear();

      auto status = getNeighbors().get();
      if (!status.ok()) {
        return error(std::move(status));
      }
      dss_[i] = std::move(reqDs_);
    }
  }

  return Status::OK();
}

Status ShortestPathExecutor::close() {
  // clear the members
  dss_.clear();
  visiteds_.clear();
  vidToVertex_.clear();
  leftPaths_.clear();
  rightPaths_.clear();

  return Executor::close();
}

Status ShortestPathExecutor::buildRequestDataSet(std::string inputVar, DataSet& ds) {
  auto inputIter = ectx_->getResult(inputVar).iter();
  SequentialIter* iter = static_cast<SequentialIter*>(inputIter.get());

  ds.colNames = {kVid};
  ds.rows.reserve(iter->size());

  std::unordered_set<Value> uniqueSet;
  uniqueSet.reserve(iter->size());

  const auto& vidType = *(qctx()->rctx()->session()->space().spaceDesc.vid_type_ref());
  // TODO: build src for shortestPathNode_
  auto* src = shortestPathNode_->src();
  QueryExpressionContext ctx(ectx_);

  for (; iter->valid(); iter->next()) {
    auto vid = src->eval(ctx(iter));
    if (!SchemaUtil::isValidVid(vid, vidType)) {
      LOG(WARNING) << "Mismatched vid type: " << vid.type()
                   << ", space vid type: " << SchemaUtil::typeToString(vidType);
      continue;
    }
    if (!uniqueSet.emplace(vid).second) {
      continue;
    }
    // [from TraverseExecutor] Need copy here, Argument executor may depends on this variable.
    // auto prevPath = *iter->row();
    // AddPrevPath(prev, vid, std::move(prevPath));

    ds.emplace_back(Row({std::move(vid)}));
  }
  // leftPaths_.emplace_back(std::move(prev));

  if (ds.rows.empty()) {
    VLOG(1) << "Empty input.";
    DataSet emptyResult;
    return finish(ResultBuilder().value(Value(std::move(emptyResult))).build());
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
                     reqDs_.colNames,
                     std::move(reqDs_.rows),
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

  return judge(iter.get());
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
  reqDs.colNames = reqDs_.colNames;

  auto* vFilter = shortestPathNode_->vFilter();
  auto* eFilter = shortestPathNode_->eFilter();

  for (; iter->valid(); iter->next()) {
    // TODO: step
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
    if (vidToVertex_.find(srcV.getVertex().vid) == vidToVertex_.end()) {
      vidToVertex_.emplace(srcV.getVertex().vid, srcV);
      visiteds_[direction_].emplace(srcV.getVertex().vid);
    }
    Value e = iter->getEdge();
    auto& dst = iter->getEdgeProp("*", kDst);
    if (!SchemaUtil::isValidVid(dst, *(spaceInfo.spaceDesc.vid_type_ref()))) {
      continue;
    }
    List path = List(std::vector<Value>{srcV, e});
    AddPrevPath(prevPaths, dst, std::move(path));

    if (uniqueDst.emplace(dst).second) {
      // Join the left and right;
      if (visiteds_[1 - direction_].find(dst) != visiteds_[1 - direction_].end()) {
        // result.emplace(dst);
        result = std::move(dst);
        break;
      }
      reqDs.rows.emplace_back(Row({std::move(dst)}));
    }
  }

  reqDs_ = std::move(reqDs);
  return buildResult(findPaths(result));
}

Status ShortestPathExecutor::buildResult(DataSet result) {
  result.colNames = shortestPathNode_->colNames();

  return finish(ResultBuilder().value(Value(std::move(result))).build());
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
DataSet ShortestPathExecutor::findPaths(Value nodeVid) {
  DataSet ds;

  std::deque<Value> deque;

  auto node = vidToVertex_.find(nodeVid);
  deque.push_front(node->second);

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
    deque.push_front(prevPath[1]);  // edge
    deque.push_front(prevPath[0]);  // vertex

    curVid = prevPath[0].getVertex().vid;
  }

  Row path;
  path.reserve(deque.size());
  while (!deque.empty()) {
    path.emplace_back(deque.front());
    deque.pop_front();
  }

  ds.rows.emplace_back(std::move(path));
  return ds;
}

}  // namespace graph
}  // namespace nebula
