/* Copyright (c) 2021 vesoft inc. All rights reserved.
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

/*

lds = buildRequestDataSet
rds = buildRequestDataSet

lq.push(lds)
rq.push(rds)

while(lq, rq) {

  for(int i = 0; i < lq.size(); i++) {
    ds = lq.front();
    lq.pop()

    getneighbors(ds, 0)
    // inside set data and judge
    //
  }
}

*/

namespace nebula {
namespace graph {

// ShortestPath
folly::Future<Status> ShortestPathExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  // function: find the path to source/destination
  // {key: path} key: node, path: the prev node of path to src/dst node
  std::unordered_map<Value, Paths> prevPathToSrc;
  std::unordered_map<Value, Paths> prevPathToDst;

  auto leftStatus =
      buildRequestDataSet(shortestPathNode_->leftInputVar(), leftReqDataSet_, prevPathToSrc);
  if (!leftStatus.ok()) {
    return error(std::move(leftStatus));
  }
  auto rightStatus =
      buildRequestDataSet(shortestPathNode_->rightInputVar(), rightReqDataSet_, prevPathToDst);
  if (!rightStatus.ok()) {
    return error(std::move(rightStatus));
  }

  // std::list<DataSet> leftDs;
  // std::list<DataSet> rightDs;

  // while (!leftDs.empty() && !rightDs.empty()) {
  //   auto allLeftNeighbors = getNeighbors(leftDs);
  // }

  std::queue<DataSet> srcQueue;
  std::queue<DataSet> dstQueue;

  while (!srcQueue.empty() && !dstQueue.empty()) {
    step += 1;
    std::queue<DataSet>& q = srcQueue.size() < dstQueue.size() ? srcQueue : dstQueue;

    int i = q.size();
    while (i != 0) {
      i -= 1;
      auto& ds = q.front();
      q.pop();

      auto neighbors = getNeighbors(ds);
    }
  }
  // std::list<DataSet> ds;
  // while (!ds.empty()) {
  //   // get all neighbors
  //   auto allNeighbors = getNeighbors(reqDs);
  //   // judge get the end
  //   // if (FindIntersection(allNeighbors, neighborsFromDst)) {}
  //   // get the shortest path
  // }

  buildResult(leftReqDataSet_);

  return Status::OK();
}

Status ShortestPathExecutor::close() {
  // clear the members
  leftReqDataSet_.rows.clear();
  return Executor::close();
}

Status ShortestPathExecutor::buildRequestDataSet(std::string inputVar,
                                                 DataSet& ds,
                                                 std::unordered_map<Value, Paths>& prev) {
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
    auto prevPath = *iter->row();
    buildPrevPath(prev, vid, std::move(prevPath));

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

folly::Future<StatusOr<std::vector<DataSet>>> ShortestPathExecutor::getNeighbors(DataSet& ds,
                                                                                 int flag) {
  StorageClient* storageClient = qctx_->getStorageClient();
  storage::StorageClient::CommonRequestParam param(shortestPathNode_->space(),
                                                   qctx()->rctx()->session()->id(),
                                                   qctx()->plan()->id(),
                                                   qctx()->plan()->isProfileEnabled());

  return storageClient
      ->getNeighbors(param,
                     ds.colNames,
                     std::move(ds.rows),
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
      .thenValue([this, flag](StorageRpcResponse<GetNeighborsResponse>&& resp) mutable {
        return handleResponse(std::move(resp), flag);
      });
}

Status ShortestPathExecutor::getAllNeighbors(std::vector<DataSet>& dss) {
  std::vector<DataSet> result;
  for (auto& ds : dss) {
    StatusOr<std::vector<DataSet>> neighborsRet = getNeighbors(ds).get();
    NG_RETURN_IF_ERROR(neighborsRet.status());
  }
}

folly::Future<StatusOr<std::vector<DataSet>>> ShortestPathExecutor::handleResponse(
    RpcResponse&& resps, int flag) {
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

  return dss;
}

// // find the dst in the neighbors (the intersection of dst and neighbors)
// DataSet ShortestPath::FindIntersection() {}

// // get the paths to dst and src from intersection, join both of them and return.
// std::vector<Path> ShortestPath::GetPaths(std::unordered_set<Value, Path> prevPathToSrc,
//                                          std::unordered_set<Value, Path> prevPathToDst,
//                                          std::vector<Value> intersection) {}

Status ShortestPathExecutor::judge(DataSet ds, std::unordered_set<Row> map) {
  for (Row& row : ds.rows) {
    if (map.find(row) != map.end()) {
      return Status::OK();
    }
  }
}

Status ShortestPathExecutor::buildResult(DataSet result) {
  // This means we are reaching a dead end, return empty.
  // if (range_ != nullptr && currentStep_ < range_->min()) {
  //   return finish(ResultBuilder().value(Value(DataSet())).build());
  // }

  // result.colNames = traverse_->colNames();
  // result.rows.reserve(cnt_);
  // for (auto& currentStepPaths : paths_) {
  //   for (auto& paths : currentStepPaths) {
  //     std::move(paths.second.begin(), paths.second.end(), std::back_inserter(result.rows));
  //   }
  // }

  return finish(ResultBuilder().value(Value(std::move(result))).build());
}

Status ShortestPathExecutor::buildInterimPath(GetNeighborsIter* iter, VertexType type) {
  const auto& spaceInfo = qctx()->rctx()->session()->space();
  DataSet reqDs;
  reqDs.colNames = reqDs_.colNames;
  size_t count = 0;

  const std::unordered_map<Value, Paths>& prev = paths_.back();
  if (currentStep_ == 1 && zeroStep()) {
    paths_.emplace_back();
    NG_RETURN_IF_ERROR(handleZeroStep(prev, iter->getVertices(), paths_.back(), count));
    // If 0..0 case, release memory and return immediately.
    if (range_ != nullptr && range_->max() == 0) {
      releasePrevPaths(count);
      return Status::OK();
    }
  }
  paths_.emplace_back();
  std::unordered_map<Value, Paths>& current = paths_.back();

  auto* vFilter = traverse_->vFilter();
  auto* eFilter = traverse_->eFilter();
  QueryExpressionContext ctx(ectx_);
  std::unordered_set<Value> uniqueDst;

  for (; iter->valid(); iter->next()) {
    auto& dst = iter->getEdgeProp("*", kDst);
    if (!SchemaUtil::isValidVid(dst, *(spaceInfo.spaceDesc.vid_type_ref()))) {
      continue;
    }
    if (vFilter != nullptr && currentStep_ == 1) {
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
    auto srcV = iter->getVertex();
    auto e = iter->getEdge();
    // Join on dst = src
    auto pathToSrcFound = prev.find(srcV.getVertex().vid);
    if (pathToSrcFound == prev.end()) {
      return Status::Error("Can't find prev paths.");
    }
    const auto& paths = pathToSrcFound->second;
    for (auto& prevPath : paths) {
      if (hasSameEdge(prevPath, e.getEdge())) {
        continue;
      }
      if (uniqueDst.emplace(dst).second) {
        reqDs.rows.emplace_back(Row({std::move(dst)}));
      }
      if (currentStep_ == 1) {
        Row path;
        if (traverse_->trackPrevPath()) {
          path = prevPath;
        }
        path.values.emplace_back(srcV);
        List neighbors;
        neighbors.values.emplace_back(e);
        path.values.emplace_back(std::move(neighbors));
        buildPath(current, dst, std::move(path));
        ++count;
      } else {
        auto path = prevPath;
        auto& eList = path.values.back().mutableList().values;
        eList.emplace_back(srcV);
        eList.emplace_back(e);
        buildPath(current, dst, std::move(path));
        ++count;
      }
    }  // `prevPath'
  }    // `iter'

  releasePrevPaths(count);
  reqDs_ = std::move(reqDs);
  return Status::OK();
}

// The shortest path from src to dst is like [src]..[node]..[dst].
// When we find the node, we need to find the path to src and dst.
// So build the backward paths to src and dst for finding path [node]-> [src] and [dst].
void ShortestPathExecutor::buildPrevPath(std::unordered_map<Value, Paths>& prevPaths,
                                         const Value& node,
                                         Row&& path) {
  auto pathFound = prevPaths.find(node);
  if (pathFound == prevPaths.end()) {
    Paths interimPaths;
    interimPaths.emplace_back(std::move(path));
    prevPaths.emplace(node, std::move(interimPaths));
  } else {
    auto& interimPaths = pathFound->second;
    interimPaths.emplace_back(std::move(path));
  }
}

}  // namespace graph
}  // namespace nebula
