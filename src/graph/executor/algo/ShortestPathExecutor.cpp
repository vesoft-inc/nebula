// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#include "graph/executor/algo/ShortestPathExecutor.h"

#include "graph/executor/algo/SingleShortestPath.h"
// #include "graph/executor/algo/BatchShortestPath.h"

#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"
#include "sys/sysinfo.h"

using nebula::storage::StorageClient;

DEFINE_uint32(num_path_thread, 0, "number of concurrent threads when do shortest path");
namespace nebula {
namespace graph {
folly::Future<Status> ShortestPathExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  if (FLAGS_num_path_thread == 0) {
    FLAGS_num_path_thread = get_nprocs_conf();
  }
  DataSet result;
  result.colNames = pathNode_->colNames();

  std::unordered_set<Value> startVids;
  std::unordered_set<Value> endVids;
  size_t rowSize = checkInput(startVids, endVids);
  std::unique_ptr<ShortestPathBase> pathPtr = nullptr;
  if (rowSize <= FLAGS_num_path_thread) {
    pathPtr = std::make_unique<SingleShortestPath>(pathNode_, qctx_, &otherStats_);
  }
  pathPtr = std::make_unique<SingleShortestPath>(pathNode_, qctx_, &otherStats_);
  auto status = pathPtr->execute(startVids, endVids, &result).get();
  NG_RETURN_IF_ERROR(status);
  return finish(ResultBuilder().value(Value(std::move(result))).build());
}

size_t ShortestPathExecutor::checkInput(std::unordered_set<Value>& startVids,
                                        std::unordered_set<Value>& endVids) {
  auto iter = ectx_->getResult(pathNode_->inputVar()).iter();
  const auto& vidType = *(qctx()->rctx()->session()->space().spaceDesc.vid_type_ref());
  std::unordered_set<std::pair<Value, Value>> cartesianProduct;
  cartesianProduct.reserve(iter->size());
  for (; iter->valid(); iter->next()) {
    auto start = iter->getColumn(0);
    auto end = iter->getColumn(1);
    if (!SchemaUtil::isValidVid(start, vidType) || !SchemaUtil::isValidVid(end, vidType)) {
      LOG(ERROR) << "Mismatched shortestPath vid type. start type : " << start.type()
                 << ", end type: " << end.type()
                 << ", space vid type: " << SchemaUtil::typeToString(vidType);
      continue;
    }
    if (start == end) {
      // continue or return error
      continue;
    }
    if (cartesianProduct.emplace(std::pair<Value, Value>{start, end}).second) {
      startVids.emplace(std::move(start));
      endVids.emplace(std::move(end));
    }
  }
  return cartesianProduct.size();
}

}  // namespace graph
}  // namespace nebula
