// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#include "graph/executor/algo/ShortestPathExecutor.h"

#include "common/memory/MemoryTracker.h"
#include "graph/executor/algo/BatchShortestPath.h"
#include "graph/executor/algo/SingleShortestPath.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"
#include "sys/sysinfo.h"

using nebula::storage::StorageClient;

namespace nebula {
namespace graph {

folly::Future<Status> ShortestPathExecutor::execute() {
  // MemoryTrackerVerified
  SCOPED_TIMER(&execTime_);
  if (FLAGS_num_path_thread == 0) {
    FLAGS_num_path_thread = get_nprocs_conf();
  }
  DataSet result;
  result.colNames = pathNode_->colNames();

  HashSet startVids;
  HashSet endVids;
  size_t rowSize = checkInput(startVids, endVids);
  std::unique_ptr<ShortestPathBase> pathPtr = nullptr;
  if (rowSize <= FLAGS_num_path_thread) {
    pathPtr = std::make_unique<SingleShortestPath>(pathNode_, qctx_, &otherStats_);
  } else {
    pathPtr = std::make_unique<BatchShortestPath>(pathNode_, qctx_, &otherStats_);
  }
  auto status = pathPtr->execute(startVids, endVids, &result).get();
  NG_RETURN_IF_ERROR(status);
  return finish(ResultBuilder().value(Value(std::move(result))).build());
}

size_t ShortestPathExecutor::checkInput(HashSet& startVids, HashSet& endVids) {
  auto iter = ectx_->getResult(pathNode_->inputVar()).iter();
  const auto& metaVidType = *(qctx()->rctx()->session()->space().spaceDesc.vid_type_ref());
  auto vidType = SchemaUtil::propTypeToValueType(metaVidType.get_type());
  for (; iter->valid(); iter->next()) {
    auto start = iter->getColumn(0);
    auto end = iter->getColumn(1);
    if (start.type() != vidType || end.type() != vidType) {
      LOG(ERROR) << "Mismatched shortestPath vid type. start type : " << start.type()
                 << ", end type: " << end.type() << ", space vid type: " << vidType;
      continue;
    }
    if (start == end) {
      // continue or return error
      continue;
    }
    startVids.emplace(std::move(start));
    endVids.emplace(std::move(end));
  }
  return startVids.size() * endVids.size();
}

}  // namespace graph
}  // namespace nebula
