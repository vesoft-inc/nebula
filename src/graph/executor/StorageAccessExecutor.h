// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_STORAGEACCESSEXECUTOR_H_
#define GRAPH_EXECUTOR_STORAGEACCESSEXECUTOR_H_

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "clients/storage/StorageClientBase.h"
#include "graph/executor/Executor.h"
#include "graph/util/Utils.h"

namespace nebula {

class Expression;

namespace graph {

class Iterator;
struct SpaceInfo;

// It's used for data write/update/query
class StorageAccessExecutor : public Executor {
 protected:
  StorageAccessExecutor(const std::string &name, const PlanNode *node, QueryContext *qctx)
      : Executor(name, node, qctx) {}

  // Parameter isPartialSuccessAccepted to specify
  // whether to return an error for partial succeeded.
  // An error will be returned if isPartialSuccessAccepted
  // is set to false and completeness is less than 100.
  template <typename Resp>
  StatusOr<Result::State> handleCompleteness(const storage::StorageRpcResponse<Resp> &rpcResp,
                                             bool isPartialSuccessAccepted) const {
    auto completeness = rpcResp.completeness();
    if (completeness != 100) {
      const auto &failedCodes = rpcResp.failedParts();
      for (auto it = failedCodes.begin(); it != failedCodes.end(); it++) {
        LOG(ERROR) << name_ << " failed, error " << apache::thrift::util::enumNameSafe(it->second)
                   << ", part " << it->first;
      }
      // cannot execute at all, or partial success is not accepted
      if (completeness == 0 || !isPartialSuccessAccepted) {
        if (failedCodes.size() > 0) {
          return handleErrorCode(failedCodes.begin()->second, failedCodes.begin()->first);
        }
        return Status::Error("Request to storage failed, without failedCodes.");
      }
      // partial success is accepted
      qctx()->setPartialSuccess();
      return Result::State::kPartialSuccess;
    }
    return Result::State::kSuccess;
  }

  Status handleErrorCode(nebula::cpp2::ErrorCode code, PartitionID partId) const {
    switch (code) {
      case nebula::cpp2::ErrorCode::E_RPC_FAILURE:
        return Status::Error("Storage Error: RPC failure, probably timeout.");
      case nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND:
        return Status::Error("Storage Error: Vertex or edge not found.");
      case nebula::cpp2::ErrorCode::E_DATA_TYPE_MISMATCH: {
        std::string error =
            "Storage Error: The data type does not meet the requirements. "
            "Use the correct type of data.";
        return Status::Error(std::move(error));
      }
      case nebula::cpp2::ErrorCode::E_INVALID_VID: {
        std::string error =
            "Storage Error: The VID must be a 64-bit integer"
            " or a string fitting space vertex id length limit.";
        return Status::Error(std::move(error));
      }
      case nebula::cpp2::ErrorCode::E_INVALID_FIELD_VALUE: {
        std::string error =
            "Storage Error: Invalid field value: "
            "may be the filed is not NULL "
            "or without default value or wrong schema.";
        return Status::Error(std::move(error));
      }
      case nebula::cpp2::ErrorCode::E_LEADER_CHANGED:
        return Status::Error(
            folly::sformat("Storage Error: Not the leader of {}. Please retry later.", partId));
      case nebula::cpp2::ErrorCode::E_INVALID_FILTER:
        return Status::Error("Storage Error: Invalid filter.");
      case nebula::cpp2::ErrorCode::E_INVALID_UPDATER:
        return Status::Error("Storage Error: Invalid Update col or yield col.");
      case nebula::cpp2::ErrorCode::E_INVALID_SPACEVIDLEN:
        return Status::Error("Storage Error: Invalid space vid len.");
      case nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND:
        return Status::Error("Storage Error: Space not found.");
      case nebula::cpp2::ErrorCode::E_PART_NOT_FOUND:
        return Status::Error(folly::sformat("Storage Error: Part {} not found.", partId));
      case nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND:
        return Status::Error("Storage Error: Tag not found.");
      case nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND:
        return Status::Error("Storage Error: Tag prop not found.");
      case nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND:
        return Status::Error("Storage Error: Edge not found.");
      case nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND:
        return Status::Error("Storage Error: Edge prop not found.");
      case nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND:
        return Status::Error("Storage Error: Index not found.");
      case nebula::cpp2::ErrorCode::E_INVALID_DATA:
        return Status::Error("Storage Error: Invalid data, may be wrong value type.");
      case nebula::cpp2::ErrorCode::E_NOT_NULLABLE:
        return Status::Error("Storage Error: The not null field cannot be null.");
      case nebula::cpp2::ErrorCode::E_FIELD_UNSET:
        return Status::Error(
            "Storage Error: "
            "The not null field doesn't have a default value.");
      case nebula::cpp2::ErrorCode::E_OUT_OF_RANGE:
        return Status::Error("Storage Error: Out of range value.");
      case nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR:
        return Status::Error(
            "Storage Error: More than one request trying to "
            "add/update/delete one edge/vertex at the same time.");
      case nebula::cpp2::ErrorCode::E_FILTER_OUT:
        return Status::OK();
      case nebula::cpp2::ErrorCode::E_RAFT_TERM_OUT_OF_DATE:
        return Status::Error(folly::sformat(
            "Storage Error: Term of part {} is out of date. Please retry later.", partId));
      case nebula::cpp2::ErrorCode::E_RAFT_WAL_FAIL:
        return Status::Error("Storage Error: Write wal failed. Probably disk is almost full.");
      case nebula::cpp2::ErrorCode::E_RAFT_WRITE_BLOCKED:
        return Status::Error(
            "Storage Error: Write is blocked when creating snapshot. Please retry later.");
      case nebula::cpp2::ErrorCode::E_RAFT_BUFFER_OVERFLOW:
        return Status::Error(folly::sformat(
            "Storage Error: Part {} raft buffer is full. Please retry later.", partId));
      case nebula::cpp2::ErrorCode::E_RAFT_ATOMIC_OP_FAILED:
        return Status::Error("Storage Error: Atomic operation failed.");
        // E_GRAPH_MEMORY_EXCEEDED may happen during rpc response deserialize.
      case nebula::cpp2::ErrorCode::E_GRAPH_MEMORY_EXCEEDED:
        DLOG(ERROR) << "test ";
        return Status::GraphMemoryExceeded("(%d)", static_cast<int32_t>(code));
      case nebula::cpp2::ErrorCode::E_STORAGE_MEMORY_EXCEEDED:
        return Status::StorageMemoryExceeded("(%d)", static_cast<int32_t>(code));
      default:
        auto status = Status::Error("Storage Error: part: %d, error: %s(%d).",
                                    partId,
                                    apache::thrift::util::enumNameSafe(code).c_str(),
                                    static_cast<int32_t>(code));
        LOG(ERROR) << status;
        return status;
    }
    return Status::OK();
  }

  template <typename RESP>
  void addStats(storage::StorageRpcResponse<RESP> &resp) {
    auto &hostLatency = resp.hostLatency();
    for (size_t i = 0; i < hostLatency.size(); ++i) {
      auto info = util::collectRespProfileData(resp.responses()[i].get_result(), hostLatency[i]);
      addState(folly::sformat("resp[{}]", i), std::move(info));
    }
  }

  bool isIntVidType(const SpaceInfo &space) const;

  StatusOr<DataSet> buildRequestDataSetByVidType(Iterator *iter,
                                                 Expression *expr,
                                                 bool dedup,
                                                 bool isCypher = false);

  StatusOr<std::vector<Value>> buildRequestListByVidType(Iterator *iter,
                                                         Expression *expr,
                                                         bool dedup,
                                                         bool isCypher = false);
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_STORAGEACCESSEXECUTOR_H_
