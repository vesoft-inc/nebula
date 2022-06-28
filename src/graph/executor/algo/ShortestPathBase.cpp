// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#include "graph/executor/algo/ShortestPathBase.h"

#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"
#include "graph/util/Utils.h"

using apache::thrift::optional_field_ref;
using nebula::storage::StorageClient;

namespace nebula {
namespace graph {
folly::Future<std::vector<Value>> ShortestPathBase::getMeetVidsProps(
    const std::vector<Value>& meetVids) {
  nebula::DataSet vertices({kVid});
  vertices.rows.reserve(meetVids.size());
  for (auto& vid : meetVids) {
    vertices.emplace_back(Row({vid}));
  }

  time::Duration getPropsTime;
  StorageClient* storageClient = qctx_->getStorageClient();
  StorageClient::CommonRequestParam param(pathNode_->space(),
                                          qctx_->rctx()->session()->id(),
                                          qctx_->plan()->id(),
                                          qctx_->plan()->isProfileEnabled());
  return DCHECK_NOTNULL(storageClient)
      ->getProps(param,
                 std::move(vertices),
                 pathNode_->vertexProps(),
                 nullptr,
                 nullptr,
                 false,
                 {},
                 -1,
                 nullptr)
      .via(qctx_->rctx()->runner())
      .thenValue([this, getPropsTime](PropRpcResponse&& resp) {
        addStats(resp, getPropsTime.elapsedInUSec());
        return handlePropResp(std::move(resp));
      });
}

std::vector<Value> ShortestPathBase::handlePropResp(PropRpcResponse&& resps) {
  std::vector<Value> vertices;
  auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
  if (!result.ok()) {
    LOG(WARNING) << "GetProp partial fail";
    return vertices;
  }
  nebula::DataSet v;
  for (auto& resp : resps.responses()) {
    if (resp.props_ref().has_value()) {
      if (UNLIKELY(!v.append(std::move(*resp.props_ref())))) {
        // it's impossible according to the interface
        LOG(WARNING) << "Heterogeneous props dataset";
      }
    } else {
      LOG(WARNING) << "GetProp partial success";
    }
  }
  auto val = std::make_shared<Value>(std::move(v));
  auto iter = std::make_unique<PropIter>(val);
  vertices.reserve(iter->size());
  for (; iter->valid(); iter->next()) {
    vertices.emplace_back(iter->getVertex());
  }
  return vertices;
}

std::string ShortestPathBase::getStorageDetail(
    optional_field_ref<const std::map<std::string, int32_t>&> ref) const {
  if (ref.has_value()) {
    auto content = util::join(*ref, [](auto& iter) -> std::string {
      return folly::sformat("{}:{}(us)", iter.first, iter.second);
    });
    return "{" + content + "}";
  }
  return "";
}

Status ShortestPathBase::handleErrorCode(nebula::cpp2::ErrorCode code, PartitionID partId) const {
  switch (code) {
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

void ShortestPathBase::addStats(RpcResponse& resp,
                                size_t stepNum,
                                int64_t timeInUSec,
                                bool reverse) const {
  auto& hostLatency = resp.hostLatency();
  std::stringstream ss;
  ss << "{\n";
  for (size_t i = 0; i < hostLatency.size(); ++i) {
    size_t size = 0u;
    auto& result = resp.responses()[i];
    if (result.vertices_ref().has_value()) {
      size = (*result.vertices_ref()).size();
    }
    auto& info = hostLatency[i];
    ss << "{" << folly::sformat("{} exec/total/vertices: ", std::get<0>(info).toString())
       << folly::sformat("{}(us)/{}(us)/{},", std::get<1>(info), std::get<2>(info), size) << "\n"
       << folly::sformat("total_rpc_time: {}(us)", timeInUSec) << "\n";
    auto detail = getStorageDetail(result.result.latency_detail_us_ref());
    if (!detail.empty()) {
      ss << folly::sformat("storage_detail: {}", detail);
    }
    ss << "\n}";
  }
  ss << "\n}";
  if (reverse) {
    statsLock_.lock();
    stats_->emplace(folly::sformat("reverse step {}", stepNum), ss.str());
    statsLock_.unlock();
  } else {
    statsLock_.lock();
    stats_->emplace(folly::sformat("step {}", stepNum), ss.str());
    statsLock_.unlock();
  }
}

void ShortestPathBase::addStats(PropRpcResponse& resp, int64_t timeInUSec) const {
  auto& hostLatency = resp.hostLatency();
  std::stringstream ss;
  ss << "{\n";
  for (size_t i = 0; i < hostLatency.size(); ++i) {
    auto& info = hostLatency[i];
    ss << "{" << folly::sformat("{} exec/total: ", std::get<0>(info).toString())
       << folly::sformat("{}(us)/{}(us),", std::get<1>(info), std::get<2>(info)) << "\n"
       << folly::sformat("total_rpc_time: {}(us)", timeInUSec) << "\n";
    auto detail = getStorageDetail(resp.responses()[i].result_ref()->latency_detail_us_ref());
    if (!detail.empty()) {
      ss << folly::sformat("storage_detail: {}", detail);
    }
    ss << "\n}";
  }
  ss << "\n}";
  statsLock_.lock();
  stats_->emplace(folly::sformat("get_prop "), ss.str());
  statsLock_.unlock();
}

}  // namespace graph
}  // namespace nebula
