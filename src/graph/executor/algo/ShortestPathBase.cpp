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

std::vector<Row> ShortestPathBase::createLeftPath(size_t rowNum, const Value& meetVid) {
  auto& allSteps = allLeftPaths_[rowNum];
  auto& lastSteps = allSteps.back();
  auto findMeetVid = lastSteps.find(meetVid);
  std::vector<Row> leftPaths(findMeetVid->second);
  for (auto stepIter = allSteps.rbegin() + 1; stepIter != allSteps.rend(); ++stepIter) {
    std::vector<Row> temp;
    for (auto& leftPath : leftPaths) {
      Value id = leftPath.values.front().getVertex().vid;
      auto findId = stepIter->find(id);
      for (auto& step : findId->second) {
        auto newPath = leftPath;
        newPath.values.insert(newPath.values.begin(), step.values.begin(), step.values.end());
        temp.emplace_back(std::move(newPath));
      }
    }
    leftPaths.swap(temp);
  }
  std::vector<Row> result;
  result.reserve(leftPaths.size());
  for (auto& path : leftPaths) {
    Row row;
    auto src = path.values.front();
    path.values.erase(path.values.begin());
    row.emplace_back(std::move(src));
    row.emplace_back(std::move(path));
    result.emplace_back(std::move(row));
  }
  return result;
}

std::vector<Row> ShortestPathBase::createRightPath(size_t rowNum,
                                                   const Value& meetVid,
                                                   bool oddStep) {
  auto& allSteps = allRightPaths_[rowNum];
  std::vector<Row> rightPaths;
  auto& lastSteps = allSteps.back();
  if (oddStep) {
    for (auto& steps : lastSteps) {
      bool flag = false;
      for (auto& step : steps.second) {
        auto& vertex = step.values.front();
        if (vertex.getVertex().vid == meetVid) {
          rightPaths.emplace_back(Row({vertex}));
          flag = true;
          break;
        }
      }
      if (flag) {
        break;
      }
    }
  } else {
    auto findMeetVid = lastSteps.find(meetVid);
    rightPaths = findMeetVid->second;
  }
  for (auto stepIter = allSteps.rbegin() + 1; stepIter != allSteps.rend() - 1; ++stepIter) {
    std::vector<Row> temp;
    for (auto& rightPath : rightPaths) {
      Value id = rightPath.values.front().getVertex().vid;
      auto findId = stepIter->find(id);
      for (auto& step : findId->second) {
        auto newPath = rightPath;
        newPath.values.insert(newPath.values.begin(), step.values.begin(), step.values.end());
        temp.emplace_back(std::move(newPath));
      }
    }
    rightPaths.swap(temp);
  }
  for (auto& path : rightPaths) {
    std::reverse(path.values.begin(), path.values.end());
  }
  return rightPaths;
}

folly::Future<Status> ShortestPathBase::getMeetVidsProps(const std::vector<Value>& meetVids,
                                                         std::vector<Value>& meetVertices) {
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
      .ensure([this, getPropsTime]() {
        stats_->emplace("get_prop_total_rpc",
                        folly::sformat("{}(us)", getPropsTime.elapsedInUSec()));
      })
      .thenValue([this, &meetVertices](PropRpcResponse&& resp) {
        addStats(resp, stats_);
        return handlePropResp(std::move(resp), meetVertices);
      });
}

Status ShortestPathBase::handlePropResp(PropRpcResponse&& resps, std::vector<Value>& vertices) {
  auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
  NG_RETURN_IF_ERROR(result);
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
  return Status::OK();
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

}  // namespace graph
}  // namespace nebula
