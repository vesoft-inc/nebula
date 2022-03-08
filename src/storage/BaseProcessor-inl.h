/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_BASEPROCESSOR_INL_H
#define STORAGE_BASEPROCESSOR_INL_H

#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

template <typename RESP>
nebula::cpp2::ErrorCode BaseProcessor<RESP>::writeResultTo(WriteResult code, bool isEdge) {
  switch (code) {
    case WriteResult::SUCCEEDED:
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    case WriteResult::UNKNOWN_FIELD:
      if (isEdge) {
        return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
      }
      return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
    case WriteResult::NOT_NULLABLE:
      return nebula::cpp2::ErrorCode::E_NOT_NULLABLE;
    case WriteResult::TYPE_MISMATCH:
      return nebula::cpp2::ErrorCode::E_DATA_TYPE_MISMATCH;
    case WriteResult::FIELD_UNSET:
      return nebula::cpp2::ErrorCode::E_FIELD_UNSET;
    case WriteResult::OUT_OF_RANGE:
      return nebula::cpp2::ErrorCode::E_OUT_OF_RANGE;
    case WriteResult::INCORRECT_VALUE:
      return nebula::cpp2::ErrorCode::E_INVALID_FIELD_VALUE;
    default:
      return nebula::cpp2::ErrorCode::E_UNKNOWN;
  }
}

template <typename RESP>
void BaseProcessor<RESP>::handleAsync(GraphSpaceID spaceId,
                                      PartitionID partId,
                                      nebula::cpp2::ErrorCode code) {
  VLOG(3) << "partId:" << partId << ", code: " << static_cast<int32_t>(code);

  bool finished = false;
  {
    std::lock_guard<std::mutex> lg(this->lock_);
    handleErrorCode(code, spaceId, partId);
    this->callingNum_--;
    if (this->callingNum_ == 0) {
      finished = true;
    }
  }

  if (finished) {
    this->onFinished();
  }
}

template <typename RESP>
meta::cpp2::ColumnDef BaseProcessor<RESP>::columnDef(std::string name,
                                                     nebula::cpp2::PropertyType type) {
  nebula::meta::cpp2::ColumnDef column;
  column.name_ref() = std::move(name);
  column.type_ref()->type_ref() = type;
  return column;
}

template <typename RESP>
void BaseProcessor<RESP>::pushResultCode(nebula::cpp2::ErrorCode code,
                                         PartitionID partId,
                                         HostAddr leader) {
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    cpp2::PartitionResult thriftRet;
    thriftRet.code_ref() = code;
    thriftRet.part_id_ref() = partId;
    if (leader != HostAddr("", 0)) {
      thriftRet.leader_ref() = leader;
    }
    codes_.emplace_back(std::move(thriftRet));
  }
}

template <typename RESP>
void BaseProcessor<RESP>::handleErrorCode(nebula::cpp2::ErrorCode code,
                                          GraphSpaceID spaceId,
                                          PartitionID partId) {
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (code == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
      handleLeaderChanged(spaceId, partId);
    } else {
      pushResultCode(code, partId);
    }
  }
}

template <typename RESP>
void BaseProcessor<RESP>::handleLeaderChanged(GraphSpaceID spaceId, PartitionID partId) {
  auto addrRet = env_->kvstore_->partLeader(spaceId, partId);
  if (ok(addrRet)) {
    auto leader = value(std::move(addrRet));
    this->pushResultCode(nebula::cpp2::ErrorCode::E_LEADER_CHANGED, partId, leader);
  } else {
    LOG(ERROR) << "Fail to get part leader, spaceId: " << spaceId << ", partId: " << partId
               << ", ResultCode: " << static_cast<int32_t>(error(addrRet));
    this->pushResultCode(error(addrRet), partId);
  }
}

template <typename RESP>
void BaseProcessor<RESP>::doPut(GraphSpaceID spaceId,
                                PartitionID partId,
                                std::vector<kvstore::KV>&& data) {
  this->env_->kvstore_->asyncMultiPut(
      spaceId, partId, std::move(data), [spaceId, partId, this](nebula::cpp2::ErrorCode code) {
        handleAsync(spaceId, partId, code);
      });
}

template <typename RESP>
void BaseProcessor<RESP>::doRemove(GraphSpaceID spaceId,
                                   PartitionID partId,
                                   std::vector<std::string>&& keys) {
  this->env_->kvstore_->asyncMultiRemove(
      spaceId, partId, std::move(keys), [spaceId, partId, this](nebula::cpp2::ErrorCode code) {
        handleAsync(spaceId, partId, code);
      });
}

template <typename RESP>
void BaseProcessor<RESP>::doRemoveRange(GraphSpaceID spaceId,
                                        PartitionID partId,
                                        const std::string& start,
                                        const std::string& end) {
  this->env_->kvstore_->asyncRemoveRange(
      spaceId, partId, start, end, [spaceId, partId, this](nebula::cpp2::ErrorCode code) {
        handleAsync(spaceId, partId, code);
      });
}

template <typename RESP>
StatusOr<std::string> BaseProcessor<RESP>::encodeRowVal(const meta::NebulaSchemaProvider* schema,
                                                        const std::vector<std::string>& propNames,
                                                        const std::vector<Value>& props,
                                                        WriteResult& wRet) {
  RowWriterV2 rowWrite(schema);
  // If req.prop_names is not empty, use the property name in req.prop_names
  // Otherwise, use property name in schema
  if (!propNames.empty()) {
    for (size_t i = 0; i < propNames.size(); i++) {
      wRet = rowWrite.setValue(propNames[i], props[i]);
      if (wRet != WriteResult::SUCCEEDED) {
        return Status::Error("Add field failed");
      }
    }
  } else {
    for (size_t i = 0; i < props.size(); i++) {
      wRet = rowWrite.setValue(i, props[i]);
      if (wRet != WriteResult::SUCCEEDED) {
        return Status::Error("Add field failed");
      }
    }
  }

  wRet = rowWrite.finish();
  if (wRet != WriteResult::SUCCEEDED) {
    return Status::Error("Add field failed");
  }

  return std::move(rowWrite).moveEncodedStr();
}

template <typename RESP>
nebula::cpp2::ErrorCode BaseProcessor<RESP>::checkStatType(
    const meta::SchemaProviderIf::Field& field, cpp2::StatType statType) {
  // todo(doodle): how to deal with nullable fields? For now, null add anything
  // is null, if there is even one null, the result will be invalid
  auto fType = field.type();
  switch (statType) {
    case cpp2::StatType::SUM:
    case cpp2::StatType::AVG:
    case cpp2::StatType::MIN:
    case cpp2::StatType::MAX: {
      if (fType == nebula::cpp2::PropertyType::INT64 ||
          fType == nebula::cpp2::PropertyType::INT32 ||
          fType == nebula::cpp2::PropertyType::INT16 || fType == nebula::cpp2::PropertyType::INT8 ||
          fType == nebula::cpp2::PropertyType::FLOAT ||
          fType == nebula::cpp2::PropertyType::DOUBLE) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
      }
      return nebula::cpp2::ErrorCode::E_INVALID_STAT_TYPE;
    }
    case cpp2::StatType::COUNT: {
      break;
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula
#endif
