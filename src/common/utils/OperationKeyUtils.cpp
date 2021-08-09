/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/time/WallClock.h"
#include "utils/OperationKeyUtils.h"

namespace nebula {

// static
std::string OperationKeyUtils::modifyOperationKey(PartitionID part, const std::string& key) {
    uint32_t item = (part << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kOperation);
    int64_t ts = folly::Endian::big(time::WallClock::fastNowInMicroSec());
    uint32_t type = static_cast<uint32_t>(NebulaOperationType::kModify);
    int32_t keySize = key.size();
    std::string result;
    result.reserve(sizeof(int32_t) + sizeof(int64_t) + sizeof(NebulaOperationType) + keySize);
    result.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
          .append(reinterpret_cast<const char*>(&ts), sizeof(int64_t))
          .append(reinterpret_cast<const char*>(&type), sizeof(NebulaOperationType))
          .append(key);
    return result;
}

// static
std::string OperationKeyUtils::deleteOperationKey(PartitionID part) {
    uint32_t item = (part << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kOperation);
    int64_t ts = folly::Endian::big(time::WallClock::fastNowInMicroSec());
    uint32_t type = static_cast<uint32_t>(NebulaOperationType::kDelete);
    std::string result;
    result.reserve(sizeof(int32_t) + sizeof(int64_t) + sizeof(NebulaOperationType));
    result.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
          .append(reinterpret_cast<const char*>(&ts), sizeof(int64_t))
          .append(reinterpret_cast<const char*>(&type), sizeof(NebulaOperationType));
    return result;
}

// static
bool OperationKeyUtils::isModifyOperation(const folly::StringPiece& rawKey) {
    auto position = rawKey.data() + sizeof(PartitionID) + sizeof(int64_t);
    auto len = sizeof(NebulaOperationType);
    auto type = readInt<uint32_t>(position, len);
    return static_cast<uint32_t>(NebulaOperationType::kModify) == type;
}

// static
bool OperationKeyUtils::isDeleteOperation(const folly::StringPiece& rawKey) {
    auto position = rawKey.data() + sizeof(PartitionID) + sizeof(int64_t);
    auto len = sizeof(NebulaOperationType);
    auto type = readInt<uint32_t>(position, len);
    return static_cast<uint32_t>(NebulaOperationType::kDelete) == type;
}

// static
std::string OperationKeyUtils::getOperationKey(const folly::StringPiece& rawValue) {
    auto offset = sizeof(int32_t) + sizeof(int64_t) + sizeof(NebulaOperationType);
    return rawValue.subpiece(offset).toString();
}

// static
std::string OperationKeyUtils::operationPrefix(PartitionID partId) {
    uint32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kOperation);
    std::string result;
    result.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID));
    return result;
}

}  // namespace nebula
