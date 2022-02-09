/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/utils/NebulaKeyUtils.h"

#include "common/utils/IndexKeyUtils.h"

namespace nebula {

// static
bool NebulaKeyUtils::isValidVidLen(size_t vIdLen, const VertexID& srcVId, const VertexID& dstVId) {
  if (srcVId.size() > vIdLen || dstVId.size() > vIdLen) {
    return false;
  }
  return true;
}

// static
std::string NebulaKeyUtils::firstKey(const std::string& prefix, size_t count) {
  std::string key;
  key.reserve(prefix.size() + count);
  key.append(prefix).append(count, '\0');
  return key;
}

// static
std::string NebulaKeyUtils::lastKey(const std::string& prefix, size_t count) {
  std::string key;
  key.reserve(prefix.size() + count);
  key.append(prefix).append(count, '\377');
  return key;
}

// static
std::string NebulaKeyUtils::tagKey(
    size_t vIdLen, PartitionID partId, const VertexID& vId, TagID tagId, char pad) {
  CHECK_GE(vIdLen, vId.size());
  int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kTag_);

  std::string key;
  key.reserve(kTagLen + vIdLen);
  key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
      .append(vId.data(), vId.size())
      .append(vIdLen - vId.size(), pad)
      .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID));
  return key;
}

// static
std::string NebulaKeyUtils::edgeKey(size_t vIdLen,
                                    PartitionID partId,
                                    const VertexID& srcId,
                                    EdgeType type,
                                    EdgeRanking rank,
                                    const VertexID& dstId,
                                    EdgeVerPlaceHolder ev) {
  CHECK_GE(vIdLen, srcId.size());
  CHECK_GE(vIdLen, dstId.size());
  int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kEdge);

  std::string key;
  key.reserve(kEdgeLen + (vIdLen << 1));
  key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
      .append(srcId.data(), srcId.size())
      .append(vIdLen - srcId.size(), '\0')
      .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
      .append(NebulaKeyUtils::encodeRank(rank))
      .append(dstId.data(), dstId.size())
      .append(vIdLen - dstId.size(), '\0')
      .append(1, ev);
  return key;
}

// static
std::string NebulaKeyUtils::vertexKey(size_t vIdLen,
                                      PartitionID partId,
                                      const VertexID& vId,
                                      char pad) {
  CHECK_GE(vIdLen, vId.size());
  PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kVertex);
  std::string key;
  key.reserve(kTagLen + vIdLen);
  key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
      .append(vId.data(), vId.size())
      .append(vIdLen - vId.size(), pad);
  return key;
}

// static
std::string NebulaKeyUtils::vertexPrefix(PartitionID partId) {
  PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kVertex);
  std::string key;
  key.reserve(sizeof(PartitionID));
  key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID));
  return key;
}

// static
std::string NebulaKeyUtils::systemCommitKey(PartitionID partId) {
  int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kSystem);
  uint32_t type = static_cast<uint32_t>(NebulaSystemKeyType::kSystemCommit);
  std::string key;
  key.reserve(kSystemLen);
  key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
      .append(reinterpret_cast<const char*>(&type), sizeof(NebulaSystemKeyType));
  return key;
}

// static
std::string NebulaKeyUtils::systemPartKey(PartitionID partId) {
  uint32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kSystem);
  uint32_t type = static_cast<uint32_t>(NebulaSystemKeyType::kSystemPart);
  std::string key;
  key.reserve(kSystemLen);
  key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
      .append(reinterpret_cast<const char*>(&type), sizeof(NebulaSystemKeyType));
  return key;
}

// static
std::string NebulaKeyUtils::kvKey(PartitionID partId, const folly::StringPiece& name) {
  std::string key;
  key.reserve(sizeof(PartitionID) + name.size());
  int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kKeyValue);
  key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
      .append(name.data(), name.size());
  return key;
}

// static
std::string NebulaKeyUtils::tagPrefix(size_t vIdLen,
                                      PartitionID partId,
                                      const VertexID& vId,
                                      TagID tagId) {
  CHECK_GE(vIdLen, vId.size());
  PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kTag_);

  std::string key;
  key.reserve(sizeof(PartitionID) + vIdLen + sizeof(TagID));
  key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
      .append(vId.data(), vId.size())
      .append(vIdLen - vId.size(), '\0')
      .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID));
  return key;
}

// static
std::string NebulaKeyUtils::tagPrefix(size_t vIdLen, PartitionID partId, const VertexID& vId) {
  CHECK_GE(vIdLen, vId.size());
  PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kTag_);
  std::string key;
  key.reserve(sizeof(PartitionID) + vIdLen);
  key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
      .append(vId.data(), vId.size())
      .append(vIdLen - vId.size(), '\0');
  return key;
}

// static
std::string NebulaKeyUtils::tagPrefix(PartitionID partId) {
  PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kTag_);
  std::string key;
  key.reserve(sizeof(PartitionID));
  key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID));
  return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(size_t vIdLen,
                                       PartitionID partId,
                                       const VertexID& srcId,
                                       EdgeType type) {
  CHECK_GE(vIdLen, srcId.size());
  PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kEdge);

  std::string key;
  key.reserve(sizeof(PartitionID) + vIdLen + sizeof(EdgeType));
  key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
      .append(srcId.data(), srcId.size())
      .append(vIdLen - srcId.size(), '\0')
      .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType));
  return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(size_t vIdLen, PartitionID partId, const VertexID& srcId) {
  CHECK_GE(vIdLen, srcId.size());
  PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kEdge);
  std::string key;
  key.reserve(sizeof(PartitionID) + vIdLen);
  key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
      .append(srcId.data(), srcId.size())
      .append(vIdLen - srcId.size(), '\0');
  return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(PartitionID partId) {
  PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kEdge);
  std::string key;
  key.reserve(sizeof(PartitionID));
  key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID));
  return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(size_t vIdLen,
                                       PartitionID partId,
                                       const VertexID& srcId,
                                       EdgeType type,
                                       EdgeRanking rank,
                                       const VertexID& dstId) {
  CHECK_GE(vIdLen, srcId.size());
  CHECK_GE(vIdLen, dstId.size());
  int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kEdge);
  std::string key;
  key.reserve(sizeof(PartitionID) + (vIdLen << 1) + sizeof(EdgeType) + sizeof(EdgeRanking));
  key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
      .append(srcId.data(), srcId.size())
      .append(vIdLen - srcId.size(), '\0')
      .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
      .append(NebulaKeyUtils::encodeRank(rank))
      .append(dstId.data(), dstId.size())
      .append(vIdLen - dstId.size(), '\0');
  return key;
}

// static
std::vector<std::string> NebulaKeyUtils::snapshotPrefix(PartitionID partId) {
  std::vector<std::string> result;
  // snapshot of meta would be all key-value pairs
  if (partId == 0) {
    result.emplace_back("");
  } else {
    result.emplace_back(tagPrefix(partId));
    result.emplace_back(edgePrefix(partId));
    result.emplace_back(IndexKeyUtils::indexPrefix(partId));
    // kSystem will be written when balance data
    // kOperation will be blocked by jobmanager later
  }
  return result;
}

std::string NebulaKeyUtils::systemPrefix() {
  int8_t type = static_cast<uint32_t>(NebulaKeyType::kSystem);
  std::string key;
  key.reserve(sizeof(int8_t));
  key.append(reinterpret_cast<const char*>(&type), sizeof(int8_t));
  return key;
}

std::string NebulaKeyUtils::toLockKey(const folly::StringPiece& rawKey) {
  std::string ret = rawKey.str();
  ret.back() = 0;
  return ret;
}

std::string NebulaKeyUtils::toEdgeKey(const folly::StringPiece& lockKey) {
  std::string ret = lockKey.str();
  ret.back() = 1;
  return ret;
}

std::string NebulaKeyUtils::adminTaskKey(int32_t seqId, JobID jobId, TaskID taskId) {
  std::string key;
  key.reserve(sizeof(int32_t) + sizeof(JobID) + sizeof(TaskID));
  key.append(reinterpret_cast<char*>(&seqId), sizeof(int32_t));
  key.append(reinterpret_cast<char*>(&jobId), sizeof(JobID));
  key.append(reinterpret_cast<char*>(&taskId), sizeof(TaskID));
  return key;
}

std::string NebulaKeyUtils::dataVersionKey() {
  return "\xFF\xFF\xFF\xFF";
}

std::string NebulaKeyUtils::dataVersionValue() {
  return "3.0";
}

}  // namespace nebula
