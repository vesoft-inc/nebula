/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_UTILS_NEBULAKEYUTILS_H_
#define COMMON_UTILS_NEBULAKEYUTILS_H_

#include "common/utils/Types.h"

namespace nebula {

/**
 * TagKeyUtils:
 * type(1) + partId(3) + vertexId(*) + tagId(4)
 *
 * EdgeKeyUtils:
 * type(1) + partId(3) + srcId(*) + edgeType(4) + edgeRank(8) + dstId(*) +
 * placeHolder(1)
 *
 * For data in Nebula 1.0, all vertexId is int64_t, so the size would be 8.
 * For data in Nebula 2.0, all vertexId is fixed length string according to
 * space property.
 *
 * LockKeyUtils:
 * type(1) + partId(3) + srcId(*) + edgeType(4) + edgeRank(8) + dstId(*) +
 * placeHolder(1)
 * */

/**
 * This class supply some utils for transition between Vertex/Edge and key in
 * kvstore.
 * */
class NebulaKeyUtils final {
 public:
  ~NebulaKeyUtils() = default;

  /*
   * Check the validity of vid length
   */
  static bool isValidVidLen(size_t vIdLen, const VertexID& srcvId, const VertexID& dstvId = "");

  /**
   * Generate the first key with prefix.
   * count means the number of count '\0' is filled after the prefix.
   * */
  static std::string firstKey(const std::string& prefix, size_t count);

  /**
   * Generate the last key with prefix.
   * count means the number of count '\377' is filled after the prefix.
   * */
  static std::string lastKey(const std::string& prefix, size_t count);

  /**
   * Generate tag key for kv store
   * */
  static std::string tagKey(
      size_t vIdLen, PartitionID partId, const VertexID& vId, TagID tagId, char pad = '\0');

  static std::string edgeKey(size_t vIdLen,
                             PartitionID partId,
                             const VertexID& srcId,
                             EdgeType type,
                             EdgeRanking rank,
                             const VertexID& dstId,
                             EdgeVerPlaceHolder ev = 1);

  static std::string vertexKey(size_t vIdLen,
                               PartitionID partId,
                               const VertexID& vId,
                               char pad = '\0');

  static std::string vertexPrefix(PartitionID partId);

  static std::string systemCommitKey(PartitionID partId);

  static std::string systemPartKey(PartitionID partId);

  static std::string systemBalanceKey(PartitionID partId);

  static std::string kvKey(PartitionID partId, const folly::StringPiece& name);
  static std::string kvPrefix(PartitionID partId);

  /**
   * Prefix for tag
   * */
  static std::string tagPrefix(size_t vIdLen, PartitionID partId, const VertexID& vId, TagID tagId);

  static std::string tagPrefix(size_t vIdLen, PartitionID partId, const VertexID& vId);

  static std::string tagPrefix(PartitionID partId);

  /**
   * Prefix for edge
   * */
  static std::string edgePrefix(size_t vIdLen,
                                PartitionID partId,
                                const VertexID& srcId,
                                EdgeType type);

  static std::string edgePrefix(size_t vIdLen, PartitionID partId, const VertexID& srcId);

  static std::string edgePrefix(size_t vIdLen,
                                PartitionID partId,
                                const VertexID& srcId,
                                EdgeType type,
                                EdgeRanking rank,
                                const VertexID& dstId);

  static std::string edgePrefix(PartitionID partId);

  static std::string systemPrefix();

  static std::vector<std::string> snapshotPrefix(PartitionID partId);

  static PartitionID getPart(const folly::StringPiece& rawKey) {
    return readInt<PartitionID>(rawKey.data(), sizeof(PartitionID)) >> 8;
  }

  static bool isTag(size_t vIdLen, const folly::StringPiece& rawKey) {
    if (rawKey.size() != kTagLen + vIdLen) {
      return false;
    }
    constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
    auto type = readInt<uint32_t>(rawKey.data(), len) & kTypeMask;
    return static_cast<NebulaKeyType>(type) == NebulaKeyType::kTag_;
  }

  static VertexIDSlice getVertexId(size_t vIdLen, const folly::StringPiece& rawKey) {
    if (rawKey.size() != kTagLen + vIdLen) {
      dumpBadKey(rawKey, kTagLen + vIdLen, vIdLen);
    }
    auto offset = sizeof(PartitionID);
    return rawKey.subpiece(offset, vIdLen);
  }

  static TagID getTagId(size_t vIdLen, const folly::StringPiece& rawKey) {
    if (rawKey.size() != kTagLen + vIdLen) {
      dumpBadKey(rawKey, kTagLen + vIdLen, vIdLen);
    }
    auto offset = sizeof(PartitionID) + vIdLen;
    return readInt<TagID>(rawKey.data() + offset, sizeof(TagID));
  }

  static bool isEdge(size_t vIdLen, const folly::StringPiece& rawKey, char suffix = kEdgeVersion) {
    if (rawKey.size() != kEdgeLen + (vIdLen << 1)) {
      return false;
    }
    if (rawKey.back() != suffix) {
      return false;
    }
    constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
    auto type = readInt<uint32_t>(rawKey.data(), len) & kTypeMask;
    return static_cast<NebulaKeyType>(type) == NebulaKeyType::kEdge;
  }

  static bool isVertex(const folly::StringPiece& rawKey) {
    constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
    auto type = readInt<uint32_t>(rawKey.data(), len) & kTypeMask;
    return static_cast<NebulaKeyType>(type) == NebulaKeyType::kVertex;
  }

  static bool isLock(size_t vIdLen, const folly::StringPiece& rawKey) {
    return isEdge(vIdLen, rawKey, kLockVersion);
  }

  static bool isSystem(const folly::StringPiece& rawKey) {
    constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
    auto type = readInt<uint32_t>(rawKey.data(), len) & kTypeMask;
    return static_cast<NebulaKeyType>(type) == NebulaKeyType::kSystem;
  }

  static bool isSystemCommit(const folly::StringPiece& rawKey) {
    if (rawKey.size() != kSystemLen) {
      return false;
    }
    if (!isSystem(rawKey)) {
      return false;
    }
    auto position = rawKey.data() + sizeof(PartitionID);
    auto len = sizeof(NebulaSystemKeyType);
    auto type = readInt<uint32_t>(position, len);
    return static_cast<NebulaSystemKeyType>(type) == NebulaSystemKeyType::kSystemCommit;
  }

  static bool isSystemPart(const folly::StringPiece& rawKey) {
    if (rawKey.size() != kSystemLen) {
      return false;
    }
    if (!isSystem(rawKey)) {
      return false;
    }
    auto position = rawKey.data() + sizeof(PartitionID);
    auto len = sizeof(NebulaSystemKeyType);
    auto type = readInt<uint32_t>(position, len);
    return static_cast<NebulaSystemKeyType>(type) == NebulaSystemKeyType::kSystemPart;
  }

  static bool isSystemBalance(const folly::StringPiece& rawKey) {
    if (rawKey.size() != kSystemLen) {
      return false;
    }
    if (!isSystem(rawKey)) {
      return false;
    }
    auto position = rawKey.data() + sizeof(PartitionID);
    auto len = sizeof(NebulaSystemKeyType);
    auto type = readInt<uint32_t>(position, len);
    return static_cast<NebulaSystemKeyType>(type) == NebulaSystemKeyType::kSystemBalance;
  }

  static VertexIDSlice getSrcId(size_t vIdLen, const folly::StringPiece& rawKey) {
    if (rawKey.size() < kEdgeLen + (vIdLen << 1)) {
      dumpBadKey(rawKey, kEdgeLen + (vIdLen << 1), vIdLen);
    }
    auto offset = sizeof(PartitionID);
    return rawKey.subpiece(offset, vIdLen);
  }

  static VertexIDSlice getDstId(size_t vIdLen, const folly::StringPiece& rawKey) {
    if (rawKey.size() < kEdgeLen + (vIdLen << 1)) {
      dumpBadKey(rawKey, kEdgeLen + (vIdLen << 1), vIdLen);
    }
    auto offset = sizeof(PartitionID) + vIdLen + sizeof(EdgeType) + sizeof(EdgeRanking);
    return rawKey.subpiece(offset, vIdLen);
  }

  static EdgeType getEdgeType(size_t vIdLen, const folly::StringPiece& rawKey) {
    if (rawKey.size() < kEdgeLen + (vIdLen << 1)) {
      dumpBadKey(rawKey, kEdgeLen + (vIdLen << 1), vIdLen);
    }
    auto offset = sizeof(PartitionID) + vIdLen;
    return readInt<EdgeType>(rawKey.data() + offset, sizeof(EdgeType));
  }

  static EdgeRanking getRank(size_t vIdLen, const folly::StringPiece& rawKey) {
    if (rawKey.size() < kEdgeLen + (vIdLen << 1)) {
      dumpBadKey(rawKey, kEdgeLen + (vIdLen << 1), vIdLen);
    }
    auto offset = sizeof(PartitionID) + vIdLen + sizeof(EdgeType);
    return NebulaKeyUtils::decodeRank(rawKey.data() + offset);
  }

  static std::string encodeRank(EdgeRanking rank) {
    rank ^= folly::to<int64_t>(1) << 63;
    auto val = folly::Endian::big(rank);
    std::string raw;
    raw.reserve(sizeof(int64_t));
    raw.append(reinterpret_cast<const char*>(&val), sizeof(int64_t));
    return raw;
  }

  static EdgeRanking decodeRank(const folly::StringPiece& raw) {
    auto val = *reinterpret_cast<const int64_t*>(raw.data());
    val = folly::Endian::big(val);
    val ^= folly::to<int64_t>(1) << 63;
    return val;
  }

  static folly::StringPiece keyWithNoVersion(const folly::StringPiece& rawKey) {
    // TODO(heng) We should change the method if varint data version supported.
    return rawKey.subpiece(0, rawKey.size() - sizeof(EdgeVerPlaceHolder));
  }

  /**
   * @brief gen edge key from lock, this will used at resume
   *        if enableMvcc ver of edge and lock will be same,
   *        else ver of lock should be 0, and ver of edge should be 1
   */
  static std::string toEdgeKey(const folly::StringPiece& lockKey);

  /**
   * @brief gen edge lock from lock
   *        if enableMvcc ver of edge and lock will be same,
   *        else ver of lock should be 0, and ver of edge should be 1
   */
  static std::string toLockKey(const folly::StringPiece& rawKey);

  static EdgeVerPlaceHolder getLockVersion(const folly::StringPiece&) {
    return 0;
  }

  static folly::StringPiece lockWithNoVersion(const folly::StringPiece& rawKey) {
    // TODO(liuyu) We should change the method if varint data version
    // supported.
    return rawKey.subpiece(0, rawKey.size() - 1);
  }

  static void dumpBadKey(const folly::StringPiece& rawKey, size_t expect, size_t vIdLen) {
    std::stringstream msg;
    msg << "rawKey.size() != expect size"
        << ", rawKey.size() = " << rawKey.size() << ", expect = " << expect
        << ", vIdLen = " << vIdLen << ", rawkey hex format:\n";
    msg << folly::hexDump(rawKey.data(), rawKey.size());
    LOG(FATAL) << msg.str();
  }

  static std::string adminTaskKey(int32_t seqId, GraphSpaceID spaceId, JobID jobId, TaskID taskId);

  static bool isAdminTaskKey(const folly::StringPiece& rawKey);

  static std::tuple<int32_t, GraphSpaceID, JobID, TaskID> parseAdminTaskKey(folly::StringPiece key);

  static std::string dataVersionKey();

  static std::string dataVersionValue();

  static_assert(sizeof(NebulaKeyType) == sizeof(PartitionID));

 private:
  NebulaKeyUtils() = delete;

  static constexpr char kLockVersion = 0;
  static constexpr char kEdgeVersion = 1;
};

}  // namespace nebula
#endif  // COMMON_UTILS_NEBULAKEYUTILS_H_
