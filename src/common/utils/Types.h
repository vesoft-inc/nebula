/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_UTILS_TYPES_H_
#define COMMON_UTILS_TYPES_H_

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"

namespace nebula {

enum class NebulaKeyType : uint32_t {
  kTag_ = 0x00000001,
  kEdge = 0x00000002,
  kIndex = 0x00000003,
  kSystem = 0x00000004,
  kOperation = 0x00000005,
  kKeyValue = 0x00000006,
  kVertex = 0x00000007,
  kPrime = 0x00000008,        // used in TOSS, if we write a lock succeed
  kDoublePrime = 0x00000009,  // used in TOSS, if we get RPC back from remote.
  kVector_ = 0x000000F0,      // used for vector data
  kIdVid = 0x000000A0,        // used for id-vid map
  kVidId = 0x000000B0,        // used for vid-id map
};

enum class NebulaSystemKeyType : uint32_t {
  kSystemCommit = 0x00000001,
  kSystemPart = 0x00000002,
  kSystemBalance = 0x00000003,
};

enum class NebulaOperationType : uint32_t {
  kModify = 0x00000001,
  kDelete = 0x00000002,
};

using VertexIDSlice = folly::StringPiece;
using IndexID = int32_t;

template <typename T>
static typename std::enable_if<std::is_integral<T>::value, T>::type readInt(const char* data,
                                                                            int32_t len) {
  CHECK_GE(len, sizeof(T));
  return *reinterpret_cast<const T*>(data);
}

static constexpr int32_t kVectorTagLen = sizeof(PartitionID) + sizeof(TagID) + sizeof(PropID);
static constexpr int32_t kVectorEdgeLen = sizeof(PartitionID) + sizeof(EdgeType) +
                                          sizeof(EdgeRanking) + sizeof(EdgeVerPlaceHolder) +
                                          sizeof(PropID);
// size of tag key except vertexId
static constexpr int32_t kTagLen = sizeof(PartitionID) + sizeof(TagID);

// size of tag key except srcId and dstId
static constexpr int32_t kEdgeLen =
    sizeof(PartitionID) + sizeof(EdgeType) + sizeof(EdgeRanking) + sizeof(EdgeVerPlaceHolder);

static constexpr int32_t kSystemLen = sizeof(PartitionID) + sizeof(NebulaSystemKeyType);

// The partition id offset in 4 Bytes
static constexpr uint8_t kPartitionOffset = 8;

// The key type bits Mask
// See KeyType enum
static constexpr uint32_t kTypeMask = 0x000000FF;
static constexpr uint32_t kTypeMaskWithCF = 0x000000F0;
static constexpr uint32_t kTypeMaskWithoutCF = 0x0000000F;

static constexpr int32_t kTagIndexLen = sizeof(PartitionID) + sizeof(IndexID);

static constexpr int32_t kEdgeIndexLen =
    sizeof(PartitionID) + sizeof(IndexID) + sizeof(EdgeRanking);

}  // namespace nebula
#endif  // COMMON_UTILS_TYPES_H_
