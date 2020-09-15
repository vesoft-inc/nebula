/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTILS_TYPES_H_
#define UTILS_TYPES_H_

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"

namespace nebula {

enum class NebulaKeyType : uint32_t {
    kData              = 0x00000001,
    kIndex             = 0x00000002,
    kUUID              = 0x00000003,
    kSystem            = 0x00000004,
    kOperation         = 0x00000005,
};

enum class NebulaSystemKeyType : uint32_t {
    kSystemCommit      = 0x00000001,
    kSystemPart        = 0x00000002,
};

enum class NebulaOperationType : uint32_t {
    kModify            = 0x00000001,
    kDelete            = 0x00000002,
};

using VertexIDSlice = folly::StringPiece;
using VertexIntID = int64_t;
using IndexID = int32_t;

template<typename T>
static typename std::enable_if<std::is_integral<T>::value, T>::type
readInt(const char* data, int32_t len) {
    CHECK_GE(len, sizeof(T));
    return *reinterpret_cast<const T*>(data);
}

// size of vertex key except vertexId
static constexpr int32_t kVertexLen = sizeof(PartitionID) + sizeof(TagID) + sizeof(TagVersion);

// size of vertex key except srcId and dstId
static constexpr int32_t kEdgeLen = sizeof(PartitionID) + sizeof(EdgeType) +
                                    sizeof(EdgeRanking) + sizeof(EdgeVersion);

static constexpr int32_t kSystemLen = sizeof(PartitionID) + sizeof(NebulaSystemKeyType);

// The partition id offset in 4 Bytes
static constexpr uint8_t kPartitionOffset = 8;

// The key type bits Mask
// See KeyType enum
static constexpr uint32_t kTypeMask     = 0x000000FF;

// The Tag/Edge type bit Mask, the most significant bit is to indicate sign,
// the next bit is to indicate it is tag or edge, 0 for Tag, 1 for Edge
static constexpr uint32_t kTagEdgeMask      = 0x40000000;
// For extract Tag/Edge value
static constexpr uint32_t kTagEdgeValueMask = ~kTagEdgeMask;
// Write edge by &=
static constexpr uint32_t kEdgeMaskSet      = kTagEdgeMask;
// Write Tag by |=
static constexpr uint32_t kTagMaskSet       = ~kTagEdgeMask;

static constexpr int32_t kVertexIndexLen = sizeof(PartitionID) + sizeof(IndexID);

static constexpr int32_t kEdgeIndexLen = sizeof(PartitionID) + sizeof(IndexID) +
                                         sizeof(EdgeRanking);

}  // namespace nebula
#endif  // UTILS_TYPES_H_

