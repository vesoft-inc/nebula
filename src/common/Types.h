/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_TYPES_H_
#define COMMON_TYPES_H_

#include "base/Base.h"
#include "thrift/ThriftTypes.h"

namespace nebula {

enum class NebulaKeyType : uint32_t {
    kData              = 0x00000001,
    kIndex             = 0x00000002,
    kUUID              = 0x00000003,
    kSystem            = 0x00000004,
};

enum class NebulaSystemKeyType : uint32_t {
    kSystemCommit      = 0x00000001,
    kSystemPart        = 0x00000002,
};

using VertexIntID = int64_t;
using IndexID = int32_t;

template<typename T>
static typename std::enable_if<std::is_integral<T>::value, T>::type
readInt(const char* data, int32_t len) {
    CHECK_GE(len, sizeof(T));
    return *reinterpret_cast<const T*>(data);
}

static constexpr int32_t kVertexLen = sizeof(PartitionID) + sizeof(VertexIntID)
                                    + sizeof(TagID) + sizeof(TagVersion);

static constexpr int32_t kEdgeLen = sizeof(PartitionID) + sizeof(VertexIntID)
                                  + sizeof(EdgeType) + sizeof(VertexIntID)
                                  + sizeof(EdgeRanking) + sizeof(EdgeVersion);

static constexpr int32_t kSystemLen = sizeof(PartitionID) + sizeof(NebulaSystemKeyType);

// The partition id offset in 4 Bytes
static constexpr uint8_t kPartitionOffset = 8;

// The key type bits Mask
// See KeyType enum
static constexpr uint32_t kTypeMask     = 0x000000FF;

// The Tag/Edge type bit Mask
// 0 for Tag, 1 for Edge
// 0x40 - 0b0100,0000
static constexpr uint32_t kTagEdgeMask      = 0x40000000;
// For extract Tag/Edge value
static constexpr uint32_t kTagEdgeValueMask = ~kTagEdgeMask;
// Write edge by &=
static constexpr uint32_t kEdgeMaskSet      = kTagEdgeMask;
// Write Tag by |=
static constexpr uint32_t kTagMaskSet       = ~kTagEdgeMask;

static constexpr int32_t kVertexIndexLen = sizeof(PartitionID) + sizeof(IndexID)
                                           + sizeof(VertexID);

static constexpr int32_t kEdgeIndexLen = sizeof(PartitionID) + sizeof(IndexID)
                                         + sizeof(VertexID) * 2 + sizeof(EdgeRanking);

static constexpr int32_t kIndexLen = std::min(kVertexIndexLen, kEdgeIndexLen);

}  // namespace nebula
#endif  // COMMON_TYPES_H_

