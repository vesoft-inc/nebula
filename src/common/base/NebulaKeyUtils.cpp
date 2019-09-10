/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/NebulaKeyUtils.h"

namespace nebula {

// static
std::string NebulaKeyUtils::vertexKey(PartitionID partId, VertexID vId,
                                      TagID tagId, TagVersion tv) {
    static int32_t tagMask = 0xBFFFFFFF;
    std::string key;
    key.reserve(kVertexLen);

    tagId &= tagMask;
    int32_t item = (partId << 8) | (NebulaKeyType::kData);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID))
       .append(reinterpret_cast<const char*>(&tv), sizeof(TagVersion));
    return key;
}

// static
std::string NebulaKeyUtils::edgeKey(PartitionID partId,
                                    VertexID srcId,
                                    EdgeType type,
                                    EdgeRanking rank,
                                    VertexID dstId,
                                    EdgeVersion ev) {
    static int32_t edgeMask = 0x40000000;
    std::string key;
    key.reserve(kEdgeLen);
    type |= edgeMask;

    int32_t item = (partId << 8) | (NebulaKeyType::kData);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
       .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
       .append(reinterpret_cast<const char*>(&dstId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&ev), sizeof(EdgeVersion));
    return key;
}

// static
std::string NebulaKeyUtils::vertexPrefix(PartitionID partId, VertexID vId, TagID tagId) {
    static int32_t tagMask = 0xBFFFFFFF;
    std::string key;
    key.reserve(kVertexLen);

    tagId &= tagMask;
    PartitionID item = (partId << 8) | (NebulaKeyType::kData);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID));
    return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(PartitionID partId, VertexID srcId, EdgeType type) {
    static int32_t edgeMask = 0x40000000;
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID) + sizeof(EdgeType));

    type |= edgeMask;
    PartitionID item = (partId << 8) | (NebulaKeyType::kData);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType));
    return key;
}

// static
std::string NebulaKeyUtils::prefix(PartitionID partId) {
    std::string key;
    key.reserve(sizeof(PartitionID));
    PartitionID item = (partId << 8) | (NebulaKeyType::kData);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID));
    return key;
}

// static
std::string NebulaKeyUtils::vertexPrefix(PartitionID partId, VertexID vId) {
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID));
    PartitionID item = (partId << 8) | (NebulaKeyType::kData);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID));
    return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(PartitionID partId, VertexID vId) {
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID));
    PartitionID item = (partId << 8) | (NebulaKeyType::kData);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID));
    return key;
}

// static
std::string NebulaKeyUtils::prefix(PartitionID partId, VertexID src, EdgeType type,
                                   EdgeRanking ranking, VertexID dst) {
    static int32_t edgeMask = 0x40000000;
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID) + sizeof(EdgeType)
                + sizeof(VertexID) + sizeof(EdgeRanking));

    type |= edgeMask;
    PartitionID item = (partId << 8) | (NebulaKeyType::kData);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&src), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
       .append(reinterpret_cast<const char*>(&ranking), sizeof(EdgeRanking))
       .append(reinterpret_cast<const char*>(&dst), sizeof(VertexID));
    return key;
}

}  // namespace nebula

