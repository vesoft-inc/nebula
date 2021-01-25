/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "tools/db-upgrade/NebulaKeyUtilsV1.h"

namespace nebula {

// static
std::string NebulaKeyUtilsV1::indexPrefix(PartitionID partId, IndexID indexId) {
    PartitionID item =
        (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyTypeV1::kIndex);
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(IndexID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
    return key;
}

// static
std::string NebulaKeyUtilsV1::vertexPrefix(PartitionID partId, VertexID vId, TagID tagId) {
    tagId &= kTagMaskSet;
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyTypeV1::kData);

    std::string key;
    key.reserve(kVertexLen);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID));
    return key;
}

// static
std::string NebulaKeyUtilsV1::edgePrefix(PartitionID partId, VertexID srcId, EdgeType type) {
    type |= kEdgeMaskSet;
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyTypeV1::kData);

    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID) + sizeof(EdgeType));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType));
    return key;
}

// static
std::string NebulaKeyUtilsV1::edgePrefix(PartitionID partId,
                                         VertexID srcId,
                                         EdgeType type,
                                         EdgeRanking rank,
                                         VertexID dstId) {
    type |= kEdgeMaskSet;
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyTypeV1::kData);
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID) + sizeof(EdgeType) + sizeof(VertexID) +
                sizeof(EdgeRanking));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
        .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexID))
        .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
        .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
        .append(reinterpret_cast<const char*>(&dstId), sizeof(VertexID));
    return key;
}

// static
std::string NebulaKeyUtilsV1::prefix(PartitionID partId) {
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyTypeV1::kData);
    std::string key;
    key.reserve(sizeof(PartitionID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID));
    return key;
}

// static
std::string NebulaKeyUtilsV1::snapshotPrefix(PartitionID partId) {
    // snapshot of meta would be all key-value pairs
    if (partId == 0) {
        return "";
    }
    return prefix(partId);
}

// static
std::string NebulaKeyUtilsV1::vertexPrefix(PartitionID partId, VertexID vId) {
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyTypeV1::kData);
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID));
    return key;
}

// static
std::string NebulaKeyUtilsV1::edgePrefix(PartitionID partId, VertexID vId) {
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyTypeV1::kData);
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID));
    return key;
}

// static
std::string NebulaKeyUtilsV1::systemPrefix() {
    int8_t type = static_cast<uint32_t>(NebulaKeyTypeV1::kSystem);
    std::string key;
    key.reserve(sizeof(int8_t));
    key.append(reinterpret_cast<const char*>(&type), sizeof(int8_t));
    return key;
}

}  // namespace nebula
