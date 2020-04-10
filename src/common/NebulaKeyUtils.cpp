/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/NebulaKeyUtils.h"

namespace nebula {

// static
std::string NebulaKeyUtils::vertexKey(PartitionID partId, VertexIntID vId,
                                      TagID tagId, TagVersion tv) {
    tagId &= kTagMaskSet;
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);

    std::string key;
    key.reserve(kVertexLen);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexIntID))
       .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID))
       .append(reinterpret_cast<const char*>(&tv), sizeof(TagVersion));
    return key;
}

// static
std::string NebulaKeyUtils::edgeKey(PartitionID partId,
                                    VertexIntID srcId,
                                    EdgeType type,
                                    EdgeRanking rank,
                                    VertexIntID dstId,
                                    EdgeVersion ev) {
    type |= kEdgeMaskSet;
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);

    std::string key;
    key.reserve(kEdgeLen);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexIntID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
       .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
       .append(reinterpret_cast<const char*>(&dstId), sizeof(VertexIntID))
       .append(reinterpret_cast<const char*>(&ev), sizeof(EdgeVersion));
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
std::string NebulaKeyUtils::uuidKey(PartitionID partId, const folly::StringPiece& name) {
    std::string key;
    key.reserve(sizeof(PartitionID) + name.size());
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kUUID);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(name.data(), name.size());
    return key;
}

// static
std::string NebulaKeyUtils::kvKey(PartitionID partId, const folly::StringPiece& name) {
    std::string key;
    key.reserve(sizeof(PartitionID) + name.size());
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(name.data(), name.size());
    return key;
}

// static
std::string NebulaKeyUtils::vertexPrefix(PartitionID partId, VertexIntID vId, TagID tagId) {
    tagId &= kTagMaskSet;
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);

    std::string key;
    key.reserve(kVertexLen);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexIntID))
       .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID));
    return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(PartitionID partId, VertexIntID srcId, EdgeType type) {
    type |= kEdgeMaskSet;
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);

    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexIntID) + sizeof(EdgeType));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexIntID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType));
    return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(PartitionID partId,
                                       VertexIntID srcId,
                                       EdgeType type,
                                       EdgeRanking rank,
                                       VertexIntID dstId) {
    type |= kEdgeMaskSet;
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexIntID)
                + sizeof(EdgeType) + sizeof(VertexIntID)
                + sizeof(EdgeRanking));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
            .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexIntID))
            .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
            .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
            .append(reinterpret_cast<const char*>(&dstId), sizeof(VertexIntID));
    return key;
}

// static
std::string NebulaKeyUtils::prefix(PartitionID partId) {
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);
    std::string key;
    key.reserve(sizeof(PartitionID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID));
    return key;
}

// static
std::string NebulaKeyUtils::vertexPrefix(PartitionID partId, VertexIntID vId) {
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexIntID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexIntID));
    return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(PartitionID partId, VertexIntID vId) {
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexIntID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexIntID));
    return key;
}

// static
std::string NebulaKeyUtils::systemPrefix() {
    int8_t type = static_cast<uint32_t>(NebulaKeyType::kSystem);
    std::string key;
    key.reserve(sizeof(int8_t));
    key.append(reinterpret_cast<const char*>(&type), sizeof(int8_t));
    return key;
}

// static
std::string NebulaKeyUtils::prefix(PartitionID partId, VertexIntID src, EdgeType type,
                                   EdgeRanking ranking, VertexIntID dst) {
    type |= kEdgeMaskSet;
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);

    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexIntID) + sizeof(EdgeType)
                + sizeof(VertexIntID) + sizeof(EdgeRanking));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&src), sizeof(VertexIntID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
       .append(reinterpret_cast<const char*>(&ranking), sizeof(EdgeRanking))
       .append(reinterpret_cast<const char*>(&dst), sizeof(VertexIntID));
    return key;
}

}  // namespace nebula

