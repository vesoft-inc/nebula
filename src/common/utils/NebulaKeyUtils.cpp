/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "utils/NebulaKeyUtils.h"

namespace nebula {

// static
std::string NebulaKeyUtils::vertexKey(PartitionID partId, VertexID vId,
                                      TagID tagId, TagVersion tv) {
    tagId &= kTagMaskSet;
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);

    std::string key;
    key.reserve(kVertexLen);
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
    type |= kEdgeMaskSet;
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);

    std::string key;
    key.reserve(kEdgeLen);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
       .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
       .append(reinterpret_cast<const char*>(&dstId), sizeof(VertexID))
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
void NebulaKeyUtils::indexRaw(const IndexValues &values, std::string& raw) {
    std::vector<int32_t> colsLen;
    for (auto& col : values) {
        if (col.first == cpp2::SupportedType::STRING) {
            colsLen.emplace_back(col.second.size());
        }
        raw.append(col.second.data(), col.second.size());
    }
    for (auto len : colsLen) {
        raw.append(reinterpret_cast<const char*>(&len), sizeof(int32_t));
    }
}

// static
std::string NebulaKeyUtils::vertexIndexKey(PartitionID partId, IndexID indexId, VertexID vId,
                                           const IndexValues& values) {
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
    std::string key;
    key.reserve(256);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
    indexRaw(values, key);
    key.append(reinterpret_cast<const char*>(&vId), sizeof(VertexID));
    return key;
}

// static
std::string NebulaKeyUtils::edgeIndexKey(PartitionID partId, IndexID indexId,
                                         VertexID srcId, EdgeRanking rank,
                                         VertexID dstId, const IndexValues& values) {
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
    std::string key;
    key.reserve(256);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
    indexRaw(values, key);
    key.append(reinterpret_cast<const char*>(&srcId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
       .append(reinterpret_cast<const char*>(&dstId), sizeof(VertexID));
    return key;
}

// static
std::string NebulaKeyUtils::indexPrefix(PartitionID partId, IndexID indexId) {
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(IndexID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
    return key;
}

// static
std::string NebulaKeyUtils::vertexPrefix(PartitionID partId, VertexID vId, TagID tagId) {
    tagId &= kTagMaskSet;
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);

    std::string key;
    key.reserve(kVertexLen);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID));
    return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(PartitionID partId, VertexID srcId, EdgeType type) {
    type |= kEdgeMaskSet;
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);

    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID) + sizeof(EdgeType));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType));
    return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(PartitionID partId,
                                       VertexID srcId,
                                       EdgeType type,
                                       EdgeRanking rank,
                                       VertexID dstId) {
    type |= kEdgeMaskSet;
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID)
                + sizeof(EdgeType) + sizeof(VertexID)
                + sizeof(EdgeRanking));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
            .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexID))
            .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
            .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
            .append(reinterpret_cast<const char*>(&dstId), sizeof(VertexID));
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
std::string NebulaKeyUtils::snapshotPrefix(PartitionID partId) {
    // snapshot of meta would be all key-value pairs
    if (partId == 0) {
        return "";
    }
    return prefix(partId);
}

// static
std::string NebulaKeyUtils::vertexPrefix(PartitionID partId, VertexID vId) {
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID));
    return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(PartitionID partId, VertexID vId) {
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID));
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
std::string NebulaKeyUtils::prefix(PartitionID partId, VertexID src, EdgeType type,
                                   EdgeRanking ranking, VertexID dst) {
    type |= kEdgeMaskSet;
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kData);

    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID) + sizeof(EdgeType)
                + sizeof(VertexID) + sizeof(EdgeRanking));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&src), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
       .append(reinterpret_cast<const char*>(&ranking), sizeof(EdgeRanking))
       .append(reinterpret_cast<const char*>(&dst), sizeof(VertexID));
    return key;
}

}  // namespace nebula

