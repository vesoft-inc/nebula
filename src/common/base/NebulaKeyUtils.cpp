/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/NebulaKeyUtils.h"

namespace nebula {

const std::string kIndexPrefix = "_i";           // NOLINT

// static
std::string NebulaKeyUtils::vertexKey(PartitionID partId, VertexID vId,
                                      TagID tagId, TagVersion tv) {
    constexpr uint32_t tagMask = 0xBFFFFFFF;
    tagId &= tagMask;
    int32_t item = (partId << 8) | (NebulaKeyType::kData);

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
    constexpr uint32_t edgeMask = 0x40000000;
    type |= edgeMask;
    int32_t item = (partId << 8) | (NebulaKeyType::kData);

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
    int32_t item = (partId << 8) | (NebulaKeyType::kSystem);
    uint32_t type = NebulaSystemKeyType::kSystemCommit;
    std::string key;
    key.reserve(kSystemLen);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&type), sizeof(NebulaSystemKeyType));
    return key;
}

// static
std::string NebulaKeyUtils::systemPartKey(PartitionID partId) {
    uint32_t item = (partId << 8) | (NebulaKeyType::kSystem);
    uint32_t type = NebulaSystemKeyType::kSystemPart;
    std::string key;
    key.reserve(kSystemLen);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&type), sizeof(NebulaSystemKeyType));
    return key;
}

// static
std::string NebulaKeyUtils::vertexPrefix(PartitionID partId, VertexID vId, TagID tagId) {
    constexpr uint32_t tagMask = 0xBFFFFFFF;
    tagId &= tagMask;
    PartitionID item = (partId << 8) | (NebulaKeyType::kData);

    std::string key;
    key.reserve(kVertexLen);
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID));
    return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(PartitionID partId, VertexID srcId, EdgeType type) {
    constexpr uint32_t edgeMask = 0x40000000;
    type |= edgeMask;
    PartitionID item = (partId << 8) | (NebulaKeyType::kData);

    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID) + sizeof(EdgeType));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType));
    return key;
}

// static
std::string NebulaKeyUtils::prefix(PartitionID partId) {
    PartitionID item = (partId << 8) | (NebulaKeyType::kData);
    std::string key;
    key.reserve(sizeof(PartitionID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID));
    return key;
}

// static
std::string NebulaKeyUtils::vertexPrefix(PartitionID partId, VertexID vId) {
    PartitionID item = (partId << 8) | (NebulaKeyType::kData);
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID));
    return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(PartitionID partId, VertexID vId) {
    PartitionID item = (partId << 8) | (NebulaKeyType::kData);
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID));
    return key;
}

// static
std::string NebulaKeyUtils::systemPrefix() {
    int8_t type = NebulaKeyType::kSystem;
    std::string key;
    key.reserve(sizeof(int8_t));
    key.append(reinterpret_cast<const char*>(&type), sizeof(int8_t));
    return key;
}

// static
std::string NebulaKeyUtils::prefix(PartitionID partId, VertexID src, EdgeType type,
                                   EdgeRanking ranking, VertexID dst) {
    constexpr uint32_t edgeMask = 0x40000000;
    type |= edgeMask;
    PartitionID item = (partId << 8) | (NebulaKeyType::kData);

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

// static
std::string NebulaKeyUtils::tagIndexkey(PartitionID partId, IndexID indexId,
                                        VertexID vId, TagVersion ts,
                                        const folly::StringPiece &values) {
    std::string key;
    key.reserve(sizeof(PartitionID) + kIndexPrefix.size() + sizeof(IndexID) + values.size() +
                sizeof(VertexID) + sizeof(TagVersion));
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
       .append(kIndexPrefix)
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID))
       .append(values.data())
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&ts), sizeof(TagVersion));
    return key;
}

// static
std::string NebulaKeyUtils::edgeIndexkey(PartitionID partId, IndexID indexId, EdgeType type,
                                         EdgeVersion ts, const folly::StringPiece &values) {
    std::string key;
    key.reserve(sizeof(PartitionID) + kIndexPrefix.size() + sizeof(IndexID) + values.size() +
                sizeof(EdgeType) + sizeof(EdgeVersion));
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
       .append(kIndexPrefix)
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID))
       .append(values.data())
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
       .append(reinterpret_cast<const char*>(&ts), sizeof(EdgeVersion));
    return key;
}

// static
std::string NebulaKeyUtils::partPrefix(PartitionID partId) {
        std::string key;
        key.reserve(64);
        key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
        return key;
}

// static
std::string NebulaKeyUtils::indexValue(const folly::StringPiece &row) {
    std::string key;
    key.reserve(row.size());
    key.append(row.data());
    return key;
}

// static
std::string NebulaKeyUtils::indexPrefix(PartitionID partId, IndexID indexId) {
    std::string key;
    key.reserve(sizeof(PartitionID) + kIndexPrefix.size() + sizeof(IndexID));

    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
       .append(kIndexPrefix)
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
    return key;
}

// static
TagID NebulaKeyUtils::parseTagId(const folly::StringPiece &key) {
    auto offset = sizeof(PartitionID) + sizeof(VertexID);
    return *reinterpret_cast<const TagID*>(key.begin() + offset);
}

// static
VertexID NebulaKeyUtils::parseVertexId(const folly::StringPiece &key) {
    auto offset = sizeof(PartitionID);
    return *reinterpret_cast<const TagVersion*>(key.begin() + offset);
}

// static
TagVersion NebulaKeyUtils::parseTagVersion(const folly::StringPiece &key) {
    auto offset = sizeof(PartitionID) + sizeof(VertexID) + sizeof(TagID);
    return *reinterpret_cast<const TagVersion*>(key.begin() + offset);
}

// static
EdgeType NebulaKeyUtils::parseEdgeType(const folly::StringPiece &key) {
    auto offset = sizeof(PartitionID) + sizeof(VertexID);
    return *reinterpret_cast<const EdgeType*>(key.begin() + offset);
}

// static
EdgeVersion NebulaKeyUtils::parseEdgeVersion(const folly::StringPiece &key) {
        auto offset = sizeof(PartitionID) + sizeof(VertexID) + sizeof(EdgeType) +
                      sizeof(EdgeRanking) + sizeof(VertexID);
        return *reinterpret_cast<const EdgeVersion*>(key.begin() + offset);
}

}  // namespace nebula

