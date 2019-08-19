/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/NebulaKeyUtils.h"

namespace nebula {

const std::string kIndexPrefix = "_i";           // NOLINT
const std::string kIndexLogPrefix = "_wal_i";    // NOLINT

// static
std::string NebulaKeyUtils::vertexKey(PartitionID partId, VertexID vId,
                                      TagID tagId, TagVersion tv) {
    std::string key;
    key.reserve(kVertexLen);
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
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
    std::string key;
    key.reserve(kEdgeLen);
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
       .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
       .append(reinterpret_cast<const char*>(&dstId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&ev), sizeof(EdgeVersion));
    return key;
}

// static
std::string NebulaKeyUtils::prefix(PartitionID partId) {
    std::string key;
    key.reserve(sizeof(PartitionID));
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
    return key;
}

// static
std::string NebulaKeyUtils::prefix(PartitionID partId, VertexID srcId, EdgeType type) {
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID) + sizeof(EdgeType));
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType));
    return key;
}

// static
std::string NebulaKeyUtils::prefix(PartitionID partId, VertexID vId) {
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID));
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID));
    return key;
}

// static
std::string NebulaKeyUtils::prefix(PartitionID partId, VertexID src, EdgeType type,
                                   EdgeRanking ranking, VertexID dst) {
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID) + sizeof(EdgeType)
                + sizeof(VertexID) + sizeof(EdgeRanking));
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
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
std::string NebulaKeyUtils::preIndexKey(PartitionID partId, IndexID indexId, int64_t version) {
    std::string key;
    key.reserve(sizeof(PartitionID) + kIndexLogPrefix.size() + sizeof(IndexID) + sizeof(int64_t));
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
       .append(kIndexLogPrefix)
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID))
       .append(reinterpret_cast<const char*>(&version), sizeof(int64_t));
    return key;
}

// static
std::string NebulaKeyUtils::preIndexPrefix(PartitionID partId, IndexID indexId) {
        std::string key;
        key.reserve(sizeof(PartitionID) + kIndexLogPrefix.size() + sizeof(IndexID));
        key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
                .append(kIndexLogPrefix)
                .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
        return key;
}

// static
std::string NebulaKeyUtils::preIndexValue(nebula::cpp2::IndexType type,
                                          nebula::cpp2::IndexOpType op,
                                          std::pair<std::string, std::string> original,
                                          std::pair<std::string, std::string> newly) {
    std::string val;
    auto origKey = original.first;
    auto origVal = original.second;
    auto newlyKey = newly.first;
    auto newlyVal = newly.second;
    auto origKeyLen = origKey.size(), origValLen = origVal.size(),
                      newlyKeyLen = newlyKey.size(), newlyValLen = newlyVal.size();

    auto len = (op == nebula::cpp2::IndexOpType::UPDATE) ?
               sizeof(nebula::cpp2::IndexType) + sizeof(nebula::cpp2::IndexOpType) +
               sizeof(int32_t) * 4 + origKeyLen + origValLen +
               newlyKeyLen + newlyValLen
               :
               sizeof(nebula::cpp2::IndexType) + sizeof(nebula::cpp2::IndexOpType) +
               sizeof(int32_t) * 2 + origKeyLen + origValLen;
    val.reserve(len);
    switch (op) {
        case nebula::cpp2::IndexOpType::UPDATE :
        {
            val.append(reinterpret_cast<const char*>(&type), sizeof(nebula::cpp2::IndexType))
               .append(reinterpret_cast<const char*>(&op), sizeof(nebula::cpp2::IndexOpType))
               .append(reinterpret_cast<const char*>(&origKeyLen), sizeof(int32_t))
               .append(origKey)
               .append(reinterpret_cast<const char*>(&origValLen), sizeof(int32_t))
               .append(origVal)
               .append(reinterpret_cast<const char*>(&newlyKeyLen), sizeof(int32_t))
               .append(newlyKey)
               .append(reinterpret_cast<const char*>(&newlyValLen), sizeof(int32_t))
               .append(newlyVal);
            break;
        }
        case nebula::cpp2::IndexOpType::INSERT :
        case nebula::cpp2::IndexOpType::DELETE :
        {
            val.append(reinterpret_cast<const char*>(&type), sizeof(nebula::cpp2::IndexType))
               .append(reinterpret_cast<const char*>(&op), sizeof(nebula::cpp2::IndexOpType))
               .append(reinterpret_cast<const char*>(&origKeyLen), sizeof(int32_t))
               .append(origKey)
               .append(reinterpret_cast<const char*>(&origValLen), sizeof(int32_t))
               .append(origVal);
            break;
        }
    }
    return val;
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

