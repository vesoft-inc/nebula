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
    std::string key;
    key.reserve(kVertexLen);
    for (int32_t n = 0; n < 3; n++) {
        char item = (partId >> (8 * n)) & 0xff;
        key.append(sizeof(char), item);
    }
    key.append(reinterpret_cast<const char*>(&vId), sizeof(VertexID))
       .append(sizeof(char), NebulaKeyType::kVertex)
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
    for (int32_t n = 0; n < 3; n++) {
        char item = (partId >> (8 * n)) & 0xff;
        key.append(sizeof(char), item);
    }
    key.append(reinterpret_cast<const char*>(&srcId), sizeof(VertexID))
       .append(sizeof(char), NebulaKeyType::kEdge)
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
       .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
       .append(reinterpret_cast<const char*>(&dstId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&ev), sizeof(EdgeVersion));
    return key;
}

// static
std::string NebulaKeyUtils::vertexPrefix(PartitionID partId, VertexID vId, TagID tagId) {
    static const size_t n = 1;
    std::string key;
    key.reserve(kVertexLen);
    for (int32_t n = 0; n < 3; n++) {
        char item = (partId >> (8 * n)) & 0xff;
        key.append(sizeof(char), item);
    }
    key.append(n, NebulaKeyType::kVertex)
       .append(reinterpret_cast<const char*>(&partId),
               sizeof(PartitionID) - sizeof(NebulaKeyType::kVertex))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID));
    return key;
}

// static
std::string NebulaKeyUtils::edgePrefix(PartitionID partId, VertexID srcId, EdgeType type) {
    static const size_t n = 1;
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID) + sizeof(EdgeType));
    key.append(n, NebulaKeyType::kEdge)
       .append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID) - sizeof(NebulaKeyType))
       .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType));
    return key;
}

// static
std::string NebulaKeyUtils::prefix(PartitionID partId) {
    static const size_t n = 1;
    std::string key;
    key.reserve(sizeof(PartitionID));
    key.append(n, NebulaKeyType::kVertex)
       .append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
    return key;
}

// static
std::string NebulaKeyUtils::prefix(PartitionID partId, VertexID vId) {
    static const size_t n = 1;
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID));
    key.append(n, NebulaKeyType::kVertex)
       .append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID) - sizeof(NebulaKeyType))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID));
    return key;
}

// static
std::string NebulaKeyUtils::prefix(PartitionID partId, VertexID src, EdgeType type,
                                   EdgeRanking ranking, VertexID dst) {
    static const size_t n = 1;
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID) + sizeof(EdgeType)
                + sizeof(VertexID) + sizeof(EdgeRanking));
    key.append(n, NebulaKeyType::kEdge)
       .append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID) - sizeof(NebulaKeyType))
       .append(reinterpret_cast<const char*>(&src), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
       .append(reinterpret_cast<const char*>(&ranking), sizeof(EdgeRanking))
       .append(reinterpret_cast<const char*>(&dst), sizeof(VertexID));
    return key;
}

}  // namespace nebula

