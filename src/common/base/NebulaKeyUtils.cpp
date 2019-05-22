/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/NebulaKeyUtils.h"

namespace nebula {

// static
std::string NebulaKeyUtils::vertexKey(PartitionID partId, VertexID vId,
                                      TagID tagId, TagVersion ts) {
    std::string key;
    key.reserve(kVertexLen);
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID))
       .append(reinterpret_cast<const char*>(&ts), sizeof(TagVersion));
    return key;
}

// static
std::string NebulaKeyUtils::edgeKey(PartitionID partId,
                                    VertexID srcId,
                                    EdgeType type,
                                    EdgeRanking rank,
                                    VertexID dstId,
                                    EdgeVersion ts) {
    std::string key;
    key.reserve(kEdgeLen);
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
       .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
       .append(reinterpret_cast<const char*>(&dstId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&ts), sizeof(EdgeVersion));
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

}  // namespace nebula

