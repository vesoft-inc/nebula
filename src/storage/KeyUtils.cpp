/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "storage/KeyUtils.h"

namespace nebula {
namespace storage {

// static
std::string KeyUtils::vertexKey(PartitionID partId, VertexID vId,
                                TagID tagId, Timestamp ts) {
    std::string key;
    key.reserve(kVertexLen);
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID))
       .append(reinterpret_cast<const char*>(&ts), sizeof(Timestamp));
    return key;
}

// static
std::string KeyUtils::edgeKey(PartitionID partId, VertexID srcId,
                              VertexID dstId, EdgeType type,
                              EdgeRank rank, Timestamp ts) {
    std::string key;
    key.reserve(kEdgeLen);
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
       .append(reinterpret_cast<const char*>(&dstId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRank))
       .append(reinterpret_cast<const char*>(&ts), sizeof(Timestamp));
    return key;
}

// static
std::string KeyUtils::prefix(PartitionID partId, VertexID srcId, EdgeType type) {
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID) + sizeof(EdgeType));
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&srcId), sizeof(VertexID))
       .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType));
    return key;
}

// static
std::string KeyUtils::prefix(PartitionID partId, VertexID vId) {
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(VertexID));
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&vId), sizeof(VertexID));
    return key;
}

}  // namespace storage
}  // namespace nebula

