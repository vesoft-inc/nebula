/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_KEYUTILS_H_
#define STORAGE_KEYUTILS_H_

#include "base/Base.h"

namespace nebula {
namespace storage {

/**
 * VertexKeyUtils:
 * partId(4) + vertexId(8) + tagId(4) + version(8)
 *
 * EdgeKeyUtils:
 * partId(4) + srcId(8) + edgeType(4) + dstId(8) + edgeRank(8) + version(8)
 *
 * */


using Vertex = std::tuple<VertexID, TagID>;

using Edge = std::tuple<VertexID, EdgeType, VertexID, EdgeRanking>;

/**
 * This class supply some utils for transition between Vertex/Edge and key in kvstore.
 * */
class KeyUtils final {
public:
    ~KeyUtils() = default;
    /**
     * Generate vertex key for kv store
     * */
    static std::string vertexKey(PartitionID partId, VertexID vId,
                                 TagID tagId, TagVersion ts);

    /**
     * Generate edge key for kv store
     * */
    static std::string edgeKey(PartitionID partId, VertexID srcId,
                               EdgeType type, VertexID dstId,
                               EdgeRanking rank, EdgeVersion ts);

    /**
     * Prefix for srcId edges with some edgeType
     * */
    static std::string prefix(PartitionID partId, VertexID srcId, EdgeType type);

    /**
     * Prefix for some vertexId
     * */
    static std::string prefix(PartitionID partId, VertexID vId);

    static bool isVertex(const std::string& rawKey) {
        return rawKey.size() == kVertexLen;
    }

    static bool isEdge(const std::string& rawKey) {
        return rawKey.size() == kEdgeLen;
    }

    /**
     * Parse vertex from rawKey, return false if failed.
     * */
//    static bool parseVertex(const std::string& rawKey, Vertex& vertex);

    /**
     * Parse edge from rawkey, return false if failed.
     **/
//    static bool parseEdge(const std::string& rawKey, Edge& edge);

private:
    KeyUtils() = delete;

private:
    static constexpr int32_t kVertexLen = sizeof(PartitionID) + sizeof(VertexID)
                                        + sizeof(TagID) + sizeof(TagVersion);
    static constexpr int32_t kEdgeLen = sizeof(PartitionID) + sizeof(VertexID) + sizeof(EdgeType)
                                      + sizeof(VertexID) + sizeof(EdgeRanking) + sizeof(EdgeVersion);
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_KEYUTILS_H_

