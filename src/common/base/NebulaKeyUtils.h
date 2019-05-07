/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_NEBULAKEYUTILS_H_
#define COMMON_BASE_NEBULAKEYUTILS_H_

#include "base/Base.h"

namespace nebula {

/**
 * VertexKeyUtils:
 * partId(4) + vertexId(8) + tagId(4) + version(8)
 *
 * EdgeKeyUtils:
 * partId(4) + srcId(8) + edgeType(4) + edgeRank(8) + dstId(8) + version(8)
 *
 * */


using Vertex = std::tuple<VertexID, TagID>;

using Edge = std::tuple<VertexID, EdgeType, VertexID, EdgeRanking>;

/**
 * This class supply some utils for transition between Vertex/Edge and key in kvstore.
 * */
class NebulaKeyUtils final {
public:
    ~NebulaKeyUtils() = default;
    /**
     * Generate vertex key for kv store
     * */
    static std::string vertexKey(PartitionID partId, VertexID vId,
                                 TagID tagId, TagVersion ts);

    /**
     * Generate edge key for kv store
     * */
    static std::string edgeKey(PartitionID partId, VertexID srcId,
                               EdgeType type, EdgeRanking rank,
                               VertexID dstId, EdgeVersion ts);

    /**
     * Prefix for srcId edges with some edgeType
     * */
    static std::string prefix(PartitionID partId, VertexID srcId, EdgeType type);

    /**
     * Prefix for some vertexId
     * */
    static std::string prefix(PartitionID partId, VertexID vId);

    static std::string prefix(PartitionID partId, VertexID src, EdgeType type,
                              EdgeRanking ranking, VertexID dst);

    static bool isVertex(const std::string& rawKey) {
        return rawKey.size() == kVertexLen;
    }

    static int32_t getTagId(folly::StringPiece rawKey) {
        CHECK_EQ(rawKey.size(), kVertexLen);
        auto offset = sizeof(PartitionID) + sizeof(VertexID);
        return readInt<int32_t>(rawKey.data() + offset, sizeof(TagID));
    }

    static bool isEdge(const std::string& rawKey) {
        return rawKey.size() == kEdgeLen;
    }

    static int64_t getSrcId(folly::StringPiece rawKey) {
        CHECK_EQ(rawKey.size(), kEdgeLen);
        return readInt<int64_t>(rawKey.data() + sizeof(PartitionID),
                                rawKey.size() - sizeof(PartitionID));
    }

    static int64_t getDstId(folly::StringPiece rawKey) {
        CHECK_EQ(rawKey.size(), kEdgeLen);
        auto offset = kEdgeLen - sizeof(EdgeVersion) - sizeof(VertexID);
        return readInt<int64_t>(rawKey.data() + offset,
                                rawKey.size() - offset);
    }

    static int32_t getEdgeType(folly::StringPiece rawKey) {
        CHECK_EQ(rawKey.size(), kEdgeLen);
        auto offset = sizeof(PartitionID) + sizeof(VertexID);
        return readInt<int32_t>(rawKey.data() + offset,
                                rawKey.size() - offset);
    }

    static int64_t getRank(folly::StringPiece rawKey) {
        CHECK_EQ(rawKey.size(), kEdgeLen);
        auto offset = sizeof(PartitionID) + sizeof(VertexID) + sizeof(EdgeType);
        return readInt<int64_t>(rawKey.data() + offset,
                                rawKey.size() - offset);
    }

    template<typename T>
    static typename std::enable_if<std::is_integral<T>::value, T>::type
    readInt(const char* data, int32_t len) {
        CHECK_GE(len, sizeof(T));
        return *reinterpret_cast<const T*>(data);
    }

private:
    NebulaKeyUtils() = delete;

private:
    static constexpr int32_t kVertexLen = sizeof(PartitionID) + sizeof(VertexID)
                                        + sizeof(TagID) + sizeof(TagVersion);
    static constexpr int32_t kEdgeLen = sizeof(PartitionID) + sizeof(VertexID)
                                      + sizeof(EdgeType) + sizeof(VertexID)
                                      + sizeof(EdgeRanking) + sizeof(EdgeVersion);
};

}  // namespace nebula
#endif  // COMMON_BASE_NEBULAKEYUTILS_H_

