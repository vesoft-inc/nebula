/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_NEBULAKEYUTILS_H_
#define COMMON_BASE_NEBULAKEYUTILS_H_

#include "common/Types.h"

namespace nebula {

/**
 * VertexKeyUtils:
 * type(1) + partId(3) + vertexId(8) + tagId(4) + version(8)
 *
 * EdgeKeyUtils:
 * type(1) + partId(3) + srcId(8) + edgeType(4) + edgeRank(8) + dstId(8) + version(8)
 *
 * */

/**
 * This class supply some utils for transition between Vertex/Edge and key in kvstore.
 * */
class NebulaKeyUtils final {
public:
    ~NebulaKeyUtils() = default;
    /**
     * Generate vertex key for kv store
     * */
    static std::string vertexKey(PartitionID partId, VertexIntID vId,
                                 TagID tagId, TagVersion tv);

    /**
     * Generate edge key for kv store
     * */
    static std::string edgeKey(PartitionID partId, VertexIntID srcId,
                               EdgeType type, EdgeRanking rank,
                               VertexIntID dstId, EdgeVersion ev);

    static std::string systemCommitKey(PartitionID partId);

    static std::string systemPartKey(PartitionID partId);

    static std::string uuidKey(PartitionID partId, const folly::StringPiece& name);

    static std::string kvKey(PartitionID partId, const folly::StringPiece& name);



    /**
     * Prefix for
     * */
    static std::string vertexPrefix(PartitionID partId, VertexIntID vId, TagID tagId);

    /**
     * Prefix for srcId edges with some edgeType
     * */
    static std::string edgePrefix(PartitionID partId, VertexIntID srcId, EdgeType type);

    static std::string vertexPrefix(PartitionID partId, VertexIntID vId);

    static std::string edgePrefix(PartitionID partId, VertexIntID vId);

    static std::string edgePrefix(PartitionID partId,
                                  VertexIntID srcId,
                                  EdgeType type,
                                  EdgeRanking rank,
                                  VertexIntID dstId);

    static std::string systemPrefix();

    static std::string prefix(PartitionID partId, VertexIntID src, EdgeType type,
                              EdgeRanking ranking, VertexIntID dst);

    static std::string prefix(PartitionID partId);

    static PartitionID getPart(const folly::StringPiece& rawKey) {
        return readInt<PartitionID>(rawKey.data(), sizeof(PartitionID)) >> 8;
    }

    static bool isVertex(const folly::StringPiece& rawKey) {
        if (rawKey.size() != kVertexLen) {
            return false;
        }
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
        auto type = readInt<uint32_t>(rawKey.data(), len) & kTypeMask;
        if (static_cast<uint32_t>(NebulaKeyType::kData) != type) {
            return false;
        }
        auto offset = sizeof(PartitionID) + sizeof(VertexIntID);
        TagID tagId =  readInt<TagID>(rawKey.data() + offset, sizeof(TagID));
        return !(tagId & kTagEdgeMask);
    }

    static VertexIntID getVertexId(const folly::StringPiece& rawKey) {
        CHECK_EQ(rawKey.size(), kVertexLen);
        auto offset = sizeof(PartitionID);
        return readInt<VertexIntID>(rawKey.data() + offset, sizeof(VertexIntID));
    }

    static TagID getTagId(const folly::StringPiece& rawKey) {
        // CHECK_EQ(rawKey.size(), kVertexLen);
        if (rawKey.size() != kVertexLen) {
            std::stringstream msg;
            msg << " rawKey.size() != kVertexLen."
                << "\nrawKey.size()=" << rawKey.size()
                << "\nkVertexLen=" << kVertexLen
                << "\nrawkey string format=" << rawKey
                << "\nrawkey dec format:";
            for (auto i = 0U; i != rawKey.size(); ++i) {
                msg << "\nrawKey[" << i << "]=" << static_cast<int>(rawKey[i]);
            }
            LOG(FATAL) << msg.str();
        }
        auto offset = sizeof(PartitionID) + sizeof(VertexIntID);
        return readInt<TagID>(rawKey.data() + offset, sizeof(TagID));
    }

    static bool isEdge(const folly::StringPiece& rawKey) {
        if (rawKey.size() != kEdgeLen) {
            return false;
        }
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
        auto type = readInt<uint32_t>(rawKey.data(), len) & kTypeMask;
        if (static_cast<uint32_t>(NebulaKeyType::kData) != type) {
            return false;
        }
        auto offset = sizeof(PartitionID) + sizeof(VertexIntID);
        EdgeType etype = readInt<EdgeType>(rawKey.data() + offset, sizeof(EdgeType));
        return etype & kTagEdgeMask;
    }

    static bool isSystemCommit(const folly::StringPiece& rawKey) {
        if (rawKey.size() != kSystemLen) {
            return false;
        }
        auto position = rawKey.data() + sizeof(PartitionID);
        auto len = sizeof(NebulaSystemKeyType);
        auto type = readInt<uint32_t>(position, len);
        return static_cast<uint32_t>(NebulaSystemKeyType::kSystemCommit) == type;
    }

    static bool isSystemPart(const folly::StringPiece& rawKey) {
        if (rawKey.size() != kSystemLen) {
            return false;
        }
        auto position = rawKey.data() + sizeof(PartitionID);
        auto len = sizeof(NebulaSystemKeyType);
        auto type = readInt<uint32_t>(position, len);
        return static_cast<uint32_t>(NebulaSystemKeyType::kSystemPart) == type;
    }

    static VertexIntID getSrcId(const folly::StringPiece& rawKey) {
        CHECK_EQ(rawKey.size(), kEdgeLen);
        return readInt<VertexIntID>(rawKey.data() + sizeof(PartitionID), sizeof(VertexIntID));
    }

    static VertexIntID getDstId(const folly::StringPiece& rawKey) {
        CHECK_EQ(rawKey.size(), kEdgeLen);
        auto offset = sizeof(PartitionID) + sizeof(VertexIntID) +
                      sizeof(EdgeType) + sizeof(EdgeRanking);
        return readInt<VertexIntID>(rawKey.data() + offset, sizeof(VertexIntID));
    }

    static EdgeType getEdgeType(const folly::StringPiece& rawKey) {
        CHECK_EQ(rawKey.size(), kEdgeLen);
        auto offset = sizeof(PartitionID) + sizeof(VertexIntID);
        EdgeType type = readInt<EdgeType>(rawKey.data() + offset, sizeof(EdgeType));
        return type > 0 ? type & kTagEdgeValueMask : type;
    }

    static EdgeRanking getRank(const folly::StringPiece& rawKey) {
        CHECK_EQ(rawKey.size(), kEdgeLen);
        auto offset = sizeof(PartitionID) + sizeof(VertexIntID) + sizeof(EdgeType);
        return readInt<EdgeRanking>(rawKey.data() + offset, sizeof(EdgeRanking));
    }

    static int64_t getVersion(const folly::StringPiece& rawKey) {
        CHECK(isVertex(rawKey) || isEdge(rawKey));
        auto offset = rawKey.size() - sizeof(int64_t);
        return readInt<int64_t>(rawKey.data() + offset, sizeof(int64_t));
    }

    static bool isDataKey(const folly::StringPiece& key) {
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
        auto type = readInt<int32_t>(key.data(), len) & kTypeMask;
        return static_cast<uint32_t>(NebulaKeyType::kData) == type;
    }


    static bool isUUIDKey(const folly::StringPiece& key) {
        auto type = readInt<int32_t>(key.data(), sizeof(int32_t)) & kTypeMask;
        return static_cast<uint32_t>(NebulaKeyType::kUUID) == type;
    }

    static folly::StringPiece keyWithNoVersion(const folly::StringPiece& rawKey) {
        // TODO(heng) We should change the method if varint data version supportted.
        return rawKey.subpiece(0, rawKey.size() - sizeof(int64_t));
    }

    static bool isIndexKey(const folly::StringPiece& key) {
        if (key.size() < kIndexLen) {
            return false;
        }
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
        auto type = readInt<int32_t>(key.data(), len) & kTypeMask;
        return static_cast<uint32_t>(NebulaKeyType::kIndex) == type;
    }

    static IndexID getIndexId(const folly::StringPiece& rawKey) {
        CHECK_GT(rawKey.size(), kIndexLen);
        auto offset = sizeof(PartitionID);
        return readInt<IndexID>(rawKey.data() + offset, sizeof(IndexID));
    }

private:
    NebulaKeyUtils() = delete;
};

}  // namespace nebula
#endif  // COMMON_BASE_NEBULAKEYUTILS_H_

