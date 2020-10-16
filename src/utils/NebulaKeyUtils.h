/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTILS_NEBULAKEYUTILS_H_
#define UTILS_NEBULAKEYUTILS_H_

#include "utils/Types.h"

namespace nebula {

/**
 * VertexKeyUtils:
 * type(1) + partId(3) + vertexId(*) + tagId(4) + version(8)
 *
 * EdgeKeyUtils:
 * type(1) + partId(3) + srcId(*) + edgeType(4) + edgeRank(8) + dstId(*) + version(8)
 *
 * For data in Nebula 1.0, all vertexId is int64_t, so the size would be 8.
 * For data in Nebula 2.0, all vertexId is fixed length string according to space property.
 *
 * */

/**
 * This class supply some utils for transition between Vertex/Edge and key in kvstore.
 * */
class NebulaKeyUtils final {
public:
    ~NebulaKeyUtils() = default;

    /*
     * Check the validity of vid length
     */
    static bool isValidVidLen(size_t vIdLen, VertexID srcvId, VertexID dstvId = "");

    /**
     * Generate vertex key for kv store
     * */
    static std::string vertexKey(size_t vIdLen, PartitionID partId, VertexID vId,
                                 TagID tagId, TagVersion tv);

    /**
     * Generate edge key for kv store
     * */
    static std::string edgeKey(size_t vIdLen, PartitionID partId, VertexID srcId,
                               EdgeType type, EdgeRanking rank,
                               VertexID dstId, EdgeVersion ev);

    static std::string systemCommitKey(PartitionID partId);

    static std::string systemPartKey(PartitionID partId);

    static std::string uuidKey(PartitionID partId, const folly::StringPiece& name);

    static std::string kvKey(PartitionID partId, const folly::StringPiece& name);

    /**
     * Prefix for vertex
     * */
    static std::string vertexPrefix(size_t vIdLen, PartitionID partId, VertexID vId, TagID tagId);

    static std::string vertexPrefix(size_t vIdLen, PartitionID partId, VertexID vId);

    /**
     * Prefix for edge
     * */
    static std::string edgePrefix(size_t vIdLen, PartitionID partId, VertexID srcId, EdgeType type);

    static std::string edgePrefix(size_t vIdLen, PartitionID partId, VertexID srcId);

    static std::string edgePrefix(size_t vIdLen,
                                  PartitionID partId,
                                  VertexID srcId,
                                  EdgeType type,
                                  EdgeRanking rank,
                                  VertexID dstId);

    static std::string systemPrefix();

    static std::string partPrefix(PartitionID partId);

    static std::string snapshotPrefix(PartitionID partId);

    static PartitionID getPart(const folly::StringPiece& rawKey) {
        return readInt<PartitionID>(rawKey.data(), sizeof(PartitionID)) >> 8;
    }

    static bool isVertex(size_t vIdLen, const folly::StringPiece& rawKey) {
        if (rawKey.size() != kVertexLen + vIdLen) {
            return false;
        }
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
        auto type = readInt<uint32_t>(rawKey.data(), len) & kTypeMask;
        if (static_cast<uint32_t>(NebulaKeyType::kData) != type) {
            return false;
        }
        auto offset = sizeof(PartitionID) + vIdLen;
        TagID tagId = readInt<TagID>(rawKey.data() + offset, sizeof(TagID));
        return !(tagId & kTagEdgeMask);
    }

    static VertexIDSlice getVertexId(size_t vIdLen, const folly::StringPiece& rawKey) {
        if (rawKey.size() != kVertexLen + vIdLen) {
            dumpBadKey(rawKey, kVertexLen + vIdLen, vIdLen);
        }
        auto offset = sizeof(PartitionID);
        return rawKey.subpiece(offset, vIdLen);
    }

    static TagID getTagId(size_t vIdLen, const folly::StringPiece& rawKey) {
        if (rawKey.size() != kVertexLen + vIdLen) {
            dumpBadKey(rawKey, kVertexLen + vIdLen, vIdLen);
        }
        auto offset = sizeof(PartitionID) + vIdLen;
        return readInt<TagID>(rawKey.data() + offset, sizeof(TagID));
    }

    static bool isEdge(size_t vIdLen, const folly::StringPiece& rawKey) {
        if (rawKey.size() != kEdgeLen + (vIdLen << 1)) {
            return false;
        }
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
        auto type = readInt<uint32_t>(rawKey.data(), len) & kTypeMask;
        if (static_cast<uint32_t>(NebulaKeyType::kData) != type) {
            return false;
        }
        auto offset = sizeof(PartitionID) + vIdLen;
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

    static VertexIDSlice getSrcId(size_t vIdLen, const folly::StringPiece& rawKey) {
        if (rawKey.size() != kEdgeLen + (vIdLen << 1)) {
            dumpBadKey(rawKey, kEdgeLen + (vIdLen << 1), vIdLen);
        }
        auto offset = sizeof(PartitionID);
        return rawKey.subpiece(offset, vIdLen);
    }

    static VertexIDSlice getDstId(size_t vIdLen, const folly::StringPiece& rawKey) {
        if (rawKey.size() != kEdgeLen + (vIdLen << 1)) {
            dumpBadKey(rawKey, kEdgeLen + (vIdLen << 1), vIdLen);
        }
        auto offset = sizeof(PartitionID) + vIdLen +
                      sizeof(EdgeType) + sizeof(EdgeRanking);
        return rawKey.subpiece(offset, vIdLen);
    }

    static EdgeType getEdgeType(size_t vIdLen, const folly::StringPiece& rawKey) {
        if (rawKey.size() != kEdgeLen + (vIdLen << 1)) {
            dumpBadKey(rawKey, kEdgeLen + (vIdLen << 1), vIdLen);
        }
        auto offset = sizeof(PartitionID) + vIdLen;
        EdgeType type = readInt<EdgeType>(rawKey.data() + offset, sizeof(EdgeType));
        return type > 0 ? type & kTagEdgeValueMask : type;
    }

    static EdgeRanking getRank(size_t vIdLen, const folly::StringPiece& rawKey) {
        if (rawKey.size() != kEdgeLen + (vIdLen << 1)) {
            dumpBadKey(rawKey, kEdgeLen + (vIdLen << 1), vIdLen);
        }
        auto offset = sizeof(PartitionID) + vIdLen + sizeof(EdgeType);
        return readInt<EdgeRanking>(rawKey.data() + offset, sizeof(EdgeRanking));
    }

    static int64_t getVersion(size_t vIdLen, const folly::StringPiece& rawKey) {
        CHECK(isVertex(vIdLen, rawKey) || isEdge(vIdLen, rawKey));
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

    static void dumpBadKey(const folly::StringPiece& rawKey, size_t expect, size_t vIdLen) {
        std::stringstream msg;
        msg << "rawKey.size() != expect size"
            << ", rawKey.size() = " << rawKey.size()
            << ", expect = " << expect
            << ", vIdLen = " << vIdLen
            << ", rawkey hex format:\n";
        msg << folly::hexDump(rawKey.data(), rawKey.size());
        LOG(FATAL) << msg.str();
    }

private:
    NebulaKeyUtils() = delete;
};

}  // namespace nebula
#endif  // UTILS_NEBULAKEYUTILS_H_

