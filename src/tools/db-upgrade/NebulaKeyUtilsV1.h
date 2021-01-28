/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef TOOLS_DBUPGRADE_NEBULAKEYUTILSV1_H_
#define TOOLS_DBUPGRADE_NEBULAKEYUTILSV1_H_

#include "utils/Types.h"

namespace nebula {

enum class NebulaKeyTypeV1 : uint32_t {
    kData              = 0x00000001,
    kIndex             = 0x00000002,
    kUUID              = 0x00000003,
    kSystem            = 0x00000004,
};

/**
 * This class supply some utils for transition between Vertex/Edge and key in kvstore.
 * */
class NebulaKeyUtilsV1 final {
public:
    ~NebulaKeyUtilsV1() = default;

    using VertexID = int64_t;

    static std::string indexPrefix(PartitionID partId, IndexID indexId);

    static std::string vertexPrefix(PartitionID partId, VertexID vId, TagID tagId);

    static std::string edgePrefix(PartitionID partId, VertexID srcId, EdgeType type);

    static std::string vertexPrefix(PartitionID partId, VertexID vId);

    static std::string edgePrefix(PartitionID partId, VertexID vId);

    static std::string edgePrefix(PartitionID partId,
                                  VertexID srcId,
                                  EdgeType type,
                                  EdgeRanking rank,
                                  VertexID dstId);

    static std::string systemPrefix();

    static std::string prefix(PartitionID partId);

    static std::string snapshotPrefix(PartitionID partId);

    static PartitionID getPart(const folly::StringPiece& rawKey) {
        return readInt<PartitionID>(rawKey.data(), sizeof(PartitionID)) >> 8;
    }

    static bool isVertex(const folly::StringPiece& rawKey) {
        if (rawKey.size() != kVertexLen) {
            return false;
        }
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyTypeV1));
        auto type = readInt<uint32_t>(rawKey.data(), len) & kTypeMask;
        if (static_cast<uint32_t>(NebulaKeyTypeV1::kData) != type) {
            return false;
        }
        auto offset = sizeof(PartitionID) + sizeof(VertexID);
        TagID tagId =  readInt<TagID>(rawKey.data() + offset, sizeof(TagID));
        return !(tagId & kTagEdgeMask);
    }

    static VertexID getVertexId(const folly::StringPiece& rawKey) {
        CHECK_EQ(rawKey.size(), kVertexLen);
        auto offset = sizeof(PartitionID);
        return readInt<VertexID>(rawKey.data() + offset, sizeof(VertexID));
    }

    static TagID getTagId(const folly::StringPiece& rawKey) {
        // CHECK_EQ(rawKey.size(), kVertexLen);
        if (rawKey.size() != kVertexLen) {
            std::stringstream msg;
            msg << " rawKey.size() != kVertexLen."
                << "\nrawKey.size()=" << rawKey.size()
                << "\nkVertexLen=" << kVertexLen
                << "\nhexDump:\n"
                << folly::hexDump(rawKey.data(), rawKey.size());
            LOG(FATAL) << msg.str();
        }
        auto offset = sizeof(PartitionID) + sizeof(VertexID);
        return readInt<TagID>(rawKey.data() + offset, sizeof(TagID));
    }

    static bool isEdge(const folly::StringPiece& rawKey) {
        if (rawKey.size() != kEdgeLen) {
            return false;
        }
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyTypeV1));
        auto type = readInt<uint32_t>(rawKey.data(), len) & kTypeMask;
        if (static_cast<uint32_t>(NebulaKeyTypeV1::kData) != type) {
            return false;
        }
        auto offset = sizeof(PartitionID) + sizeof(VertexID);
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

    static VertexID getSrcId(const folly::StringPiece& rawKey) {
        CHECK_EQ(rawKey.size(), kEdgeLen);
        return readInt<VertexID>(rawKey.data() + sizeof(PartitionID), sizeof(VertexID));
    }

    static VertexID getDstId(const folly::StringPiece& rawKey) {
        CHECK_EQ(rawKey.size(), kEdgeLen);
        auto offset = sizeof(PartitionID) + sizeof(VertexID) +
                      sizeof(EdgeType) + sizeof(EdgeRanking);
        return readInt<VertexID>(rawKey.data() + offset, sizeof(VertexID));
    }

    static EdgeType getEdgeType(const folly::StringPiece& rawKey) {
        CHECK_EQ(rawKey.size(), kEdgeLen);
        auto offset = sizeof(PartitionID) + sizeof(VertexID);
        EdgeType type = readInt<EdgeType>(rawKey.data() + offset, sizeof(EdgeType));
        return type > 0 ? type & kTagEdgeValueMask : type;
    }

    static EdgeRanking getRank(const folly::StringPiece& rawKey) {
        CHECK_EQ(rawKey.size(), kEdgeLen);
        auto offset = sizeof(PartitionID) + sizeof(VertexID) + sizeof(EdgeType);
        return readInt<EdgeRanking>(rawKey.data() + offset, sizeof(EdgeRanking));
    }

    static int64_t getVersion(const folly::StringPiece& rawKey) {
        CHECK(isVertex(rawKey) || isEdge(rawKey));
        auto offset = rawKey.size() - sizeof(int64_t);
        return readInt<int64_t>(rawKey.data() + offset, sizeof(int64_t));
    }

    static IndexID getIndexId(const folly::StringPiece& rawKey) {
        CHECK_GT(rawKey.size(), kIndexLen);
        auto offset = sizeof(PartitionID);
        return readInt<IndexID>(rawKey.data() + offset, sizeof(IndexID));
    }

    template<typename T>
    static typename std::enable_if<std::is_integral<T>::value, T>::type
    readInt(const char* data, int32_t len) {
        CHECK_GE(len, sizeof(T));
        return *reinterpret_cast<const T*>(data);
    }

    static bool isDataKey(const folly::StringPiece& key) {
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyTypeV1));
        auto type = readInt<int32_t>(key.data(), len) & kTypeMask;
        return static_cast<uint32_t>(NebulaKeyTypeV1::kData) == type;
    }

    static bool isIndexKey(const folly::StringPiece& key) {
        if (key.size() < kIndexLen) {
            return false;
        }
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyTypeV1));
        auto type = readInt<int32_t>(key.data(), len) & kTypeMask;
        return static_cast<uint32_t>(NebulaKeyTypeV1::kIndex) == type;
    }

    static bool isUUIDKey(const folly::StringPiece& key) {
        auto type = readInt<int32_t>(key.data(), sizeof(int32_t)) & kTypeMask;
        return static_cast<uint32_t>(NebulaKeyTypeV1::kUUID) == type;
    }

    static folly::StringPiece keyWithNoVersion(const folly::StringPiece& rawKey) {
        // TODO(heng) We should change the method if varint data version supportted.
        return rawKey.subpiece(0, rawKey.size() - sizeof(int64_t));
    }

    static VertexID getIndexVertexID(const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kVertexIndexLen);
        auto offset = rawKey.size() - sizeof(VertexID);
        return *reinterpret_cast<const VertexID*>(rawKey.data() + offset);
     }

    static VertexID getIndexSrcId(const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kEdgeIndexLen);
        auto offset = rawKey.size() -
                      sizeof(VertexID) * 2 - sizeof(EdgeRanking);
        return readInt<VertexID>(rawKey.data() + offset, sizeof(VertexID));
    }

    static VertexID getIndexDstId(const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kEdgeIndexLen);
        auto offset = rawKey.size() - sizeof(VertexID);
        return readInt<VertexID>(rawKey.data() + offset, sizeof(VertexID));
    }

    static EdgeRanking getIndexRank(const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kEdgeIndexLen);
        auto offset = rawKey.size() - sizeof(VertexID) - sizeof(EdgeRanking);
        return readInt<EdgeRanking>(rawKey.data() + offset, sizeof(EdgeRanking));
    }

private:
    NebulaKeyUtilsV1() = delete;

private:
    static constexpr int32_t kVertexLen = sizeof(PartitionID) + sizeof(VertexID)
                                        + sizeof(TagID) + sizeof(TagVersion);

    static constexpr int32_t kEdgeLen = sizeof(PartitionID) + sizeof(VertexID)
                                      + sizeof(EdgeType) + sizeof(VertexID)
                                      + sizeof(EdgeRanking) + sizeof(EdgeVersion);

    static constexpr int32_t kVertexIndexLen = sizeof(PartitionID) + sizeof(IndexID)
                                               + sizeof(VertexID);

    static constexpr int32_t kEdgeIndexLen = sizeof(PartitionID) + sizeof(IndexID)
                                             + sizeof(VertexID) * 2 + sizeof(EdgeRanking);

    static constexpr int32_t kIndexLen = std::min(kVertexIndexLen, kEdgeIndexLen);

    static constexpr int32_t kSystemLen = sizeof(PartitionID) + sizeof(NebulaSystemKeyType);

    // The partition id offset in 4 Bytes
    static constexpr uint8_t kPartitionOffset = 8;

    // The key type bits Mask
    // See KeyType enum
    static constexpr uint32_t kTypeMask     = 0x000000FF;

    // The most significant bit is sign bit, tag is always 0
    // The second most significant bit is tag/edge type bit Mask
    // 0 for Tag, 1 for Edge
    static constexpr uint32_t kTagEdgeMask      = 0x40000000;
    // For extract Tag/Edge value
    static constexpr uint32_t kTagEdgeValueMask = ~kTagEdgeMask;
    // Write edge by |= 0x40000000
    static constexpr uint32_t kEdgeMaskSet      = kTagEdgeMask;
    // Write Tag by &= 0xbfffffff
    static constexpr uint32_t kTagMaskSet       = ~kTagEdgeMask;
};


}  // namespace nebula
#endif  // TOOLS_DBUPGRADE_NEBULAKEYUTILSV1_H_

