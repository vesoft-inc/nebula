/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_NEBULAKEYUTILS_H_
#define COMMON_BASE_NEBULAKEYUTILS_H_

#include "base/Base.h"
#include "interface/gen-cpp2/common_types.h"

namespace nebula {
using IndexValues = std::vector<std::pair<nebula::cpp2::SupportedType, std::string>>;

/**
 * VertexKeyUtils:
 * type(1) + partId(3) + vertexId(8) + tagId(4) + version(8)
 *
 * EdgeKeyUtils:
 * type(1) + partId(3) + srcId(8) + edgeType(4) + edgeRank(8) + dstId(8) + version(8)
 *
 * */

enum class NebulaKeyType : uint32_t {
    kData              = 0x00000001,
    kIndex             = 0x00000002,
    kUUID              = 0x00000003,
    kSystem            = 0x00000004,
};

enum class NebulaSystemKeyType : uint32_t {
    kSystemCommit      = 0x00000001,
    kSystemPart        = 0x00000002,
};

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
                                 TagID tagId, TagVersion tv);

    /**
     * Generate edge key for kv store
     * */
    static std::string edgeKey(PartitionID partId, VertexID srcId,
                               EdgeType type, EdgeRanking rank,
                               VertexID dstId, EdgeVersion ev);

    static std::string systemCommitKey(PartitionID partId);

    static std::string systemPartKey(PartitionID partId);

    static std::string uuidKey(PartitionID partId, const folly::StringPiece& name);

    static std::string kvKey(PartitionID partId, const folly::StringPiece& name);

    /**
     * Generate vertex|edge index key for kv store
     **/
    static void indexRaw(const IndexValues &values, std::string& raw);

    static std::string vertexIndexKey(PartitionID partId, IndexID indexId, VertexID vId,
                                      const IndexValues& values);

    static std::string edgeIndexKey(PartitionID partId, IndexID indexId,
                                    VertexID srcId, EdgeRanking rank,
                                    VertexID dstId, const IndexValues& values);

    static std::string indexPrefix(PartitionID partId, IndexID indexId);

    /**
     * Prefix for
     * */
    static std::string vertexPrefix(PartitionID partId, VertexID vId, TagID tagId);

    /**
     * Prefix for srcId edges with some edgeType
     * */
    static std::string edgePrefix(PartitionID partId, VertexID srcId, EdgeType type);

    static std::string vertexPrefix(PartitionID partId, VertexID vId);

    static std::string edgePrefix(PartitionID partId, VertexID vId);

    static std::string edgePrefix(PartitionID partId,
                                  VertexID srcId,
                                  EdgeType type,
                                  EdgeRanking rank,
                                  VertexID dstId);

    static std::string systemPrefix();

    static std::string prefix(PartitionID partId, VertexID src, EdgeType type,
                              EdgeRanking ranking, VertexID dst);

    static std::string prefix(PartitionID partId);

    static PartitionID getPart(const folly::StringPiece& rawKey) {
        return readInt<PartitionID>(rawKey.data(), sizeof(PartitionID)) >> 8;
    }

    static bool isVertex(const folly::StringPiece& rawKey) {
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
        auto type = readInt<uint32_t>(rawKey.data(), len) & kTypeMask;
        if (static_cast<uint32_t>(NebulaKeyType::kData) != type) {
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
        CHECK_EQ(rawKey.size(), kVertexLen);
        auto offset = sizeof(PartitionID) + sizeof(VertexID);
        return readInt<TagID>(rawKey.data() + offset, sizeof(TagID));
    }

    static bool isEdge(const folly::StringPiece& rawKey) {
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
        auto type = readInt<uint32_t>(rawKey.data(), len) & kTypeMask;
        if (static_cast<uint32_t>(NebulaKeyType::kData) != type) {
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

    template<typename T>
    static typename std::enable_if<std::is_integral<T>::value, T>::type
    readInt(const char* data, int32_t len) {
        CHECK_GE(len, sizeof(T));
        return *reinterpret_cast<const T*>(data);
    }

    static bool isDataKey(const folly::StringPiece& key) {
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
        auto type = readInt<int32_t>(key.data(), len) & kTypeMask;
        return static_cast<uint32_t>(NebulaKeyType::kData) == type;
    }

    static bool isIndexKey(const folly::StringPiece& key) {
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
        auto type = readInt<int32_t>(key.data(), len) & kTypeMask;
        return static_cast<uint32_t>(NebulaKeyType::kIndex) == type;
    }

    static bool isUUIDKey(const folly::StringPiece& key) {
        auto type = readInt<int32_t>(key.data(), sizeof(int32_t)) & kTypeMask;
        return static_cast<uint32_t>(NebulaKeyType::kUUID) == type;
    }

    static folly::StringPiece keyWithNoVersion(const folly::StringPiece& rawKey) {
        // TODO(heng) We should change the method if varint data version supportted.
        return rawKey.subpiece(0, rawKey.size() - sizeof(int64_t));
    }

    static std::string encodeVariant(const VariantType& v)  {
        switch (v.which()) {
            case VAR_INT64:
                return encodeInt64(boost::get<int64_t>(v));
            case VAR_DOUBLE:
                return encodeDouble(boost::get<double>(v));
            case VAR_BOOL: {
                auto val = boost::get<bool>(v);
                std::string raw;
                raw.reserve(sizeof(bool));
                raw.append(reinterpret_cast<const char*>(&val), sizeof(bool));
                return raw;
            }
            case VAR_STR:
                return boost::get<std::string>(v);
            default:
                std::string errMsg = folly::stringPrintf("Unknown VariantType: %d", v.which());
                LOG(ERROR) << errMsg;
        }
        return "";
    }

    /**
     * Default, positive number first bit is 0, negative number is 1 .
     * To keep the string in order, the first bit must to be inverse.
     * for example as below :
     *    9223372036854775807     -> "\377\377\377\377\377\377\377\377"
     *    1                       -> "\200\000\000\000\000\000\000\001"
     *    0                       -> "\200\000\000\000\000\000\000\000"
     *    -1                      -> "\177\377\377\377\377\377\377\377"
     *    -9223372036854775808    -> "\000\000\000\000\000\000\000\000"
     */

    static std::string encodeInt64(int64_t v) {
        v ^= folly::to<int64_t>(1) << 63;
        auto val = folly::Endian::big(v);
        std::string raw;
        raw.reserve(sizeof(int64_t));
        raw.append(reinterpret_cast<const char*>(&val), sizeof(int64_t));
        return raw;
    }

    /*
     * Default, the double memory structure is :
     *   sign bit（1bit）+  exponent bit(11bit) + float bit(52bit)
     *   The first bit is the sign bit, 0 for positive and 1 for negative
     *   To keep the string in order, the first bit must to be inverse,
     *   then need to subtract from maximum.
     */
    static std::string encodeDouble(double v) {
        if (v < 0) {
            v = -(std::numeric_limits<double>::max() + v);
        }
        auto val = folly::Endian::big(v);
        auto* c = reinterpret_cast<char*>(&val);
        c[0] ^= 0x80;
        std::string raw;
        raw.reserve(sizeof(double));
        raw.append(c, sizeof(double));
        return raw;
    }

private:
    NebulaKeyUtils() = delete;

private:
    static constexpr int32_t kVertexLen = sizeof(PartitionID) + sizeof(VertexID)
                                        + sizeof(TagID) + sizeof(TagVersion);

    static constexpr int32_t kEdgeLen = sizeof(PartitionID) + sizeof(VertexID)
                                      + sizeof(EdgeType) + sizeof(VertexID)
                                      + sizeof(EdgeRanking) + sizeof(EdgeVersion);

    static constexpr int32_t kSystemLen = sizeof(PartitionID) + sizeof(NebulaSystemKeyType);

    // The partition id offset in 4 Bytes
    static constexpr uint8_t kPartitionOffset = 8;

    // The key type bits Mask
    // See KeyType enum
    static constexpr uint32_t kTypeMask     = 0x000000FF;

    // The Tag/Edge type bit Mask
    // 0 for Tag, 1 for Edge
    // 0x40 - 0b0100,0000
    static constexpr uint32_t kTagEdgeMask      = 0x40000000;
    // For extract Tag/Edge value
    static constexpr uint32_t kTagEdgeValueMask = ~kTagEdgeMask;
    // Write edge by &=
    static constexpr uint32_t kEdgeMaskSet      = kTagEdgeMask;
    // Write Tag by |=
    static constexpr uint32_t kTagMaskSet       = ~kTagEdgeMask;
};

}  // namespace nebula
#endif  // COMMON_BASE_NEBULAKEYUTILS_H_

