/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_NEBULAKEYUTILS_H_
#define COMMON_BASE_NEBULAKEYUTILS_H_

#include "base/Base.h"
#include "interface/gen-cpp2/common_types.h"
#include "common/filter/Expressions.h"

using IndexValues = std::vector<std::pair<nebula::cpp2::SupportedType, std::string>>;

namespace nebula {

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

enum class NebulaBoundValueType : uint8_t {
    kMax                = 0x0001,
    kMin                = 0x0002,
    kSubtraction        = 0x0003,
    kAddition           = 0x0004,
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

    static std::string snapshotPrefix(PartitionID partId);

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
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
        auto type = readInt<int32_t>(key.data(), len) & kTypeMask;
        return static_cast<uint32_t>(NebulaKeyType::kData) == type;
    }

    static bool isIndexKey(const folly::StringPiece& key) {
        if (key.size() < kIndexLen) {
            return false;
        }
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

    // Only int and double are supported
    static std::string boundVariant(nebula::cpp2::SupportedType type,
                                    NebulaBoundValueType op,
                                    const VariantType& v = 0L) {
        switch (op) {
            case NebulaBoundValueType::kMax : {
                std::vector<unsigned char> bytes = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
                return std::string(bytes.begin(), bytes.end());
            }
            case NebulaBoundValueType::kMin : {
                std::string str;
                str.reserve(sizeof(int64_t));
                str.append(sizeof(int64_t), '\0');
                return str;
            }
            case NebulaBoundValueType::kSubtraction : {
                std::string str;
                if (type == nebula::cpp2::SupportedType::INT) {
                    str = encodeInt64(boost::get<int64_t>(v));
                } else {
                    str = encodeDouble(boost::get<double>(v));
                }
                std::vector<unsigned char> bytes(str.begin(), str.end());
                for (size_t i = bytes.size(); i > 0; i--) {
                    if (bytes[i-1] > 0) {
                        bytes[i-1] -= 1;
                        break;
                    }
                }
                return std::string(bytes.begin(), bytes.end());
            }
            case NebulaBoundValueType::kAddition : {
                std::string str;
                if (type == nebula::cpp2::SupportedType::INT) {
                    str = encodeInt64(boost::get<int64_t>(v));
                } else {
                    str = encodeDouble(boost::get<double>(v));
                }
                std::vector<unsigned char> bytes(str.begin(), str.end());
                for (size_t i = bytes.size(); i > 0; i--) {
                    if (bytes[i-1] < 255) {
                        bytes[i-1] += 1;
                        break;
                    }
                }
                return std::string(bytes.begin(), bytes.end());
            }
        }
        return "";
    }

    static bool checkAndCastVariant(nebula::cpp2::SupportedType sType,
                                    VariantType& v) {
        nebula::cpp2::SupportedType type = nebula::cpp2::SupportedType::UNKNOWN;
        switch (v.which()) {
            case VAR_INT64: {
                type = nebula::cpp2::SupportedType::INT;
                break;
            }
            case VAR_DOUBLE: {
                type = nebula::cpp2::SupportedType::DOUBLE;
                break;
            }
            case VAR_BOOL: {
                type = nebula::cpp2::SupportedType::BOOL;
                break;
            }
            case VAR_STR: {
                type = nebula::cpp2::SupportedType::STRING;
                break;
            }
            default:
                return false;
        }
        if (sType != type) {
            switch (sType) {
                case nebula::cpp2::SupportedType::INT:
                case nebula::cpp2::SupportedType::TIMESTAMP: {
                    v = Expression::toInt(v);
                    break;
                }
                case nebula::cpp2::SupportedType::BOOL: {
                    v = Expression::toBool(v);
                    break;
                }
                case nebula::cpp2::SupportedType::FLOAT:
                case nebula::cpp2::SupportedType::DOUBLE: {
                    v = Expression::toDouble(v);
                    break;
                }
                case nebula::cpp2::SupportedType::STRING: {
                    v = Expression::toString(v);
                    break;
                }
                default: {
                    return false;
                }
            }
        }
        return true;
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

    static int64_t decodeInt64(const folly::StringPiece& raw) {
        auto val = *reinterpret_cast<const int64_t*>(raw.data());
        val = folly::Endian::big(val);
        val ^= folly::to<int64_t>(1) << 63;
        return val;
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
            /**
             *   TODO : now, the -(std::numeric_limits<double>::min())
             *   have a problem of precision overflow. current return value is -nan.
             */
            auto i = *reinterpret_cast<const int64_t*>(&v);
            i = -(std::numeric_limits<int64_t>::max() + i);
            v = *reinterpret_cast<const double*>(&i);
        }
        auto val = folly::Endian::big(v);
        auto* c = reinterpret_cast<char*>(&val);
        c[0] ^= 0x80;
        std::string raw;
        raw.reserve(sizeof(double));
        raw.append(c, sizeof(double));
        return raw;
    }

    static double decodeDouble(const folly::StringPiece& raw) {
        char* v = const_cast<char*>(raw.data());
        v[0] ^= 0x80;
        auto val = *reinterpret_cast<const double*>(v);
        val = folly::Endian::big(val);
        if (val < 0) {
            auto i = *reinterpret_cast<const int64_t*>(&val);
            i = -(std::numeric_limits<int64_t >::max() + i);
            val = *reinterpret_cast<const double*>(&i);
        }
        return val;
    }

    static OptVariantType decodeVariant(const folly::StringPiece& raw,
                                        nebula::cpp2::SupportedType type) {
        switch (type) {
            case nebula::cpp2::SupportedType::BOOL : {
                return *reinterpret_cast<const bool*>(raw.data());
            }
            case nebula::cpp2::SupportedType::INT :
            case nebula::cpp2::SupportedType::TIMESTAMP : {
                return decodeInt64(raw);
            }
            case nebula::cpp2::SupportedType::DOUBLE :
            case nebula::cpp2::SupportedType::FLOAT : {
                return decodeDouble(raw);
            }
            case nebula::cpp2::SupportedType::STRING : {
                return raw.str();
            }
            default:
                return OptVariantType(Status::Error("Unknown type"));
        }
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
    NebulaKeyUtils() = delete;

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

