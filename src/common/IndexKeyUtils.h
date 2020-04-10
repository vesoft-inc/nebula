/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_INDEXKEYUTILS_H_
#define COMMON_INDEXKEYUTILS_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include "thrift/ThriftTypes.h"
#include "interface/gen-cpp2/meta_types.h"
#include "common/Types.h"


namespace nebula {

using VariantType = boost::variant<int64_t, double, bool, std::string>;

using OptVariantType = StatusOr<VariantType>;

using IndexValues = std::vector<std::pair<nebula::meta::cpp2::PropertyType, std::string>>;

/**
 * This class supply some utils for transition between Vertex/Edge and key in kvstore.
 * */
class IndexKeyUtils final {
public:
    ~IndexKeyUtils() = default;

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
                                        nebula::meta::cpp2::PropertyType type) {
        switch (type) {
            case nebula::meta::cpp2::PropertyType::BOOL : {
                return *reinterpret_cast<const bool*>(raw.data());
            }
            case nebula::meta::cpp2::PropertyType::INT64 :
            case nebula::meta::cpp2::PropertyType::TIMESTAMP : {
                return decodeInt64(raw);
            }
            case nebula::meta::cpp2::PropertyType::DOUBLE :
            case nebula::meta::cpp2::PropertyType::FLOAT : {
                return decodeDouble(raw);
            }
            case nebula::meta::cpp2::PropertyType::STRING : {
                return raw.str();
            }
            default:
                return OptVariantType(Status::Error("Unknown type"));
        }
    }

    static VertexIntID getIndexVertexIntID(const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kVertexIndexLen);
        auto offset = rawKey.size() - sizeof(VertexIntID);
        return *reinterpret_cast<const VertexIntID*>(rawKey.data() + offset);
     }

    static VertexIntID getIndexSrcId(const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kEdgeIndexLen);
        auto offset = rawKey.size() -
                      sizeof(VertexIntID) * 2 - sizeof(EdgeRanking);
        return readInt<VertexIntID>(rawKey.data() + offset, sizeof(VertexIntID));
    }

    static VertexIntID getIndexDstId(const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kEdgeIndexLen);
        auto offset = rawKey.size() - sizeof(VertexIntID);
        return readInt<VertexIntID>(rawKey.data() + offset, sizeof(VertexIntID));
    }

    static EdgeRanking getIndexRank(const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kEdgeIndexLen);
        auto offset = rawKey.size() - sizeof(VertexIntID) - sizeof(EdgeRanking);
        return readInt<EdgeRanking>(rawKey.data() + offset, sizeof(EdgeRanking));
    }

private:
    IndexKeyUtils() = delete;

};

}  // namespace nebula
#endif  // COMMON_INDEXKEYUTILS_H_

