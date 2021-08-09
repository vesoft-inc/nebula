/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTILS_INDEXKEYUTILS_H_
#define UTILS_INDEXKEYUTILS_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "codec/RowReader.h"
#include "utils/Types.h"
#include "codec/RowReader.h"

namespace nebula {

using PropertyType = nebula::meta::cpp2::PropertyType;

/**
 * This class supply some utils for index in kvstore.
 * */
class IndexKeyUtils final {
public:
    ~IndexKeyUtils() = default;

    static Value::Type toValueType(PropertyType type) {
        switch (type) {
            case PropertyType::BOOL:
                return Value::Type::BOOL;
            case PropertyType::INT64:
            case PropertyType::INT32:
            case PropertyType::INT16:
            case PropertyType::INT8:
            case PropertyType::TIMESTAMP:
                return Value::Type::INT;
            case PropertyType::VID:
                return Value::Type::VERTEX;
            case PropertyType::FLOAT:
            case PropertyType::DOUBLE:
                return Value::Type::FLOAT;
            case PropertyType::STRING:
            case PropertyType::FIXED_STRING:
                return Value::Type::STRING;
            case PropertyType::DATE:
                return Value::Type::DATE;
            case PropertyType::TIME:
                return Value::Type::TIME;
            case PropertyType::DATETIME:
                return Value::Type::DATETIME;
            case PropertyType::UNKNOWN:
                return Value::Type::__EMPTY__;
        }
        return Value::Type::__EMPTY__;
    }

    static std::string encodeNullValue(Value::Type type, const int16_t* strLen) {
        size_t len = 0;
        switch (type) {
            case Value::Type::INT: {
                len = sizeof(int64_t);
                break;
            }
            case Value::Type::FLOAT: {
                len = sizeof(double);
                break;
            }
            case Value::Type::BOOL: {
                len = sizeof(bool);
                break;
            }
            case Value::Type::STRING: {
                len = static_cast<size_t>(*strLen);
                break;
            }
            case Value::Type::TIME: {
                len = sizeof(int32_t) + sizeof(int8_t) * 3;
                break;
            }
            case Value::Type::DATE: {
                len = sizeof(int8_t) * 2 + sizeof(int16_t);
                break;
            }
            case Value::Type::DATETIME: {
                len = sizeof(int32_t) + sizeof(int16_t) + sizeof(int8_t) * 5;
                break;
            }
            default :
                LOG(ERROR) << "Unsupported default value type";
        }
        std::string raw;
        raw.reserve(len);
        raw.append(len, static_cast<char>(0xFF));
        return raw;
    }

    static std::string encodeValue(const Value& v, int16_t len) {
        if (v.type() == Value::Type::STRING) {
            std::string fs = v.getStr();
            if (static_cast<size_t>(len) > v.getStr().size()) {
                fs.append(len - v.getStr().size(), '\0');
            } else {
                fs = fs.substr(0, len);
            }
            return fs;
        } else {
            return encodeValue(v);
        }
    }

    static std::string encodeValue(const Value& v) {
        switch (v.type()) {
            case Value::Type::INT:
                return encodeInt64(v.getInt());
            case Value::Type::FLOAT:
                return encodeDouble(v.getFloat());
            case Value::Type::BOOL: {
                auto val = v.getBool();
                std::string raw;
                raw.reserve(sizeof(bool));
                raw.append(reinterpret_cast<const char*>(&val), sizeof(bool));
                return raw;
            }
            case Value::Type::STRING:
                return v.getStr();
            case Value::Type::TIME:
                return encodeTime(v.getTime());
            case Value::Type::DATE: {
                return encodeDate(v.getDate());
            }
            case Value::Type::DATETIME: {
                return encodeDateTime(v.getDateTime());
            }
            default :
                LOG(ERROR) << "Unsupported default value type";
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

    static std::string encodeRank(EdgeRanking rank) {
        return IndexKeyUtils::encodeInt64(rank);
    }

    static EdgeRanking decodeRank(const folly::StringPiece& raw) {
        return IndexKeyUtils::decodeInt64(raw);
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

    static std::string encodeTime(const nebula::Time& t) {
        auto hour = folly::Endian::big8(t.hour);
        auto minute = folly::Endian::big8(t.minute);
        auto sec = folly::Endian::big8(t.sec);
        auto microsec = folly::Endian::big32(t.microsec);
        std::string buf;
        buf.reserve(sizeof(int32_t) + sizeof(int8_t) * 3);
        buf.append(reinterpret_cast<const char*>(&hour), sizeof(int8_t))
           .append(reinterpret_cast<const char*>(&minute), sizeof(int8_t))
           .append(reinterpret_cast<const char*>(&sec), sizeof(int8_t))
           .append(reinterpret_cast<const char*>(&microsec), sizeof(int32_t));
        return buf;
    }

    static nebula::Time decodeTime(const folly::StringPiece& raw) {
        int8_t hour = *reinterpret_cast<const int8_t *>(raw.data());
        int8_t minute = *reinterpret_cast<const int8_t *>(raw.data() + sizeof(int8_t));
        int8_t sec = *reinterpret_cast<const int8_t *>(raw.data() + 2 * sizeof(int8_t));
        int32_t microsec = *reinterpret_cast<const int32_t *>(raw.data() + 3 * sizeof(int8_t));

        nebula::Time t;
        t.hour = folly::Endian::big8(hour);
        t.minute = folly::Endian::big8(minute);
        t.sec = folly::Endian::big8(sec);
        t.microsec = folly::Endian::big32(microsec);
        return t;
    }

    static std::string encodeDate(const nebula::Date& d) {
        auto year = folly::Endian::big16(d.year);
        auto month = folly::Endian::big8(d.month);
        auto day = folly::Endian::big8(d.day);
        std::string buf;
        buf.reserve(sizeof(int8_t) * 2 + sizeof(int16_t));
        buf.append(reinterpret_cast<const char*>(&year), sizeof(int16_t))
           .append(reinterpret_cast<const char*>(&month), sizeof(int8_t))
           .append(reinterpret_cast<const char*>(&day), sizeof(int8_t));
        return buf;
    }

    static nebula::Date decodeDate(const folly::StringPiece& raw) {
        int16_t year = *reinterpret_cast<const int16_t *>(raw.data());
        int8_t month = *reinterpret_cast<const int8_t *>(raw.data() + sizeof(int16_t));
        int8_t  day = *reinterpret_cast<const int8_t *>(
            raw.data() + sizeof(int16_t) + sizeof(int8_t));
        return Date(folly::Endian::big16(year),
                    folly::Endian::big8(month),
                    folly::Endian::big8(day));
    }

    static std::string encodeDateTime(const nebula::DateTime& dt) {
        auto year = folly::Endian::big16(static_cast<uint16_t>(dt.year));
        auto month = folly::Endian::big8(static_cast<uint8_t>(dt.month));
        auto day = folly::Endian::big8(static_cast<uint8_t>(dt.day));
        auto hour = folly::Endian::big8(static_cast<uint8_t>(dt.hour));
        auto minute = folly::Endian::big8(static_cast<uint8_t>(dt.minute));
        auto sec = folly::Endian::big8(static_cast<uint8_t>(dt.sec));
        auto microsec = folly::Endian::big32(static_cast<uint32_t>(dt.microsec));
        std::string buf;
        buf.reserve(sizeof(int32_t) + sizeof(int16_t) + sizeof(int8_t) * 5);
        buf.append(reinterpret_cast<const char*>(&year), sizeof(int16_t))
           .append(reinterpret_cast<const char*>(&month), sizeof(int8_t))
           .append(reinterpret_cast<const char*>(&day), sizeof(int8_t))
           .append(reinterpret_cast<const char*>(&hour), sizeof(int8_t))
           .append(reinterpret_cast<const char*>(&minute), sizeof(int8_t))
           .append(reinterpret_cast<const char*>(&sec), sizeof(int8_t))
           .append(reinterpret_cast<const char*>(&microsec), sizeof(int32_t));
        return buf;
    }

    static nebula::DateTime decodeDateTime(const folly::StringPiece& raw) {
        int16_t year = *reinterpret_cast<const int16_t *>(raw.data());
        int8_t month = *reinterpret_cast<const int8_t *>(raw.data() + sizeof(int16_t));
        int8_t day = *reinterpret_cast<const int8_t *>(
            raw.data() + sizeof(int16_t) + sizeof(int8_t));
        int8_t hour = *reinterpret_cast<const int8_t *>(
            raw.data() + sizeof(int16_t) + 2 * sizeof(int8_t));
        int8_t minute = *reinterpret_cast<const int8_t *>(
            raw.data() + sizeof(int16_t) + 3 * sizeof(int8_t));
        int8_t sec = *reinterpret_cast<const int8_t *>(
            raw.data() + sizeof(int16_t) + 4 * sizeof(int8_t));
        int32_t microsec = *reinterpret_cast<const int32_t *>(
            raw.data() + sizeof(int16_t) + 5 * sizeof(int8_t));

        nebula::DateTime dt;
        dt.year = folly::Endian::big16(year);
        dt.month = folly::Endian::big8(month);
        dt.day = folly::Endian::big8(day);
        dt.hour = folly::Endian::big8(hour);
        dt.minute = folly::Endian::big8(minute);
        dt.sec = folly::Endian::big8(sec);
        dt.microsec = folly::Endian::big32(microsec);
        return dt;
    }

    static Value decodeValue(const folly::StringPiece& raw, Value::Type type) {
        Value v;
        switch (type) {
            case Value::Type::INT: {
                v.setInt(decodeInt64(raw));
                break;
            }
            case Value::Type::FLOAT: {
                v.setFloat(decodeDouble(raw));
                break;
            }
            case Value::Type::BOOL: {
                v.setBool(*reinterpret_cast<const bool*>(raw.data()));
                break;
            }
            case Value::Type::STRING: {
                v.setStr(raw.subpiece(0, raw.find_first_of('\0')).toString());
                break;
            }
            case Value::Type::TIME: {
                v.setTime(decodeTime(raw));
                break;
            }
            case Value::Type::DATE: {
                v.setDate(decodeDate(raw));
                break;
            }
            case Value::Type::DATETIME: {
                v.setDateTime(decodeDateTime(raw));
                break;
            }
            default:
                return Value(NullType::BAD_DATA);
        }
        return v;
    }

    static Value getValueFromIndexKey(size_t vIdLen,
                                      folly::StringPiece key,
                                      const std::string& prop,
                                      const std::vector<meta::cpp2::ColumnDef>& cols,
                                      bool isEdgeIndex = false,
                                      bool hasNullableCol = false) {
        size_t len = 0;
        std::bitset<16> nullableBit;
        int8_t nullableColPosit = 15;
        size_t offset = sizeof(PartitionID) + sizeof(IndexID);
        auto tailLen = (!isEdgeIndex) ? vIdLen : vIdLen * 2 + sizeof(EdgeRanking);

        auto it = std::find_if(cols.begin(), cols.end(),
                               [&prop] (const auto& col) {
                                   return prop == col.get_name();
                               });
        if (it == cols.end()) {
            return Value(NullType::BAD_DATA);
        }
        auto type = IndexKeyUtils::toValueType(it->get_type().get_type());

        if (hasNullableCol) {
            auto bitOffset = key.size() - tailLen - sizeof(u_short);
            auto v = *reinterpret_cast<const u_short*>(key.data() + bitOffset);
            nullableBit = v;
        }

        for (const auto& col : cols) {
            if (hasNullableCol && col.get_name() == prop && nullableBit.test(nullableColPosit)) {
                return Value(NullType::__NULL__);
            }
            switch (IndexKeyUtils::toValueType(col.type.get_type())) {
                case Value::Type::BOOL: {
                    len = sizeof(bool);
                    break;
                }
                case Value::Type::INT: {
                    len = sizeof(int64_t);
                    break;
                }
                case Value::Type::FLOAT: {
                    len = sizeof(double);
                    break;
                }
                case Value::Type::STRING: {
                    len = *col.type.get_type_length();
                    break;
                }
                case Value::Type::TIME: {
                    len = sizeof(int8_t) * 3 + sizeof(int32_t);
                    break;
                }
                case Value::Type::DATE: {
                    len = sizeof(int8_t) * 2 + sizeof(int16_t);
                    break;
                }
                case Value::Type::DATETIME: {
                    len = sizeof(int32_t) + sizeof(int16_t) + sizeof(int8_t) * 5;
                    break;
                }
                default:
                    len = 0;
            }
            if (hasNullableCol) {
                nullableColPosit -= 1;
            }
            if (col.get_name() == prop) {
                break;
            }
            offset += len;
        }
        auto propVal = key.subpiece(offset, len);
        return decodeValue(propVal, type);
    }

    static VertexIDSlice getIndexVertexID(size_t vIdLen, const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kVertexIndexLen + vIdLen);
        auto offset = rawKey.size() - vIdLen;
        return rawKey.subpiece(offset, vIdLen);
     }

    static VertexIDSlice getIndexSrcId(size_t vIdLen, const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kEdgeIndexLen + vIdLen * 2);
        auto offset = rawKey.size() - (vIdLen << 1) - sizeof(EdgeRanking);
        return rawKey.subpiece(offset, vIdLen);
    }

    static VertexIDSlice getIndexDstId(size_t vIdLen, const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kEdgeIndexLen + vIdLen * 2);
        auto offset = rawKey.size() - vIdLen;
        return rawKey.subpiece(offset, vIdLen);
    }

    static EdgeRanking getIndexRank(size_t vIdLen, const folly::StringPiece& rawKey) {
        CHECK_GE(rawKey.size(), kEdgeIndexLen + vIdLen * 2);
        auto offset = rawKey.size() - vIdLen - sizeof(EdgeRanking);
        return IndexKeyUtils::decodeRank(rawKey.data() + offset);
    }

    static bool isIndexKey(const folly::StringPiece& key) {
        constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
        auto type = readInt<int32_t>(key.data(), len) & kTypeMask;
        return static_cast<uint32_t>(NebulaKeyType::kIndex) == type;
    }

    static IndexID getIndexId(const folly::StringPiece& rawKey) {
        auto offset = sizeof(PartitionID);
        return readInt<IndexID>(rawKey.data() + offset, sizeof(IndexID));
    }

    /**
     * Generate vertex|edge index key for kv store
     **/
    static std::string encodeValues(std::vector<Value>&& values,
                                    const std::vector<nebula::meta::cpp2::ColumnDef>& cols);

    /**
     * param valueTypes ： column type of each index column. If there are no nullable columns
     *                     in the index, the parameter can be empty.
     **/
    static std::string vertexIndexKey(size_t vIdLen, PartitionID partId,
                                      IndexID indexId, const VertexID& vId,
                                      std::string&& values);

    /**
     * param valueTypes ： column type of each index column. If there are no nullable columns
     *                     in the index, the parameter can be empty.
     **/
    static std::string edgeIndexKey(size_t vIdLen, PartitionID partId,
                                    IndexID indexId, const VertexID& srcId,
                                    EdgeRanking rank, const VertexID& dstId,
                                    std::string&& values);

    static std::string indexPrefix(PartitionID partId, IndexID indexId);

    static std::string indexPrefix(PartitionID partId);

    static std::string indexVal(const Value& v);

    static Value parseIndexTTL(const folly::StringPiece& raw);

    static StatusOr<std::string>
    collectIndexValues(RowReader* reader,
                       const std::vector<nebula::meta::cpp2::ColumnDef>& cols);

private:
    IndexKeyUtils() = delete;

    static Status checkValue(const Value& v, bool isNullable);
};

}  // namespace nebula
#endif  // UTILS_INDEXKEYUTILS_H_

