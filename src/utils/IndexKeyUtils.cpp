/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "utils/IndexKeyUtils.h"

namespace nebula {

// static
void IndexKeyUtils::encodeValues(const std::vector<Value>& values, std::string& raw) {
    std::vector<int32_t> colsLen;
    for (auto& value : values) {
        if (value.type() == Value::Type::STRING) {
            colsLen.emplace_back(value.getStr().size());
        }
        raw.append(encodeValue(value));
    }
    for (auto len : colsLen) {
        raw.append(reinterpret_cast<const char*>(&len), sizeof(int32_t));
    }
}

// static
void IndexKeyUtils::encodeValuesWithNull(const std::vector<Value>& values,
                                         const std::vector<Value::Type>& colsType,
                                         std::string& raw) {
    std::vector<int32_t> colsLen;
    // An index has a maximum of 16 columns. 2 byte (16 bit) is enough.
    u_short nullableBitset = 0;

    for (size_t i = 0; i < values.size(); i++) {
        std::string val;
        // if the value is null, the nullable bit should be '1'.
        // And create a string of a fixed length，filled with 0.
        // if the value is not null, encode value.
        if (values[i].isNull()) {
            nullableBitset |= 0x8000 >> i;
            val = encodeNullValue(colsType[i]);
        } else {
            val = encodeValue(values[i]);
        }

        if (colsType[i] == Value::Type::STRING) {
            colsLen.emplace_back(val.size());
        }
        raw.append(val);
    }

    raw.append(reinterpret_cast<const char*>(&nullableBitset), sizeof(u_short));

    for (auto len : colsLen) {
        raw.append(reinterpret_cast<const char*>(&len), sizeof(int32_t));
    }
}

// static
std::string IndexKeyUtils::vertexIndexKey(size_t vIdLen, PartitionID partId,
                                          IndexID indexId, VertexID vId,
                                          const std::vector<Value>& values,
                                          const std::vector<Value::Type>& valueTypes) {
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
    std::string key;
    key.reserve(256);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
    // If have not nullable columns in the index, the valueTypes is empty.
    if (valueTypes.empty()) {
        encodeValues(values, key);
    } else {
        encodeValuesWithNull(values, valueTypes, key);
    }
    key.append(vId.data(), vId.size())
       .append(vIdLen - vId.size(), '\0');
    return key;
}

// static
std::string IndexKeyUtils::edgeIndexKey(size_t vIdLen, PartitionID partId,
                                        IndexID indexId, VertexID srcId,
                                        EdgeRanking rank, VertexID dstId,
                                        const std::vector<Value>& values,
                                        const std::vector<Value::Type>& valueTypes) {
    int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
    std::string key;
    key.reserve(256);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
    // If have not nullable columns in the index, the valueTypes is empty.
    if (valueTypes.empty()) {
        encodeValues(values, key);
    } else {
        encodeValuesWithNull(values, valueTypes, key);
    }
    key.append(srcId.data(), srcId.size())
       .append(vIdLen - srcId.size(), '\0')
       .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
       .append(dstId.data(), dstId.size())
       .append(vIdLen - dstId.size(), '\0');
    return key;
}

// static
std::string IndexKeyUtils::indexPrefix(PartitionID partId, IndexID indexId) {
    PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
    std::string key;
    key.reserve(sizeof(PartitionID) + sizeof(IndexID));
    key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID))
       .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
    return key;
}

// static
StatusOr<std::vector<Value>>
IndexKeyUtils::collectIndexValues(RowReader* reader,
                                  const std::vector<nebula::meta::cpp2::ColumnDef>& cols,
                                  std::vector<Value::Type>& colsType) {
    std::vector<Value> values;
    bool haveNullCol = false;
    if (reader == nullptr) {
        return Status::Error("Invalid row reader");
    }
    for (auto& col : cols) {
        auto v = reader->getValueByName(col.get_name());
        auto isNullable = col.__isset.nullable && *(col.get_nullable());
        if (isNullable && !haveNullCol) {
            haveNullCol = true;
        }
        colsType.emplace_back(IndexKeyUtils::toValueType(col.get_type()));
        auto ret = checkValue(v, isNullable);
        if (!ret.ok()) {
            LOG(ERROR) << "prop error by : " << col.get_name()
                       << ". status : " << ret;
            return ret;
        }
        values.emplace_back(std::move(v));
    }
    if (!haveNullCol) {
        colsType.clear();
    }
    return values;
}

// static
Status IndexKeyUtils::checkValue(const Value& v, bool isNullable) {
    if (!v.isNull()) {
        return Status::OK();
    }

    switch (v.getNull()) {
        case nebula::NullType::UNKNOWN_PROP : {
            return Status::Error("Unknown prop");
        }
        case nebula::NullType::__NULL__ : {
            if (!isNullable) {
                return Status::Error("Not allowed to be null");
            }
            return Status::OK();
        }
        case nebula::NullType::BAD_DATA : {
            return Status::Error("Bad data");
        }
        case nebula::NullType::BAD_TYPE : {
            return Status::Error("Bad type");
        }
        case nebula::NullType::ERR_OVERFLOW : {
            return Status::Error("Data overflow");
        }
        case nebula::NullType::DIV_BY_ZERO : {
            return Status::Error("Div zero");
        }
        case nebula::NullType::NaN : {
            return Status::Error("NaN");
        }
    }
    return Status::OK();
}

}  // namespace nebula

