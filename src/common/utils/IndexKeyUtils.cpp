/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/utils/IndexKeyUtils.h"

#include <thrift/lib/cpp2/protocol/Serializer.h>

namespace nebula {

// static
std::vector<std::string> IndexKeyUtils::encodeValues(
    std::vector<Value>&& values, const std::vector<nebula::meta::cpp2::ColumnDef>& cols) {
  bool hasNullCol = false;
  // An index has a maximum of 16 columns. 2 byte (16 bit) is enough.
  u_short nullableBitSet = 0;
  auto findGeo = [](const meta::cpp2::ColumnDef& col) {
    return col.get_type().get_type() == meta::cpp2::PropertyType::GEOGRAPHY;
  };
  bool hasGeo = std::find_if(cols.begin(), cols.end(), findGeo) != cols.end();
  // Only support to create index on a single geography column currently;
  DCHECK(!hasGeo || cols.size() == 1);
  std::vector<std::string> indexes;

  if (!hasGeo) {
    std::string index;
    for (size_t i = 0; i < values.size(); i++) {
      auto isNullable = cols[i].nullable_ref().value_or(false);
      if (isNullable) {
        hasNullCol = true;
      }

      if (!values[i].isNull()) {
        // string index need to fill with '\0' if length is less than schema
        if (cols[i].type.type == meta::cpp2::PropertyType::FIXED_STRING) {
          auto len = static_cast<size_t>(*cols[i].type.get_type_length());
          index.append(encodeValue(values[i], len));
        } else {
          index.append(encodeValue(values[i]));
        }
      } else {
        nullableBitSet |= 0x8000 >> i;
        auto type = IndexKeyUtils::toValueType(cols[i].type.get_type());
        index.append(encodeNullValue(type, cols[i].type.get_type_length()));
      }
    }
    indexes.emplace_back(std::move(index));
  } else {
    hasNullCol = cols.back().nullable_ref().value_or(false);
    DCHECK_EQ(values.size(), 1);
    const auto& value = values.back();
    if (!value.isNull()) {
      DCHECK(value.type() == Value::Type::GEOGRAPHY);
      indexes = encodeGeography(value.getGeography());
    } else {
      nullableBitSet |= 0x8000;
      auto type = IndexKeyUtils::toValueType(cols.back().type.get_type());
      indexes.emplace_back(encodeNullValue(type, nullptr));
    }
  }
  // if has nullable field, append nullableBitSet to the end
  if (hasNullCol) {
    for (auto& index : indexes) {
      index.append(reinterpret_cast<const char*>(&nullableBitSet), sizeof(u_short));
    }
  }
  return indexes;
}

// static
std::vector<std::string> IndexKeyUtils::vertexIndexKeys(size_t vIdLen,
                                                        PartitionID partId,
                                                        IndexID indexId,
                                                        const VertexID& vId,
                                                        std::vector<std::string>&& values) {
  int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
  std::vector<std::string> keys;
  keys.reserve(values.size());
  for (const auto& value : values) {
    std::string key;
    key.reserve(256);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
        .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID))
        .append(value)
        .append(vId.data(), vId.size())
        .append(vIdLen - vId.size(), '\0');
    keys.emplace_back(key);
  }
  return keys;
}

// static
std::vector<std::string> IndexKeyUtils::edgeIndexKeys(size_t vIdLen,
                                                      PartitionID partId,
                                                      IndexID indexId,
                                                      const VertexID& srcId,
                                                      EdgeRanking rank,
                                                      const VertexID& dstId,
                                                      std::vector<std::string>&& values) {
  int32_t item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
  std::vector<std::string> keys;
  keys.reserve(values.size());
  for (const auto& value : values) {
    std::string key;
    key.reserve(256);
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t))
        .append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID))
        .append(value)
        .append(srcId.data(), srcId.size())
        .append(vIdLen - srcId.size(), '\0')
        .append(IndexKeyUtils::encodeRank(rank))
        .append(dstId.data(), dstId.size())
        .append(vIdLen - dstId.size(), '\0');
    keys.emplace_back(key);
  }
  return keys;
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
std::string IndexKeyUtils::indexPrefix(PartitionID partId) {
  PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(NebulaKeyType::kIndex);
  std::string key;
  key.reserve(sizeof(PartitionID));
  key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID));
  return key;
}

// static
std::string IndexKeyUtils::indexVal(const Value& v) {
  std::string val, cVal;
  apache::thrift::CompactSerializer::serialize(v, &cVal);
  // A length is needed at here for compatibility in the future
  auto len = cVal.size();
  val.reserve(sizeof(size_t) + len);
  val.append(reinterpret_cast<const char*>(&len), sizeof(size_t)).append(cVal);
  return val;
}

// static
Value IndexKeyUtils::parseIndexTTL(const folly::StringPiece& raw) {
  Value value;
  auto len = *reinterpret_cast<const size_t*>(raw.data());
  apache::thrift::CompactSerializer::deserialize(raw.subpiece(sizeof(size_t), len), value);
  return value;
}

// static
StatusOr<std::vector<std::string>> IndexKeyUtils::collectIndexValues(
    RowReader* reader, const std::vector<nebula::meta::cpp2::ColumnDef>& cols) {
  if (reader == nullptr) {
    return Status::Error("Invalid row reader");
  }
  std::vector<Value> values;
  for (const auto& col : cols) {
    auto v = reader->getValueByName(col.get_name());
    auto isNullable = col.nullable_ref().value_or(false);
    auto ret = checkValue(v, isNullable);
    if (!ret.ok()) {
      LOG(ERROR) << "prop error by : " << col.get_name() << ". status : " << ret;
      return ret;
    }
    values.emplace_back(std::move(v));
  }
  return encodeValues(std::move(values), cols);
}

// static
Status IndexKeyUtils::checkValue(const Value& v, bool isNullable) {
  if (!v.isNull()) {
    return Status::OK();
  }

  switch (v.getNull()) {
    case nebula::NullType::UNKNOWN_PROP: {
      return Status::Error("Unknown prop");
    }
    case nebula::NullType::__NULL__: {
      if (!isNullable) {
        return Status::Error("Not allowed to be null");
      }
      return Status::OK();
    }
    case nebula::NullType::BAD_DATA: {
      return Status::Error("Bad data");
    }
    case nebula::NullType::BAD_TYPE: {
      return Status::Error("Bad type");
    }
    case nebula::NullType::ERR_OVERFLOW: {
      return Status::Error("Data overflow");
    }
    case nebula::NullType::DIV_BY_ZERO: {
      return Status::Error("Div zero");
    }
    case nebula::NullType::NaN: {
      return Status::Error("NaN");
    }
    case nebula::NullType::OUT_OF_RANGE: {
      return Status::Error("Out of range");
    }
  }
  LOG(FATAL) << "Unknown Null type " << static_cast<int>(v.getNull());
}

}  // namespace nebula
