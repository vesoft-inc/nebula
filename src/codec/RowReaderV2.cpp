/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "codec/RowReaderV2.h"

namespace nebula {

using nebula::cpp2::PropertyType;

template <typename T, typename Container>
Value extractIntOrFloat(const folly::StringPiece& data, size_t& offset) {
  int32_t containerOffset;
  memcpy(reinterpret_cast<void*>(&containerOffset), data.data() + offset, sizeof(int32_t));
  if (static_cast<size_t>(containerOffset) >= data.size()) {
    LOG(ERROR) << "Container offset out of bounds. Offset: " << containerOffset
               << ", Data size: " << data.size();
    return Value::kNullValue;
  }
  int32_t containerSize;
  memcpy(reinterpret_cast<void*>(&containerSize), data.data() + containerOffset, sizeof(int32_t));
  containerOffset += sizeof(int32_t);
  Container container;
  for (int32_t i = 0; i < containerSize; ++i) {
    T value;
    if (static_cast<size_t>(containerOffset + sizeof(T)) > data.size()) {
      LOG(ERROR) << "Reading beyond data bounds. Attempting to read at offset: " << containerOffset
                 << ", Data size: " << data.size();
      return Value::kNullValue;
    }
    memcpy(reinterpret_cast<void*>(&value), data.data() + containerOffset, sizeof(T));
    containerOffset += sizeof(T);

    if constexpr (std::is_same_v<Container, List>) {
      container.values.emplace_back(Value(value));
    } else if constexpr (std::is_same_v<Container, Set>) {
      container.values.insert(Value(value));
    }
  }
  return Value(std::move(container));
}

bool RowReaderV2::resetImpl(meta::NebulaSchemaProvider const* schema, folly::StringPiece row) {
  schema_ = schema;
  data_ = row;

  DCHECK(!!schema_);

  size_t numVerBytes = data_[0] & 0x07;
  headerLen_ = numVerBytes + 1;

#ifndef NDEBUG
  // Get the schema version
  SchemaVer schemaVer = 0;
  if (numVerBytes > 0) {
    memcpy(reinterpret_cast<void*>(&schemaVer), &data_[1], numVerBytes);
  }
  DCHECK_EQ(schemaVer, schema_->getVersion());
#endif

  // Null flags
  size_t numNullables = schema_->getNumNullableFields();
  if (numNullables > 0) {
    numNullBytes_ = ((numNullables - 1) >> 3) + 1;
  } else {
    numNullBytes_ = 0;
  }

  return true;
}

bool RowReaderV2::isNull(size_t pos) const {
  static const uint8_t bits[] = {0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01};

  size_t offset = headerLen_ + (pos >> 3);
  int8_t flag = data_[offset] & bits[pos & 0x0000000000000007L];
  return flag != 0;
}

Value RowReaderV2::getValueByName(const std::string& prop) const {
  int64_t index = schema_->getFieldIndex(prop);
  return getValueByIndex(index);
}

Value RowReaderV2::getValueByIndex(const int64_t index) const {
  if (index < 0 || static_cast<size_t>(index) >= schema_->getNumFields()) {
    return Value(NullType::UNKNOWN_PROP);
  }

  auto field = schema_->field(index);
  size_t offset = headerLen_ + numNullBytes_ + field->offset();

  if (field->nullable() && isNull(field->nullFlagPos())) {
    return NullType::__NULL__;
  }

  switch (field->type()) {
    case PropertyType::BOOL: {
      if (data_[offset]) {
        return true;
      } else {
        return false;
      }
    }
    case PropertyType::INT8: {
      return static_cast<int8_t>(data_[offset]);
    }
    case PropertyType::INT16: {
      int16_t val;
      memcpy(reinterpret_cast<void*>(&val), &data_[offset], sizeof(int16_t));
      return val;
    }
    case PropertyType::INT32: {
      int32_t val;
      memcpy(reinterpret_cast<void*>(&val), &data_[offset], sizeof(int32_t));
      return val;
    }
    case PropertyType::INT64: {
      int64_t val;
      memcpy(reinterpret_cast<void*>(&val), &data_[offset], sizeof(int64_t));
      return val;
    }
    case PropertyType::VID: {
      // This is to be compatible with V1, so we treat it as
      // 8-byte long string
      return std::string(&data_[offset], sizeof(int64_t));
    }
    case PropertyType::FLOAT: {
      float val;
      memcpy(reinterpret_cast<void*>(&val), &data_[offset], sizeof(float));
      return val;
    }
    case PropertyType::DOUBLE: {
      double val;
      memcpy(reinterpret_cast<void*>(&val), &data_[offset], sizeof(double));
      return val;
    }
    case PropertyType::STRING: {
      int32_t strOffset;
      int32_t strLen;
      memcpy(reinterpret_cast<void*>(&strOffset), &data_[offset], sizeof(int32_t));
      memcpy(reinterpret_cast<void*>(&strLen), &data_[offset + sizeof(int32_t)], sizeof(int32_t));
      if (static_cast<size_t>(strOffset) == data_.size() && strLen == 0) {
        return std::string();
      }
      CHECK_LT(strOffset, data_.size());
      return std::string(&data_[strOffset], strLen);
    }
    case PropertyType::FIXED_STRING: {
      return std::string(&data_[offset], field->size());
    }
    case PropertyType::TIMESTAMP: {
      Timestamp ts;
      memcpy(reinterpret_cast<void*>(&ts), &data_[offset], sizeof(Timestamp));
      return ts;
    }
    case PropertyType::DATE: {
      Date dt;
      memcpy(reinterpret_cast<void*>(&dt.year), &data_[offset], sizeof(int16_t));
      memcpy(reinterpret_cast<void*>(&dt.month), &data_[offset + sizeof(int16_t)], sizeof(int8_t));
      memcpy(reinterpret_cast<void*>(&dt.day),
             &data_[offset + sizeof(int16_t) + sizeof(int8_t)],
             sizeof(int8_t));
      return dt;
    }
    case PropertyType::TIME: {
      Time t;
      memcpy(reinterpret_cast<void*>(&t.hour), &data_[offset], sizeof(int8_t));
      memcpy(reinterpret_cast<void*>(&t.minute), &data_[offset + sizeof(int8_t)], sizeof(int8_t));
      memcpy(reinterpret_cast<void*>(&t.sec), &data_[offset + 2 * sizeof(int8_t)], sizeof(int8_t));
      memcpy(reinterpret_cast<void*>(&t.microsec),
             &data_[offset + 3 * sizeof(int8_t)],
             sizeof(int32_t));
      return t;
    }
    case PropertyType::DATETIME: {
      DateTime dt;
      int16_t year;
      int8_t month;
      int8_t day;
      int8_t hour;
      int8_t minute;
      int8_t sec;
      int32_t microsec;
      memcpy(reinterpret_cast<void*>(&year), &data_[offset], sizeof(int16_t));
      memcpy(reinterpret_cast<void*>(&month), &data_[offset + sizeof(int16_t)], sizeof(int8_t));
      memcpy(reinterpret_cast<void*>(&day),
             &data_[offset + sizeof(int16_t) + sizeof(int8_t)],
             sizeof(int8_t));
      memcpy(reinterpret_cast<void*>(&hour),
             &data_[offset + sizeof(int16_t) + 2 * sizeof(int8_t)],
             sizeof(int8_t));
      memcpy(reinterpret_cast<void*>(&minute),
             &data_[offset + sizeof(int16_t) + 3 * sizeof(int8_t)],
             sizeof(int8_t));
      memcpy(reinterpret_cast<void*>(&sec),
             &data_[offset + sizeof(int16_t) + 4 * sizeof(int8_t)],
             sizeof(int8_t));
      memcpy(reinterpret_cast<void*>(&microsec),
             &data_[offset + sizeof(int16_t) + 5 * sizeof(int8_t)],
             sizeof(int32_t));
      dt.year = year;
      dt.month = month;
      dt.day = day;
      dt.hour = hour;
      dt.minute = minute;
      dt.sec = sec;
      dt.microsec = microsec;
      return dt;
    }
    case PropertyType::DURATION: {
      Duration d;
      memcpy(reinterpret_cast<void*>(&d.seconds), &data_[offset], sizeof(int64_t));
      memcpy(reinterpret_cast<void*>(&d.microseconds),
             &data_[offset + sizeof(int64_t)],
             sizeof(int32_t));
      memcpy(reinterpret_cast<void*>(&d.months),
             &data_[offset + sizeof(int64_t) + sizeof(int32_t)],
             sizeof(int32_t));
      return d;
    }
    case PropertyType::GEOGRAPHY: {
      int32_t strOffset;
      int32_t strLen;
      memcpy(reinterpret_cast<void*>(&strOffset), &data_[offset], sizeof(int32_t));
      memcpy(reinterpret_cast<void*>(&strLen), &data_[offset + sizeof(int32_t)], sizeof(int32_t));
      if (static_cast<size_t>(strOffset) == data_.size() && strLen == 0) {
        return Value::kEmpty;  // Is it ok to return Value::kEmpty?
      }
      CHECK_LT(strOffset, data_.size());
      auto wkb = std::string(&data_[strOffset], strLen);
      // Parse a geography from the wkb, normalize it and then verify its validity.
      auto geogRet = Geography::fromWKB(wkb, true, true);
      if (!geogRet.ok()) {
        LOG(WARNING) << "Geography::fromWKB failed: " << geogRet.status();
        return Value::kNullBadData;  // Is it ok to return Value::kNullBadData?
      }
      return std::move(geogRet).value();
    }
    case PropertyType::LIST_STRING: {
      int32_t listOffset;
      memcpy(reinterpret_cast<void*>(&listOffset), &data_[offset], sizeof(int32_t));
      if (static_cast<size_t>(listOffset) >= data_.size()) {
        LOG(ERROR) << "List offset out of bounds for LIST_STRING.";
        return Value::kNullValue;
      }
      int32_t listSize;
      memcpy(reinterpret_cast<void*>(&listSize), &data_[listOffset], sizeof(int32_t));
      listOffset += sizeof(int32_t);

      List list;
      for (int32_t i = 0; i < listSize; ++i) {
        int32_t strLen;
        memcpy(reinterpret_cast<void*>(&strLen), &data_[listOffset], sizeof(int32_t));
        listOffset += sizeof(int32_t);
        if (static_cast<size_t>(listOffset + strLen) > data_.size()) {
          LOG(ERROR) << "String length out of bounds for LIST_STRING.";
          return Value::kNullValue;
        }
        std::string str(&data_[listOffset], strLen);
        listOffset += strLen;
        list.values.emplace_back(str);
      }
      return Value(std::move(list));
    }
    case PropertyType::LIST_INT:
      return nebula::extractIntOrFloat<int32_t, List>(data_, offset);
    case PropertyType::LIST_FLOAT:
      return nebula::extractIntOrFloat<float, List>(data_, offset);
    case PropertyType::LIST_LIST_STRING: {
      int32_t numLists;
      if (offset + sizeof(int32_t) > data_.size()) {
        LOG(ERROR) << "Offset out of bounds for LIST_LIST_STRING.";
        return Value(NullType::BAD_DATA);
      }
      memcpy(&numLists, &data_[offset], sizeof(int32_t));
      offset += sizeof(int32_t);
      std::vector<List> listOfLists;
      listOfLists.reserve(numLists);
      for (int i = 0; i < numLists; ++i) {
        int32_t listLen;
        if (offset + sizeof(int32_t) > data_.size()) {
          LOG(ERROR) << "List length out of bounds for LIST_LIST_STRING.";
          return Value(NullType::BAD_DATA);
        }
        memcpy(&listLen, &data_[offset], sizeof(int32_t));
        offset += sizeof(int32_t);
        std::vector<std::string> strings;
        strings.reserve(listLen);
        for (int j = 0; j < listLen; ++j) {
          int32_t strLen;
          if (offset + sizeof(int32_t) > data_.size()) {
            LOG(ERROR) << "String length out of bounds for LIST_LIST_STRING.";
            return Value(NullType::BAD_DATA);
          }
          memcpy(&strLen, &data_[offset], sizeof(int32_t));
          offset += sizeof(int32_t);
          if (offset + strLen > data_.size()) {
            LOG(ERROR) << "String offset out of bounds for LIST_LIST_STRING.";
            return Value(NullType::BAD_DATA);
          }
          strings.push_back(std::string(&data_[offset], strLen));
          offset += strLen;
        }
        List list = List::createFromVector(strings);
        listOfLists.push_back(std::move(list));
      }
      List outerList = List::createFromVector(listOfLists);
      return Value(std::move(outerList));
    }
    case PropertyType::LIST_LIST_INT:
      return nebula::extractIntOrFloat<int64_t, List>(data_, offset);
    case PropertyType::LIST_LIST_FLOAT:
      return nebula::extractIntOrFloat<float, List>(data_, offset);
    case PropertyType::SET_STRING: {
      int32_t setOffset;
      memcpy(reinterpret_cast<void*>(&setOffset), &data_[offset], sizeof(int32_t));
      if (static_cast<size_t>(setOffset) >= data_.size()) {
        LOG(ERROR) << "Set offset out of bounds for SET_STRING.";
        return Value::kNullValue;
      }
      int32_t setSize;
      memcpy(reinterpret_cast<void*>(&setSize), &data_[setOffset], sizeof(int32_t));
      setOffset += sizeof(int32_t);

      Set set;
      std::unordered_set<std::string> uniqueStrings;
      for (int32_t i = 0; i < setSize; ++i) {
        int32_t strLen;
        memcpy(reinterpret_cast<void*>(&strLen), &data_[setOffset], sizeof(int32_t));
        setOffset += sizeof(int32_t);
        if (static_cast<size_t>(setOffset + strLen) > data_.size()) {
          LOG(ERROR) << "String length out of bounds for SET_STRING.";
          return Value::kNullValue;
        }
        std::string str(&data_[setOffset], strLen);
        setOffset += strLen;
        uniqueStrings.insert(std::move(str));
      }
      for (const auto& str : uniqueStrings) {
        set.values.insert(Value(str));
      }
      return Value(std::move(set));
    }
    case PropertyType::SET_INT:
      return nebula::extractIntOrFloat<int32_t, Set>(data_, offset);
    case PropertyType::SET_FLOAT:
      return nebula::extractIntOrFloat<float, Set>(data_, offset);
    case PropertyType::SET_SET_STRING: {
      int32_t numSets;
      if (offset + sizeof(int32_t) > data_.size()) {
        LOG(ERROR) << "Offset out of bounds for SET_SET_STRING.";
        return Value(NullType::BAD_DATA);
      }
      memcpy(&numSets, &data_[offset], sizeof(int32_t));
      offset += sizeof(int32_t);
      std::vector<Set> setsOfSets;
      setsOfSets.reserve(numSets);
      for (int i = 0; i < numSets; ++i) {
        int32_t setLen;
        if (offset + sizeof(int32_t) > data_.size()) {
          LOG(ERROR) << "Set length out of bounds for SET_SET_STRING.";
          return Value(NullType::BAD_DATA);
        }
        memcpy(&setLen, &data_[offset], sizeof(int32_t));
        offset += sizeof(int32_t);
        std::vector<std::string> strings;
        strings.reserve(setLen);
        for (int j = 0; j < setLen; ++j) {
          int32_t strLen;
          if (offset + sizeof(int32_t) > data_.size()) {
            LOG(ERROR) << "String length out of bounds for SET_SET_STRING.";
            return Value(NullType::BAD_DATA);
          }
          memcpy(&strLen, &data_[offset], sizeof(int32_t));
          offset += sizeof(int32_t);
          if (offset + strLen > data_.size()) {
            LOG(ERROR) << "String offset out of bounds for SET_SET_STRING.";
            return Value(NullType::BAD_DATA);
          }
          strings.push_back(std::string(&data_[offset], strLen));
          offset += strLen;
        }
        Set innerSet = Set::createFromVector(strings);
        setsOfSets.push_back(std::move(innerSet));
      }
      Set outerSet = Set::createFromVector(setsOfSets);
      return Value(std::move(outerSet));
    }
    case PropertyType::SET_SET_INT:
      return nebula::extractIntOrFloat<int64_t, Set>(data_, offset);
    case PropertyType::SET_SET_FLOAT:
      return nebula::extractIntOrFloat<float, Set>(data_, offset);
    case PropertyType::UNKNOWN:
      break;
  }
  LOG(FATAL) << "Should not reach here, illegal property type: " << static_cast<int>(field->type());
  return Value::kNullBadType;
}

int64_t RowReaderV2::getTimestamp() const noexcept {
  return *reinterpret_cast<const int64_t*>(data_.begin() + (data_.size() - sizeof(int64_t)));
}

}  // namespace nebula
