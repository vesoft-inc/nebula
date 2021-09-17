/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/exec/IndexScanNode2.h"

namespace nebula {
namespace storage {
// Define of Path
std::unique_ptr<Path> Path::make(std::shared_ptr<nebula::meta::cpp2::IndexItem> index,
                                 std::shared_ptr<const nebula::meta::NebulaSchemaProvider> schema,
                                 const std::vector<cpp2::IndexColumnHint>& hints) {
  std::unique_ptr<Path> ret;
  if (hints.back().get_scan_type() == cpp2::ScanType::RANGE) {
    ret.reset(new RangePath(index, schema, hints));
  } else {
    ret.reset(new PrefixPath(index, schema, hints));
  }
  return std::move(ret);
}
Path::Qualified Path::qualified(const folly::StringPiece& key) {
  Qualified ret = Qualified::COMPATIBLE;
  for (auto& func : QFList_) {
    ret = std::min(ret, func(key.toString()));
  }
  return ret;
}
std::string Path::encodeValue(const Value& value, const ColumnTypeDef& colDef, std::string& key) {
  std::string val;
  if (value.type() == Value::Type::STRING) {
    val = IndexKeyUtils::encodeValue(value, *colDef.get_type_length());
    if (val.back() != '\0') {
      QFList_.clear();
      QFList_.emplace_back([](const std::string& key) { return Qualified::UNCERTAIN; });
    }
  } else {
    val = IndexKeyUtils::encodeValue(value);
  }
  key.append(val);
  return val;
}
// End of Path

// Define of RangePath
RangePath::RangePath(std::shared_ptr<nebula::meta::cpp2::IndexItem> index,
                     std::shared_ptr<const nebula::meta::NebulaSchemaProvider> schema,
                     const std::vector<cpp2::IndexColumnHint>& hints)
    : Path(index, schema, hints) {
  buildKey();
}
void RangePath::resetPart(PartitionID partId) {
  std::string p = IndexKeyUtils::indexPrefix(partId);
  startKey_.replace(0, p.size(), p);
  endKey_.replace(0, p.size(), p);
}
Path::Qualified RangePath::qualified(const Map<std::string, Value>& rowData) {
  for (size_t i = 0; i < hints_.size() - 1; i++) {
    auto& hint = hints_[i];
    if (hint.get_begin_value() != rowData.at(hint.get_column_name())) {
      return Qualified::INCOMPATIBLE;
    }
  }
  auto& hint = hints_.back();
  // TODO(hs.zhang): IDL添加开闭区间信息
  if (hint.begin_value_ref().has_value()) {
    bool ret = hint.get_begin_value() < rowData.at(hint.get_column_name());
    if (!ret) {
      return Qualified::INCOMPATIBLE;
    }
  }
  if (hint.end_value_ref().has_value()) {
    bool ret = hint.get_end_value() > rowData.at(hint.get_column_name());
    if (!ret) {
      return Qualified::INCOMPATIBLE;
    }
  }
  return Qualified::COMPATIBLE;
}
void RangePath::buildKey() {
  std::string common;
  common.append(IndexKeyUtils::indexPrefix(0, index_->index_id_ref().value()));
  auto fieldIter = index_->get_fields().begin();
  for (size_t i = 0; i < hints_.size() - 1; i++, fieldIter++) {
    auto& hint = hints_[i];
    CHECK(fieldIter->get_name() == hint.get_column_name());
    auto type = IndexKeyUtils::toValueType(fieldIter->get_type().get_type());
    CHECK(type == Value::Type::STRING && !fieldIter->get_type().type_length_ref().has_value());
    encodeValue(hint.get_begin_value(), fieldIter->get_type(), common);
  }
  auto& hint = hints_.back();
  startKey_ = common;
  endKey_ = std::move(common);
  if (hint.begin_value_ref().has_value()) {
    encodeBeginValue(hint.get_begin_value(), fieldIter->get_type(), startKey_);
  }
  if (hint.end_value_ref().has_value()) {
    encodeEndValue(hint.get_end_value(), fieldIter->get_type(), endKey_);
  } else {
    endKey_.append(startKey_.size() - endKey_.size(), '0xFF');
  }
}
std::string RangePath::encodeBeginValue(const Value& value,
                                        const ColumnTypeDef& colDef,
                                        std::string& key) {
  std::string val = IndexKeyUtils::encodeValue(value, *colDef.get_type_length());
  if (value.type() == Value::Type::STRING && val.back() != '\0') {
    QFList_.emplace_back([&startKey = this->startKey_, startPos = key.size(), length = val.size()](
                             const std::string& key) {
      int ret = memcmp(startKey.data() + startPos, key.data() + startPos, length);
      CHECK_LE(ret, 0);
      return ret == 0 ? Qualified::UNCERTAIN : Qualified::COMPATIBLE;
    });
  } else {
    QFList_.emplace_back([&startKey = this->startKey_,
                          &allowEq = this->eqToStart_,
                          startPos = key.size(),
                          length = val.size()](const std::string& key) {
      int ret = memcmp(startKey.data() + startPos, key.data() + startPos, length);
      CHECK_LE(ret, 0);
      return (ret == 0 && !allowEq) ? Qualified::INCOMPATIBLE : Qualified::COMPATIBLE;
    });
  }
  key += val;
  return val;
}
std::string RangePath::encodeEndValue(const Value& value,
                                      const ColumnTypeDef& colDef,
                                      std::string& key) {
  std::string val = IndexKeyUtils::encodeValue(value, *colDef.get_type_length());
  if (value.type() == Value::Type::STRING && val.back() != '\0') {
    QFList_.emplace_back([&endKey = this->endKey_, startPos = key.size(), length = val.size()](
                             const std::string& key) {
      int ret = memcmp(endKey.data() + startPos, key.data() + startPos, length);
      CHECK_GE(ret, 0);
      return ret == 0 ? Qualified::UNCERTAIN : Qualified::COMPATIBLE;
    });
  } else {
    QFList_.emplace_back([&endKey = this->endKey_,
                          &allowEq = this->eqToEnd_,
                          startPos = key.size(),
                          length = val.size()](const std::string& key) {
      int ret = memcmp(endKey.data() + startPos, key.data() + startPos, length);
      CHECK_GE(ret, 0);
      return (ret == 0 && !allowEq) ? Qualified::INCOMPATIBLE : Qualified::COMPATIBLE;
    });
  }
  key += val;
  return val;
}

// End of RangePath

// Define of PrefixPath
PrefixPath::PrefixPath(std::shared_ptr<nebula::meta::cpp2::IndexItem> index,
                       std::shared_ptr<const nebula::meta::NebulaSchemaProvider> schema,
                       const std::vector<cpp2::IndexColumnHint>& hints)
    : Path(index, schema, hints) {
  buildKey();
}
Path::Qualified PrefixPath::qualified(const Map<std::string, Value>& rowData) {
  for (auto& hint : hints_) {
    if (hint.get_begin_value() != rowData.at(hint.get_column_name())) {
      return Qualified::INCOMPATIBLE;
    }
  }
  return Qualified::COMPATIBLE;
}
void PrefixPath::resetPart(PartitionID partId) {
  std::string p = IndexKeyUtils::indexPrefix(partId);
  prefix_.replace(0, p.size(), p);
}
void PrefixPath::buildKey() {
  std::string common;
  common.append(IndexKeyUtils::indexPrefix(0, index_->index_id_ref().value()));
  auto fieldIter = index_->get_fields().begin();
  for (size_t i = 0; i < hints_.size(); i++, fieldIter++) {
    auto& hint = hints_[i];
    CHECK(fieldIter->get_name() == hint.get_column_name());
    auto type = IndexKeyUtils::toValueType(fieldIter->get_type().get_type());
    CHECK(type == Value::Type::STRING && !fieldIter->get_type().type_length_ref().has_value());
    encodeValue(hint.get_begin_value(), fieldIter->get_type(), common);
  }
  prefix_ = std::move(common);
}
// End of PrefixPath
// Define of IndexScan
nebula::cpp2::ErrorCode IndexScanNode::execute(PartitionID partId) {
  auto ret = resetIter(partId);
  return ret;
}
IndexNode::ErrorOr<Row> IndexScanNode::next(bool& hasNext) {
  hasNext = true;
  while (iter_ && iter_->valid()) {
    if (!checkTTL()) {
      continue;
    }
    auto q = path_->qualified(iter_->key());
    if (q == Path::Qualified::INCOMPATIBLE) {
      continue;
    }
    if (q == Path::Qualified::COMPATIBLE) {
      return decodeFromIndex(iter_->key());
    }
    std::pair<std::string, std::string> kv;
    auto ret = getBaseData(iter_->key(), kv);
    if (ret == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      continue;
    }
    Map<std::string, Value> rowData = decodeFromBase(kv.first, kv.second);
    q = path_->qualified(rowData);
    CHECK(q != Path::Qualified::UNCERTAIN);
    if (q == Path::Qualified::INCOMPATIBLE) {
      continue;
    }
    if (q == Path::Qualified::COMPATIBLE) {
      Row row;
      for (auto& col : requiredColumns_) {
        row.emplace_back(std::move(rowData.at(col)));
      }
    }
  }
  hasNext = false;
  return ErrorOr<Row>(Row());
}
bool IndexScanNode::checkTTL() {
  if (iter_->val().empty() || ttlProps_.first == false) {
    return true;
  }
  auto v = IndexKeyUtils::parseIndexTTL(iter_->val());
  if (CommonUtils::checkDataExpiredForTTL(
          getSchema(), std::move(v), ttlProps_.second.second, ttlProps_.second.first)) {
    return false;
  }
  return true;
}
nebula::cpp2::ErrorCode IndexScanNode::resetIter(PartitionID partId) {
  path_->resetPart(partId);
  nebula::cpp2::ErrorCode ret;
  if (path_->isRange()) {
    auto rangePath = dynamic_cast<RangePath*>(path_.get());
    ret =
        kvstore_->range(spaceId_, partId, rangePath->getStartKey(), rangePath->getEndKey(), &iter_);
  } else {
    auto prefixPath = dynamic_cast<PrefixPath*>(path_.get());
    ret = kvstore_->prefix(spaceId_, partId, prefixPath->getPrefixKey(), &iter_);
  }
  return ret;
}
void IndexScanNode::decodePropFromIndex(folly::StringPiece key,
                                        const Map<std::string, size_t>& colPosMap,
                                        std::vector<Value>& values) {
  size_t offset = sizeof(PartitionID) + sizeof(IndexID);
  size_t len = 0;
  std::bitset<16> nullableBit;
  int8_t nullableColPosit = 15;
  if (indexNullable_) {
    auto bitOffset = key.size() - context_->vIdLen() - sizeof(uint16_t);
    auto v = *reinterpret_cast<const uint16_t*>(key.data() + bitOffset);
    nullableBit = v;
  }
  for (auto& field : index_->get_fields()) {
    int len = 0;
    auto type = IndexKeyUtils::toValueType(field.type.get_type());
    switch (type) {
      case Value::Type::BOOL:
        len = sizeof(bool);
        break;
      case Value::Type::INT:
        len = sizeof(int64_t);
        break;
      case Value::Type::FLOAT:
        len = sizeof(double);
        break;
      case Value::Type::STRING:
        len = *field.type.get_type_length();
        break;
      case Value::Type::TIME:
        len = sizeof(int8_t) * 3 + sizeof(int32_t);
        break;
      case Value::Type::DATE:
        len = sizeof(int8_t) * 2 + sizeof(int16_t);
        break;
      case Value::Type::DATETIME:
        len = sizeof(int32_t) + sizeof(int16_t) + sizeof(int8_t) * 5;
        break;
      default:
        LOG(FATAL) << "Unexpect value type:" << int(field.type.get_type());
    }
    if (colPosMap.count(field.get_name())) {
      if (indexNullable_ && nullableBit.test(nullableColPosit)) {
        values[colPosMap.at(field.get_name())] = Value(NullType::__NULL__);
      } else {
        values[colPosMap.at(field.get_name())] =
            IndexKeyUtils::decodeValue(key.subpiece(offset, len), type);
      }
    }
    offset += len;
    nullableColPosit -= 1;
  }
}

// End of IndexScan
}  // namespace storage
}  // namespace nebula
