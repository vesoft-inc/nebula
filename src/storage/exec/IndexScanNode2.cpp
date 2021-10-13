/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/exec/IndexScanNode2.h"

namespace nebula {
namespace storage {
// Define of Path
Path::Path(nebula::meta::cpp2::IndexItem* index,
           const meta::SchemaProviderIf* schema,
           const std::vector<cpp2::IndexColumnHint>& hints)
    : index_(index), schema_(schema), hints_(hints) {
  bool nullFlag = false;
  for (auto field : index->get_fields()) {
    bool tmp = field.nullable_ref().value_or(false);
    nullable_.push_back(tmp);
    nullFlag |= tmp;
    // TODO: improve performance of compute nullable offset in index_key
    auto type = IndexKeyUtils::toValueType(field.get_type().get_type());
    auto tmpStr = IndexKeyUtils::encodeNullValue(type, field.get_type().get_type_length());
    index_nullable_offset_ += tmpStr.size();
  }
  for (auto x : nullable_) {
    DVLOG(1) << x;
  }
  DVLOG(1) << nullFlag;
  if (!nullFlag) {
    nullable_.clear();
  }
}
std::unique_ptr<Path> Path::make(nebula::meta::cpp2::IndexItem* index,
                                 const meta::SchemaProviderIf* schema,
                                 const std::vector<cpp2::IndexColumnHint>& hints) {
  std::unique_ptr<Path> ret;
  if (hints.back().get_scan_type() == cpp2::ScanType::RANGE) {
    ret.reset(new RangePath(index, schema, hints));
  } else {
    ret.reset(new PrefixPath(index, schema, hints));
  }
  return ret;
}
Path::Qualified Path::qualified(const folly::StringPiece& key) {
  Qualified ret = Qualified::COMPATIBLE;
  for (auto& func : QFList_) {
    ret = std::min(ret, func(key.toString()));
  }
  return ret;
}
std::string Path::encodeValue(const Value& value,
                              const ColumnTypeDef& colDef,
                              size_t index,
                              std::string& key) {
  std::string val;
  if (value.type() == Value::Type::STRING) {
    val = IndexKeyUtils::encodeValue(value, *colDef.get_type_length());
    if (val.back() != '\0') {
      QFList_.clear();
      QFList_.emplace_back([](const std::string&) { return Qualified::UNCERTAIN; });
    }
  } else {
    val = IndexKeyUtils::encodeValue(value);
  }
  if (!nullable_.empty() && nullable_[index] == true) {
    QFList_.emplace_back(
        [isNull = value.isNull(), index, offset = index_nullable_offset_](const std::string& k) {
          std::bitset<16> nullableBit;
          auto v = *reinterpret_cast<const u_short*>(k.data() + offset);
          nullableBit = v;
          DVLOG(3) << isNull;
          DVLOG(3) << nullableBit.test(15 - index);
          return nullableBit.test(15 - index) ^ isNull ? Qualified::INCOMPATIBLE
                                                       : Qualified::COMPATIBLE;
        });
  }
  key.append(val);
  return val;
}
// End of Path

// Define of RangePath
RangePath::RangePath(nebula::meta::cpp2::IndexItem* index,
                     const meta::SchemaProviderIf* schema,
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
  // TODO(hs.zhang): improve performance.Check include or not during build key.
  if (hint.begin_value_ref().is_set()) {
    bool ret = includeStart_ ? hint.get_begin_value() <= rowData.at(hint.get_column_name())
                             : hint.get_begin_value() < rowData.at(hint.get_column_name());
    if (!ret) {
      return Qualified::INCOMPATIBLE;
    }
  }
  if (hint.end_value_ref().is_set()) {
    DVLOG(2) << includeEnd_;
    bool ret = includeEnd_ ? hint.get_end_value() >= rowData.at(hint.get_column_name())
                           : hint.get_end_value() > rowData.at(hint.get_column_name());
    DVLOG(2) << ret;
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
    encodeValue(hint.get_begin_value(), fieldIter->get_type(), i, common);
  }
  auto& hint = hints_.back();
  startKey_ = common;
  endKey_ = std::move(common);
  size_t index = hints_.size() - 1;
  if (hint.begin_value_ref().is_set()) {
    encodeBeginValue(hint.get_begin_value(), fieldIter->get_type(), index, startKey_);
    includeStart_ = hint.get_include_begin();
  } else {
    includeStart_ = false;
  }
  if (hint.end_value_ref().is_set()) {
    encodeEndValue(hint.get_end_value(), fieldIter->get_type(), index, endKey_);
    includeEnd_ = hint.get_include_end();
  } else {
    endKey_.append(startKey_.size() - endKey_.size(), '\xFF');
    includeEnd_ = true;
  }
}
std::string RangePath::encodeBeginValue(const Value& value,
                                        const ColumnTypeDef& colDef,
                                        size_t index,
                                        std::string& key) {
  std::string val = IndexKeyUtils::encodeValue(value, colDef.type_length_ref().value_or(0));
  if (value.type() == Value::Type::STRING && val.back() != '\0') {
    QFList_.emplace_back([&startKey = this->startKey_, startPos = key.size(), length = val.size()](
                             const std::string& k) {
      int ret = memcmp(startKey.data() + startPos, k.data() + startPos, length);
      CHECK_LE(ret, 0);
      return ret == 0 ? Qualified::UNCERTAIN : Qualified::COMPATIBLE;
    });
  } else {
    QFList_.emplace_back([&startKey = this->startKey_,
                          &allowEq = this->includeStart_,
                          startPos = key.size(),
                          length = val.size()](const std::string& k) {
      int ret = memcmp(startKey.data() + startPos, k.data() + startPos, length);
      DVLOG(3) << '\n' << folly::hexDump(startKey.data() + startPos, length);
      DVLOG(3) << '\n' << folly::hexDump(k.data() + startPos, length);
      CHECK_LE(ret, 0);
      return (ret == 0 && !allowEq) ? Qualified::INCOMPATIBLE : Qualified::COMPATIBLE;
    });
  }
  if (value.isFloat()) {
    QFList_.emplace_back([startPos = key.size(), length = val.size()](const std::string& k) {
      std::string s(length, '\xFF');
      return memcmp(k.data() + startPos, s.data(), length) == 0 ? Qualified::INCOMPATIBLE
                                                                : Qualified::COMPATIBLE;
    });
  } else if (!nullable_.empty() && nullable_[index] == true) {
    QFList_.emplace_back([index, offset = index_nullable_offset_](const std::string& k) {
      DVLOG(1) << "check null";
      std::bitset<16> nullableBit;
      auto v = *reinterpret_cast<const u_short*>(k.data() + offset);
      nullableBit = v;
      DVLOG(1) << nullableBit;
      DVLOG(1) << nullableBit.test(15 - index);
      return nullableBit.test(15 - index) ? Qualified::INCOMPATIBLE : Qualified::COMPATIBLE;
    });
  }
  key += val;
  return val;
}
std::string RangePath::encodeEndValue(const Value& value,
                                      const ColumnTypeDef& colDef,
                                      size_t index,
                                      std::string& key) {
  std::string val = IndexKeyUtils::encodeValue(value, colDef.type_length_ref().value_or(0));
  if (value.type() == Value::Type::STRING && val.back() != '\0') {
    QFList_.emplace_back([&endKey = this->endKey_, startPos = key.size(), length = val.size()](
                             const std::string& k) {
      int ret = memcmp(endKey.data() + startPos, k.data() + startPos, length);
      CHECK_GE(ret, 0);
      return ret == 0 ? Qualified::UNCERTAIN : Qualified::COMPATIBLE;
    });
  } else {
    QFList_.emplace_back([&endKey = this->endKey_,
                          &allowEq = this->includeEnd_,
                          startPos = key.size(),
                          length = val.size()](const std::string& k) {
      int ret = memcmp(endKey.data() + startPos, k.data() + startPos, length);
      CHECK_GE(ret, 0);
      return (ret == 0 && !allowEq) ? Qualified::INCOMPATIBLE : Qualified::COMPATIBLE;
    });
  }
  if (value.isFloat()) {
    QFList_.emplace_back([startPos = key.size(), length = val.size()](const std::string& k) {
      std::string s(length, '\xFF');
      return memcmp(k.data() + startPos, s.data(), length) == 0 ? Qualified::INCOMPATIBLE
                                                                : Qualified::COMPATIBLE;
    });
  } else if (!nullable_.empty() && nullable_[index] == true) {
    QFList_.emplace_back([index, offset = index_nullable_offset_](const std::string& k) {
      std::bitset<16> nullableBit;
      auto v = *reinterpret_cast<const u_short*>(k.data() + offset);
      nullableBit = v;
      return nullableBit.test(15 - index) ? Qualified::INCOMPATIBLE : Qualified::COMPATIBLE;
    });
  }
  key += val;
  return val;
}

// End of RangePath

// Define of PrefixPath
PrefixPath::PrefixPath(nebula::meta::cpp2::IndexItem* index,
                       const meta::SchemaProviderIf* schema,
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
    CHECK(type != Value::Type::STRING || fieldIter->get_type().type_length_ref().has_value());
    encodeValue(hint.get_begin_value(), fieldIter->get_type(), i, common);
  }
  prefix_ = std::move(common);
}
// End of PrefixPath
// Define of IndexScan

IndexScanNode::IndexScanNode(const IndexScanNode& node)
    : IndexNode(node),
      partId_(node.partId_),
      indexId_(node.indexId_),
      index_(node.index_),
      indexNullable_(node.indexNullable_),
      columnHints_(node.columnHints_),
      kvstore_(node.kvstore_),
      requiredColumns_(node.requiredColumns_),
      ttlProps_(node.ttlProps_) {}

::nebula::cpp2::ErrorCode IndexScanNode::init(InitContext& ctx) {
  for (auto& col : ctx.requiredColumns) {
    requiredColumns_.push_back(col);
  }
  ctx.returnColumns = requiredColumns_;
  for (size_t i = 0; i < ctx.returnColumns.size(); i++) {
    ctx.retColMap[ctx.returnColumns[i]] = i;
  }
  // Analyze whether the scan needs to access base data.
  // TODO(hs.zhang): The performance is better to judge based on whether the string is truncated
  auto tmp = ctx.requiredColumns;
  for (auto& field : index_->get_fields()) {
    if (field.get_type().get_type() == PropertyType::FIXED_STRING) {
      continue;
    }
    tmp.erase(field.get_name());
  }
  tmp.erase(kVid);
  tmp.erase(kTag);
  tmp.erase(kRank);
  tmp.erase(kSrc);
  tmp.erase(kDst);
  needAccessBase_ = !tmp.empty();
  path_ = Path::make(index_.get(), getSchema(), columnHints_);
  return ::nebula::cpp2::ErrorCode::SUCCEEDED;
}
nebula::cpp2::ErrorCode IndexScanNode::doExecute(PartitionID partId) {
  partId_ = partId;
  auto ret = resetIter(partId);
  return ret;
}
IndexNode::ErrorOr<Row> IndexScanNode::doNext(bool& hasNext) {
  hasNext = true;
  for (; iter_ && iter_->valid(); iter_->next()) {
    DVLOG(1) << '\n' << folly::hexDump(iter_->key().data(), iter_->key().size());
    if (!checkTTL()) {
      continue;
    }
    auto q = path_->qualified(iter_->key());
    if (q == Path::Qualified::INCOMPATIBLE) {
      continue;
    }
    bool compatible = q == Path::Qualified::COMPATIBLE;
    if (compatible && !needAccessBase_) {
      DVLOG(3) << 123;
      auto key = iter_->key();
      iter_->next();
      return decodeFromIndex(key);
    }
    DVLOG(3) << 123;
    std::pair<std::string, std::string> kv;
    auto ret = getBaseData(iter_->key(), kv);
    if (ret == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      if (LIKELY(!fatalOnBaseNotFound_)) {
        LOG(WARNING) << "base data not found";
      } else {
        LOG(FATAL) << "base data not found";
      }
      continue;
    }
    Map<std::string, Value> rowData = decodeFromBase(kv.first, kv.second);
    if (!compatible) {
      DVLOG(3) << 123;
      q = path_->qualified(rowData);
      CHECK(q != Path::Qualified::UNCERTAIN);
      if (q == Path::Qualified::INCOMPATIBLE) {
        DVLOG(3) << 123;
        continue;
      }
    }
    DVLOG(3) << 123;
    Row row;
    for (auto& col : requiredColumns_) {
      row.emplace_back(std::move(rowData.at(col)));
    }
    iter_->next();
    return row;
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
    ret = kvstore_->range(spaceId_,
                          partId,
                          rangePath->includeStart(),
                          rangePath->getStartKey(),
                          rangePath->includeEnd(),
                          rangePath->getEndKey(),
                          &iter_);
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
