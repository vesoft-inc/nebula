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
           const std::vector<cpp2::IndexColumnHint>& hints,
           int64_t vidLen)
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
    totalKeyLength_ += tmpStr.size();
  }
  for (auto x : nullable_) {
    DVLOG(1) << x;
  }
  DVLOG(1) << nullFlag;
  if (!nullFlag) {
    nullable_.clear();
  } else {
    totalKeyLength_ += 2;
  }
  if (index_->get_schema_id().tag_id_ref().has_value()) {
    totalKeyLength_ += vidLen;
    suffixLength_ = vidLen;
  } else {
    totalKeyLength_ += vidLen * 2 + sizeof(EdgeRanking);
    suffixLength_ = vidLen * 2 + sizeof(EdgeRanking);
  }
}
std::unique_ptr<Path> Path::make(nebula::meta::cpp2::IndexItem* index,
                                 const meta::SchemaProviderIf* schema,
                                 const std::vector<cpp2::IndexColumnHint>& hints,
                                 int64_t vidLen) {
  std::unique_ptr<Path> ret;
  if (hints.empty() || hints.back().get_scan_type() == cpp2::ScanType::PREFIX) {
    ret.reset(new PrefixPath(index, schema, hints, vidLen));

  } else {
    ret.reset(new RangePath(index, schema, hints, vidLen));
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
  bool isNull = false;
  if (value.type() == Value::Type::STRING) {
    val = IndexKeyUtils::encodeValue(value, *colDef.get_type_length());
    if (val.back() != '\0') {
      QFList_.clear();
      QFList_.emplace_back([](const std::string&) { return Qualified::UNCERTAIN; });
    }
  } else if (value.type() == Value::Type::NULLVALUE) {
    auto vtype = IndexKeyUtils::toValueType(colDef.get_type());
    val = IndexKeyUtils::encodeNullValue(vtype, colDef.get_type_length());
    isNull = true;
  } else {
    val = IndexKeyUtils::encodeValue(value);
  }
  if (!nullable_.empty() && nullable_[index] == true) {
    QFList_.emplace_back([isNull, index, offset = index_nullable_offset_](const std::string& k) {
      std::bitset<16> nullableBit;
      auto v = *reinterpret_cast<const u_short*>(k.data() + offset);
      nullableBit = v;
      DVLOG(3) << isNull;
      DVLOG(3) << nullableBit.test(15 - index);
      return nullableBit.test(15 - index) ^ isNull ? Qualified::INCOMPATIBLE
                                                   : Qualified::COMPATIBLE;
    });
  } else if (isNull) {
    QFList_.emplace_back([](const std::string&) { return Qualified::INCOMPATIBLE; });
  }
  key.append(val);
  return val;
}
const std::string& Path::toString() { return serializeString_; }

// End of Path

// Define of RangePath
RangePath::RangePath(nebula::meta::cpp2::IndexItem* index,
                     const meta::SchemaProviderIf* schema,
                     const std::vector<cpp2::IndexColumnHint>& hints,
                     int64_t vidLen)
    : Path(index, schema, hints, vidLen) {
  buildKey();
}
void RangePath::resetPart(PartitionID partId) {
  std::string p = IndexKeyUtils::indexPrefix(partId);
  startKey_ = startKey_.replace(0, p.size(), p);
  endKey_ = endKey_.replace(0, p.size(), p);
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
    CHECK(type != Value::Type::STRING || fieldIter->get_type().type_length_ref().has_value());
    encodeValue(hint.get_begin_value(), fieldIter->get_type(), i, common);
    serializeString_ +=
        fmt::format("{}={}, ", hint.get_column_name(), hint.get_begin_value().toString());
  }
  auto& hint = hints_.back();
  size_t index = hints_.size() - 1;
  auto [a, b] = encodeRange(hint, fieldIter->get_type(), index, common.size());
  std::string left =
      hint.begin_value_ref().is_set()
          ? fmt::format(
                "{}{}", hint.get_include_begin() ? '[' : '(', hint.get_begin_value().toString())
          : "[-INF";
  std::string right =
      hint.end_value_ref().is_set()
          ? fmt::format("{}{}", hint.get_end_value().toString(), hint.get_include_end() ? ']' : ')')
          : "INF]";
  serializeString_ += fmt::format("{}={},{}", hint.get_column_name(), left, right);
  startKey_ = common + a;
  endKey_ = common + b;
  if (!hint.end_value_ref().is_set()) {
    endKey_.append(totalKeyLength_ - endKey_.size() + 1, '\xFF');
  }
  DVLOG(2) << "start key:\n" << folly::hexDump(startKey_.data(), startKey_.size());
  DVLOG(2) << "end key:\n" << folly::hexDump(endKey_.data(), endKey_.size());
}
std::tuple<std::string, std::string> RangePath::encodeRange(
    const cpp2::IndexColumnHint& hint,
    const nebula::meta::cpp2::ColumnTypeDef& colTypeDef,
    size_t colIndex,
    size_t offset) {
  std::string startKey, endKey;
  bool needCheckNullable = !nullable_.empty() && nullable_[colIndex];
  if (hint.end_value_ref().is_set()) {
    includeEnd_ = hint.get_include_end();
    auto tmp = encodeEndValue(hint.get_end_value(), colTypeDef, endKey, offset);
    if (memcmp(tmp.data(), std::string(tmp.size(), '\xFF').data(), tmp.size() != 0)) {
      needCheckNullable &= false;
    }
  }
  if (hint.begin_value_ref().is_set()) {
    includeStart_ = hint.get_include_begin();
    encodeBeginValue(hint.get_begin_value(), colTypeDef, startKey, offset);
  }
  if (UNLIKELY(needCheckNullable)) {
    QFList_.emplace_back([colIndex, offset = index_nullable_offset_](const std::string& k) {
      DVLOG(1) << "check null";
      std::bitset<16> nullableBit;
      auto v = *reinterpret_cast<const u_short*>(k.data() + offset);
      nullableBit = v;
      DVLOG(1) << nullableBit;
      DVLOG(1) << nullableBit.test(15 - colIndex);
      return nullableBit.test(15 - colIndex) ? Qualified::INCOMPATIBLE : Qualified::COMPATIBLE;
    });
  }
  return {startKey, endKey};
}
std::string RangePath::encodeBeginValue(const Value& value,
                                        const ColumnTypeDef& colDef,
                                        std::string& key,
                                        size_t offset) {
  std::string val;
  bool greater = !includeStart_;
  CHECK_NE(value.type(), Value::Type::NULLVALUE);
  if (value.type() == Value::Type::STRING) {
    bool truncated = false;
    val = encodeString(value, *colDef.get_type_length(), truncated);
    greater &= !truncated;
    if (UNLIKELY(truncated)) {
      QFList_.emplace_back([val, startPos = offset](const std::string& k) {
        int ret = memcmp(val.data(), k.data() + startPos, val.size());
        DVLOG(1) << folly::hexDump(val.data(), val.size());
        DVLOG(1) << folly::hexDump(k.data(), k.size());
        CHECK_LE(ret, 0);
        return ret == 0 ? Qualified::UNCERTAIN : Qualified::COMPATIBLE;
      });
    }
  } else if (value.type() == Value::Type::FLOAT) {
    bool isNaN = false;
    val = encodeFloat(value, isNaN);
    greater |= isNaN;
    // TODO(hs.zhang): Optimize the logic of judging NaN
    QFList_.emplace_back([startPos = offset, length = val.size()](const std::string& k) {
      int ret = memcmp("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", k.data() + startPos, length);
      return ret == 0 ? Qualified::INCOMPATIBLE : Qualified::COMPATIBLE;
    });
  } else {
    val = IndexKeyUtils::encodeValue(value);
  }
  if (greater) {
    val.append(suffixLength_ + 1, '\xFF');
  }
  key += val;
  return val;
}
std::string RangePath::encodeEndValue(const Value& value,
                                      const ColumnTypeDef& colDef,
                                      std::string& key,
                                      size_t offset) {
  CHECK_NE(value.type(), Value::Type::NULLVALUE);
  std::string val;
  bool greater = includeEnd_;
  if (value.type() == Value::Type::STRING) {
    bool truncated = false;
    val = encodeString(value, *colDef.get_type_length(), truncated);
    greater |= truncated;
    if (UNLIKELY(truncated)) {
      QFList_.emplace_back([val, startPos = offset](const std::string& k) {
        int ret = memcmp(val.data(), k.data() + startPos, val.size());
        DVLOG(3) << folly::hexDump(val.data(), val.size());
        DVLOG(3) << folly::hexDump(k.data(), k.size());
        CHECK_GE(ret, 0);
        return ret == 0 ? Qualified::UNCERTAIN : Qualified::COMPATIBLE;
      });
    }
  } else if (value.type() == Value::Type::FLOAT) {
    bool isNaN = false;
    val = encodeFloat(value, isNaN);
    greater |= isNaN;
    if (UNLIKELY(isNaN)) {
      QFList_.emplace_back([startPos = offset, length = val.size()](const std::string& k) {
        DVLOG(3) << "check NaN:" << startPos << "\t" << length;
        DVLOG(3) << '\n' << folly::hexDump(k.data(), k.size());
        int ret = memcmp("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", k.data() + startPos, length);
        return ret == 0 ? Qualified::INCOMPATIBLE : Qualified::COMPATIBLE;
      });
    }
  } else {
    val = IndexKeyUtils::encodeValue(value);
  }
  DVLOG(2) << includeEnd_;
  DVLOG(2) << greater;
  if (greater) {
    val.append(suffixLength_ + 1, '\xFF');
  }
  key += val;
  DVLOG(2) << '\n' << folly::hexDump(key.data(), key.size());
  return val;
}
inline std::string RangePath::encodeString(const Value& value, size_t len, bool& truncated) {
  std::string val = IndexKeyUtils::encodeValue(value);
  if (val.size() < len) {
    val.append(len - val.size(), '\x00');
  } else {
    val = val.substr(0, len);
    truncated = true;
  }
  return val;
}
std::string RangePath::encodeFloat(const Value& value, bool& isNaN) {
  std::string val = IndexKeyUtils::encodeValue(value);
  // check NaN
  if (UNLIKELY(memcmp(val.data(), "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", val.size()) == 0)) {
    isNaN = true;
  }
  return val;
}

// End of RangePath

// Define of PrefixPath
PrefixPath::PrefixPath(nebula::meta::cpp2::IndexItem* index,
                       const meta::SchemaProviderIf* schema,
                       const std::vector<cpp2::IndexColumnHint>& hints,
                       int64_t vidLen)
    : Path(index, schema, hints, vidLen) {
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
  prefix_ = prefix_.replace(0, p.size(), p);
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
    serializeString_ +=
        fmt::format("{}={}, ", hint.get_column_name(), hint.get_begin_value().toString());
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
      requiredAndHintColumns_(node.requiredAndHintColumns_),
      ttlProps_(node.ttlProps_),
      needAccessBase_(node.needAccessBase_) {
  if (node.path_->isRange()) {
    path_ = std::make_unique<RangePath>(*dynamic_cast<RangePath*>(node.path_.get()));
  } else {
    path_ = std::make_unique<PrefixPath>(*dynamic_cast<PrefixPath*>(node.path_.get()));
  }
}

::nebula::cpp2::ErrorCode IndexScanNode::init(InitContext& ctx) {
  DCHECK(requiredColumns_.empty());
  ttlProps_ = CommonUtils::ttlProps(getSchema());
  requiredAndHintColumns_ = ctx.requiredColumns;

  for (auto& hint : columnHints_) {
    requiredAndHintColumns_.insert(hint.get_column_name());
  }
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
    if (field.get_type().get_type() == PropertyType::GEOGRAPHY) {
      continue;
    }
    tmp.erase(field.get_name());
  }
  tmp.erase(kVid);
  tmp.erase(kTag);
  tmp.erase(kRank);
  tmp.erase(kSrc);
  tmp.erase(kDst);
  tmp.erase(kType);
  needAccessBase_ = !tmp.empty();
  path_ = Path::make(index_.get(), getSchema(), columnHints_, context_->vIdLen());
  return ::nebula::cpp2::ErrorCode::SUCCEEDED;
}
nebula::cpp2::ErrorCode IndexScanNode::doExecute(PartitionID partId) {
  partId_ = partId;
  auto ret = resetIter(partId);
  return ret;
}
IndexNode::ErrorOr<Row> IndexScanNode::doNext(bool& hasNext) {
  DVLOG(3) << "x";
  DVLOG(3) << columnHints_.size();
  hasNext = true;
  for (; iter_ && iter_->valid(); iter_->next()) {
    DLOG(INFO) << '\n' << folly::hexDump(iter_->key().data(), iter_->key().size());
    if (!checkTTL()) {
      continue;
    }
    DVLOG(3) << "x";
    auto q = path_->qualified(iter_->key());
    if (q == Path::Qualified::INCOMPATIBLE) {
      continue;
    }
    DVLOG(3) << "x";
    bool compatible = q == Path::Qualified::COMPATIBLE;
    if (compatible && !needAccessBase_) {
      DVLOG(3) << 123;
      auto key = iter_->key().toString();
      iter_->next();
      auto ret = decodeFromIndex(key);
      DLOG(INFO) << ret;
      return ret;
    }
    DVLOG(3) << "x";
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
    DVLOG(3) << "x";

    Map<std::string, Value> rowData = decodeFromBase(kv.first, kv.second);
    DVLOG(3) << "x";
    for (auto& iter : rowData) {
      DVLOG(3) << iter.first << ":" << iter.second;
    }
    if (!compatible) {
      DVLOG(3) << "x";
      DVLOG(3) << path_.get();
      q = path_->qualified(rowData);
      CHECK(q != Path::Qualified::UNCERTAIN);
      if (q == Path::Qualified::INCOMPATIBLE) {
        continue;
      }
    }
    DVLOG(3) << 123;
    Row row;
    for (auto& col : requiredColumns_) {
      DVLOG(3) << col;
      DVLOG(3) << rowData.at(col);
      row.emplace_back(std::move(rowData.at(col)));
    }
    iter_->next();
    DLOG(INFO) << row;
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
  nebula::cpp2::ErrorCode ret = nebula::cpp2::ErrorCode::SUCCEEDED;
  if (path_->isRange()) {
    auto rangePath = dynamic_cast<RangePath*>(path_.get());
    DVLOG(1) << '\n'
             << folly::hexDump(rangePath->getStartKey().data(), rangePath->getStartKey().size());
    DVLOG(1) << '\n'
             << folly::hexDump(rangePath->getEndKey().data(), rangePath->getEndKey().size());
    std::unique_ptr<::nebula::kvstore::KVIterator> iter;
    ret =
        kvstore_->range(spaceId_, partId, rangePath->getStartKey(), rangePath->getEndKey(), &iter);
    iter_ = std::move(iter);
  } else {
    auto prefixPath = dynamic_cast<PrefixPath*>(path_.get());
    DVLOG(1) << '\n'
             << folly::hexDump(prefixPath->getPrefixKey().data(),
                               prefixPath->getPrefixKey().size());
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
std::string IndexScanNode::identify() {
  return fmt::format("{}(IndexID={}, Path=({}))", name_, indexId_, path_->toString());
}
// End of IndexScan
}  // namespace storage
}  // namespace nebula
