/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "storage/exec/IndexScanNode.h"

namespace nebula {
namespace storage {
// Define of Path
Path::Path(nebula::meta::cpp2::IndexItem* index,
           const meta::NebulaSchemaProvider* schema,
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
                                 const meta::NebulaSchemaProvider* schema,
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

QualifiedStrategy::Result Path::qualified(const folly::StringPiece& key) {
  return strategySet_(key);
}

std::string Path::encodeValue(const Value& value,
                              const ColumnTypeDef& colDef,
                              size_t index,
                              std::string& key) {
  std::string val;
  bool isNull = false;
  switch (colDef.get_type()) {
    case ::nebula::cpp2::PropertyType::STRING:
    case ::nebula::cpp2::PropertyType::FIXED_STRING: {
      if (value.type() == Value::Type::NULLVALUE) {
        val = IndexKeyUtils::encodeNullValue(Value::Type::STRING, colDef.get_type_length());
        isNull = true;
      } else {
        val = IndexKeyUtils::encodeValue(value, *colDef.get_type_length());
        if (val.back() != '\0') {
          strategySet_.insert(QualifiedStrategy::constant<QualifiedStrategy::UNCERTAIN>());
        }
      }
      break;
    }
    default: {
      if (value.type() == Value::Type::NULLVALUE) {
        auto vType = IndexKeyUtils::toValueType(colDef.get_type());
        val = IndexKeyUtils::encodeNullValue(vType, colDef.get_type_length());
        isNull = true;
      } else {
        val = IndexKeyUtils::encodeValue(value);
      }
      break;
    }
  }
  // If the current colDef can be null, then it is necessary to additionally determine whether the
  // corresponding value under a nullable is null when parsing the key (the encoding of the maximum
  // value, for example, the encoding of INT_MAX and null are the same, both are 8*' \xFF')
  if (!nullable_.empty() && nullable_[index] == true) {
    if (isNull) {
      strategySet_.insert(QualifiedStrategy::checkNull<true>(index, index_nullable_offset_));
    } else {
      strategySet_.insert(QualifiedStrategy::checkNull<false>(index, index_nullable_offset_));
    }
  } else if (isNull) {
    strategySet_.insert(QualifiedStrategy::constant<QualifiedStrategy::INCOMPATIBLE>());
  }
  key.append(val);
  return val;
}

const std::string& Path::toString() {
  return serializeString_;
}

// End of Path

// Define of RangePath
RangePath::RangePath(nebula::meta::cpp2::IndexItem* index,
                     const meta::NebulaSchemaProvider* schema,
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

QualifiedStrategy::Result RangePath::qualified(const Map<std::string, Value>& rowData) {
  for (size_t i = 0; i < hints_.size() - 1; i++) {
    auto& hint = hints_[i];
    if (hint.get_begin_value() != rowData.at(hint.get_column_name())) {
      return QualifiedStrategy::INCOMPATIBLE;
    }
  }
  auto& hint = hints_.back();
  // TODO(hs.zhang): improve performance.Check include or not during build key.
  if (hint.begin_value_ref().is_set() && !hint.get_begin_value().empty()) {
    bool ret = includeStart_ ? hint.get_begin_value() <= rowData.at(hint.get_column_name())
                             : hint.get_begin_value() < rowData.at(hint.get_column_name());
    if (!ret) {
      return QualifiedStrategy::INCOMPATIBLE;
    }
  }
  if (hint.end_value_ref().is_set() && !hint.get_end_value().empty()) {
    bool ret = includeEnd_ ? hint.get_end_value() >= rowData.at(hint.get_column_name())
                           : hint.get_end_value() > rowData.at(hint.get_column_name());
    if (!ret) {
      return QualifiedStrategy::INCOMPATIBLE;
    }
  }
  return QualifiedStrategy::COMPATIBLE;
}

void RangePath::buildKey() {
  std::string commonIndexPrefix;
  commonIndexPrefix.append(IndexKeyUtils::indexPrefix(0, index_->index_id_ref().value()));
  auto fieldIter = index_->get_fields().begin();
  for (size_t i = 0; i < hints_.size() - 1; i++, fieldIter++) {
    auto& hint = hints_[i];
    CHECK(fieldIter->get_name() == hint.get_column_name());
    auto type = IndexKeyUtils::toValueType(fieldIter->get_type().get_type());
    CHECK(type != Value::Type::STRING || fieldIter->get_type().type_length_ref().has_value());
    encodeValue(hint.get_begin_value(), fieldIter->get_type(), i, commonIndexPrefix);
    serializeString_ +=
        fmt::format("{}={}, ", hint.get_column_name(), hint.get_begin_value().toString());
  }
  auto& hint = hints_.back();
  size_t index = hints_.size() - 1;
  // The first n-1 columnHint has been spelled out the common prefix, and then according to the nth
  // columnHint to determine the RangePath Scan range [a, b). Note that [a, b) must be the range of
  // include begin but exclude end.
  // [startKey, endKey) = common prefix + [a, b)
  auto [a, b] = encodeRange(hint, fieldIter->get_type(), index, commonIndexPrefix.size());
  // left will be `[a`,`(a`, or `[INF`
  std::string left =
      (hint.begin_value_ref().is_set() && !hint.get_begin_value().empty())
          ? fmt::format(
                "{}{}", hint.get_include_begin() ? '[' : '(', hint.get_begin_value().toString())
          : "[-INF";
  // left will be `b]`,`b)`, or `[INF`
  std::string right =
      (hint.end_value_ref().is_set() && !hint.get_end_value().empty())
          ? fmt::format("{}{}", hint.get_end_value().toString(), hint.get_include_end() ? ']' : ')')
          : "INF]";
  serializeString_ += fmt::format("{}={},{}", hint.get_column_name(), left, right);
  startKey_ = commonIndexPrefix + a;
  endKey_ = commonIndexPrefix + b;
  // If `end_value` is not set, `b` will be empty. So `endKey_` should append '\xFF' until
  // endKey_.size() > `totalKeyLength_` to indicate positive infinity prefixed with
  // `commonIndexPrefix`
  if (!hint.end_value_ref().is_set() || hint.get_end_value().empty()) {
    endKey_.append(totalKeyLength_ - endKey_.size() + 1, '\xFF');
  }
}

std::tuple<std::string, std::string> RangePath::encodeRange(
    const cpp2::IndexColumnHint& hint,
    const nebula::meta::cpp2::ColumnTypeDef& colTypeDef,
    size_t colIndex,
    size_t offset) {
  std::string startKey, endKey;
  bool needCheckNullable = !nullable_.empty() && nullable_[colIndex];
  if (hint.end_value_ref().is_set() && !hint.get_end_value().empty()) {
    includeEnd_ = hint.get_include_end();
    auto tmp = encodeEndValue(hint.get_end_value(), colTypeDef, endKey, offset);
    if (memcmp(tmp.data(), std::string(tmp.size(), '\xFF').data(), tmp.size()) != 0) {
      needCheckNullable &= false;
    }
  }
  if (hint.begin_value_ref().is_set() && !hint.get_begin_value().empty()) {
    includeStart_ = hint.get_include_begin();
    encodeBeginValue(hint.get_begin_value(), colTypeDef, startKey, offset);
  }
  if (UNLIKELY(needCheckNullable)) {
    strategySet_.insert(QualifiedStrategy::checkNull(colIndex, index_nullable_offset_));
  }
  if (UNLIKELY(colTypeDef.get_type() == nebula::cpp2::PropertyType::GEOGRAPHY)) {
    strategySet_.insert(QualifiedStrategy::dedupGeoIndex(suffixLength_));
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
  if (colDef.get_type() == ::nebula::cpp2::PropertyType::GEOGRAPHY) {
    val = value.getStr();
  } else if (value.type() == Value::Type::STRING) {
    bool truncated = false;
    val = encodeString(value, *colDef.get_type_length(), truncated);
    greater &= !truncated;
    if (UNLIKELY(truncated)) {
      strategySet_.insert(QualifiedStrategy::compareTruncated<true>(val, offset));
    }
  } else if (value.type() == Value::Type::FLOAT) {
    bool isNaN = false;
    val = encodeFloat(value, isNaN);
    greater |= isNaN;
    // TODO(hs.zhang): Optimize the logic of judging NaN
    strategySet_.insert(QualifiedStrategy::checkNaN(offset));
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
  if (colDef.get_type() == ::nebula::cpp2::PropertyType::GEOGRAPHY) {
    val = value.getStr();
  } else if (value.type() == Value::Type::STRING) {
    bool truncated = false;
    val = encodeString(value, *colDef.get_type_length(), truncated);
    greater |= truncated;
    if (UNLIKELY(truncated)) {
      strategySet_.insert(QualifiedStrategy::compareTruncated<false>(val, offset));
    }
  } else if (value.type() == Value::Type::FLOAT) {
    bool isNaN = false;
    val = encodeFloat(value, isNaN);
    greater |= isNaN;
    if (UNLIKELY(isNaN)) {
      strategySet_.insert(QualifiedStrategy::checkNaN(offset));
    }
  } else {
    val = IndexKeyUtils::encodeValue(value);
  }
  if (greater) {
    val.append(suffixLength_ + 1, '\xFF');
  }
  key += val;
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
                       const meta::NebulaSchemaProvider* schema,
                       const std::vector<cpp2::IndexColumnHint>& hints,
                       int64_t vidLen)
    : Path(index, schema, hints, vidLen) {
  buildKey();
}

QualifiedStrategy::Result PrefixPath::qualified(const Map<std::string, Value>& rowData) {
  for (auto& hint : hints_) {
    if (hint.get_begin_value() != rowData.at(hint.get_column_name())) {
      return QualifiedStrategy::INCOMPATIBLE;
    }
  }
  return QualifiedStrategy::COMPATIBLE;
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
  for (; fieldIter != index_->get_fields().end(); fieldIter++) {
    if (UNLIKELY(fieldIter->get_type().get_type() == nebula::cpp2::PropertyType::GEOGRAPHY)) {
      strategySet_.insert(QualifiedStrategy::dedupGeoIndex(suffixLength_));
      break;
    }
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
      columnHints_(node.columnHints_),
      kvstore_(node.kvstore_),
      indexNullable_(node.indexNullable_),
      requiredColumns_(node.requiredColumns_),
      requiredAndHintColumns_(node.requiredAndHintColumns_),
      ttlProps_(node.ttlProps_),
      needAccessBase_(node.needAccessBase_),
      colPosMap_(node.colPosMap_) {
  if (node.path_->isRange()) {
    path_ = std::make_unique<RangePath>(*dynamic_cast<RangePath*>(node.path_.get()));
  } else {
    path_ = std::make_unique<PrefixPath>(*dynamic_cast<PrefixPath*>(node.path_.get()));
  }
}

::nebula::cpp2::ErrorCode IndexScanNode::init(InitContext& ctx) {
  DCHECK(requiredColumns_.empty());
  ttlProps_ = CommonUtils::ttlProps(getSchema().back().get());
  requiredAndHintColumns_ = ctx.requiredColumns;
  auto schema = getSchema().back();
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
  colPosMap_ = ctx.retColMap;
  // Analyze whether the scan needs to access base data. We check it by if requiredColumns is subset
  // of index fields. In other words, if scan node is required to return property that index does
  // not contain, we need to access base data.
  // TODO(hs.zhang): The performance is better to judge based on whether the string is truncated
  auto tmp = ctx.requiredColumns;
  for (auto& field : index_->get_fields()) {
    // TODO(doodle): Both STRING and FIXED_STRING properties in tag/edge will be transformed into
    // FIXED_STRING in ColumnDef of IndexItem. As for FIXED_STRING in tag/edge property, we don't
    // need to access base data actually.
    if (field.get_type().get_type() == ::nebula::cpp2::PropertyType::FIXED_STRING) {
      continue;
    }
    if (field.get_type().get_type() == ::nebula::cpp2::PropertyType::GEOGRAPHY) {
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
  path_ = Path::make(index_.get(), getSchema().back().get(), columnHints_, context_->vIdLen());
  return ::nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode IndexScanNode::doExecute(PartitionID partId) {
  partId_ = partId;
  auto ret = resetIter(partId);
  return ret;
}

IndexNode::Result IndexScanNode::doNext() {
  for (; iter_ && iter_->valid(); iter_->next()) {
    if (!checkTTL()) {
      continue;
    }
    auto q = path_->qualified(iter_->key());
    if (q == QualifiedStrategy::INCOMPATIBLE) {
      continue;
    }
    bool compatible = q == QualifiedStrategy::COMPATIBLE;
    if (compatible && !needAccessBase_) {
      auto key = iter_->key().toString();
      iter_->next();
      Row row = decodeFromIndex(key);
      return Result(std::move(row));
    }
    std::pair<std::string, std::string> kv;
    auto ret = getBaseData(iter_->key(), kv);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {  // do nothing
    } else if (ret == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      if (LIKELY(!fatalOnBaseNotFound_)) {
        LOG(WARNING) << "base data not found";
      } else {
        LOG(FATAL) << "base data not found";
      }
      continue;
    } else {
      return Result(ret);
    }
    Map<std::string, Value> rowData = decodeFromBase(kv.first, kv.second);
    if (!compatible) {
      q = path_->qualified(rowData);
      CHECK(q != QualifiedStrategy::UNCERTAIN);
      if (q == QualifiedStrategy::INCOMPATIBLE) {
        continue;
      }
    }
    Row row;
    for (auto& col : requiredColumns_) {
      row.emplace_back(std::move(rowData.at(col)));
    }
    iter_->next();
    return Result(std::move(row));
  }
  return Result();
}

bool IndexScanNode::checkTTL() {
  if (iter_->val().empty() || ttlProps_.first == false) {
    return true;
  }
  auto v = IndexKeyUtils::parseIndexTTL(iter_->val());
  if (CommonUtils::checkDataExpiredForTTL(getSchema().back().get(),
                                          std::move(v),
                                          ttlProps_.second.second,
                                          ttlProps_.second.first)) {
    return false;
  }
  return true;
}

nebula::cpp2::ErrorCode IndexScanNode::resetIter(PartitionID partId) {
  path_->resetPart(partId);
  nebula::cpp2::ErrorCode ret = nebula::cpp2::ErrorCode::SUCCEEDED;
  if (path_->isRange()) {
    auto rangePath = dynamic_cast<RangePath*>(path_.get());
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
  if (colPosMap.empty()) {
    return;
  }
  // offset is the start position of index values
  size_t offset = sizeof(PartitionID) + sizeof(IndexID);
  std::bitset<16> nullableBit;
  int8_t nullableColPosit = 15;
  if (indexNullable_) {
    // key has been truncated outside, it **ONLY** contains partId, indexId, indexValue and
    // nullableBits. So the last two bytes is the nullableBits
    auto bitOffset = key.size() - sizeof(uint16_t);
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
      case Value::Type::GEOGRAPHY:  // colPosMap will never need GEOGRAPHY type
        len = 8;
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
