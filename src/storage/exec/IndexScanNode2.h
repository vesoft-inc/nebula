/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once
#include <cstring>
#include <functional>

#include "common/base/Base.h"
#include "common/datatypes/DataSet.h"
#include "common/utils/IndexKeyUtils.h"
#include "folly/Optional.h"
#include "interface/gen-cpp2/meta_types.h"
#include "interface/gen-cpp2/storage_types.h"
#include "storage/CommonUtils.h"
#include "storage/exec/IndexNode.h"
namespace nebula {
namespace storage {
template <typename K, typename V>
using Map = folly::F14FastMap<K, V>;

class Path;
class IndexScanNode : public IndexNode {
 public:
  IndexScanNode(RuntimeContext* context,
                IndexID indexId,
                const std::vector<cpp2::IndexColumnHint>& columnHists)
      : IndexNode(context), indexId_(indexId), columnHists_(columnHists) {}
  nebula::cpp2::ErrorCode execute(PartitionID partId) override {
    auto ret = resetIter(partId);
    return ret;
  }
  StatusOr<Row> next(bool& hasNext) {
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
    return Status::OK();
  }

 protected:
  virtual Row decodeFromIndex(folly::StringPiece key) = 0;
  virtual nebula::cpp2::ErrorCode getBaseData(folly::StringPiece key,
                                              std::pair<std::string, std::string>& kv) = 0;
  virtual Map<std::string, Value> decodeFromBase(const std::string& key,
                                                 const std::string& value) = 0;
  virtual const meta::SchemaProviderIf* getSchema() = 0;
  std::pair<bool, std::pair<int64_t, std::string>> ttlProps_;
  std::unique_ptr<std::pair<bool, std::pair<int64_t, std::string>>> ttlProps_;
  bool checkTTL() {
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
  nebula::cpp2::ErrorCode resetIter(PartitionID partId) {
    path_->resetPart(partId);
    nebula::cpp2::ErrorCode ret;
    if (path_->isRange()) {
      auto rangePath = dynamic_cast<RangePath*>(path_.get());
      ret = kvstore_->range(
          spaceId_, partId, rangePath->getStartKey(), rangePath->getEndKey(), &iter_);
    } else {
      auto prefixPath = dynamic_cast<PrefixPath*>(path_.get());
      ret = kvstore_->prefix(spaceId_, partId, prefixPath->getPrefixKey(), &iter_);
    }
    return ret;
  }
  const IndexID indexId_;
  const std::vector<cpp2::IndexColumnHint>& columnHists_;
  std::unique_ptr<Path> path_;
  std::unique_ptr<kvstore::KVIterator> iter_;
  nebula::kvstore::KVStore* kvstore_;
  std::vector<std::string> requiredColumns_;
};
/**
 * Path
 *
 * Path表示一个Index查询范围（range或prefix）。
 */
class Path {
 public:
  using ColumnTypeDef = ::nebula::meta::cpp2::ColumnTypeDef;
  Path(std::shared_ptr<nebula::meta::cpp2::IndexItem> index,
       std::shared_ptr<const nebula::meta::NebulaSchemaProvider> schema,
       const std::vector<cpp2::IndexColumnHint>& hints)
      : index_(index), schema_(schema), hints_(hints) {}
  enum class Qualified : int16_t { INCOMPATIBLE = 0, UNCERTAIN = 1, COMPATIBLE = 2 };
  using QualifiedFunction = std::function<Qualified(const std::string&)>;
  virtual Qualified qualified(const folly::StringPiece& key) {
    Qualified ret = Qualified::COMPATIBLE;
    for (auto& func : QFList_) {
      ret = std::min(ret, func(key.toString()));
    }
    return ret;
  }
  virtual Qualified qualified(const Map<std::string, Value>& rowData) = 0;
  virtual ~Path() = default;
  virtual void resetPart(PartitionID partId) = 0;
  virtual bool isRange() { return dynamic_cast<RangePath*>(this) != nullptr; }

 protected:
  std::string encodeValue(const Value& value, const ColumnTypeDef& colDef, std::string& key) {
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
  std::vector<QualifiedFunction> QFList_;
  std::shared_ptr<nebula::meta::cpp2::IndexItem> index_;
  std::shared_ptr<const nebula::meta::NebulaSchemaProvider> schema_;
  const std::vector<cpp2::IndexColumnHint>& hints_;
  std::string startKey_, endKey_;
  bool eqToStart_, eqToEnd_;
};
class PrefixPath : public Path {
 public:
  PrefixPath(std::shared_ptr<nebula::meta::cpp2::IndexItem> index,
             std::shared_ptr<const nebula::meta::NebulaSchemaProvider> schema,
             const std::vector<cpp2::IndexColumnHint>& hints)
      : Path(index, schema, hints) {
    buildKey();
  }
  const std::string& getPrefixKey();
  Qualified qualified(const Map<std::string, Value>& rowData) override {
    for (auto& hint : hints_) {
      if (hint.get_begin_value() != rowData.at(hint.get_column_name())) {
        return Qualified::INCOMPATIBLE;
      }
    }
    return Qualified::COMPATIBLE;
  }
  void resetPart(PartitionID partId) override {
    std::string p = IndexKeyUtils::indexPrefix(partId);
    prefix_.replace(0, p.size(), p);
  }

 private:
  std::string prefix_;
  void buildKey() {
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
};
class RangePath : public Path {
 public:
  RangePath(std::shared_ptr<nebula::meta::cpp2::IndexItem> index,
            std::shared_ptr<const nebula::meta::NebulaSchemaProvider> schema,
            const std::vector<cpp2::IndexColumnHint>& hints)
      : Path(index, schema, hints) {
    buildKey();
  }
  void resetPart(PartitionID partId) override {
    std::string p = IndexKeyUtils::indexPrefix(partId);
    startKey_.replace(0, p.size(), p);
    endKey_.replace(0, p.size(), p);
  }

  Qualified qualified(const Map<std::string, Value>& rowData) override {
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
  inline bool eqToStart() { return eqToStart_; }
  inline bool eqToEnd() { return eqToEnd_; }
  inline const std::string& getStartKey() { return startKey_; }
  inline const std::string& getEndKey() { return endKey_; }

 private:
  void buildKey() {
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

  std::string encodeBeginValue(const Value& value, const ColumnTypeDef& colDef, std::string& key) {
    std::string val = IndexKeyUtils::encodeValue(value, *colDef.get_type_length());
    if (value.type() == Value::Type::STRING && val.back() != '\0') {
      QFList_.emplace_back([&startKey = this->startKey_,
                            startPos = key.size(),
                            length = val.size()](const std::string& key) {
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
  std::string encodeEndValue(const Value& value, const ColumnTypeDef& colDef, std::string& key) {
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
};
class PathBuilder {
 public:
  PathBuilder(std::shared_ptr<nebula::meta::cpp2::IndexItem> index,
              std::shared_ptr<const nebula::meta::NebulaSchemaProvider> schema,
              const std::vector<cpp2::IndexColumnHint>& hints);
  std::unique_ptr<Path> release() {
    std::unique_ptr<Path> ret;
    if (hints_.back().get_scan_type() == cpp2::ScanType::RANGE) {
      ret.reset(new RangePath(index_, schema_, hints_));
    } else {
      ret.reset(new PrefixPath(index_, schema_, hints_));
    }
    return std::move(ret);
  }

 private:
  std::shared_ptr<nebula::meta::cpp2::IndexItem> index_;
  std::shared_ptr<const nebula::meta::NebulaSchemaProvider> schema_;
  const std::vector<cpp2::IndexColumnHint>& hints_;
};

/* define inline functions */
}  // namespace storage

}  // namespace nebula
