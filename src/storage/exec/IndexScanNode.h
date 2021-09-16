/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_INDEXSCANNODE_H_
#define STORAGE_EXEC_INDEXSCANNODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/StorageIterator.h"

namespace nebula {
namespace storage {

template <typename T>
class IndexScanNode : public RelNode<T> {
 public:
  IndexScanNode(RuntimeContext* context,
                IndexID indexId,
                int64_t limit,
                std::vector<cpp2::PagingScanContext>* nextScan)
      : context_(context), indexId_(indexId), limit_(limit), nextScan_(nextScan) {}

  virtual ~IndexScanNode() = default;

  virtual IndexIterator* iterator() = 0;

  virtual std::vector<kvstore::KV> moveData() = 0;

 protected:
  RuntimeContext* context_;
  IndexID indexId_;
  int64_t limit_;
  std::vector<cpp2::PagingScanContext>* nextScan_;
};

template <typename T>
class IndexGeneralScanNode final : public IndexScanNode<T> {
 public:
  using RelNode<T>::doExecute;

  IndexGeneralScanNode(RuntimeContext* context,
                       IndexID indexId,
                       std::vector<cpp2::IndexColumnHint> columnHints,
                       int64_t limit,
                       std::vector<cpp2::PagingScanContext>* nextScan,
                       std::string filter = "")
      : IndexScanNode<T>(context, indexId, limit, nextScan),
        columnHints_(std::move(columnHints)),
        filter_(std::move(filter)) {
    /**
     * columnHints's elements are {scanType = PREFIX|RANGE; beginStr; endStr},
     *                            {scanType = PREFIX|RANGE; beginStr;
     * endStr},... if the scanType is RANGE, means the index scan is range scan.
     * if all scanType are PREFIX, means the index scan is prefix scan.
     * there should be only one RANGE hnit, and it must be the last one.
     */
    for (size_t i = 0; i < columnHints_.size(); i++) {
      if (columnHints_[i].get_scan_type() == cpp2::ScanType::RANGE) {
        isRangeScan_ = true;
        CHECK_EQ(columnHints_.size() - 1, i);
        break;
      }
    }
    RelNode<T>::name_ = "IndexGeneralScanNode";
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId) override {
    partId_ = partId;
    auto ret = RelNode<T>::doExecute(partId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }
    auto scanRet = scanStr();
    if (!scanRet.ok()) {
      return nebula::cpp2::ErrorCode::E_INVALID_FIELD_VALUE;
    }
    scanPair_ = scanRet.value();
    std::unique_ptr<kvstore::KVIterator> iter;
    ret = isRangeScan_
              ? this->context_->env()->kvstore_->range(
                    this->context_->spaceId(), partId, scanPair_.first, scanPair_.second, &iter)
              : this->context_->env()->kvstore_->prefix(
                    this->context_->spaceId(), partId, scanPair_.first, &iter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED && iter && iter->valid()) {
      this->context_->isEdge()
          ? iter_.reset(new EdgeIndexIterator(std::move(iter), this->context_->vIdLen()))
          : iter_.reset(new VertexIndexIterator(std::move(iter), this->context_->vIdLen()));
    } else {
      iter_.reset();
      return ret;
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  IndexIterator* iterator() override { return iter_.get(); }

  std::vector<kvstore::KV> moveData() override {
    auto* sh = this->context_->isEdge() ? this->context_->edgeSchema_ : this->context_->tagSchema_;
    auto ttlProp = CommonUtils::ttlProps(sh);
    data_.clear();
    int64_t count = 0;
    while (!!iter_ && iter_->valid()) {
      if (this->limit_ > -1 && count++ == this->limit_) {
        cpp2::PagingScanContext ctx;
        ctx.set_part(partId_);
        ctx.set_index_id(this->indexId_);
        if (!filter_.empty()) {
          ctx.set_filter(filter_);
        }

        cpp2::PagingScanPair pair;
        pair.set_start(iter_->key());
        if (isRangeScan_) {
          pair.set_end(scanPair_.second);
        } else {
          auto len = iter_->key().size();
          auto end = scanPair_.first;
          end.append(len - end.size(), static_cast<char>(0xFF));
          pair.set_end(std::move(end));
        }
        ctx.set_scan_pair(std::move(pair));
        this->nextScan_->emplace_back(std::move(ctx));
        break;
      }
      if (this->context_->isPlanKilled()) {
        return {};
      }
      if (!iter_->val().empty() && ttlProp.first) {
        auto v = IndexKeyUtils::parseIndexTTL(iter_->val());
        if (CommonUtils::checkDataExpiredForTTL(
                sh, std::move(v), ttlProp.second.second, ttlProp.second.first)) {
          iter_->next();
          continue;
        }
      }
      data_.emplace_back(iter_->key(), "");
      iter_->next();
    }
    return std::move(data_);
  }

 private:
  StatusOr<std::pair<std::string, std::string>> scanStr() {
    auto iRet = this->context_->isEdge() ? this->context_->env()->indexMan_->getEdgeIndex(
                                               this->context_->spaceId(), this->indexId_)
                                         : this->context_->env()->indexMan_->getTagIndex(
                                               this->context_->spaceId(), this->indexId_);
    if (!iRet.ok()) {
      return Status::IndexNotFound();
    }
    if (isRangeScan_) {
      return getRangeStr(iRet.value()->get_fields());
    } else {
      return getPrefixStr(iRet.value()->get_fields());
    }
  }

  StatusOr<std::pair<std::string, std::string>> getPrefixStr(
      const std::vector<::nebula::meta::cpp2::ColumnDef>& fields) {
    std::string prefix;
    prefix.append(IndexKeyUtils::indexPrefix(partId_, this->indexId_));
    for (auto& col : columnHints_) {
      auto iter = std::find_if(fields.begin(), fields.end(), [col](const auto& field) {
        return col.get_column_name() == field.get_name();
      });
      if (iter == fields.end()) {
        VLOG(3) << "Field " << col.get_column_name() << " not found ";
        return Status::Error("Field not found");
      }
      auto type = IndexKeyUtils::toValueType(iter->type.type);
      if (type == Value::Type::STRING && !iter->type.type_length_ref().has_value()) {
        return Status::Error("String property index has not set prefix length.");
      }
      prefix.append(encodeValue(*col.begin_value_ref(), type, iter->type.get_type_length()));
    }
    return std::make_pair(prefix, "");
  }

  StatusOr<std::pair<std::string, std::string>> getRangeStr(
      const std::vector<::nebula::meta::cpp2::ColumnDef>& fields) {
    std::string start, end;
    start.append(IndexKeyUtils::indexPrefix(partId_, this->indexId_));
    end.append(IndexKeyUtils::indexPrefix(partId_, this->indexId_));
    for (auto& col : columnHints_) {
      auto iter = std::find_if(fields.begin(), fields.end(), [col](const auto& field) {
        return col.get_column_name() == field.get_name();
      });
      if (iter == fields.end()) {
        VLOG(3) << "Field " << col.get_column_name() << " not found ";
        return Status::Error("Field not found");
      }
      auto type = IndexKeyUtils::toValueType(iter->get_type().get_type());
      if (type == Value::Type::STRING && !iter->get_type().type_length_ref().has_value()) {
        return Status::Error("String property index has not set prefix length.");
      }
      if (col.get_scan_type() == cpp2::ScanType::PREFIX) {
        start.append(encodeValue(*col.begin_value_ref(), type, iter->type.get_type_length()));
        end.append(encodeValue(*col.begin_value_ref(), type, iter->type.get_type_length()));
      } else {
        start.append(encodeValue(*col.begin_value_ref(), type, iter->type.get_type_length()));
        end.append(encodeValue(*col.end_value_ref(), type, iter->type.get_type_length()));
      }
    }
    return std::make_pair(start, end);
  }

  // precondition: if type is STRING, strLen must be valid
  std::string encodeValue(const Value& val, Value::Type type, const int16_t* strLen) {
    if (val.isNull()) {
      return IndexKeyUtils::encodeNullValue(type, strLen);
    }
    if (type == Value::Type::STRING) {
      return IndexKeyUtils::encodeValue(val, *strLen);
    } else {
      return IndexKeyUtils::encodeValue(val);
    }
  }

 private:
  bool isRangeScan_{false};
  std::unique_ptr<IndexIterator> iter_;
  std::pair<std::string, std::string> scanPair_;
  std::vector<cpp2::IndexColumnHint> columnHints_;
  std::vector<kvstore::KV> data_;
  PartitionID partId_;
  std::string filter_{};
};

template <typename T>
class IndexPagingScanNode final : public IndexScanNode<T> {
 public:
  using RelNode<T>::doExecute;

  IndexPagingScanNode(RuntimeContext* context,
                      IndexID indexId,
                      const cpp2::PagingScanContext& ctx,
                      int64_t limit,
                      std::vector<cpp2::PagingScanContext>* nextScan)
      : IndexScanNode<T>(context, indexId, limit, nextScan), ctx_(ctx) {
    RelNode<T>::name_ = "IndexPagingScanNode";
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId) override {
    auto ret = RelNode<T>::doExecute(partId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }
    auto const& start = ctx_.get_scan_pair().get_start();
    auto const& end = ctx_.get_scan_pair().get_end();
    std::unique_ptr<kvstore::KVIterator> iter;
    ret = this->context_->env()->kvstore_->range(
        this->context_->spaceId(), partId, start, end, &iter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED && iter && iter->valid()) {
      this->context_->isEdge()
          ? iter_.reset(new EdgeIndexIterator(std::move(iter), this->context_->vIdLen()))
          : iter_.reset(new VertexIndexIterator(std::move(iter), this->context_->vIdLen()));
    } else {
      iter_.reset();
      return ret;
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  IndexIterator* iterator() override { return iter_.get(); }

  std::vector<kvstore::KV> moveData() override {
    auto* sh = this->context_->isEdge() ? this->context_->edgeSchema_ : this->context_->tagSchema_;
    auto ttlProp = CommonUtils::ttlProps(sh);
    data_.clear();
    int64_t count = 0;
    while (!!iter_ && iter_->valid()) {
      if (this->limit_ > -1 && count++ == this->limit_) {
        ctx_.scan_pair_ref().value().set_start(iter_->key());
        this->nextScan_->emplace_back(ctx_);
        break;
      }
      if (this->context_->isPlanKilled()) {
        return {};
      }
      if (!iter_->val().empty() && ttlProp.first) {
        auto v = IndexKeyUtils::parseIndexTTL(iter_->val());
        if (CommonUtils::checkDataExpiredForTTL(
                sh, std::move(v), ttlProp.second.second, ttlProp.second.first)) {
          iter_->next();
          continue;
        }
      }
      data_.emplace_back(iter_->key(), "");
      iter_->next();
    }
    return std::move(data_);
  }

 private:
  std::unique_ptr<IndexIterator> iter_;
  cpp2::PagingScanContext ctx_;
  std::vector<kvstore::KV> data_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_INDEXSCANNODE_H_
