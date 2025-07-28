/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_EXEC_EDGENODE_H_
#define STORAGE_EXEC_EDGENODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/StorageIterator.h"

namespace nebula {
namespace storage {

// EdgeNode will return a StorageIterator which iterates over the specified
// edgeType of given vertexId
template <typename T>
class EdgeNode : public IterateNode<T> {
 public:
  nebula::cpp2::ErrorCode collectEdgePropsIfValid(NullHandler nullHandler,
                                                  PropHandler valueHandler) {
    if (!this->valid()) {
      return nullHandler(props_);
    }
    return valueHandler(this->key(), this->reader(), props_);
  }

  const std::string& getEdgeName() const {
    return edgeName_;
  }

  EdgeType edgeType() const {
    return edgeType_;
  }

 protected:
  EdgeNode(RuntimeContext* context,
           EdgeContext* edgeContext,
           EdgeType edgeType,
           const std::vector<PropContext>* props,
           StorageExpressionContext* expCtx,
           Expression* exp,
           bool maySkipDecode = false)
      : context_(context),
        edgeContext_(edgeContext),
        edgeType_(edgeType),
        props_(props),
        expCtx_(expCtx),
        exp_(exp) {
    UNUSED(expCtx_);
    UNUSED(exp_);
    auto schemaIter = edgeContext_->schemas_.find(std::abs(edgeType_));
    CHECK(schemaIter != edgeContext_->schemas_.end());
    CHECK(!schemaIter->second.empty());
    schemas_ = &(schemaIter->second);
    ttl_ = QueryUtils::getEdgeTTLInfo(edgeContext_, std::abs(edgeType_));
    edgeName_ = edgeContext_->edgeNames_[edgeType_];
    if (!ttl_.has_value() && maySkipDecode) {
      skipDecode_ = true;
    }
    IterateNode<T>::name_ = "EdgeNode";
  }

  EdgeNode(RuntimeContext* context, EdgeContext* ctx) : context_(context), edgeContext_(ctx) {
    IterateNode<T>::name_ = "EdgeNode";
  }

  RuntimeContext* context_;
  EdgeContext* edgeContext_;
  EdgeType edgeType_;
  const std::vector<PropContext>* props_;
  StorageExpressionContext* expCtx_;
  Expression* exp_;

  const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;
  std::optional<std::pair<std::string, int64_t>> ttl_;
  std::string edgeName_;
  // when no ttl exists and we don't need to read property in value, skip build RowReader
  bool skipDecode_{false};
};

// FetchEdgeNode is used to fetch a single edge
class FetchEdgeNode final : public EdgeNode<cpp2::EdgeKey> {
 public:
  using RelNode::doExecute;

  FetchEdgeNode(RuntimeContext* context,
                EdgeContext* edgeContext,
                EdgeType edgeType,
                const std::vector<PropContext>* props,
                StorageExpressionContext* expCtx = nullptr,
                Expression* exp = nullptr)
      : EdgeNode(context, edgeContext, edgeType, props, expCtx, exp) {
    name_ = "FetchEdgeNode";
  }

  bool valid() const override {
    return valid_;
  }

  void next() override {
    valid_ = false;
  }

  folly::StringPiece key() const override {
    return key_;
  }

  folly::StringPiece val() const override {
    return val_;
  }

  RowReaderWrapper* reader() const override {
    return reader_.get();
  }

  std::vector<folly::StringPiece> vectorKeys() const {
    std::vector<folly::StringPiece> ret;
    for (auto& key : vectorKeys_) {
      ret.emplace_back(key);
    }
    return ret;
  }

  std::vector<folly::StringPiece> vectorValues() const {
    std::vector<folly::StringPiece> ret;
    for (auto& value : vectorValues_) {
      ret.emplace_back(value);
    }
    return ret;
  }

  std::vector<RowReaderWrapper*> vectorReaders() const {
    std::vector<RowReaderWrapper*> ret;
    for (auto& reader : vectorReaders_) {
      ret.emplace_back(reader.get());
    }
    return ret;
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
    valid_ = false;
    auto ret = RelNode::doExecute(partId, edgeKey);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }

    VLOG(1) << "partId " << partId << ", edgeType " << edgeType_ << ", prop size "
            << props_->size();
    if (edgeType_ != *edgeKey.edge_type_ref()) {
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    }
    key_ = NebulaKeyUtils::edgeKey(context_->vIdLen(),
                                   partId,
                                   (*edgeKey.src_ref()).getStr(),
                                   *edgeKey.edge_type_ref(),
                                   *edgeKey.ranking_ref(),
                                   (*edgeKey.dst_ref()).getStr());
    ret = context_->env()->kvstore_->get(context_->spaceId(), partId, key_, &val_);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED &&
        ret != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      return ret;
    }

    if (context_->edgeSchema_ != nullptr && context_->edgeSchema_->hasVectorCol()) {
      for (auto& prop : *props_) {
        if (prop.isVector()) {
          auto index = context_->edgeSchema_->getVectorFieldIndex(prop.name());
          auto vecKey = NebulaKeyUtils::vectorEdgeKey(context_->vIdLen(),
                                                      partId,
                                                      (*edgeKey.src_ref()).getStr(),
                                                      *edgeKey.edge_type_ref(),
                                                      *edgeKey.ranking_ref(),
                                                      (*edgeKey.dst_ref()).getStr(),
                                                      static_cast<int32_t>(index));
          std::string vecVal;
          ret = context_->env()->kvstore_->get(context_->spaceId(), partId, vecKey, &vecVal);
          if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
            vectorKeys_.emplace_back(std::move(vecKey));
            vectorValues_.emplace_back(std::move(vecVal));
            vectorIndexes_.emplace_back(index);
          } else if (ret != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
            vectorKeys_.clear();
            vectorValues_.clear();
            vectorIndexes_.clear();
            return nebula::cpp2::ErrorCode::SUCCEEDED;
          } else {
            return ret;
          }
        }
      }
    }
    if (ret == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND && vectorValues_.empty()) {
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      doExecute(key_, val_, vectorKeys_, vectorValues_, vectorIndexes_);
    }
    return ret;
  }

  nebula::cpp2::ErrorCode doExecute(const std::string& key,
                                    const std::string& value,
                                    const std::vector<std::string>& vecKeys,
                                    const std::vector<std::string>& vecValues,
                                    const std::vector<int32_t>& vecIndexes) {
    key_ = key;
    val_ = value;
    vectorKeys_ = vecKeys;
    vectorValues_ = vecValues;
    vectorIndexes_ = vecIndexes;
    resetReader();
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  nebula::cpp2::ErrorCode doExecute(const std::string& key, const std::string& value) {
    key_ = key;
    val_ = value;
    resetReader();
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  void clear() {
    valid_ = false;
    key_.clear();
    val_.clear();
    reader_.reset();
    for (auto& vkey : vectorKeys_) {
      vkey.clear();
    }
    vectorKeys_.clear();
    for (auto& vvalue : vectorValues_) {
      vvalue.clear();
    }
    vectorValues_.clear();
    for (auto& reader : vectorReaders_) {
      reader.reset();
    }
    vectorReaders_.clear();
  }

 private:
  void resetReader() {
    reader_.reset(*schemas_, val_);
    if (!reader_ ||
        (ttl_.has_value() &&
         CommonUtils::checkDataExpiredForTTL(
             schemas_->back().get(), reader_.get(), ttl_.value().first, ttl_.value().second))) {
      reader_.reset();
      return;
    }
    if (!vectorValues_.empty()) {
      for (size_t i = 0; i < vectorValues_.size(); i++) {
        auto& vecValue = vectorValues_[i];
        auto index = vectorIndexes_[i];
        RowReaderWrapper vecReader;
        vecReader.reset(*schemas_, vecValue, true, index);
        if (!vecReader ||
            (ttl_.has_value() && CommonUtils::checkDataExpiredForTTL(schemas_->back().get(),
                                                                     vecReader.get(),
                                                                     ttl_.value().first,
                                                                     ttl_.value().second))) {
          vecReader.reset();
        }
        vectorReaders_.emplace_back(std::move(vecReader));
      }
    }
    valid_ = true;
  }

  bool valid_ = false;
  std::string key_;
  std::string val_;
  RowReaderWrapper reader_;
  // for vector property
  std::vector<std::string> vectorKeys_;
  std::vector<std::string> vectorValues_;
  std::vector<int32_t> vectorIndexes_;
  std::vector<RowReaderWrapper> vectorReaders_;
};

// SingleEdgeNode is used to scan all edges of a specified edgeType of the same
// srcId
class SingleEdgeNode final : public EdgeNode<VertexID> {
 public:
  using RelNode::doExecute;
  SingleEdgeNode(RuntimeContext* context,
                 EdgeContext* edgeContext,
                 EdgeType edgeType,
                 const std::vector<PropContext>* props,
                 StorageExpressionContext* expCtx = nullptr,
                 Expression* exp = nullptr,
                 bool maySkipDecode = false)
      : EdgeNode(context, edgeContext, edgeType, props, expCtx, exp, maySkipDecode) {
    name_ = "SingleEdgeNode";
  }

  SingleEdgeIterator* iter() {
    return iter_.get();
  }

  bool valid() const override {
    return iter_ && iter_->valid();
  }

  void next() override {
    iter_->next();
  }

  folly::StringPiece key() const override {
    return iter_->key();
  }

  folly::StringPiece val() const override {
    return iter_->val();
  }

  RowReaderWrapper* reader() const override {
    return iter_->reader();
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const VertexID& vId) override {
    auto ret = RelNode::doExecute(partId, vId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }

    VLOG(1) << "partId " << partId << ", vId " << vId << ", edgeType " << edgeType_
            << ", prop size " << props_->size();
    std::unique_ptr<kvstore::KVIterator> iter;
    prefix_ = NebulaKeyUtils::edgePrefix(context_->vIdLen(), partId, vId, edgeType_);
    ret = context_->env()->kvstore_->prefix(context_->spaceId(), partId, prefix_, &iter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED && iter && iter->valid()) {
      if (!skipDecode_) {
        iter_.reset(new SingleEdgeIterator(context_, std::move(iter), edgeType_, schemas_, &ttl_));
      } else {
        iter_.reset(new SingleEdgeKeyIterator(std::move(iter), edgeType_));
      }
    } else {
      iter_.reset();
    }
    return ret;
  }

 private:
  std::unique_ptr<SingleEdgeIterator> iter_;
  std::string prefix_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_EDGENODE_H_
