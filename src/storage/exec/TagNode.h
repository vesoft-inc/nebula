/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_EXEC_TAGNODE_H_
#define STORAGE_EXEC_TAGNODE_H_

#include "codec/RowReaderWrapper.h"
#include "common/base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/StorageIterator.h"

namespace nebula {
namespace storage {

/**
 * @brief TagNode will return a DataSet of specified props of tagId
 *
 * @see IterateNode<T>
 */
class TagNode final : public IterateNode<VertexID> {
 public:
  using RelNode::doExecute;

  /**
   * @brief Construct a new Tag Node object
   *
   * @param context Runtime Context.
   * @param ctx Tag Context.
   * @param tagId Tag id to get.
   * @param props Tag's props to get.
   */
  TagNode(RuntimeContext* context,
          TagContext* ctx,
          TagID tagId,
          const std::vector<PropContext>* props,
          StorageExpressionContext* expCtx = nullptr,
          Expression* exp = nullptr)
      : context_(context),
        tagContext_(ctx),
        tagId_(tagId),
        props_(props),
        expCtx_(expCtx),
        exp_(exp) {
    UNUSED(expCtx_);
    UNUSED(exp_);
    auto schemaIter = tagContext_->schemas_.find(tagId_);
    CHECK(schemaIter != tagContext_->schemas_.end());
    CHECK(!schemaIter->second.empty());
    schemas_ = &(schemaIter->second);
    ttl_ = QueryUtils::getTagTTLInfo(tagContext_, tagId_);
    tagName_ = tagContext_->tagNames_[tagId_];
    name_ = "TagNode";
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const VertexID& vId) override {
    valid_ = false;
    auto ret = RelNode::doExecute(partId, vId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }

    VLOG(1) << "partId " << partId << ", vId " << vId << ", tagId " << tagId_ << ", prop size "
            << props_->size();
    key_ = NebulaKeyUtils::tagKey(context_->vIdLen(), partId, vId, tagId_);
    ret = context_->env()->kvstore_->get(context_->spaceId(), partId, key_, &value_);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED &&
        ret != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      return ret;
    }

    if (context_->tagSchema_ != nullptr && context_->tagSchema_->hasVectorCol()) {
      for (auto& prop : *props_) {
        if (prop.isVector()) {
          auto index = context_->tagSchema_->getVectorFieldIndex(prop.name());
          auto vecKey = NebulaKeyUtils::vectorTagKey(
              context_->vIdLen(), partId, vId, tagId_, static_cast<int32_t>(index));
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
      doExecute(key_, value_, vectorKeys_, vectorValues_, vectorIndexes_);
    }

    return ret;
  }

  /**
   * @brief For resuming from a breakpoint.
   *
   * @param key Next key to be read
   * @param value Next value to be read
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode doExecute(const std::string& key,
                                    const std::string& value,
                                    const std::vector<std::string>& vecKeys,
                                    const std::vector<std::string>& vecValues,
                                    const std::vector<int32_t>& vecIndexes) {
    key_ = key;
    value_ = value;
    vectorKeys_ = vecKeys;
    vectorValues_ = vecValues;
    vectorIndexes_ = vecIndexes;
    resetReader();
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  nebula::cpp2::ErrorCode doExecute(const std::string& key, const std::string& value) {
    key_ = key;
    value_ = value;
    resetReader();
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  /**
   * @brief Collect tag's prop
   *
   * @param nullHandler Callback if prop is null.
   * @param valueHandler Callback if prop is not null.
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode collectTagPropsIfValid(NullHandler nullHandler,
                                                 PropHandler valueHandler) {
    if (!valid()) {
      return nullHandler(props_);
    }
    return valueHandler(key_, reader_.get(), props_);
  }

  bool valid() const override {
    return valid_;
  }

  void next() override {
    // tag only has one valid record, so stop iterate
    valid_ = false;
  }

  folly::StringPiece key() const override {
    return key_;
  }

  folly::StringPiece val() const override {
    return value_;
  }

  RowReaderWrapper* reader() const override {
    return reader_.get();
  }

  std::vector<folly::StringPiece> vectorKeys() const override {
    std::vector<folly::StringPiece> ret;
    for (auto& key : vectorKeys_) {
      ret.emplace_back(key);
    }
    return ret;
  }

  std::vector<folly::StringPiece> vectorValues() const override {
    std::vector<folly::StringPiece> ret;
    for (auto& value : vectorValues_) {
      ret.emplace_back(value);
    }
    return ret;
  }

  std::vector<RowReaderWrapper*> vectorReaders() const override {
    std::vector<RowReaderWrapper*> ret;
    for (auto& reader : vectorReaders_) {
      ret.emplace_back(reader.get());
    }
    return ret;
  }

  const std::string& getTagName() const {
    return tagName_;
  }

  TagID tagId() const {
    return tagId_;
  }

  void clear() {
    valid_ = false;
    key_.clear();
    value_.clear();
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
    reader_.reset(*schemas_, value_);
    if (!reader_ ||
        (ttl_.has_value() &&
         CommonUtils::checkDataExpiredForTTL(
             schemas_->back().get(), reader_.get(), ttl_.value().first, ttl_.value().second))) {
      reader_.reset();
      return;
    }
    if (!vectorValues_.empty()) {
      vectorReaders_.reserve(vectorValues_.size());
      for (size_t i = 0; i < vectorValues_.size(); i++) {
        auto index = vectorIndexes_[i];
        RowReaderWrapper vectorReader;
        vectorReader.reset(*schemas_, vectorValues_[i], true, index);
        if (!vectorReader ||
            (ttl_.has_value() && CommonUtils::checkDataExpiredForTTL(schemas_->back().get(),
                                                                     vectorReader.get(),
                                                                     ttl_.value().first,
                                                                     ttl_.value().second))) {
          LOG(ERROR) << "LZY TagNode reset vector reader, index: " << index;
          vectorReader.reset();
        }
        vectorReaders_.emplace_back(std::move(vectorReader));
      }
    }
    valid_ = true;
  }

  RuntimeContext* context_;
  TagContext* tagContext_;
  TagID tagId_;
  const std::vector<PropContext>* props_;
  StorageExpressionContext* expCtx_;
  Expression* exp_;
  const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>* schemas_ = nullptr;
  std::optional<std::pair<std::string, int64_t>> ttl_;
  std::string tagName_;

  bool valid_ = false;
  std::string key_;
  std::string value_;
  RowReaderWrapper reader_;
  // for vector property
  std::vector<std::string> vectorKeys_;
  std::vector<std::string> vectorValues_;
  std::vector<int32_t> vectorIndexes_;
  std::vector<RowReaderWrapper> vectorReaders_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_TAGNODE_H_
