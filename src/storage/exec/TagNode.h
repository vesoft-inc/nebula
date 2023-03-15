/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_EXEC_TAGNODE_H_
#define STORAGE_EXEC_TAGNODE_H_

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
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      return doExecute(key_, value_);
    } else if (ret == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      // regard key not found as succeed as well, upper node will handle it
      return nebula::cpp2::ErrorCode::SUCCEEDED;
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
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_TAGNODE_H_
