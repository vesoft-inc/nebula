/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_EXEC_MULTITAGNODE_H
#define STORAGE_EXEC_MULTITAGNODE_H

#include "common/base/Base.h"
#include "storage/context/StorageExpressionContext.h"
#include "storage/exec/EdgeNode.h"
#include "storage/exec/StorageIterator.h"
#include "storage/exec/TagNode.h"

namespace nebula {
namespace storage {

/**
 * @brief MultiTagNode is a replacement of HashJoinNode in execution of "go over" if Graph don't
 * pass any Edge prop
 *
 * @see IterateNode
 */
class MultiTagNode : public IterateNode<VertexID> {
 public:
  using RelNode::doExecute;

  MultiTagNode(RuntimeContext* context,
               const std::vector<TagNode*>& tagNodes,
               StorageExpressionContext* expCtx)
      : context_(context), tagNodes_(tagNodes), expCtx_(expCtx) {
    IterateNode::name_ = "MultiTagNode";
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const VertexID& vId) override {
    auto ret = RelNode::doExecute(partId, vId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }

    if (expCtx_ != nullptr) {
      expCtx_->clear();
    }
    result_.setList(nebula::List());
    auto& result = result_.mutableList();
    if (context_->resultStat_ == ResultStatus::ILLEGAL_DATA) {
      return nebula::cpp2::ErrorCode::E_INVALID_DATA;
    }

    // add result of each tag node to tagResult
    for (auto* tagNode : tagNodes_) {
      ret = tagNode->collectTagPropsIfValid(
          [&result](const std::vector<PropContext>*) -> nebula::cpp2::ErrorCode {
            result.values.emplace_back(Value());
            return nebula::cpp2::ErrorCode::SUCCEEDED;
          },
          [this, &result, tagNode](
              folly::StringPiece key,
              RowReader* reader,
              const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
            nebula::List list;
            list.reserve(props->size());
            const auto& tagName = tagNode->getTagName();
            for (const auto& prop : *props) {
              VLOG(2) << "Collect prop " << prop.name_;
              auto value = QueryUtils::readVertexProp(
                  key, context_->vIdLen(), context_->isIntId(), reader, prop);
              if (!value.ok()) {
                return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
              }
              if (prop.filtered_ && expCtx_ != nullptr) {
                expCtx_->setTagProp(tagName, prop.name_, value.value());
              }
              if (prop.returned_) {
                list.emplace_back(std::move(value).value());
              }
            }
            result.values.emplace_back(std::move(list));
            return nebula::cpp2::ErrorCode::SUCCEEDED;
          });
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return ret;
      }
    }

    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  bool valid() const override {
    auto ret = tagNodes_.back()->valid();
    return ret;
  }

  void next() override {
    tagNodes_.back()->next();
  }

  folly::StringPiece key() const override {
    LOG(FATAL) << "not allowed to do this";
    return "";
  }

  folly::StringPiece val() const override {
    LOG(FATAL) << "not allowed to do this";
    return "";
  }

  RowReader* reader() const override {
    LOG(FATAL) << "not allowed to do this";
    return nullptr;
  }

 private:
  RuntimeContext* context_;
  std::vector<TagNode*> tagNodes_;
  StorageExpressionContext* expCtx_;
};

}  // namespace storage
}  // namespace nebula
#endif
