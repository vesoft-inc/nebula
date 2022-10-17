/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_EXEC_GETPROPNODE_H_
#define STORAGE_EXEC_GETPROPNODE_H_

#include "common/base/Base.h"
#include "storage/exec/EdgeNode.h"
#include "storage/exec/TagNode.h"

namespace nebula {
namespace storage {

class GetTagPropNode : public QueryNode<VertexID> {
 public:
  using RelNode<VertexID>::doExecute;

  GetTagPropNode(RuntimeContext* context,
                 std::vector<TagNode*> tagNodes,
                 nebula::DataSet* resultDataSet,
                 Expression* filter,
                 std::size_t limit,
                 TagContext* tagContext)
      : context_(context),
        tagNodes_(std::move(tagNodes)),
        resultDataSet_(resultDataSet),
        expCtx_(filter == nullptr
                    ? nullptr
                    : new StorageExpressionContext(context->vIdLen(), context->isIntId())),
        filter_(filter),
        limit_(limit),
        tagContext_(tagContext) {
    name_ = "GetTagPropNode";
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const VertexID& vId) override {
    if (resultDataSet_->size() >= limit_) {
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    }
    auto ret = RelNode::doExecute(partId, vId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }

    // If none of the tag node valid, will check if vertex exists:
    // 1. if use_vertex_key is true, check it by vertex key
    // 2. if use_vertex_key is false, check it by scanning vertex prefix
    // If vertex does not exists, do not emplace the row.
    if (!std::any_of(tagNodes_.begin(), tagNodes_.end(), [](const auto& tagNode) {
          return tagNode->valid();
        })) {
      if (FLAGS_use_vertex_key) {
        auto kvstore = context_->env()->kvstore_;
        auto vertexKey = NebulaKeyUtils::vertexKey(context_->vIdLen(), partId, vId);
        std::string value;
        ret = kvstore->get(context_->spaceId(), partId, vertexKey, &value);
        if (ret == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
          return nebula::cpp2::ErrorCode::SUCCEEDED;
        } else if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
          return ret;
        }
      } else {
        // check if vId has any valid tag by prefix scan
        std::unique_ptr<kvstore::KVIterator> iter;
        auto tagPrefix = NebulaKeyUtils::tagPrefix(context_->vIdLen(), partId, vId);
        ret = context_->env()->kvstore_->prefix(context_->spaceId(), partId, tagPrefix, &iter);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
          return ret;
        } else if (!iter->valid()) {
          return nebula::cpp2::ErrorCode::SUCCEEDED;
        }

        bool hasValidTag = false;
        for (; iter->valid(); iter->next()) {
          // check if tag schema exists
          auto key = iter->key();
          auto tagId = NebulaKeyUtils::getTagId(context_->vIdLen(), key);
          auto schemaIter = tagContext_->schemas_.find(tagId);
          if (schemaIter == tagContext_->schemas_.end()) {
            continue;
          }
          // check if ttl expired
          auto schemas = &(schemaIter->second);
          RowReaderWrapper reader;
          reader.reset(*schemas, iter->val());
          if (!reader) {
            continue;
          }
          auto ttl = QueryUtils::getTagTTLInfo(tagContext_, tagId);
          if (ttl.has_value() &&
              CommonUtils::checkDataExpiredForTTL(
                  schemas->back().get(), reader.get(), ttl.value().first, ttl.value().second)) {
            continue;
          }
          hasValidTag = true;
          break;
        }
        if (!hasValidTag) {
          return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        // if has any valid tag, will emplace a row with vId
      }
    }

    List row;
    // vertexId is the first column
    if (context_->isIntId()) {
      row.emplace_back(*reinterpret_cast<const int64_t*>(vId.data()));
    } else {
      row.emplace_back(vId);
    }
    auto vIdLen = context_->vIdLen();
    auto isIntId = context_->isIntId();
    for (auto* tagNode : tagNodes_) {
      ret = tagNode->collectTagPropsIfValid(
          [&row, tagNode, this](const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
            for (const auto& prop : *props) {
              if (prop.returned_) {
                row.emplace_back(Value());
              }
              if (prop.filtered_ && expCtx_ != nullptr) {
                expCtx_->setTagProp(tagNode->getTagName(), prop.name_, Value());
              }
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
          },
          [&row, vIdLen, isIntId, tagNode, this](
              folly::StringPiece key,
              RowReader* reader,
              const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
            auto status = QueryUtils::collectVertexProps(
                key, vIdLen, isIntId, reader, props, row, expCtx_.get(), tagNode->getTagName());
            if (!status.ok()) {
              return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
          });
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return ret;
      }
    }
    if (filter_ == nullptr) {
      resultDataSet_->rows.emplace_back(std::move(row));
    } else {
      auto result = QueryUtils::vTrue(filter_->eval(*expCtx_));
      if (result.ok() && result.value()) {
        resultDataSet_->rows.emplace_back(std::move(row));
      }
    }
    if (expCtx_ != nullptr) {
      expCtx_->clear();
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

 private:
  RuntimeContext* context_;
  std::vector<TagNode*> tagNodes_;
  nebula::DataSet* resultDataSet_;
  std::unique_ptr<StorageExpressionContext> expCtx_{nullptr};
  Expression* filter_{nullptr};
  const std::size_t limit_{std::numeric_limits<std::size_t>::max()};
  TagContext* tagContext_;
};

class GetEdgePropNode : public QueryNode<cpp2::EdgeKey> {
 public:
  using RelNode::doExecute;

  GetEdgePropNode(RuntimeContext* context,
                  std::vector<EdgeNode<cpp2::EdgeKey>*> edgeNodes,
                  nebula::DataSet* resultDataSet,
                  Expression* filter,
                  std::size_t limit)
      : context_(context),
        edgeNodes_(std::move(edgeNodes)),
        resultDataSet_(resultDataSet),
        expCtx_(filter == nullptr
                    ? nullptr
                    : new StorageExpressionContext(context->vIdLen(), context->isIntId())),
        filter_(filter),
        limit_(limit) {
    QueryNode::name_ = "GetEdgePropNode";
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
    if (resultDataSet_->size() >= limit_) {
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    }
    auto ret = RelNode::doExecute(partId, edgeKey);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }

    List row;
    auto vIdLen = context_->vIdLen();
    auto isIntId = context_->isIntId();
    for (auto* edgeNode : edgeNodes_) {
      ret = edgeNode->collectEdgePropsIfValid(
          [&row, edgeNode, this](const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
            for (const auto& prop : *props) {
              if (prop.returned_) {
                row.emplace_back(Value());
              }
              if (prop.filtered_ && expCtx_ != nullptr) {
                expCtx_->setEdgeProp(edgeNode->getEdgeName(), prop.name_, Value());
              }
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
          },
          [&row, vIdLen, isIntId, edgeNode, this](
              folly::StringPiece key,
              RowReader* reader,
              const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
            auto status = QueryUtils::collectEdgeProps(
                key, vIdLen, isIntId, reader, props, row, expCtx_.get(), edgeNode->getEdgeName());
            if (!status.ok()) {
              return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
          });
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return ret;
      }
    }
    if (filter_ == nullptr) {
      resultDataSet_->rows.emplace_back(std::move(row));
    } else {
      auto result = QueryUtils::vTrue(filter_->eval(*expCtx_));
      if (result.ok() && result.value()) {
        resultDataSet_->rows.emplace_back(std::move(row));
      }
    }
    if (expCtx_ != nullptr) {
      expCtx_->clear();
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

 private:
  RuntimeContext* context_;
  std::vector<EdgeNode<cpp2::EdgeKey>*> edgeNodes_;
  nebula::DataSet* resultDataSet_;
  std::unique_ptr<StorageExpressionContext> expCtx_{nullptr};
  Expression* filter_{nullptr};
  const std::size_t limit_{std::numeric_limits<std::size_t>::max()};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_GETPROPNODE_H_
