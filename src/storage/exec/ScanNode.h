/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_EXEC_SCANNODE_H
#define STORAGE_EXEC_SCANNODE_H

#include "common/base/Base.h"
#include "storage/exec/GetPropNode.h"

namespace nebula {
namespace storage {

using Cursor = std::string;

/**
 * @brief Node to scan vertices of one partition
 */
class ScanVertexPropNode : public QueryNode<Cursor> {
 public:
  using RelNode<Cursor>::doExecute;

  /**
   * @brief Construct a new Scan Vertex Prop Node object
   *
   * @param context
   * @param tagNodes
   * @param enableReadFollower
   * @param limit
   * @param cursors
   * @param resultDataSet
   * @param expCtx
   * @param filter
   */
  ScanVertexPropNode(RuntimeContext* context,
                     std::vector<std::unique_ptr<TagNode>> tagNodes,
                     bool enableReadFollower,
                     int64_t limit,
                     std::unordered_map<PartitionID, cpp2::ScanCursor>* cursors,
                     nebula::DataSet* resultDataSet,
                     StorageExpressionContext* expCtx = nullptr,
                     Expression* filter = nullptr)
      : context_(context),
        tagNodes_(std::move(tagNodes)),
        enableReadFollower_(enableReadFollower),
        limit_(limit),
        cursors_(cursors),
        resultDataSet_(resultDataSet),
        expCtx_(expCtx),
        filter_(filter) {
    name_ = "ScanVertexPropNode";
    for (std::size_t i = 0; i < tagNodes_.size(); ++i) {
      tagNodesIndex_.emplace(tagNodes_[i]->tagId(), i);
    }
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const Cursor& cursor) override {
    auto ret = RelNode::doExecute(partId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }

    std::string start;
    std::string prefix = NebulaKeyUtils::tagPrefix(partId);
    if (cursor.empty()) {
      start = prefix;
    } else {
      start = cursor;
    }

    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = context_->env()->kvstore_->rangeWithPrefix(
        context_->planContext_->spaceId_, partId, start, prefix, &iter, enableReadFollower_);
    if (kvRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return kvRet;
    }

    const auto rowLimit = limit_;
    auto vIdLen = context_->vIdLen();
    auto isIntId = context_->isIntId();
    std::string currentVertexId;
    for (; iter->valid() && static_cast<int64_t>(resultDataSet_->rowSize()) < rowLimit;
         iter->next()) {
      auto key = iter->key();
      auto tagId = NebulaKeyUtils::getTagId(vIdLen, key);
      auto tagIdIndex = tagNodesIndex_.find(tagId);
      if (tagIdIndex == tagNodesIndex_.end()) {
        continue;
      }
      auto vertexId = NebulaKeyUtils::getVertexId(vIdLen, key);
      if (vertexId != currentVertexId && !currentVertexId.empty()) {
        ret = collectOneRow(isIntId, vIdLen, currentVertexId);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
          return ret;
        }
      }  // collect vertex row
      currentVertexId = vertexId;
      if (static_cast<int64_t>(resultDataSet_->rowSize()) >= rowLimit) {
        break;
      }
      auto value = iter->val();
      tagNodes_[tagIdIndex->second]->doExecute(key.toString(), value.toString());
    }  // iterate key
    if (static_cast<int64_t>(resultDataSet_->rowSize()) < rowLimit) {
      ret = collectOneRow(isIntId, vIdLen, currentVertexId);
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return ret;
      }
    }

    cpp2::ScanCursor c;
    if (iter->valid()) {
      c.next_cursor_ref() = iter->key().str();
    }
    cursors_->emplace(partId, std::move(c));
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  nebula::cpp2::ErrorCode collectOneRow(bool isIntId,
                                        std::size_t vIdLen,
                                        const std::string& currentVertexId) {
    List row;
    nebula::cpp2::ErrorCode ret = nebula::cpp2::ErrorCode::SUCCEEDED;
    // vertexId is the first column
    if (isIntId) {
      row.emplace_back(*reinterpret_cast<const int64_t*>(currentVertexId.data()));
    } else {
      row.emplace_back(currentVertexId.c_str());
    }
    // if none of the tag node valid, do not emplace the row
    if (std::any_of(tagNodes_.begin(), tagNodes_.end(), [](const auto& tagNode) {
          return tagNode->valid();
        })) {
      for (auto& tagNode : tagNodes_) {
        ret = tagNode->collectTagPropsIfValid(
            [&row, tagNode = tagNode.get(), this](
                const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
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
            [&row, vIdLen, isIntId, tagNode = tagNode.get(), this](
                folly::StringPiece key,
                RowReaderWrapper* reader,
                const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
              for (const auto& prop : *props) {
                if (prop.returned_ || (prop.filtered_ && expCtx_ != nullptr)) {
                  auto value = QueryUtils::readVertexProp(key, vIdLen, isIntId, reader, prop);
                  if (!value.ok()) {
                    return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
                  }
                  if (prop.filtered_ && expCtx_ != nullptr) {
                    expCtx_->setTagProp(tagNode->getTagName(), prop.name_, value.value());
                  }
                  if (prop.returned_) {
                    VLOG(2) << "Collect prop " << prop.name_;
                    row.emplace_back(std::move(value).value());
                  }
                }
              }
              return nebula::cpp2::ErrorCode::SUCCEEDED;
            });
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
          break;
        }
      }
      if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
        if (filter_ == nullptr) {
          resultDataSet_->rows.emplace_back(std::move(row));
        } else {
          auto result = QueryUtils::vTrue(filter_->eval(*expCtx_));
          if (result.ok() && result.value()) {
            resultDataSet_->rows.emplace_back(std::move(row));
          }
        }
      }
      expCtx_->clear();
      for (auto& tagNode : tagNodes_) {
        tagNode->clear();
      }
    }
    return ret;
  }

 private:
  RuntimeContext* context_;
  std::vector<std::unique_ptr<TagNode>> tagNodes_;
  std::unordered_map<TagID, std::size_t> tagNodesIndex_;
  bool enableReadFollower_;
  int64_t limit_;
  // cursors for next scan
  std::unordered_map<PartitionID, cpp2::ScanCursor>* cursors_;
  nebula::DataSet* resultDataSet_;
  StorageExpressionContext* expCtx_{nullptr};
  Expression* filter_{nullptr};
};

// Node to scan edge of one partition
class ScanEdgePropNode : public QueryNode<Cursor> {
 public:
  using RelNode<Cursor>::doExecute;

  ScanEdgePropNode(RuntimeContext* context,
                   std::vector<std::unique_ptr<FetchEdgeNode>> edgeNodes,
                   bool enableReadFollower,
                   int64_t limit,
                   std::unordered_map<PartitionID, cpp2::ScanCursor>* cursors,
                   nebula::DataSet* resultDataSet,
                   StorageExpressionContext* expCtx = nullptr,
                   Expression* filter = nullptr)
      : context_(context),
        edgeNodes_(std::move(edgeNodes)),
        enableReadFollower_(enableReadFollower),
        limit_(limit),
        cursors_(cursors),
        resultDataSet_(resultDataSet),
        expCtx_(expCtx),
        filter_(filter) {
    QueryNode::name_ = "ScanEdgePropNode";
    for (std::size_t i = 0; i < edgeNodes_.size(); ++i) {
      edgeNodesIndex_.emplace(edgeNodes_[i]->edgeType(), i);
    }
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const Cursor& cursor) override {
    auto ret = RelNode::doExecute(partId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }

    std::string start;
    std::string prefix = NebulaKeyUtils::edgePrefix(partId);
    if (cursor.empty()) {
      start = prefix;
    } else {
      start = cursor;
    }

    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = context_->env()->kvstore_->rangeWithPrefix(
        context_->spaceId(), partId, start, prefix, &iter, enableReadFollower_);
    if (kvRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return kvRet;
    }

    auto rowLimit = limit_;
    auto vIdLen = context_->vIdLen();
    auto isIntId = context_->isIntId();
    for (; iter->valid() && static_cast<int64_t>(resultDataSet_->rowSize()) < rowLimit;
         iter->next()) {
      auto key = iter->key();
      if (!NebulaKeyUtils::isEdge(vIdLen, key)) {
        continue;
      }
      auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen, key);
      auto edgeNodeIndex = edgeNodesIndex_.find(edgeType);
      if (edgeNodeIndex == edgeNodesIndex_.end()) {
        continue;
      }
      auto value = iter->val();
      edgeNodes_[edgeNodeIndex->second]->doExecute(key.toString(), value.toString());
      ret = collectOneRow(isIntId, vIdLen);
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return ret;
      }
    }

    cpp2::ScanCursor c;
    if (iter->valid()) {
      c.next_cursor_ref() = iter->key().str();
    }
    cursors_->emplace(partId, std::move(c));
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  nebula::cpp2::ErrorCode collectOneRow(bool isIntId, std::size_t vIdLen) {
    List row;
    nebula::cpp2::ErrorCode ret = nebula::cpp2::ErrorCode::SUCCEEDED;
    // Usually there is only one edge node, when all of the edgeNodes are invalid (e.g. ttl
    // expired), just skip the row. If we don't skip it, there will be a whole line of empty value.
    if (!std::any_of(edgeNodes_.begin(), edgeNodes_.end(), [](const auto& edgeNode) {
          return edgeNode->valid();
        })) {
      return ret;
    }
    for (auto& edgeNode : edgeNodes_) {
      ret = edgeNode->collectEdgePropsIfValid(
          [&row, edgeNode = edgeNode.get(), this](
              const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
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
          [&row, vIdLen, isIntId, edgeNode = edgeNode.get(), this](
              folly::StringPiece key,
              RowReaderWrapper* reader,
              const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
            for (const auto& prop : *props) {
              if (prop.returned_ || (prop.filtered_ && expCtx_ != nullptr)) {
                auto value = QueryUtils::readEdgeProp(key, vIdLen, isIntId, reader, prop);
                if (!value.ok()) {
                  return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
                }
                if (prop.filtered_ && expCtx_ != nullptr) {
                  expCtx_->setEdgeProp(edgeNode->getEdgeName(), prop.name_, value.value());
                }
                if (prop.returned_) {
                  VLOG(2) << "Collect prop " << prop.name_;
                  row.emplace_back(std::move(value).value());
                }
              }
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
          });
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        break;
      }
    }
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      if (filter_ == nullptr) {
        resultDataSet_->rows.emplace_back(std::move(row));
      } else {
        auto result = QueryUtils::vTrue(filter_->eval(*expCtx_));
        if (result.ok() && result.value()) {
          resultDataSet_->rows.emplace_back(std::move(row));
        }
      }
    }
    expCtx_->clear();
    for (auto& edgeNode : edgeNodes_) {
      edgeNode->clear();
    }
    return ret;
  }

 private:
  RuntimeContext* context_;
  std::vector<std::unique_ptr<FetchEdgeNode>> edgeNodes_;
  std::unordered_map<EdgeType, std::size_t> edgeNodesIndex_;
  bool enableReadFollower_;
  int64_t limit_;
  // cursors for next scan
  std::unordered_map<PartitionID, cpp2::ScanCursor>* cursors_;
  nebula::DataSet* resultDataSet_;
  StorageExpressionContext* expCtx_{nullptr};
  Expression* filter_{nullptr};
};

}  // namespace storage
}  // namespace nebula
#endif
