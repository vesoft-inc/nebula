/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "common/base/Base.h"
#include "storage/exec/GetPropNode.h"

namespace nebula {
namespace storage {

using Cursor = std::string;

// Node to scan vertices of one partition
class ScanVertexPropNode : public QueryNode<Cursor> {
 public:
  using RelNode<Cursor>::doExecute;

  explicit ScanVertexPropNode(RuntimeContext* context,
                              std::vector<std::unique_ptr<TagNode>> tagNodes,
                              bool enableReadFollower,
                              int64_t limit,
                              std::unordered_map<PartitionID, cpp2::ScanCursor>* cursors,
                              nebula::DataSet* resultDataSet)
      : context_(context),
        tagNodes_(std::move(tagNodes)),
        enableReadFollower_(enableReadFollower),
        limit_(limit),
        cursors_(cursors),
        resultDataSet_(resultDataSet) {
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
        collectOneRow(isIntId, vIdLen, currentVertexId);
      }  // collect vertex row
      currentVertexId = vertexId;
      if (static_cast<int64_t>(resultDataSet_->rowSize()) >= rowLimit) {
        break;
      }
      auto value = iter->val();
      tagNodes_[tagIdIndex->second]->doExecute(key.toString(), value.toString());
    }  // iterate key
    if (static_cast<int64_t>(resultDataSet_->rowSize()) < rowLimit) {
      collectOneRow(isIntId, vIdLen, currentVertexId);
    }

    cpp2::ScanCursor c;
    if (iter->valid()) {
      c.set_has_next(true);
      c.set_next_cursor(iter->key().str());
    } else {
      c.set_has_next(false);
    }
    cursors_->emplace(partId, std::move(c));
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  void collectOneRow(bool isIntId, std::size_t vIdLen, const std::string& currentVertexId) {
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
            [&row](const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
              for (const auto& prop : *props) {
                if (prop.returned_) {
                  row.emplace_back(Value());
                }
              }
              return nebula::cpp2::ErrorCode::SUCCEEDED;
            },
            [&row, vIdLen, isIntId](
                folly::StringPiece key,
                RowReader* reader,
                const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
              if (!QueryUtils::collectVertexProps(key, vIdLen, isIntId, reader, props, row).ok()) {
                return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
              }
              return nebula::cpp2::ErrorCode::SUCCEEDED;
            });
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
          break;
        }
      }
      if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
        resultDataSet_->rows.emplace_back(std::move(row));
      }
      for (auto& tagNode : tagNodes_) {
        tagNode->clear();
      }
    }
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
                   nebula::DataSet* resultDataSet)
      : context_(context),
        edgeNodes_(std::move(edgeNodes)),
        enableReadFollower_(enableReadFollower),
        limit_(limit),
        cursors_(cursors),
        resultDataSet_(resultDataSet) {
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
      collectOneRow(isIntId, vIdLen);
    }

    cpp2::ScanCursor c;
    if (iter->valid()) {
      c.set_has_next(true);
      c.set_next_cursor(iter->key().str());
    } else {
      c.set_has_next(false);
    }
    cursors_->emplace(partId, std::move(c));
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  void collectOneRow(bool isIntId, std::size_t vIdLen) {
    List row;
    nebula::cpp2::ErrorCode ret = nebula::cpp2::ErrorCode::SUCCEEDED;
    for (auto& edgeNode : edgeNodes_) {
      ret = edgeNode->collectEdgePropsIfValid(
          [&row](const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
            for (const auto& prop : *props) {
              if (prop.returned_) {
                row.emplace_back(Value());
              }
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
          },
          [&row, vIdLen, isIntId](
              folly::StringPiece key,
              RowReader* reader,
              const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
            if (!QueryUtils::collectEdgeProps(key, vIdLen, isIntId, reader, props, row).ok()) {
              return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
          });
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        break;
      }
    }
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      resultDataSet_->rows.emplace_back(std::move(row));
    }
    for (auto& edgeNode : edgeNodes_) {
      edgeNode->clear();
    }
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
};

}  // namespace storage
}  // namespace nebula
