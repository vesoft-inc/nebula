/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <bits/stdint-intn.h>

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
    std::vector<TagNode*> tags;
    for (const auto& t : tagNodes_) {
      tags.emplace_back(t.get());
    }
    node_ = std::make_unique<GetTagPropNode>(context, tags, resultDataSet);
    for (auto* tag : tags) {
      node_->addDependency(tag);
    }
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const Cursor& cursor) override {
    auto ret = RelNode::doExecute(partId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }

    std::string start;
    std::string prefix = NebulaKeyUtils::vertexPrefix(partId);
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
    RowReaderWrapper reader;
    std::string currentVertexId;
    for (; iter->valid() && static_cast<int64_t>(resultDataSet_->rowSize()) < rowLimit;
         iter->next()) {
      auto key = iter->key();

      auto vertexId = NebulaKeyUtils::getVertexId(context_->vIdLen(), key);
      if (vertexId != currentVertexId) {
        currentVertexId = vertexId;
      } else {
        // skip same vertex id tag key
        continue;
      }
      node_->doExecute(partId, vertexId.subpiece(0, vertexId.find_first_of('\0')).toString());
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

 private:
  RuntimeContext* context_;
  std::unique_ptr<GetTagPropNode> node_;
  std::vector<std::unique_ptr<TagNode>> tagNodes_;
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
                   std::unordered_set<EdgeType> edgeTypes,
                   std::vector<std::unique_ptr<EdgeNode<cpp2::EdgeKey>>> edgeNodes,
                   bool enableReadFollower,
                   int64_t limit,
                   std::unordered_map<PartitionID, cpp2::ScanCursor>* cursors,
                   nebula::DataSet* resultDataSet)
      : context_(context),
        edgeTypes_(std::move(edgeTypes)),
        edgeNodes_(std::move(edgeNodes)),
        enableReadFollower_(enableReadFollower),
        limit_(limit),
        cursors_(cursors),
        resultDataSet_(resultDataSet) {
    QueryNode::name_ = "ScanEdgePropNode";
    std::vector<EdgeNode<cpp2::EdgeKey>*> edges;
    for (const auto& e : edgeNodes_) {
      edges.emplace_back(e.get());
    }
    node_ = std::make_unique<GetEdgePropNode>(context, edges, resultDataSet);

    for (auto* edge : edges) {
      node_->addDependency(edge);
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
    RowReaderWrapper reader;

    for (; iter->valid() && static_cast<int64_t>(resultDataSet_->rowSize()) < rowLimit;
         iter->next()) {
      auto key = iter->key();
      if (!NebulaKeyUtils::isEdge(context_->vIdLen(), key)) {
        continue;
      }

      auto srcId = NebulaKeyUtils::getSrcId(context_->vIdLen(), key);
      auto dstId = NebulaKeyUtils::getDstId(context_->vIdLen(), key);
      auto edgeType = NebulaKeyUtils::getEdgeType(context_->vIdLen(), key);
      auto ranking = NebulaKeyUtils::getRank(context_->vIdLen(), key);

      if (edgeTypes_.find(edgeType) == edgeTypes_.end()) {
        continue;
      }

      cpp2::EdgeKey edgeKey;
      edgeKey.set_src(srcId.subpiece(0, srcId.find_first_of('\0')).toString());
      edgeKey.set_dst(dstId.subpiece(0, dstId.find_first_of('\0')).toString());
      edgeKey.set_edge_type(edgeType);
      edgeKey.set_ranking(ranking);
      node_->doExecute(partId, std::move(edgeKey));
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

 private:
  RuntimeContext* context_;
  std::unordered_set<EdgeType> edgeTypes_;
  std::vector<std::unique_ptr<EdgeNode<cpp2::EdgeKey>>> edgeNodes_;
  std::unique_ptr<GetEdgePropNode> node_;
  bool enableReadFollower_;
  int64_t limit_;
  // cursors for next scan
  std::unordered_map<PartitionID, cpp2::ScanCursor>* cursors_;
  nebula::DataSet* resultDataSet_;
};

}  // namespace storage
}  // namespace nebula
