/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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

  explicit GetTagPropNode(RuntimeContext* context,
                          std::vector<TagNode*> tagNodes,
                          nebula::DataSet* resultDataSet)
      : context_(context), tagNodes_(std::move(tagNodes)), resultDataSet_(resultDataSet) {
    name_ = "GetTagPropNode";
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const VertexID& vId) override {
    auto vidPrefix = NebulaKeyUtils::vertexPrefix(context_->vIdLen(), partId, vId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto rc = context_->env()->kvstore_->prefix(context_->spaceId(), partId, vidPrefix, &iter);
    if (rc == nebula::cpp2::ErrorCode::SUCCEEDED && !iter->valid()) {
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    auto ret = RelNode::doExecute(partId, vId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
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
        return ret;
      }
    }
    resultDataSet_->rows.emplace_back(std::move(row));
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

 private:
  RuntimeContext* context_;
  std::vector<TagNode*> tagNodes_;
  nebula::DataSet* resultDataSet_;
};

class GetEdgePropNode : public QueryNode<cpp2::EdgeKey> {
 public:
  using RelNode::doExecute;

  GetEdgePropNode(RuntimeContext* context,
                  std::vector<EdgeNode<cpp2::EdgeKey>*> edgeNodes,
                  nebula::DataSet* resultDataSet)
      : context_(context), edgeNodes_(std::move(edgeNodes)), resultDataSet_(resultDataSet) {
    QueryNode::name_ = "GetEdgePropNode";
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
    auto ret = RelNode::doExecute(partId, edgeKey);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }

    List row;
    auto vIdLen = context_->vIdLen();
    auto isIntId = context_->isIntId();
    for (auto* edgeNode : edgeNodes_) {
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
        return ret;
      }
    }
    resultDataSet_->rows.emplace_back(std::move(row));
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

 private:
  RuntimeContext* context_;
  std::vector<EdgeNode<cpp2::EdgeKey>*> edgeNodes_;
  nebula::DataSet* resultDataSet_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_GETPROPNODE_H_
