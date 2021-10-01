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
      : QueryNode<VertexID>(context, "GetTagPropNode"),
        tagNodes_(std::move(tagNodes)),
        resultDataSet_(resultDataSet) {}

  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const VertexID& vId) override {
    auto ret = RelNode::doExecute(partId, vId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }

    // if none of the tag node valid, do not emplace the row
    if (!std::any_of(tagNodes_.begin(), tagNodes_.end(), [](const auto& tagNode) {
          return tagNode->valid();
        })) {
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    List row;
    // vertexId is the first column
    if (this->isIntId()) {
      row.emplace_back(*reinterpret_cast<const int64_t*>(vId.data()));
    } else {
      row.emplace_back(vId);
    }
    auto vIdLen = this->vIdLen();
    auto isIntId = this->isIntId();
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
  std::vector<TagNode*> tagNodes_;
  nebula::DataSet* resultDataSet_;
};

class GetEdgePropNode : public QueryNode<cpp2::EdgeKey> {
 public:
  using RelNode::doExecute;

  GetEdgePropNode(RuntimeContext* context,
                  std::vector<EdgeNode<cpp2::EdgeKey>*> edgeNodes,
                  nebula::DataSet* resultDataSet)
      : QueryNode<cpp2::EdgeKey>(context, "GetEdgePropNode"),
        edgeNodes_(std::move(edgeNodes)),
        resultDataSet_(resultDataSet) {}

  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
    auto ret = RelNode::doExecute(partId, edgeKey);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }

    List row;
    auto vIdLen = this->context_->vIdLen();
    auto isIntId = this->context_->isIntId();
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
  std::vector<EdgeNode<cpp2::EdgeKey>*> edgeNodes_;
  nebula::DataSet* resultDataSet_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_GETPROPNODE_H_
