/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_EXEC_GETDSTBYSRCNODE_H_
#define STORAGE_EXEC_GETDSTBYSRCNODE_H_

#include "common/base/Base.h"

namespace nebula {
namespace storage {

class GetDstBySrcNode : public QueryNode<VertexID> {
 public:
  using RelNode::doExecute;

  GetDstBySrcNode(RuntimeContext* context,
                  const std::vector<SingleEdgeNode*>& edgeNodes,
                  EdgeContext* edgeContext,
                  nebula::List* result)
      : context_(context), edgeNodes_(edgeNodes), edgeContext_(edgeContext), result_(result) {
    name_ = "GetDstBySrcNode";
  }

  // need to override doExecute because the return format of GetNeighborsNode and
  // GetDstBySrcNode are different
  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const VertexID& vId) override {
    auto ret = RelNode::doExecute(partId, vId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }

    std::vector<SingleEdgeIterator*> iters;
    for (auto* edgeNode : edgeNodes_) {
      iters.emplace_back(edgeNode->iter());
    }
    iter_.reset(new MultiEdgeIterator(std::move(iters)));
    if (iter_->valid()) {
      setCurrentEdgeInfo(iter_->edgeType());
    }
    return iterateEdges();
  }

 private:
  nebula::cpp2::ErrorCode iterateEdges() {
    for (; iter_->valid(); iter_->next()) {
      EdgeType type = iter_->edgeType();
      if (type != context_->edgeType_) {
        // update info when edgeType changes while iterating over different edge types
        setCurrentEdgeInfo(type);
      }
      auto key = iter_->key();
      auto reader = iter_->reader();
      auto props = context_->props_;
      DCHECK_EQ(props->size(), 1);

      nebula::List list;
      // collect props need to return
      if (!QueryUtils::collectEdgeProps(
               key, context_->vIdLen(), context_->isIntId(), reader, props, list)
               .ok()) {
        return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
      }
      result_->values.emplace_back(std::move(list.values[0]));
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  void setCurrentEdgeInfo(EdgeType type) {
    auto idxIter = edgeContext_->indexMap_.find(type);
    CHECK(idxIter != edgeContext_->indexMap_.end());
    auto schemaIter = edgeContext_->schemas_.find(std::abs(type));
    CHECK(schemaIter != edgeContext_->schemas_.end());
    CHECK(!schemaIter->second.empty());

    context_->edgeSchema_ = schemaIter->second.back().get();
    auto idx = idxIter->second;
    context_->edgeType_ = type;
    context_->edgeName_ = edgeNodes_[iter_->getIdx()]->getEdgeName();
    context_->props_ = &(edgeContext_->propContexts_[idx].second);
  }

 private:
  RuntimeContext* context_;
  std::vector<SingleEdgeNode*> edgeNodes_;
  EdgeContext* edgeContext_;
  nebula::List* result_;
  std::unique_ptr<MultiEdgeIterator> iter_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_GETDSTBYSRCNODE_H_
