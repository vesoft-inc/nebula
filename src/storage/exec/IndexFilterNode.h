/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef STORAGE_EXEC_INDEXFILTERNODE_H_
#define STORAGE_EXEC_INDEXFILTERNODE_H_

#include "common/base/Base.h"
#include "common/context/ExpressionContext.h"
#include "common/expression/Expression.h"
#include "storage/exec/IndexEdgeNode.h"
#include "storage/exec/IndexScanNode.h"
#include "storage/exec/IndexVertexNode.h"
#include "storage/exec/RelNode.h"

namespace nebula {
namespace storage {

template <typename T>
class IndexFilterNode final : public RelNode<T> {
 public:
  using RelNode<T>::doExecute;

  // evalExprByIndex_ is true, all fileds in filter is in index. No need to read
  // data anymore.
  IndexFilterNode(RuntimeContext* context,
                  IndexScanNode<T>* indexScanNode,
                  StorageExpressionContext* exprCtx,
                  Expression* exp,
                  bool isEdge,
                  int64_t limit = -1)
      : context_(context),
        indexScanNode_(indexScanNode),
        exprCtx_(exprCtx),
        filterExp_(exp),
        isEdge_(isEdge),
        limit_(limit) {
    evalExprByIndex_ = true;
    RelNode<T>::name_ = "IndexFilterNode";
  }

  // evalExprByIndex_ is false, some fileds in filter is out of index, which
  // need to read data.
  IndexFilterNode(RuntimeContext* context,
                  IndexEdgeNode<T>* indexEdgeNode,
                  StorageExpressionContext* exprCtx,
                  Expression* exp,
                  int64_t limit = -1)
      : context_(context),
        indexEdgeNode_(indexEdgeNode),
        exprCtx_(exprCtx),
        filterExp_(exp),
        limit_(limit) {
    evalExprByIndex_ = false;
    isEdge_ = true;
  }

  // evalExprByIndex_ is false, some fileds in filter is out of index, which
  // need to read data.
  IndexFilterNode(RuntimeContext* context,
                  IndexVertexNode<T>* indexVertexNode,
                  StorageExpressionContext* exprCtx,
                  Expression* exp,
                  int64_t limit = -1)
      : context_(context),
        indexVertexNode_(indexVertexNode),
        exprCtx_(exprCtx),
        filterExp_(exp),
        limit_(limit) {
    evalExprByIndex_ = false;
    isEdge_ = false;
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId) override {
    data_.clear();
    auto ret = RelNode<T>::doExecute(partId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }
    std::vector<kvstore::KV> data;
    if (evalExprByIndex_) {
      data = indexScanNode_->moveData();
    } else if (isEdge_) {
      data = indexEdgeNode_->moveData();
    } else {
      data = indexVertexNode_->moveData();
    }
    int64_t count = 0;
    for (const auto& k : data) {
      if (context_->isPlanKilled()) {
        return nebula::cpp2::ErrorCode::E_PLAN_IS_KILLED;
      }
      if (evalExprByIndex_) {
        if (check(k.first)) {
          data_.emplace_back(k.first, k.second);
          count++;
        }
      } else {
        const auto& schemas =
            isEdge_ ? indexEdgeNode_->getSchemas() : indexVertexNode_->getSchemas();
        auto reader = RowReaderWrapper::getRowReader(schemas, k.second);
        if (!reader) {
          continue;
        }
        if (check(reader.get(), k.first)) {
          data_.emplace_back(k.first, k.second);
          count++;
        }
      }
      if (limit_ > 0 && count >= limit_) {
        break;
      }
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  std::vector<kvstore::KV> moveData() { return std::move(data_); }

  const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>& getSchemas() {
    return isEdge_ ? indexEdgeNode_->getSchemas() : indexVertexNode_->getSchemas();
  }

  bool hasNullableCol() const { return exprCtx_->hasNullableCol(); }

  const std::vector<meta::cpp2::ColumnDef>& indexCols() const { return exprCtx_->indexCols(); }

 private:
  bool check(const std::string& raw) {
    if (filterExp_ != nullptr) {
      exprCtx_->reset(raw);
      auto result = filterExp_->eval(*exprCtx_);
      if (result.type() == Value::Type::BOOL) {
        return result.getBool();
      } else {
        return false;
      }
    }
    return false;
  }

  bool check(RowReader* reader, const std::string& raw) {
    if (filterExp_ != nullptr) {
      exprCtx_->reset(reader, raw);
      auto result = filterExp_->eval(*exprCtx_);
      if (result.type() == Value::Type::BOOL) {
        return result.getBool();
      } else {
        return false;
      }
    }
    return false;
  }

 private:
  RuntimeContext* context_;
  IndexScanNode<T>* indexScanNode_{nullptr};
  IndexEdgeNode<T>* indexEdgeNode_{nullptr};
  IndexVertexNode<T>* indexVertexNode_{nullptr};
  StorageExpressionContext* exprCtx_;
  Expression* filterExp_;
  bool isEdge_;
  bool evalExprByIndex_;
  int64_t limit_;
  std::vector<kvstore::KV> data_{};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_INDEXFILTERNODE_H_
