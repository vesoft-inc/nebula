/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_EXEC_FILTERNODE_H_
#define STORAGE_EXEC_FILTERNODE_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "storage/context/StorageExpressionContext.h"
#include "storage/exec/HashJoinNode.h"

namespace nebula {
namespace storage {

enum class FilterMode {
  TAG_AND_EDGE = 0,
  TAG_ONLY = 1,
};

/*
FilterNode will receive the result from upstream, check whether tag data or edge
data could pass the expression filter. FilterNode can only accept one upstream
node, user must make sure that the upstream only output only tag data or edge
data, but not both.

As for GetNeighbors, it will have filter that involves both tag and edge
expression. In that case, FilterNode has a upstream of HashJoinNode, which will
keep popping out edge data. All tag data has been put into ExpressionContext
before FilterNode is doExecuted. By that means, it can check the filter of tag +
edge.
*/
template <typename T>
class FilterNode : public IterateNode<T> {
 public:
  using RelNode<T>::doExecute;

  FilterNode(RuntimeContext* context,
             IterateNode<T>* upstream,
             StorageExpressionContext* expCtx = nullptr,
             Expression* exp = nullptr,
             Expression* tagFilterExp = nullptr)
      : IterateNode<T>(upstream),
        context_(context),
        expCtx_(expCtx),
        filterExp_(exp),
        tagFilterExp_(tagFilterExp) {
    IterateNode<T>::name_ = "FilterNode";
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId, const T& vId) override {
    auto ret = RelNode<T>::doExecute(partId, vId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }

    do {
      if (context_->resultStat_ == ResultStatus::ILLEGAL_DATA) {
        break;
      }
      if (this->valid() && !check()) {
        if (context_->resultStat_ != ResultStatus::TAG_FILTER_OUT) {
          context_->resultStat_ = ResultStatus::FILTER_OUT;
        }
        this->next();
        continue;
      }
      break;
    } while (true);
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  void setFilterMode(FilterMode mode) {
    mode_ = mode;
  }

 private:
  bool check() override {
    if (filterExp_ == nullptr) {
      return true;
    }
    switch (mode_) {
      case FilterMode::TAG_AND_EDGE:
        return checkTagAndEdge();
      case FilterMode::TAG_ONLY:
        return checkTagOnly();
      default:
        return checkTagAndEdge();
    }
  }

  bool checkTagOnly() {
    auto result = filterExp_->eval(*expCtx_);
    // NULL is always false
    auto ret = result.toBool();
    return ret.isBool() && ret.getBool();
  }

  // return true when the value iter points to a value which can filter
  bool checkTagAndEdge() {
    expCtx_->reset(this->reader(), this->key().str());
    if (tagFilterExp_ != nullptr) {
      auto res = tagFilterExp_->eval(*expCtx_);
      if (!res.isBool() || !res.getBool()) {
        context_->resultStat_ = ResultStatus::TAG_FILTER_OUT;
        return false;
      }
    }
    // result is false when filter out
    auto result = filterExp_->eval(*expCtx_);
    // NULL is always false
    auto ret = result.toBool();
    return ret.isBool() && ret.getBool();
  }

 private:
  RuntimeContext* context_;
  StorageExpressionContext* expCtx_;
  Expression* filterExp_{nullptr};
  Expression* tagFilterExp_{nullptr};
  FilterMode mode_{FilterMode::TAG_AND_EDGE};
  int32_t callCheck{0};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_FILTERNODE_H_
