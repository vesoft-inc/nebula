/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once

#include "folly/Likely.h"
#include "storage/exec/IndexNode.h"
namespace nebula {
namespace storage {
class IndexProjectionNode : public IndexNode {
 public:
  IndexProjectionNode(RuntimeContext* context, const std::vector<std::string>& requiredColumns);
  nebula::cpp2::ErrorCode init(InitContext& ctx) override {
    DCHECK_EQ(children_.size(), 1);
    for (auto& col : requiredColumns_) {
      ctx.requiredColumns.insert(col);
    }
    auto ret = children_[0]->init(ctx);
    if (UNLIKELY(ret != ::nebula::cpp2::ErrorCode::SUCCEEDED)) {
      return ret;
    }
    for (auto& col : requiredColumns_) {
      auto iter = ctx.retColMap.find(col);
      DCHECK_NE(iter, ctx.retColMap.end());
      colPos_[col] = iter->second;
    }
    ctx.returnColumns = requiredColumns_;
    ctx.retColMap.clear();
    for (size_t i = 0; i < ctx.returnColumns.size(); i++) {
      ctx.retColMap[ctx.returnColumns[i]] = i;
    }
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  ErrorOr<Row> doNext(bool& hasNext) override {
    DCHECK_EQ(children_.size(), 1);
    auto& child = *children_[0];
    auto result = child.next(hasNext);
    if (::nebula::ok(result) && hasNext) {
      return ErrorOr<Row>(project(::nebula::value(std::move(result))));
    } else {
      return ErrorOr<Row>(Row());
    }
  }

 private:
  Row project(Row&& row) {
    Row ret;
    for (auto& col : requiredColumns_) {
      ret.emplace_back(std::move(row[colPos_[col]]));
    }
  }
  std::vector<std::string> requiredColumns_;
  Map<std::string, size_t> colPos_;
};
}  // namespace storage
}  // namespace nebula
