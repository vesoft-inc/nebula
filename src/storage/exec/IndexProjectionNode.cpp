/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/exec/IndexProjectionNode.h"
namespace nebula {
namespace storage {
IndexProjectionNode::IndexProjectionNode(RuntimeContext* context,
                                         const std::vector<std::string>& requiredColumns)
    : IndexNode(context, "IndexProjectionNode"), requiredColumns_(requiredColumns) {}
nebula::cpp2::ErrorCode IndexProjectionNode::init(InitContext& ctx) {
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
IndexNode::ErrorOr<Row> IndexProjectionNode::doNext(bool& hasNext) {
  DCHECK_EQ(children_.size(), 1);
  auto& child = *children_[0];
  auto result = child.next(hasNext);
  if (::nebula::ok(result) && hasNext) {
    return ErrorOr<Row>(project(::nebula::value(std::move(result))));
  } else {
    return ErrorOr<Row>(Row());
  }
}
Row IndexProjectionNode::project(Row&& row) {
  Row ret;
  for (auto& col : requiredColumns_) {
    ret.emplace_back(std::move(row[colPos_[col]]));
  }
  return ret;
}

}  // namespace storage
}  // namespace nebula
