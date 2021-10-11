/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once
#include "common/base/ErrorOr.h"
#include "common/datatypes/DataSet.h"
#include "common/time/Duration.h"
#include "folly/AtomicLinkedList.h"
#include "folly/container/F14Map.h"
#include "interface/gen-cpp2/common_types.h"
#include "storage/CommonUtils.h"
namespace nebula {
namespace storage {
using ErrorCode = ::nebula::cpp2::ErrorCode;
template <typename K, typename V>
using Map = folly::F14FastMap<K, V>;
template <typename K>
using Set = folly::F14FastSet<K>;
struct InitContext {
  Set<std::string> requiredColumns;
  std::vector<std::string> returnColumns;
  Map<std::string, size_t> retColMap;
};
class IndexNode {
 public:
  template <typename ResultType>
  using ErrorOr = ::nebula::ErrorOr<ErrorCode, ResultType>;
  IndexNode(const IndexNode& node);
  explicit IndexNode(RuntimeContext* context, const std::string& name);
  virtual ~IndexNode() = default;
  virtual ::nebula::cpp2::ErrorCode init(InitContext& initCtx) {
    DCHECK_EQ(children_.size(), 1);
    return children_[0]->init(initCtx);
  }

  inline nebula::cpp2::ErrorCode execute(PartitionID partId);

  inline ErrorOr<Row> next(bool& hasNext);

  void addChild(std::unique_ptr<IndexNode> child) { children_.emplace_back(std::move(child)); }
  virtual std::unique_ptr<IndexNode> copy() = 0;
  const std::vector<std::unique_ptr<IndexNode>>& children() { return children_; }

 protected:
  virtual ErrorOr<Row> doNext(bool& hasNext) = 0;
  void beforeNext();
  void afterNext();
  virtual nebula::cpp2::ErrorCode doExecute(PartitionID partId);
  void beforeExecute();
  void afterExecute();

  RuntimeContext* context_;
  GraphSpaceID spaceId_;
  std::vector<std::unique_ptr<IndexNode>> children_;
  std::string name_;
  time::Duration duration_;
};

/* Defination of inline function */
inline IndexNode::ErrorOr<Row> IndexNode::next(bool& hasNext) {
  beforeNext();
  auto ret = doNext(hasNext);
  afterNext();
  return ret;
}
inline void IndexNode::beforeNext() { duration_.resume(); }
inline void IndexNode::afterNext() { duration_.pause(); }
inline nebula::cpp2::ErrorCode IndexNode::execute(PartitionID partId) {
  beforeExecute();
  auto ret = doExecute(partId);
  afterExecute();
  return std::move(ret);
}
inline void IndexNode::beforeExecute() { duration_.resume(); }
inline void IndexNode::afterExecute() { duration_.pause(); }
}  // namespace storage
}  // namespace nebula
